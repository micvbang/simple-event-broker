package app

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/micvbang/go-helpy/sizey"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/sebbench"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/spf13/cobra"
)

var benchmarkFlags BenchmarkFlags

func init() {
	fs := benchmarkCmd.Flags()

	fs.IntVarP(&benchmarkFlags.numRuns, "runs", "r", 1, "Number of times to run the benchmark")
	fs.IntVarP(&benchmarkFlags.numWorkers, "workers", "w", runtime.NumCPU()*2, "Number of workers to add data")
	fs.BoolVarP(&benchmarkFlags.localBroker, "local-broker", "l", true, "Whether to start a broker only for these benchmarks")
	fs.StringVar(&benchmarkFlags.remoteBrokerAddress, "remote-broker-address", "http://localhost:51313", "Address of remote broker to connect to instead of starting local broker")
	fs.StringVar(&benchmarkFlags.remoteBrokerAPIKey, "remote-broker-api-key", "api-key", "API key to use for remote broker")

	fs.IntVar(&benchmarkFlags.numBatches, "batches", 4096, "Number of records to generate")
	fs.IntVar(&benchmarkFlags.recordSize, "record-size", 1024, "Size of records in bytes")
	fs.IntVarP(&benchmarkFlags.numRecordsPerBatch, "records-per-batch", "b", 1024, "Records per batch")

	fs.IntVar(&benchmarkFlags.batchBytesMax, "batcher-max-bytes", 10*sizey.MB, "Maximum number of bytes to wait before committing incoming batch to storage")
	fs.DurationVar(&benchmarkFlags.batchBlockTime, "batcher-block-time", 5*time.Millisecond, "Maximum amount of time to wait before committing incoming batches to storage")
}

var benchmarkCmd = &cobra.Command{
	Use:   "benchmark",
	Short: "Run benchmark workload",
	Long:  "Run benchmark workload to check performance of various configurations",
	RunE: func(cmd *cobra.Command, args []string) error {
		flags := benchmarkFlags
		log := logger.NewWithLevel(context.Background(), logger.LevelWarn)

		numRecords := flags.numBatches * flags.numRecordsPerBatch
		bytesPerBatch := flags.numRecordsPerBatch * flags.recordSize
		totalBytes := numRecords * flags.recordSize
		fmt.Printf("Generating %d batches of size %s (%d records), totalling %s\n", flags.numBatches, sizey.FormatBytes(bytesPerBatch), numRecords, sizey.FormatBytes(totalBytes))

		var broker sebbench.Broker
		if flags.localBroker {
			broker = sebbench.NewLocalBroker(log, flags.batchBlockTime, flags.batchBytesMax)
		} else {
			broker = &sebbench.RemoteBroker{
				RemoteBrokerAddress: flags.remoteBrokerAddress,
				RemoteBrokerAPIKey:  flags.remoteBrokerAPIKey,
			}
		}

		runs := make([]sebbench.Stats, flags.numRuns)
		for runID := range flags.numRuns {
			fmt.Printf("Run %d/%d\n", runID+1, flags.numRuns)

			const topicName = "some-topic"

			batches := make(chan sebrecords.Batch, 8)
			go sebbench.GenerateBatches(log, batches, flags.numBatches, flags.numRecordsPerBatch, flags.recordSize)

			client, err := broker.Start()
			if err != nil {
				log.Fatalf("broker start: %w", err)
			}

			wg := &sync.WaitGroup{}
			t0 := time.Now()
			// brokerAddRecords(log, wg, broker, batches, flags.numWorkers, topicName)
			httpAddRecords(log, wg, client, batches, flags.numWorkers, topicName)

			wg.Wait()
			elapsed := time.Since(t0)

			broker.Stop()

			runs[runID] = sebbench.Stats{
				Elapsed:          elapsed,
				RecordsPerSecond: float64(numRecords) / elapsed.Seconds(),
				BatchesPerSecond: float64(flags.numBatches) / elapsed.Seconds(),
				MbitPerSecond:    float64((totalBytes*8)/1024/1024) / elapsed.Seconds(),
			}
			fmt.Printf("%s\n\n", runs[runID].String())
		}

		fmt.Printf("Config:\n")
		fmt.Printf("Num workers:\t\t%d\n", flags.numWorkers)
		fmt.Printf("Num batches:\t\t%d\n", flags.numBatches)
		fmt.Printf("Num records/batch:\t%d\n", flags.numRecordsPerBatch)
		fmt.Printf("Record size:\t\t%s (%dB)\n", sizey.FormatBytes(flags.recordSize), flags.recordSize)
		fmt.Printf("Total bytes:\t\t%s (%dB)\n", sizey.FormatBytes(totalBytes), totalBytes)

		if flags.localBroker {
			fmt.Printf("Batch block time:\t%s\n", flags.batchBlockTime)
			fmt.Printf("Batch bytes max:\t%s (%d)\n", sizey.FormatBytes(flags.batchBytesMax), flags.batchBytesMax)
		}
		fmt.Printf("\n")

		sebbench.PrintStats(runs)

		return nil
	},
}

func httpAddRecords(log logger.Logger, wg *sync.WaitGroup, client *seb.RecordClient, batches <-chan sebrecords.Batch, workers int, topicName string) {
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()

			for batch := range batches {
				err := client.AddRecords(topicName, batch.Sizes(), batch.Data())
				if err != nil {
					log.Fatalf("failed to add records: %s", err)
				}
			}
		}()
	}
}

// TODO: we might want to allow running benchmarks without HTTP layer being involved
// func brokerAddRecords(log logger.Logger, wg *sync.WaitGroup, broker *sebbroker.Broker, batches <-chan recordBatch, workers int, topicName string) {
// 	wg.Add(workers)

// 	for range workers {
// 		go func() {
// 			defer wg.Done()

// 			for batch := range batches {
// 				offsets, err := broker.AddRecords(topicName, batch.recordSizes, batch.records)
// 				if err != nil {
// 					log.Fatalf("failed to add records: %s", err)
// 				}

// 				if len(offsets) != len(batch.recordSizes) {
// 					log.Fatalf("expected %d offsets, got %d", len(batch.recordSizes), len(offsets))
// 				}
// 			}
// 		}()
// 	}
// }

func batchToRecords(recordSizes []uint32, batch []byte) [][]byte {
	records := make([][]byte, len(recordSizes))
	bytesUsed := uint32(0)
	for i, recordSize := range recordSizes {
		records[i] = batch[bytesUsed : bytesUsed+recordSize]
		bytesUsed += recordSize
	}

	return records
}

type BenchmarkFlags struct {
	numRuns    int
	numWorkers int

	localBroker         bool
	remoteBrokerAddress string
	remoteBrokerAPIKey  string

	numBatches         int
	recordSize         int
	numRecordsPerBatch int

	batchBlockTime time.Duration
	batchBytesMax  int
}
