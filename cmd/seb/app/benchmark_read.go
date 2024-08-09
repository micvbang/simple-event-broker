package app

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/micvbang/go-helpy/sizey"
	"github.com/micvbang/go-helpy/uint64y"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/sebbench"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/spf13/cobra"
)

var benchmarkReadFlags BenchmarkReadFlags

func init() {
	fs := benchmarkReadCmd.Flags()

	fs.IntVarP(&benchmarkReadFlags.numRuns, "runs", "r", 1, "Number of times to run the benchmark")
	fs.IntVarP(&benchmarkReadFlags.numWorkers, "workers", "w", runtime.NumCPU()*2, "Number of workers to add data")
	fs.BoolVarP(&benchmarkReadFlags.localBroker, "local-broker", "l", true, "Whether to start a broker only for these benchmarks")
	fs.StringVar(&benchmarkReadFlags.remoteBrokerAddress, "remote-broker-address", "http://localhost:51313", "Address of remote broker to connect to instead of starting local broker")
	fs.StringVar(&benchmarkReadFlags.remoteBrokerAPIKey, "remote-broker-api-key", "api-key", "API key to use for remote broker")

	fs.IntVar(&benchmarkReadFlags.numBatches, "batches", 4096, "Number of records to generate")
	fs.IntVar(&benchmarkReadFlags.recordSize, "record-size", 1024, "Size of records in bytes")
	fs.IntVarP(&benchmarkReadFlags.numRecordsPerBatch, "records-per-batch", "b", 1024, "Records per batch")

	fs.IntVar(&benchmarkReadFlags.batchBytesMax, "batcher-max-bytes", 50*sizey.MB, "Maximum number of bytes to wait before committing incoming batch to storage")
	fs.DurationVar(&benchmarkReadFlags.batchBlockTime, "batcher-block-time", 5*time.Millisecond, "Maximum amount of time to wait before committing incoming batches to storage")

	fs.DurationVarP(&benchmarkReadFlags.benchmarkDuration, "duration", "d", 30*time.Second, "Amount of time to run the benchmark for")
}

var benchmarkReadCmd = &cobra.Command{
	Use:   "benchmark-read",
	Short: "Run benchmark read workload",
	Long:  "Run benchmark read workload to check performance of various configurations",
	RunE: func(cmd *cobra.Command, args []string) error {
		flags := benchmarkReadFlags
		log := logger.NewWithLevel(context.Background(), logger.LevelWarn)

		numRecords := flags.numBatches * flags.numRecordsPerBatch
		bytesPerBatch := flags.numRecordsPerBatch * flags.recordSize
		totalBytes := numRecords * flags.recordSize
		fmt.Printf("Generating %d batches of size %s (%d records), totalling %s\n", flags.numBatches, sizey.FormatBytes(bytesPerBatch), numRecords, sizey.FormatBytes(totalBytes))

		var broker sebbench.Broker
		if flags.localBroker {
			log.Warnf("Using local broker")
			broker = sebbench.NewLocalBroker(log, flags.batchBlockTime, flags.batchBytesMax)
		} else {
			log.Warnf("Using remote broker")
			broker = &sebbench.RemoteBroker{
				RemoteBrokerAddress: flags.remoteBrokerAddress,
				RemoteBrokerAPIKey:  flags.remoteBrokerAPIKey,
			}
		}

		const topicName = "some-topic"
		client, err := broker.Start()
		if err != nil {
			log.Fatalf("broker start: %w", err)
		}

		// TODO: make it configurable whether to insert data or not
		// insert data to read
		if flags.localBroker {
			numRecords := flags.numBatches * flags.numRecordsPerBatch
			bytesPerBatch := flags.numRecordsPerBatch * flags.recordSize
			totalBytes := numRecords * flags.recordSize
			fmt.Printf("Inserting %d batches of size %s (%d records), totalling %s to prepare for read benchmark\n", flags.numBatches, sizey.FormatBytes(bytesPerBatch), numRecords, sizey.FormatBytes(totalBytes))

			batches := make(chan sebrecords.Batch, 8)
			go sebbench.GenerateBatches(log, batches, flags.numBatches, flags.numRecordsPerBatch, flags.recordSize)

			wg := &sync.WaitGroup{}
			t0 := time.Now()
			httpAddRecords(log, wg, client, batches, flags.numWorkers, topicName)
			wg.Wait()
			fmt.Printf("Took %s\n", time.Since(t0))
		}

		runs := make([]sebbench.Stats, flags.numRuns)
		for runID := range flags.numRuns {
			fmt.Printf("Run %d/%d\n", runID+1, flags.numRuns)

			readRequests := make(chan readRequest, flags.numWorkers*2)

			// TODO: make parameters configurable
			numRequests := 10_000
			requestRecords := flags.numRecordsPerBatch * 8
			go generateReadRequests(readRequests, numRequests, uint64(numRecords), requestRecords)

			wg := &sync.WaitGroup{}
			t0 := time.Now()
			httpGetRecords(log, wg, client, flags.numWorkers, readRequests, topicName)

			wg.Wait()
			elapsed := time.Since(t0)

			runs[runID] = sebbench.Stats{
				Elapsed:          elapsed,
				RecordsPerSecond: float64(requestRecords*numRequests) / elapsed.Seconds(),
				BatchesPerSecond: float64(numRequests) / elapsed.Seconds(),
				MbitPerSecond:    float64(requestRecords*numRequests*flags.recordSize*8) / 1024 / 1024 / elapsed.Seconds(),
			}
			fmt.Printf("%s\n\n", runs[runID].String())
		}

		broker.Stop()

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

func generateReadRequests(readRequests chan<- readRequest, numRequests int, numRecords uint64, requestRecords int) {
	for range numRequests {
		offset := uint64y.RandomN(numRecords)
		readRequests <- readRequest{
			offset:     offset,
			maxRecords: min(int(numRecords-offset), requestRecords),
		}
	}
	close(readRequests)
}

type readRequest struct {
	offset     uint64
	maxRecords int
}

func httpGetRecords(log logger.Logger, wg *sync.WaitGroup, client *seb.RecordClient, workers int, readRequests <-chan readRequest, topicName string) {
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()

			for request := range readRequests {
				records, err := client.GetRecords(topicName, request.offset, seb.GetRecordsInput{
					MaxRecords:   request.maxRecords,
					SoftMaxBytes: 50 * sizey.MB,
					Timeout:      5 * time.Second,
				})
				if err != nil {
					log.Fatalf(err.Error())
				}

				if len(records) != request.maxRecords {
					log.Fatalf("expected %d records, got %d", request.maxRecords, len(records))
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

type BenchmarkReadFlags struct {
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

	benchmarkDuration time.Duration
}
