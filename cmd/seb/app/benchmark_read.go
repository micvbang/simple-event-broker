package app

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/micvbang/go-helpy"
	"github.com/micvbang/go-helpy/sizey"
	"github.com/micvbang/go-helpy/syncy"
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

	fs.IntVar(&benchmarkReadFlags.logLevel, "log-level", int(logger.LevelWarn), "Number of times to run the benchmark")
	fs.IntVarP(&benchmarkReadFlags.numRuns, "runs", "r", 1, "Number of times to run the benchmark")
	fs.StringVar(&benchmarkReadFlags.topicName, "topic-name", "some-topic", "Name of topic to use for test")
	fs.IntVarP(&benchmarkReadFlags.numWorkers, "workers", "w", runtime.NumCPU()*2, "Number of workers to add data")
	fs.BoolVarP(&benchmarkReadFlags.localBroker, "local-broker", "l", true, "Whether to start a broker only for these benchmarks")
	fs.StringVar(&benchmarkReadFlags.remoteBrokerAddress, "remote-broker-address", "http://localhost:51313", "Address of remote broker to connect to instead of starting local broker")
	fs.StringVar(&benchmarkReadFlags.remoteBrokerAPIKey, "remote-broker-api-key", "api-key", "API key to use for remote broker")

	fs.IntVar(&benchmarkReadFlags.numBatches, "batches", 4096, "Number of records to generate")
	fs.IntVar(&benchmarkReadFlags.recordSize, "record-size", 1024, "Size of records in bytes")
	fs.IntVar(&benchmarkReadFlags.numRecordsPerBatch, "records-per-batch", 1024, "Records per batch")
	fs.IntVar(&benchmarkReadFlags.numRequests, "requests", 10_000, "Number of requests to send")
	fs.IntVar(&benchmarkReadFlags.numRecordsPerRequest, "records-per-request", 1024, "Records per request")

	fs.IntVar(&benchmarkReadFlags.batchBytesMax, "batcher-max-bytes", 50*sizey.MB, "Maximum number of bytes to wait before committing incoming batch to storage")
	fs.DurationVar(&benchmarkReadFlags.batchBlockTime, "batcher-block-time", 5*time.Millisecond, "Maximum amount of time to wait before committing incoming batches to storage")
}

var benchmarkReadCmd = &cobra.Command{
	Use:   "benchmark-read",
	Short: "Run benchmark read workload",
	Long:  "Run benchmark read workload to check performance of various configurations",
	RunE: func(cmd *cobra.Command, args []string) error {
		flags := benchmarkReadFlags
		log := logger.NewWithLevel(context.Background(), logger.LogLevel(flags.logLevel))

		bytesPerBatch := flags.numRecordsPerBatch * flags.recordSize
		fmt.Printf("Benchmark config:\n")
		fmt.Printf("Num workers:\t\t%d\n", flags.numWorkers)
		fmt.Printf("Num requests:\t\t%d\n", flags.numRequests)
		fmt.Printf("Records/request:\t%d\n", flags.numRecordsPerRequest)

		if flags.localBroker {
			totalBytesRequested := flags.numRequests * flags.numRecordsPerRequest * flags.recordSize
			bytesPerRequest := flags.numRecordsPerRequest * flags.recordSize
			fmt.Printf("Record size:\t\t%v (%vB)\n", sizey.FormatBytes(flags.recordSize), flags.recordSize)
			fmt.Printf("Bytes/request:\t\t%s (%dB)\n", sizey.FormatBytes(bytesPerRequest), bytesPerRequest)
			fmt.Printf("Total bytes:\t\t%s (%dB)\n", sizey.FormatBytes(totalBytesRequested), totalBytesRequested)

			fmt.Printf("\nLocal broker config:\n")
			fmt.Printf("Batch block time:\t%s\n", flags.batchBlockTime)
			fmt.Printf("Batch bytes max:\t%s (%d)\n", sizey.FormatBytes(flags.batchBytesMax), flags.batchBytesMax)
		}
		fmt.Printf("\n")

		batchPool := syncy.NewPool(func() *sebrecords.Batch {
			return helpy.Pointer(sebrecords.NewBatch(make([]uint32, 0, flags.numRecordsPerRequest), make([]byte, 0, 50*sizey.MB)))
		})

		var broker sebbench.Broker
		if flags.localBroker {
			log.Warnf("Using local broker")
			broker = sebbench.NewLocalBroker(log, batchPool, flags.batchBlockTime, flags.batchBytesMax)
		} else {
			log.Warnf("Using remote broker")
			broker = &sebbench.RemoteBroker{
				RemoteBrokerAddress: flags.remoteBrokerAddress,
				RemoteBrokerAPIKey:  flags.remoteBrokerAPIKey,
			}
		}

		client, err := broker.Start()
		if err != nil {
			log.Fatalf("broker start: %w", err)
		}

		if flags.localBroker {
			numRecords := flags.numBatches * flags.numRecordsPerBatch
			bytesToInsert := numRecords * flags.recordSize
			fmt.Printf("Inserting %d batches of size %s (%d records), totalling %s to prepare for read benchmark\n",
				flags.numBatches, sizey.FormatBytes(bytesPerBatch), numRecords, sizey.FormatBytes(bytesToInsert))

			batches := make(chan sebrecords.Batch, 8)
			go sebbench.GenerateBatches(log, batches, flags.numBatches, flags.numRecordsPerBatch, flags.recordSize)

			wg := &sync.WaitGroup{}
			t0 := time.Now()
			httpAddRecords(log, wg, client, batches, flags.numWorkers, flags.topicName)
			wg.Wait()
			fmt.Printf("Took %s\n\n", time.Since(t0))
		}

		topic, err := client.GetTopic(flags.topicName)
		if err != nil {
			log.Fatalf("retrieving topic: %v", err)
		}
		fmt.Printf("Topic '%s' next offset: %d\n", topic.Name, topic.NextOffset)

		bufPool := syncy.NewPool(func() []byte {
			return make([]byte, 0, 50*sizey.MB)
		})
		runs := make([]sebbench.Stats, flags.numRuns)
		for runID := range flags.numRuns {
			fmt.Printf("Run %d/%d\n", runID+1, flags.numRuns)

			readRequests := make(chan readRequest, flags.numRequests)
			totalRecordsRead := generateReadRequests(readRequests, flags.numRequests, topic.NextOffset-1, flags.numRecordsPerRequest)

			t0 := time.Now()
			totalBytesRead := httpGetRecords(log, client, flags.numWorkers, readRequests, flags.topicName, bufPool)
			elapsed := time.Since(t0)

			runs[runID] = sebbench.Stats{
				Elapsed:          elapsed,
				RecordsPerSecond: float64(totalRecordsRead) / elapsed.Seconds(),
				// BatchesPerSecond: float64(flags.numRequests) / elapsed.Seconds(),
				MbitPerSecond: float64(totalBytesRead*8) / 1024 / 1024 / elapsed.Seconds(),
			}
			fmt.Printf("%s\n\n", runs[runID].String())
		}

		broker.Stop()

		sebbench.PrintStats(runs)

		return nil
	},
}

func generateReadRequests(readRequests chan<- readRequest, numRequests int, maxOffset uint64, recordsPerRequest int) int {
	totalRecords := 0
	for range numRequests {
		offset := uint64y.RandomN(maxOffset)
		requestRecords := min(int(maxOffset-offset), recordsPerRequest)
		readRequests <- readRequest{
			offset:     offset,
			maxRecords: requestRecords,
		}

		totalRecords += requestRecords
	}
	close(readRequests)

	return totalRecords
}

type readRequest struct {
	offset     uint64
	maxRecords int
}

func httpGetRecords(log logger.Logger, client *seb.RecordClient, workers int, readRequests <-chan readRequest, topicName string, bufPool *syncy.Pool[[]byte]) int64 {
	wg := &sync.WaitGroup{}
	wg.Add(workers)

	var totalBytes atomic.Int64
	for range workers {
		go func() {
			defer wg.Done()

			workerTotalBytes := int64(0)
			for request := range readRequests {
				buf := bufPool.Get()

				records, err := client.GetRecords(topicName, request.offset, seb.GetRecordsInput{
					MaxRecords: request.maxRecords,
					Timeout:    5 * time.Second,
					Buffer:     buf,
				})
				if err != nil {
					log.Fatalf(err.Error())
				}

				if len(records) != request.maxRecords {
					log.Fatalf("expected %d records, got %d (offset %d)", request.maxRecords, len(records), request.offset)
				}

				for _, record := range records {
					workerTotalBytes += int64(len(record))
				}

				bufPool.Put(buf)
			}

			totalBytes.Add(workerTotalBytes)
		}()
	}

	wg.Wait()
	return totalBytes.Load()
}

type BenchmarkReadFlags struct {
	logLevel   int
	numRuns    int
	numWorkers int

	localBroker         bool
	remoteBrokerAddress string
	remoteBrokerAPIKey  string

	numBatches           int
	recordSize           int
	numRecordsPerBatch   int
	numRecordsPerRequest int
	numRequests          int

	topicName string

	batchBlockTime time.Duration
	batchBytesMax  int
}
