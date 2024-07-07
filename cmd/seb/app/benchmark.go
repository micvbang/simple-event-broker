package app

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"math/rand"

	"github.com/micvbang/go-helpy/sizey"
	"github.com/micvbang/go-helpy/slicey"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/httphandlers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebbroker"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/spf13/cobra"
)

var benchmarkFlags BenchmarkFlags

func init() {
	fs := benchmarkCmd.Flags()

	fs.IntVarP(&benchmarkFlags.numRuns, "runs", "r", 1, "Number of times to run the benchmark")
	fs.IntVarP(&benchmarkFlags.numWorkers, "workers", "w", runtime.NumCPU()*2, "Number of workers to add data")
	fs.BoolVar(&benchmarkFlags.localBroker, "local-broker", true, "Whether to benchmark against a dedicated broker being started for this benchmark")

	fs.IntVar(&benchmarkFlags.numBatches, "batches", 4096, "Number of records to generate")
	fs.IntVar(&benchmarkFlags.recordSize, "record-size", 1024, "Size of records in bytes")
	fs.IntVarP(&benchmarkFlags.numRecordsPerBatch, "records-per-batch", "b", 1024, "Records per batch")

	fs.IntVar(&benchmarkFlags.batchBytesMax, "batcher-max-bytes", 10*sizey.MB, "Maximum number of bytes to wait before committing incoming batch to storage")
	fs.DurationVar(&benchmarkFlags.batchBlockTime, "batcher-block-time", 5*time.Millisecond, "Maximum amount of time to wait before committing incoming batches to storage")
}

type benchmarkStats struct {
	elapsed          time.Duration
	recordsPerSecond float64
	batchesPerSecond float64
	mbitPerSecond    float64
}

func (bs benchmarkStats) String() string {
	return fmt.Sprintf(`took %v
records/second: %.2f
batches/second: %.2f
mbit/second: %.2f`, bs.elapsed, bs.recordsPerSecond, bs.batchesPerSecond, bs.mbitPerSecond)
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

		var broker broker = &remoteBroker{}
		if flags.localBroker {
			broker = newLocalBroker(log, flags.batchBlockTime, flags.batchBytesMax)
		}

		runs := make([]benchmarkStats, flags.numRuns)
		for runID := range flags.numRuns {
			fmt.Printf("Run %d/%d\n", runID+1, flags.numRuns)

			const topicName = "some-topic"

			batches := make(chan recordBatch, 8)
			go generateBatches(log, batches, flags.numBatches, flags.numRecordsPerBatch, flags.recordSize)

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

			runs[runID] = benchmarkStats{
				elapsed:          elapsed,
				recordsPerSecond: float64(numRecords) / elapsed.Seconds(),
				batchesPerSecond: float64(flags.numBatches) / elapsed.Seconds(),
				mbitPerSecond:    float64((totalBytes*8)/1024/1024) / elapsed.Seconds(),
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

		fmt.Println("Stats:")
		runtimes := slicey.Map(runs, func(v benchmarkStats) float64 {
			return v.elapsed.Seconds()
		})
		runtimeAvg, runtimeMin, runtimeMax := computeStats(runtimes)
		fmt.Printf("Seconds/run avg/min/max:\t%.2f / %.2f / %.2f\n", runtimeAvg, runtimeMin, runtimeMax)

		rps := slicey.Map(runs, func(v benchmarkStats) float64 {
			return v.recordsPerSecond
		})
		rpsAvg, rpsMin, rpsMax := computeStats(rps)
		fmt.Printf("Records/second avg/min/max:\t%.2f / %.2f / %.2f\n", rpsAvg, rpsMin, rpsMax)

		bps := slicey.Map(runs, func(v benchmarkStats) float64 {
			return v.batchesPerSecond
		})
		bpsAvg, bpsMin, bpsMax := computeStats(bps)
		fmt.Printf("Batches/second avg/min/max:\t%.2f / %.2f / %.2f\n", bpsAvg, bpsMin, bpsMax)

		mbitps := slicey.Map(runs, func(v benchmarkStats) float64 {
			return v.mbitPerSecond
		})
		mbitpsAvg, mbitpsMin, mbitpsMax := computeStats(mbitps)
		fmt.Printf("Mbit/second avg/min/max:\t%.2f / %.2f / %.2f\n", mbitpsAvg, mbitpsMin, mbitpsMax)

		return nil
	},
}

type broker interface {
	Start() (*seb.RecordClient, error)
	Stop() error
}

type remoteBroker struct{}

func (rb remoteBroker) Start() (*seb.RecordClient, error) {
	// TODO: take these as arguments
	return seb.NewRecordClient("http://localhost:51313", "api-key")
}

func (rb remoteBroker) Stop() error {
	return nil
}

type localBroker struct {
	log        logger.Logger
	tmpDir     string
	httpServer *httptest.Server

	batchBlockTime time.Duration
	batchBytesMax  int
}

func newLocalBroker(log logger.Logger, batchBlockTime time.Duration, batchBytesMax int) *localBroker {
	return &localBroker{
		log:            log,
		batchBlockTime: batchBlockTime,
		batchBytesMax:  batchBytesMax,
	}
}

func (lb *localBroker) Start() (*seb.RecordClient, error) {
	var err error
	lb.tmpDir, err = os.MkdirTemp("", "seb_bench*")
	if err != nil {
		return nil, fmt.Errorf("creating temp dir: %w", err)
	}

	broker, err := makeDiskBroker(lb.log, lb.batchBlockTime, lb.batchBytesMax, lb.tmpDir)
	if err != nil {
		return nil, fmt.Errorf("initializing broker: %w", err)
	}

	mux := http.NewServeMux()
	const apiKey = "api-key"
	httphandlers.RegisterRoutes(lb.log, mux, broker, apiKey)

	lb.httpServer = httptest.NewServer(mux)
	client, err := seb.NewRecordClient(lb.httpServer.URL, apiKey)
	if err != nil {
		return nil, fmt.Errorf("creating record client: %s", err)
	}

	return client, nil
}

func (lb *localBroker) Stop() error {
	lb.httpServer.Close()

	err := os.RemoveAll(lb.tmpDir)
	if err != nil {
		return fmt.Errorf("removing temp dir '%s': %w", lb.tmpDir, err)
	}

	return nil
}

func computeStats(vs []float64) (avg, min_, max_ float64) {
	if len(vs) == 0 {
		return 0, 0, 0
	}

	avg = float64(slicey.Sum(vs)) / float64(len(vs))
	min_ = vs[0]
	max_ = vs[0]
	for _, v := range vs {
		min_ = min(min_, v)
		max_ = max(max_, v)
	}

	return avg, min_, max_
}

func makeDiskBroker(log logger.Logger, blockTime time.Duration, batchBytesMax int, rootDir string) (*sebbroker.Broker, error) {
	cache, err := sebcache.NewDiskCache(log, filepath.Join(rootDir, "cache"))
	if err != nil {
		return nil, fmt.Errorf("failed to init cache: %w", err)
	}
	storage := sebtopic.NewDiskStorage(log, filepath.Join(rootDir, "storage"))
	s3TopicFactory := sebbroker.NewTopicFactory(storage, cache)

	batcherFactory := sebbroker.NewBlockingBatcherFactory(blockTime, batchBytesMax)
	broker := sebbroker.New(log, s3TopicFactory, sebbroker.WithBatcherFactory(batcherFactory))

	return broker, nil
}

type recordBatch struct {
	recordSizes []uint32
	records     []byte
}

func generateBatches(log logger.Logger, batches chan<- recordBatch, numBatches int, recordsPerBatch int, recordSize int) {
	randSource := rand.New(rand.NewSource(1))
	batchSize := recordsPerBatch * recordSize

	for range numBatches {
		records := make([]byte, batchSize)
		n, err := randSource.Read(records)
		if err != nil {
			log.Fatalf("failed to generate random data: %s", err)
		}

		if n != len(records) {
			log.Fatalf("expected to generate %d bytes, got %d", len(records), n)
		}

		recordSizes := make([]uint32, recordsPerBatch)
		for i := range recordsPerBatch {
			recordSizes[i] = uint32(recordSize)
		}

		batches <- recordBatch{
			recordSizes: recordSizes,
			records:     records,
		}
	}
	close(batches)
}

func httpAddRecords(log logger.Logger, wg *sync.WaitGroup, client *seb.RecordClient, batches <-chan recordBatch, workers int, topicName string) {
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()

			for batch := range batches {
				records := batchToRecords(batch.recordSizes, batch.records)
				err := client.AddRecords(topicName, records)
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
	numRuns     int
	numWorkers  int
	localBroker bool

	numBatches         int
	recordSize         int
	numRecordsPerBatch int

	batchBlockTime time.Duration
	batchBytesMax  int
}
