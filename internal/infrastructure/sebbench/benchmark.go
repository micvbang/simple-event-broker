package sebbench

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"github.com/micvbang/go-helpy/slicey"
	"github.com/micvbang/go-helpy/syncy"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/httphandlers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebbroker"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
)

type Broker interface {
	Start() (*seb.RecordClient, error)
	Stop() error
}

type RemoteBroker struct {
	RemoteBrokerAddress string
	RemoteBrokerAPIKey  string

	client *seb.RecordClient
}

func (rb *RemoteBroker) Start() (*seb.RecordClient, error) {
	var err error
	rb.client, err = seb.NewRecordClient(rb.RemoteBrokerAddress, rb.RemoteBrokerAPIKey)
	return rb.client, err
}

func (rb *RemoteBroker) Stop() error {
	rb.client.CloseIdleConnections()
	return nil
}

type LocalBroker struct {
	log        logger.Logger
	tmpDir     string
	httpServer *httptest.Server

	batchBlockTime time.Duration
	batchBytesMax  int
	batchPool      *syncy.Pool[*sebrecords.Batch]
}

func NewLocalBroker(log logger.Logger, batchPool *syncy.Pool[*sebrecords.Batch], batchBlockTime time.Duration, batchBytesMax int) *LocalBroker {
	return &LocalBroker{
		log:            log,
		batchBlockTime: batchBlockTime,
		batchBytesMax:  batchBytesMax,
		batchPool:      batchPool,
	}
}

func (lb *LocalBroker) Start() (*seb.RecordClient, error) {
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
	httphandlers.RegisterRoutes(lb.log, mux, lb.batchPool, broker, apiKey)

	lb.httpServer = httptest.NewServer(mux)
	client, err := seb.NewRecordClient(lb.httpServer.URL, apiKey)
	if err != nil {
		return nil, fmt.Errorf("creating record client: %s", err)
	}

	return client, nil
}

func (lb *LocalBroker) Stop() error {
	lb.httpServer.Close()

	err := os.RemoveAll(lb.tmpDir)
	if err != nil {
		return fmt.Errorf("removing temp dir '%s': %w", lb.tmpDir, err)
	}

	return nil
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

// GenerateBatches generates a single random batch of size
// recordsPerBatch*recordSize, and sends it numBatches times on the batches
// channel.
func GenerateBatches(log logger.Logger, batches chan<- sebrecords.Batch, numBatches int, recordsPerBatch int, recordSize int) {
	randSource := rand.New(rand.NewSource(1))
	batchSize := recordsPerBatch * recordSize

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

	for range numBatches {
		batches <- sebrecords.NewBatch(recordSizes, records)
	}
	close(batches)
}

type Stats struct {
	Elapsed          time.Duration
	RecordsPerSecond float64
	BatchesPerSecond float64
	MbitPerSecond    float64
}

func (bs Stats) String() string {
	return fmt.Sprintf(`took %v
records/second: %.2f
batches/second: %.2f
mbit/second: %.2f`, bs.Elapsed, bs.RecordsPerSecond, bs.BatchesPerSecond, bs.MbitPerSecond)
}

func PrintStats(runs []Stats) {
	fmt.Println("Stats:")
	runtimes := slicey.Map(runs, func(v Stats) float64 {
		return v.Elapsed.Seconds()
	})
	runtimeAvg, runtimeMin, runtimeMax := computeStats(runtimes)
	fmt.Printf("Seconds/run avg/min/max:\t%.2f / %.2f / %.2f\n", runtimeAvg, runtimeMin, runtimeMax)

	rps := slicey.Map(runs, func(v Stats) float64 {
		return v.RecordsPerSecond
	})
	rpsAvg, rpsMin, rpsMax := computeStats(rps)
	fmt.Printf("Records/second avg/min/max:\t%.2f / %.2f / %.2f\n", rpsAvg, rpsMin, rpsMax)

	bps := slicey.Map(runs, func(v Stats) float64 {
		return v.BatchesPerSecond
	})
	bpsAvg, bpsMin, bpsMax := computeStats(bps)
	fmt.Printf("Batches/second avg/min/max:\t%.2f / %.2f / %.2f\n", bpsAvg, bpsMin, bpsMax)

	mbitps := slicey.Map(runs, func(v Stats) float64 {
		return v.MbitPerSecond
	})
	mbitpsAvg, mbitpsMin, mbitpsMax := computeStats(mbitps)
	fmt.Printf("Mbit/second avg/min/max:\t%.2f / %.2f / %.2f\n", mbitpsAvg, mbitpsMin, mbitpsMax)
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
