package app

import (
	"context"
	"crypto/subtle"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/micvbang/go-helpy/sizey"
	"github.com/micvbang/simple-event-broker/internal/httphandlers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/micvbang/simple-event-broker/internal/sebhttp"
	"github.com/micvbang/simple-event-broker/internal/storage"
)

func Run() {
	ctx := context.Background()

	flags := parseFlags()

	log := logger.NewWithLevel(ctx, logger.LogLevel(flags.logLevel))
	log.Debugf("flags: %v", flags)

	diskCache, err := storage.NewDiskCacheDefault(log.Name("disk cache"), flags.cacheDir)
	if err != nil {
		log.Fatalf("creating disk cache: %w", err)
	}

	go cacheEviction(log.Name("cache eviction"), diskCache, flags.cacheMaxBytes, flags.cacheEvictionInterval)

	blockingS3Storage, err := makeBlockingS3Storage(log, diskCache, flags.batchBlockTime, flags.bucketName)
	if err != nil {
		log.Fatalf("making blocking s3 storage: %s", err)
	}

	mux := http.NewServeMux()
	registerRoutes(log, mux, blockingS3Storage, flags.httpAPIKey)

	addr := fmt.Sprintf("%s:%d", flags.httpListenAddress, flags.httpListenPort)
	log.Infof("Listening on %s", addr)
	err = http.ListenAndServe(addr, mux)
	log.Fatalf("ListenAndServe returned: %s", err)
}

func cacheEviction(log logger.Logger, cache *storage.DiskCache, cacheMaxBytes int64, interval time.Duration) {
	log = log.
		WithField("max bytes", cacheMaxBytes).
		WithField("interval", interval)

	for {
		cacheSize := cache.Size()

		if cacheSize > cacheMaxBytes {
			fillLevel := float32(cacheSize) / float32(cacheMaxBytes) * 100

			log.Infof("cache full (%.2f%%, %d/%d bytes), evicting items", fillLevel, cacheSize, cacheMaxBytes)
			err := cache.EvictLeastRecentlyUsed(cacheMaxBytes)
			if err != nil {
				log.Errorf("failed to evict cache: %s", err)
			}
		}

		log.Debugf("sleeping")
		time.Sleep(interval)
	}
}

func registerRoutes(log logger.Logger, mux *http.ServeMux, storage *storage.Storage, apiKey string) {
	// TODO: we don't want something more secure and easier to manage than a
	// single, static API key.
	apiKeyBs := []byte(apiKey)

	requireAPIKey := sebhttp.NewAPIKeyHandler(log.Name("api key handler"), func(ctx context.Context, apiKey string) (bool, error) {
		apiKeyIsValid := subtle.ConstantTimeCompare(apiKeyBs, []byte(apiKey)) == 1
		return apiKeyIsValid, nil
	})

	mux.HandleFunc("POST /record", requireAPIKey(httphandlers.AddRecord(log, storage)))
	mux.HandleFunc("GET /record", requireAPIKey(httphandlers.GetRecord(log, storage)))
}

func makeBlockingS3Storage(log logger.Logger, cache *storage.DiskCache, sleepTime time.Duration, s3BucketName string) (*storage.Storage, error) {
	contextCreator := func() context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), sleepTime)
		go func() {
			// We have to cancel the context. Just ensure that it's cancelled at
			// some point in the future.
			time.Sleep(sleepTime * 2)
			cancel()
		}()

		return ctx
	}

	session, err := session.NewSession()
	if err != nil {
		return nil, fmt.Errorf("creating s3 session: %s", err)
	}

	s3TopicStorage := func(log logger.Logger, topicName string) (*storage.TopicStorage, error) {
		return storage.NewS3TopicStorage(log.Name("s3 storage"), storage.S3StorageInput{
			S3:         s3.New(session),
			BucketName: s3BucketName,
			RootDir:    "/tmp/recordbatch",
			TopicName:  topicName,
			Cache:      cache,
		})
	}

	blockingBatcher := func(log logger.Logger, ts *storage.TopicStorage) storage.RecordBatcher {
		return recordbatch.NewBlockingBatcher(log.Name("blocking batcher"), contextCreator, func(b recordbatch.RecordBatch) error {
			t0 := time.Now()
			err := ts.AddRecordBatch(b)
			log.Debugf("persisting to s3: %v", time.Since(t0))
			return err
		})
	}

	return storage.New(log.Name("storage"), s3TopicStorage, blockingBatcher), nil
}

type flags struct {
	bucketName     string
	batchBlockTime time.Duration
	logLevel       int

	httpListenAddress string
	httpListenPort    int
	httpAPIKey        string

	cacheDir              string
	cacheMaxBytes         int64
	cacheEvictionInterval time.Duration
}

func parseFlags() flags {
	fs := flag.NewFlagSet("seb-api", flag.ExitOnError)

	f := flags{}

	fs.StringVar(&f.bucketName, "b", "simple-commit-log-delete-me", "Bucket name")
	fs.DurationVar(&f.batchBlockTime, "s", time.Second, "Amount of time to wait between receiving first message in batch and committing it")
	fs.IntVar(&f.logLevel, "log-level", int(logger.LevelInfo), "Log level, info=4, debug=5")

	fs.StringVar(&f.httpListenAddress, "l", "127.0.0.1", "Address to listen for HTTP traffic")
	fs.IntVar(&f.httpListenPort, "p", 8080, "Port to listen for HTTP traffic")
	fs.StringVar(&f.httpAPIKey, "api-key", "api-key", "API key for authorizing HTTP requests (this is not safe and needs to be changed)")

	fs.StringVar(&f.cacheDir, "c", path.Join(os.TempDir(), "seb-cache"), "Local dir to use when caching record batches")
	fs.Int64Var(&f.cacheMaxBytes, "cache-size", 1*sizey.GB, "Maximum number of bytes to keep in the cache (soft limit)")
	fs.DurationVar(&f.cacheEvictionInterval, "cache-eviction-interval", 5*time.Minute, "Amount of time between enforcing maximum cache size")

	err := fs.Parse(os.Args[1:])
	if err != nil {
		fs.Usage()
		os.Exit(1)
	}

	required := []struct {
		value string
		name  string
	}{
		{name: "bucket name", value: f.bucketName},
	}

	for _, r := range required {
		if len(r.value) == 0 {
			fmt.Printf("ERROR: %s required\n", r.name)
			fs.Usage()
			os.Exit(1)
		}
	}

	return f
}
