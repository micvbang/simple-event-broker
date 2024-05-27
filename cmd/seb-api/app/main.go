package app

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/micvbang/go-helpy/sizey"
	"github.com/micvbang/simple-event-broker/internal/cache"
	"github.com/micvbang/simple-event-broker/internal/httphandlers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebbroker"
)

func Run() {
	ctx := context.Background()

	flags := parseFlags()

	log := logger.NewWithLevel(ctx, logger.LogLevel(flags.logLevel))
	log.Debugf("flags: %v", flags)

	c, err := cache.NewDiskCache(log, flags.cacheDir)
	if err != nil {
		log.Fatalf("creating disk cache: %w", err)
	}

	go cache.EvictionLoop(ctx, log.Name("cache eviction"), c, flags.cacheMaxBytes, flags.cacheEvictionInterval)

	blockingS3Storage, err := makeBlockingS3Broker(log, c, flags.recordBatchSoftMaxBytes, flags.recordBatchBlockTime, flags.s3BucketName)
	if err != nil {
		log.Fatalf("making blocking s3 storage: %s", err)
	}

	mux := http.NewServeMux()
	httphandlers.RegisterRoutes(log, mux, blockingS3Storage, flags.httpAPIKey)

	errs := make(chan error, 8)

	go func() {
		addr := fmt.Sprintf("%s:%d", flags.httpListenAddress, flags.httpListenPort)
		log.Infof("Listening on %s", addr)
		errs <- http.ListenAndServe(addr, mux)
	}()

	if flags.httpEnableDebug {
		go func() {
			logPprof := log.Name("pprof")
			errs <- httphelpers.ListenAndServePprof(logPprof, flags.httpDebugListenAddress, flags.httpDebugListenPort)
		}()
	}

	err = <-errs
	log.Errorf("main returned: %s", err)
}

func makeBlockingS3Broker(log logger.Logger, cache *cache.Cache, bytesSoftMax int, blockTime time.Duration, s3BucketName string) (*sebbroker.Broker, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("creating s3 session: %s", err)
	}

	s3TopicFactory := sebbroker.NewS3TopicFactory(cfg, s3BucketName, cache)
	blockingBatcherFactory := sebbroker.NewBlockingBatcherFactory(blockTime, bytesSoftMax)

	broker := sebbroker.New(
		log.Name("storage"),
		s3TopicFactory,
		sebbroker.WithBatcherFactory(blockingBatcherFactory),
	)
	return broker, nil
}

type flags struct {
	logLevel int

	s3BucketName string

	httpListenAddress string
	httpListenPort    int
	httpAPIKey        string

	httpEnableDebug        bool
	httpDebugListenAddress string
	httpDebugListenPort    int

	cacheDir              string
	cacheMaxBytes         int64
	cacheEvictionInterval time.Duration

	recordBatchBlockTime    time.Duration
	recordBatchSoftMaxBytes int
}

func parseFlags() flags {
	fs := flag.NewFlagSet("seb-api", flag.ExitOnError)

	f := flags{}

	fs.IntVar(&f.logLevel, "log-level", int(logger.LevelInfo), "Log level, info=4, debug=5")

	// http
	fs.StringVar(&f.httpListenAddress, "http-address", "127.0.0.1", "Address to listen for HTTP traffic")
	fs.IntVar(&f.httpListenPort, "http-port", 51313, "Port to listen for HTTP traffic")
	fs.StringVar(&f.httpAPIKey, "http-api-key", "api-key", "API key for authorizing HTTP requests (this is not safe and needs to be changed)")

	// http debug
	fs.BoolVar(&f.httpEnableDebug, "http-debug-enable", false, "Whether to enable DEBUG endpoints")
	fs.StringVar(&f.httpDebugListenAddress, "http-debug-address", "127.0.0.1", "Address to expose DEBUG endpoints. You very likely want this to remain localhost!")
	fs.IntVar(&f.httpDebugListenPort, "http-debug-port", 5000, "Port to serve DEBUG endpoints on")

	// s3
	fs.StringVar(&f.s3BucketName, "s3-bucket", "", "Bucket name")

	// caching
	fs.StringVar(&f.cacheDir, "cache-dir", path.Join(os.TempDir(), "seb-cache"), "Local dir to use when caching record batches")
	fs.Int64Var(&f.cacheMaxBytes, "cache-size", 1*sizey.GB, "Maximum number of bytes to keep in the cache (soft limit)")
	fs.DurationVar(&f.cacheEvictionInterval, "cache-eviction-interval", 5*time.Minute, "Amount of time between enforcing maximum cache size")

	// batching
	fs.DurationVar(&f.recordBatchBlockTime, "batch-wait-time", time.Second, "Amount of time to wait between receiving first record in batch and committing it")
	fs.IntVar(&f.recordBatchSoftMaxBytes, "batch-bytes-max", 10*sizey.MB, "Soft maximum for the number of bytes to include in each record batch")

	err := fs.Parse(os.Args[1:])
	if err != nil {
		fmt.Println(err.Error())
		fs.Usage()
		os.Exit(1)
	}

	required := []struct {
		value string
		name  string
	}{
		{name: "bucket name", value: f.s3BucketName},
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
