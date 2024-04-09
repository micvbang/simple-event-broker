package api

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/micvbang/simple-event-broker/internal/httphandlers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/micvbang/simple-event-broker/internal/storage"
)

func Run() {
	ctx := context.Background()
	log := logger.NewWithLevel(ctx, logger.LevelDebug)

	flags := parseFlags()

	log.Debugf("flags: %v", flags)

	diskCache, err := storage.NewDiskCache(log.Name("disk cache"), flags.cacheDir)
	if err != nil {
		log.Fatalf("creating disk cache: %w", err)
	}

	blockingS3Storage, err := makeBlockingS3Storage(log, diskCache, flags.sleepTime, flags.bucketName)
	if err != nil {
		log.Fatalf("making blocking s3 storage: %s", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /add", httphandlers.AddRecord(log, blockingS3Storage))
	mux.HandleFunc("GET /get", httphandlers.GetRecord(log, blockingS3Storage))

	addr := fmt.Sprintf("%s:%d", flags.httpListenAddress, flags.httpListenPort)
	log.Infof("Listening on %s", addr)
	err = http.ListenAndServe(addr, mux)
	log.Fatalf("ListenAndServe returned: %s", err)
}

func makeBlockingS3Storage(log logger.Logger, cache *storage.DiskCache, sleepTime time.Duration, s3BucketName string) (storage.Storage, error) {
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
	bucketName        string
	sleepTime         time.Duration
	cacheDir          string
	httpListenAddress string
	httpListenPort    int
}

func parseFlags() flags {
	fs := flag.NewFlagSet("seb-api", flag.ExitOnError)

	f := flags{}

	fs.StringVar(&f.bucketName, "b", "simple-commit-log-delete-me", "Bucket name")
	fs.DurationVar(&f.sleepTime, "s", time.Second, "Amount of time to wait between receiving first message in batch and committing it")
	fs.StringVar(&f.cacheDir, "c", path.Join(os.TempDir(), "seb-cache"), "Local dir to use when caching record batches")
	fs.StringVar(&f.httpListenAddress, "l", "127.0.0.1", "Address to listen for HTTP traffic")
	fs.IntVar(&f.httpListenPort, "p", 8080, "Port to listen for HTTP traffic")

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
