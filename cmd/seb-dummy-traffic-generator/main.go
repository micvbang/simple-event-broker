package main

import (
	"context"
	"crypto/rand"
	"flag"
	"os"
	"time"

	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/go-helpy/slicey"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

func main() {
	flags := parseFlags()

	ctx := context.Background()
	log := logger.NewWithLevel(ctx, logger.LevelInfo)

	client, err := seb.NewRecordClient("http://localhost:8080", "api-key")
	if err != nil {
		log.Fatalf("creating RecordClient: %s", err)
	}

	records := make(chan []byte, 2*flags.numWorkers)
	topicNames := []string{"local-test-delete-me"}

	for i := 0; i < flags.numWorkers; i++ {
		go func() {
			for record := range records {
				time.Sleep(time.Duration(inty.RandomN(1)) * time.Second)

				err := client.Add(slicey.Random(topicNames), record)
				if err != nil {
					log.Errorf("err: %s", err)
				}
			}
		}()
	}

	bs := make([]byte, 4096)
	_, err = rand.Read(bs)
	if err != nil {
		log.Fatalf("failed generating payload: %s", err)
	}

	for i := 0; i < flags.numRequests; i++ {
		records <- bs
	}
}

type flags struct {
	numWorkers  int
	numRequests int
}

func parseFlags() flags {
	fs := flag.NewFlagSet("scl-dump", flag.ExitOnError)

	f := flags{}

	fs.IntVar(&f.numWorkers, "w", 100, "Number of workers")
	fs.IntVar(&f.numRequests, "n", 50_000_000, "Number of requests to send")

	err := fs.Parse(os.Args[1:])
	if err != nil {
		fs.Usage()
		os.Exit(1)
	}

	return f
}
