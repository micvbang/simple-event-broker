package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/micvbang/go-helpy/sizey"
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

	switch flags.job {
	case "send":
		sendMessages(log, client, flags.topicName, flags.numWorkers, flags.numRequests)
	case "check":
		checkMessages(log, client, flags.topicName, flags.numRequests)
	default:
		log.Errorf("job must be either 'send' or 'check', got '%s'", flags.job)
	}
}

func checkMessages(log logger.Logger, client *seb.RecordClient, topicName string, numRequests int) {
	recordIDs := map[int]struct{}{}

	var lastPrint uint64
	var offset uint64
	for offset < uint64(numRequests) {
		records, err := client.GetBatch(topicName, offset, seb.GetBatchInput{
			SoftMaxBytes: 10 * sizey.MB,
		})
		if err != nil {
			log.Fatalf("failed to get batch: %s", err)
		}

		offset += uint64(len(records))
		if offset-lastPrint > 10_000 {
			lastPrint = offset
			log.Infof("Offset %d/%d (%.1f%%)", offset, numRequests, float64(offset)/float64(numRequests)*100)
		}

		for i, record := range records {
			request := Message{}
			err := json.Unmarshal(record, &request)
			if err != nil {
				log.Fatalf("failed to parse record %d", offset+uint64(i))
			}

			recordIDs[request.ID] = struct{}{}
			if !slicey.Equal(request.Data, sha256Int(request.ID)) {
				log.Fatalf("unexpected data for request id %d", request.Data)
			}
		}
	}

	if len(recordIDs) != numRequests {
		log.Errorf("expected %d records, got %d", numRequests, len(recordIDs))
	}

	for i := 0; i < numRequests; i++ {
		_, exists := recordIDs[i]
		if !exists {
			log.Errorf("expected record with id %d", i)
		}
	}
}

func sendMessages(log logger.Logger, client *seb.RecordClient, topicName string, numWorkers int, numRequests int) {
	records := make(chan []byte, 2*numWorkers)

	wg := &sync.WaitGroup{}
	var numCompleted atomic.Int32

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()

			for record := range records {
				// time.Sleep(time.Duration(inty.RandomN(1)) * time.Second)

				err := client.Add(topicName, record)
				if err != nil {
					log.Errorf("err: %s", err)
				}
			}
			numCompleted.Add(1)
		}()
	}

	recordsSent := 0
	for i := 0; i < numRequests; i++ {
		bs, err := json.Marshal(Message{
			ID:   i,
			Data: sha256Int(i),
		})

		if err != nil {
			log.Fatalf("failed to JSON marshal: %w", err)
		}

		records <- bs

		recordsSent += 1
	}
	close(records)

	wg.Wait()
	log.Infof("records sent: %d", recordsSent)
	log.Infof("completed workers: %d", numCompleted.Load())
}

func sha256Int(i int) []byte {
	bs := sha256.Sum256([]byte(strconv.Itoa(i)))
	return bs[:]
}

type Message struct {
	ID   int    `json:"id"`
	Data []byte `json:"data"`
}

type flags struct {
	job         string
	numWorkers  int
	numRequests int
	topicName   string

	sebHost   string
	sebAPIKey string
}

func parseFlags() flags {
	fs := flag.NewFlagSet("scl-dump", flag.ExitOnError)

	f := flags{}

	fs.StringVar(&f.job, "job", "send", "Job to perform, 'send' or 'check'")
	fs.IntVar(&f.numWorkers, "workers", 2000, "Number of workers")
	fs.StringVar(&f.topicName, "topic-name", "local-test-delete-me", "Name of topic to use for test")
	fs.IntVar(&f.numRequests, "requests", 1_000_000, "Number of requests to send")

	fs.StringVar(&f.sebHost, "seb-host", "http://localhost:8080", "Name of topic to use for test")
	fs.StringVar(&f.sebAPIKey, "seb-api-key", "api-key", "Name of topic to use for test")

	err := fs.Parse(os.Args[1:])
	if err != nil {
		fs.Usage()
		os.Exit(1)
	}

	return f
}
