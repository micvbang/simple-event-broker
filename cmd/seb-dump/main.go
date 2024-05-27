package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
)

func main() {
	flags := parseFlags()

	ctx := context.Background()
	log := logger.NewWithLevel(ctx, logger.LevelInfo)

	absInputPath, err := filepath.Abs(flags.inputPath)
	if err != nil {
		log.Fatalf("failed to get the absolute path: %s", err)
	}

	rootDir := filepath.Dir(absInputPath)
	topicName := filepath.Base(absInputPath)
	fmt.Printf("Dumping records [%d; %d] from topic '%s'\n", flags.startFromOffset, flags.startFromOffset+flags.numRecords-1, topicName)

	cache, err := sebcache.NewMemoryCache(log)
	if err != nil {
		log.Fatalf("creating disk cache: %w", err)
	}

	diskTopicStorage := sebtopic.NewDiskStorage(log, rootDir)

	topic, err := sebtopic.New(log, diskTopicStorage, topicName, cache)
	if err != nil {
		log.Fatalf("failed to initialized disk storage: %s", err)
	}

	for i := flags.startFromOffset; i < flags.startFromOffset+flags.numRecords; i++ {
		record, err := topic.ReadRecord(uint64(i))
		if err != nil {
			if errors.Is(err, seb.ErrOutOfBounds) {
				fmt.Printf("out of bounds\n")
				return
			}

			fmt.Printf("ERROR: reading record %d: %s\n", i, err)
		}

		fmt.Printf("record %d: %s\n", i, record)
	}
}

type flags struct {
	inputPath       string
	startFromOffset int
	numRecords      int
}

func parseFlags() flags {
	fs := flag.NewFlagSet("seb-dump", flag.ExitOnError)

	f := flags{}

	fs.StringVar(&f.inputPath, "path", "", "Path of seb topic you wish to dump contents of")
	fs.IntVar(&f.startFromOffset, "start-from", 0, "Offset to start dumping from")
	fs.IntVar(&f.numRecords, "num", 10, "Number of records to dump")

	err := fs.Parse(os.Args[1:])
	if err != nil {
		fs.Usage()
		os.Exit(1)
	}

	return f
}
