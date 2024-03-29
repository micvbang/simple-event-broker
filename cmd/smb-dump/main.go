package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/storage"
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
	fmt.Printf("Dumping records [%d; %d] from topic '%s'\n", flags.startFromRecordID, flags.startFromRecordID+flags.numRecords-1, topicName)

	diskStorage, err := storage.NewDiskStorage(log, rootDir, topicName)
	if err != nil {
		log.Fatalf("failed to initialized disk storage: %s", err)
	}

	for i := flags.startFromRecordID; i < flags.startFromRecordID+flags.numRecords; i++ {
		record, err := diskStorage.ReadRecord(uint64(i))
		if err != nil {
			if errors.Is(err, storage.ErrOutOfBounds) {
				fmt.Printf("out of bounds\n")
				return
			}

			fmt.Printf("ERROR: reading record %d: %s\n", i, err)
		}

		fmt.Printf("record %d: %s\n", i, record)
	}
}

type flags struct {
	inputPath         string
	startFromRecordID int
	numRecords        int
}

func parseFlags() flags {
	fs := flag.NewFlagSet("seb-dump", flag.ExitOnError)

	f := flags{}

	fs.StringVar(&f.inputPath, "path", "", "Path of seb topic you wish to dump contents of")
	fs.IntVar(&f.startFromRecordID, "start-from", 0, "Record ID to start dumping from")
	fs.IntVar(&f.numRecords, "num", 10, "Number of records to dump")

	err := fs.Parse(os.Args[1:])
	if err != nil {
		fs.Usage()
		os.Exit(1)
	}

	return f
}
