package app

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/micvbang/go-helpy"
	"github.com/micvbang/go-helpy/sizey"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/spf13/cobra"
)

var clientGetFlags RequestFlags

func init() {
	fs := clientGetCmd.Flags()

	fs.IntVar(&clientGetFlags.logLevel, "log-level", int(logger.LevelInfo), "Log level, info=4, debug=5")

	// broker
	fs.StringVar(&clientGetFlags.brokerAddress, "remote-broker-address", "http://localhost:51313", "Address of remote broker to connect to instead of starting local broker")
	fs.StringVar(&clientGetFlags.brokerAPIKey, "remote-broker-api-key", "api-key", "API key to use for remote broker")

	// request
	fs.StringVarP(&clientGetFlags.topicName, "topic-name", "t", "", "Name of topic to request data from")
	fs.Uint64VarP(&clientGetFlags.offset, "offset", "o", 0, "Offset to request data from")
	fs.IntVar(&clientGetFlags.maxRecords, "max-records", 32, "Maximum number of records to request")
	fs.IntVar(&clientGetFlags.softMaxBytes, "max-bytes", 5*sizey.MB, "Maximum bytes to request")
	fs.DurationVar(&clientGetFlags.timeout, "timeout", time.Second, "Maximum duration to wait for response")

	// visuals
	fs.IntVarP(&clientGetFlags.dumpRecordBytes, "dump-record-bytes", "b", 64, "Number of bytes to dump for each record, 0 for all of them")

	clientGetCmd.MarkFlagRequired("topic-name")
}

var clientGetCmd = &cobra.Command{
	Use:   "get",
	Short: "Request records using HTTP client",
	Long:  "Request records from Seb instance using HTTP client",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		flags := clientGetFlags
		log := logger.NewWithLevel(ctx, logger.LogLevel(flags.logLevel))
		client, err := seb.NewRecordClient(flags.brokerAddress, flags.brokerAPIKey)
		if err != nil {
			log.Fatalf("creating client: %s", err)
		}

		records, err := client.GetRecords(flags.topicName, flags.offset, seb.GetRecordsInput{
			MaxRecords: flags.maxRecords,
			Buffer:     make([]byte, 0, flags.softMaxBytes),
			Timeout:    flags.timeout,
		})
		if err != nil {
			log.Fatalf("requesting records: %s", err)
		}

		fmt.Printf("Records:\n")
		for i, record := range records {
			dumpBytes := helpy.Clamp(clientGetFlags.dumpRecordBytes, 1, len(record))
			if clientGetFlags.dumpRecordBytes == 0 {
				dumpBytes = len(record)
			}

			var tail string
			if dumpBytes != len(record) {
				tail = fmt.Sprintf("\t[+%d bytes]", len(record)-dumpBytes)
			}
			fmt.Printf("%d: %s%s\n", flags.offset+uint64(i), string(record[:dumpBytes]), tail)
			m := map[string]any{}
			err := json.Unmarshal(record, &m)
			if err != nil {
				log.Warnf("parsing as json: %s", err)
			}
		}

		return nil
	},
}

type RequestFlags struct {
	logLevel      int
	brokerAddress string
	brokerAPIKey  string

	topicName    string
	offset       uint64
	maxRecords   int
	softMaxBytes int
	timeout      time.Duration

	dumpRecordBytes int
}
