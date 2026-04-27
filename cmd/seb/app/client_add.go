package app

import (
	"context"

	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/spf13/cobra"
)

var clientAddFlags AddFlags

func init() {
	fs := clientAddCmd.Flags()

	fs.IntVar(&clientAddFlags.logLevel, "log-level", int(logger.LevelInfo), "Log level, info=4, debug=5")

	// broker
	fs.StringVar(&clientAddFlags.brokerAddress, "remote-broker-address", "http://localhost:51313", "Address of remote broker to connect to instead of starting local broker")
	fs.StringVar(&clientAddFlags.brokerAPIKey, "remote-broker-api-key", "api-key", "API key to use for remote broker")

	// request
	fs.StringVarP(&clientAddFlags.topicName, "topic-name", "t", "", "Name of topic to request data from")

	clientAddCmd.MarkFlagRequired("topic-name")
}

var clientAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Add records using HTTP client",
	Long:  "Add records to Seb instance using HTTP client",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		flags := clientAddFlags
		log := logger.NewWithLevel(ctx, logger.LogLevel(flags.logLevel))
		client, err := seb.NewRecordClient(flags.brokerAddress, flags.brokerAPIKey)
		if err != nil {
			log.Fatalf("creating client: %s", err)
		}

		err = client.AddRecords(flags.topicName, []uint32{4, 10}, []byte("01230123456789"))
		if err != nil {
			log.Fatalf("adding records: %s", err)
		}

		return nil
	},
}

type AddFlags struct {
	logLevel      int
	brokerAddress string
	brokerAPIKey  string

	topicName string
}
