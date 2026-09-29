package app

import (
	"context"
	"fmt"

	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/spf13/cobra"
)

var clientTopicFlags TopicFlags

func init() {
	fs := clientTopicCmd.Flags()

	fs.IntVar(&clientTopicFlags.logLevel, "log-level", int(logger.LevelInfo), "Log level, info=4, debug=5")

	// broker
	fs.StringVar(&clientTopicFlags.brokerAddress, "remote-broker-address", "http://localhost:51313", "Address of remote broker to connect to instead of starting local broker")
	fs.StringVar(&clientTopicFlags.brokerAPIKey, "remote-broker-api-key", "api-key", "API key to use for remote broker")

	// request
	fs.StringVarP(&clientTopicFlags.topicName, "topic-name", "t", "", "Name of topic to request metadata for")

	clientTopicCmd.MarkFlagRequired("topic-name")
}

var clientTopicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Request topic metadata using HTTP client",
	Long:  "Request topic metadata from Seb instance using HTTP client",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		flags := clientTopicFlags
		log := logger.NewWithLevel(ctx, logger.LogLevel(flags.logLevel))
		client, err := seb.NewRecordClient(flags.brokerAddress, flags.brokerAPIKey)
		if err != nil {
			log.Fatalf("creating client: %s", err)
		}

		topic, err := client.GetTopic(flags.topicName)
		if err != nil {
			log.Fatalf("requesting topic metadata: %s", err)
		}

		fmt.Printf("Name: %s\n", topic.Name)
		fmt.Printf("NextOffset: %d\n", topic.NextOffset)
		fmt.Printf("LastInsertTime: %s\n", topic.LastInsertTime)

		return nil
	},
}

type TopicFlags struct {
	logLevel      int
	brokerAddress string
	brokerAPIKey  string

	topicName string
}
