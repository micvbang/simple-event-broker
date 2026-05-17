package app

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/spf13/cobra"
)

var offsetsListFlags OffsetsListFlags

func init() {
	fs := offsetsListCmd.Flags()

	fs.IntVar(&offsetsListFlags.logLevel, "log-level", int(logger.LevelInfo), "Log level, info=4, debug=5")

	// s3
	fs.StringVar(&offsetsListFlags.s3BucketName, "s3-bucket", "", "S3 bucket name")
	fs.StringVar(&offsetsListFlags.s3KeyPrefix, "s3-key-prefix", "", "S3 key prefix to prepend to topic keys")

	// topic
	fs.StringVarP(&offsetsListFlags.topicName, "topic-name", "t", "", "Name of the topic to list offsets for")

	offsetsListCmd.MarkFlagRequired("s3-bucket")
	offsetsListCmd.MarkFlagRequired("topic-name")
}

var offsetsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List record batch offsets",
	Long:  "Read the current record batch offsets for a topic and print the total number of offsets and the most recent one",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		flags := offsetsListFlags
		log := logger.NewWithLevel(ctx, logger.LogLevel(flags.logLevel))

		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return fmt.Errorf("loading aws config: %w", err)
		}

		s3Client := s3.NewFromConfig(cfg)
		storage := sebtopic.NewS3Storage(log.Name("s3 storage"), s3Client, flags.s3BucketName, flags.s3KeyPrefix)

		offsets, err := sebtopic.ListRecordBatchOffsets(ctx, log, storage, flags.topicName)
		if err != nil {
			return fmt.Errorf("listing record batch offsets: %w", err)
		}

		fmt.Printf("Total offsets: %d\n", len(offsets))
		if len(offsets) == 0 {
			fmt.Println("Most recent offset: <none>")
		} else {
			fmt.Printf("Most recent offset: %d\n", offsets[len(offsets)-1])
		}

		return nil
	},
}

type OffsetsListFlags struct {
	logLevel int

	s3BucketName string
	s3KeyPrefix  string

	topicName string
}
