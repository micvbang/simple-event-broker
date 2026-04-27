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

var offsetsWriteFlags OffsetsWriteFlags

func init() {
	fs := offsetsWriteCmd.Flags()

	fs.IntVar(&offsetsWriteFlags.logLevel, "log-level", int(logger.LevelInfo), "Log level, info=4, debug=5")

	// s3
	fs.StringVar(&offsetsWriteFlags.s3BucketName, "s3-bucket", "", "S3 bucket name")
	fs.StringVar(&offsetsWriteFlags.s3KeyPrefix, "s3-key-prefix", "", "S3 key prefix to prepend to topic keys")

	// topic
	fs.StringVarP(&offsetsWriteFlags.topicName, "topic-name", "t", "", "Name of the topic to write offsets for")

	offsetsWriteCmd.MarkFlagRequired("s3-bucket")
	offsetsWriteCmd.MarkFlagRequired("topic-name")
}

var offsetsWriteCmd = &cobra.Command{
	Use:   "write",
	Short: "Write record batch offsets to S3",
	Long:  "List existing record batch offsets for a topic and write them to S3 as an .offsets file",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		flags := offsetsWriteFlags
		log := logger.NewWithLevel(ctx, logger.LogLevel(flags.logLevel))

		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return fmt.Errorf("loading aws config: %w", err)
		}

		s3Client := s3.NewFromConfig(cfg)
		storage := sebtopic.NewS3Storage(log.Name("s3 storage"), s3Client, flags.s3BucketName, flags.s3KeyPrefix)

		log.Infof("listing record batch offsets for topic %q", flags.topicName)
		offsets, err := sebtopic.ListRecordBatchOffsets(log, storage, flags.topicName)
		if err != nil {
			return fmt.Errorf("listing record batch offsets: %w", err)
		}
		log.Infof("found %d offsets", len(offsets))

		err = sebtopic.WriteRecordBatchOffsets(log, storage, flags.topicName, offsets)
		if err != nil {
			return fmt.Errorf("writing record batch offsets: %w", err)
		}
		log.Infof("wrote %d offsets for topic %q", len(offsets), flags.topicName)

		return nil
	},
}

type OffsetsWriteFlags struct {
	logLevel int

	s3BucketName string
	s3KeyPrefix  string

	topicName string
}
