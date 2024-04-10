package storage

import (
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

func NewS3TopicStorageForTests(log logger.Logger, s3 s3iface.S3API, bucketName string) *S3TopicStorage {
	return &S3TopicStorage{
		log:        log,
		s3:         s3,
		bucketName: bucketName,
	}
}
