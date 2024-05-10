package tester

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Mock struct {
	MockPutObject func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)

	PutObjectCalled bool

	MockGetObject   func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	GetObjectCalled bool

	MockListObjectsV2 func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)

	ListObjectPagesCalled bool
}

func (sm *S3Mock) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	sm.PutObjectCalled = true
	return sm.MockPutObject(ctx, params, optFns...)
}

func (sm *S3Mock) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	sm.GetObjectCalled = true
	return sm.MockGetObject(ctx, params, optFns...)
}

func (sm *S3Mock) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	sm.ListObjectPagesCalled = true
	return sm.MockListObjectsV2(ctx, params, optFns...)
}
