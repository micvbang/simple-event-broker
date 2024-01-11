package tester

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type S3Mock struct {
	s3iface.S3API

	MockPutObject   func(*s3.PutObjectInput) (*s3.PutObjectOutput, error)
	PutObjectCalled bool

	MockGetObject   func(*s3.GetObjectInput) (*s3.GetObjectOutput, error)
	GetObjectCalled bool

	MockListObjectPages   func(*s3.ListObjectsInput, func(*s3.ListObjectsOutput, bool) bool) error
	ListObjectPagesCalled bool
}

func (sm *S3Mock) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	sm.PutObjectCalled = true
	return sm.MockPutObject(input)
}

func (sm *S3Mock) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	sm.GetObjectCalled = true
	return sm.MockGetObject(input)
}

func (sm *S3Mock) ListObjectsPages(input *s3.ListObjectsInput, f func(*s3.ListObjectsOutput, bool) bool) error {
	sm.ListObjectPagesCalled = true
	return sm.MockListObjectPages(input, f)
}
