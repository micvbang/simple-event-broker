package tester

import (
	"fmt"
	"io"

	"github.com/micvbang/simple-event-broker/internal/sebtopic"
)

type MockTopicStorage struct {
	ListFilesMock  func(topicName string, extension string) ([]sebtopic.File, error)
	ListFilesCalls []storageListFilesCall

	ReaderMock  func(recordBatchPath string) (io.ReadCloser, error)
	ReaderCalls []storageReaderCall

	WriterMock  func(recordBatchPath string) (io.WriteCloser, error)
	WriterCalls []storageWriterCall
}

type storageListFilesCall struct {
	TopicName string
	Extension string

	Out0 []sebtopic.File
	Out1 error
}

func (_v *MockTopicStorage) ListFiles(topicName string, extension string) ([]sebtopic.File, error) {
	if _v.ListFilesMock == nil {
		msg := fmt.Sprintf("call to %T.ListFiles, but MockListFiles is not set", _v)
		panic(msg)
	}

	_v.ListFilesCalls = append(_v.ListFilesCalls, storageListFilesCall{
		TopicName: topicName,
		Extension: extension,
	})
	out0, out1 := _v.ListFilesMock(topicName, extension)
	_v.ListFilesCalls[len(_v.ListFilesCalls)-1].Out0 = out0
	_v.ListFilesCalls[len(_v.ListFilesCalls)-1].Out1 = out1
	return out0, out1
}

type storageReaderCall struct {
	RecordBatchPath string

	Out0 io.ReadCloser
	Out1 error
}

func (_v *MockTopicStorage) Reader(recordBatchPath string) (io.ReadCloser, error) {
	if _v.ReaderMock == nil {
		msg := fmt.Sprintf("call to %T.Reader, but MockReader is not set", _v)
		panic(msg)
	}

	_v.ReaderCalls = append(_v.ReaderCalls, storageReaderCall{
		RecordBatchPath: recordBatchPath,
	})
	out0, out1 := _v.ReaderMock(recordBatchPath)
	_v.ReaderCalls[len(_v.ReaderCalls)-1].Out0 = out0
	_v.ReaderCalls[len(_v.ReaderCalls)-1].Out1 = out1
	return out0, out1
}

type storageWriterCall struct {
	RecordBatchPath string

	Out0 io.WriteCloser
	Out1 error
}

func (_v *MockTopicStorage) Writer(recordBatchPath string) (io.WriteCloser, error) {
	if _v.WriterMock == nil {
		msg := fmt.Sprintf("call to %T.Writer, but MockWriter is not set", _v)
		panic(msg)
	}

	_v.WriterCalls = append(_v.WriterCalls, storageWriterCall{
		RecordBatchPath: recordBatchPath,
	})
	out0, out1 := _v.WriterMock(recordBatchPath)
	_v.WriterCalls[len(_v.WriterCalls)-1].Out0 = out0
	_v.WriterCalls[len(_v.WriterCalls)-1].Out1 = out1
	return out0, out1
}
