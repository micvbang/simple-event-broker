package httphandlers

import (
	"context"
	"fmt"

	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
)

type MockDependencies struct {
	AddRecordsMock  func(topicName string, batch sebrecords.Batch) ([]uint64, error)
	AddRecordsCalls []dependenciesAddRecordsCall

	GetRecordMock  func(batch *sebrecords.Batch, topicName string, offset uint64) ([]byte, error)
	GetRecordCalls []dependenciesGetRecordCall

	GetRecordsMock  func(ctx context.Context, batch *sebrecords.Batch, topicName string, offset uint64, maxRecords int, softMaxBytes int) error
	GetRecordsCalls []dependenciesGetRecordsCall

	MetadataMock  func(topicName string) (sebtopic.Metadata, error)
	MetadataCalls []dependenciesMetadataCall
}

type dependenciesAddRecordsCall struct {
	TopicName string
	Batch     sebrecords.Batch

	Out0 []uint64
	Out1 error
}

func (_v *MockDependencies) AddRecords(topicName string, batch sebrecords.Batch) ([]uint64, error) {
	if _v.AddRecordsMock == nil {
		msg := fmt.Sprintf("call to %T.AddRecords, but MockAddRecords is not set", _v)
		panic(msg)
	}

	_v.AddRecordsCalls = append(_v.AddRecordsCalls, dependenciesAddRecordsCall{
		TopicName: topicName,
		Batch:     batch,
	})
	out0, out1 := _v.AddRecordsMock(topicName, batch)
	_v.AddRecordsCalls[len(_v.AddRecordsCalls)-1].Out0 = out0
	_v.AddRecordsCalls[len(_v.AddRecordsCalls)-1].Out1 = out1
	return out0, out1
}

type dependenciesGetRecordCall struct {
	Batch     *sebrecords.Batch
	TopicName string
	Offset    uint64

	Out0 []byte
	Out1 error
}

func (_v *MockDependencies) GetRecord(batch *sebrecords.Batch, topicName string, offset uint64) ([]byte, error) {
	if _v.GetRecordMock == nil {
		msg := fmt.Sprintf("call to %T.GetRecord, but MockGetRecord is not set", _v)
		panic(msg)
	}

	_v.GetRecordCalls = append(_v.GetRecordCalls, dependenciesGetRecordCall{
		Batch:     batch,
		TopicName: topicName,
		Offset:    offset,
	})
	out0, out1 := _v.GetRecordMock(batch, topicName, offset)
	_v.GetRecordCalls[len(_v.GetRecordCalls)-1].Out0 = out0
	_v.GetRecordCalls[len(_v.GetRecordCalls)-1].Out1 = out1
	return out0, out1
}

type dependenciesGetRecordsCall struct {
	Ctx          context.Context
	Batch        *sebrecords.Batch
	TopicName    string
	Offset       uint64
	MaxRecords   int
	SoftMaxBytes int

	Out0 error
}

func (_v *MockDependencies) GetRecords(ctx context.Context, batch *sebrecords.Batch, topicName string, offset uint64, maxRecords int, softMaxBytes int) error {
	if _v.GetRecordsMock == nil {
		msg := fmt.Sprintf("call to %T.GetRecords, but MockGetRecords is not set", _v)
		panic(msg)
	}

	_v.GetRecordsCalls = append(_v.GetRecordsCalls, dependenciesGetRecordsCall{
		Ctx:          ctx,
		Batch:        batch,
		TopicName:    topicName,
		Offset:       offset,
		MaxRecords:   maxRecords,
		SoftMaxBytes: softMaxBytes,
	})
	out0 := _v.GetRecordsMock(ctx, batch, topicName, offset, maxRecords, softMaxBytes)
	_v.GetRecordsCalls[len(_v.GetRecordsCalls)-1].Out0 = out0
	return out0
}

type dependenciesMetadataCall struct {
	TopicName string

	Out0 sebtopic.Metadata
	Out1 error
}

func (_v *MockDependencies) Metadata(topicName string) (sebtopic.Metadata, error) {
	if _v.MetadataMock == nil {
		msg := fmt.Sprintf("call to %T.Metadata, but MockMetadata is not set", _v)
		panic(msg)
	}

	_v.MetadataCalls = append(_v.MetadataCalls, dependenciesMetadataCall{
		TopicName: topicName,
	})
	out0, out1 := _v.MetadataMock(topicName)
	_v.MetadataCalls[len(_v.MetadataCalls)-1].Out0 = out0
	_v.MetadataCalls[len(_v.MetadataCalls)-1].Out1 = out1
	return out0, out1
}
