package httphandlers

import (
	"context"
	"fmt"

	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
)

type MockDependencies struct {
	AddRecordMock  func(topicName string, record sebrecords.Record) (uint64, error)
	AddRecordCalls []dependenciesAddRecordCall

	AddRecordsMock  func(topicName string, records []sebrecords.Record) ([]uint64, error)
	AddRecordsCalls []dependenciesAddRecordsCall

	GetRecordMock  func(topicName string, offset uint64) (sebrecords.Record, error)
	GetRecordCalls []dependenciesGetRecordCall

	GetRecordsMock  func(ctx context.Context, topicName string, offset uint64, maxRecords int, softMaxBytes int) ([]sebrecords.Record, error)
	GetRecordsCalls []dependenciesGetRecordsCall

	MetadataMock  func(topicName string) (sebtopic.Metadata, error)
	MetadataCalls []dependenciesMetadataCall
}

type dependenciesAddRecordCall struct {
	TopicName string
	Record    sebrecords.Record

	Out0 uint64
	Out1 error
}

func (_v *MockDependencies) AddRecord(topicName string, record sebrecords.Record) (uint64, error) {
	if _v.AddRecordMock == nil {
		msg := fmt.Sprintf("call to %T.AddRecord, but MockAddRecord is not set", _v)
		panic(msg)
	}

	_v.AddRecordCalls = append(_v.AddRecordCalls, dependenciesAddRecordCall{
		TopicName: topicName,
		Record:    record,
	})
	out0, out1 := _v.AddRecordMock(topicName, record)
	_v.AddRecordCalls[len(_v.AddRecordCalls)-1].Out0 = out0
	_v.AddRecordCalls[len(_v.AddRecordCalls)-1].Out1 = out1
	return out0, out1
}

type dependenciesAddRecordsCall struct {
	TopicName string
	Records   []sebrecords.Record

	Out0 []uint64
	Out1 error
}

func (_v *MockDependencies) AddRecords(topicName string, records []sebrecords.Record) ([]uint64, error) {
	if _v.AddRecordsMock == nil {
		msg := fmt.Sprintf("call to %T.AddRecords, but MockAddRecords is not set", _v)
		panic(msg)
	}

	_v.AddRecordsCalls = append(_v.AddRecordsCalls, dependenciesAddRecordsCall{
		TopicName: topicName,
		Records:   records,
	})
	out0, out1 := _v.AddRecordsMock(topicName, records)
	_v.AddRecordsCalls[len(_v.AddRecordsCalls)-1].Out0 = out0
	_v.AddRecordsCalls[len(_v.AddRecordsCalls)-1].Out1 = out1
	return out0, out1
}

type dependenciesGetRecordCall struct {
	TopicName string
	Offset    uint64

	Out0 sebrecords.Record
	Out1 error
}

func (_v *MockDependencies) GetRecord(topicName string, offset uint64) (sebrecords.Record, error) {
	if _v.GetRecordMock == nil {
		msg := fmt.Sprintf("call to %T.GetRecord, but MockGetRecord is not set", _v)
		panic(msg)
	}

	_v.GetRecordCalls = append(_v.GetRecordCalls, dependenciesGetRecordCall{
		TopicName: topicName,
		Offset:    offset,
	})
	out0, out1 := _v.GetRecordMock(topicName, offset)
	_v.GetRecordCalls[len(_v.GetRecordCalls)-1].Out0 = out0
	_v.GetRecordCalls[len(_v.GetRecordCalls)-1].Out1 = out1
	return out0, out1
}

type dependenciesGetRecordsCall struct {
	Ctx          context.Context
	TopicName    string
	Offset       uint64
	MaxRecords   int
	SoftMaxBytes int

	Out0 []sebrecords.Record
	Out1 error
}

func (_v *MockDependencies) GetRecords(ctx context.Context, topicName string, offset uint64, maxRecords int, softMaxBytes int) ([]sebrecords.Record, error) {
	if _v.GetRecordsMock == nil {
		msg := fmt.Sprintf("call to %T.GetRecords, but MockGetRecords is not set", _v)
		panic(msg)
	}

	_v.GetRecordsCalls = append(_v.GetRecordsCalls, dependenciesGetRecordsCall{
		Ctx:          ctx,
		TopicName:    topicName,
		Offset:       offset,
		MaxRecords:   maxRecords,
		SoftMaxBytes: softMaxBytes,
	})
	out0, out1 := _v.GetRecordsMock(ctx, topicName, offset, maxRecords, softMaxBytes)
	_v.GetRecordsCalls[len(_v.GetRecordsCalls)-1].Out0 = out0
	_v.GetRecordsCalls[len(_v.GetRecordsCalls)-1].Out1 = out1
	return out0, out1
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
