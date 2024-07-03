package tester

import "fmt"

type MockWriteCloser struct {
	CloseMock  func() error
	CloseCalls []writeCloserCloseCall

	WriteMock  func(p []byte) (n int, err error)
	WriteCalls []writeCloserWriteCall
}

type writeCloserCloseCall struct {
	Out0 error
}

func (_v *MockWriteCloser) Close() error {
	if _v.CloseMock == nil {
		msg := fmt.Sprintf("call to %T.Close, but MockClose is not set", _v)
		panic(msg)
	}

	_v.CloseCalls = append(_v.CloseCalls, writeCloserCloseCall{})
	out0 := _v.CloseMock()
	_v.CloseCalls[len(_v.CloseCalls)-1].Out0 = out0
	return out0
}

type writeCloserWriteCall struct {
	P []byte

	Out0 int
	Out1 error
}

func (_v *MockWriteCloser) Write(p []byte) (n int, err error) {
	if _v.WriteMock == nil {
		msg := fmt.Sprintf("call to %T.Write, but MockWrite is not set", _v)
		panic(msg)
	}

	_v.WriteCalls = append(_v.WriteCalls, writeCloserWriteCall{
		P: p,
	})
	out0, out1 := _v.WriteMock(p)
	_v.WriteCalls[len(_v.WriteCalls)-1].Out0 = out0
	_v.WriteCalls[len(_v.WriteCalls)-1].Out1 = out1
	return out0, out1
}
