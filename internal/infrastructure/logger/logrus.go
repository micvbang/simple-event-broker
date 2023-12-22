package logger

import (
	"context"

	"github.com/sirupsen/logrus"
)

func NewLogrus(ctx context.Context, log *logrus.Logger) Logger {
	return &logrusEntryWrapper{
		Entry: log.WithContext(ctx),
	}
}

type logrusEntryWrapper struct {
	*logrus.Entry
}

func (le *logrusEntryWrapper) WithField(key string, value interface{}) Logger {
	return &logrusEntryWrapper{
		Entry: le.Entry.WithField(key, value),
	}
}

func (le *logrusEntryWrapper) Name(name string) Logger {
	return &logrusEntryWrapper{
		Entry: le.Entry.WithField("name", name),
	}
}
