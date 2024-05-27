package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/micvbang/go-helpy/sizey"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

func EvictionLoop(ctx context.Context, log logger.Logger, cache *Cache, cacheMaxBytes int64, interval time.Duration) error {
	log = log.
		WithField("max bytes", cacheMaxBytes).
		WithField("interval", interval)

	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		cacheSize := cache.Size()
		if cacheSize <= cacheMaxBytes {
			continue
		}

		fillLevel := float32(cacheSize) / float32(cacheMaxBytes) * 100
		log.Infof("cache full (%.2f%%, %s/%s bytes), evicting items", fillLevel, sizey.FormatBytes(cacheSize), sizey.FormatBytes(cacheMaxBytes))

		err := cache.EvictLeastRecentlyUsed(cacheMaxBytes)
		if err != nil {
			return fmt.Errorf("evicting cache: %w", err)
		}
	}
}
