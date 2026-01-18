package maintenance

import (
	"context"
	"sync"
	"time"

	"github.com/sdrshn-nmbr/bulletant/internal/storage"
)

type CompactionConfig struct {
	Compacter               storage.Compacter
	Interval                time.Duration
	MaxEntries              uint32
	MaxBytes                uint64
	RateLimitBytesPerSecond uint64
	MaxInFlight             int
	TempPath                string
}

type CompactionScheduler struct {
	compacter  storage.Compacter
	interval   time.Duration
	maxEntries uint32
	maxBytes   uint64
	tempPath   string

	trigger chan struct{}
	stop    chan struct{}
	wg      sync.WaitGroup
	sem     chan struct{}
	limiter storage.RateLimiter

	startOnce sync.Once
	stopOnce  sync.Once
}

func NewCompactionScheduler(cfg CompactionConfig) *CompactionScheduler {
	maxInFlight := cfg.MaxInFlight
	if maxInFlight <= 0 {
		maxInFlight = 1
	}

	var limiter storage.RateLimiter
	if cfg.RateLimitBytesPerSecond > 0 {
		limiter = NewTokenBucket(int64(cfg.RateLimitBytesPerSecond))
	}

	return &CompactionScheduler{
		compacter:  cfg.Compacter,
		interval:   cfg.Interval,
		maxEntries: cfg.MaxEntries,
		maxBytes:   cfg.MaxBytes,
		tempPath:   cfg.TempPath,
		trigger:    make(chan struct{}, 1),
		stop:       make(chan struct{}),
		sem:        make(chan struct{}, maxInFlight),
		limiter:    limiter,
	}
}

func (s *CompactionScheduler) Start(ctx context.Context) {
	if s == nil {
		return
	}
	s.startOnce.Do(func() {
		s.wg.Add(1)
		go s.run(ctx)
	})
}

func (s *CompactionScheduler) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		close(s.stop)
		s.wg.Wait()
	})
}

func (s *CompactionScheduler) Trigger() {
	if s == nil {
		return
	}
	select {
	case s.trigger <- struct{}{}:
	default:
	}
}

func (s *CompactionScheduler) run(ctx context.Context) {
	defer s.wg.Done()

	var ticker *time.Ticker
	if s.interval > 0 {
		ticker = time.NewTicker(s.interval)
		defer ticker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		case <-s.trigger:
			s.launch(ctx)
		case <-s.tickChan(ticker):
			s.launch(ctx)
		}
	}
}

func (s *CompactionScheduler) tickChan(ticker *time.Ticker) <-chan time.Time {
	if ticker == nil {
		return nil
	}
	return ticker.C
}

func (s *CompactionScheduler) launch(ctx context.Context) {
	select {
	case s.sem <- struct{}{}:
	default:
		return
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() { <-s.sem }()

		opts := storage.CompactOptions{
			MaxEntries:  s.maxEntries,
			MaxBytes:    s.maxBytes,
			TempPath:    s.tempPath,
			Context:     ctx,
			RateLimiter: s.limiter,
		}
		_, _ = s.compacter.Compact(opts)
	}()
}
