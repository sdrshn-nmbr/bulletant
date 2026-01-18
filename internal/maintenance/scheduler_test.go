package maintenance

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdrshn-nmbr/bulletant/internal/storage"
)

type fakeCompacter struct {
	calls  int32
	called chan struct{}
	block  chan struct{}
}

func (f *fakeCompacter) Compact(opts storage.CompactOptions) (storage.CompactStats, error) {
	atomic.AddInt32(&f.calls, 1)
	if f.called != nil {
		f.called <- struct{}{}
	}
	if f.block != nil {
		<-f.block
	}
	return storage.CompactStats{}, nil
}

func TestCompactionSchedulerTrigger(t *testing.T) {
	compacter := &fakeCompacter{called: make(chan struct{}, 1)}
	scheduler := NewCompactionScheduler(CompactionConfig{
		Compacter:   compacter,
		Interval:    0,
		MaxEntries:  1,
		MaxBytes:    1,
		MaxInFlight: 1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduler.Start(ctx)
	defer scheduler.Stop()

	scheduler.Trigger()
	select {
	case <-compacter.called:
	case <-time.After(2 * time.Second):
		t.Fatal("compaction did not trigger")
	}
}

func TestCompactionSchedulerMaxInFlight(t *testing.T) {
	compacter := &fakeCompacter{
		called: make(chan struct{}, 2),
		block:  make(chan struct{}),
	}
	scheduler := NewCompactionScheduler(CompactionConfig{
		Compacter:   compacter,
		Interval:    0,
		MaxEntries:  1,
		MaxBytes:    1,
		MaxInFlight: 1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduler.Start(ctx)
	defer scheduler.Stop()

	scheduler.Trigger()
	scheduler.Trigger()

	select {
	case <-compacter.called:
	case <-time.After(2 * time.Second):
		t.Fatal("compaction did not trigger")
	}

	select {
	case <-compacter.called:
		t.Fatal("unexpected concurrent compaction")
	case <-time.After(200 * time.Millisecond):
	}

	close(compacter.block)
}
