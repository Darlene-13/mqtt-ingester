package ingest

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Darlene-13/mqtt-ingester/internal/model"
	"github.com/Darlene-13/mqtt-ingester/internal/store"
)

type Config struct {
	Workers     int
	QueueSize   int           //4096
	BatchSize   int           // 500
	FlushEvery  time.Duration //250ms
	EnqueueWait time.Duration //5ms before dropping
}

type Stats struct {
	Received, Written, Dropped, DecodeErr, Failed atomic.Uint64
}

type Pipeline struct {
	cfg   Config
	in    chan []byte
	w     *store.Writer
	wg    sync.WaitGroup
	Stats Stats
}

func New(cfg Config, w *store.Writer) *Pipeline {
	return &Pipeline{cfg: cfg, in: make(chan []byte, cfg.QueueSize), w: w}
}

func (p *Pipeline) Start() {
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

func (p *Pipeline) Stop() {
	close(p.in)
	p.wg.Wait()
}
func (p *Pipeline) QueueDepth() int {
	return len(p.in)
}
func (p *Pipeline) Submit(payload []byte) {
	p.Stats.Received.Add(1)
	select {
	case p.in <- payload:
		return
	default:
	}

	t := time.NewTimer(p.cfg.EnqueueWait)
	defer t.Stop()
	select {
	case p.in <- payload:
	case <-t.C:
		p.Stats.Dropped.Add(1)
	}
}

func (p *Pipeline) worker() {
	defer p.wg.Done()
	batch := make([]model.Reading, 0, p.cfg.BatchSize)
	tick := time.NewTicker(p.cfg.FlushEvery)
	defer tick.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		p.writeWithRetry(batch)
		batch = batch[:0]
	}

	for {
		select {
		case payload, ok := <-p.in:
			if !ok {
				flush()
				return
			}

			var r model.Reading
			if err := model.Decode(payload, &r); err != nil {
				p.Stats.DecodeErr.Add(1)
				continue
			}

			batch = append(batch, r)
			if len(batch) >= p.cfg.BatchSize {
				flush()
				tick.Reset(p.cfg.FlushEvery)
			}

		case <-tick.C:
			flush()
		}
	}
}

func (p *Pipeline) writeWithRetry(batch []model.Reading) {
	n := uint64(len(batch))
	backoff := 100 * time.Millisecond
	for attempt := range 4 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := p.w.Write(ctx, batch)
		cancel()

		if err == nil {
			p.Stats.Written.Add(1)
			return
		}

		log.Printf("failed to write batch (attempt %d): %v", attempt, err)
		time.Sleep(backoff)
		backoff *= 2
	}
	// Production: spill to a local file / dead-letter topic instead of losing it
	p.Stats.Dropped.Add(n)
}
