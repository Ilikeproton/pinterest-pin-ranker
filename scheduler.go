package main

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const maxConcurrency = 32

type Scheduler struct {
	store   *Store
	crawler *Crawler

	concurrency  atomic.Int64
	globalRun    atomic.Bool
	activeTokens chan struct{}
	wakeCh       chan struct{}
	stopCh       chan struct{}

	loopOnce sync.Once
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func NewScheduler(store *Store, crawler *Crawler, concurrency int, globalRunning bool) *Scheduler {
	s := &Scheduler{
		store:        store,
		crawler:      crawler,
		activeTokens: make(chan struct{}, maxConcurrency),
		wakeCh:       make(chan struct{}, 1),
		stopCh:       make(chan struct{}),
	}
	s.concurrency.Store(int64(clampConcurrency(concurrency)))
	s.globalRun.Store(globalRunning)
	return s
}

func (s *Scheduler) Start() {
	s.loopOnce.Do(func() {
		s.wg.Add(1)
		go s.loop()
	})
}

func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
	s.wg.Wait()
}

func (s *Scheduler) loop() {
	defer s.wg.Done()
	ticker := time.NewTicker(700 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.dispatch()
		case <-s.wakeCh:
			s.dispatch()
		case <-s.stopCh:
			return
		}
	}
}

func (s *Scheduler) dispatch() {
	if !s.globalRun.Load() {
		return
	}

	desired := int(s.concurrency.Load())
	if desired <= 0 {
		return
	}
	if desired > maxConcurrency {
		desired = maxConcurrency
	}

	active := len(s.activeTokens)
	slots := desired - active
	if slots <= 0 {
		return
	}

	for i := 0; i < slots; i++ {
		select {
		case s.activeTokens <- struct{}{}:
			s.wg.Add(1)
			go s.runOne()
		default:
			return
		}
	}
}

func (s *Scheduler) runOne() {
	defer func() {
		<-s.activeTokens
		s.wg.Done()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()

	task, err := s.store.ClaimPendingTask(ctx)
	if err != nil {
		log.Printf("claim task error: %v", err)
		return
	}
	if task == nil {
		return
	}

	hearts, err := s.processTask(ctx, *task)
	if err != nil {
		if markErr := s.store.MarkTaskFailure(ctx, *task, err); markErr != nil {
			log.Printf("mark task failure error: %v", markErr)
		}
		return
	}

	if err := s.store.MarkTaskDone(ctx, task.ID, hearts); err != nil {
		log.Printf("mark task done error: %v", err)
		return
	}

	if _, err := s.store.FinalizeBatchIfReady(ctx, task.BatchID); err != nil {
		log.Printf("finalize batch warning: %v", err)
	}
}

func (s *Scheduler) processTask(ctx context.Context, task CrawlTask) (int, error) {
	result, err := s.crawler.CrawlPin(ctx, task.URL)
	if err != nil {
		return 0, err
	}

	pin, err := s.store.UpsertPin(ctx, result)
	if err != nil {
		return 0, err
	}

	effectiveHearts := pin.Hearts
	if result.Hearts > effectiveHearts {
		effectiveHearts = result.Hearts
	}

	qualified := effectiveHearts >= task.Threshold
	included, err := s.store.UpsertBatchPin(ctx, task.BatchID, pin.ID, effectiveHearts, qualified, task.MaxImages)
	if err != nil {
		return 0, err
	}

	imageURL := result.ImageURL
	if imageURL == "" {
		imageURL = pin.ImageURL
	}

	if included && !pin.Downloaded && imageURL != "" {
		path, downloadErr := s.crawler.DownloadImage(ctx, result.URL, imageURL)
		if downloadErr != nil {
			log.Printf("batch=%d pin=%s image download failed: %v", task.BatchID, result.URL, downloadErr)
		} else if err := s.store.UpdatePinImage(ctx, pin.ID, imageURL, path); err != nil {
			log.Printf("batch=%d pin=%s update image path failed: %v", task.BatchID, result.URL, err)
		}
	}

	batchFull, fullErr := s.store.BatchReachedImageCap(ctx, task.BatchID)
	if fullErr != nil {
		log.Printf("batch=%d cap check warning: %v", task.BatchID, fullErr)
	}

	if !batchFull && len(result.Links) > 0 && task.Depth < task.MaxDepth {
		if err := s.store.EnqueueMany(ctx, task.BatchID, result.Links, task.Depth+1); err != nil {
			log.Printf("enqueue discovered links failed: %v", err)
		}
	}

	return effectiveHearts, nil
}

func (s *Scheduler) SetConcurrency(concurrency int) {
	s.concurrency.Store(int64(clampConcurrency(concurrency)))
	s.wake()
}

func (s *Scheduler) Concurrency() int {
	return int(s.concurrency.Load())
}

func (s *Scheduler) SetGlobalRunning(running bool) {
	s.globalRun.Store(running)
	s.wake()
}

func (s *Scheduler) GlobalRunning() bool {
	return s.globalRun.Load()
}

func (s *Scheduler) wake() {
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}
