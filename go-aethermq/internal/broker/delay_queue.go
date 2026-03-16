package broker

import (
	"container/heap"
	"sync"
	"time"

	"github.com/aethermq/aethermq/internal/common"
)

type DelayQueue struct {
	mu       sync.Mutex
	pq       priorityQueue
	ready    chan struct{}
	running  bool
}

type delayItem struct {
	message   *common.Message
	deliverAt time.Time
	index     int
}

type priorityQueue []*delayItem

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].deliverAt.Before(pq[j].deliverAt)
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*delayItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

func NewDelayQueue() *DelayQueue {
	dq := &DelayQueue{
		pq:    make(priorityQueue, 0),
		ready: make(chan struct{}, 1),
	}
	heap.Init(&dq.pq)
	return dq
}

func (dq *DelayQueue) Schedule(msg *common.Message, deliverAt time.Time) {
	dq.mu.Lock()
	defer dq.mu.Unlock()

	item := &delayItem{
		message:   msg,
		deliverAt: deliverAt,
	}
	heap.Push(&dq.pq, item)

	select {
	case dq.ready <- struct{}{}:
	default:
	}
}

func (dq *DelayQueue) Run() {
	dq.running = true
	for dq.running {
		dq.mu.Lock()
		if dq.pq.Len() == 0 {
			dq.mu.Unlock()
			<-dq.ready
			continue
		}

		item := dq.pq[0]
		now := time.Now()
		if item.deliverAt.After(now) {
			dq.mu.Unlock()
			time.Sleep(item.deliverAt.Sub(now))
			continue
		}

		heap.Pop(&dq.pq)
		dq.mu.Unlock()

		item.message.DeliverAt = 0
	}
}

func (dq *DelayQueue) Close() {
	dq.running = false
	close(dq.ready)
}
