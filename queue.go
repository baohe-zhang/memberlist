package memberlist

import (
	"container/list"
	"sync"
)

// TransmitLimitedQueue is used to queue messages to broadcast to
// the cluster (via gossip) but limits the number of transmits per
// message. It also prioritizes messages with lower transmit counts
// (hence newer messages).
type TransmitLimitedQueue struct {
	// NumNodes returns the number of nodes in the cluster. This is
	// used to determine the retransmit count, which is calculated
	// based on the log of this.
	NumNodes func() int

	// RetransmitMult is the multiplier used to determine the maximum
	// number of retransmissions attempted.
	RetransmitMult int

	mu     sync.Mutex
	tq     []*list.List // outer index is numTransmits
	tqSize int          // total item count in tq
	tm     map[string]*list.Element
}

type limitedBroadcast struct {
	transmits int // Number of transmissions attempted.
	b         Broadcast

	name   string // set if Broadcast is a NamedBroadcast
	unique bool   // set if Broadcast is a UniqueBroadcast
}

// for testing; emits in transmit order if reverse=false
func (q *TransmitLimitedQueue) orderedView(reverse bool) []*limitedBroadcast {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make([]*limitedBroadcast, 0, q.tqSize)
	q.walkReadOnlyLocked(reverse, func(cur *limitedBroadcast, bucketIdx int) bool {
		out = append(out, cur)
		return true
	})

	return out
}

func (q *TransmitLimitedQueue) walkReadOnlyLocked(reverse bool, f func(*limitedBroadcast, int) bool) {
	if q.tqSize == 0 {
		return
	}

	body := func(cur *limitedBroadcast, bucketIdx int) bool {
		prevTransmits := cur.transmits

		keepGoing := f(cur, bucketIdx)
		if prevTransmits != cur.transmits {
			panic("edited queue while walking read only")
		}
		return keepGoing
	}

	if reverse {
		// end with transmit 0
		for i := len(q.tq) - 1; i >= 0; i-- { // i is transmits
			bucket := q.tq[i]
			// walk in reverse insertion order
			for e, j := bucket.Back(), bucket.Len()-1; e != nil; e, j = e.Prev(), j-1 {
				cur := e.Value.(*limitedBroadcast)
				if !body(cur, j) {
					return
				}
			}
		}
	} else {
		// start with transmit 0
		for i := 0; i < len(q.tq); i++ { // i is transmits
			bucket := q.tq[i]
			// walk in insertion order
			for e, j := bucket.Front(), 0; e != nil; e, j = e.Next(), j+1 {
				cur := e.Value.(*limitedBroadcast)
				if !body(cur, j) {
					return
				}
			}
		}
	}
}

// Broadcast is something that can be broadcasted via gossip to
// the memberlist cluster.
type Broadcast interface {
	// Invalidates checks if enqueuing the current broadcast
	// invalidates a previous broadcast
	Invalidates(b Broadcast) bool

	// Returns a byte form of the message
	Message() []byte

	// Finished is invoked when the message will no longer
	// be broadcast, either due to invalidation or to the
	// transmit limit being reached
	Finished()
}

type NamedBroadcast interface {
	Broadcast
	Name() string
}

type UniqueBroadcast interface {
	Broadcast
	UniqueBroadcast()
}

// QueueBroadcast is used to enqueue a broadcast
func (q *TransmitLimitedQueue) QueueBroadcast(b Broadcast) {
	q.queueBroadcast(b, 0)
}
func (q *TransmitLimitedQueue) queueBroadcast(b Broadcast, initialTransmits int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.tm == nil {
		q.tm = make(map[string]*list.Element)
	}

	lb := &limitedBroadcast{transmits: initialTransmits, b: b}
	if nb, ok := b.(NamedBroadcast); ok {
		lb.name = nb.Name()
	} else if _, ok := b.(UniqueBroadcast); ok {
		lb.unique = true
	}

	// Check if this message invalidates another.
	if lb.name != "" {
		if e, ok := q.tm[lb.name]; ok {
			old := e.Value.(*limitedBroadcast)
			old.b.Finished()
			q.deleteItem(old.transmits, e)
		}
	} else if !lb.unique {
		// Slow path.
		for i := 0; i < len(q.tq); i++ {
			bucket := q.tq[i]
			var next *list.Element
			for e := bucket.Front(); e != nil; e = next {
				next = e.Next()
				cur := e.Value.(*limitedBroadcast)

				if b.Invalidates(cur.b) {
					cur.b.Finished()
					q.deleteItem(i, e)
				}
			}
		}
	}

	// Append to the relevant queue.
	q.addFirstTime(lb)
}

// must hold lock
func (q *TransmitLimitedQueue) deleteItem(transmits int, e *list.Element) {
	cur := e.Value.(*limitedBroadcast)
	q.tq[transmits].Remove(e)
	q.tqSize--
	if cur.name != "" {
		delete(q.tm, cur.name)
	}
}

// must hold lock
func (q *TransmitLimitedQueue) addFirstTime(cur *limitedBroadcast) {
	q.maybeGrow(cur.transmits + 1)
	e := q.tq[cur.transmits].PushFront(cur)
	q.tqSize++
	if cur.name != "" {
		q.tm[cur.name] = e
	}
}

// must hold lock
func (q *TransmitLimitedQueue) maybeGrow(maxTransmits int) {
	if maxTransmits <= len(q.tq) {
		return
	}

	oldLen := len(q.tq)
	for i := oldLen; i < maxTransmits; i++ {
		q.tq = append(q.tq, list.New()) // trigger normal slice growth under the covers
	}
}

// GetBroadcasts is used to get a number of broadcasts, up to a byte limit
// and applying a per-message overhead as provided.
func (q *TransmitLimitedQueue) GetBroadcasts(overhead, limit int) [][]byte {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Fast path the default case
	if q.tqSize == 0 {
		return nil
	}

	transmitLimit := retransmitLimit(q.RetransmitMult, q.NumNodes())

	var bytesUsed int
	var toSend [][]byte

	// Take the cap we just calculated and resize our queue internals to fit.
	q.maybeGrow(transmitLimit)

	// Visit fresher items first (lower transmits, and more recently inserted).
	var reinsert []*limitedBroadcast
OUTER:
	for i := 0; i < len(q.tq); i++ { // i is transmits
		bucket := q.tq[i]

		var next *list.Element
		for e := bucket.Front(); e != nil; e = next {
			next = e.Next()
			cur := e.Value.(*limitedBroadcast)

			if bytesUsed+overhead >= limit {
				break OUTER // bail out early
			}

			// Check if this is within our limits
			msg := cur.b.Message()
			if bytesUsed+overhead+len(msg) > limit {
				continue
			}
			// Add to slice to send
			bytesUsed += overhead + len(msg)
			toSend = append(toSend, msg)

			// Check if we should stop transmission
			cur.transmits++
			if cur.transmits >= transmitLimit {
				cur.b.Finished()
				q.deleteItem(i, e)
			} else {
				q.deleteItem(i, e)
				// Delay reinsertion to avoid grabbing the same
				// message multiple times.
				reinsert = append(reinsert, cur)
			}
		}
	}

	for _, cur := range reinsert {
		q.addFirstTime(cur)
	}

	return toSend
}

// NumQueued returns the number of queued messages
func (q *TransmitLimitedQueue) NumQueued() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.tqSize
}

// Reset clears all the queued messages
func (q *TransmitLimitedQueue) Reset() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.walkReadOnlyLocked(false, func(cur *limitedBroadcast, bucketIdx int) bool {
		cur.b.Finished()
		return true
	})

	q.tq = nil
	q.tqSize = 0
	q.tm = nil
}

// Prune will retain the maxRetain latest messages, and the rest
// will be discarded. This can be used to prevent unbounded queue sizes
func (q *TransmitLimitedQueue) Prune(maxRetain int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Do nothing if queue size is less than the limit
	n := q.tqSize
	if n < maxRetain {
		return
	}

	// Invalidate the messages we will be removing
	pruneCount := n - maxRetain

	var pruned int
	for i := len(q.tq) - 1; i >= 0; i-- { // i is transmits
		remaining := pruneCount - pruned
		if remaining <= 0 {
			return
		}

		var retainInBucket int
		if remaining >= n {
			retainInBucket = 0
		} else {
			retainInBucket = remaining
		}

		bucket := q.tq[i]
		// walk in reverse insertion order
		var prev *list.Element
		for e := bucket.Back(); e != nil && bucket.Len() > retainInBucket; e = prev {
			prev = e.Prev()
			cur := e.Value.(*limitedBroadcast)
			cur.b.Finished()
			q.deleteItem(i, e)
			pruned++
		}
	}
}
