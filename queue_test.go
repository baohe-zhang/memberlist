package memberlist

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransmitLimited_Queue(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 1 }}
	q.QueueBroadcast(&memberlistBroadcast{"test", nil, nil})
	q.QueueBroadcast(&memberlistBroadcast{"foo", nil, nil})
	q.QueueBroadcast(&memberlistBroadcast{"bar", nil, nil})

	if q.NumQueued() != 3 {
		t.Fatalf("bad len")
	}
	dump := q.orderedView(true)
	if dump[0].b.(*memberlistBroadcast).node != "test" {
		t.Fatalf("missing test")
	}
	if dump[1].b.(*memberlistBroadcast).node != "foo" {
		t.Fatalf("missing foo")
	}
	if dump[2].b.(*memberlistBroadcast).node != "bar" {
		t.Fatalf("missing bar")
	}

	// Should invalidate previous message
	q.QueueBroadcast(&memberlistBroadcast{"test", nil, nil})

	if q.NumQueued() != 3 {
		t.Fatalf("bad len")
	}
	dump = q.orderedView(true)
	if dump[0].b.(*memberlistBroadcast).node != "foo" {
		t.Fatalf("missing foo")
	}
	if dump[1].b.(*memberlistBroadcast).node != "bar" {
		t.Fatalf("missing bar")
	}
	if dump[2].b.(*memberlistBroadcast).node != "test" {
		t.Fatalf("missing test")
	}
}

func TestTransmitLimited_GetBroadcasts(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 3, NumNodes: func() int { return 10 }}

	// 18 bytes per message
	q.QueueBroadcast(&memberlistBroadcast{"test", []byte("1. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"foo", []byte("2. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"bar", []byte("3. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"baz", []byte("4. this is a test."), nil})

	// 2 byte overhead per message, should get all 4 messages
	all := q.GetBroadcasts(2, 80)
	if len(all) != 4 {
		t.Fatalf("missing messages: %v", all)
	}

	// 3 byte overhead, should only get 3 messages back
	partial := q.GetBroadcasts(3, 80)
	if len(partial) != 3 {
		t.Fatalf("missing messages: %v", partial)
	}
}

func TestTransmitLimited_GetBroadcasts_Limit(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 10 }}

	require.Equal(t, 2, retransmitLimit(q.RetransmitMult, q.NumNodes()), "sanity check transmit limits")

	// 18 bytes per message
	q.QueueBroadcast(&memberlistBroadcast{"test", []byte("1. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"foo", []byte("2. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"bar", []byte("3. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"baz", []byte("4. this is a test."), nil})

	// 3 byte overhead, should only get 3 messages back
	partial1 := q.GetBroadcasts(3, 80)
	require.Equal(t, 3, len(partial1), "missing messages: %v", partial1)

	partial2 := q.GetBroadcasts(3, 80)
	require.Equal(t, 3, len(partial2), "missing messages: %v", partial2)

	// Only two not expired
	partial3 := q.GetBroadcasts(3, 80)
	require.Equal(t, 2, len(partial3), "missing messages: %v", partial3)

	// Should get nothing
	partial5 := q.GetBroadcasts(3, 80)
	if len(partial5) != 0 {
		t.Fatalf("missing messages: %v", partial5)
	}
}

func TestTransmitLimited_Prune(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 10 }}

	ch1 := make(chan struct{}, 1)
	ch2 := make(chan struct{}, 1)

	// 18 bytes per message
	q.QueueBroadcast(&memberlistBroadcast{"test", []byte("1. this is a test."), ch1})
	q.QueueBroadcast(&memberlistBroadcast{"foo", []byte("2. this is a test."), ch2})
	q.QueueBroadcast(&memberlistBroadcast{"bar", []byte("3. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"baz", []byte("4. this is a test."), nil})

	// Keep only 2
	q.Prune(2)

	if q.NumQueued() != 2 {
		t.Fatalf("bad len")
	}

	// Should notify the first two
	select {
	case <-ch1:
	default:
		t.Fatalf("expected invalidation")
	}
	select {
	case <-ch2:
	default:
		t.Fatalf("expected invalidation")
	}

	dump := q.orderedView(true)

	if dump[0].b.(*memberlistBroadcast).node != "bar" {
		t.Fatalf("missing bar")
	}
	if dump[1].b.(*memberlistBroadcast).node != "baz" {
		t.Fatalf("missing baz")
	}
}

func TestTransmitLimited_ordering(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 10 }}

	insert := func(name string, transmits int) {
		q.queueBroadcast(&memberlistBroadcast{name, []byte(name), make(chan struct{})}, transmits)
	}

	insert("node0", 0)
	insert("node1", 10)
	insert("node2", 3)
	insert("node3", 4)
	insert("node4", 7)

	dump := q.orderedView(true)

	if dump[0].transmits != 10 {
		t.Fatalf("bad val %v, %d", dump[0].b.(*memberlistBroadcast).node, dump[0].transmits)
	}
	if dump[1].transmits != 7 {
		t.Fatalf("bad val %v, %d", dump[7].b.(*memberlistBroadcast).node, dump[7].transmits)
	}
	if dump[2].transmits != 4 {
		t.Fatalf("bad val %v, %d", dump[2].b.(*memberlistBroadcast).node, dump[2].transmits)
	}
	if dump[3].transmits != 3 {
		t.Fatalf("bad val %v, %d", dump[3].b.(*memberlistBroadcast).node, dump[3].transmits)
	}
	if dump[4].transmits != 0 {
		t.Fatalf("bad val %v, %d", dump[4].b.(*memberlistBroadcast).node, dump[4].transmits)
	}
}
