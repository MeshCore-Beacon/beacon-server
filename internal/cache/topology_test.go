package cache

import (
	"context"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

type topologyReader struct {
	stubReader
	calls int
}

func (r *topologyReader) GetTopologyLinks(_ context.Context, _ []string, since, until time.Time) (*api.TopologyLinks, error) {
	r.calls++
	return &api.TopologyLinks{Since: since.UnixMilli(), Until: until.UnixMilli()}, nil
}

func TestTopologyCacheReusesTheWholeMinuteAndKeepsExactWindows(t *testing.T) {
	c, redis := newTestClient(t)
	t.Cleanup(func() { _ = c.Close() })
	inner := &topologyReader{}
	r := &CachedReader{inner: inner, c: c}
	until := time.Now().UTC().Truncate(time.Minute).Add(time.Minute)
	since := until.Add(-24 * time.Hour)
	read := func(regions []string, start, end time.Time) {
		t.Helper()
		if _, err := r.GetTopologyLinks(context.Background(), regions, start, end); err != nil {
			t.Fatal(err)
		}
	}
	read([]string{"YKF", "YOW"}, since, until)
	redis.FastForward(45 * time.Second)
	read([]string{"YOW", "YKF"}, since, until)
	if inner.calls != 1 {
		t.Fatalf("same minute snapshot recomputed: %d calls", inner.calls)
	}
	read([]string{"YOW", "YKF", "YKF"}, since, until)
	if inner.calls != 1 {
		t.Fatal("duplicate region widened the cache key")
	}
	read([]string{"YKF", "YOW"}, since.Add(time.Millisecond), until)
	if inner.calls != 2 {
		t.Fatal("different exact window reused a snapshot")
	}
	read([]string{"YKF", "YOW"}, since.Add(time.Minute), until.Add(time.Minute))
	if inner.calls != 3 {
		t.Fatal("new minute must get its own snapshot")
	}
}
