package sharded

import (
	"fmt"
	"sync"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
)

func TestNewValidation(t *testing.T) {
	if _, err := New[string, int](10, 0); err == nil {
		t.Fatal("expected error for zero shards")
	}
	if _, err := New[string, int](4, 8); err == nil {
		t.Fatal("expected error for size < shards")
	}
	if _, err := New[string, int](8, 4); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestShardCapsSum(t *testing.T) {
	for _, tc := range []struct{ size, n int }{
		{8, 4}, {10, 4}, {11, 4}, {100, 7}, {5, 5}, {1, 1},
	} {
		caps := shardCaps(tc.size, tc.n)
		sum := 0
		for _, c := range caps {
			if c <= 0 {
				t.Fatalf("size=%d n=%d: non-positive shard cap", tc.size, tc.n)
			}
			sum += c
		}
		if sum != tc.size {
			t.Fatalf("size=%d n=%d: caps sum to %d", tc.size, tc.n, sum)
		}
		// base-plus-remainder: caps differ by at most 1, extras go first
		for i := 1; i < len(caps); i++ {
			if caps[i] > caps[i-1] {
				t.Fatalf("size=%d n=%d: caps not front-loaded: %v", tc.size, tc.n, caps)
			}
		}
	}
}

func TestDistributionDeterministicAndUniform(t *testing.T) {
	const shards = 8
	c, err := New[string, int](8000, shards)
	if err != nil {
		t.Fatal(err)
	}
	counts := make([]int, shards)
	const keys = 20000
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("key-%d", i)
		idx := c.hash(k) % uint64(shards)
		// determinism: hashing the same key twice must agree
		if c.hash(k)%uint64(shards) != idx {
			t.Fatalf("hash not stable for %q", k)
		}
		counts[idx]++
	}
	// uniformity: each shard should get roughly keys/shards; allow a
	// generous 25% band around the ideal.
	ideal := float64(keys) / shards
	for i, n := range counts {
		if float64(n) < ideal*0.75 || float64(n) > ideal*1.25 {
			t.Fatalf("shard %d got %d keys, ideal %.0f (counts %v)", i, n, ideal, counts)
		}
	}
}

func TestTotalCapacityBound(t *testing.T) {
	const size, shards = 100, 7
	c, err := New[int, int](size, shards)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < size*10; i++ {
		c.Add(i, i)
	}
	if c.Len() > size {
		t.Fatalf("Len() = %d, exceeds size %d", c.Len(), size)
	}
	if c.Cap() != size {
		t.Fatalf("Cap() = %d, want %d", c.Cap(), size)
	}
	st := c.Stats()
	if st.Cap != size || st.Len != c.Len() || st.Shards != shards {
		t.Fatalf("bad stats: %+v", st)
	}
	sum := 0
	for _, ss := range st.PerShard {
		sum += ss.Cap
	}
	if sum != size {
		t.Fatalf("per-shard caps sum to %d, want %d", sum, size)
	}
}

// forceOneShard routes every key to shard 0 so eviction order can be
// compared directly against the root lru.Cache.
func forceOneShard[K comparable](K) uint64 { return 0 }

func TestEvictionOrderMatchesRoot(t *testing.T) {
	const size, shards = 9, 3 // shard caps: 3,3,3; all keys land in shard 0 (cap 3)
	sc, err := NewWithEvictAndHash[int, int](size, shards, nil, forceOneShard[int])
	if err != nil {
		t.Fatal(err)
	}
	rc, err := lru.New[int, int](3)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if got, want := sc.Add(i, i), rc.Add(i, i); got != want {
			t.Fatalf("Add(%d): evicted=%v, root=%v", i, got, want)
		}
		skeys, rkeys := sc.Keys(), rc.Keys()
		if len(skeys) != len(rkeys) {
			t.Fatalf("keys len %d vs root %d", len(skeys), len(rkeys))
		}
		for j := range rkeys {
			if skeys[j] != rkeys[j] {
				t.Fatalf("keys[%d] = %d, root has %d (all %v vs %v)", j, skeys[j], rkeys[j], skeys, rkeys)
			}
		}
	}
	// recency via Get must also match
	sc.Get(18)
	rc.Get(18)
	sc.Add(100, 100)
	rc.Add(100, 100)
	skeys, rkeys := sc.Keys(), rc.Keys()
	for j := range rkeys {
		if skeys[j] != rkeys[j] {
			t.Fatalf("after touch: %v vs root %v", skeys, rkeys)
		}
	}
}

func TestEvictCallback(t *testing.T) {
	var mu sync.Mutex
	evicted := make(map[int]int)
	onEvict := func(k, v int) {
		mu.Lock()
		evicted[k] = v
		mu.Unlock()
	}
	c, err := NewWithEvict[int, int](16, 4, onEvict)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		c.Add(i, i)
	}
	mu.Lock()
	n := len(evicted)
	mu.Unlock()
	if n != 64-c.Len() {
		t.Fatalf("evicted callback fired %d times, want %d", n, 64-c.Len())
	}
	// Remove and Purge must also trigger the callback, like the root cache.
	k := c.Keys()[0]
	c.Remove(k)
	mu.Lock()
	if _, ok := evicted[k]; !ok {
		t.Fatalf("Remove(%d) did not fire callback", k)
	}
	before := len(evicted)
	remaining := c.Len()
	mu.Unlock()
	c.Purge()
	mu.Lock()
	if len(evicted) != before+remaining {
		t.Fatalf("Purge fired %d callbacks, want %d", len(evicted)-before, remaining)
	}
	mu.Unlock()
	if c.Len() != 0 {
		t.Fatalf("Len after Purge = %d", c.Len())
	}
}

func TestResize(t *testing.T) {
	c, err := New[int, int](100, 6)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		c.Add(i, i)
	}
	before := c.Len()
	evicted := c.Resize(50)
	if evicted != before-c.Len() {
		t.Fatalf("Resize evicted %d, but Len went %d -> %d", evicted, before, c.Len())
	}
	if c.Len() > 50 {
		t.Fatalf("Len after Resize = %d, exceeds 50", c.Len())
	}
	st := c.Stats()
	if st.Cap != 50 {
		t.Fatalf("Cap after Resize = %d, want 50", st.Cap)
	}
	sum := 0
	for _, ss := range st.PerShard {
		sum += ss.Cap
	}
	if sum != 50 {
		t.Fatalf("shard caps sum to %d, want 50", sum)
	}
	// grow back up; no evictions expected
	if evicted := c.Resize(200); evicted != 0 {
		t.Fatalf("grow Resize evicted %d, want 0", evicted)
	}
	if c.Cap() != 200 {
		t.Fatalf("Cap after grow = %d, want 200", c.Cap())
	}
	// clamp: size < shards becomes shards
	c.Resize(2)
	if c.Cap() != 6 {
		t.Fatalf("Cap after clamp = %d, want 6 (shard count)", c.Cap())
	}
}

func TestStatsHitsMisses(t *testing.T) {
	c, err := New[string, int](16, 4)
	if err != nil {
		t.Fatal(err)
	}
	c.Add("a", 1)
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatal("Get(a) failed")
	}
	if _, ok := c.Get("missing"); ok {
		t.Fatal("Get(missing) should miss")
	}
	// Peek/Contains must not affect hit/miss counters
	c.Peek("a")
	c.Contains("a")
	st := c.Stats()
	if st.Hits != 1 || st.Misses != 1 {
		t.Fatalf("Hits=%d Misses=%d, want 1/1", st.Hits, st.Misses)
	}
	perShardHits := uint64(0)
	for _, ss := range st.PerShard {
		perShardHits += ss.Hits
	}
	if perShardHits != st.Hits {
		t.Fatalf("per-shard hits %d != total %d", perShardHits, st.Hits)
	}
}

func TestGetAddRemoveContainsPeek(t *testing.T) {
	c, err := New[string, int](32, 4)
	if err != nil {
		t.Fatal(err)
	}
	c.Add("x", 42)
	if !c.Contains("x") {
		t.Fatal("Contains(x) = false")
	}
	if v, ok := c.Peek("x"); !ok || v != 42 {
		t.Fatal("Peek(x) failed")
	}
	if ok, evicted := c.ContainsOrAdd("x", 1); !ok || evicted {
		t.Fatal("ContainsOrAdd on existing key")
	}
	if v, ok, _ := c.PeekOrAdd("y", 7); ok || v != 0 {
		t.Fatal("PeekOrAdd on missing key")
	}
	if !c.Remove("x") || c.Contains("x") {
		t.Fatal("Remove(x) failed")
	}
	if c.Remove("x") {
		t.Fatal("second Remove(x) should report not present")
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	c, err := New[int, int](1024, 16)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				k := (g*5000 + i) % 2048
				switch i % 5 {
				case 0:
					c.Add(k, i)
				case 1:
					c.Get(k)
				case 2:
					c.Contains(k)
				case 3:
					c.Peek(k)
				case 4:
					c.Remove(k)
				}
			}
		}(g)
	}
	// concurrent observers and resizers
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = c.Len()
			_ = c.Stats()
			_ = c.Keys()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			c.Resize(512 + i*8)
		}
	}()
	wg.Wait()
	if c.Len() > c.Cap() {
		t.Fatalf("Len %d exceeds Cap %d after concurrent run", c.Len(), c.Cap())
	}
}
