// Copyright IBM Corp. 2014, 2025
// SPDX-License-Identifier: MPL-2.0

package sharded

import (
	"fmt"
	"sync"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
)

func shardIndexOf[K comparable, V any](c *Cache[K, V], key K) int {
	for i, s := range c.shards {
		if s == c.shardFor(key) {
			return i
		}
	}
	return -1
}

func TestSplitCapacity(t *testing.T) {
	cases := []struct {
		size, shards int
		want         []int
	}{
		{10, 4, []int{3, 3, 2, 2}},
		{8, 8, []int{1, 1, 1, 1, 1, 1, 1, 1}},
		{9, 8, []int{2, 1, 1, 1, 1, 1, 1, 1}},
		{100, 1, []int{100}},
		{3, 8, []int{1, 1, 1, 0, 0, 0, 0, 0}},
	}
	for _, tc := range cases {
		got := splitCapacity(tc.size, tc.shards)
		if len(got) != len(tc.want) {
			t.Fatalf("splitCapacity(%d, %d) len = %d, want %d", tc.size, tc.shards, len(got), len(tc.want))
		}
		sum := 0
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("splitCapacity(%d, %d) = %v, want %v", tc.size, tc.shards, got, tc.want)
			}
			sum += got[i]
		}
		if sum != tc.size {
			t.Fatalf("splitCapacity(%d, %d) sums to %d", tc.size, tc.shards, sum)
		}
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := NewString[int](10, 0); err == nil {
		t.Fatal("expected error for zero shard count")
	}
	if _, err := NewString[int](4, 8); err == nil {
		t.Fatal("expected error for size < shard count")
	}
	if _, err := New[string, int](10, 2, nil); err == nil {
		t.Fatal("expected error for nil hash")
	}
}

func TestShardDistributionDeterministicAndUniform(t *testing.T) {
	const shardCount = 16
	c1, err := NewString[int](1024, shardCount)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := NewString[int](1024, shardCount)
	if err != nil {
		t.Fatal(err)
	}

	counts := make([]int, shardCount)
	const n = 20000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		idx1 := shardIndexOf(c1, key)
		idx2 := shardIndexOf(c2, key)
		if idx1 != idx2 {
			t.Fatalf("key %q maps to shard %d in c1 but %d in c2", key, idx1, idx2)
		}
		counts[idx1]++
	}

	mean := float64(n) / shardCount
	for i, cnt := range counts {
		if float64(cnt) < mean*0.5 || float64(cnt) > mean*1.5 {
			t.Fatalf("shard %d holds %d of %d keys, outside [0.5, 1.5]x mean %.0f", i, cnt, n, mean)
		}
	}
}

func TestTotalCapacityEnforced(t *testing.T) {
	c, err := NewString[int](100, 8)
	if err != nil {
		t.Fatal(err)
	}
	if c.Cap() != 100 {
		t.Fatalf("Cap() = %d, want 100", c.Cap())
	}
	st := c.Stats()
	if st.Capacity != 100 || st.ShardCount != 8 {
		t.Fatalf("Stats: capacity %d shards %d, want 100/8", st.Capacity, st.ShardCount)
	}
	sum := 0
	for _, ss := range st.Shards {
		sum += ss.Capacity
	}
	if sum != 100 {
		t.Fatalf("per-shard capacities sum to %d, want 100", sum)
	}

	for i := 0; i < 5000; i++ {
		c.Add(fmt.Sprintf("key-%d", i), i)
	}
	if c.Len() > 100 {
		t.Fatalf("Len() = %d exceeds total capacity 100", c.Len())
	}
	if c.Len() != 100 {
		t.Fatalf("Len() = %d, want 100 (every shard should be full)", c.Len())
	}
}

// TestEvictionSemanticsMatchRoot runs identical operation sequences against a
// single-shard sharded cache and a root lru.Cache and requires identical
// observable behavior.
func TestEvictionSemanticsMatchRoot(t *testing.T) {
	root, err := lru.New[string, int](16)
	if err != nil {
		t.Fatal(err)
	}
	sh, err := NewString[int](16, 1)
	if err != nil {
		t.Fatal(err)
	}

	key := func(i int) string { return fmt.Sprintf("k%d", i) }
	check := func(step string) {
		t.Helper()
		for i := 0; i < 40; i++ {
			if got, want := sh.Contains(key(i)), root.Contains(key(i)); got != want {
				t.Fatalf("%s: Contains(%q) = %v, root says %v", step, key(i), got, want)
			}
		}
		if sh.Len() != root.Len() {
			t.Fatalf("%s: Len() = %d, root says %d", step, sh.Len(), root.Len())
		}
	}

	// Fill beyond capacity.
	for i := 0; i < 24; i++ {
		if got, want := sh.Add(key(i), i), root.Add(key(i), i); got != want {
			t.Fatalf("Add(%q): evicted = %v, root says %v", key(i), got, want)
		}
	}
	check("after fill")

	// Refresh some entries, then add more; the refreshed ones must survive.
	for _, i := range []int{8, 10, 12} {
		sh.Get(key(i))
		root.Get(key(i))
	}
	for i := 24; i < 30; i++ {
		sh.Add(key(i), i)
		root.Add(key(i), i)
	}
	check("after refresh+add")

	// Peek must not affect recency.
	sh.Peek(key(14))
	root.Peek(key(14))
	sh.Add(key(30), 30)
	root.Add(key(30), 30)
	check("after peek+add")

	// Remove must agree.
	if got, want := sh.Remove(key(20)), root.Remove(key(20)); got != want {
		t.Fatalf("Remove: %v, root says %v", got, want)
	}
	check("after remove")

	// Resize must agree.
	if got, want := sh.Resize(4), root.Resize(4); got != want {
		t.Fatalf("Resize: evicted %d, root says %d", got, want)
	}
	check("after resize")
}

func TestResizeCapacitySum(t *testing.T) {
	c, err := NewString[int](100, 8)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		c.Add(fmt.Sprintf("key-%d", i), i)
	}

	for _, size := range []int{50, 97, 8, 200, 3} {
		c.Resize(size)
		st := c.Stats()
		sum := 0
		for _, ss := range st.Shards {
			sum += ss.Capacity
		}
		if sum != size || c.Cap() != size || st.Capacity != size {
			t.Fatalf("after Resize(%d): shard caps sum %d, Cap() %d, Stats capacity %d", size, sum, c.Cap(), st.Capacity)
		}
		if c.Len() > size {
			t.Fatalf("after Resize(%d): Len() = %d", size, c.Len())
		}
	}
}

func TestEvictCallback(t *testing.T) {
	var mu sync.Mutex
	evicted := make(map[string]int)
	onEvicted := func(k string, v int) {
		mu.Lock()
		evicted[k] = v
		mu.Unlock()
	}

	c, err := NewStringWithEvict[int](8, 1, onEvicted)
	if err != nil {
		t.Fatal(err)
	}

	// Overflow: adding the 9th distinct key must evict the oldest.
	for i := 0; i < 9; i++ {
		c.Add(fmt.Sprintf("k%d", i), i)
	}
	mu.Lock()
	if len(evicted) != 1 || evicted["k0"] != 0 {
		t.Fatalf("after overflow: evicted = %v, want {k0:0}", evicted)
	}
	mu.Unlock()

	// Remove fires the callback, matching the root cache.
	if !c.Remove("k1") {
		t.Fatal("Remove(k1) = false, want true")
	}
	mu.Lock()
	if evicted["k1"] != 1 {
		t.Fatalf("after Remove: evicted = %v, want k1 present", evicted)
	}
	mu.Unlock()

	// Resize evictions fire the callback.
	before := c.Len()
	n := c.Resize(2)
	mu.Lock()
	if len(evicted) != 2+before-2 || n != before-2 {
		t.Fatalf("after Resize: evicted %d entries (callback saw %d total), want %d", n, len(evicted), before-2)
	}
	mu.Unlock()

	// Purge fires the callback for every remaining entry.
	remaining := c.Len()
	c.Purge()
	mu.Lock()
	if len(evicted) != 2+before {
		t.Fatalf("after Purge: callback saw %d entries, want %d", len(evicted), 2+before)
	}
	mu.Unlock()
	if c.Len() != 0 || remaining != 2 {
		t.Fatalf("after Purge: Len() = %d, want 0 (had %d)", c.Len(), remaining)
	}
}

func TestStatsHitMiss(t *testing.T) {
	c, err := NewString[int](16, 4)
	if err != nil {
		t.Fatal(err)
	}
	c.Add("a", 1)
	c.Get("a") // hit
	c.Get("a") // hit
	c.Get("b") // miss
	c.Peek("a")
	c.Contains("b")

	st := c.Stats()
	if st.Hits != 2 || st.Misses != 1 {
		t.Fatalf("Stats: hits %d misses %d, want 2/1", st.Hits, st.Misses)
	}
	var hits, misses uint64
	for _, ss := range st.Shards {
		hits += ss.Hits
		misses += ss.Misses
	}
	if hits != st.Hits || misses != st.Misses {
		t.Fatalf("per-shard totals %d/%d disagree with aggregate %d/%d", hits, misses, st.Hits, st.Misses)
	}
	if st.Len != 1 || st.Capacity != 16 {
		t.Fatalf("Stats: len %d capacity %d, want 1/16", st.Len, st.Capacity)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	c, err := NewStringWithEvict[int](1024, 32, func(string, int) {})
	if err != nil {
		t.Fatal(err)
	}

	const workers = 16
	const ops = 2000
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("key-%d", (id*ops+i)%512)
				switch i % 8 {
				case 0, 1, 2:
					c.Add(key, i)
				case 3, 4, 5:
					c.Get(key)
				case 6:
					c.Remove(key)
				case 7:
					c.Peek(key)
					c.Contains(key)
				}
			}
		}(w)
	}
	// Concurrent readers of aggregate state.
	stop := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-stop:
				return
			default:
				_ = c.Len()
				_ = c.Cap()
				_ = c.Stats()
			}
		}
	}()
	wg.Wait()
	close(stop)
	<-readerDone

	if c.Len() > c.Cap() {
		t.Fatalf("Len() = %d exceeds Cap() = %d after concurrent run", c.Len(), c.Cap())
	}
}

func TestConcurrentResize(t *testing.T) {
	c, err := NewString[int](512, 16)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				c.Add(fmt.Sprintf("key-%d", (id*500+i)%256), i)
				c.Get(fmt.Sprintf("key-%d", (id*500+i+1)%256))
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			c.Resize(64 + i*8)
		}
	}()
	wg.Wait()
	if c.Len() > c.Cap() {
		t.Fatalf("Len() = %d exceeds Cap() = %d after concurrent resize", c.Len(), c.Cap())
	}
}
