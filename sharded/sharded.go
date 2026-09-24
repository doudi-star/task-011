// Copyright IBM Corp. 2014, 2025
// SPDX-License-Identifier: MPL-2.0

// Package sharded provides a thread-safe fixed size LRU cache whose entries
// are spread across a fixed number of independent shards. Each shard owns a
// separate lru.Cache guarded by its own lock, so operations on keys that map
// to different shards never contend with each other.
//
// Sharding and capacity rules (normative):
//
//   - The shard for a key is hash(key) % shardCount. The shard count is fixed
//     at construction and never changes for the lifetime of the cache.
//   - The total capacity is split deterministically: every shard gets
//     size/shardCount (integer division), and the first size%shardCount
//     shards (indexes 0..size%shardCount-1) get one extra slot. The sum of
//     the shard capacities therefore always equals the configured size
//     exactly.
//   - Construction requires size >= shardCount so that every shard has a
//     capacity of at least 1 (the underlying lru.Cache rejects size 0).
//   - Resize applies the same split rule to the new total. Unlike the
//     constructor it accepts sizes smaller than the shard count; shards that
//     are assigned capacity 0 hold nothing and immediately evict anything
//     added to them.
//
// Eviction, recency and callback semantics within a shard are exactly those
// of the root lru.Cache. There is no cross-shard eviction ordering: the cache
// as a whole is "per-shard LRU", not a global LRU.
package sharded

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru/v2"
)

// HashFunc computes a stable hash for a key. It must be deterministic: the
// same key must always produce the same hash for the lifetime of a Cache.
type HashFunc[K comparable] func(K) uint64

// StringHash hashes a string with FNV-1a (64 bit). It is the hash used by
// NewString and NewStringWithEvict.
func StringHash(s string) uint64 {
	h := fnv.New64a()
	// hash.Hash never returns an error from Write.
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// shard is a single partition of the cache.
type shard[K comparable, V any] struct {
	cache  *lru.Cache[K, V]
	hits   atomic.Uint64
	misses atomic.Uint64
}

// Cache is a thread-safe sharded LRU cache. The zero value is not usable;
// construct it with New, NewWithEvict, NewString or NewStringWithEvict.
type Cache[K comparable, V any] struct {
	shards []*shard[K, V]
	hash   HashFunc[K]
}

// New creates a sharded LRU cache with the given total capacity and shard
// count. See the package documentation for the exact capacity split rules.
func New[K comparable, V any](size, shardCount int, hash HashFunc[K]) (*Cache[K, V], error) {
	return NewWithEvict[K, V](size, shardCount, hash, nil)
}

// NewWithEvict constructs a sharded LRU cache with the given eviction
// callback. The callback is invoked once per evicted entry, outside of any
// shard lock, and may be invoked concurrently from multiple goroutines.
func NewWithEvict[K comparable, V any](size, shardCount int, hash HashFunc[K], onEvicted func(key K, value V)) (*Cache[K, V], error) {
	if shardCount <= 0 {
		return nil, errors.New("sharded: shard count must be positive")
	}
	if hash == nil {
		return nil, errors.New("sharded: hash function must not be nil")
	}
	if size < shardCount {
		return nil, fmt.Errorf("sharded: size (%d) must be >= shard count (%d) so every shard has capacity >= 1", size, shardCount)
	}

	c := &Cache[K, V]{
		shards: make([]*shard[K, V], shardCount),
		hash:   hash,
	}
	for i, shardCap := range splitCapacity(size, shardCount) {
		sc, err := lru.NewWithEvict[K, V](shardCap, onEvicted)
		if err != nil {
			return nil, err
		}
		c.shards[i] = &shard[K, V]{cache: sc}
	}
	return c, nil
}

// NewString creates a sharded LRU cache with string keys, using FNV-1a
// (StringHash) to assign keys to shards.
func NewString[V any](size, shardCount int) (*Cache[string, V], error) {
	return New[string, V](size, shardCount, StringHash)
}

// NewStringWithEvict constructs a string-keyed sharded LRU cache with the
// given eviction callback, using FNV-1a (StringHash) to assign keys to
// shards.
func NewStringWithEvict[V any](size, shardCount int, onEvicted func(key string, value V)) (*Cache[string, V], error) {
	return NewWithEvict[string, V](size, shardCount, StringHash, onEvicted)
}

// splitCapacity distributes size across shardCount shards: every shard gets
// size/shardCount, and the first size%shardCount shards get one extra slot.
// The result always sums to size.
func splitCapacity(size, shardCount int) []int {
	base := size / shardCount
	rem := size % shardCount
	caps := make([]int, shardCount)
	for i := range caps {
		caps[i] = base
		if i < rem {
			caps[i]++
		}
	}
	return caps
}

// shardFor returns the shard responsible for key.
func (c *Cache[K, V]) shardFor(key K) *shard[K, V] {
	return c.shards[c.hash(key)%uint64(len(c.shards))]
}

// ShardCount returns the number of shards in the cache.
func (c *Cache[K, V]) ShardCount() int {
	return len(c.shards)
}

// Add adds a value to the cache. Returns true if an eviction occurred.
func (c *Cache[K, V]) Add(key K, value V) (evicted bool) {
	return c.shardFor(key).cache.Add(key, value)
}

// Get looks up a key's value from the cache, updating its recency, and
// records a hit or a miss in the shard's statistics.
func (c *Cache[K, V]) Get(key K) (value V, ok bool) {
	s := c.shardFor(key)
	value, ok = s.cache.Get(key)
	if ok {
		s.hits.Add(1)
	} else {
		s.misses.Add(1)
	}
	return value, ok
}

// Contains checks if a key is in the cache, without updating the
// recent-ness or deleting it for being stale.
func (c *Cache[K, V]) Contains(key K) bool {
	return c.shardFor(key).cache.Contains(key)
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *Cache[K, V]) Peek(key K) (value V, ok bool) {
	return c.shardFor(key).cache.Peek(key)
}

// Remove removes the provided key from the cache.
func (c *Cache[K, V]) Remove(key K) (present bool) {
	return c.shardFor(key).cache.Remove(key)
}

// Purge is used to completely clear the cache.
func (c *Cache[K, V]) Purge() {
	for _, s := range c.shards {
		s.cache.Purge()
	}
}

// Resize changes the total cache size, redistributing it across the shards
// with the same split rule used at construction. It returns the total
// number of evicted entries across all shards.
func (c *Cache[K, V]) Resize(size int) (evicted int) {
	for i, shardCap := range splitCapacity(size, len(c.shards)) {
		evicted += c.shards[i].cache.Resize(shardCap)
	}
	return evicted
}

// Len returns the total number of items in the cache.
func (c *Cache[K, V]) Len() int {
	var n int
	for _, s := range c.shards {
		n += s.cache.Len()
	}
	return n
}

// Cap returns the total capacity of the cache, i.e. the sum of the shard
// capacities.
func (c *Cache[K, V]) Cap() int {
	var n int
	for _, s := range c.shards {
		n += s.cache.Cap()
	}
	return n
}

// ShardStats reports the capacity, occupancy and hit/miss counters of a
// single shard.
type ShardStats struct {
	Capacity int
	Len      int
	Hits     uint64
	Misses   uint64
}

// Stats reports aggregate and per-shard statistics. Hits and Misses count
// Get calls only; the per-shard slice is indexed by shard number.
type Stats struct {
	ShardCount int
	Capacity   int
	Len        int
	Hits       uint64
	Misses     uint64
	Shards     []ShardStats
}

// Stats returns a snapshot of the cache's statistics. The snapshot is
// assembled shard by shard and is not atomic across shards.
func (c *Cache[K, V]) Stats() Stats {
	st := Stats{
		ShardCount: len(c.shards),
		Shards:     make([]ShardStats, len(c.shards)),
	}
	for i, s := range c.shards {
		ss := ShardStats{
			Capacity: s.cache.Cap(),
			Len:      s.cache.Len(),
			Hits:     s.hits.Load(),
			Misses:   s.misses.Load(),
		}
		st.Shards[i] = ss
		st.Capacity += ss.Capacity
		st.Len += ss.Len
		st.Hits += ss.Hits
		st.Misses += ss.Misses
	}
	return st
}
