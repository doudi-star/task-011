// Package sharded provides a thread-safe fixed size sharded LRU cache.
//
// Keys are mapped to one of N shards by a stable hash (FNV-1a by default,
// overridable via NewWithEvictAndHash). Each shard owns an independent
// lru.Cache with its own lock, so concurrent operations on keys that land
// in different shards never contend on the same mutex.
//
// Capacity split: the total size is divided across shards as
// base = size / shards, with the first (size % shards) shards receiving
// base+1 entries and the rest receiving base. The sum of shard capacities
// is therefore always exactly the configured size. Because every shard
// must hold at least one entry, size must be >= shards.
package sharded

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru/v2"
)

// HashFunc maps a key to a 64-bit hash. It must be stable: the same key
// must always produce the same hash for the lifetime of the cache.
type HashFunc[K comparable] func(key K) uint64

// DefaultHash is the HashFunc used when none is provided. It is FNV-1a
// (64-bit) over the key's bytes for string keys, over the big-endian
// binary representation for bool and all sized integer/float keys, and
// over fmt.Sprintf("%#v", key) for any other key type.
func DefaultHash[K comparable](key K) uint64 {
	h := fnv.New64a()
	switch k := any(key).(type) {
	case string:
		h.Write([]byte(k))
	case []byte:
		h.Write(k)
	case bool:
		var b [1]byte
		if k {
			b[0] = 1
		}
		h.Write(b[:])
	case int:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(k))
		h.Write(b[:])
	case int8:
		h.Write([]byte{byte(k)})
	case int16:
		var b [2]byte
		binary.BigEndian.PutUint16(b[:], uint16(k))
		h.Write(b[:])
	case int32:
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(k))
		h.Write(b[:])
	case int64:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(k))
		h.Write(b[:])
	case uint:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(k))
		h.Write(b[:])
	case uint8:
		h.Write([]byte{k})
	case uint16:
		var b [2]byte
		binary.BigEndian.PutUint16(b[:], k)
		h.Write(b[:])
	case uint32:
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], k)
		h.Write(b[:])
	case uint64:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], k)
		h.Write(b[:])
	case float32:
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], math.Float32bits(k))
		h.Write(b[:])
	case float64:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], math.Float64bits(k))
		h.Write(b[:])
	default:
		fmt.Fprintf(h, "%#v", key)
	}
	return h.Sum64()
}

// ShardStats reports observability data for a single shard.
type ShardStats struct {
	// Cap is the shard's current capacity.
	Cap int
	// Len is the number of items currently in the shard.
	Len int
	// Hits is the number of successful Get calls on the shard.
	Hits uint64
	// Misses is the number of Get calls on the shard that did not find
	// the key.
	Misses uint64
}

// Stats reports observability data for the whole cache.
type Stats struct {
	// Shards is the number of shards.
	Shards int
	// Cap is the total capacity (sum of shard capacities).
	Cap int
	// Len is the total number of items (sum of shard lengths).
	Len int
	// Hits is the total number of successful Get calls.
	Hits uint64
	// Misses is the total number of failed Get calls.
	Misses uint64
	// PerShard holds per-shard stats, indexed by shard number.
	PerShard []ShardStats
}

// shard is a single partition of the cache. All locking is handled by the
// embedded lru.Cache; hits/misses use atomics so reads never need an
// extra lock.
type shard[K comparable, V any] struct {
	cache  *lru.Cache[K, V]
	hits   atomic.Uint64
	misses atomic.Uint64
}

// Cache is a thread-safe fixed size sharded LRU cache. There is no
// cache-wide lock: every operation touches exactly one shard (except
// Purge, Len, Cap, Resize and Stats, which iterate all shards).
//
// Note that eviction is per-shard: the cache as a whole is not a strict
// global LRU, since each shard evicts its own least recently used entry
// independently.
type Cache[K comparable, V any] struct {
	shards []*shard[K, V]
	hash   HashFunc[K]
}

// New creates a sharded LRU of the given total size spread over the given
// number of shards, using DefaultHash. size must be >= shards and both
// must be positive.
func New[K comparable, V any](size, shards int) (*Cache[K, V], error) {
	return NewWithEvict[K, V](size, shards, nil)
}

// NewWithEvict constructs a sharded cache with the given eviction
// callback. The callback is invoked once per evicted entry, outside of
// any shard lock, with the same semantics as lru.NewWithEvict.
func NewWithEvict[K comparable, V any](size, shards int, onEvicted func(key K, value V)) (*Cache[K, V], error) {
	return NewWithEvictAndHash[K, V](size, shards, onEvicted, nil)
}

// NewWithEvictAndHash constructs a sharded cache with the given eviction
// callback and hash function. If hash is nil, DefaultHash is used.
func NewWithEvictAndHash[K comparable, V any](size, shards int, onEvicted func(key K, value V), hash HashFunc[K]) (*Cache[K, V], error) {
	if shards <= 0 {
		return nil, errors.New("sharded: must provide a positive shard count")
	}
	if size < shards {
		return nil, fmt.Errorf("sharded: size (%d) must be >= shards (%d)", size, shards)
	}
	if hash == nil {
		hash = DefaultHash[K]
	}
	c := &Cache[K, V]{
		shards: make([]*shard[K, V], shards),
		hash:   hash,
	}
	for i, cap := range shardCaps(size, shards) {
		sc, err := lru.NewWithEvict[K, V](cap, onEvicted)
		if err != nil {
			return nil, err
		}
		c.shards[i] = &shard[K, V]{cache: sc}
	}
	return c, nil
}

// shardCaps splits size across n shards: base = size / n for every shard,
// plus one extra entry for the first size % n shards. The result always
// sums to exactly size.
func shardCaps(size, n int) []int {
	caps := make([]int, n)
	base, rem := size/n, size%n
	for i := range caps {
		caps[i] = base
		if i < rem {
			caps[i]++
		}
	}
	return caps
}

func (c *Cache[K, V]) shardFor(key K) *shard[K, V] {
	return c.shards[c.hash(key)%uint64(len(c.shards))]
}

// Add adds a value to the cache. Returns true if an eviction occurred.
func (c *Cache[K, V]) Add(key K, value V) (evicted bool) {
	return c.shardFor(key).cache.Add(key, value)
}

// Get looks up a key's value from the cache, updating its recency. Hits
// and misses are counted per shard for Stats.
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

// Contains checks if a key is in the cache, without updating recency.
func (c *Cache[K, V]) Contains(key K) bool {
	return c.shardFor(key).cache.Contains(key)
}

// Peek returns the key's value (or undefined if not found) without
// updating the "recently used"-ness of the key.
func (c *Cache[K, V]) Peek(key K) (value V, ok bool) {
	return c.shardFor(key).cache.Peek(key)
}

// ContainsOrAdd checks if a key is in the cache without updating
// recency, and if not, adds the value. Returns whether the key was found
// and whether an eviction occurred.
func (c *Cache[K, V]) ContainsOrAdd(key K, value V) (ok, evicted bool) {
	return c.shardFor(key).cache.ContainsOrAdd(key, value)
}

// PeekOrAdd checks if a key is in the cache without updating recency,
// and if not, adds the value. Returns the existing value (if any),
// whether the key was found, and whether an eviction occurred.
func (c *Cache[K, V]) PeekOrAdd(key K, value V) (previous V, ok, evicted bool) {
	return c.shardFor(key).cache.PeekOrAdd(key, value)
}

// Remove removes the provided key from the cache, returning whether the
// key was contained.
func (c *Cache[K, V]) Remove(key K) (present bool) {
	return c.shardFor(key).cache.Remove(key)
}

// Purge clears all shards. Eviction callbacks, if registered, fire for
// every entry.
func (c *Cache[K, V]) Purge() {
	for _, s := range c.shards {
		s.cache.Purge()
	}
}

// Resize changes the total cache size, redistributing capacity across the
// existing shards with the same base-plus-remainder rule used at
// construction, so the shard capacities always sum to exactly size. The
// shard count itself is fixed for the lifetime of the cache. Returns the
// total number of evicted entries. size must be >= the shard count.
// If size < shards, it is clamped up to shards (one entry per shard).
func (c *Cache[K, V]) Resize(size int) (evicted int) {
	if size < len(c.shards) {
		size = len(c.shards)
	}
	caps := shardCaps(size, len(c.shards))
	for i, s := range c.shards {
		evicted += s.cache.Resize(caps[i])
	}
	return evicted
}

// Keys returns a slice of the keys in the cache, per shard from oldest to
// newest, concatenated in shard order. There is no global recency order
// across shards.
func (c *Cache[K, V]) Keys() []K {
	keys := make([]K, 0, c.Len())
	for _, s := range c.shards {
		keys = append(keys, s.cache.Keys()...)
	}
	return keys
}

// Values returns a slice of the values in the cache, per shard from
// oldest to newest, concatenated in shard order.
func (c *Cache[K, V]) Values() []V {
	values := make([]V, 0, c.Len())
	for _, s := range c.shards {
		values = append(values, s.cache.Values()...)
	}
	return values
}

// Len returns the total number of items in the cache.
func (c *Cache[K, V]) Len() int {
	var n int
	for _, s := range c.shards {
		n += s.cache.Len()
	}
	return n
}

// Cap returns the total capacity of the cache (the sum of shard
// capacities, which always equals the configured size).
func (c *Cache[K, V]) Cap() int {
	var n int
	for _, s := range c.shards {
		n += s.cache.Cap()
	}
	return n
}

// Stats returns a snapshot of shard count, capacities, lengths and
// hit/miss counters, both in aggregate and per shard.
func (c *Cache[K, V]) Stats() Stats {
	st := Stats{
		Shards:   len(c.shards),
		PerShard: make([]ShardStats, len(c.shards)),
	}
	for i, s := range c.shards {
		ss := ShardStats{
			Cap:    s.cache.Cap(),
			Len:    s.cache.Len(),
			Hits:   s.hits.Load(),
			Misses: s.misses.Load(),
		}
		st.PerShard[i] = ss
		st.Cap += ss.Cap
		st.Len += ss.Len
		st.Hits += ss.Hits
		st.Misses += ss.Misses
	}
	return st
}
