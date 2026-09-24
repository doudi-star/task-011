sharded
=======

`sharded` is a thread-safe, fixed-size, sharded LRU cache built on top of
the root `lru.Cache`. It exists for read-heavy, high-concurrency workloads
where a single cache-wide mutex becomes the bottleneck: keys are spread
across N shards and each shard has its own lock, so operations on keys in
different shards never contend.

```go
import (
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/hashicorp/golang-lru/v2/sharded"
)

c, _ := sharded.NewWithEvict[string, string](10000, 32, func(k, v string) {
	log.Printf("evicted %s", k)
})
c.Add("key", "value")
v, ok := c.Get("key")
```

Sharding and capacity rules
===========================

- **Shard assignment**: `shard = hash(key) % shards`. The default hash is
  FNV-1a (64-bit) over the key's bytes for strings, over the big-endian
  binary representation for bools and all sized ints/floats, and over
  `fmt.Sprintf("%#v", key)` for everything else. It is stable for the
  lifetime of the process. A custom hash can be supplied via
  `NewWithEvictAndHash`; it must be deterministic.
- **Capacity split**: with total `size` and `n` shards, every shard gets
  `base = size / n` entries, and the first `size % n` shards get one extra
  entry (`base+1`). The shard capacities therefore always sum to **exactly
  `size`** — the configured total is the real total, no rounding loss.
- **Minimum size**: every shard needs capacity >= 1, so `New*` returns an
  error when `size < shards` (or `shards <= 0`).
- **Resize**: `Resize(size)` keeps the shard count fixed (it never changes
  after construction) and redistributes `size` across the existing shards
  with the same base-plus-remainder rule, so capacities again sum to
  exactly `size`. If `size < shards` it is clamped up to `shards` (one
  entry per shard). Shrinking evicts per shard, oldest first, and returns
  the total number of evicted entries.

Concurrency model
=================

- There is **no cache-wide lock**. `Get`, `Add`, `Remove`, `Contains`,
  `Peek`, `ContainsOrAdd` and `PeekOrAdd` touch exactly one shard and hold
  only that shard's lock (via the root `lru.Cache`).
- `Purge`, `Len`, `Cap`, `Resize`, `Keys`, `Values` and `Stats` iterate
  all shards, locking one at a time. They never hold more than one shard
  lock, so they cannot deadlock against each other, but their results are
  point-in-time per shard, not an atomic snapshot of the whole cache.
- Hit/miss counters are per-shard atomics and add no locking.
- Eviction callbacks fire outside of any shard lock, matching the root
  package's behavior.

Observability
=============

`Stats()` returns the shard count, total/per-shard capacity and length,
and total/per-shard Get hit/miss counts. Only `Get` affects hit/miss
counters; `Contains` and `Peek` do not.

Semantics and explicit non-goals
================================

- **Per-shard eviction**: each shard evicts its own LRU entry
  independently. The cache as a whole is *not* a strict global LRU — a
  hot shard evicts its own oldest entry even if colder entries exist in
  other shards. With a uniform hash and `shards << size` this is
  statistically close to a global LRU.
- **No global ordering**: `Keys`/`Values` concatenate per-shard
  oldest-to-newest slices in shard order; there is no cross-shard recency
  order. `RemoveOldest`/`GetOldest` from the root package are
  intentionally not provided, since "oldest" is not defined across
  independent shards.
- **Fixed shard count**: the number of shards is set at construction and
  never changes; only capacity is redistributed by `Resize`. Changing the
  shard count would require rehashing every key and is out of scope.
- **Hash stability**: keys are not rehashed or moved between shards after
  insertion, so a `HashFunc` must be stable. The `%#v` fallback for
  exotic key types is for convenience, not speed — hot paths should use
  string/integer keys or supply a custom `HashFunc`.
