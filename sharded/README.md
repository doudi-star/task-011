# sharded

`sharded` is a drop-in companion to the root `lru.Cache` for read-heavy,
high-concurrency workloads. It spreads keys across a fixed number of
independent shards, each holding its own `lru.Cache` with its own lock, so
goroutines operating on keys in different shards never contend on the same
mutex.

```go
c, _ := sharded.NewString[any](1<<20, 64) // 1M entries across 64 shards
c.Add("key", value)
v, ok := c.Get("key")
```

For non-string keys, pass your own stable hash:

```go
c, _ := sharded.New[uint64, any](size, shards, func(k uint64) uint64 {
	return k * 0x9E3779B97F4A7C15 // any deterministic hash works
})
```

## Sharding and capacity rules

These rules are normative and will not change without a major version bump:

- **Shard assignment.** A key lives on shard `hash(key) % shardCount`. The
  hash function is supplied by the caller (`StringHash`, FNV-1a, for the
  string constructors) and must be deterministic.
- **Capacity split.** A total capacity of `size` over `n` shards gives every
  shard `size / n` (integer division) slots; the first `size % n` shards
  (indexes `0 .. size%n-1`) get one extra slot. The shard capacities
  therefore always sum to exactly `size` — the configured total is the real
  total, with no rounding slack.
- **Construction constraint.** `New`/`NewWithEvict` require
  `size >= shardCount` so every shard has capacity >= 1 (the underlying
  `lru.Cache` rejects size 0). Anything else returns an error.
- **Resize.** `Resize(size)` re-applies the same split rule to the new total
  and returns the total number of evicted entries across all shards. Unlike
  the constructor it accepts `size < shardCount`: shards assigned capacity 0
  hold nothing and immediately evict anything added to them. The shard count
  itself never changes after construction; there is no re-sharding.

## Concurrency model

- Each shard is a root `lru.Cache` guarded by its own `sync.RWMutex`. There
  is no cache-wide lock on the read/write path: `Get`, `Add`, `Remove`,
  `Contains` and `Peek` touch exactly one shard.
- `Len`, `Cap`, `Purge`, `Resize` and `Stats` iterate over all shards and
  take each shard's lock briefly in turn. They are safe to call concurrently
  with anything, but their results are assembled shard by shard and are not
  an atomic cross-shard snapshot.
- Hit/miss counters are `sync/atomic` values updated outside the shard lock.
- The eviction callback (if any) is invoked outside of any shard lock, as in
  the root package, and may be invoked concurrently from multiple goroutines.
  It must be thread-safe and must not call back into the same cache.

## Semantics inherited from the root cache

Within a shard, everything behaves exactly like `lru.Cache`: `Add` updates
recency and evicts the oldest entry on overflow, `Get` refreshes recency,
`Peek`/`Contains` do not, `Remove` and `Purge` fire the eviction callback,
and `Resize` evicts oldest-first. `Get`, `Add`, `Remove`, `Contains`,
`Peek`, `Purge`, `Len`, `Cap` and `Resize` have the same signatures and
return-value meanings as the root package.

## Observability

`Stats()` returns aggregate and per-shard statistics for reconciliation:

```go
st := c.Stats()
// st.ShardCount, st.Capacity, st.Len, st.Hits, st.Misses
// st.Shards[i].Capacity / .Len / .Hits / .Misses per shard
```

Hits and misses count `Get` calls only (`Peek`/`Contains` are deliberately
not counted, matching their "no side effects" role).

## Deliberate non-goals and edge cases

- **No global LRU ordering.** Eviction is per-shard. A hot shard evicts its
  own oldest entries even if other shards hold colder ones. With skewed key
  hashes, effective capacity utilization can be lower than the configured
  total.
- **No re-sharding.** The shard count is fixed at construction. To change
  it, build a new cache and re-populate; entries are not migrated.
- **No cross-shard operations.** `Keys`, `Values`, `RemoveOldest`,
  `GetOldest`, `ContainsOrAdd` and `PeekOrAdd` from the root package are not
  exposed, because their global semantics would either require a cache-wide
  lock or be misleading. Ask for them if you need them.
- **Hash quality is the caller's responsibility.** A poor hash concentrates
  load on a few shards and reintroduces the contention this package exists
  to remove. `StringHash` (FNV-1a) is a sensible default for string keys.
- **Zero-capacity shards after `Resize`.** Resizing below the shard count
  leaves some shards at capacity 0; adds to them evict immediately (and fire
  the eviction callback). This is supported but usually not what you want.
