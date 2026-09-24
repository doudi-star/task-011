golang-lru
==========

This provides the `lru` package which implements a fixed-size
thread safe LRU cache. It is based on the cache in Groupcache.

Documentation
=============

Full docs are available on [Go Packages](https://pkg.go.dev/github.com/hashicorp/golang-lru/v2)

LRU cache example
=================

```go
package main

import (
	"fmt"
	"github.com/hashicorp/golang-lru/v2"
)

func main() {
	l, _ := lru.New[int, any](128)
	for i := 0; i < 256; i++ {
		l.Add(i, nil)
	}
	if l.Len() != 128 {
		panic(fmt.Sprintf("bad len: %v", l.Len()))
	}
}
```

Expirable LRU cache example
===========================

```go
package main

import (
	"fmt"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

func main() {
	// make cache with 10ms TTL and 5 max keys
	cache := expirable.NewLRU[string, string](5, nil, time.Millisecond*10)


	// set value under key1.
	cache.Add("key1", "val1")

	// get value under key1
	r, ok := cache.Get("key1")

	// check for OK value
	if ok {
		fmt.Printf("value before expiration is found: %v, value: %q\n", ok, r)
	}

	// wait for cache to expire
	time.Sleep(time.Millisecond * 12)

	// get value under key1 after key expiration
	r, ok = cache.Get("key1")
	fmt.Printf("value after expiration is found: %v, value: %q\n", ok, r)

	// set value under key2, would evict old entry because it is already expired.
	cache.Add("key2", "val2")

	fmt.Printf("Cache len: %d\n", cache.Len())
	// Output:
	// value before expiration is found: true, value: "val1"
	// value after expiration is found: false, value: ""
	// Cache len: 1
}
```

Sharded LRU cache example
=========================

For read-heavy, high-concurrency workloads, the `sharded` package spreads
keys across a fixed number of independent `lru.Cache` shards, each with its
own lock, so goroutines no longer contend on a single mutex. The total
capacity is split deterministically across shards (every shard gets
`size/shards`, the first `size%shards` shards get one extra slot), so the
shard capacities always sum to exactly the configured size. See
[sharded/README.md](sharded/README.md) for the full sharding, capacity and
concurrency rules.

```go
package main

import (
	"fmt"

	"github.com/hashicorp/golang-lru/v2/sharded"
)

func main() {
	// 1024 entries spread across 16 shards
	c, _ := sharded.NewString[int](1024, 16)
	for i := 0; i < 2048; i++ {
		c.Add(fmt.Sprintf("key-%d", i), i)
	}
	if c.Len() > 1024 {
		panic(fmt.Sprintf("bad len: %v", c.Len()))
	}
	st := c.Stats()
	fmt.Printf("shards: %d, capacity: %d, len: %d\n", st.ShardCount, st.Capacity, st.Len)
}
```
