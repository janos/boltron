# bboltext: Optimized Cursor Skipping for bbolt

`bboltext` provides an experimental, highly optimized cursor skipping function for [go.etcd.io/bbolt](https://github.com/etcd-io/bbolt). It allows you to efficiently skip `N` keys forward or backward during cursor iteration without incurring the full **O(N)** performance penalty of calling `Cursor.Next()` or `Cursor.Prev()` repeatedly.

## The Problem: The Cost of Pagination

In standard `bbolt`, paginating through large buckets or skipping a specific number of records requires a loop:

```go
// Naive, O(N) skipping
for i := 0; i < 5000; i++ {
    c.Next()
}

```

Because `bbolt`'s internal B-tree branch nodes do not store the total number of keys in their subtrees, true O(1) skipping across the entire database is mathematically impossible without altering the fundamental disk format. Consequently, `bbolt` must traverse the tree and materialize keys step-by-step, involving stack manipulation and memory overhead on every iteration. For large offsets, this becomes a significant performance bottleneck.

## The Solution: Unsafe Node Jumping

To achieve the best possible performance without modifying bbolt's core code, `bboltext` uses Go's `unsafe` package to directly access the unexported internal B-tree stack of the `bbolt.Cursor`.

Because B-tree leaf nodes contain many keys sequentially, the vast majority of cursor steps happen *within* a single node. The `Skip` function takes advantage of this by:

1. **Peeking at the Cursor Stack:** Using structurally mirrored types to read bbolt's internal state.
2. **Fast-Forwarding Within Leaves:** If the target skip destination lands within the current leaf node, we mathematically jump the index forward/backward in **O(1)** time.
3. **Safe Boundary Crossing:** When a skip crosses a node or page boundary, we fast-forward to the very edge of the node and allow bbolt's native `Next()` or `Prev()` to handle the complex B-tree traversal safely.

## Usage

The API provides a single, straightforward function: `Skip(c *bbolt.Cursor, n int, forward bool)`.

```go
package main

import (
    "fmt"
    "log"

    "go.etcd.io/bbolt"
    "resenje.org/boltron/internal/bboltext"
)

func main() {
    db, err := bbolt.Open("my.db", 0600, nil)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    db.View(func(tx *bbolt.Tx) error {
        b := tx.Bucket([]byte("MyBucket"))
        if b == nil {
            return nil
        }

        c := b.Cursor()
        c.First()

        // Skip 5000 keys forward
        k, v := bboltext.Skip(c, 5000, true)
        if k != nil {
            fmt.Printf("Landed on key: %s\n", k)
        }

        return nil
    })
}

```

---

## Safe Mode (Build Flag Fallback)

If you are running in a strict security environment, compiling for an unsupported architecture, or wish to completely avoid `unsafe` memory manipulation, you can compile your application using the safe fallback flag:

```bash
go build -tags boltron_pagination_safe .

```

When built with the `boltron_pagination_safe` tag, the package completely strips out the `unsafe` and `reflect` logic, falling back to a clean, dependency-free **O(N)** loop using standard `c.Next()` and `c.Prev()` methods. The API signature remains exactly the same.

---

## Benchmarks & Performance

The performance of `bboltext` is heavily tied to how well your data packs into `bbolt`'s physical pages. When multiple records fit into a single B-tree node, the $O(1)$ jump logic shines.

Here is how the optimized `Skip` function performs on an out-of-the-box configuration (16 KB OS Page Size) for a **5,000-key skip**:

| Data Profile | Native `Next` (ns/op) | Optimized `Skip` (ns/op) | Performance Gain |
| --- | --- | --- | --- |
| **Small Keys & Values** | 35,103 | 517 | **~67x Faster** |
| **Large Keys** | 37,707 | 3,332 | **~11x Faster** |
| **Large Values (4 KB)** | 109,061 | 165,186 | **~34% Slower** |

### The "Large Value" Constraint & Page Size

Notice the performance regression on Large Values. This exposes a fundamental rule of B-trees: **If a single key/value pair is larger than the database's `PageSize`, bbolt is forced to allocate exactly 1 record per leaf node.**

When this happens, there is nothing for the optimized function to "skip" over within the node. It immediately falls back to `c.Next()`, effectively becoming an O(N) loop with a tiny bit of `unsafe` checking overhead on top.

**The Fix:** If your application frequently stores large payloads (e.g., 4 KB JSON documents) and requires heavy pagination, you should tune bbolt's `PageSize` upon opening the database to accommodate multiple large records per node:

```go
db, err := bbolt.Open("my.db", 0600, &bbolt.Options{
    PageSize: 32768, // 32 KB
})

```

By allowing bbolt to pack multiple large values into a 32 KB leaf node, the optimized `Skip` logic engages again, returning the performance to **faster than native** speeds.

---

## Safety and Upstream Compatibility

Because this package relies on the exact memory layout of bbolt's internal, unexported structs, it validates its layout dynamically at runtime.

The package includes an `init()` function that uses the `reflect` package to zero-allocation inspect bbolt's struct sizes and field names at startup. If you upgrade `go.etcd.io/bbolt` to a future version that alters these internal structs, the application will **panic safely on startup** rather than risking silent memory corruption in production.