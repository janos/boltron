// Copyright (c) 2026, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bboltext

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"go.etcd.io/bbolt"
)

func BenchmarkSkip(b *testing.B) {
	// Define the topological extremes requested
	scenarios := []struct {
		name    string
		numKeys int
		keySize int
		valSize int
	}{
		{"SmallDB_SmallKV", 10000, 16, 16},     // Standard small records
		{"SmallDB_LargeKey", 10000, 256, 16},   // Large keys force shallower trees but wider pages
		{"SmallDB_LargeVal", 10000, 16, 4096},  // Large values force many page allocations/overflows
		{"LargeDB_SmallKV", 100000, 16, 16},    // Deep tree structure, many branch nodes
		{"LargeDB_LargeVal", 100000, 16, 4096}, // Massive DB size, heavy disk/memory mapping impact
	}

	skipDistances := []int{100, 1000, 5000}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			db, bucketName, cleanup := setupBenchDB(b, scenario.numKeys, scenario.keySize, scenario.valSize)
			defer cleanup()

			if err := db.View(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(bucketName)
				c := bucket.Cursor()

				for _, skipN := range skipDistances {
					// We can't skip more than the DB holds in a meaningful way
					if skipN >= scenario.numKeys {
						continue
					}

					benchName := fmt.Sprintf("Skip_%d", skipN)

					b.Run(benchName, func(b *testing.B) {
						b.Run("Native_Next", func(b *testing.B) {
							b.ResetTimer()
							b.ReportAllocs()
							for i := 0; i < b.N; i++ {
								c.First() // Reset position for realistic pagination baseline
								for range skipN {
									c.Next()
								}
							}
						})

						b.Run("Optimized", func(b *testing.B) {
							b.ResetTimer()
							b.ReportAllocs()
							for i := 0; i < b.N; i++ {
								c.First() // Reset position
								Skip(c, skipN, true)
							}
						})
					})
				}
				return nil
			}); err != nil {
				b.Fatalf("db.View failed: %v", err)
			}
		})
	}
}

// setupBenchDB creates a populated database tailored for the benchmark constraints.
func setupBenchDB(b *testing.B, numKeys, keySize, valSize int) (*bbolt.DB, []byte, func()) {
	b.Helper()

	f, err := os.CreateTemp("", "bolt-skip-bench-*.db")
	if err != nil {
		b.Fatal(err)
	}
	dbPath := f.Name()
	f.Close()

	// Standard page size for realistic benchmarks
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		os.Remove(dbPath)
		b.Fatal(err)
	}

	bucketName := []byte("bench_bucket")
	valPad := bytes.Repeat([]byte("v"), valSize)

	err = db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucket(bucketName)
		if err != nil {
			return err
		}

		for i := range numKeys {
			// Ensure keys are lexicographically ordered and padded to keySize
			kStr := fmt.Sprintf("%010d", i)
			if len(kStr) < keySize {
				kStr += string(bytes.Repeat([]byte("k"), keySize-len(kStr)))
			}

			if err := bucket.Put([]byte(kStr), valPad); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		db.Close()
		os.Remove(dbPath)
		b.Fatal(err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(dbPath)
	}

	return db, bucketName, cleanup
}
