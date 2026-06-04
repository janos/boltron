// Copyright (c) 2026, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bboltext

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

func TestSkip(t *testing.T) {
	const numKeys = 50000 // Large enough to guarantee multiple tree levels
	db, bucketName := setupDB(t, numKeys)

	// Helper to format expected keys
	expectedKey := func(i int) []byte {
		if i < 0 || i >= numKeys {
			return nil
		}
		return fmt.Appendf(nil, "key-%06d", i)
	}

	if err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)

		t.Run("Forward Large Jumps", func(t *testing.T) {
			c := b.Cursor()
			c.First()
			currentIndex := 0
			skipSize := 1234 // Arbitrary large jump

			for currentIndex < numKeys {
				k, _ := Skip(c, skipSize, true)
				currentIndex += skipSize

				expected := expectedKey(currentIndex)
				if !bytes.Equal(k, expected) {
					t.Fatalf("Failed at index %d. Expected %s, got %s", currentIndex, expected, k)
				}
				if k == nil {
					break
				}
			}
		})

		t.Run("Backward Large Jumps", func(t *testing.T) {
			c := b.Cursor()
			c.Last()
			currentIndex := numKeys - 1
			skipSize := 876

			for currentIndex >= 0 {
				k, _ := Skip(c, skipSize, false)
				currentIndex -= skipSize

				expected := expectedKey(currentIndex)
				if !bytes.Equal(k, expected) {
					t.Fatalf("Failed at index %d. Expected %s, got %s", currentIndex, expected, k)
				}
				if k == nil {
					break
				}
			}
		})

		t.Run("Randomized Forward and Backward", func(t *testing.T) {
			c := b.Cursor()
			c.First()
			currentIndex := 0
			r := rand.New(rand.NewSource(time.Now().UnixNano()))

			// Walk forward randomly
			for currentIndex < numKeys {
				step := r.Intn(500) + 1
				k, _ := Skip(c, step, true)
				currentIndex += step

				expected := expectedKey(currentIndex)
				if !bytes.Equal(k, expected) {
					t.Fatalf("Forward Random failed at index %d. Expected %s, got %s", currentIndex, expected, k)
				}
				if k == nil {
					break
				}
			}

			// Walk back randomly
			c.Last()
			currentIndex = numKeys - 1
			for currentIndex >= 0 {
				step := r.Intn(500) + 1
				k, _ := Skip(c, step, false)
				currentIndex -= step

				expected := expectedKey(currentIndex)
				if !bytes.Equal(k, expected) {
					t.Fatalf("Backward Random failed at index %d. Expected %s, got %s", currentIndex, expected, k)
				}
				if k == nil {
					break
				}
			}
		})

		t.Run("Mixed Next Prev and Skip", func(t *testing.T) {
			c := b.Cursor()
			c.First()

			// Track our logical position manually
			currentIndex := 0

			// 1. Move forward with Next a few times
			for range 3 {
				c.Next()
				currentIndex++
			}

			// 2. Skip forward
			k, _ := Skip(c, 50, true)
			currentIndex += 50
			if expected := expectedKey(currentIndex); !bytes.Equal(k, expected) {
				t.Fatalf("After Skip forward, expected %s, got %s", expected, k)
			}

			// 3. Move backward with Prev
			for range 5 {
				c.Prev()
				currentIndex--
			}

			// 4. Skip backward
			k, _ = Skip(c, 25, false)
			currentIndex -= 25
			if expected := expectedKey(currentIndex); !bytes.Equal(k, expected) {
				t.Fatalf("After Skip backward, expected %s, got %s", expected, k)
			}

			// 5. One more standard Next to ensure stack state isn't corrupted
			k, _ = c.Next()
			currentIndex++
			if expected := expectedKey(currentIndex); !bytes.Equal(k, expected) {
				t.Fatalf("After final Next, expected %s, got %s", expected, k)
			}
		})

		t.Run("Out of Bounds Handling", func(t *testing.T) {
			c := b.Cursor()

			// Try to skip past the end
			c.First()
			k, _ := Skip(c, numKeys+100, true)
			if k != nil {
				t.Fatalf("Expected nil when skipping past the end, got %s", k)
			}

			// Try to skip past the beginning
			c.Last()
			k, _ = Skip(c, numKeys+100, false)
			if k != nil {
				t.Fatalf("Expected nil when skipping past the beginning, got %s", k)
			}
		})

		return nil
	}); err != nil {
		t.Fatalf("db.View failed: %v", err)
	}
}

// setupDB creates a temporary database and populates it with a specified number of keys.
func setupDB(t *testing.T, numKeys int) (*bbolt.DB, []byte) {
	t.Helper()

	f, err := os.CreateTemp("", "bolt-skip-detailed-test-*.db")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := f.Name()
	f.Close()

	// Use a small page size to force a deeper B-tree and more node splits
	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{PageSize: 4096})
	if err != nil {
		os.Remove(dbPath)
		t.Fatal(err)
	}

	bucketName := []byte("large_bucket")

	err = db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucket(bucketName)
		if err != nil {
			return err
		}
		for i := range numKeys {
			k := fmt.Appendf(nil, "key-%06d", i)
			v := fmt.Appendf(nil, "val-%06d", i)
			if err := b.Put(k, v); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		db.Close()
		os.Remove(dbPath)
		t.Fatal(err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(dbPath)
	}

	t.Cleanup(cleanup)

	return db, bucketName
}
