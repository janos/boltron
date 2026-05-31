// Copyright (c) 2026, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build boltron_pagination_safe

package bboltext

import "go.etcd.io/bbolt"

// Skip moves the cursor forward or backward by n keys, returning the K/V pair.
// This is the safe, O(N) fallback implementation using standard cursor methods.
func Skip(c *bbolt.Cursor, n int, forward bool) (key []byte, value []byte) {
	if n <= 0 {
		return nil, nil
	}

	for i := 0; i < n; i++ {
		if forward {
			key, value = c.Next()
		} else {
			key, value = c.Prev()
		}

		// Break early if we hit the end of the bucket
		if key == nil {
			return nil, nil
		}
	}

	return key, value
}
