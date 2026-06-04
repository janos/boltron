// Copyright (c) 2026, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build !boltron_pagination_safe

package bboltext

import (
	"reflect"
	"unsafe"

	bolt "go.etcd.io/bbolt"
)

func init() {
	// checkFieldOffset panics if the named field's memory offset differs between
	// realType and mirrorType. This catches struct field reordering in bbolt
	// internals: a reordering that preserves total struct size would pass the
	// existing size check, but would place the wrong data under the unsafe pointer
	// cast, causing silent memory corruption. Only fields actually read or written
	// through the unsafe cast are checked.
	checkFieldOffset := func(realType, mirrorType reflect.Type, fieldName, context string) {
		realField, ok := realType.FieldByName(fieldName)
		if !ok {
			panic("boltron: " + context + "." + fieldName + " field is missing or renamed in bbolt")
		}
		mirrorField, ok := mirrorType.FieldByName(fieldName)
		if !ok {
			panic("boltron: " + context + "." + fieldName + " field is missing in mirror type")
		}
		if realField.Offset != mirrorField.Offset {
			panic("boltron: " + context + "." + fieldName + " field offset mismatch between bbolt and mirror type. Unsupported bbolt version.")
		}
	}

	// 1. Validate bbolt.Cursor layout.
	realCursorType := reflect.TypeFor[bolt.Cursor]()
	mirrorCursorType := reflect.TypeFor[cursorMirror]()

	if realCursorType.Size() != mirrorCursorType.Size() {
		panic("boltron: bbolt.Cursor memory layout size mismatch. Unsupported bbolt version.")
	}
	// Skip() reads mirror.stack — its offset must match.
	checkFieldOffset(realCursorType, mirrorCursorType, "stack", "bbolt.Cursor")

	// 2. Traverse down to real bbolt.elemRef via the "stack" field.
	stackField, ok := realCursorType.FieldByName("stack")
	if !ok {
		panic("boltron: bbolt.Cursor.stack field is missing or renamed")
	}
	realElemRefType := stackField.Type.Elem() // The type inside the slice []elemRef
	mirrorElemRefType := reflect.TypeFor[elemRefMirror]()

	if realElemRefType.Size() != mirrorElemRefType.Size() {
		panic("boltron: bbolt.elemRef memory layout size mismatch.")
	}
	// getCount() reads ref.node and ref.page; Skip() reads and writes ref.index.
	checkFieldOffset(realElemRefType, mirrorElemRefType, "page", "bbolt.elemRef")
	checkFieldOffset(realElemRefType, mirrorElemRefType, "node", "bbolt.elemRef")
	checkFieldOffset(realElemRefType, mirrorElemRefType, "index", "bbolt.elemRef")

	// 3. Traverse down to real bbolt.node via the "node" field in elemRef.
	nodeField, ok := realElemRefType.FieldByName("node")
	if !ok {
		panic("boltron: bbolt.elemRef.node field is missing or renamed")
	}
	realNodeType := nodeField.Type.Elem() // The type pointed to by *node
	mirrorNodeType := reflect.TypeFor[nodeMirror]()

	if realNodeType.Size() != mirrorNodeType.Size() {
		panic("boltron: bbolt.node memory layout size mismatch.")
	}
	// getCount() reads ref.node.inodes — its offset must match.
	checkFieldOffset(realNodeType, mirrorNodeType, "inodes", "bbolt.node")

	// 4. Traverse down to real bbolt.page via the "page" field in elemRef.
	pageField, ok := realElemRefType.FieldByName("page")
	if !ok {
		panic("bboltext: bbolt.elemRef.page field is missing or renamed")
	}
	realPageType := pageField.Type.Elem() // The type pointed to by *page
	mirrorPageType := reflect.TypeFor[pageMirror]()

	// v1.3.x layout is 24 bytes (includes ptr uintptr)
	// v1.5.x layout is 16 bytes (ptr uintptr removed for checkptr safety)
	if realPageType.Size() != 16 && realPageType.Size() != 24 {
		panic("bboltext: bbolt.page memory layout size mismatch. Expected 16 or 24 bytes.")
	}
	// getCount() reads ref.page.count — verify its offset. count follows id
	// (uint64, 8 bytes) and flags (uint16, 2 bytes), so it is stable at offset 10
	// across both the 16-byte (v1.4+) and 24-byte (v1.3) page layouts.
	checkFieldOffset(realPageType, mirrorPageType, "count", "bbolt.page")

	// 5. Traverse down to real bbolt.inode via the "inodes" field in node.
	inodesField, ok := realNodeType.FieldByName("inodes")
	if !ok {
		panic("boltron: bbolt.node.inodes field is missing or renamed")
	}
	realInodeType := inodesField.Type.Elem() // The type inside the slice []inode
	mirrorInodeType := reflect.TypeFor[inodeMirror]()

	if realInodeType.Size() != mirrorInodeType.Size() {
		panic("boltron: bbolt.inode memory layout size mismatch.")
	}
}

// Struct mirrors for bbolt internals
// These match the unexported memory layouts to allow unsafe manipulation.
// Go will automatically handle struct padding (e.g., 5 bytes after the bools in nodeMirror).

type cursorMirror struct {
	bucket unsafe.Pointer
	stack  []elemRefMirror
}

type elemRefMirror struct {
	page  *pageMirror
	node  *nodeMirror
	index int
}

type pageMirror struct {
	id       uint64 //nolint:unused
	flags    uint16 //nolint:unused
	count    uint16
	overflow uint32  //nolint:unused
	ptr      uintptr //nolint:unused
}

type inodeMirror struct {
	flags uint32 //nolint:unused
	pgid  uint64 //nolint:unused
	key   []byte //nolint:unused
	value []byte //nolint:unused
}

type nodeMirror struct {
	bucket     unsafe.Pointer   //nolint:unused
	isLeaf     bool             //nolint:unused
	unbalanced bool             //nolint:unused
	spilled    bool             //nolint:unused
	key        []byte           //nolint:unused
	pgid       uint64           //nolint:unused
	parent     unsafe.Pointer   //nolint:unused
	children   []unsafe.Pointer //nolint:unused
	inodes     []inodeMirror
}

// getCount safely extracts the element count of the current node or page in the cursor stack.
func getCount(ref *elemRefMirror) int {
	if ref.node != nil {
		return len(ref.node.inodes)
	}
	if ref.page != nil {
		return int(ref.page.count)
	}
	return 0
}

// Skip moves the cursor forward or backward by n keys, returning the K/V pair.
// It bypasses O(N) Next/Prev calls by jumping within leaf nodes using unsafe,
// using official Next/Prev only to safely cross page boundaries.
func Skip(c *bolt.Cursor, n int, forward bool) (key []byte, value []byte) {
	if n <= 0 {
		return nil, nil // Typically skip expects n > 0.
	}

	// Cast the cursor to our mirror struct to access the unexported stack.
	mirror := (*cursorMirror)(unsafe.Pointer(c))

	for n > 0 {
		if len(mirror.stack) == 0 {
			return nil, nil // Reached end of the bucket
		}

		// Look at the top of the stack (the current leaf node).
		top := &mirror.stack[len(mirror.stack)-1]
		count := getCount(top)

		if forward {
			remaining := count - 1 - top.index

			if remaining <= 0 {
				// Tightened fallback: walk boundaries quickly
				key, value = c.Next()
				n--
				if key == nil {
					return nil, nil
				}
				continue // Go back to check the new node's capacity
			}

			if n <= remaining {
				top.index += (n - 1)
				return c.Next()
			}

			top.index += remaining
			n -= remaining
			key, value = c.Next()
			n--
			if key == nil {
				return nil, nil
			}
		} else { // backward
			remaining := top.index

			if remaining <= 0 {
				// Already at the beginning of this node. Let bbolt handle boundary crossing safely.
				key, value = c.Prev()
				n--
				if key == nil {
					return nil, nil
				}
				continue
			}

			if n <= remaining {
				// Target is within current node.
				top.index -= (n - 1)
				return c.Prev()
			}

			// Move to the very beginning of the current node, then cross boundary.
			top.index -= remaining
			n -= remaining

			key, value = c.Prev()
			n--
			if key == nil {
				return nil, nil
			}
		}
	}

	return key, value
}
