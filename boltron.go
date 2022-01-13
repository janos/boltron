// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package boltron provides type safe generic constructs to design data models
// using BoltDB embedded key value store.
//
// Definitions statically define encodings and options for other types to be
// serialized and they provide methods to access and modify serialized data
// within bolt transactions.
//
// There are three basic types with their definitions: Collection, Association
// and List.
//
// One complex types Collections, Associations and Lists provides methods to
// manage dynamically created collections and lists.
package boltron

import (
	"bytes"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

func deepBucket(tx *bolt.Tx, create bool, path ...[]byte) (*bolt.Bucket, error) {
	length := len(path)
	if length < 1 {
		return nil, fmt.Errorf("insufficient number of bucket path elements %d < 1", length)
	}
	bucket, err := rootBucket(tx, create, path[0])
	if err != nil {
		return nil, fmt.Errorf("create root bucket: %w", err)
	}
	if !create && bucket == nil {
		return nil, nil
	}
	for i := 1; i < length; i++ {
		var err error
		bucket, err = nestedBucket(bucket, create, path[i])
		if err != nil {
			return nil, fmt.Errorf("create %v nested bucket: %s", i, err)
		}
		if !create && bucket == nil {
			return nil, nil
		}
	}
	return bucket, nil
}

func rootBucket(tx *bolt.Tx, create bool, name []byte) (*bolt.Bucket, error) {
	root := tx.Bucket(name)
	if root != nil {
		return root, nil
	}
	if create {
		return tx.CreateBucket(name)
	}
	return nil, nil
}

func nestedBucket(b *bolt.Bucket, create bool, name []byte) (*bolt.Bucket, error) {
	nested := b.Bucket(name)
	if nested != nil {
		return nested, nil
	}
	if create {
		return b.CreateBucket(name)
	}
	return nil, nil
}

func bucketPath(s ...string) [][]byte {
	r := make([][]byte, 0, len(s))
	for _, e := range s {
		r = append(r, []byte(e))
	}
	return r
}

func iterateKeys[K any](bucket *bolt.Bucket, keyEncoding Encoding[K], start *K, reverse bool, f func(k, v []byte) (bool, error)) (next *K, err error) {
	var startKey []byte
	if start != nil {

		k, err := keyEncoding.Encode(*start)
		if err != nil {
			return nil, fmt.Errorf("encode start key: %w", err)
		}
		startKey = k
	}

	nextKey, _, err := iterate(bucket, startKey, reverse, f)
	if err != nil {
		return nil, err
	}

	if nextKey != nil {
		n, err := keyEncoding.Decode(nextKey)
		if err != nil {
			return nil, fmt.Errorf("decode start key: %w", err)
		}
		next = &n
	}

	return next, nil
}

func iterateList[V, O any](bucket *bolt.Bucket, valueEncoding Encoding[V], orderByEncoding Encoding[O], start *ListElement[V, O], reverse bool, f func(k, v []byte) (bool, error)) (next *ListElement[V, O], err error) {
	var startKey []byte
	if start != nil {

		v, err := valueEncoding.Encode(start.Value)
		if err != nil {
			return nil, fmt.Errorf("encode start value: %w", err)
		}

		o, err := orderByEncoding.Encode(start.OrderBy)
		if err != nil {
			return nil, fmt.Errorf("encode start order by: %w", err)
		}
		startKey = append(o, v...)
	}

	nextKey, nextValue, err := iterate(bucket, startKey, reverse, f)
	if err != nil {
		return nil, err
	}

	if nextKey != nil {
		value, err := valueEncoding.Decode(nextValue)
		if err != nil {
			return nil, fmt.Errorf("decode start key: %w", err)
		}
		nextOrderBy := nextKey[:len(nextKey)-len(nextValue)]
		orderBy, err := orderByEncoding.Decode(nextOrderBy)
		if err != nil {
			return nil, fmt.Errorf("decode start key: %w", err)
		}
		next = &ListElement[V, O]{
			Value:   value,
			OrderBy: orderBy,
		}
	}

	return next, nil
}

func iterate(bucket *bolt.Bucket, startKey []byte, reverse bool, f func(k, v []byte) (bool, error)) (nextKey, nextValue []byte, err error) {
	cursor := bucket.Cursor()

	var first, next func() (k, v []byte)
	if !reverse {
		first = cursor.First
		next = cursor.Next
	} else {
		first = cursor.Last
		next = cursor.Prev
	}

	var k, v []byte
	if startKey == nil {
		k, v = first()
	} else {
		k, v = cursor.Seek(startKey)
		if reverse && !bytes.Equal(k, startKey) {
			k, v = next()
		}
	}

	var count int
	for ; k != nil; k, v = next() {
		count++
		cont, err := f(k, v)
		if err != nil {
			return nil, nil, fmt.Errorf("element %v: %w", count, err)
		}
		if !cont {
			nextKey, nextValue = next()
			break
		}
	}

	return nextKey, nextValue, nil
}

func page[E any](bucket *bolt.Bucket, bucketOfBuckets bool, number, limit int, reverse bool, f func(k, v []byte) (E, error)) (s []E, totalElements, pages int, err error) {
	if number <= 0 {
		return nil, 0, 0, ErrInvalidPageNumber
	}
	if limit <= 0 {
		limit = 100
	}
	start := (number - 1) * limit
	end := number * limit

	if bucketOfBuckets {
		totalElements = bucket.Stats().BucketN - 1 // exclude the top bucket
	} else {
		totalElements = bucket.Stats().KeyN
	}
	pages = totalElements / limit
	if totalElements%limit != 0 {
		pages++
	}
	cursor := bucket.Cursor()
	var count int
	var last, prev func() (k, v []byte)
	if reverse {
		last = cursor.Last
		prev = cursor.Prev
	} else {
		last = cursor.First
		prev = cursor.Next
	}
	for k, v := last(); k != nil; k, v = prev() {
		count++
		if count <= start {
			continue
		}
		if count > end {
			break
		}

		e, err := f(k, v)
		if err != nil {
			return nil, 0, 0, err
		}

		s = append(s, e)
	}

	return s, totalElements, pages, err
}

func withDefaultError(v, d error) error {
	if v != nil {
		return v
	}
	return d
}
