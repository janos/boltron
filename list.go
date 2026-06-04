// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron

import (
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// List defines a list of values, ordered by the provided order type.
// List values are unique, but the order by values are not. If the order is
// defined by the values encoding, or it is not important, order by encoding
// should be set to NullEncoding.
type List[V, O any] struct {
	bucketPath       [][]byte
	bucketPathIndex  [][]byte
	valueEncoding    Encoding[V]
	orderByEncoding  Encoding[O]
	fillPercent      float64
	errValueNotFound error
	addCallback      func(value, orderBy []byte) error // used by Lists
	removeCallback   func(value, orderBy []byte) error // used by Lists
}

// ListOptions provides additional configuration for a List.
type ListOptions struct {
	// FillPercent is the value for the bolt bucket fill percent.
	FillPercent float64
	// ErrValueNotFound is returned if the value is not found.
	ErrValueNotFound error
}

// NewList constructs a new List with a unique name and key
// and order by encodings.
func NewList[V, O any](
	name string,
	valueEncoding Encoding[V],
	orderByEncoding Encoding[O],
	o *ListOptions,
) *List[V, O] {
	if o == nil {
		o = new(ListOptions)
	}
	return &List[V, O]{
		bucketPath:       bucketPath("boltron: list: " + name + " values"),
		bucketPathIndex:  bucketPath("boltron: list: " + name + " index"),
		valueEncoding:    valueEncoding,
		orderByEncoding:  orderByEncoding,
		fillPercent:      o.FillPercent,
		errValueNotFound: withDefaultError(o.ErrValueNotFound, ErrNotFound),
	}
}

// Tx returns an List transaction with access to the stored data
// through the bolt transaction.
func (d *List[V, O]) Tx(tx *bolt.Tx) *ListTx[V, O] {
	return &ListTx[V, O]{
		tx:   tx,
		list: d,
	}
}

// txFromBuckets returns a ListTx whose list and index buckets are already
// resolved. This avoids the need to call Tx(nil) and then manually inject bucket
// caches, which would leave a nil bolt.Tx that panics if any code path opens a
// new bucket.
func (d *List[V, O]) txFromBuckets(listBucket, indexBucket *bolt.Bucket) *ListTx[V, O] {
	return &ListTx[V, O]{
		list:             d,
		listBucketCache:  listBucket,
		indexBucketCache: indexBucket,
	}
}

// ListTx provides methods to access and change ordered list of values.
type ListTx[V, O any] struct {
	tx               *bolt.Tx
	listBucketCache  *bolt.Bucket
	indexBucketCache *bolt.Bucket
	list             *List[V, O]
}

func (l *ListTx[V, O]) listBucket(create bool) (*bolt.Bucket, error) {
	if l.listBucketCache != nil {
		return l.listBucketCache, nil
	}
	bucket, err := deepBucket(l.tx, create, l.list.bucketPath...)
	if err != nil {
		return nil, err
	}
	if l.list.fillPercent > 0 && bucket != nil {
		bucket.FillPercent = l.list.fillPercent
	}
	l.listBucketCache = bucket
	return bucket, nil
}

func (l *ListTx[V, O]) indexBucket(create bool) (*bolt.Bucket, error) {
	if l.indexBucketCache != nil {
		return l.indexBucketCache, nil
	}
	bucket, err := deepBucket(l.tx, create, l.list.bucketPathIndex...)
	if err != nil {
		return nil, err
	}
	if l.list.fillPercent > 0 && bucket != nil {
		bucket.FillPercent = l.list.fillPercent
	}
	l.indexBucketCache = bucket
	return bucket, nil
}

// Has returns true if the value already exists in the database.
func (o *ListTx[V, O]) Has(value V) (bool, error) {
	v, err := o.list.valueEncoding.Encode(value)
	if err != nil {
		return false, fmt.Errorf("encode value: %w", err)
	}
	indexBucket, err := o.indexBucket(false)
	if err != nil {
		return false, fmt.Errorf("index bucket: %w", err)
	}
	if indexBucket == nil {
		return false, nil
	}
	return indexBucket.Get(v) != nil, nil
}

// OrderBy returns the saved order by instance for the provided value.
func (l *ListTx[V, O]) OrderBy(value V) (orderBy O, err error) {
	v, err := l.list.valueEncoding.Encode(value)
	if err != nil {
		return orderBy, fmt.Errorf("encode value: %w", err)
	}

	indexBucket, err := l.indexBucket(false)
	if err != nil {
		return orderBy, fmt.Errorf("index bucket: %w", err)
	}
	if indexBucket == nil {
		return orderBy, l.list.errValueNotFound
	}

	o := indexBucket.Get(v)
	if o == nil {
		return orderBy, l.list.errValueNotFound
	}

	orderBy, err = l.list.orderByEncoding.Decode(o)
	if err != nil {
		return orderBy, fmt.Errorf("decode order by: %w", err)
	}

	return orderBy, nil
}

// Add adds a value to the list with an order by instance.
func (l *ListTx[V, O]) Add(value V, orderBy O) error {
	v, err := l.list.valueEncoding.Encode(value)
	if err != nil {
		return fmt.Errorf("encode value: %w", err)
	}
	o, err := l.list.orderByEncoding.Encode(orderBy)
	if err != nil {
		return fmt.Errorf("encode order by: %w", err)
	}

	indexBucket, err := l.indexBucket(true)
	if err != nil {
		return fmt.Errorf("index bucket: %w", err)
	}

	listBucket, err := l.listBucket(true)
	if err != nil {
		return fmt.Errorf("list bucket: %w", err)
	}

	if previous := indexBucket.Get(v); previous != nil {
		previousValue := append(previous, v...)
		// ensure the deletion for data consistency
		if listBucket.Get(previousValue) == nil {
			return errors.New("previous value not found")
		}
		if err := listBucket.Delete(previousValue); err != nil {
			return fmt.Errorf("delete previous value: %w", err)
		}
	}

	if err := listBucket.Put(append(o, v...), v); err != nil {
		return fmt.Errorf("put to list bucket: %w", err)
	}
	if err := indexBucket.Put(v, o); err != nil {
		return fmt.Errorf("put to index bucket: %w", err)
	}

	if l.list.addCallback != nil {
		if err := l.list.addCallback(v, o); err != nil {
			return fmt.Errorf("add callback: %w", err)
		}
	}

	return nil
}

// Remove removes the value and its associated order by from the database. If
// ensure flag is set to true and the value does not exist, ErrNotFound is
// returned.
func (l *ListTx[V, O]) Remove(value V, ensure bool) error {
	v, err := l.list.valueEncoding.Encode(value)
	if err != nil {
		return fmt.Errorf("encode value: %w", err)
	}

	indexBucket, err := l.indexBucket(false)
	if err != nil {
		return fmt.Errorf("index bucket: %w", err)
	}

	if indexBucket == nil {
		if ensure {
			return l.list.errValueNotFound
		}
		return nil
	}

	o := indexBucket.Get(v)
	if o == nil {
		if ensure {
			return l.list.errValueNotFound
		}
		return nil
	}

	listBucket, err := l.listBucket(false)
	if err != nil {
		return fmt.Errorf("list bucket: %w", err)
	}

	if listBucket == nil {
		if ensure {
			return l.list.errValueNotFound
		}
		return nil
	}

	if err := listBucket.Delete(append(o, v...)); err != nil {
		return fmt.Errorf("delete from list bucket: %w", err)
	}
	if err := indexBucket.Delete(v); err != nil {
		return fmt.Errorf("delete from index bucket: %w", err)
	}

	if l.list.removeCallback != nil {
		if err := l.list.removeCallback(v, o); err != nil {
			return fmt.Errorf("remove callback: %w", err)
		}
	}

	return nil
}

// Iterate iterates over keys and values in the lexicographical order of keys.
// If the callback function f returns false, the iteration stops and the next
// can be used to continue the iteration.
func (l *ListTx[V, O]) Iterate(start *ListElement[V, O], reverse bool, f func(V, O) (bool, error)) (next *ListElement[V, O], err error) {
	listBucket, err := l.listBucket(false)
	if err != nil {
		return nil, fmt.Errorf("list bucket: %w", err)
	}
	if listBucket == nil {
		return nil, nil
	}
	return iterateList(listBucket, l.list.valueEncoding, l.list.orderByEncoding, start, reverse, func(ov, v []byte) (bool, error) {
		value, err := l.list.valueEncoding.Decode(v)
		if err != nil {
			return false, fmt.Errorf("decode value: %w", err)
		}

		orderBy, err := l.list.orderByEncoding.Decode(ov[:len(ov)-len(v)])
		if err != nil {
			return false, fmt.Errorf("decode order by: %w", err)
		}

		return f(value, orderBy)
	})
}

// IterateValues iterates over values in the lexicographical order of order by.
// If the callback function f returns false, the iteration stops and the next
// can be used to continue the iteration.
func (l *ListTx[V, O]) IterateValues(start *ListElement[V, O], reverse bool, f func(V) (bool, error)) (next *ListElement[V, O], err error) {
	listBucket, err := l.listBucket(false)
	if err != nil {
		return nil, fmt.Errorf("list bucket: %w", err)
	}
	if listBucket == nil {
		return nil, nil
	}
	return iterateList(listBucket, l.list.valueEncoding, l.list.orderByEncoding, start, reverse, func(_, v []byte) (bool, error) {
		value, err := l.list.valueEncoding.Decode(v)
		if err != nil {
			return false, fmt.Errorf("decode value: %w", err)
		}

		return f(value)
	})
}

// Size returns the number of list elements.
func (l *ListTx[V, O]) Size() (int, error) {
	listBucket, err := l.listBucket(false)
	if err != nil {
		return 0, fmt.Errorf("list bucket: %w", err)
	}
	if listBucket == nil {
		return 0, nil
	}
	return size(listBucket, false), nil
}

// ListElement is the type returned by List pagination methods as slice elements
// that contain both value and order by.
type ListElement[V, O any] struct {
	Value   V
	OrderBy O
}

// Page returns at most a limit of elements of values and order by instances at
// the provided page number.
func (l *ListTx[V, O]) Page(number, limit int, reverse bool) (s []ListElement[V, O], totalElements, pages int, err error) {
	listBucket, err := l.listBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("list bucket: %w", err)
	}
	if listBucket == nil {
		return nil, 0, 0, nil
	}
	return page(listBucket, false, number, limit, reverse, func(ov, v []byte) (e ListElement[V, O], err error) {
		value, err := l.list.valueEncoding.Decode(v)
		if err != nil {
			return e, fmt.Errorf("decode value: %w", err)
		}

		orderBy, err := l.list.orderByEncoding.Decode(ov[:len(ov)-len(v)])
		if err != nil {
			return e, fmt.Errorf("decode order by: %w", err)
		}

		return ListElement[V, O]{
			Value:   value,
			OrderBy: orderBy,
		}, nil
	})
}

// PageOfValues returns at most a limit of elements of values at the provided
// page number.
func (l *ListTx[V, O]) PageOfValues(number, limit int, reverse bool) (s []V, totalElements, pages int, err error) {
	listBucket, err := l.listBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("list bucket: %w", err)
	}
	if listBucket == nil {
		return nil, 0, 0, nil
	}
	return page(listBucket, false, number, limit, reverse, func(_, v []byte) (value V, err error) {
		return l.list.valueEncoding.Decode(v)
	})
}
