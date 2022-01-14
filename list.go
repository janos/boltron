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

// ListDefinition defines a list of values, ordered by the provided order type.
// List values are unique, but the order by values are not. If the order is
// defined by the values encoding, or it is not important, order by encoding
// should be set to NullEncoding.
type ListDefinition[V, O any] struct {
	bucketPath       [][]byte
	bucketPathIndex  [][]byte
	valueEncoding    Encoding[V]
	orderByEncoding  Encoding[O]
	errValueNotFound error
	addCallback      func(value, orderBy []byte) error // used by Lists
}

// ListOptions provides additional configuration for a List.
type ListOptions struct {
	// ErrValueNotFound is returned if the value is not found.
	ErrValueNotFound error
}

// NewListDefinition constructs a new ListDefinition with a unique name and key
// and order by encodings.
func NewListDefinition[V, O any](
	name string,
	valueEncoding Encoding[V],
	orderByEncoding Encoding[O],
	o *ListOptions,
) *ListDefinition[V, O] {
	if o == nil {
		o = new(ListOptions)
	}
	return &ListDefinition[V, O]{
		bucketPath:       bucketPath("boltron: list: " + name + " values"),
		bucketPathIndex:  bucketPath("boltron: list: " + name + " index"),
		valueEncoding:    valueEncoding,
		orderByEncoding:  orderByEncoding,
		errValueNotFound: withDefaultError(o.ErrValueNotFound, ErrNotFound),
	}
}

// List returns a List that has access to the stored data through the bolt
// transaction.
func (d *ListDefinition[V, O]) List(tx *bolt.Tx) *List[V, O] {
	return &List[V, O]{
		tx:         tx,
		definition: d,
	}
}

// List provides methods to access and change ordered list of values.
type List[V, O any] struct {
	tx               *bolt.Tx
	listBucketCache  *bolt.Bucket
	indexBucketCache *bolt.Bucket
	definition       *ListDefinition[V, O]
}

func (l *List[V, O]) listBucket(create bool) (*bolt.Bucket, error) {
	if l.listBucketCache != nil {
		return l.listBucketCache, nil
	}
	bucket, err := deepBucket(l.tx, create, l.definition.bucketPath...)
	if err != nil {
		return nil, err
	}
	l.listBucketCache = bucket
	return bucket, nil
}

func (l *List[V, O]) indexBucket(create bool) (*bolt.Bucket, error) {
	if l.indexBucketCache != nil {
		return l.indexBucketCache, nil
	}
	bucket, err := deepBucket(l.tx, create, l.definition.bucketPathIndex...)
	if err != nil {
		return nil, err
	}
	l.indexBucketCache = bucket
	return bucket, nil
}

// Has returns true if the value already exists in the database.
func (o *List[V, O]) Has(value V) (bool, error) {
	v, err := o.definition.valueEncoding.Encode(value)
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
func (l *List[V, O]) OrderBy(value V) (orderBy O, err error) {
	v, err := l.definition.valueEncoding.Encode(value)
	if err != nil {
		return orderBy, fmt.Errorf("encode value: %w", err)
	}

	indexBucket, err := l.indexBucket(false)
	if err != nil {
		return orderBy, fmt.Errorf("index bucket: %w", err)
	}
	if indexBucket == nil {
		return orderBy, l.definition.errValueNotFound
	}

	o := indexBucket.Get(v)
	if o == nil {
		return orderBy, l.definition.errValueNotFound
	}

	orderBy, err = l.definition.orderByEncoding.Decode(o)
	if err != nil {
		return orderBy, fmt.Errorf("decode order by: %w", err)
	}

	return orderBy, nil
}

// Add adds a value to the list with an order by instance.
func (l *List[V, O]) Add(value V, orderBy O) error {
	v, err := l.definition.valueEncoding.Encode(value)
	if err != nil {
		return fmt.Errorf("encode value: %w", err)
	}
	o, err := l.definition.orderByEncoding.Encode(orderBy)
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

	if l.definition.addCallback != nil {
		if err := l.definition.addCallback(v, o); err != nil {
			return fmt.Errorf("add callback: %w", err)
		}
	}

	return nil
}

// Remove removes the value and its associated order by from the database. If
// ensure flag is set to true and the value does not exist, ErrNotFound is
// returned.
func (l *List[V, O]) Remove(value V, ensure bool) error {
	v, err := l.definition.valueEncoding.Encode(value)
	if err != nil {
		return fmt.Errorf("encode value: %w", err)
	}

	indexBucket, err := l.indexBucket(false)
	if err != nil {
		return fmt.Errorf("index bucket: %w", err)
	}

	if indexBucket == nil {
		if ensure {
			return l.definition.errValueNotFound
		}
		return nil
	}

	o := indexBucket.Get(v)
	if o == nil {
		if ensure {
			return l.definition.errValueNotFound
		}
		return nil
	}

	listBucket, err := l.listBucket(false)
	if err != nil {
		return fmt.Errorf("list bucket: %w", err)
	}

	if listBucket == nil {
		if ensure {
			return l.definition.errValueNotFound
		}
		return nil
	}

	if err := listBucket.Delete(append(o, v...)); err != nil {
		return fmt.Errorf("delete from list bucket: %w", err)
	}
	if err := indexBucket.Delete(v); err != nil {
		return fmt.Errorf("delete from index bucket: %w", err)
	}

	return nil
}

// Iterate iterates over keys and values in the lexicographical order of keys.
// If the callback function f returns false, the iteration stops and the next
// can be used to continue the iteration.
func (l *List[V, O]) Iterate(start *ListElement[V, O], reverse bool, f func(V, O) (bool, error)) (next *ListElement[V, O], err error) {
	listBucket, err := l.listBucket(false)
	if err != nil {
		return nil, fmt.Errorf("list bucket: %w", err)
	}
	if listBucket == nil {
		return nil, nil
	}
	return iterateList(listBucket, l.definition.valueEncoding, l.definition.orderByEncoding, start, reverse, func(ov, v []byte) (bool, error) {
		value, err := l.definition.valueEncoding.Decode(v)
		if err != nil {
			return false, fmt.Errorf("decode value: %w", err)
		}

		orderBy, err := l.definition.orderByEncoding.Decode(ov[:len(ov)-len(v)])
		if err != nil {
			return false, fmt.Errorf("decode order by: %w", err)
		}

		return f(value, orderBy)
	})
}

// IterateValues iterates over values in the lexicographical order of order by.
// If the callback function f returns false, the iteration stops and the next
// can be used to continue the iteration.
func (l *List[V, O]) IterateValues(start *ListElement[V, O], reverse bool, f func(V) (bool, error)) (next *ListElement[V, O], err error) {
	listBucket, err := l.listBucket(false)
	if err != nil {
		return nil, fmt.Errorf("list bucket: %w", err)
	}
	if listBucket == nil {
		return nil, nil
	}
	return iterateList(listBucket, l.definition.valueEncoding, l.definition.orderByEncoding, start, reverse, func(_, v []byte) (bool, error) {
		value, err := l.definition.valueEncoding.Decode(v)
		if err != nil {
			return false, fmt.Errorf("decode value: %w", err)
		}

		return f(value)
	})
}

// ListElement is the type returned by List pagination methods as slice elements
// that cointain both value and order by.
type ListElement[V, O any] struct {
	Value   V
	OrderBy O
}

// Page returns at most a limit of elements of values and order by instances at
// the provided page number.
func (l *List[V, O]) Page(number, limit int, reverse bool) (s []ListElement[V, O], totalElements, pages int, err error) {
	listBucket, err := l.listBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("list bucket: %w", err)
	}
	if listBucket == nil {
		return nil, 0, 0, nil
	}
	return page(listBucket, false, number, limit, reverse, func(ov, v []byte) (e ListElement[V, O], err error) {
		value, err := l.definition.valueEncoding.Decode(v)
		if err != nil {
			return e, fmt.Errorf("decode value: %w", err)
		}

		orderBy, err := l.definition.orderByEncoding.Decode(ov[:len(ov)-len(v)])
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
func (l *List[V, O]) PageOfValues(number, limit int, reverse bool) (s []V, totalElements, pages int, err error) {
	listBucket, err := l.listBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("list bucket: %w", err)
	}
	if listBucket == nil {
		return nil, 0, 0, nil
	}
	return page(listBucket, false, number, limit, reverse, func(_, v []byte) (value V, err error) {
		return l.definition.valueEncoding.Decode(v)
	})
}
