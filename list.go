// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron

import (
	"bytes"
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
	indexBucketPath  [][]byte
	valueEncoding    Encoding[V]
	orderByEncoding  Encoding[O]
	fillPercent      float64
	errValueNotFound error
}

// ListOptions provides additional configuration for a List.
type ListOptions struct {
	// FillPercent is the value for the bolt bucket fill percent.
	FillPercent float64
	// ErrValueNotFound is returned if the value is not found.
	ErrValueNotFound error
	// UniqueValues enables global uniqueness indexing across all list instances.
	UniqueValues bool
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
	var indexBucketPath [][]byte
	if o.UniqueValues {
		indexBucketPath = bucketPath("boltron: list: index: " + name + " unique_values")
	}
	return &ListDefinition[V, O]{
		bucketPath:       bucketPath("boltron: list: " + name + " values"),
		bucketPathIndex:  bucketPath("boltron: list: " + name + " index"),
		indexBucketPath:  indexBucketPath,
		valueEncoding:    valueEncoding,
		orderByEncoding:  orderByEncoding,
		fillPercent:      o.FillPercent,
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
	if l.definition.fillPercent > 0 && bucket != nil {
		bucket.FillPercent = l.definition.fillPercent
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
	if l.definition.fillPercent > 0 && bucket != nil {
		bucket.FillPercent = l.definition.fillPercent
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

	if l.definition.indexBucketPath != nil {
		indexBucket, err := deepBucket(l.tx, true, l.definition.indexBucketPath...)
		if err != nil {
			return fmt.Errorf("unique index bucket: %w", err)
		}
		existingPathData := indexBucket.Get(v)
		if existingPathData != nil {
			existingPath, err := decodePath(existingPathData)
			if err != nil {
				return fmt.Errorf("decode existing path: %w", err)
			}
			if !equalPaths(existingPath, l.definition.bucketPath) {
				return ErrValueExists
			}
		} else {
			if err := indexBucket.Put(v, encodePath(l.definition.bucketPath)); err != nil {
				return fmt.Errorf("write index entry: %w", err)
			}
		}
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

	// 1. Cascading nested sub-buckets cleanup
	suffixes := [][]byte{
		v,
		append(v, []byte(" left")...),
		append(v, []byte(" right")...),
		append(v, []byte(" values")...),
		append(v, []byte(" index")...),
	}
	for _, suffix := range suffixes {
		if listBucket.Bucket(suffix) != nil {
			if err := listBucket.DeleteBucket(suffix); err != nil {
				return fmt.Errorf("delete nested bucket %s: %w", suffix, err)
			}
		}
	}

	// 2. Remove unique values index entry
	if l.definition.indexBucketPath != nil {
		uniqueBucket, err := deepBucket(l.tx, false, l.definition.indexBucketPath...)
		if err != nil {
			return fmt.Errorf("unique index bucket: %w", err)
		}
		if uniqueBucket != nil {
			if err := uniqueBucket.Delete(v); err != nil {
				return fmt.Errorf("delete unique value index: %w", err)
			}
		}
	}

	if err := listBucket.Delete(append(o, v...)); err != nil {
		return fmt.Errorf("delete from list bucket: %w", err)
	}
	return indexBucket.Delete(v)
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

// Size returns the number of list elements.
func (l *List[V, O]) Size() (int, error) {
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

// Collection returns a nested Collection inside the list under the given value.
func (l *List[V, O]) Collection[K2, V2 any](value V, definition *CollectionDefinition[K2, V2]) (*Collection[K2, V2], error) {
	val, err := l.definition.valueEncoding.Encode(value)
	if err != nil {
		return nil, fmt.Errorf("encode value: %w", err)
	}

	newPath := make([][]byte, len(l.definition.bucketPath)+1)
	copy(newPath, l.definition.bucketPath)
	newPath[len(l.definition.bucketPath)] = val

	nestedDef := &CollectionDefinition[K2, V2]{
		bucketPath:      newPath,
		indexBucketPath: definition.indexBucketPath,
		keyEncoding:     definition.keyEncoding,
		valueEncoding:   definition.valueEncoding,
		fillPercent:     definition.fillPercent,
		errNotFound:     definition.errNotFound,
		errKeyExists:    definition.errKeyExists,
	}

	return nestedDef.Collection(l.tx), nil
}

// Association returns a nested Association inside the list under the given value.
func (l *List[V, O]) Association[L2, R2 any](value V, definition *AssociationDefinition[L2, R2]) (*Association[L2, R2], error) {
	val, err := l.definition.valueEncoding.Encode(value)
	if err != nil {
		return nil, fmt.Errorf("encode value: %w", err)
	}

	newPathLeft := make([][]byte, len(l.definition.bucketPath)+1)
	copy(newPathLeft, l.definition.bucketPath)
	newPathLeft[len(l.definition.bucketPath)] = append(append([]byte(nil), val...), []byte(" left")...)

	newPathRight := make([][]byte, len(l.definition.bucketPath)+1)
	copy(newPathRight, l.definition.bucketPath)
	newPathRight[len(l.definition.bucketPath)] = append(append([]byte(nil), val...), []byte(" right")...)

	nestedDef := &AssociationDefinition[L2, R2]{
		bucketPathLeft:       newPathLeft,
		bucketPathRight:      newPathRight,
		indexBucketPathLeft:  definition.indexBucketPathLeft,
		indexBucketPathRight: definition.indexBucketPathRight,
		leftEncoding:         definition.leftEncoding,
		rightEncoding:        definition.rightEncoding,
		fillPercent:          definition.fillPercent,
		errLeftNotFound:      definition.errLeftNotFound,
		errRightNotFound:     definition.errRightNotFound,
		errLeftExists:        definition.errLeftExists,
		errRightExists:       definition.errRightExists,
	}

	return nestedDef.Association(l.tx), nil
}

// List returns a nested List inside the list under the given value.
func (l *List[V, O]) List[V2, O2 any](value V, definition *ListDefinition[V2, O2]) (*List[V2, O2], error) {
	val, err := l.definition.valueEncoding.Encode(value)
	if err != nil {
		return nil, fmt.Errorf("encode value: %w", err)
	}

	newPath := make([][]byte, len(l.definition.bucketPath)+1)
	copy(newPath, l.definition.bucketPath)
	newPath[len(l.definition.bucketPath)] = append(append([]byte(nil), val...), []byte(" values")...)

	newPathIndex := make([][]byte, len(l.definition.bucketPath)+1)
	copy(newPathIndex, l.definition.bucketPath)
	newPathIndex[len(l.definition.bucketPath)] = append(append([]byte(nil), val...), []byte(" index")...)

	nestedDef := &ListDefinition[V2, O2]{
		bucketPath:       newPath,
		bucketPathIndex:  newPathIndex,
		indexBucketPath:  definition.indexBucketPath,
		valueEncoding:    definition.valueEncoding,
		orderByEncoding:  definition.orderByEncoding,
		fillPercent:      definition.fillPercent,
		errValueNotFound: definition.errValueNotFound,
	}

	return nestedDef.List(l.tx), nil
}

// ParentKey returns the decoded parent key of the container. If the container is at the root level, ErrNoParent is returned.
func (d *ListDefinition[V, O]) ParentKey[P any](tx *bolt.Tx, value V, parentKeyEncoding Encoding[P]) (parentKey P, err error) {
	if d.indexBucketPath == nil {
		if len(d.bucketPath) <= 1 {
			return parentKey, ErrNoParent
		}
		return parentKey, fmt.Errorf("unique values index not configured")
	}
	val, err := d.valueEncoding.Encode(value)
	if err != nil {
		return parentKey, fmt.Errorf("encode value: %w", err)
	}
	indexBucket, err := deepBucket(tx, false, d.indexBucketPath...)
	if err != nil {
		return parentKey, fmt.Errorf("index bucket: %w", err)
	}
	if indexBucket == nil {
		return parentKey, ErrNotFound
	}
	pathData := indexBucket.Get(val)
	if pathData == nil {
		return parentKey, d.errValueNotFound
	}
	path, err := decodePath(pathData)
	if err != nil {
		return parentKey, fmt.Errorf("decode path: %w", err)
	}
	if len(path) <= 1 {
		return parentKey, ErrNoParent
	}
	last := path[len(path)-1]

	suffixes := []string{" left", " right", " values", " index"}
	for _, suffix := range suffixes {
		if before, ok := bytes.CutSuffix(last, []byte(suffix)); ok {
			last = before
			break
		}
	}

	parentKey, err = parentKeyEncoding.Decode(last)
	if err != nil {
		return parentKey, fmt.Errorf("decode parent key: %w", err)
	}
	return parentKey, nil
}
