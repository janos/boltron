// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron

import (
	"bytes"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// CollectionDefinition defines the most basic data model which is a Collection
// of keys and values. Each key is a unique within a Collection.
type CollectionDefinition[K, V any] struct {
	bucketPath      [][]byte
	indexBucketPath [][]byte
	keyEncoding     Encoding[K]
	valueEncoding   Encoding[V]
	fillPercent     float64
	errNotFound     error
	errKeyExists    error
}

// CollectionOptions provides additional configuration for a Collection.
type CollectionOptions struct {
	// FillPercent is the value for the bolt bucket fill percent.
	FillPercent float64
	// ErrNotFound is returned if the key is not found.
	ErrNotFound error
	// ErrKeyExists is returned if the key already exists and its value is not
	// allowed to be overwritten.
	ErrKeyExists error
	// UniqueKeys enables global uniqueness indexing across all instances.
	UniqueKeys bool
}

// NewCollectionDefinition constructs a new CollectionDefinition with a unique
// name and key and value encodings.
func NewCollectionDefinition[K, V any](
	name string,
	keyEncoding Encoding[K],
	valueEncoding Encoding[V],
	o *CollectionOptions,
) *CollectionDefinition[K, V] {
	if o == nil {
		o = new(CollectionOptions)
	}
	var indexBucketPath [][]byte
	if o.UniqueKeys {
		indexBucketPath = bucketPath("boltron: collection: index: " + name + " unique_keys")
	}
	return &CollectionDefinition[K, V]{
		bucketPath:      bucketPath("boltron: collection: " + name),
		indexBucketPath: indexBucketPath,
		keyEncoding:     keyEncoding,
		valueEncoding:   valueEncoding,
		fillPercent:     o.FillPercent,
		errNotFound:     withDefaultError(o.ErrNotFound, ErrNotFound),
		errKeyExists:    withDefaultError(o.ErrKeyExists, ErrKeyExists),
	}
}

// Collection returns a Collection that has access to the stored data through
// the bolt transaction.
func (d *CollectionDefinition[K, V]) Collection(tx *bolt.Tx) *Collection[K, V] {
	return &Collection[K, V]{
		tx:         tx,
		definition: d,
	}
}

// Collection provides methods to access and change key/value pairs.
type Collection[K, V any] struct {
	tx          *bolt.Tx
	bucketCache *bolt.Bucket
	definition  *CollectionDefinition[K, V]
}

func (c *Collection[K, V]) bucket(create bool) (*bolt.Bucket, error) {
	if c.bucketCache != nil {
		return c.bucketCache, nil
	}
	bucket, err := deepBucket(c.tx, create, c.definition.bucketPath...)
	if err != nil {
		return nil, err
	}
	if c.definition.fillPercent > 0 && bucket != nil {
		bucket.FillPercent = c.definition.fillPercent
	}
	c.bucketCache = bucket
	return bucket, nil
}

// Has returns true if the key already exists in the database.
func (c *Collection[K, V]) Has(key K) (bool, error) {
	k, err := c.definition.keyEncoding.Encode(key)
	if err != nil {
		return false, fmt.Errorf("encode key: %w", err)
	}
	bucket, err := c.bucket(false)
	if err != nil {
		return false, fmt.Errorf("bucket: %w", err)
	}
	if bucket == nil {
		return false, nil
	}
	return bucket.Get(k) != nil, nil
}

// Get returns a value associated with the given key. If key does not exist,
// ErrNotFound is returned.
func (c *Collection[K, V]) Get(key K) (value V, err error) {
	k, err := c.definition.keyEncoding.Encode(key)
	if err != nil {
		return value, fmt.Errorf("encode key: %w", err)
	}
	bucket, err := c.bucket(false)
	if err != nil {
		return value, fmt.Errorf("bucket: %w", err)
	}
	if bucket == nil {
		return value, c.definition.errNotFound
	}
	v := bucket.Get(k)
	if v == nil {
		return value, c.definition.errNotFound
	}
	value, err = c.definition.valueEncoding.Decode(v)
	if err != nil {
		return value, fmt.Errorf("decode value: %w", err)
	}
	return value, nil
}

// Save saves the key/value pair. If the overwrite flag is set to false and key
// already exists, configured ErrKeyExists is returned.
func (c *Collection[K, V]) Save(key K, value V, overwrite bool) (overwritten bool, err error) {
	k, err := c.definition.keyEncoding.Encode(key)
	if err != nil {
		return false, fmt.Errorf("encode key: %w", err)
	}
	bucket, err := c.bucket(true)
	if err != nil {
		return false, fmt.Errorf("bucket: %w", err)
	}
	v, err := c.definition.valueEncoding.Encode(value)
	if err != nil {
		return false, fmt.Errorf("encode value: %w", err)
	}
	currentValue := bucket.Get(k)
	overwritten = currentValue != nil && !bytes.Equal(currentValue, v)
	if overwritten && !overwrite {
		return false, c.definition.errKeyExists
	}

	if c.definition.indexBucketPath != nil {
		indexBucket, err := deepBucket(c.tx, true, c.definition.indexBucketPath...)
		if err != nil {
			return false, fmt.Errorf("index bucket: %w", err)
		}
		existingPathData := indexBucket.Get(k)
		if existingPathData != nil {
			existingPath, err := decodePath(existingPathData)
			if err != nil {
				return false, fmt.Errorf("decode existing path: %w", err)
			}
			if !equalPaths(existingPath, c.definition.bucketPath) {
				return false, c.definition.errKeyExists
			}
		} else {
			if err := indexBucket.Put(k, encodePath(c.definition.bucketPath)); err != nil {
				return false, fmt.Errorf("write index entry: %w", err)
			}
		}
	}

	return overwritten, bucket.Put(k, v)
}

// Delete removes the key and its associated value from the database. If ensure
// flag is set to true and the key does not exist, configured ErrNotFound is
// returned.
func (c *Collection[K, V]) Delete(key K, ensure bool) error {
	k, err := c.definition.keyEncoding.Encode(key)
	if err != nil {
		return fmt.Errorf("encode key: %w", err)
	}
	bucket, err := c.bucket(false)
	if err != nil {
		return fmt.Errorf("bucket: %w", err)
	}
	if bucket == nil {
		if ensure {
			return c.definition.errNotFound
		}
		return nil
	}
	if ensure {
		v := bucket.Get(k)
		if v == nil {
			return c.definition.errNotFound
		}
	}

	// 1. Cascading nested sub-buckets cleanup
	suffixes := [][]byte{
		k,
		append(append([]byte(nil), k...), []byte(" left")...),
		append(append([]byte(nil), k...), []byte(" right")...),
		append(append([]byte(nil), k...), []byte(" values")...),
		append(append([]byte(nil), k...), []byte(" index")...),
	}
	for _, suffix := range suffixes {
		if bucket.Bucket(suffix) != nil {
			prefixPath := append(c.definition.bucketPath, suffix)
			if err := cleanupIndexesForPath(c.tx, prefixPath); err != nil {
				return fmt.Errorf("cleanup indexes for %s: %w", suffix, err)
			}
			if err := bucket.DeleteBucket(suffix); err != nil {
				return fmt.Errorf("delete nested bucket %s: %w", suffix, err)
			}
		}
	}

	// 2. Remove unique keys index entry
	if c.definition.indexBucketPath != nil {
		indexBucket, err := deepBucket(c.tx, false, c.definition.indexBucketPath...)
		if err != nil {
			return fmt.Errorf("index bucket: %w", err)
		}
		if indexBucket != nil {
			if err := indexBucket.Delete(k); err != nil {
				return fmt.Errorf("delete unique key index: %w", err)
			}
		}
	}

	return bucket.Delete(k)
}

// Iterate iterates over keys and values in the lexicographical order of keys.
func (c *Collection[K, V]) Iterate(start *K, reverse bool, f func(K, V) (bool, error)) (next *K, err error) {
	bucket, err := c.bucket(false)
	if err != nil {
		return nil, fmt.Errorf("bucket: %w", err)
	}
	if bucket == nil {
		return nil, nil
	}
	return iterateKeys(bucket, c.definition.keyEncoding, start, reverse, func(k, v []byte) (bool, error) {
		key, err := c.definition.keyEncoding.Decode(k)
		if err != nil {
			return false, fmt.Errorf("decode key: %w", err)
		}

		value, err := c.definition.valueEncoding.Decode(v)
		if err != nil {
			return false, fmt.Errorf("decode value: %w", err)
		}

		return f(key, value)
	})
}

// IterateKeys iterates over keys in the lexicographical order of keys. If the
// callback function f returns false, the iteration stops and the next can be
// used to continue the iteration.
func (c *Collection[K, V]) IterateKeys(start *K, reverse bool, f func(K) (bool, error)) (next *K, err error) {
	bucket, err := c.bucket(false)
	if err != nil {
		return nil, fmt.Errorf("bucket: %w", err)
	}
	if bucket == nil {
		return nil, nil
	}
	return iterateKeys(bucket, c.definition.keyEncoding, start, reverse, func(k, _ []byte) (bool, error) {
		key, err := c.definition.keyEncoding.Decode(k)
		if err != nil {
			return false, fmt.Errorf("decode key: %w", err)
		}

		return f(key)
	})
}

// IterateValues iterates over values in the lexicographical order of keys. If
// the callback function f returns false, the iteration stops and the next can
// be used to continue the iteration.
func (c *Collection[K, V]) IterateValues(start *K, reverse bool, f func(V) (bool, error)) (next *K, err error) {
	bucket, err := c.bucket(false)
	if err != nil {
		return nil, fmt.Errorf("bucket: %w", err)
	}
	if bucket == nil {
		return nil, nil
	}
	return iterateKeys(bucket, c.definition.keyEncoding, start, reverse, func(_, v []byte) (bool, error) {
		value, err := c.definition.valueEncoding.Decode(v)
		if err != nil {
			return false, fmt.Errorf("decode value: %w", err)
		}

		return f(value)
	})
}

// Size returns the number of collection elements.
func (c *Collection[K, V]) Size() (int, error) {
	bucket, err := c.bucket(false)
	if err != nil {
		return 0, fmt.Errorf("bucket: %w", err)
	}
	if bucket == nil {
		return 0, nil
	}
	return size(bucket, false), nil
}

// CollectionElement is the type returned by pagination methods as slice
// elements that contain both key and value.
type CollectionElement[K, V any] struct {
	Key   K
	Value V
}

// Page returns at most a limit of elements of key/value pairs at the provided
// page number.
func (c *Collection[K, V]) Page(number, limit int, reverse bool) (s []CollectionElement[K, V], totalElements, pages int, err error) {
	bucket, err := c.bucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("bucket: %w", err)
	}
	if bucket == nil {
		return nil, 0, 0, nil
	}
	return page(bucket, false, number, limit, reverse, func(k, v []byte) (e CollectionElement[K, V], err error) {
		key, err := c.definition.keyEncoding.Decode(k)
		if err != nil {
			return e, fmt.Errorf("key value: %w", err)
		}

		value, err := c.definition.valueEncoding.Decode(v)
		if err != nil {
			return e, fmt.Errorf("decode value: %w", err)
		}

		return CollectionElement[K, V]{
			Key:   key,
			Value: value,
		}, nil
	})
}

// PageOfKeys returns at most a limit of keys at the provided page number.
func (c *Collection[K, V]) PageOfKeys(number, limit int, reverse bool) (s []K, totalElements, pages int, err error) {
	bucket, err := c.bucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("bucket: %w", err)
	}
	if bucket == nil {
		return nil, 0, 0, nil
	}
	return page(bucket, false, number, limit, reverse, func(k, _ []byte) (key K, err error) {
		return c.definition.keyEncoding.Decode(k)
	})
}

// PageOfValues returns at most a limit of values at the provided page number.
func (c *Collection[K, V]) PageOfValues(number, limit int, reverse bool) (s []V, totalElements, pages int, err error) {
	bucket, err := c.bucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("bucket: %w", err)
	}
	if bucket == nil {
		return nil, 0, 0, nil
	}
	return page(bucket, false, number, limit, reverse, func(_, v []byte) (key V, err error) {
		return c.definition.valueEncoding.Decode(v)
	})
}

// Collection returns a nested Collection inside the current collection under the given key.
func (c *Collection[K, V]) Collection[K2, V2 any](key K, definition *CollectionDefinition[K2, V2]) (*Collection[K2, V2], error) {
	k, err := c.definition.keyEncoding.Encode(key)
	if err != nil {
		return nil, fmt.Errorf("encode key: %w", err)
	}

	newPath := make([][]byte, len(c.definition.bucketPath)+1)
	copy(newPath, c.definition.bucketPath)
	newPath[len(c.definition.bucketPath)] = k

	nestedDef := &CollectionDefinition[K2, V2]{
		bucketPath:      newPath,
		indexBucketPath: definition.indexBucketPath,
		keyEncoding:     definition.keyEncoding,
		valueEncoding:   definition.valueEncoding,
		fillPercent:     definition.fillPercent,
		errNotFound:     definition.errNotFound,
		errKeyExists:    definition.errKeyExists,
	}

	return nestedDef.Collection(c.tx), nil
}

// Association returns a nested Association inside the current collection under the given key.
func (c *Collection[K, V]) Association[L2, R2 any](key K, definition *AssociationDefinition[L2, R2]) (*Association[L2, R2], error) {
	k, err := c.definition.keyEncoding.Encode(key)
	if err != nil {
		return nil, fmt.Errorf("encode key: %w", err)
	}

	newPathLeft := make([][]byte, len(c.definition.bucketPath)+1)
	copy(newPathLeft, c.definition.bucketPath)
	newPathLeft[len(c.definition.bucketPath)] = append(append([]byte(nil), k...), []byte(" left")...)

	newPathRight := make([][]byte, len(c.definition.bucketPath)+1)
	copy(newPathRight, c.definition.bucketPath)
	newPathRight[len(c.definition.bucketPath)] = append(append([]byte(nil), k...), []byte(" right")...)

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

	return nestedDef.Association(c.tx), nil
}

// List returns a nested List inside the current collection under the given key.
func (c *Collection[K, V]) List[V2, O2 any](key K, definition *ListDefinition[V2, O2]) (*List[V2, O2], error) {
	k, err := c.definition.keyEncoding.Encode(key)
	if err != nil {
		return nil, fmt.Errorf("encode key: %w", err)
	}

	newPath := make([][]byte, len(c.definition.bucketPath)+1)
	copy(newPath, c.definition.bucketPath)
	newPath[len(c.definition.bucketPath)] = append(append([]byte(nil), k...), []byte(" values")...)

	newPathIndex := make([][]byte, len(c.definition.bucketPath)+1)
	copy(newPathIndex, c.definition.bucketPath)
	newPathIndex[len(c.definition.bucketPath)] = append(append([]byte(nil), k...), []byte(" index")...)

	nestedDef := &ListDefinition[V2, O2]{
		bucketPath:       newPath,
		bucketPathIndex:  newPathIndex,
		indexBucketPath:  definition.indexBucketPath,
		valueEncoding:    definition.valueEncoding,
		orderByEncoding:  definition.orderByEncoding,
		fillPercent:      definition.fillPercent,
		errValueNotFound: definition.errValueNotFound,
	}

	return nestedDef.List(c.tx), nil
}

// ParentKey returns the decoded parent key of the container. If the container is at the root level, ErrNoParent is returned.
func (d *CollectionDefinition[K, V]) ParentKey[P any](tx *bolt.Tx, key K, parentKeyEncoding Encoding[P]) (parentKey P, err error) {
	if d.indexBucketPath == nil {
		if len(d.bucketPath) <= 1 {
			return parentKey, ErrNoParent
		}
		return parentKey, fmt.Errorf("unique keys index not configured")
	}
	k, err := d.keyEncoding.Encode(key)
	if err != nil {
		return parentKey, fmt.Errorf("encode key: %w", err)
	}
	indexBucket, err := deepBucket(tx, false, d.indexBucketPath...)
	if err != nil {
		return parentKey, fmt.Errorf("index bucket: %w", err)
	}
	if indexBucket == nil {
		return parentKey, ErrNotFound
	}
	pathData := indexBucket.Get(k)
	if pathData == nil {
		return parentKey, d.errNotFound
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
