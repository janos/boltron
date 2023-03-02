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
	bucketPath     [][]byte
	keyEncoding    Encoding[K]
	valueEncoding  Encoding[V]
	fillPercent    float64
	errNotFound    error
	errKeyExists   error
	saveCallback   func(key []byte) error
	deleteCallback func(key []byte) error
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
	return &CollectionDefinition[K, V]{
		bucketPath:    bucketPath("boltron: collection: " + name),
		keyEncoding:   keyEncoding,
		valueEncoding: valueEncoding,
		errNotFound:   withDefaultError(o.ErrNotFound, ErrNotFound),
		errKeyExists:  withDefaultError(o.ErrKeyExists, ErrKeyExists),
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
	if c.definition.fillPercent > 0 {
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

	if c.definition.saveCallback != nil {
		if err := c.definition.saveCallback(k); err != nil {
			return false, fmt.Errorf("save callback: %w", err)
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

	if c.definition.deleteCallback != nil {
		if err := c.definition.deleteCallback(k); err != nil {
			return fmt.Errorf("delete callback: %w", err)
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
