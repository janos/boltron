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

type CollectionDefinition[K, V any] struct {
	bucketName    []byte
	keyEncoding   Encoding[K]
	valueEncoding Encoding[V]
	fillPercent   float64
	errNotFound   error
	errKeyExists  error
}

type CollectionOptions struct {
	FillPercent  float64
	ErrNotFound  error
	ErrKeyExists error
}

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
		bucketName:    []byte("boltron: collection: " + name),
		keyEncoding:   keyEncoding,
		valueEncoding: valueEncoding,
		errNotFound:   withDefaultError(o.ErrNotFound, ErrNotFound),
		errKeyExists:  withDefaultError(o.ErrKeyExists, ErrKeyExists),
	}
}

func (d *CollectionDefinition[K, V]) Collection(tx *bolt.Tx) *Collection[K, V] {
	return &Collection[K, V]{
		tx:         tx,
		definition: d,
	}
}

type Collection[K, V any] struct {
	tx          *bolt.Tx
	bucketCache *bolt.Bucket
	definition  *CollectionDefinition[K, V]
}

func (c *Collection[K, V]) bucket(create bool) (*bolt.Bucket, error) {
	if c.bucketCache != nil {
		return c.bucketCache, nil
	}
	bucket, err := rootBucket(c.tx, create, c.definition.bucketName)
	if err != nil {
		return nil, err
	}
	if c.definition.fillPercent > 0 {
		bucket.FillPercent = c.definition.fillPercent
	}
	c.bucketCache = bucket
	return bucket, nil
}

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

func (c *Collection[K, V]) Save(key K, value V, overwrite bool) (overwriten bool, err error) {
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
	overwriten = currentValue != nil && !bytes.Equal(currentValue, v)
	if overwriten && !overwrite {
		return false, c.definition.errKeyExists
	}
	return overwriten, bucket.Put(k, v)
}

func (c *Collection[K, V]) Delete(key K, ensure bool) error {
	k, err := c.definition.keyEncoding.Encode(key)
	if err != nil {
		return fmt.Errorf("encode key: %w", err)
	}
	bucket, err := c.bucket(false)
	if err != nil {
		return err
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
	return bucket.Delete(k)
}

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

func (c *Collection[K, V]) Page(number, limit int, reverse bool) (s []Element[K, V], totalElements, pages int, err error) {
	bucket, err := c.bucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("bucket: %w", err)
	}
	if bucket == nil {
		return nil, 0, 0, nil
	}
	return page(bucket, false, number, limit, reverse, func(k, v []byte) (e Element[K, V], err error) {
		key, err := c.definition.keyEncoding.Decode(k)
		if err != nil {
			return e, fmt.Errorf("key value: %w", err)
		}

		value, err := c.definition.valueEncoding.Decode(v)
		if err != nil {
			return e, fmt.Errorf("decode value: %w", err)
		}

		return newElement(key, value)
	})
}

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
