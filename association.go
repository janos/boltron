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

// AssociationDefinition defines one-to-one relation between keys and values.
// The relation is unique.
type AssociationDefinition[K, V any] struct {
	bucketNameKeys   []byte
	bucketNameValues []byte
	keyEncoding      Encoding[K]
	valueEncoding    Encoding[V]
	errNotFound      error
	errKeyExists     error
	errValueExists   error
}

// AssociationOptions provides additional configuration for an Association.
type AssociationOptions struct {
	// ErrNotFound is returned if the key or value is not found.
	ErrNotFound error
	// ErrKeyExists is returned if the key already exists.
	ErrKeyExists error
	// ErrValueExists is returned if the value already exists.
	ErrValueExists error
}

// NewAssociationDefinition constructs a new AssociationDefinition with a unique
// name and key and value encodings.
func NewAssociationDefinition[K, V any](
	name string,
	keyEncoding Encoding[K],
	valueEncoding Encoding[V],
	o *AssociationOptions,
) *AssociationDefinition[K, V] {
	if o == nil {
		o = new(AssociationOptions)
	}
	return &AssociationDefinition[K, V]{
		bucketNameKeys:   []byte("boltron: association: " + name + " keys"),
		bucketNameValues: []byte("boltron: association: " + name + " values"),
		keyEncoding:      keyEncoding,
		valueEncoding:    valueEncoding,
		errNotFound:      withDefaultError(o.ErrNotFound, ErrNotFound),
		errKeyExists:     withDefaultError(o.ErrKeyExists, ErrKeyExists),
		errValueExists:   withDefaultError(o.ErrValueExists, ErrValueExists),
	}
}

// Association returns an Association that has access to the stored data through
// the bolt transaction.
func (d *AssociationDefinition[K, V]) Association(tx *bolt.Tx) *Association[K, V] {
	return &Association[K, V]{
		tx:         tx,
		definition: d,
	}
}

// Association provides methods to access and change relations.
type Association[K, V any] struct {
	tx                *bolt.Tx
	keysBucketCache   *bolt.Bucket
	valuesBucketCache *bolt.Bucket
	definition        *AssociationDefinition[K, V]
}

func (a *Association[K, V]) keysBucket(create bool) (*bolt.Bucket, error) {
	if a.keysBucketCache != nil {
		return a.keysBucketCache, nil
	}
	bucket, err := rootBucket(a.tx, create, a.definition.bucketNameKeys)
	if err != nil {
		return nil, err
	}
	a.keysBucketCache = bucket
	return bucket, nil
}

func (a *Association[K, V]) valuesBucket(create bool) (*bolt.Bucket, error) {
	if a.valuesBucketCache != nil {
		return a.valuesBucketCache, nil
	}
	bucket, err := rootBucket(a.tx, create, a.definition.bucketNameValues)
	if err != nil {
		return nil, err
	}
	a.valuesBucketCache = bucket
	return bucket, nil
}

// HasKey returns true if the key already exists in the database.
func (a *Association[K, V]) HasKey(key K) (bool, error) {
	k, err := a.definition.keyEncoding.Encode(key)
	if err != nil {
		return false, fmt.Errorf("encode key: %w", err)
	}
	keysBucket, err := a.keysBucket(false)
	if err != nil {
		return false, fmt.Errorf("keys bucket: %w", err)
	}
	if keysBucket == nil {
		return false, nil
	}
	return keysBucket.Get(k) != nil, nil
}

// HasValue returns true if the value already exists in the database.
func (a *Association[K, V]) HasValue(value V) (bool, error) {
	v, err := a.definition.valueEncoding.Encode(value)
	if err != nil {
		return false, fmt.Errorf("encode value: %w", err)
	}

	valuesBucket, err := a.valuesBucket(true)
	if err != nil {
		return false, fmt.Errorf("values bucket: %w", err)
	}

	return valuesBucket.Get(v) != nil, nil
}

// Key returns a key associated with the given value. If value does not exist,
// ErrNotFound is returned.
func (a *Association[K, V]) Key(value V) (key K, err error) {
	v, err := a.definition.valueEncoding.Encode(value)
	if err != nil {
		return key, fmt.Errorf("encode value: %w", err)
	}

	valuesBucket, err := a.valuesBucket(false)
	if err != nil {
		return key, fmt.Errorf("values bucket: %w", err)
	}
	if valuesBucket == nil {
		return key, a.definition.errNotFound
	}

	k := valuesBucket.Get(v)
	if k == nil {
		return key, a.definition.errNotFound
	}
	key, err = a.definition.keyEncoding.Decode(k)
	if err != nil {
		return key, fmt.Errorf("decode key: %w", err)
	}
	return key, nil
}

// Value returns a value associated with the given key. If key does not exist,
// ErrNotFound is returned.
func (a *Association[K, V]) Value(key K) (value V, err error) {
	k, err := a.definition.keyEncoding.Encode(key)
	if err != nil {
		return value, fmt.Errorf("encode key: %w", err)
	}
	keysBucket, err := a.keysBucket(false)
	if err != nil {
		return value, fmt.Errorf("keys bucket: %w", err)
	}
	if keysBucket == nil {
		return value, a.definition.errNotFound
	}
	v := keysBucket.Get(k)
	if v == nil {
		return value, a.definition.errNotFound
	}
	value, err = a.definition.valueEncoding.Decode(v)
	if err != nil {
		return value, fmt.Errorf("decode value: %w", err)
	}
	return value, nil
}

// Set saves the relation between the key and value. If key already exists,
// configured ErrKeyExists is returned, if value exists, configured
// ErrValueExists is returned.
func (a *Association[K, V]) Set(key K, value V) error {
	k, err := a.definition.keyEncoding.Encode(key)
	if err != nil {
		return fmt.Errorf("encode key: %w", err)
	}
	keysBucket, err := a.keysBucket(true)
	if err != nil {
		return fmt.Errorf("keys bucket: %w", err)
	}

	currentValue := keysBucket.Get(k)

	v, err := a.definition.valueEncoding.Encode(value)
	if err != nil {
		return fmt.Errorf("encode value: %w", err)
	}

	valuesBucket, err := a.valuesBucket(true)
	if err != nil {
		return fmt.Errorf("values bucket: %w", err)
	}

	currentKey := valuesBucket.Get(v)

	if bytes.Equal(k, currentKey) && bytes.Equal(v, currentValue) {
		return nil
	}

	if currentKey != nil {
		return a.definition.errValueExists
	}

	if currentValue != nil {
		return a.definition.errKeyExists
	}

	if err := keysBucket.Put(k, v); err != nil {
		return fmt.Errorf("put key: %w", err)
	}
	if err := valuesBucket.Put(v, k); err != nil {
		return fmt.Errorf("put value: %w", err)
	}

	return nil
}

// DeleteByKey removes the relation that contains the provided key. If ensure
// flag is set to true and the key does not exist, configured ErrNotFound is
// returned.
func (a *Association[K, V]) DeleteByKey(key K, ensure bool) error {
	k, err := a.definition.keyEncoding.Encode(key)
	if err != nil {
		return fmt.Errorf("encode key: %w", err)
	}

	keysBucket, err := a.keysBucket(true)
	if err != nil {
		return fmt.Errorf("keys bucket: %w", err)
	}

	v := keysBucket.Get(k)
	if v == nil {
		if ensure {
			return a.definition.errNotFound
		}
		return nil
	}

	if err := keysBucket.Delete(k); err != nil {
		return fmt.Errorf("delete key: %w", err)
	}

	valuesBucket, err := a.valuesBucket(true)
	if err != nil {
		return fmt.Errorf("values bucket: %w", err)
	}

	if err := valuesBucket.Delete(v); err != nil {
		return fmt.Errorf("delete value: %w", err)
	}

	return nil
}

// DeleteByValue removes the relation that contains the provided value. If
// ensure flag is set to true and the value does not exist, ErrNotFound is
// returned.
func (a *Association[K, V]) DeleteByValue(value V, ensure bool) error {
	v, err := a.definition.valueEncoding.Encode(value)
	if err != nil {
		return fmt.Errorf("encode value: %w", err)
	}

	valuesBucket, err := a.valuesBucket(true)
	if err != nil {
		return fmt.Errorf("values bucket: %w", err)
	}

	k := valuesBucket.Get(v)
	if k == nil {
		if ensure {
			return a.definition.errNotFound
		}
		return nil
	}

	keysBucket, err := a.keysBucket(true)
	if err != nil {
		return fmt.Errorf("keys bucket: %w", err)
	}

	if err := keysBucket.Delete(k); err != nil {
		return fmt.Errorf("delete key: %w", err)
	}

	if err := valuesBucket.Delete(v); err != nil {
		return fmt.Errorf("delete value: %w", err)
	}

	return nil
}

// Iterate iterates over keys and values in the lexicographical order of keys.
func (a *Association[K, V]) Iterate(start *K, reverse bool, f func(K, V) (bool, error)) (next *K, err error) {
	keysBucket, err := a.keysBucket(false)
	if err != nil {
		return nil, fmt.Errorf("keys bucket: %w", err)
	}
	if keysBucket == nil {
		return nil, nil
	}
	return iterateKeys(keysBucket, a.definition.keyEncoding, start, reverse, func(k, v []byte) (bool, error) {
		key, err := a.definition.keyEncoding.Decode(k)
		if err != nil {
			return false, fmt.Errorf("decode key: %w", err)
		}

		value, err := a.definition.valueEncoding.Decode(v)
		if err != nil {
			return false, fmt.Errorf("decode value: %w", err)
		}

		return f(key, value)
	})
}

// IterateKeys iterates over keys in the lexicographical order of keys.
func (a *Association[K, V]) IterateKeys(start *K, reverse bool, f func(K) (bool, error)) (next *K, err error) {
	keysBucket, err := a.keysBucket(false)
	if err != nil {
		return nil, fmt.Errorf("keys bucket: %w", err)
	}
	if keysBucket == nil {
		return nil, nil
	}
	return iterateKeys(keysBucket, a.definition.keyEncoding, start, reverse, func(k, _ []byte) (bool, error) {
		key, err := a.definition.keyEncoding.Decode(k)
		if err != nil {
			return false, fmt.Errorf("decode key: %w", err)
		}

		return f(key)
	})
}

// IterateValues iterates over values in the lexicographical order of values.
func (a *Association[K, V]) IterateValues(start *V, reverse bool, f func(V) (bool, error)) (next *V, err error) {
	valuesBucket, err := a.valuesBucket(false)
	if err != nil {
		return nil, fmt.Errorf("values bucket: %w", err)
	}
	if valuesBucket == nil {
		return nil, nil
	}
	return iterateKeys(valuesBucket, a.definition.valueEncoding, start, reverse, func(v, _ []byte) (bool, error) {
		value, err := a.definition.valueEncoding.Decode(v)
		if err != nil {
			return false, fmt.Errorf("decode value: %w", err)
		}

		return f(value)
	})
}

// Page returns at most a limit of elements of keys and values at the provided
// page number.
func (a *Association[K, V]) Page(number, limit int, reverse bool) (s []Element[K, V], totalElements, pages int, err error) {
	keysBucket, err := a.keysBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("keys bucket: %w", err)
	}
	if keysBucket == nil {
		return nil, 0, 0, nil
	}
	return page(keysBucket, false, number, limit, reverse, func(k, v []byte) (e Element[K, V], err error) {
		key, err := a.definition.keyEncoding.Decode(k)
		if err != nil {
			return e, fmt.Errorf("key value: %w", err)
		}

		value, err := a.definition.valueEncoding.Decode(v)
		if err != nil {
			return e, fmt.Errorf("decode value: %w", err)
		}

		return newElement(key, value)
	})
}

// PageOfKeys returns at most a limit of keys at the provided page number.
func (a *Association[K, V]) PageOfKeys(number, limit int, reverse bool) (s []K, totalElements, pages int, err error) {
	keysBucket, err := a.keysBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("keys bucket: %w", err)
	}
	if keysBucket == nil {
		return nil, 0, 0, nil
	}
	return page(keysBucket, false, number, limit, reverse, func(k, _ []byte) (key K, err error) {
		return a.definition.keyEncoding.Decode(k)
	})
}

// PageOfValues returns at most a limit of values at the provided page number.
func (a *Association[K, V]) PageOfValues(number, limit int, reverse bool) (s []V, totalElements, pages int, err error) {
	valuesBucket, err := a.valuesBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("values bucket: %w", err)
	}
	if valuesBucket == nil {
		return nil, 0, 0, nil
	}
	return page(valuesBucket, false, number, limit, reverse, func(v, _ []byte) (key V, err error) {
		return a.definition.valueEncoding.Decode(v)
	})
}
