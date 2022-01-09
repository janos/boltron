// Copyright (c) 2022, Janoš Guljaš <janos@resenje.org>
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

// CollectionsDefinition defines a set of Collections, each identified by an
// unique collection key. All collections have the same key and value encodings.
type CollectionsDefinition[C, K, V any] struct {
	bucketNameCollections []byte
	bucketNameKeys        []byte
	collectionKeyEncoding Encoding[C]
	keyEncoding           Encoding[K]
	valueEncoding         Encoding[V]
	fillPercent           float64
	uniqueKeys            bool
	errCollectionNotFound error
	errKeyNotFound        error
	errKeyExists          error
}

// CollectionsOptions provides additional configuration for a Collections
// instance.
type CollectionsOptions struct {
	// FillPercent is the value for the bolt bucket fill percent for every
	// collection.
	FillPercent float64
	// UniqueKeys marks if a key can be added only to a single collection.
	UniqueKeys bool
	// ErrCollectionNotFound is returned if the collection identified by its key
	// is not found.
	ErrCollectionNotFound error
	// ErrKeyNotFound is returned if the key is not found.
	ErrKeyNotFound error
	// ErrKeyExists is returned if UniqueValues option is set to true and the
	// key already exists in another collection.
	ErrKeyExists error
}

// NewCollectionsDefinition constructs a new CollectionsDefinition with a unique
// name and collection key, key and value encodings.
func NewCollectionsDefinition[C, K, V any](
	name string,
	collectionKeyEncoding Encoding[C],
	keyEncoding Encoding[K],
	valueEncoding Encoding[V],
	o *CollectionsOptions,
) *CollectionsDefinition[C, K, V] {
	if o == nil {
		o = new(CollectionsOptions)
	}
	return &CollectionsDefinition[C, K, V]{
		bucketNameCollections: []byte("boltron: collections: " + name + " collections"),
		bucketNameKeys:        []byte("boltron: collections: " + name + " keys"),
		collectionKeyEncoding: collectionKeyEncoding,
		keyEncoding:           keyEncoding,
		valueEncoding:         valueEncoding,
		fillPercent:           o.FillPercent,
		uniqueKeys:            o.UniqueKeys,
		errCollectionNotFound: withDefaultError(o.ErrCollectionNotFound, ErrNotFound),
		errKeyNotFound:        withDefaultError(o.ErrKeyNotFound, ErrNotFound),
		errKeyExists:          withDefaultError(o.ErrKeyExists, ErrKeyExists),
	}
}

// Collections returns a Collections instance that has access to the stored data
// through the bolt transaction.
func (d *CollectionsDefinition[C, K, V]) Collections(tx *bolt.Tx) *Collections[C, K, V] {
	return &Collections[C, K, V]{
		tx:         tx,
		definition: d,
	}
}

// Collections provides methods to access and change a set of Collections.
type Collections[C, K, V any] struct {
	tx                     *bolt.Tx
	collectionsBucketCache *bolt.Bucket
	keysBucketCache        *bolt.Bucket
	definition             *CollectionsDefinition[C, K, V]
}

func (c *Collections[C, K, V]) collectionsBucket(create bool) (*bolt.Bucket, error) {
	if c.collectionsBucketCache != nil {
		return c.collectionsBucketCache, nil
	}
	bucket, err := rootBucket(c.tx, create, c.definition.bucketNameCollections)
	if err != nil {
		return nil, err
	}
	c.collectionsBucketCache = bucket
	return bucket, nil
}

func (c *Collections[C, K, V]) keysBucket(create bool) (*bolt.Bucket, error) {
	if c.keysBucketCache != nil {
		return c.keysBucketCache, nil
	}
	bucket, err := rootBucket(c.tx, create, c.definition.bucketNameKeys)
	if err != nil {
		return nil, err
	}
	c.keysBucketCache = bucket
	return bucket, nil
}

// Collections returns a Collections instance that is associated with the
// provided collection key. If the returned value of exists is false, the
// collection still does not exist but it will be created if a key/value pair is
// saved to it.
func (c *Collections[C, K, V]) Collection(key C) (collection *Collection[K, V], exists bool, err error) {
	k, err := c.definition.collectionKeyEncoding.Encode(key)
	if err != nil {
		return nil, false, fmt.Errorf("encode collection key: %w", err)
	}
	collectionsBucket, err := c.collectionsBucket(false)
	if err != nil {
		return nil, false, fmt.Errorf("collections bucket: %w", err)
	}
	exists = collectionsBucket != nil && collectionsBucket.Bucket(k) != nil

	return &Collection[K, V]{
		tx: c.tx,
		definition: &CollectionDefinition[K, V]{
			bucketPath:    [][]byte{c.definition.bucketNameCollections, k},
			keyEncoding:   c.definition.keyEncoding,
			valueEncoding: c.definition.valueEncoding,
			fillPercent:   c.definition.fillPercent,
			errNotFound:   c.definition.errKeyNotFound,
			saveCallback: func(key []byte) error {
				keysBucket, err := c.keysBucket(true)
				if err != nil {
					return fmt.Errorf("keys bucket: %w", err)
				}
				keyBucket := keysBucket.Bucket(key)
				if keyBucket != nil {
					if c.definition.uniqueKeys {
						firstKey, _ := keyBucket.Cursor().First()
						if firstKey != nil && !bytes.Equal(firstKey, k) {
							return c.definition.errKeyExists
						}
					}
				} else {
					b, err := keysBucket.CreateBucket(key)
					if err != nil {
						return fmt.Errorf("create key bucket: %w", err)
					}
					keyBucket = b
				}
				return keyBucket.Put(k, nil)
			},
		},
	}, exists, nil
}

// HasCollection returns true if the Collection associated with the collection
// key already exists in the database.
func (c *Collections[C, K, V]) HasCollection(key C) (bool, error) {
	ck, err := c.definition.collectionKeyEncoding.Encode(key)
	if err != nil {
		return false, fmt.Errorf("encode collection key: %w", err)
	}

	collectionsBucket, err := c.collectionsBucket(false)
	if err != nil {
		return false, fmt.Errorf("collections bucket: %w", err)
	}
	if collectionsBucket == nil {
		return false, nil
	}

	return collectionsBucket.Bucket(ck) != nil, nil
}

// HasKey returns true if the key already exists in any Collection.
func (c *Collections[C, K, V]) HasKey(key K) (bool, error) {
	k, err := c.definition.keyEncoding.Encode(key)
	if err != nil {
		return false, fmt.Errorf("encode key: %w", err)
	}

	keysBucket, err := c.keysBucket(false)
	if err != nil {
		return false, fmt.Errorf("keys bucket: %w", err)
	}
	if keysBucket == nil {
		return false, nil
	}

	return keysBucket.Bucket(k) != nil, nil
}

// DeleteCollection removes the collection from the database. If ensure flag is
// set to true and the value does not exist, configured ErrCollectionNotFound is
// returned.
func (c *Collections[C, K, V]) DeleteCollection(key C, ensure bool) error {
	ck, err := c.definition.collectionKeyEncoding.Encode(key)
	if err != nil {
		return fmt.Errorf("encode collection key: %w", err)
	}

	collectionsBucket, err := c.collectionsBucket(false)
	if err != nil {
		return fmt.Errorf("collections bucket: %w", err)
	}
	if collectionsBucket == nil {
		if ensure {
			return c.definition.errCollectionNotFound
		}
		return nil
	}

	collectionBucket := collectionsBucket.Bucket(ck)
	if collectionBucket == nil {
		if ensure {
			return c.definition.errCollectionNotFound
		}
		return nil
	}

	keysBucket, err := c.keysBucket(false)
	if err != nil {
		return fmt.Errorf("keys bucket: %w", err)
	}
	if keysBucket == nil {
		return errors.New("keys bucket does not exist")
	}

	if err := collectionBucket.ForEach(func(k, _ []byte) error {
		keyBucket := keysBucket.Bucket(k)
		if keyBucket == nil {
			return nil
		}
		if keyBucket.Get(ck) == nil {
			return nil
		}
		if err := keyBucket.Delete(ck); err != nil {
			return fmt.Errorf("delete collection key from key bucket: %w", err)
		}
		if keyBucket.Stats().KeyN == 1 { // stats are updated after the transaction
			if err := keysBucket.DeleteBucket(k); err != nil {
				return fmt.Errorf("delete bucket from keys bucket: %w", err)
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("delete key in keys buckets: %w", err)
	}

	if err := collectionsBucket.DeleteBucket(ck); err != nil {
		return fmt.Errorf("delete collection bucket: %w", err)
	}

	return nil
}

// DeleteKey removes the key from all collections that contain it. If ensure
// flag is set to true and the key does not exist, configured ErrKeyNotFound is
// returned.
func (c *Collections[C, K, V]) DeleteKey(key K, ensure bool) error {
	k, err := c.definition.keyEncoding.Encode(key)
	if err != nil {
		return fmt.Errorf("encode key: %w", err)
	}

	keysBucket, err := c.keysBucket(false)
	if err != nil {
		return fmt.Errorf("keys bucket: %w", err)
	}
	if keysBucket == nil {
		if ensure {
			return c.definition.errKeyNotFound
		}
		return nil
	}

	keyBucket := keysBucket.Bucket(k)
	if keyBucket == nil {
		if ensure {
			return c.definition.errKeyNotFound
		}
		return nil
	}

	collectionsBucket, err := c.collectionsBucket(false)
	if err != nil {
		return fmt.Errorf("collections bucket: %w", err)
	}

	if collectionsBucket != nil {
		collection := (&CollectionDefinition[K, V]{
			keyEncoding:   c.definition.keyEncoding,
			valueEncoding: c.definition.valueEncoding,
			errNotFound:   c.definition.errKeyNotFound,
			errKeyExists:  c.definition.errKeyExists,
		}).Collection(nil)

		if err := keyBucket.ForEach(func(k, _ []byte) error {
			collection.bucketCache = collectionsBucket.Bucket(k)
			return collection.Delete(key, false)
		}); err != nil {
			return fmt.Errorf("delete key in collection bucket: %w", err)
		}
	}

	if err := keysBucket.DeleteBucket(k); err != nil {
		return fmt.Errorf("delete key bucket: %w", err)
	}

	return nil
}

// IterateCollections iterates over collection keys in the lexicographical order
// of keys.
func (c *Collections[C, K, V]) IterateCollections(start *C, reverse bool, f func(C) (bool, error)) (next *C, err error) {
	collectionsBucket, err := c.collectionsBucket(false)
	if err != nil {
		return nil, fmt.Errorf("collections bucket: %w", err)
	}
	if collectionsBucket == nil {
		return nil, nil
	}
	return iterateKeys(collectionsBucket, c.definition.collectionKeyEncoding, start, reverse, func(k, _ []byte) (bool, error) {
		key, err := c.definition.collectionKeyEncoding.Decode(k)
		if err != nil {
			return false, fmt.Errorf("decode collection key: %w", err)
		}

		return f(key)
	})
}

// PageOfCollections returns at most a limit of collection keys at the provided
// page number.
func (c *Collections[C, K, V]) PageOfCollections(number, limit int, reverse bool) (s []C, totalElements, pages int, err error) {
	collectionsBucket, err := c.collectionsBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("collections bucket: %w", err)
	}
	if collectionsBucket == nil {
		return nil, 0, 0, nil
	}
	return page(collectionsBucket, true, number, limit, reverse, func(k, _ []byte) (C, error) {
		return c.definition.collectionKeyEncoding.Decode(k)
	})
}

// IterateCollectionsWithKey iterates over collection keys that contain the
// provided key in the lexicographical order of collection keys.
func (c *Collections[C, K, V]) IterateCollectionsWithKey(key K, start *C, reverse bool, f func(C) (bool, error)) (next *C, err error) {
	k, err := c.definition.keyEncoding.Encode(key)
	if err != nil {
		return nil, fmt.Errorf("encode key: %w", err)
	}
	keysBucket, err := c.keysBucket(false)
	if err != nil {
		return nil, fmt.Errorf("keys bucket: %w", err)
	}
	if keysBucket == nil {
		return nil, nil
	}
	keyBucket := keysBucket.Bucket(k)
	if keyBucket == nil {
		return nil, nil
	}
	return iterateKeys(keyBucket, c.definition.collectionKeyEncoding, start, reverse, func(k, _ []byte) (bool, error) {
		key, err := c.definition.collectionKeyEncoding.Decode(k)
		if err != nil {
			return false, fmt.Errorf("decode collection key: %w", err)
		}

		return f(key)
	})
}

// PageOfCollectionsWithKey returns at most a limit of collection keys that
// contain the provided key at the provided page number.
func (c *Collections[C, K, V]) PageOfCollectionsWithKey(key K, number, limit int, reverse bool) (s []C, totalElements, pages int, err error) {
	k, err := c.definition.keyEncoding.Encode(key)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("encode key: %w", err)
	}
	keysBucket, err := c.keysBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("keys bucket: %w", err)
	}
	if keysBucket == nil {
		return nil, 0, 0, nil
	}
	keyBucket := keysBucket.Bucket(k)
	if keyBucket == nil {
		return nil, 0, 0, nil
	}
	return page(keyBucket, false, number, limit, reverse, func(k, _ []byte) (C, error) {
		return c.definition.collectionKeyEncoding.Decode(k)
	})
}

// IterateKeys iterates over all keys in the lexicographical order of keys.
func (c *Collections[C, K, V]) IterateKeys(start *K, reverse bool, f func(K) (bool, error)) (next *K, err error) {
	keysBucket, err := c.keysBucket(false)
	if err != nil {
		return nil, fmt.Errorf("keys bucket: %w", err)
	}
	if keysBucket == nil {
		return nil, nil
	}
	return iterateKeys(keysBucket, c.definition.keyEncoding, start, reverse, func(k, _ []byte) (bool, error) {
		key, err := c.definition.keyEncoding.Decode(k)
		if err != nil {
			return false, fmt.Errorf("decode key: %w", err)
		}

		return f(key)
	})
}

// PageOfKeys returns at most a limit of keys at the provided page number.
func (c *Collections[C, K, V]) PageOfKeys(number, limit int, reverse bool) (s []K, totalElements, pages int, err error) {
	keysBucket, err := c.keysBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("keys bucket: %w", err)
	}
	if keysBucket == nil {
		return nil, 0, 0, nil
	}
	return page(keysBucket, true, number, limit, reverse, func(k, _ []byte) (K, error) {
		return c.definition.keyEncoding.Decode(k)
	})
}
