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

// AssociationsDefinition defines a set of Associations, each identified by an
// unique key. All associations have the same key and value encodings.
type AssociationsDefinition[A, K, V any] struct {
	bucketNameKeys         []byte
	bucketNameValues       []byte
	bucketNameKeysIndex    []byte
	associationKeyEncoding Encoding[A]
	keyEncoding            Encoding[K]
	valueEncoding          Encoding[V]
	uniqueKeys             bool
	errAssociationNotFound error
	errNotFound            error
	errKeyExists           error
	errValueExists         error
}

// AssociationsOptions provides additional configuration for an Association
// instance.
type AssociationsOptions struct {
	// UniqueKeys marks if a key can be added only to a single association.
	UniqueKeys bool
	// ErrAssociationNotFound is returned if the association identified by the
	// key is not found.
	ErrAssociationNotFound error
	// ErrNotFound is returned if the key or value is not found in the same
	// Association.
	ErrNotFound error
	// ErrKeyExists is returned if UniqueValues option is set to true and the
	// key already exists in another association. Also if the key exists in the
	// same association.
	ErrKeyExists error
	// ErrValueExists is returned if the value already exists in the same
	// association.
	ErrValueExists error
}

// NewAssociationsDefinition constructs a new AssociationsDefinition with a
// unique name and association key, key and value encodings.
func NewAssociationsDefinition[A, K, V any](
	name string,
	associationKeyEncoding Encoding[A],
	keyEncoding Encoding[K],
	valueEncoding Encoding[V],
	o *AssociationsOptions,
) *AssociationsDefinition[A, K, V] {
	if o == nil {
		o = new(AssociationsOptions)
	}
	return &AssociationsDefinition[A, K, V]{
		bucketNameKeys:         []byte("boltron: associations: " + name + " keys"),
		bucketNameValues:       []byte("boltron: associations: " + name + " values"),
		bucketNameKeysIndex:    []byte("boltron: associations: " + name + " keys index"),
		associationKeyEncoding: associationKeyEncoding,
		keyEncoding:            keyEncoding,
		valueEncoding:          valueEncoding,
		uniqueKeys:             o.UniqueKeys,
		errAssociationNotFound: withDefaultError(o.ErrAssociationNotFound, ErrNotFound),
		errNotFound:            withDefaultError(o.ErrNotFound, ErrNotFound),
		errKeyExists:           withDefaultError(o.ErrKeyExists, ErrKeyExists),
		errValueExists:         withDefaultError(o.ErrValueExists, ErrValueExists),
	}
}

// Associations returns an Associations instance that has access to the stored
// data through the bolt transaction.
func (d *AssociationsDefinition[A, K, V]) Associations(tx *bolt.Tx) *Associations[A, K, V] {
	return &Associations[A, K, V]{
		tx:         tx,
		definition: d,
	}
}

// Associations provides methods to access and change a set of Associations.
type Associations[A, K, V any] struct {
	tx                   *bolt.Tx
	keysIndexBucketCache *bolt.Bucket
	keysBucketCache      *bolt.Bucket
	valuesBucketCache    *bolt.Bucket
	definition           *AssociationsDefinition[A, K, V]
}

func (a *Associations[A, K, V]) keysIndexBucket(create bool) (*bolt.Bucket, error) {
	if a.keysIndexBucketCache != nil {
		return a.keysIndexBucketCache, nil
	}
	bucket, err := rootBucket(a.tx, create, a.definition.bucketNameKeysIndex)
	if err != nil {
		return nil, err
	}
	a.keysIndexBucketCache = bucket
	return bucket, nil
}

func (a *Associations[A, K, V]) keysBucket(create bool) (*bolt.Bucket, error) {
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

func (a *Associations[A, K, V]) valuesBucket(create bool) (*bolt.Bucket, error) {
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

// Association returns an Association instance that is associated with the
// provided association key. If the returned value of exists is false, the
// association still does not exist but it will be created if a value is added
// to it.
func (a *Associations[A, K, V]) Association(key A) (association *Association[K, V], exists bool, err error) {
	ak, err := a.definition.associationKeyEncoding.Encode(key)
	if err != nil {
		return nil, false, fmt.Errorf("encode association key: %w", err)
	}
	keysBucket, err := a.keysBucket(false)
	if err != nil {
		return nil, false, fmt.Errorf("keys index bucket: %w", err)
	}
	exists = keysBucket != nil && keysBucket.Bucket(ak) != nil

	return &Association[K, V]{
		tx: a.tx,
		definition: &AssociationDefinition[K, V]{
			bucketPathKeys:   [][]byte{a.definition.bucketNameKeys, ak},
			bucketPathValues: [][]byte{a.definition.bucketNameValues, ak},
			keyEncoding:      a.definition.keyEncoding,
			valueEncoding:    a.definition.valueEncoding,
			errNotFound:      a.definition.errNotFound,
			errKeyExists:     a.definition.errKeyExists,
			errValueExists:   a.definition.errValueExists,
			setCallback: func(key []byte) error {
				keysIndexBucket, err := a.keysIndexBucket(true)
				if err != nil {
					return fmt.Errorf("keys index bucket: %w", err)
				}
				keyIndex := keysIndexBucket.Bucket(key)
				if keyIndex != nil {
					if a.definition.uniqueKeys {
						firstKey, _ := keyIndex.Cursor().First()
						if firstKey != nil && !bytes.Equal(firstKey, ak) {
							return a.definition.errValueExists
						}
					}
				} else {
					b, err := keysIndexBucket.CreateBucket(key)
					if err != nil {
						return fmt.Errorf("create key index bucket: %w", err)
					}
					keyIndex = b
				}
				return keyIndex.Put(ak, nil)
			},
		},
	}, exists, nil
}

// HasAssociation returns true if the Association associated with the
// association key already exists in the database.
func (a *Associations[A, K, V]) HasAssociation(key A) (bool, error) {
	ak, err := a.definition.associationKeyEncoding.Encode(key)
	if err != nil {
		return false, fmt.Errorf("encode association key: %w", err)
	}

	keysBucket, err := a.keysBucket(false)
	if err != nil {
		return false, fmt.Errorf("keys bucket: %w", err)
	}
	if keysBucket == nil {
		return false, nil
	}

	return keysBucket.Bucket(ak) != nil, nil
}

// HasKey returns true if the key already exists in any Association.
func (a *Associations[A, K, V]) HasKey(key K) (bool, error) {
	k, err := a.definition.keyEncoding.Encode(key)
	if err != nil {
		return false, fmt.Errorf("encode key: %w", err)
	}

	keysIndexBucket, err := a.keysIndexBucket(false)
	if err != nil {
		return false, fmt.Errorf("keys index bucket: %w", err)
	}
	if keysIndexBucket == nil {
		return false, nil
	}

	return keysIndexBucket.Bucket(k) != nil, nil
}

// DeleteAssociation removes the association from the database. If ensure flag
// is set to true and the value does not exist, configured
// ErrAssociationNotFound is returned.
func (a *Associations[A, K, V]) DeleteAssociation(key A, ensure bool) error {
	ak, err := a.definition.associationKeyEncoding.Encode(key)
	if err != nil {
		return fmt.Errorf("encode association key: %w", err)
	}

	keysBucket, err := a.keysBucket(false)
	if err != nil {
		return fmt.Errorf("keys bucket: %w", err)
	}
	if keysBucket == nil {
		if ensure {
			return a.definition.errAssociationNotFound
		}
		return nil
	}

	keyBucket := keysBucket.Bucket(ak)
	if keyBucket == nil {
		if ensure {
			return a.definition.errAssociationNotFound
		}
		return nil
	}

	valuesBucket, err := a.valuesBucket(false)
	if err != nil {
		return fmt.Errorf("values bucket: %w", err)
	}
	if valuesBucket == nil {
		if ensure {
			return a.definition.errAssociationNotFound
		}
		return nil
	}

	keysIndexBucket, err := a.keysIndexBucket(false)
	if err != nil {
		return fmt.Errorf("keys index bucket: %w", err)
	}
	if keysIndexBucket == nil {
		return errors.New("keys index bucket does not exist")
	}

	if err := keyBucket.ForEach(func(k, _ []byte) error {
		keyIndexBucket := keysIndexBucket.Bucket(k)
		if keyIndexBucket == nil {
			return nil
		}
		if err := keyIndexBucket.Delete(ak); err != nil {
			return fmt.Errorf("delete key from keys index bucket: %w", err)
		}
		if keyIndexBucket.Stats().KeyN == 1 { // stats are updated after the transaction
			if err := keysIndexBucket.DeleteBucket(k); err != nil {
				return fmt.Errorf("delete association key from key index bucket: %w", err)
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("delete association key in keys index buckets: %w", err)
	}

	if err := keysBucket.DeleteBucket(ak); err != nil {
		return fmt.Errorf("delete keys bucket: %w", err)
	}

	if err := valuesBucket.DeleteBucket(ak); err != nil {
		return fmt.Errorf("delete values bucket: %w", err)
	}

	return nil
}

// DeleteKey removes the value from all associations that contain it. If ensure
// flag is set to true and the value does not exist, configured ErrNotFound is
// returned.
func (a *Associations[A, K, V]) DeleteKey(key K, ensure bool) error {
	k, err := a.definition.keyEncoding.Encode(key)
	if err != nil {
		return fmt.Errorf("encode key: %w", err)
	}

	keysIndexBucket, err := a.keysIndexBucket(false)
	if err != nil {
		return fmt.Errorf("lists bucket: %w", err)
	}

	keyIndexBucket := keysIndexBucket.Bucket(k)
	if keyIndexBucket == nil {
		if ensure {
			return a.definition.errNotFound
		}
		return nil
	}

	keysBucket, err := a.keysBucket(false)
	if err != nil {
		return fmt.Errorf("values bucket: %w", err)
	}
	if keysBucket == nil {
		if ensure {
			return a.definition.errNotFound
		}
		return nil
	}

	valuesBucket, err := a.valuesBucket(false)
	if err != nil {
		return fmt.Errorf("values bucket: %w", err)
	}
	if valuesBucket == nil {
		if ensure {
			return a.definition.errNotFound
		}
		return nil
	}

	if keysBucket != nil && valuesBucket != nil {
		association := (&AssociationDefinition[K, V]{
			keyEncoding:   a.definition.keyEncoding,
			valueEncoding: a.definition.valueEncoding,
			errNotFound:   a.definition.errNotFound,
		}).Association(nil)

		if err := keyIndexBucket.ForEach(func(ak, _ []byte) error {
			association.keysBucketCache = keysBucket.Bucket(ak)
			association.valuesBucketCache = valuesBucket.Bucket(ak)
			return association.DeleteByKey(key, false)
		}); err != nil {
			return fmt.Errorf("delete key in keys bucket: %w", err)
		}
	}

	if err := keysIndexBucket.DeleteBucket(k); err != nil {
		return fmt.Errorf("delete key bucket: %w", err)
	}

	return nil
}

// IterateAssociations iterates over Association keys in the lexicographical
// order of keys.
func (a *Associations[A, K, V]) IterateAssociations(start *A, reverse bool, f func(A) (bool, error)) (next *A, err error) {
	keysBucket, err := a.keysBucket(false)
	if err != nil {
		return nil, fmt.Errorf("keys bucket: %w", err)
	}
	if keysBucket == nil {
		return nil, nil
	}
	return iterateKeys(keysBucket, a.definition.associationKeyEncoding, start, reverse, func(ak, _ []byte) (bool, error) {
		key, err := a.definition.associationKeyEncoding.Decode(ak)
		if err != nil {
			return false, fmt.Errorf("decode association key: %w", err)
		}

		return f(key)
	})
}

// PageOfAssociations returns at most a limit of Association keys at the
// provided page number.
func (a *Associations[A, K, V]) PageOfAssociations(number, limit int, reverse bool) (s []A, totalElements, pages int, err error) {
	keysBucket, err := a.keysBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("keys bucket: %w", err)
	}
	if keysBucket == nil {
		return nil, 0, 0, nil
	}
	return page(keysBucket, true, number, limit, reverse, func(ak, _ []byte) (A, error) {
		return a.definition.associationKeyEncoding.Decode(ak)
	})
}

// IterateAssociationsWithKey iterates over Association keys that contain the
// provided value in the lexicographical order of keys.
func (a *Associations[A, K, V]) IterateAssociationsWithKey(key K, start *A, reverse bool, f func(A) (bool, error)) (next *A, err error) {
	k, err := a.definition.keyEncoding.Encode(key)
	if err != nil {
		return nil, fmt.Errorf("encode key: %w", err)
	}
	keysIndexBucket, err := a.keysIndexBucket(false)
	if err != nil {
		return nil, fmt.Errorf("keys index bucket: %w", err)
	}
	if keysIndexBucket == nil {
		return nil, nil
	}
	keyIndexBucket := keysIndexBucket.Bucket(k)
	if keyIndexBucket == nil {
		return nil, nil
	}
	return iterateKeys(keyIndexBucket, a.definition.associationKeyEncoding, start, reverse, func(ak, _ []byte) (bool, error) {
		key, err := a.definition.associationKeyEncoding.Decode(ak)
		if err != nil {
			return false, fmt.Errorf("decode association key: %w", err)
		}

		return f(key)
	})
}

// PageOfAssociationsWithKey returns at most a limit of Association keys that
// contain the provided key at the provided page number.
func (a *Associations[A, K, V]) PageOfAssociationsWithKey(key K, number, limit int, reverse bool) (s []A, totalElements, pages int, err error) {
	k, err := a.definition.keyEncoding.Encode(key)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("encode key: %w", err)
	}
	keysIndexBucket, err := a.keysIndexBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("keys index bucket: %w", err)
	}
	if keysIndexBucket == nil {
		return nil, 0, 0, nil
	}
	keyIndexBucket := keysIndexBucket.Bucket(k)
	if keyIndexBucket == nil {
		return nil, 0, 0, nil
	}
	return page(keyIndexBucket, false, number, limit, reverse, func(k, _ []byte) (A, error) {
		return a.definition.associationKeyEncoding.Decode(k)
	})
}

// IterateKeys iterates over all keys in the lexicographical order of values.
func (a *Associations[A, K, V]) IterateKeys(start *K, reverse bool, f func(K) (bool, error)) (next *K, err error) {
	keysIndexBucket, err := a.keysIndexBucket(false)
	if err != nil {
		return nil, fmt.Errorf("keys index bucket: %w", err)
	}
	if keysIndexBucket == nil {
		return nil, nil
	}
	return iterateKeys(keysIndexBucket, a.definition.keyEncoding, start, reverse, func(v, _ []byte) (bool, error) {
		key, err := a.definition.keyEncoding.Decode(v)
		if err != nil {
			return false, fmt.Errorf("decode key: %w", err)
		}

		return f(key)
	})
}

// PageOfKeys returns at most a limit of keys at the provided page number.
func (a *Associations[A, K, V]) PageOfKeys(number, limit int, reverse bool) (s []K, totalElements, pages int, err error) {
	keysIndexBucket, err := a.keysIndexBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("keys index bucket: %w", err)
	}
	if keysIndexBucket == nil {
		return nil, 0, 0, nil
	}
	return page(keysIndexBucket, true, number, limit, reverse, func(v, _ []byte) (K, error) {
		return a.definition.keyEncoding.Decode(v)
	})
}
