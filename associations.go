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
// unique key. All associations have the same left and right value encodings.
type AssociationsDefinition[A, L, R any] struct {
	bucketNameLeft         []byte
	bucketNameRight        []byte
	bucketNameLeftIndex    []byte
	associationKeyEncoding Encoding[A]
	leftEncoding           Encoding[L]
	rightEncoding          Encoding[R]
	uniqueLeftValues       bool
	errAssociationNotFound error
	errLeftNotFound        error
	errRightNotFound       error
	errLeftExists          error
	errRightExists         error
}

// AssociationsOptions provides additional configuration for an Association
// instance.
type AssociationsOptions struct {
	// UniqueLeftValues marks if left value can be added only to a single
	// association.
	UniqueLeftValues bool
	// ErrAssociationNotFound is returned if the association identified by the
	// key is not found.
	ErrAssociationNotFound error
	// ErrLeftNotFound is returned if left value is not found in the same
	// Association.
	ErrLeftNotFound error
	// ErrRightNotFound is returned if right value is not found in the same
	// Association.
	ErrRightNotFound error
	// ErrLeftExists is returned if UniqueValues option is set to true and the
	// left value already exists in another association. Also if the left value
	// exists in the same association.
	ErrLeftExists error
	// ErrRightExists is returned if the right value in relation already exists.
	ErrRightExists error
}

// NewAssociationsDefinition constructs a new AssociationsDefinition with a
// unique name and association key, left and right value encodings.
func NewAssociationsDefinition[A, L, R any](
	name string,
	associationKeyEncoding Encoding[A],
	leftEncoding Encoding[L],
	rightEncoding Encoding[R],
	o *AssociationsOptions,
) *AssociationsDefinition[A, L, R] {
	if o == nil {
		o = new(AssociationsOptions)
	}
	return &AssociationsDefinition[A, L, R]{
		bucketNameLeft:         []byte("boltron: associations: " + name + " left"),
		bucketNameRight:        []byte("boltron: associations: " + name + " right"),
		bucketNameLeftIndex:    []byte("boltron: associations: " + name + " left index"),
		associationKeyEncoding: associationKeyEncoding,
		leftEncoding:           leftEncoding,
		rightEncoding:          rightEncoding,
		uniqueLeftValues:       o.UniqueLeftValues,
		errAssociationNotFound: withDefaultError(o.ErrAssociationNotFound, ErrNotFound),
		errLeftNotFound:        withDefaultError(o.ErrLeftNotFound, ErrLeftNotFound),
		errRightNotFound:       withDefaultError(o.ErrRightNotFound, ErrRightNotFound),
		errLeftExists:          withDefaultError(o.ErrLeftExists, ErrLeftExists),
		errRightExists:         withDefaultError(o.ErrRightExists, ErrRightExists),
	}
}

// Associations returns an Associations instance that has access to the stored
// data through the bolt transaction.
func (d *AssociationsDefinition[A, L, R]) Associations(tx *bolt.Tx) *Associations[A, L, R] {
	return &Associations[A, L, R]{
		tx:         tx,
		definition: d,
	}
}

// Associations provides methods to access and change a set of Associations.
type Associations[A, L, R any] struct {
	tx                    *bolt.Tx
	leftIndexBucketsCache *bolt.Bucket
	leftBucketsCache      *bolt.Bucket
	rightBucketsCache     *bolt.Bucket
	definition            *AssociationsDefinition[A, L, R]
}

func (a *Associations[A, L, R]) leftIndexBuckets(create bool) (*bolt.Bucket, error) {
	if a.leftIndexBucketsCache != nil {
		return a.leftIndexBucketsCache, nil
	}
	bucket, err := rootBucket(a.tx, create, a.definition.bucketNameLeftIndex)
	if err != nil {
		return nil, err
	}
	a.leftIndexBucketsCache = bucket
	return bucket, nil
}

func (a *Associations[A, L, R]) leftBuckets(create bool) (*bolt.Bucket, error) {
	if a.leftBucketsCache != nil {
		return a.leftBucketsCache, nil
	}
	bucket, err := rootBucket(a.tx, create, a.definition.bucketNameLeft)
	if err != nil {
		return nil, err
	}
	a.leftBucketsCache = bucket
	return bucket, nil
}

func (a *Associations[A, L, R]) rightBuckets(create bool) (*bolt.Bucket, error) {
	if a.rightBucketsCache != nil {
		return a.rightBucketsCache, nil
	}
	bucket, err := rootBucket(a.tx, create, a.definition.bucketNameRight)
	if err != nil {
		return nil, err
	}
	a.rightBucketsCache = bucket
	return bucket, nil
}

// Association returns an Association instance that is associated with the
// provided association key. If the returned value of exists is false, the
// association still does not exist but it will be created if a value is added
// to it.
func (a *Associations[A, L, R]) Association(key A) (association *Association[L, R], exists bool, err error) {
	ak, err := a.definition.associationKeyEncoding.Encode(key)
	if err != nil {
		return nil, false, fmt.Errorf("encode association key: %w", err)
	}
	leftBucket, err := a.leftBuckets(false)
	if err != nil {
		return nil, false, fmt.Errorf("left buckets: %w", err)
	}
	exists = leftBucket != nil && leftBucket.Bucket(ak) != nil

	return &Association[L, R]{
		tx: a.tx,
		definition: &AssociationDefinition[L, R]{
			bucketPathLeft:   [][]byte{a.definition.bucketNameLeft, ak},
			bucketPathRight:  [][]byte{a.definition.bucketNameRight, ak},
			leftEncoding:     a.definition.leftEncoding,
			rightEncoding:    a.definition.rightEncoding,
			errLeftNotFound:  a.definition.errLeftNotFound,
			errRightNotFound: a.definition.errRightNotFound,
			errLeftExists:    a.definition.errLeftExists,
			errRightExists:   a.definition.errRightExists,
			setCallback: func(left []byte) error {
				leftIndexBuckets, err := a.leftIndexBuckets(true)
				if err != nil {
					return fmt.Errorf("left index buckets: %w", err)
				}
				leftIndexBucket := leftIndexBuckets.Bucket(left)
				if leftIndexBucket != nil {
					if a.definition.uniqueLeftValues {
						firstKey, _ := leftIndexBucket.Cursor().First()
						if firstKey != nil && !bytes.Equal(firstKey, ak) {
							return a.definition.errLeftExists
						}
					}
				} else {
					b, err := leftIndexBuckets.CreateBucket(left)
					if err != nil {
						return fmt.Errorf("create left index bucket: %w", err)
					}
					leftIndexBucket = b
				}
				return leftIndexBucket.Put(ak, nil)
			},
			deleteCallback: func(left []byte) error {
				leftIndexBuckets, err := a.leftIndexBuckets(false)
				if err != nil {
					return fmt.Errorf("left index buckets: %w", err)
				}
				if leftIndexBuckets == nil {
					return fmt.Errorf("missing associations left index buckets: %w", a.definition.errLeftNotFound)
				}
				leftIndexBucket := leftIndexBuckets.Bucket(left)
				if leftIndexBucket == nil {
					return fmt.Errorf("missing value in associations left index buckets: %w", a.definition.errLeftNotFound)
				}
				if err := leftIndexBucket.Delete(ak); err != nil {
					return fmt.Errorf("delete value from lists values bucket: %w", err)
				}
				if leftIndexBuckets.Stats().KeyN == 1 { // stats are updated after the transaction
					if err := leftIndexBuckets.DeleteBucket(left); err != nil {
						return fmt.Errorf("delete empty left bucket: %w", err)
					}
				}
				return nil
			},
		},
	}, exists, nil
}

// HasAssociation returns true if the Association associated with the
// association key already exists in the database.
func (a *Associations[A, L, R]) HasAssociation(key A) (bool, error) {
	ak, err := a.definition.associationKeyEncoding.Encode(key)
	if err != nil {
		return false, fmt.Errorf("encode association key: %w", err)
	}

	leftBuckets, err := a.leftBuckets(false)
	if err != nil {
		return false, fmt.Errorf("left buckets: %w", err)
	}
	if leftBuckets == nil {
		return false, nil
	}

	if leftBuckets.Bucket(ak) == nil {
		return false, nil
	}

	f, _ := leftBuckets.Bucket(ak).Cursor().First()
	return f != nil, nil
}

// HasLeft returns true if the left value already exists in any Association.
func (a *Associations[A, L, R]) HasLeft(left L) (bool, error) {
	l, err := a.definition.leftEncoding.Encode(left)
	if err != nil {
		return false, fmt.Errorf("encode left: %w", err)
	}

	leftIndexBuckets, err := a.leftIndexBuckets(false)
	if err != nil {
		return false, fmt.Errorf("left index buckets: %w", err)
	}
	if leftIndexBuckets == nil {
		return false, nil
	}

	if leftIndexBuckets.Bucket(l) == nil {
		return false, nil
	}

	f, _ := leftIndexBuckets.Bucket(l).Cursor().First()
	return f != nil, nil
}

// DeleteAssociation removes the association from the database. If ensure flag
// is set to true and the key does not exist, configured ErrAssociationNotFound
// is returned.
func (a *Associations[A, L, R]) DeleteAssociation(key A, ensure bool) error {
	ak, err := a.definition.associationKeyEncoding.Encode(key)
	if err != nil {
		return fmt.Errorf("encode association key: %w", err)
	}

	leftBuckets, err := a.leftBuckets(false)
	if err != nil {
		return fmt.Errorf("left buckets: %w", err)
	}
	if leftBuckets == nil {
		if ensure {
			return a.definition.errAssociationNotFound
		}
		return nil
	}

	leftBucket := leftBuckets.Bucket(ak)
	if leftBucket == nil {
		if ensure {
			return a.definition.errAssociationNotFound
		}
		return nil
	}

	rightBuckets, err := a.rightBuckets(false)
	if err != nil {
		return fmt.Errorf("right buckets: %w", err)
	}
	if rightBuckets == nil {
		if ensure {
			return a.definition.errAssociationNotFound
		}
		return nil
	}

	leftIndexBuckets, err := a.leftIndexBuckets(false)
	if err != nil {
		return fmt.Errorf("left index buckets: %w", err)
	}
	if leftIndexBuckets == nil {
		return errors.New("left index bucket does not exist")
	}

	if err := leftBucket.ForEach(func(l, _ []byte) error {
		leftIndexBucket := leftIndexBuckets.Bucket(l)
		if leftIndexBucket == nil {
			return nil
		}
		if err := leftIndexBucket.Delete(ak); err != nil {
			return fmt.Errorf("delete association key from left index buckets: %w", err)
		}
		if leftIndexBucket.Stats().KeyN == 1 { // stats are updated after the transaction
			if err := leftIndexBuckets.DeleteBucket(l); err != nil {
				return fmt.Errorf("delete association key from left index buckets: %w", err)
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("delete association key in left index buckets: %w", err)
	}

	if err := leftBuckets.DeleteBucket(ak); err != nil {
		return fmt.Errorf("delete left bucket: %w", err)
	}

	if err := rightBuckets.DeleteBucket(ak); err != nil {
		return fmt.Errorf("delete right bucket: %w", err)
	}

	return nil
}

// DeleteLeft removes the left value from all associations that contain it. If
// ensure flag is set to true and the left value does not exist, configured
// ErrNotFound is returned.
func (a *Associations[A, L, R]) DeleteLeft(left L, ensure bool) error {
	l, err := a.definition.leftEncoding.Encode(left)
	if err != nil {
		return fmt.Errorf("encode left: %w", err)
	}

	leftIndexBuckets, err := a.leftIndexBuckets(false)
	if err != nil {
		return fmt.Errorf("left index buckets: %w", err)
	}

	if leftIndexBuckets == nil {
		if ensure {
			return a.definition.errLeftNotFound
		}
		return nil
	}

	leftIndexBucket := leftIndexBuckets.Bucket(l)
	if leftIndexBucket == nil {
		if ensure {
			return a.definition.errLeftNotFound
		}
		return nil
	}

	leftBuckets, err := a.leftBuckets(false)
	if err != nil {
		return fmt.Errorf("right buckets: %w", err)
	}
	if leftBuckets == nil {
		if ensure {
			return a.definition.errLeftNotFound
		}
		return nil
	}

	rightBuckets, err := a.rightBuckets(false)
	if err != nil {
		return fmt.Errorf("right buckets: %w", err)
	}
	if rightBuckets == nil {
		if ensure {
			return a.definition.errRightNotFound
		}
		return nil
	}

	if leftBuckets != nil && rightBuckets != nil {
		association := (&AssociationDefinition[L, R]{
			leftEncoding:     a.definition.leftEncoding,
			rightEncoding:    a.definition.rightEncoding,
			errLeftNotFound:  a.definition.errLeftNotFound,
			errRightNotFound: a.definition.errRightNotFound,
		}).Association(nil)

		if err := leftIndexBucket.ForEach(func(ak, _ []byte) error {
			association.leftBucketCache = leftBuckets.Bucket(ak)
			association.rightBucketCache = rightBuckets.Bucket(ak)
			return association.DeleteByLeft(left, false)
		}); err != nil {
			return fmt.Errorf("delete relation in left and right buckets: %w", err)
		}
	}

	if err := leftIndexBuckets.DeleteBucket(l); err != nil {
		return fmt.Errorf("delete left value bucket from left index buckets: %w", err)
	}

	return nil
}

// IterateAssociations iterates over Association keys in the lexicographical
// order of keys. If the callback function f returns false, the iteration stops
// and the next can be used to continue the iteration.
func (a *Associations[A, L, R]) IterateAssociations(start *A, reverse bool, f func(A) (bool, error)) (next *A, err error) {
	leftbuckets, err := a.leftBuckets(false)
	if err != nil {
		return nil, fmt.Errorf("left buckets: %w", err)
	}
	if leftbuckets == nil {
		return nil, nil
	}
	return iterateKeys(leftbuckets, a.definition.associationKeyEncoding, start, reverse, func(ak, _ []byte) (bool, error) {
		key, err := a.definition.associationKeyEncoding.Decode(ak)
		if err != nil {
			return false, fmt.Errorf("decode association key: %w", err)
		}

		return f(key)
	})
}

// PageOfAssociations returns at most a limit of Association keys at the
// provided page number.
func (a *Associations[A, L, R]) PageOfAssociations(number, limit int, reverse bool) (s []A, totalElements, pages int, err error) {
	leftBuckets, err := a.leftBuckets(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("left buckets: %w", err)
	}
	if leftBuckets == nil {
		return nil, 0, 0, nil
	}
	return page(leftBuckets, true, number, limit, reverse, func(ak, _ []byte) (A, error) {
		return a.definition.associationKeyEncoding.Decode(ak)
	})
}

// IterateAssociationsWithLeftValue iterates over Association keys that contain
// the provided left value in the lexicographical order of keys. If the callback
// function f returns false, the iteration stops and the next can be used to
// continue the iteration.
func (a *Associations[A, L, R]) IterateAssociationsWithLeftValue(left L, start *A, reverse bool, f func(A) (bool, error)) (next *A, err error) {
	l, err := a.definition.leftEncoding.Encode(left)
	if err != nil {
		return nil, fmt.Errorf("encode left: %w", err)
	}
	leftIndexBuckets, err := a.leftIndexBuckets(false)
	if err != nil {
		return nil, fmt.Errorf("left index buckets: %w", err)
	}
	if leftIndexBuckets == nil {
		return nil, nil
	}
	leftIndexBucket := leftIndexBuckets.Bucket(l)
	if leftIndexBucket == nil {
		return nil, nil
	}
	return iterateKeys(leftIndexBucket, a.definition.associationKeyEncoding, start, reverse, func(ak, _ []byte) (bool, error) {
		key, err := a.definition.associationKeyEncoding.Decode(ak)
		if err != nil {
			return false, fmt.Errorf("decode association key: %w", err)
		}

		return f(key)
	})
}

// PageOfAssociationsWithLeftValue returns at most a limit of Association keys
// that contain the provided left value at the provided page number.
func (a *Associations[A, L, R]) PageOfAssociationsWithLeftValue(left L, number, limit int, reverse bool) (s []A, totalElements, pages int, err error) {
	l, err := a.definition.leftEncoding.Encode(left)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("encode left: %w", err)
	}
	leftIndexBuckets, err := a.leftIndexBuckets(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("left index buckets: %w", err)
	}
	if leftIndexBuckets == nil {
		return nil, 0, 0, nil
	}
	leftIndexBucket := leftIndexBuckets.Bucket(l)
	if leftIndexBucket == nil {
		return nil, 0, 0, nil
	}
	return page(leftIndexBucket, false, number, limit, reverse, func(k, _ []byte) (A, error) {
		return a.definition.associationKeyEncoding.Decode(k)
	})
}

// IterateLeftValues iterates over all left values in the lexicographical order
// of left values. If the callback function f returns false, the iteration stops
// and the next can be used to continue the iteration.
func (a *Associations[A, L, R]) IterateLeftValues(start *L, reverse bool, f func(L) (bool, error)) (next *L, err error) {
	leftIndexBuckets, err := a.leftIndexBuckets(false)
	if err != nil {
		return nil, fmt.Errorf("left index buckets: %w", err)
	}
	if leftIndexBuckets == nil {
		return nil, nil
	}
	return iterateKeys(leftIndexBuckets, a.definition.leftEncoding, start, reverse, func(l, _ []byte) (bool, error) {
		left, err := a.definition.leftEncoding.Decode(l)
		if err != nil {
			return false, fmt.Errorf("decode left: %w", err)
		}

		return f(left)
	})
}

// PageOfLeftValues returns at most a limit of left values at the provided page
// number.
func (a *Associations[A, L, R]) PageOfLeftValues(number, limit int, reverse bool) (s []L, totalElements, pages int, err error) {
	leftIndexBuckets, err := a.leftIndexBuckets(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("left index buckets: %w", err)
	}
	if leftIndexBuckets == nil {
		return nil, 0, 0, nil
	}
	return page(leftIndexBuckets, true, number, limit, reverse, func(l, _ []byte) (L, error) {
		return a.definition.leftEncoding.Decode(l)
	})
}
