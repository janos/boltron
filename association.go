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

var (
	// ErrLeftNotFound is the default error if requested left value in Association does not
	// exist.
	ErrLeftNotFound = errors.New("boltron: left value not found")
	// ErrRightNotFound is the default error if requested right value in Association does not
	// exist.
	ErrRightNotFound = errors.New("boltron: right value not found")
	// ErrLeftExists is the default error if the left value already exists in
	// the Association.
	ErrLeftExists = errors.New("boltron: left value exists")
	// ErrRightExists is the default error if the right value already exists in
	// the Association.
	ErrRightExists = errors.New("boltron: right value exists")
)

// Association defines one-to-one relation between values named left
// and right. The relation is unique.
type Association[L, R any] struct {
	bucketPathLeft   [][]byte
	bucketPathRight  [][]byte
	leftEncoding     Encoding[L]
	rightEncoding    Encoding[R]
	fillPercent      float64
	errLeftNotFound  error
	errRightNotFound error
	errLeftExists    error
	errRightExists   error
	setCallback      func(left []byte) error
	deleteCallback   func(left []byte) error
}

// AssociationOptions provides additional configuration for an Association.
type AssociationOptions struct {
	// FillPercent is the value for the bolt bucket fill percent.
	FillPercent float64
	// ErrLeftNotFound is returned if the left value is not found.
	ErrLeftNotFound error
	// ErrRightNotFound is returned if the right value is not found.
	ErrRightNotFound error
	// ErrLeftExists is returned if the left value in relation already exists.
	ErrLeftExists error
	// ErrRightExists is returned if the right value in relation already exists.
	ErrRightExists error
}

// NewAssociation constructs a new Association with a unique
// name and left and right values encodings.
func NewAssociation[L, R any](
	name string,
	leftEncoding Encoding[L],
	rightEncoding Encoding[R],
	o *AssociationOptions,
) *Association[L, R] {
	if o == nil {
		o = new(AssociationOptions)
	}
	return &Association[L, R]{
		bucketPathLeft:   bucketPath("boltron: association: " + name + " left"),
		bucketPathRight:  bucketPath("boltron: association: " + name + " right"),
		leftEncoding:     leftEncoding,
		rightEncoding:    rightEncoding,
		fillPercent:      o.FillPercent,
		errLeftNotFound:  withDefaultError(o.ErrLeftNotFound, ErrLeftNotFound),
		errRightNotFound: withDefaultError(o.ErrRightNotFound, ErrRightNotFound),
		errLeftExists:    withDefaultError(o.ErrLeftExists, ErrLeftExists),
		errRightExists:   withDefaultError(o.ErrRightExists, ErrRightExists),
	}
}

// Tx returns an Association transaction with access to the stored data
// through the bolt transaction.
func (a *Association[L, R]) Tx(tx *bolt.Tx) *AssociationTx[L, R] {
	return &AssociationTx[L, R]{
		tx:          tx,
		association: a,
	}
}

// AssociationTx provides methods to access and change relations.
type AssociationTx[L, R any] struct {
	tx               *bolt.Tx
	leftBucketCache  *bolt.Bucket
	rightBucketCache *bolt.Bucket
	association      *Association[L, R]
}

func (a *AssociationTx[L, R]) leftBucket(create bool) (*bolt.Bucket, error) {
	if a.leftBucketCache != nil {
		return a.leftBucketCache, nil
	}
	bucket, err := deepBucket(a.tx, create, a.association.bucketPathLeft...)
	if err != nil {
		return nil, err
	}
	if a.association.fillPercent > 0 && bucket != nil {
		bucket.FillPercent = a.association.fillPercent
	}
	a.leftBucketCache = bucket
	return bucket, nil
}

func (a *AssociationTx[L, R]) rightBucket(create bool) (*bolt.Bucket, error) {
	if a.rightBucketCache != nil {
		return a.rightBucketCache, nil
	}
	bucket, err := deepBucket(a.tx, create, a.association.bucketPathRight...)
	if err != nil {
		return nil, err
	}
	if a.association.fillPercent > 0 && bucket != nil {
		bucket.FillPercent = a.association.fillPercent
	}
	a.rightBucketCache = bucket
	return bucket, nil
}

// HasLeft returns true if the left already exists in the database.
func (a *AssociationTx[L, R]) HasLeft(left L) (bool, error) {
	l, err := a.association.leftEncoding.Encode(left)
	if err != nil {
		return false, fmt.Errorf("encode left: %w", err)
	}

	leftBucket, err := a.leftBucket(false)
	if err != nil {
		return false, fmt.Errorf("left bucket: %w", err)
	}

	if leftBucket == nil {
		return false, nil
	}

	return leftBucket.Get(l) != nil, nil
}

// HasRight returns true if the right value already exists in the database.
func (a *AssociationTx[L, R]) HasRight(right R) (bool, error) {
	r, err := a.association.rightEncoding.Encode(right)
	if err != nil {
		return false, fmt.Errorf("encode right: %w", err)
	}

	rightBucket, err := a.rightBucket(false)
	if err != nil {
		return false, fmt.Errorf("right bucket: %w", err)
	}

	if rightBucket == nil {
		return false, nil
	}

	return rightBucket.Get(r) != nil, nil
}

// Left returns left value associated with the given right value. If value does
// not exist, ErrNotFound is returned.
func (a *AssociationTx[L, R]) Left(right R) (left L, err error) {
	r, err := a.association.rightEncoding.Encode(right)
	if err != nil {
		return left, fmt.Errorf("encode right: %w", err)
	}

	rightBucket, err := a.rightBucket(false)
	if err != nil {
		return left, fmt.Errorf("right bucket: %w", err)
	}
	if rightBucket == nil {
		return left, a.association.errLeftNotFound
	}

	l := rightBucket.Get(r)
	if l == nil {
		return left, a.association.errLeftNotFound
	}
	left, err = a.association.leftEncoding.Decode(l)
	if err != nil {
		return left, fmt.Errorf("decode left: %w", err)
	}
	return left, nil
}

// Right returns right value associated with the given left value. If value does
// not exist, ErrNotFound is returned.
func (a *AssociationTx[L, R]) Right(left L) (right R, err error) {
	l, err := a.association.leftEncoding.Encode(left)
	if err != nil {
		return right, fmt.Errorf("encode left: %w", err)
	}
	leftBucket, err := a.leftBucket(false)
	if err != nil {
		return right, fmt.Errorf("left bucket: %w", err)
	}
	if leftBucket == nil {
		return right, a.association.errRightNotFound
	}
	r := leftBucket.Get(l)
	if r == nil {
		return right, a.association.errRightNotFound
	}
	right, err = a.association.rightEncoding.Decode(r)
	if err != nil {
		return right, fmt.Errorf("decode right: %w", err)
	}
	return right, nil
}

// Set saves the relation between the left and right values. If left value
// already exists, configured ErrLeftExists is returned, if right value exists,
// configured ErrValueExists is returned.
func (a *AssociationTx[L, R]) Set(left L, right R) error {
	l, err := a.association.leftEncoding.Encode(left)
	if err != nil {
		return fmt.Errorf("encode left: %w", err)
	}
	leftBucket, err := a.leftBucket(true)
	if err != nil {
		return fmt.Errorf("left bucket: %w", err)
	}

	currentRight := leftBucket.Get(l)

	r, err := a.association.rightEncoding.Encode(right)
	if err != nil {
		return fmt.Errorf("encode right: %w", err)
	}

	rightBucket, err := a.rightBucket(true)
	if err != nil {
		return fmt.Errorf("right bucket: %w", err)
	}

	currentLeft := rightBucket.Get(r)

	if bytes.Equal(l, currentLeft) && bytes.Equal(r, currentRight) {
		return nil
	}

	if currentLeft != nil {
		return a.association.errRightExists
	}

	if currentRight != nil {
		return a.association.errLeftExists
	}

	if err := leftBucket.Put(l, r); err != nil {
		return fmt.Errorf("put left: %w", err)
	}
	if err := rightBucket.Put(r, l); err != nil {
		return fmt.Errorf("put right: %w", err)
	}

	if a.association.setCallback != nil {
		if err := a.association.setCallback(l); err != nil {
			return fmt.Errorf("set callback: %w", err)
		}
	}

	return nil
}

// DeleteByLeft removes the relation that contains the provided left value. If
// ensure flag is set to true and the value does not exist, configured
// ErrNotFound is returned.
func (a *AssociationTx[L, R]) DeleteByLeft(left L, ensure bool) error {
	l, err := a.association.leftEncoding.Encode(left)
	if err != nil {
		return fmt.Errorf("encode left: %w", err)
	}

	leftBucket, err := a.leftBucket(false)
	if err != nil {
		return fmt.Errorf("left bucket: %w", err)
	}

	if leftBucket == nil {
		if ensure {
			return a.association.errLeftNotFound
		}
		return nil
	}

	r := leftBucket.Get(l)
	if r == nil {
		if ensure {
			return a.association.errLeftNotFound
		}
		return nil
	}

	if err := leftBucket.Delete(l); err != nil {
		return fmt.Errorf("delete left: %w", err)
	}

	rightBucket, err := a.rightBucket(false)
	if err != nil {
		return fmt.Errorf("right bucket: %w", err)
	}

	if rightBucket == nil {
		if ensure {
			return a.association.errLeftNotFound
		}
		return nil
	}

	if err := rightBucket.Delete(r); err != nil {
		return fmt.Errorf("delete right: %w", err)
	}

	if a.association.deleteCallback != nil {
		if err := a.association.deleteCallback(l); err != nil {
			return fmt.Errorf("delete callback: %w", err)
		}
	}

	return nil
}

// DeleteByRight removes the relation that contains the provided right value. If
// ensure flag is set to true and the value does not exist, ErrNotFound is
// returned.
func (a *AssociationTx[L, R]) DeleteByRight(right R, ensure bool) error {
	r, err := a.association.rightEncoding.Encode(right)
	if err != nil {
		return fmt.Errorf("encode right: %w", err)
	}

	rightBucket, err := a.rightBucket(false)
	if err != nil {
		return fmt.Errorf("right bucket: %w", err)
	}

	if rightBucket == nil {
		if ensure {
			return a.association.errRightNotFound
		}
		return nil
	}

	l := rightBucket.Get(r)
	if l == nil {
		if ensure {
			return a.association.errRightNotFound
		}
		return nil
	}

	leftBucket, err := a.leftBucket(false)
	if err != nil {
		return fmt.Errorf("left bucket: %w", err)
	}

	if leftBucket == nil {
		if ensure {
			return a.association.errRightNotFound
		}
		return nil
	}

	if err := leftBucket.Delete(l); err != nil {
		return fmt.Errorf("delete left: %w", err)
	}

	if err := rightBucket.Delete(r); err != nil {
		return fmt.Errorf("delete right: %w", err)
	}

	if a.association.deleteCallback != nil {
		if err := a.association.deleteCallback(l); err != nil {
			return fmt.Errorf("delete callback: %w", err)
		}
	}

	return nil
}

// Iterate iterates over associations in the lexicographical order of left
// values. If the callback function f returns false, the iteration stops and the
// next can be used to continue the iteration.
func (a *AssociationTx[L, R]) Iterate(start *L, reverse bool, f func(L, R) (bool, error)) (next *L, err error) {
	leftBucket, err := a.leftBucket(false)
	if err != nil {
		return nil, fmt.Errorf("left bucket: %w", err)
	}
	if leftBucket == nil {
		return nil, nil
	}
	return iterateKeys(leftBucket, a.association.leftEncoding, start, reverse, func(l, r []byte) (bool, error) {
		left, err := a.association.leftEncoding.Decode(l)
		if err != nil {
			return false, fmt.Errorf("decode left: %w", err)
		}

		right, err := a.association.rightEncoding.Decode(r)
		if err != nil {
			return false, fmt.Errorf("decode right: %w", err)
		}

		return f(left, right)
	})
}

// IterateLeftValues iterates over left values in the lexicographical order of
// left values. If the callback function f returns false, the iteration stops
// and the next can be used to continue the iteration.
func (a *AssociationTx[L, R]) IterateLeftValues(start *L, reverse bool, f func(L) (bool, error)) (next *L, err error) {
	leftBucket, err := a.leftBucket(false)
	if err != nil {
		return nil, fmt.Errorf("left bucket: %w", err)
	}
	if leftBucket == nil {
		return nil, nil
	}
	return iterateKeys(leftBucket, a.association.leftEncoding, start, reverse, func(l, _ []byte) (bool, error) {
		left, err := a.association.leftEncoding.Decode(l)
		if err != nil {
			return false, fmt.Errorf("decode left: %w", err)
		}

		return f(left)
	})
}

// IterateRightValues iterates over right values in the lexicographical order of
// right values. If the callback function f returns false, the iteration stops
// and the next can be used to continue the iteration.
func (a *AssociationTx[L, R]) IterateRightValues(start *R, reverse bool, f func(R) (bool, error)) (next *R, err error) {
	rightBucket, err := a.rightBucket(false)
	if err != nil {
		return nil, fmt.Errorf("right bucket: %w", err)
	}
	if rightBucket == nil {
		return nil, nil
	}
	return iterateKeys(rightBucket, a.association.rightEncoding, start, reverse, func(r, _ []byte) (bool, error) {
		right, err := a.association.rightEncoding.Decode(r)
		if err != nil {
			return false, fmt.Errorf("decode right: %w", err)
		}

		return f(right)
	})
}

// Size returns the number of associations.
func (a *AssociationTx[L, R]) Size() (int, error) {
	leftBucket, err := a.leftBucket(false)
	if err != nil {
		return 0, fmt.Errorf("left bucket: %w", err)
	}
	if leftBucket == nil {
		return 0, nil
	}
	return size(leftBucket, false), nil
}

// AssociationElement is the type returned by pagination methods as slice
// elements that contain both key and value.
type AssociationElement[L, R any] struct {
	Left  L
	Right R
}

// Page returns at most a limit of elements of associations at the provided page
// number.
func (a *AssociationTx[L, R]) Page(number, limit int, reverse bool) (s []AssociationElement[L, R], totalElements, pages int, err error) {
	leftBucket, err := a.leftBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("left bucket: %w", err)
	}
	if leftBucket == nil {
		return nil, 0, 0, nil
	}
	return page(leftBucket, false, number, limit, reverse, func(l, r []byte) (e AssociationElement[L, R], err error) {
		left, err := a.association.leftEncoding.Decode(l)
		if err != nil {
			return e, fmt.Errorf("left value: %w", err)
		}

		right, err := a.association.rightEncoding.Decode(r)
		if err != nil {
			return e, fmt.Errorf("decode right: %w", err)
		}

		return AssociationElement[L, R]{
			Left:  left,
			Right: right,
		}, nil
	})
}

// PageOfLeftValues returns at most a limit of left values at the provided page
// number.
func (a *AssociationTx[L, R]) PageOfLeftValues(number, limit int, reverse bool) (s []L, totalElements, pages int, err error) {
	leftBucket, err := a.leftBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("left bucket: %w", err)
	}
	if leftBucket == nil {
		return nil, 0, 0, nil
	}
	return page(leftBucket, false, number, limit, reverse, func(l, _ []byte) (left L, err error) {
		return a.association.leftEncoding.Decode(l)
	})
}

// PageOfRightValues returns at most a limit of right values at the provided
// page number.
func (a *AssociationTx[L, R]) PageOfRightValues(number, limit int, reverse bool) (s []R, totalElements, pages int, err error) {
	rightBucket, err := a.rightBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("right bucket: %w", err)
	}
	if rightBucket == nil {
		return nil, 0, 0, nil
	}
	return page(rightBucket, false, number, limit, reverse, func(r, _ []byte) (right R, err error) {
		return a.association.rightEncoding.Decode(r)
	})
}
