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

// AssociationDefinition defines one-to-one relation between values named left
// and right. The relation is unique.
type AssociationDefinition[L, R any] struct {
	bucketPathLeft       [][]byte
	bucketPathRight      [][]byte
	indexBucketPathLeft  [][]byte
	indexBucketPathRight [][]byte
	leftEncoding         Encoding[L]
	rightEncoding        Encoding[R]
	fillPercent          float64
	errLeftNotFound      error
	errRightNotFound     error
	errLeftExists        error
	errRightExists       error
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
	// UniqueLeft enables global uniqueness indexing for left values across all instances.
	UniqueLeft bool
	// UniqueRight enables global uniqueness indexing for right values across all instances.
	UniqueRight bool
}

// NewAssociationDefinition constructs a new AssociationDefinition with a unique
// name and left and right values encodings.
func NewAssociationDefinition[L, R any](
	name string,
	leftEncoding Encoding[L],
	rightEncoding Encoding[R],
	o *AssociationOptions,
) *AssociationDefinition[L, R] {
	if o == nil {
		o = new(AssociationOptions)
	}
	var indexBucketPathLeft [][]byte
	if o.UniqueLeft {
		indexBucketPathLeft = bucketPath("boltron: association: index: " + name + " unique_left")
	}
	var indexBucketPathRight [][]byte
	if o.UniqueRight {
		indexBucketPathRight = bucketPath("boltron: association: index: " + name + " unique_right")
	}
	return &AssociationDefinition[L, R]{
		bucketPathLeft:       bucketPath("boltron: association: " + name + " left"),
		bucketPathRight:      bucketPath("boltron: association: " + name + " right"),
		indexBucketPathLeft:  indexBucketPathLeft,
		indexBucketPathRight: indexBucketPathRight,
		leftEncoding:         leftEncoding,
		rightEncoding:        rightEncoding,
		fillPercent:          o.FillPercent,
		errLeftNotFound:      withDefaultError(o.ErrLeftNotFound, ErrLeftNotFound),
		errRightNotFound:     withDefaultError(o.ErrRightNotFound, ErrRightNotFound),
		errLeftExists:        withDefaultError(o.ErrLeftExists, ErrLeftExists),
		errRightExists:       withDefaultError(o.ErrRightExists, ErrRightExists),
	}
}

// Association returns an Association that has access to the stored data through
// the bolt transaction.
func (d *AssociationDefinition[L, R]) Association(tx *bolt.Tx) *Association[L, R] {
	return &Association[L, R]{
		tx:         tx,
		definition: d,
	}
}

// Association provides methods to access and change relations.
type Association[L, R any] struct {
	tx               *bolt.Tx
	leftBucketCache  *bolt.Bucket
	rightBucketCache *bolt.Bucket
	definition       *AssociationDefinition[L, R]
}

func (a *Association[L, R]) leftBucket(create bool) (*bolt.Bucket, error) {
	if a.leftBucketCache != nil {
		return a.leftBucketCache, nil
	}
	bucket, err := deepBucket(a.tx, create, a.definition.bucketPathLeft...)
	if err != nil {
		return nil, err
	}
	if a.definition.fillPercent > 0 && bucket != nil {
		bucket.FillPercent = a.definition.fillPercent
	}
	a.leftBucketCache = bucket
	return bucket, nil
}

func (a *Association[L, R]) rightBucket(create bool) (*bolt.Bucket, error) {
	if a.rightBucketCache != nil {
		return a.rightBucketCache, nil
	}
	bucket, err := deepBucket(a.tx, create, a.definition.bucketPathRight...)
	if err != nil {
		return nil, err
	}
	if a.definition.fillPercent > 0 && bucket != nil {
		bucket.FillPercent = a.definition.fillPercent
	}
	a.rightBucketCache = bucket
	return bucket, nil
}

// HasLeft returns true if the left already exists in the database.
func (a *Association[L, R]) HasLeft(left L) (bool, error) {
	l, err := a.definition.leftEncoding.Encode(left)
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
func (a *Association[L, R]) HasRight(right R) (bool, error) {
	r, err := a.definition.rightEncoding.Encode(right)
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
func (a *Association[L, R]) Left(right R) (left L, err error) {
	r, err := a.definition.rightEncoding.Encode(right)
	if err != nil {
		return left, fmt.Errorf("encode right: %w", err)
	}

	rightBucket, err := a.rightBucket(false)
	if err != nil {
		return left, fmt.Errorf("right bucket: %w", err)
	}
	if rightBucket == nil {
		return left, a.definition.errLeftNotFound
	}

	l := rightBucket.Get(r)
	if l == nil {
		return left, a.definition.errLeftNotFound
	}
	left, err = a.definition.leftEncoding.Decode(l)
	if err != nil {
		return left, fmt.Errorf("decode left: %w", err)
	}
	return left, nil
}

// Right returns right value associated with the given left value. If value does
// not exist, ErrNotFound is returned.
func (a *Association[L, R]) Right(left L) (right R, err error) {
	l, err := a.definition.leftEncoding.Encode(left)
	if err != nil {
		return right, fmt.Errorf("encode left: %w", err)
	}
	leftBucket, err := a.leftBucket(false)
	if err != nil {
		return right, fmt.Errorf("left bucket: %w", err)
	}
	if leftBucket == nil {
		return right, a.definition.errRightNotFound
	}
	r := leftBucket.Get(l)
	if r == nil {
		return right, a.definition.errRightNotFound
	}
	right, err = a.definition.rightEncoding.Decode(r)
	if err != nil {
		return right, fmt.Errorf("decode right: %w", err)
	}
	return right, nil
}

// Set saves the relation between the left and right values. If left value
// already exists, configured ErrLeftExists is returned, if right value exists,
// configured ErrRightExists is returned.
func (a *Association[L, R]) Set(left L, right R) error {
	l, err := a.definition.leftEncoding.Encode(left)
	if err != nil {
		return fmt.Errorf("encode left: %w", err)
	}
	leftBucket, err := a.leftBucket(true)
	if err != nil {
		return fmt.Errorf("left bucket: %w", err)
	}

	currentRight := leftBucket.Get(l)

	r, err := a.definition.rightEncoding.Encode(right)
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
		return a.definition.errRightExists
	}

	if currentRight != nil {
		return a.definition.errLeftExists
	}

	if a.definition.indexBucketPathLeft != nil {
		indexBucket, err := deepBucket(a.tx, true, a.definition.indexBucketPathLeft...)
		if err != nil {
			return fmt.Errorf("unique left index bucket: %w", err)
		}
		existingPathData := indexBucket.Get(l)
		if existingPathData != nil {
			existingPath, err := decodePath(existingPathData)
			if err != nil {
				return fmt.Errorf("decode existing left path: %w", err)
			}
			if !equalPaths(existingPath, a.definition.bucketPathLeft) {
				return a.definition.errLeftExists
			}
		} else {
			if err := indexBucket.Put(l, encodePath(a.definition.bucketPathLeft)); err != nil {
				return fmt.Errorf("write left index entry: %w", err)
			}
		}
	}

	if a.definition.indexBucketPathRight != nil {
		indexBucket, err := deepBucket(a.tx, true, a.definition.indexBucketPathRight...)
		if err != nil {
			return fmt.Errorf("unique right index bucket: %w", err)
		}
		existingPathData := indexBucket.Get(r)
		if existingPathData != nil {
			existingPath, err := decodePath(existingPathData)
			if err != nil {
				return fmt.Errorf("decode existing right path: %w", err)
			}
			if !equalPaths(existingPath, a.definition.bucketPathRight) {
				return a.definition.errRightExists
			}
		} else {
			if err := indexBucket.Put(r, encodePath(a.definition.bucketPathRight)); err != nil {
				return fmt.Errorf("write right index entry: %w", err)
			}
		}
	}

	if err := leftBucket.Put(l, r); err != nil {
		return fmt.Errorf("put left: %w", err)
	}
	if err := rightBucket.Put(r, l); err != nil {
		return fmt.Errorf("put right: %w", err)
	}

	return nil
}

// DeleteByLeft removes the relation that contains the provided left value. If
// ensure flag is set to true and the value does not exist, configured
// ErrNotFound is returned.
func (a *Association[L, R]) DeleteByLeft(left L, ensure bool) error {
	l, err := a.definition.leftEncoding.Encode(left)
	if err != nil {
		return fmt.Errorf("encode left: %w", err)
	}

	leftBucket, err := a.leftBucket(false)
	if err != nil {
		return fmt.Errorf("left bucket: %w", err)
	}

	if leftBucket == nil {
		if ensure {
			return a.definition.errLeftNotFound
		}
		return nil
	}

	r := leftBucket.Get(l)
	if r == nil {
		if ensure {
			return a.definition.errLeftNotFound
		}
		return nil
	}

	rightBucket, err := a.rightBucket(false)
	if err != nil {
		return fmt.Errorf("right bucket: %w", err)
	}

	if rightBucket == nil {
		if ensure {
			return a.definition.errLeftNotFound
		}
		return nil
	}

	// 1. Cascading nested sub-buckets cleanup for left
	suffixes := [][]byte{
		l,
		append(append([]byte(nil), l...), []byte(" left")...),
		append(append([]byte(nil), l...), []byte(" right")...),
		append(append([]byte(nil), l...), []byte(" values")...),
		append(append([]byte(nil), l...), []byte(" index")...),
	}
	for _, suffix := range suffixes {
		if leftBucket.Bucket(suffix) != nil {
			prefixPath := append(a.definition.bucketPathLeft, suffix)
			if err := cleanupIndexesForPath(a.tx, prefixPath); err != nil {
				return fmt.Errorf("cleanup left indexes for %s: %w", suffix, err)
			}
			if err := leftBucket.DeleteBucket(suffix); err != nil {
				return fmt.Errorf("delete nested left bucket %s: %w", suffix, err)
			}
		}
	}

	// 2. Cascading nested sub-buckets cleanup for right
	suffixesRight := [][]byte{
		r,
		append(append([]byte(nil), r...), []byte(" left")...),
		append(append([]byte(nil), r...), []byte(" right")...),
		append(append([]byte(nil), r...), []byte(" values")...),
		append(append([]byte(nil), r...), []byte(" index")...),
	}
	for _, suffix := range suffixesRight {
		if rightBucket.Bucket(suffix) != nil {
			prefixPath := append(a.definition.bucketPathRight, suffix)
			if err := cleanupIndexesForPath(a.tx, prefixPath); err != nil {
				return fmt.Errorf("cleanup right indexes for %s: %w", suffix, err)
			}
			if err := rightBucket.DeleteBucket(suffix); err != nil {
				return fmt.Errorf("delete nested right bucket %s: %w", suffix, err)
			}
		}
	}

	// 3. Remove unique left index entry
	if a.definition.indexBucketPathLeft != nil {
		uniqueLeftBucket, err := deepBucket(a.tx, false, a.definition.indexBucketPathLeft...)
		if err != nil {
			return fmt.Errorf("unique left index bucket: %w", err)
		}
		if uniqueLeftBucket != nil {
			if err := uniqueLeftBucket.Delete(l); err != nil {
				return fmt.Errorf("delete unique left index: %w", err)
			}
		}
	}

	// 4. Remove unique right index entry
	if a.definition.indexBucketPathRight != nil {
		uniqueRightBucket, err := deepBucket(a.tx, false, a.definition.indexBucketPathRight...)
		if err != nil {
			return fmt.Errorf("unique right index bucket: %w", err)
		}
		if uniqueRightBucket != nil {
			if err := uniqueRightBucket.Delete(r); err != nil {
				return fmt.Errorf("delete unique right index: %w", err)
			}
		}
	}

	if err := leftBucket.Delete(l); err != nil {
		return fmt.Errorf("delete left: %w", err)
	}

	return rightBucket.Delete(r)
}

// DeleteByRight removes the relation that contains the provided right value. If
// ensure flag is set to true and the value does not exist, ErrNotFound is
// returned.
func (a *Association[L, R]) DeleteByRight(right R, ensure bool) error {
	r, err := a.definition.rightEncoding.Encode(right)
	if err != nil {
		return fmt.Errorf("encode right: %w", err)
	}

	rightBucket, err := a.rightBucket(false)
	if err != nil {
		return fmt.Errorf("right bucket: %w", err)
	}

	if rightBucket == nil {
		if ensure {
			return a.definition.errRightNotFound
		}
		return nil
	}

	l := rightBucket.Get(r)
	if l == nil {
		if ensure {
			return a.definition.errRightNotFound
		}
		return nil
	}

	leftBucket, err := a.leftBucket(false)
	if err != nil {
		return fmt.Errorf("left bucket: %w", err)
	}

	if leftBucket == nil {
		if ensure {
			return a.definition.errRightNotFound
		}
		return nil
	}

	// 1. Cascading nested sub-buckets cleanup for left
	suffixes := [][]byte{
		l,
		append(append([]byte(nil), l...), []byte(" left")...),
		append(append([]byte(nil), l...), []byte(" right")...),
		append(append([]byte(nil), l...), []byte(" values")...),
		append(append([]byte(nil), l...), []byte(" index")...),
	}
	for _, suffix := range suffixes {
		if leftBucket.Bucket(suffix) != nil {
			prefixPath := append(a.definition.bucketPathLeft, suffix)
			if err := cleanupIndexesForPath(a.tx, prefixPath); err != nil {
				return fmt.Errorf("cleanup left indexes for %s: %w", suffix, err)
			}
			if err := leftBucket.DeleteBucket(suffix); err != nil {
				return fmt.Errorf("delete nested left bucket %s: %w", suffix, err)
			}
		}
	}

	// 2. Cascading nested sub-buckets cleanup for right
	suffixesRight := [][]byte{
		r,
		append(append([]byte(nil), r...), []byte(" left")...),
		append(append([]byte(nil), r...), []byte(" right")...),
		append(append([]byte(nil), r...), []byte(" values")...),
		append(append([]byte(nil), r...), []byte(" index")...),
	}
	for _, suffix := range suffixesRight {
		if rightBucket.Bucket(suffix) != nil {
			prefixPath := append(a.definition.bucketPathRight, suffix)
			if err := cleanupIndexesForPath(a.tx, prefixPath); err != nil {
				return fmt.Errorf("cleanup right indexes for %s: %w", suffix, err)
			}
			if err := rightBucket.DeleteBucket(suffix); err != nil {
				return fmt.Errorf("delete nested right bucket %s: %w", suffix, err)
			}
		}
	}

	// 3. Remove unique left index entry
	if a.definition.indexBucketPathLeft != nil {
		uniqueLeftBucket, err := deepBucket(a.tx, false, a.definition.indexBucketPathLeft...)
		if err != nil {
			return fmt.Errorf("unique left index bucket: %w", err)
		}
		if uniqueLeftBucket != nil {
			if err := uniqueLeftBucket.Delete(l); err != nil {
				return fmt.Errorf("delete unique left index: %w", err)
			}
		}
	}

	// 4. Remove unique right index entry
	if a.definition.indexBucketPathRight != nil {
		uniqueRightBucket, err := deepBucket(a.tx, false, a.definition.indexBucketPathRight...)
		if err != nil {
			return fmt.Errorf("unique right index bucket: %w", err)
		}
		if uniqueRightBucket != nil {
			if err := uniqueRightBucket.Delete(r); err != nil {
				return fmt.Errorf("delete unique right index: %w", err)
			}
		}
	}

	if err := leftBucket.Delete(l); err != nil {
		return fmt.Errorf("delete left: %w", err)
	}

	return rightBucket.Delete(r)
}

// Iterate iterates over associations in the lexicographical order of left
// values. If the callback function f returns false, the iteration stops and the
// next can be used to continue the iteration.
func (a *Association[L, R]) Iterate(start *L, reverse bool, f func(L, R) (bool, error)) (next *L, err error) {
	leftBucket, err := a.leftBucket(false)
	if err != nil {
		return nil, fmt.Errorf("left bucket: %w", err)
	}
	if leftBucket == nil {
		return nil, nil
	}
	return iterateKeys(leftBucket, a.definition.leftEncoding, start, reverse, func(l, r []byte) (bool, error) {
		left, err := a.definition.leftEncoding.Decode(l)
		if err != nil {
			return false, fmt.Errorf("decode left: %w", err)
		}

		right, err := a.definition.rightEncoding.Decode(r)
		if err != nil {
			return false, fmt.Errorf("decode right: %w", err)
		}

		return f(left, right)
	})
}

// IterateLeftValues iterates over left values in the lexicographical order of
// left values. If the callback function f returns false, the iteration stops
// and the next can be used to continue the iteration.
func (a *Association[L, R]) IterateLeftValues(start *L, reverse bool, f func(L) (bool, error)) (next *L, err error) {
	leftBucket, err := a.leftBucket(false)
	if err != nil {
		return nil, fmt.Errorf("left bucket: %w", err)
	}
	if leftBucket == nil {
		return nil, nil
	}
	return iterateKeys(leftBucket, a.definition.leftEncoding, start, reverse, func(l, _ []byte) (bool, error) {
		left, err := a.definition.leftEncoding.Decode(l)
		if err != nil {
			return false, fmt.Errorf("decode left: %w", err)
		}

		return f(left)
	})
}

// IterateRightValues iterates over right values in the lexicographical order of
// right values. If the callback function f returns false, the iteration stops
// and the next can be used to continue the iteration.
func (a *Association[L, R]) IterateRightValues(start *R, reverse bool, f func(R) (bool, error)) (next *R, err error) {
	rightBucket, err := a.rightBucket(false)
	if err != nil {
		return nil, fmt.Errorf("right bucket: %w", err)
	}
	if rightBucket == nil {
		return nil, nil
	}
	return iterateKeys(rightBucket, a.definition.rightEncoding, start, reverse, func(r, _ []byte) (bool, error) {
		right, err := a.definition.rightEncoding.Decode(r)
		if err != nil {
			return false, fmt.Errorf("decode right: %w", err)
		}

		return f(right)
	})
}

// Size returns the number of associations.
func (a *Association[L, R]) Size() (int, error) {
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
func (a *Association[L, R]) Page(number, limit int, reverse bool) (s []AssociationElement[L, R], totalElements, pages int, err error) {
	leftBucket, err := a.leftBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("left bucket: %w", err)
	}
	if leftBucket == nil {
		return nil, 0, 0, nil
	}
	return page(leftBucket, false, number, limit, reverse, func(l, r []byte) (e AssociationElement[L, R], err error) {
		left, err := a.definition.leftEncoding.Decode(l)
		if err != nil {
			return e, fmt.Errorf("left value: %w", err)
		}

		right, err := a.definition.rightEncoding.Decode(r)
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
func (a *Association[L, R]) PageOfLeftValues(number, limit int, reverse bool) (s []L, totalElements, pages int, err error) {
	leftBucket, err := a.leftBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("left bucket: %w", err)
	}
	if leftBucket == nil {
		return nil, 0, 0, nil
	}
	return page(leftBucket, false, number, limit, reverse, func(l, _ []byte) (left L, err error) {
		return a.definition.leftEncoding.Decode(l)
	})
}

// PageOfRightValues returns at most a limit of right values at the provided
// page number.
func (a *Association[L, R]) PageOfRightValues(number, limit int, reverse bool) (s []R, totalElements, pages int, err error) {
	rightBucket, err := a.rightBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("right bucket: %w", err)
	}
	if rightBucket == nil {
		return nil, 0, 0, nil
	}
	return page(rightBucket, false, number, limit, reverse, func(r, _ []byte) (right R, err error) {
		return a.definition.rightEncoding.Decode(r)
	})
}

// CollectionByLeft returns a nested Collection inside the left bucket of the association under the given left value.
func (a *Association[L, R]) CollectionByLeft[K2, V2 any](left L, definition *CollectionDefinition[K2, V2]) (*Collection[K2, V2], error) {
	l, err := a.definition.leftEncoding.Encode(left)
	if err != nil {
		return nil, fmt.Errorf("encode left: %w", err)
	}

	newPath := make([][]byte, len(a.definition.bucketPathLeft)+1)
	copy(newPath, a.definition.bucketPathLeft)
	newPath[len(a.definition.bucketPathLeft)] = l

	nestedDef := &CollectionDefinition[K2, V2]{
		bucketPath:      newPath,
		indexBucketPath: definition.indexBucketPath,
		keyEncoding:     definition.keyEncoding,
		valueEncoding:   definition.valueEncoding,
		fillPercent:     definition.fillPercent,
		errNotFound:     definition.errNotFound,
		errKeyExists:    definition.errKeyExists,
	}

	return nestedDef.Collection(a.tx), nil
}

// AssociationByLeft returns a nested Association inside the left bucket of the association under the given left value.
func (a *Association[L, R]) AssociationByLeft[L2, R2 any](left L, definition *AssociationDefinition[L2, R2]) (*Association[L2, R2], error) {
	l, err := a.definition.leftEncoding.Encode(left)
	if err != nil {
		return nil, fmt.Errorf("encode left: %w", err)
	}

	newPathLeft := make([][]byte, len(a.definition.bucketPathLeft)+1)
	copy(newPathLeft, a.definition.bucketPathLeft)
	newPathLeft[len(a.definition.bucketPathLeft)] = append(append([]byte(nil), l...), []byte(" left")...)

	newPathRight := make([][]byte, len(a.definition.bucketPathLeft)+1)
	copy(newPathRight, a.definition.bucketPathLeft)
	newPathRight[len(a.definition.bucketPathLeft)] = append(append([]byte(nil), l...), []byte(" right")...)

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

	return nestedDef.Association(a.tx), nil
}

// ListByLeft returns a nested List inside the left bucket of the association under the given left value.
func (a *Association[L, R]) ListByLeft[V2, O2 any](left L, definition *ListDefinition[V2, O2]) (*List[V2, O2], error) {
	l, err := a.definition.leftEncoding.Encode(left)
	if err != nil {
		return nil, fmt.Errorf("encode left: %w", err)
	}

	newPath := make([][]byte, len(a.definition.bucketPathLeft)+1)
	copy(newPath, a.definition.bucketPathLeft)
	newPath[len(a.definition.bucketPathLeft)] = append(append([]byte(nil), l...), []byte(" values")...)

	newPathIndex := make([][]byte, len(a.definition.bucketPathLeft)+1)
	copy(newPathIndex, a.definition.bucketPathLeft)
	newPathIndex[len(a.definition.bucketPathLeft)] = append(append([]byte(nil), l...), []byte(" index")...)

	nestedDef := &ListDefinition[V2, O2]{
		bucketPath:       newPath,
		bucketPathIndex:  newPathIndex,
		indexBucketPath:  definition.indexBucketPath,
		valueEncoding:    definition.valueEncoding,
		orderByEncoding:  definition.orderByEncoding,
		fillPercent:      definition.fillPercent,
		errValueNotFound: definition.errValueNotFound,
	}

	return nestedDef.List(a.tx), nil
}

// ParentKeyByLeft returns the decoded parent key of the container nested under the left value. If the container is at the root level, ErrNoParent is returned.
func (d *AssociationDefinition[L, R]) ParentKeyByLeft[P any](tx *bolt.Tx, left L, parentKeyEncoding Encoding[P]) (parentKey P, err error) {
	if d.indexBucketPathLeft == nil {
		if len(d.bucketPathLeft) <= 1 {
			return parentKey, ErrNoParent
		}
		return parentKey, fmt.Errorf("unique left index not configured")
	}
	l, err := d.leftEncoding.Encode(left)
	if err != nil {
		return parentKey, fmt.Errorf("encode left: %w", err)
	}
	indexBucket, err := deepBucket(tx, false, d.indexBucketPathLeft...)
	if err != nil {
		return parentKey, fmt.Errorf("index bucket: %w", err)
	}
	if indexBucket == nil {
		return parentKey, ErrNotFound
	}
	pathData := indexBucket.Get(l)
	if pathData == nil {
		return parentKey, d.errLeftNotFound
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

// ParentKeyByRight returns the decoded parent key of the container nested under the right value. If the container is at the root level, ErrNoParent is returned.
func (d *AssociationDefinition[L, R]) ParentKeyByRight[P any](tx *bolt.Tx, right R, parentKeyEncoding Encoding[P]) (parentKey P, err error) {
	if d.indexBucketPathRight == nil {
		if len(d.bucketPathRight) <= 1 {
			return parentKey, ErrNoParent
		}
		return parentKey, fmt.Errorf("unique right index not configured")
	}
	r, err := d.rightEncoding.Encode(right)
	if err != nil {
		return parentKey, fmt.Errorf("encode right: %w", err)
	}
	indexBucket, err := deepBucket(tx, false, d.indexBucketPathRight...)
	if err != nil {
		return parentKey, fmt.Errorf("index bucket: %w", err)
	}
	if indexBucket == nil {
		return parentKey, ErrNotFound
	}
	pathData := indexBucket.Get(r)
	if pathData == nil {
		return parentKey, d.errRightNotFound
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
