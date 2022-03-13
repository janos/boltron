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

// ListsDefinition defines a set of Lists, each identified by an unique key. All
// lists have the same value and order by encodings.
type ListsDefinition[K, V, O any] struct {
	bucketNameLists   []byte
	bucketNameIndexes []byte
	bucketNameValues  []byte
	keyEncoding       Encoding[K]
	valueEncoding     Encoding[V]
	orderByEncoding   Encoding[O]
	uniqueValues      bool
	errListNotFound   error
	errValueNotFound  error
	errValueExists    error
}

// ListsOptions provides additional configuration for a Lists instance.
type ListsOptions struct {
	// UniqueValues marks if a value can be added only to a single list.
	UniqueValues bool
	// ErrListNotFound is returned if the list identified by the key is not
	// found.
	ErrListNotFound error
	// ErrValueNotFound is returned if the value is not found.
	ErrValueNotFound error
	// ErrValueExists is returned if UniqueValues option is set to true and the
	// value already exists in another list.
	ErrValueExists error
}

// NewListsDefinition constructs a new ListsDefinition with a unique name and
// key, value and order by encodings.
func NewListsDefinition[K, V, O any](
	name string,
	keyEncoding Encoding[K],
	valueEncoding Encoding[V],
	orderByEncoding Encoding[O],
	o *ListsOptions,
) *ListsDefinition[K, V, O] {
	if o == nil {
		o = new(ListsOptions)
	}
	return &ListsDefinition[K, V, O]{
		bucketNameLists:   []byte("boltron: lists: " + name + " lists"),
		bucketNameIndexes: []byte("boltron: lists: " + name + " indexes"),
		bucketNameValues:  []byte("boltron: lists: " + name + " values"),
		keyEncoding:       keyEncoding,
		valueEncoding:     valueEncoding,
		orderByEncoding:   orderByEncoding,
		uniqueValues:      o.UniqueValues,
		errListNotFound:   withDefaultError(o.ErrListNotFound, ErrNotFound),
		errValueNotFound:  withDefaultError(o.ErrValueNotFound, ErrNotFound),
		errValueExists:    withDefaultError(o.ErrValueExists, ErrValueExists),
	}
}

// Lists returns a Lists instance that has access to the stored data through the
// bolt transaction.
func (d *ListsDefinition[K, V, O]) Lists(tx *bolt.Tx) *Lists[K, V, O] {
	return &Lists[K, V, O]{
		tx:         tx,
		definition: d,
	}
}

// Lists provides methods to access and change a set of Lists.
type Lists[K, V, O any] struct {
	tx                 *bolt.Tx
	listsBucketCache   *bolt.Bucket
	indexesBucketCache *bolt.Bucket
	valuesBucketCache  *bolt.Bucket
	definition         *ListsDefinition[K, V, O]
}

func (l *Lists[K, V, O]) listsBucket(create bool) (*bolt.Bucket, error) {
	if l.listsBucketCache != nil {
		return l.listsBucketCache, nil
	}
	bucket, err := rootBucket(l.tx, create, l.definition.bucketNameLists)
	if err != nil {
		return nil, err
	}
	l.listsBucketCache = bucket
	return bucket, nil
}

func (l *Lists[K, V, O]) indexesBucket(create bool) (*bolt.Bucket, error) {
	if l.indexesBucketCache != nil {
		return l.indexesBucketCache, nil
	}
	bucket, err := rootBucket(l.tx, create, l.definition.bucketNameIndexes)
	if err != nil {
		return nil, err
	}
	l.indexesBucketCache = bucket
	return bucket, nil
}

func (l *Lists[K, V, O]) valuesBucket(create bool) (*bolt.Bucket, error) {
	if l.valuesBucketCache != nil {
		return l.valuesBucketCache, nil
	}
	bucket, err := rootBucket(l.tx, create, l.definition.bucketNameValues)
	if err != nil {
		return nil, err
	}
	l.valuesBucketCache = bucket
	return bucket, nil
}

// List returns a List instance that is associated with the provided key. If the
// returned value of exists is false, the list still does not exist but it will
// be created if a value is added to it.
func (l *Lists[K, V, O]) List(key K) (list *List[V, O], exists bool, err error) {
	k, err := l.definition.keyEncoding.Encode(key)
	if err != nil {
		return nil, false, fmt.Errorf("encode key: %w", err)
	}
	listsBucket, err := l.listsBucket(false)
	if err != nil {
		return nil, false, fmt.Errorf("lists bucket: %w", err)
	}
	exists = listsBucket != nil && listsBucket.Bucket(k) != nil && listsBucket.Bucket(k).Stats().KeyN != 0

	return &List[V, O]{
		tx: l.tx,
		definition: &ListDefinition[V, O]{
			bucketPath:       [][]byte{l.definition.bucketNameLists, k},
			bucketPathIndex:  [][]byte{l.definition.bucketNameIndexes, k},
			valueEncoding:    l.definition.valueEncoding,
			orderByEncoding:  l.definition.orderByEncoding,
			errValueNotFound: l.definition.errValueNotFound,
			addCallback: func(value, orderBy []byte) error {
				valuesBucket, err := l.valuesBucket(true)
				if err != nil {
					return fmt.Errorf("values bucket: %w", err)
				}
				valueBucket := valuesBucket.Bucket(value)
				if valueBucket != nil {
					if l.definition.uniqueValues {
						firstKey, _ := valueBucket.Cursor().First()
						if firstKey != nil && !bytes.Equal(firstKey, k) {
							return l.definition.errValueExists
						}
					}
				} else {
					b, err := valuesBucket.CreateBucket(value)
					if err != nil {
						return fmt.Errorf("create value bucket: %w", err)
					}
					valueBucket = b
				}
				return valueBucket.Put(k, orderBy)
			},
			removeCallback: func(value, orderBy []byte) error {
				valuesBucket, err := l.valuesBucket(false)
				if err != nil {
					return fmt.Errorf("values bucket: %w", err)
				}
				if valuesBucket == nil {
					return fmt.Errorf("missing lists values bucket: %w", l.definition.errValueNotFound)
				}
				valueBucket := valuesBucket.Bucket(value)
				if valueBucket == nil {
					return fmt.Errorf("missing value in lists values bucket: %w", l.definition.errValueNotFound)
				}
				if err := valueBucket.Delete(k); err != nil {
					return fmt.Errorf("delete value from lists values bucket: %w", err)
				}
				if valuesBucket.Stats().KeyN == 1 { // stats are updated after the transaction
					if err := valuesBucket.DeleteBucket(value); err != nil {
						return fmt.Errorf("delete empty value bucket: %w", err)
					}
				}
				return nil
			},
		},
	}, exists, nil
}

// HasList returns true if the List associated with the key already exists in
// the database.
func (l *Lists[K, V, O]) HasList(key K) (bool, error) {
	k, err := l.definition.keyEncoding.Encode(key)
	if err != nil {
		return false, fmt.Errorf("encode key: %w", err)
	}

	listsBucket, err := l.listsBucket(false)
	if err != nil {
		return false, fmt.Errorf("lists bucket: %w", err)
	}
	if listsBucket == nil {
		return false, nil
	}

	if listsBucket.Bucket(k) == nil {
		return false, nil
	}

	f, _ := listsBucket.Bucket(k).Cursor().First()
	return f != nil, nil
}

// HasValue returns true if the value already exists in any List.
func (l *Lists[K, V, O]) HasValue(value V) (bool, error) {
	v, err := l.definition.valueEncoding.Encode(value)
	if err != nil {
		return false, fmt.Errorf("encode value: %w", err)
	}

	valuesBucket, err := l.valuesBucket(false)
	if err != nil {
		return false, fmt.Errorf("values bucket: %w", err)
	}
	if valuesBucket == nil {
		return false, nil
	}

	if valuesBucket.Bucket(v) == nil {
		return false, nil
	}

	f, _ := valuesBucket.Bucket(v).Cursor().First()
	return f != nil, nil
}

// DeleteList removes the list from the database. If ensure flag is set to true
// and the value does not exist, configured ErrListNotFound is returned.
func (l *Lists[K, V, O]) DeleteList(key K, ensure bool) error {
	k, err := l.definition.keyEncoding.Encode(key)
	if err != nil {
		return fmt.Errorf("encode key: %w", err)
	}

	listsBucket, err := l.listsBucket(false)
	if err != nil {
		return fmt.Errorf("lists bucket: %w", err)
	}
	if listsBucket == nil {
		if ensure {
			return l.definition.errListNotFound
		}
		return nil
	}

	listBucket := listsBucket.Bucket(k)
	if listBucket == nil {
		if ensure {
			return l.definition.errListNotFound
		}
		return nil
	}

	indexesBucket, err := l.indexesBucket(false)
	if err != nil {
		return fmt.Errorf("indexes bucket: %w", err)
	}
	if indexesBucket == nil {
		return errors.New("indexes bucket does not exist")
	}

	indexBucket := indexesBucket.Bucket(k)
	if indexBucket == nil {
		return fmt.Errorf("index bucket does not exist for list %v", key)
	}

	valuesBucket, err := l.valuesBucket(false)
	if err != nil {
		return fmt.Errorf("values bucket: %w", err)
	}
	if valuesBucket == nil {
		return errors.New("values bucket does not exist")
	}

	if err := indexBucket.ForEach(func(v, _ []byte) error {
		valueBucket := valuesBucket.Bucket(v)
		if valueBucket == nil {
			return nil
		}
		if err := valueBucket.Delete(k); err != nil {
			return fmt.Errorf("delete key from value bucket: %w", err)
		}
		if valueBucket.Stats().KeyN == 1 { // stats are updated after the transaction
			if err := valuesBucket.DeleteBucket(v); err != nil {
				return fmt.Errorf("delete bucket from values bucket: %w", err)
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("delete key in values buckets: %w", err)
	}

	if err := listsBucket.DeleteBucket(k); err != nil {
		return fmt.Errorf("delete key: %w", err)
	}

	if err := indexesBucket.DeleteBucket(k); err != nil {
		return fmt.Errorf("delete key: %w", err)
	}

	return nil
}

// DeleteValue removes the value from all lists that contain it. If ensure flag
// is set to true and the value does not exist, configured ErrValueNotFound is
// returned.
func (l *Lists[K, V, O]) DeleteValue(value V, ensure bool) error {
	v, err := l.definition.valueEncoding.Encode(value)
	if err != nil {
		return fmt.Errorf("encode value: %w", err)
	}

	valuesBucket, err := l.valuesBucket(false)
	if err != nil {
		return fmt.Errorf("values bucket: %w", err)
	}
	if valuesBucket == nil {
		if ensure {
			return l.definition.errValueNotFound
		}
		return nil
	}

	valueBucket := valuesBucket.Bucket(v)
	if valueBucket == nil {
		if ensure {
			return l.definition.errValueNotFound
		}
		return nil
	}

	listsBucket, err := l.listsBucket(false)
	if err != nil {
		return fmt.Errorf("lists bucket: %w", err)
	}

	indexesBucket, err := l.indexesBucket(false)
	if err != nil {
		return fmt.Errorf("bucket: %w", err)
	}

	if listsBucket != nil && indexesBucket != nil {
		list := (&ListDefinition[V, O]{
			valueEncoding:    l.definition.valueEncoding,
			orderByEncoding:  l.definition.orderByEncoding,
			errValueNotFound: l.definition.errValueNotFound,
		}).List(nil)

		if err := valueBucket.ForEach(func(k, _ []byte) error {
			list.listBucketCache = listsBucket.Bucket(k)
			list.indexBucketCache = indexesBucket.Bucket(k)
			return list.Remove(value, false)
		}); err != nil {
			return fmt.Errorf("delete value in keys bucket: %w", err)
		}
	}

	if err := valuesBucket.DeleteBucket(v); err != nil {
		return fmt.Errorf("delete value: %w", err)
	}

	return nil
}

// IterateLists iterates over List keys in the lexicographical order of keys. If
// the callback function f returns false, the iteration stops and the next can
// be used to continue the iteration.
func (l *Lists[K, V, O]) IterateLists(start *K, reverse bool, f func(K) (bool, error)) (next *K, err error) {
	listsBucket, err := l.listsBucket(false)
	if err != nil {
		return nil, fmt.Errorf("lists bucket: %w", err)
	}
	if listsBucket == nil {
		return nil, nil
	}
	return iterateKeys(listsBucket, l.definition.keyEncoding, start, reverse, func(k, _ []byte) (bool, error) {
		key, err := l.definition.keyEncoding.Decode(k)
		if err != nil {
			return false, fmt.Errorf("decode key: %w", err)
		}

		return f(key)
	})
}

// PageOfLists returns at most a limit of List keys at the provided page number.
func (l *Lists[K, V, O]) PageOfLists(number, limit int, reverse bool) (s []K, totalElements, pages int, err error) {
	listsBucket, err := l.listsBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("lists bucket: %w", err)
	}
	if listsBucket == nil {
		return nil, 0, 0, nil
	}
	return page(listsBucket, true, number, limit, reverse, func(k, _ []byte) (K, error) {
		return l.definition.keyEncoding.Decode(k)
	})
}

// IterateListsWithValue iterates over List keys that contain the provided value
// in the lexicographical order of keys. If the callback function f returns
// false, the iteration stops and the next can be used to continue the
// iteration.
func (l *Lists[K, V, O]) IterateListsWithValue(value V, start *K, reverse bool, f func(K, O) (bool, error)) (next *K, err error) {
	v, err := l.definition.valueEncoding.Encode(value)
	if err != nil {
		return nil, fmt.Errorf("encode value: %w", err)
	}
	valuesBucket, err := l.valuesBucket(false)
	if err != nil {
		return nil, fmt.Errorf("values bucket: %w", err)
	}
	if valuesBucket == nil {
		return nil, nil
	}
	valueBucket := valuesBucket.Bucket(v)
	if valueBucket == nil {
		return nil, nil
	}
	return iterateKeys(valueBucket, l.definition.keyEncoding, start, reverse, func(k, o []byte) (bool, error) {
		key, err := l.definition.keyEncoding.Decode(k)
		if err != nil {
			return false, fmt.Errorf("decode value: %w", err)
		}

		orderBy, err := l.definition.orderByEncoding.Decode(o)
		if err != nil {
			return false, fmt.Errorf("decode value: %w", err)
		}

		return f(key, orderBy)
	})
}

// ListsElement is the type returned by Lists pagination methods as slice
// elements that cointain both list key and the order by value of the value in
// that list.
type ListsElement[K, O any] struct {
	Key     K
	OrderBy O
}

// PageOfListsWithValue returns at most a limit of List keys and associate order
// by values that contain the provided value at the provided page number.
func (l *Lists[K, V, O]) PageOfListsWithValue(value V, number, limit int, reverse bool) (s []ListsElement[K, O], totalElements, pages int, err error) {
	v, err := l.definition.valueEncoding.Encode(value)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("encode value: %w", err)
	}
	valuesBucket, err := l.valuesBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("values bucket: %w", err)
	}
	if valuesBucket == nil {
		return nil, 0, 0, nil
	}
	valueBucket := valuesBucket.Bucket(v)
	if valueBucket == nil {
		return nil, 0, 0, nil
	}
	return page(valueBucket, false, number, limit, reverse, func(k, o []byte) (e ListsElement[K, O], err error) {
		key, err := l.definition.keyEncoding.Decode(k)
		if err != nil {
			return e, fmt.Errorf("decode value: %w", err)
		}

		orderBy, err := l.definition.orderByEncoding.Decode(o)
		if err != nil {
			return e, fmt.Errorf("decode value: %w", err)
		}

		return ListsElement[K, O]{
			Key:     key,
			OrderBy: orderBy,
		}, nil
	})
}

// IterateValues iterates over all values in the lexicographical order of
// values. If the callback function f returns false, the iteration stops and the
// next can be used to continue the iteration.
func (l *Lists[K, V, O]) IterateValues(start *V, reverse bool, f func(V) (bool, error)) (next *V, err error) {
	valuesBucket, err := l.valuesBucket(false)
	if err != nil {
		return nil, fmt.Errorf("values bucket: %w", err)
	}
	if valuesBucket == nil {
		return nil, nil
	}
	return iterateKeys(valuesBucket, l.definition.valueEncoding, start, reverse, func(v, _ []byte) (bool, error) {
		value, err := l.definition.valueEncoding.Decode(v)
		if err != nil {
			return false, fmt.Errorf("decode value: %w", err)
		}

		return f(value)
	})
}

// PageOfValues returns at most a limit of values at the provided page number.
func (l *Lists[K, V, O]) PageOfValues(number, limit int, reverse bool) (s []V, totalElements, pages int, err error) {
	valuesBucket, err := l.valuesBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("values bucket: %w", err)
	}
	if valuesBucket == nil {
		return nil, 0, 0, nil
	}
	return page(valuesBucket, true, number, limit, reverse, func(v, _ []byte) (V, error) {
		return l.definition.valueEncoding.Decode(v)
	})
}
