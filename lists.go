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

// Lists defines a set of Lists, each identified by an unique key. All
// lists have the same value and order by encodings.
type Lists[K, V, O any] struct {
	bucketNameLists   []byte
	bucketNameIndexes []byte
	bucketNameValues  []byte
	keyEncoding       Encoding[K]
	valueEncoding     Encoding[V]
	orderByEncoding   Encoding[O]
	fillPercent       float64
	uniqueValues      bool
	errListNotFound   error
	errValueNotFound  error
	errValueExists    error
}

// ListsOptions provides additional configuration for a Lists instance.
type ListsOptions struct {
	// FillPercent is the value for the bolt bucket fill percent for every
	// collection.
	FillPercent float64
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

// NewLists constructs a new Lists with a unique name and
// key, value and order by encodings.
func NewLists[K, V, O any](
	name string,
	keyEncoding Encoding[K],
	valueEncoding Encoding[V],
	orderByEncoding Encoding[O],
	o *ListsOptions,
) *Lists[K, V, O] {
	if o == nil {
		o = new(ListsOptions)
	}
	return &Lists[K, V, O]{
		bucketNameLists:   []byte("boltron: lists: " + name + " lists"),
		bucketNameIndexes: []byte("boltron: lists: " + name + " indexes"),
		bucketNameValues:  []byte("boltron: lists: " + name + " values"),
		keyEncoding:       keyEncoding,
		valueEncoding:     valueEncoding,
		orderByEncoding:   orderByEncoding,
		fillPercent:       o.FillPercent,
		uniqueValues:      o.UniqueValues,
		errListNotFound:   withDefaultError(o.ErrListNotFound, ErrNotFound),
		errValueNotFound:  withDefaultError(o.ErrValueNotFound, ErrNotFound),
		errValueExists:    withDefaultError(o.ErrValueExists, ErrValueExists),
	}
}

// Tx returns an Lists transaction with access to the stored data
// through the bolt transaction.
func (l *Lists[K, V, O]) Tx(tx *bolt.Tx) *ListsTx[K, V, O] {
	return &ListsTx[K, V, O]{
		tx:    tx,
		lists: l,
	}
}

// ListsTx provides methods to access and change a set of ListsTx.
type ListsTx[K, V, O any] struct {
	tx                 *bolt.Tx
	listsBucketCache   *bolt.Bucket
	indexesBucketCache *bolt.Bucket
	valuesBucketCache  *bolt.Bucket
	lists              *Lists[K, V, O]
}

func (l *ListsTx[K, V, O]) listsBucket(create bool) (*bolt.Bucket, error) {
	if l.listsBucketCache != nil {
		return l.listsBucketCache, nil
	}
	bucket, err := rootBucket(l.tx, create, l.lists.bucketNameLists)
	if err != nil {
		return nil, err
	}
	l.listsBucketCache = bucket
	return bucket, nil
}

func (l *ListsTx[K, V, O]) indexesBucket(create bool) (*bolt.Bucket, error) {
	if l.indexesBucketCache != nil {
		return l.indexesBucketCache, nil
	}
	bucket, err := rootBucket(l.tx, create, l.lists.bucketNameIndexes)
	if err != nil {
		return nil, err
	}
	l.indexesBucketCache = bucket
	return bucket, nil
}

func (l *ListsTx[K, V, O]) valuesBucket(create bool) (*bolt.Bucket, error) {
	if l.valuesBucketCache != nil {
		return l.valuesBucketCache, nil
	}
	bucket, err := rootBucket(l.tx, create, l.lists.bucketNameValues)
	if err != nil {
		return nil, err
	}
	l.valuesBucketCache = bucket
	return bucket, nil
}

// List returns a List instance that is associated with the provided key. If the
// returned value of exists is false, the list still does not exist but it will
// be created if a value is added to it.
func (l *ListsTx[K, V, O]) List(key K) (list *ListTx[V, O], exists bool, err error) {
	k, err := l.lists.keyEncoding.Encode(key)
	if err != nil {
		return nil, false, fmt.Errorf("encode key: %w", err)
	}
	listsBucket, err := l.listsBucket(false)
	if err != nil {
		return nil, false, fmt.Errorf("lists bucket: %w", err)
	}
	if listsBucket != nil && listsBucket.Bucket(k) != nil {
		first, _ := listsBucket.Bucket(k).Cursor().First()
		exists = first != nil
	}

	return &ListTx[V, O]{
		tx: l.tx,
		list: &List[V, O]{
			bucketPath:       [][]byte{l.lists.bucketNameLists, k},
			bucketPathIndex:  [][]byte{l.lists.bucketNameIndexes, k},
			valueEncoding:    l.lists.valueEncoding,
			orderByEncoding:  l.lists.orderByEncoding,
			fillPercent:      l.lists.fillPercent,
			errValueNotFound: l.lists.errValueNotFound,
			addCallback: func(value, orderBy []byte) error {
				valuesBucket, err := l.valuesBucket(true)
				if err != nil {
					return fmt.Errorf("values bucket: %w", err)
				}
				valueBucket := valuesBucket.Bucket(value)
				if valueBucket != nil {
					if l.lists.uniqueValues {
						firstKey, _ := valueBucket.Cursor().First()
						if firstKey != nil && !bytes.Equal(firstKey, k) {
							return l.lists.errValueExists
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
					return fmt.Errorf("missing lists values bucket: %w", l.lists.errValueNotFound)
				}
				valueBucket := valuesBucket.Bucket(value)
				if valueBucket == nil {
					return fmt.Errorf("missing value in lists values bucket: %w", l.lists.errValueNotFound)
				}
				if err := valueBucket.Delete(k); err != nil {
					return fmt.Errorf("delete value from lists values bucket: %w", err)
				}
				if first, _ := valueBucket.Cursor().First(); first == nil {
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
func (l *ListsTx[K, V, O]) HasList(key K) (bool, error) {
	k, err := l.lists.keyEncoding.Encode(key)
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
func (l *ListsTx[K, V, O]) HasValue(value V) (bool, error) {
	v, err := l.lists.valueEncoding.Encode(value)
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
func (l *ListsTx[K, V, O]) DeleteList(key K, ensure bool) error {
	k, err := l.lists.keyEncoding.Encode(key)
	if err != nil {
		return fmt.Errorf("encode key: %w", err)
	}

	listsBucket, err := l.listsBucket(false)
	if err != nil {
		return fmt.Errorf("lists bucket: %w", err)
	}
	if listsBucket == nil {
		if ensure {
			return l.lists.errListNotFound
		}
		return nil
	}

	listBucket := listsBucket.Bucket(k)
	if listBucket == nil {
		if ensure {
			return l.lists.errListNotFound
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
		if first, _ := valueBucket.Cursor().First(); first == nil {
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
func (l *ListsTx[K, V, O]) DeleteValue(value V, ensure bool) error {
	v, err := l.lists.valueEncoding.Encode(value)
	if err != nil {
		return fmt.Errorf("encode value: %w", err)
	}

	valuesBucket, err := l.valuesBucket(false)
	if err != nil {
		return fmt.Errorf("values bucket: %w", err)
	}
	if valuesBucket == nil {
		if ensure {
			return l.lists.errValueNotFound
		}
		return nil
	}

	valueBucket := valuesBucket.Bucket(v)
	if valueBucket == nil {
		if ensure {
			return l.lists.errValueNotFound
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
		list := (&List[V, O]{
			valueEncoding:    l.lists.valueEncoding,
			orderByEncoding:  l.lists.orderByEncoding,
			errValueNotFound: l.lists.errValueNotFound,
		}).Tx(nil)

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

// Size returns the number of lists.
func (l *ListsTx[K, V, O]) Size() (int, error) {
	listsBucket, err := l.listsBucket(false)
	if err != nil {
		return 0, fmt.Errorf("lists bucket: %w", err)
	}
	if listsBucket == nil {
		return 0, nil
	}
	return size(listsBucket, true), nil
}

// IterateLists iterates over List keys in the lexicographical order of keys. If
// the callback function f returns false, the iteration stops and the next can
// be used to continue the iteration.
func (l *ListsTx[K, V, O]) IterateLists(start *K, reverse bool, f func(K) (bool, error)) (next *K, err error) {
	listsBucket, err := l.listsBucket(false)
	if err != nil {
		return nil, fmt.Errorf("lists bucket: %w", err)
	}
	if listsBucket == nil {
		return nil, nil
	}
	return iterateKeys(listsBucket, l.lists.keyEncoding, start, reverse, func(k, _ []byte) (bool, error) {
		key, err := l.lists.keyEncoding.Decode(k)
		if err != nil {
			return false, fmt.Errorf("decode key: %w", err)
		}

		return f(key)
	})
}

// PageOfLists returns at most a limit of List keys at the provided page number.
func (l *ListsTx[K, V, O]) PageOfLists(number, limit int, reverse bool) (s []K, totalElements, pages int, err error) {
	listsBucket, err := l.listsBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("lists bucket: %w", err)
	}
	if listsBucket == nil {
		return nil, 0, 0, nil
	}
	return page(listsBucket, true, number, limit, reverse, func(k, _ []byte) (K, error) {
		return l.lists.keyEncoding.Decode(k)
	})
}

// IterateListsWithValue iterates over List keys that contain the provided value
// in the lexicographical order of keys. If the callback function f returns
// false, the iteration stops and the next can be used to continue the
// iteration.
func (l *ListsTx[K, V, O]) IterateListsWithValue(value V, start *K, reverse bool, f func(K, O) (bool, error)) (next *K, err error) {
	v, err := l.lists.valueEncoding.Encode(value)
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
	return iterateKeys(valueBucket, l.lists.keyEncoding, start, reverse, func(k, o []byte) (bool, error) {
		key, err := l.lists.keyEncoding.Decode(k)
		if err != nil {
			return false, fmt.Errorf("decode value: %w", err)
		}

		orderBy, err := l.lists.orderByEncoding.Decode(o)
		if err != nil {
			return false, fmt.Errorf("decode value: %w", err)
		}

		return f(key, orderBy)
	})
}

// ListsElement is the type returned by Lists pagination methods as slice
// elements that contain both list key and the order by value of the value in
// that list.
type ListsElement[K, O any] struct {
	Key     K
	OrderBy O
}

// PageOfListsWithValue returns at most a limit of List keys and associate order
// by values that contain the provided value at the provided page number.
func (l *ListsTx[K, V, O]) PageOfListsWithValue(value V, number, limit int, reverse bool) (s []ListsElement[K, O], totalElements, pages int, err error) {
	v, err := l.lists.valueEncoding.Encode(value)
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
		key, err := l.lists.keyEncoding.Decode(k)
		if err != nil {
			return e, fmt.Errorf("decode value: %w", err)
		}

		orderBy, err := l.lists.orderByEncoding.Decode(o)
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
func (l *ListsTx[K, V, O]) IterateValues(start *V, reverse bool, f func(V) (bool, error)) (next *V, err error) {
	valuesBucket, err := l.valuesBucket(false)
	if err != nil {
		return nil, fmt.Errorf("values bucket: %w", err)
	}
	if valuesBucket == nil {
		return nil, nil
	}
	return iterateKeys(valuesBucket, l.lists.valueEncoding, start, reverse, func(v, _ []byte) (bool, error) {
		value, err := l.lists.valueEncoding.Decode(v)
		if err != nil {
			return false, fmt.Errorf("decode value: %w", err)
		}

		return f(value)
	})
}

// PageOfValues returns at most a limit of values at the provided page number.
func (l *ListsTx[K, V, O]) PageOfValues(number, limit int, reverse bool) (s []V, totalElements, pages int, err error) {
	valuesBucket, err := l.valuesBucket(false)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("values bucket: %w", err)
	}
	if valuesBucket == nil {
		return nil, 0, 0, nil
	}
	return page(valuesBucket, true, number, limit, reverse, func(v, _ []byte) (V, error) {
		return l.lists.valueEncoding.Decode(v)
	})
}
