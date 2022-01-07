// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron_test

import (
	"errors"
	"fmt"
	"testing"

	bolt "go.etcd.io/bbolt"
	"resenje.org/boltron"
)

var (
	numbersDefinition = boltron.NewAssociationDefinition(
		"numbers",
		boltron.StringEncoding,
		boltron.IntBase10Encoding,
		nil,
	)

	testNumbers = []struct {
		K string
		V int
	}{
		{"five", 5},
		{"four", 4},
		{"one", 1},
		{"seven", 7},
		{"six", 6},
		{"three", 3},
		{"two", 2},
	}

	testNumbersValueSorted = []struct {
		K string
		V int
	}{
		{"one", 1},
		{"two", 2},
		{"three", 3},
		{"four", 4},
		{"five", 5},
		{"six", 6},
		{"seven", 7},
	}
)

func TestAssociation_singleRelation(t *testing.T) {
	var (
		key   = testNumbers[0].K
		value = testNumbers[0].V
	)
	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := numbersDefinition.Association(tx)

		hasKey, err := numbers.HasKey("missing")
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasKey, false)

		hasValue, err := numbers.HasValue(2)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasValue, false)

		err = numbers.Set(key, value)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := numbersDefinition.Association(tx)

		hasKey, err := numbers.HasKey(key)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasKey, true)

		hasValue, err := numbers.HasValue(value)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasValue, true)

		hasKey, err = numbers.HasKey("missing")
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasKey, false)

		hasValue, err = numbers.HasValue(2)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasValue, false)

		v, err := numbers.Value(key)
		assertErrorFail(t, "", err, nil)
		assert(t, "", v, value)

		k, err := numbers.Key(value)
		assertErrorFail(t, "", err, nil)
		assert(t, "", k, key)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := numbersDefinition.Association(tx)

		err := numbers.DeleteByKey(key, true)
		assertErrorFail(t, "", err, nil)

		hasKey, err := numbers.HasKey(key)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasKey, false)

		hasValue, err := numbers.HasValue(value)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasValue, false)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := numbersDefinition.Association(tx)

		hasKey, err := numbers.HasKey(key)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasKey, false)

		hasValue, err := numbers.HasValue(value)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasValue, false)

		err = numbers.Set(key, value)
		assertErrorFail(t, "", err, nil)

		hasKey, err = numbers.HasKey(key)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasKey, true)

		hasValue, err = numbers.HasValue(value)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasValue, true)

		err = numbers.DeleteByValue(value, true)
		assertErrorFail(t, "", err, nil)

		hasKey, err = numbers.HasKey(key)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasKey, false)

		hasValue, err = numbers.HasValue(value)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasValue, false)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := numbersDefinition.Association(tx)

		err := numbers.Set(key, value)
		assertErrorFail(t, "", err, nil)

		err = numbers.Set(key, value)
		assertErrorFail(t, "", err, nil)

		hasKey, err := numbers.HasKey(key)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasKey, true)

		hasValue, err := numbers.HasValue(value)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasValue, true)
	})
}

func TestAssociation_iterate(t *testing.T) {
	db := newNumbersDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var i int
			next, err := numbers.Iterate(nil, false, func(k string, v int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), k, testNumbers[i].K)
				assert(t, fmt.Sprintf("iterate number #%v", i), v, testNumbers[i].V)
				i++

				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var i int
			next, err := numbers.Iterate(nil, false, func(k string, v int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), k, testNumbers[i].K)
				assert(t, fmt.Sprintf("iterate number #%v", i), v, testNumbers[i].V)
				i++

				if i == 4 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, "six")

			next, err = numbers.Iterate(next, false, func(k string, v int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), k, testNumbers[i].K)
				assert(t, fmt.Sprintf("iterate number #%v", i), v, testNumbers[i].V)
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var i int
			next, err := numbers.Iterate(nil, true, func(k string, v int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), k, testNumbers[len(testNumbers)-1-i].K)
				assert(t, fmt.Sprintf("iterate number #%v", i), v, testNumbers[len(testNumbers)-1-i].V)
				i++

				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var i int
			next, err := numbers.Iterate(nil, true, func(k string, v int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), k, testNumbers[len(testNumbers)-1-i].K)
				assert(t, fmt.Sprintf("iterate number #%v", i), v, testNumbers[len(testNumbers)-1-i].V)
				i++

				if i == 4 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, "one")

			next, err = numbers.Iterate(next, true, func(k string, v int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), k, testNumbers[len(testNumbers)-1-i].K)
				assert(t, fmt.Sprintf("iterate number #%v", i), v, testNumbers[len(testNumbers)-1-i].V)
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var count int
			next, err := numbers.Iterate(nil, false, func(_ string, _ int) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestAssociation_iterateKeys(t *testing.T) {
	db := newNumbersDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var i int
			next, err := numbers.IterateKeys(nil, false, func(k string) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), k, testNumbers[i].K)
				i++

				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var i int
			next, err := numbers.IterateKeys(nil, false, func(k string) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), k, testNumbers[i].K)
				i++

				if i == 4 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, "six")

			next, err = numbers.IterateKeys(next, false, func(k string) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), k, testNumbers[i].K)
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var i int
			next, err := numbers.IterateKeys(nil, true, func(k string) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), k, testNumbers[len(testNumbers)-1-i].K)
				i++

				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var i int
			next, err := numbers.IterateKeys(nil, true, func(k string) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), k, testNumbers[len(testNumbers)-1-i].K)
				i++

				if i == 4 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, "one")

			next, err = numbers.IterateKeys(next, true, func(k string) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), k, testNumbers[len(testNumbers)-1-i].K)
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var count int
			next, err := numbers.IterateKeys(nil, false, func(_ string) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestAssociation_iterateValues(t *testing.T) {
	db := newNumbersDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var i int
			next, err := numbers.IterateValues(nil, false, func(v int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), v, testNumbersValueSorted[i].V)
				i++

				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var i int
			next, err := numbers.IterateValues(nil, false, func(v int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), v, testNumbersValueSorted[i].V)
				i++

				if i == 4 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 5)

			next, err = numbers.IterateValues(next, false, func(v int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), v, testNumbersValueSorted[i].V)
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var i int
			next, err := numbers.IterateValues(nil, true, func(v int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), v, testNumbersValueSorted[len(testNumbersValueSorted)-1-i].V)
				i++

				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var i int
			next, err := numbers.IterateValues(nil, true, func(v int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), v, testNumbersValueSorted[len(testNumbersValueSorted)-1-i].V)
				i++

				if i == 4 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 3)

			next, err = numbers.IterateValues(next, true, func(v int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), v, testNumbersValueSorted[len(testNumbersValueSorted)-1-i].V)
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var count int
			next, err := numbers.IterateValues(nil, false, func(_ int) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestAssociation_page(t *testing.T) {
	db := newNumbersDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			_, _, _, err := numbers.Page(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = numbers.Page(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := numbers.Page(1, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberElements(0, 1, 2))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.Page(2, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberElements(3, 4, 5))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.Page(3, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberElements(6))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			_, _, _, err := numbers.Page(-1, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = numbers.Page(0, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := numbers.Page(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberElements(6, 5, 4))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.Page(2, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberElements(3, 2, 1))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.Page(3, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberElements(0))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			page, totalElements, totalPages, err := numbers.Page(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestAssociation_pageOfKeys(t *testing.T) {
	db := newNumbersDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			_, _, _, err := numbers.PageOfKeys(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = numbers.PageOfKeys(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := numbers.PageOfKeys(1, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberKeys(0, 1, 2))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.PageOfKeys(2, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberKeys(3, 4, 5))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.PageOfKeys(3, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberKeys(6))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			_, _, _, err := numbers.PageOfKeys(-1, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = numbers.PageOfKeys(0, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := numbers.PageOfKeys(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberKeys(6, 5, 4))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.PageOfKeys(2, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberKeys(3, 2, 1))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.PageOfKeys(3, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberKeys(0))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			page, totalElements, totalPages, err := numbers.PageOfKeys(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestAssociation_pageOfValues(t *testing.T) {
	db := newNumbersDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			_, _, _, err := numbers.PageOfValues(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = numbers.PageOfValues(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := numbers.PageOfValues(1, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberValues(0, 1, 2))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.PageOfValues(2, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberValues(3, 4, 5))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.PageOfValues(3, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberValues(6))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			_, _, _, err := numbers.PageOfValues(-1, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = numbers.PageOfValues(0, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := numbers.PageOfValues(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberValues(6, 5, 4))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.PageOfValues(2, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberValues(3, 2, 1))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.PageOfValues(3, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberValues(0))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			page, totalElements, totalPages, err := numbers.PageOfValues(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestAssociation_ErrNotFound(t *testing.T) {
	db := newDB(t)

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := numbersDefinition.Association(tx)

		v, err := numbers.Key(0)
		assertError(t, "", err, boltron.ErrNotFound)
		assert(t, "", v, "")

		has, err := numbers.HasValue(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		k, err := numbers.Value("missing")
		assertError(t, "", err, boltron.ErrNotFound)
		assert(t, "", k, 0)

		has, err = numbers.HasKey("missing")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = numbers.DeleteByKey("missing", true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = numbers.DeleteByKey("missing", false)
		assertError(t, "", err, nil)

		err = numbers.DeleteByValue(0, true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = numbers.DeleteByValue(0, false)
		assertError(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := numbersDefinition.Association(tx)

		err := numbers.Set("one", 1)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := numbersDefinition.Association(tx)

		v, err := numbers.Key(0)
		assertError(t, "", err, boltron.ErrNotFound)
		assert(t, "", v, "")

		has, err := numbers.HasValue(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		k, err := numbers.Value("missing")
		assertError(t, "", err, boltron.ErrNotFound)
		assert(t, "", k, 0)

		has, err = numbers.HasKey("missing")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = numbers.DeleteByKey("missing", true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = numbers.DeleteByKey("missing", false)
		assertError(t, "", err, nil)

		err = numbers.DeleteByValue(0, true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = numbers.DeleteByValue(0, false)
		assertError(t, "", err, nil)
	})
}

func TestAssociation_customErrNotFound(t *testing.T) {

	errNotFoundCustom := errors.New("custom not found error")

	customNumbersDefinition := boltron.NewAssociationDefinition(
		"numbers",
		boltron.StringEncoding,
		boltron.IntBase10Encoding,
		&boltron.AssociationOptions{
			ErrNotFound: errNotFoundCustom,
		},
	)

	db := newDB(t)

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := customNumbersDefinition.Association(tx)

		v, err := numbers.Key(0)
		assertError(t, "", err, errNotFoundCustom)
		assert(t, "", v, "")

		has, err := numbers.HasValue(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		k, err := numbers.Value("missing")
		assertError(t, "", err, errNotFoundCustom)
		assert(t, "", k, 0)

		has, err = numbers.HasKey("missing")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = numbers.DeleteByKey("missing", true)
		assertError(t, "", err, errNotFoundCustom)

		err = numbers.DeleteByKey("missing", false)
		assertError(t, "", err, nil)

		err = numbers.DeleteByValue(0, true)
		assertError(t, "", err, errNotFoundCustom)

		err = numbers.DeleteByValue(0, false)
		assertError(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := customNumbersDefinition.Association(tx)

		err := numbers.Set("one", 1)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := customNumbersDefinition.Association(tx)

		v, err := numbers.Key(0)
		assertError(t, "", err, errNotFoundCustom)
		assert(t, "", v, "")

		has, err := numbers.HasValue(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		k, err := numbers.Value("missing")
		assertError(t, "", err, errNotFoundCustom)
		assert(t, "", k, 0)

		has, err = numbers.HasKey("missing")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = numbers.DeleteByKey("missing", true)
		assertError(t, "", err, errNotFoundCustom)

		err = numbers.DeleteByKey("missing", false)
		assertError(t, "", err, nil)

		err = numbers.DeleteByValue(0, true)
		assertError(t, "", err, errNotFoundCustom)

		err = numbers.DeleteByValue(0, false)
		assertError(t, "", err, nil)
	})
}

func TestAssociation_ErrKeyExists_and_ErrValueExists(t *testing.T) {

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := numbersDefinition.Association(tx)

		err := numbers.Set("one", 1)
		assertErrorFail(t, "", err, nil)

		err = numbers.Set("one", 2)
		assertErrorFail(t, "", err, boltron.ErrKeyExists)

		err = numbers.Set("two", 1)
		assertErrorFail(t, "", err, boltron.ErrValueExists)

		// no overwrite as values are the same
		err = numbers.Set("one", 1)
		assertErrorFail(t, "", err, nil)
	})
}

func TestAssociation_customErrKeyExists_and_customErrValueExists(t *testing.T) {

	errKeyExistsCustom := errors.New("custom key exists error")
	errValueExistsCustom := errors.New("custom value exists error")

	customNumbersDefinition := boltron.NewAssociationDefinition(
		"numbers",
		boltron.StringEncoding,
		boltron.IntBase10Encoding,
		&boltron.AssociationOptions{
			ErrKeyExists:   errKeyExistsCustom,
			ErrValueExists: errValueExistsCustom,
		},
	)

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := customNumbersDefinition.Association(tx)

		err := numbers.Set("one", 1)
		assertErrorFail(t, "", err, nil)

		err = numbers.Set("one", 2)
		assertErrorFail(t, "", err, errKeyExistsCustom)

		err = numbers.Set("two", 1)
		assertErrorFail(t, "", err, errValueExistsCustom)

		// no overwrite as values are the same
		err = numbers.Set("one", 1)
		assertErrorFail(t, "", err, nil)
	})
}

func newNumbersDB(t testing.TB) *bolt.DB {
	t.Helper()

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := numbersDefinition.Association(tx)

		for _, n := range testNumbers {
			err := numbers.Set(n.K, n.V)
			assertErrorFail(t, fmt.Sprintf("%+v", n), err, nil)
		}
	})

	return db
}

func numberElements(is ...int) []boltron.Element[string, int] {
	s := make([]boltron.Element[string, int], 0, len(is))
	for _, i := range is {
		s = append(s, boltron.Element[string, int]{
			Key:   testNumbers[i].K,
			Value: testNumbers[i].V,
		})
	}
	return s
}

func numberKeys(is ...int) []string {
	s := make([]string, 0, len(is))
	for _, i := range is {
		s = append(s, testNumbers[i].K)
	}
	return s
}

func numberValues(is ...int) []int {
	s := make([]int, 0, len(is))
	for _, i := range is {
		s = append(s, testNumbersValueSorted[i].V)
	}
	return s
}
