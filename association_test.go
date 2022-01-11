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
		L string
		R int
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
		left  = testNumbers[0].L
		right = testNumbers[0].R
	)
	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := numbersDefinition.Association(tx)

		hasLeft, err := numbers.HasLeft("missing")
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasLeft, false)

		hasRight, err := numbers.HasRight(2)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasRight, false)

		err = numbers.Set(left, right)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := numbersDefinition.Association(tx)

		hasLeft, err := numbers.HasLeft(left)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasLeft, true)

		hasRight, err := numbers.HasRight(right)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasRight, true)

		hasLeft, err = numbers.HasLeft("missing")
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasLeft, false)

		hasRight, err = numbers.HasRight(2)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasRight, false)

		r, err := numbers.Right(left)
		assertErrorFail(t, "", err, nil)
		assert(t, "", r, right)

		l, err := numbers.Left(right)
		assertErrorFail(t, "", err, nil)
		assert(t, "", l, left)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := numbersDefinition.Association(tx)

		err := numbers.DeleteByLeft(left, true)
		assertErrorFail(t, "", err, nil)

		hasLeft, err := numbers.HasLeft(left)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasLeft, false)

		hasRight, err := numbers.HasRight(right)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasRight, false)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := numbersDefinition.Association(tx)

		hasLeft, err := numbers.HasLeft(left)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasLeft, false)

		hasRight, err := numbers.HasRight(right)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasRight, false)

		err = numbers.Set(left, right)
		assertErrorFail(t, "", err, nil)

		hasLeft, err = numbers.HasLeft(left)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasLeft, true)

		hasRight, err = numbers.HasRight(right)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasRight, true)

		err = numbers.DeleteByRight(right, true)
		assertErrorFail(t, "", err, nil)

		hasLeft, err = numbers.HasLeft(left)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasLeft, false)

		hasRight, err = numbers.HasRight(right)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasRight, false)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := numbersDefinition.Association(tx)

		err := numbers.Set(left, right)
		assertErrorFail(t, "", err, nil)

		err = numbers.Set(left, right)
		assertErrorFail(t, "", err, nil)

		hasLeft, err := numbers.HasLeft(left)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasLeft, true)

		hasRight, err := numbers.HasRight(right)
		assertErrorFail(t, "", err, nil)
		assert(t, "", hasRight, true)
	})
}

func TestAssociation_iterate(t *testing.T) {
	db := newNumbersDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var i int
			next, err := numbers.Iterate(nil, false, func(l string, r int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), l, testNumbers[i].L)
				assert(t, fmt.Sprintf("iterate number #%v", i), r, testNumbers[i].R)
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
			next, err := numbers.Iterate(nil, false, func(l string, r int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), l, testNumbers[i].L)
				assert(t, fmt.Sprintf("iterate number #%v", i), r, testNumbers[i].R)
				i++

				if i == 4 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, "six")

			next, err = numbers.Iterate(next, false, func(l string, r int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), l, testNumbers[i].L)
				assert(t, fmt.Sprintf("iterate number #%v", i), r, testNumbers[i].R)
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
			next, err := numbers.Iterate(nil, true, func(l string, r int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), l, testNumbers[len(testNumbers)-1-i].L)
				assert(t, fmt.Sprintf("iterate number #%v", i), r, testNumbers[len(testNumbers)-1-i].R)
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
			next, err := numbers.Iterate(nil, true, func(l string, r int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), l, testNumbers[len(testNumbers)-1-i].L)
				assert(t, fmt.Sprintf("iterate number #%v", i), r, testNumbers[len(testNumbers)-1-i].R)
				i++

				if i == 4 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, "one")

			next, err = numbers.Iterate(next, true, func(l string, r int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), l, testNumbers[len(testNumbers)-1-i].L)
				assert(t, fmt.Sprintf("iterate number #%v", i), r, testNumbers[len(testNumbers)-1-i].R)
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

func TestAssociation_iterateLeftValues(t *testing.T) {
	db := newNumbersDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var i int
			next, err := numbers.IterateLeftValues(nil, false, func(l string) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), l, testNumbers[i].L)
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
			next, err := numbers.IterateLeftValues(nil, false, func(l string) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), l, testNumbers[i].L)
				i++

				if i == 4 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, "six")

			next, err = numbers.IterateLeftValues(next, false, func(l string) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), l, testNumbers[i].L)
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
			next, err := numbers.IterateLeftValues(nil, true, func(l string) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), l, testNumbers[len(testNumbers)-1-i].L)
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
			next, err := numbers.IterateLeftValues(nil, true, func(l string) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), l, testNumbers[len(testNumbers)-1-i].L)
				i++

				if i == 4 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, "one")

			next, err = numbers.IterateLeftValues(next, true, func(l string) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), l, testNumbers[len(testNumbers)-1-i].L)
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
			next, err := numbers.IterateLeftValues(nil, false, func(_ string) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestAssociation_iterateRightValues(t *testing.T) {
	db := newNumbersDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			var i int
			next, err := numbers.IterateRightValues(nil, false, func(r int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), r, testNumbersValueSorted[i].V)
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
			next, err := numbers.IterateRightValues(nil, false, func(r int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), r, testNumbersValueSorted[i].V)
				i++

				if i == 4 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 5)

			next, err = numbers.IterateRightValues(next, false, func(r int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), r, testNumbersValueSorted[i].V)
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
			next, err := numbers.IterateRightValues(nil, true, func(r int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), r, testNumbersValueSorted[len(testNumbersValueSorted)-1-i].V)
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
			next, err := numbers.IterateRightValues(nil, true, func(r int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), r, testNumbersValueSorted[len(testNumbersValueSorted)-1-i].V)
				i++

				if i == 4 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 3)

			next, err = numbers.IterateRightValues(next, true, func(r int) (bool, error) {
				assert(t, fmt.Sprintf("iterate number #%v", i), r, testNumbersValueSorted[len(testNumbersValueSorted)-1-i].V)
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
			next, err := numbers.IterateRightValues(nil, false, func(_ int) (bool, error) {
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

func TestAssociation_pageOfLeftValues(t *testing.T) {
	db := newNumbersDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			_, _, _, err := numbers.PageOfLeftValues(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = numbers.PageOfLeftValues(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := numbers.PageOfLeftValues(1, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberLeftValues(0, 1, 2))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.PageOfLeftValues(2, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberLeftValues(3, 4, 5))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.PageOfLeftValues(3, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberLeftValues(6))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			_, _, _, err := numbers.PageOfLeftValues(-1, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = numbers.PageOfLeftValues(0, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := numbers.PageOfLeftValues(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberLeftValues(6, 5, 4))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.PageOfLeftValues(2, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberLeftValues(3, 2, 1))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.PageOfLeftValues(3, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberLeftValues(0))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			page, totalElements, totalPages, err := numbers.PageOfLeftValues(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestAssociation_pageOfRightValues(t *testing.T) {
	db := newNumbersDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			_, _, _, err := numbers.PageOfRightValues(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = numbers.PageOfRightValues(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := numbers.PageOfRightValues(1, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberRightValues(0, 1, 2))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.PageOfRightValues(2, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberRightValues(3, 4, 5))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.PageOfRightValues(3, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberRightValues(6))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			_, _, _, err := numbers.PageOfRightValues(-1, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = numbers.PageOfRightValues(0, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := numbers.PageOfRightValues(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberRightValues(6, 5, 4))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.PageOfRightValues(2, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberRightValues(3, 2, 1))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = numbers.PageOfRightValues(3, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, numberRightValues(0))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			numbers := numbersDefinition.Association(tx)

			page, totalElements, totalPages, err := numbers.PageOfRightValues(1, 3, true)
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

		l, err := numbers.Left(0)
		assertError(t, "", err, boltron.ErrNotFound)
		assert(t, "", l, "")

		has, err := numbers.HasRight(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		r, err := numbers.Right("missing")
		assertError(t, "", err, boltron.ErrNotFound)
		assert(t, "", r, 0)

		has, err = numbers.HasLeft("missing")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = numbers.DeleteByLeft("missing", true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = numbers.DeleteByLeft("missing", false)
		assertError(t, "", err, nil)

		err = numbers.DeleteByRight(0, true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = numbers.DeleteByRight(0, false)
		assertError(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := numbersDefinition.Association(tx)

		err := numbers.Set("one", 1)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := numbersDefinition.Association(tx)

		l, err := numbers.Left(0)
		assertError(t, "", err, boltron.ErrNotFound)
		assert(t, "", l, "")

		has, err := numbers.HasRight(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		r, err := numbers.Right("missing")
		assertError(t, "", err, boltron.ErrNotFound)
		assert(t, "", r, 0)

		has, err = numbers.HasLeft("missing")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = numbers.DeleteByLeft("missing", true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = numbers.DeleteByLeft("missing", false)
		assertError(t, "", err, nil)

		err = numbers.DeleteByRight(0, true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = numbers.DeleteByRight(0, false)
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

		l, err := numbers.Left(0)
		assertError(t, "", err, errNotFoundCustom)
		assert(t, "", l, "")

		has, err := numbers.HasRight(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		r, err := numbers.Right("missing")
		assertError(t, "", err, errNotFoundCustom)
		assert(t, "", r, 0)

		has, err = numbers.HasLeft("missing")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = numbers.DeleteByLeft("missing", true)
		assertError(t, "", err, errNotFoundCustom)

		err = numbers.DeleteByLeft("missing", false)
		assertError(t, "", err, nil)

		err = numbers.DeleteByRight(0, true)
		assertError(t, "", err, errNotFoundCustom)

		err = numbers.DeleteByRight(0, false)
		assertError(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := customNumbersDefinition.Association(tx)

		err := numbers.Set("one", 1)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		numbers := customNumbersDefinition.Association(tx)

		l, err := numbers.Left(0)
		assertError(t, "", err, errNotFoundCustom)
		assert(t, "", l, "")

		has, err := numbers.HasRight(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		r, err := numbers.Right("missing")
		assertError(t, "", err, errNotFoundCustom)
		assert(t, "", r, 0)

		has, err = numbers.HasLeft("missing")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = numbers.DeleteByLeft("missing", true)
		assertError(t, "", err, errNotFoundCustom)

		err = numbers.DeleteByLeft("missing", false)
		assertError(t, "", err, nil)

		err = numbers.DeleteByRight(0, true)
		assertError(t, "", err, errNotFoundCustom)

		err = numbers.DeleteByRight(0, false)
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
		assertErrorFail(t, "", err, boltron.ErrLeftExists)

		err = numbers.Set("two", 1)
		assertErrorFail(t, "", err, boltron.ErrRightExists)

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
			ErrLeftExists:  errKeyExistsCustom,
			ErrRightExists: errValueExistsCustom,
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
			err := numbers.Set(n.L, n.R)
			assertErrorFail(t, fmt.Sprintf("%+v", n), err, nil)
		}
	})

	return db
}

func numberElements(is ...int) []boltron.AssociationElement[string, int] {
	s := make([]boltron.AssociationElement[string, int], 0, len(is))
	for _, i := range is {
		s = append(s, boltron.AssociationElement[string, int]{
			Left:  testNumbers[i].L,
			Right: testNumbers[i].R,
		})
	}
	return s
}

func numberLeftValues(is ...int) []string {
	s := make([]string, 0, len(is))
	for _, i := range is {
		s = append(s, testNumbers[i].L)
	}
	return s
}

func numberRightValues(is ...int) []int {
	s := make([]int, 0, len(is))
	for _, i := range is {
		s = append(s, testNumbersValueSorted[i].V)
	}
	return s
}
