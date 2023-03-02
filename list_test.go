// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	"resenje.org/boltron"
)

var (
	todoDefinition = boltron.NewListDefinition(
		"todo",
		boltron.StringEncoding,
		boltron.TimeEncoding,
		nil,
	)

	testTodo = []struct {
		Value string
		Time  time.Time
	}{
		{"Write more tests", time.Unix(1640780066, 0)},
		{"Add documentation", time.Unix(1640780100, 0)},
		{"Update README.md", time.Unix(1640780301, 0)},
		{"Make a release", time.Unix(1640782040, 0)},
		{"Use new lib in projects", time.Unix(1640782120, 0)},
		{"Get feedback on API design", time.Unix(1640782202, 0)},
		{"Plan new features", time.Unix(1640782349, 0)},
		{"Implement new features", time.Unix(1640782360, 0)},
		{"Release new features", time.Unix(1640782518, 0)},
	}
)

func TestList(t *testing.T) {
	db := newTodoDB(t)

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		todo := todoDefinition.List(tx)

		for _, v := range testTodo {
			has, err := todo.Has(v.Value)
			assertErrorFail(t, fmt.Sprintf("%+v", v), err, nil)
			assert(t, fmt.Sprintf("%+v", v), has, true)

			orderBy, err := todo.OrderBy(v.Value)
			assertErrorFail(t, fmt.Sprintf("%+v", v), err, nil)
			assertTime(t, fmt.Sprintf("%+v", v), orderBy, v.Time)
		}
	})

	removedValue := "Plan new features"

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		todo := todoDefinition.List(tx)

		err := todo.Remove(removedValue, true)
		assertErrorFail(t, "", err, nil)

		err = todo.Remove("missing1", false)
		assertErrorFail(t, "", err, nil)

		err = todo.Remove("missing2", true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		todo := todoDefinition.List(tx)

		for _, v := range testTodo {
			has, err := todo.Has(v.Value)
			assertErrorFail(t, fmt.Sprintf("%+v", v), err, nil)
			assert(t, fmt.Sprintf("%+v", v), has, v.Value != removedValue)

			orderBy, err := todo.OrderBy(v.Value)
			if v.Value == removedValue {
				assertErrorFail(t, fmt.Sprintf("%+v", v), err, boltron.ErrNotFound)
				assertTime(t, fmt.Sprintf("%+v", v), orderBy, time.Time{})
			} else {
				assertErrorFail(t, fmt.Sprintf("%+v", v), err, nil)
				assertTime(t, fmt.Sprintf("%+v", v), orderBy, v.Time)
			}
		}
	})

	value := testTodo[2].Value
	updatedTime := testTodo[2].Time.Add(10 * time.Minute)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		todo := todoDefinition.List(tx)

		err := todo.Add(value, updatedTime)
		assertErrorFail(t, "", err, nil)

		err = todo.Remove("missing1", false)
		assertErrorFail(t, "", err, nil)

		err = todo.Remove("missing2", true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		todo := todoDefinition.List(tx)

		has, err := todo.Has(value)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, true)

		orderBy, err := todo.OrderBy(value)
		assertErrorFail(t, "", err, nil)
		assertTime(t, "", orderBy, updatedTime)
	})
}

func TestList_iterate(t *testing.T) {
	db := newTodoDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			todo := todoDefinition.List(tx)

			var i int
			next, err := todo.Iterate(nil, false, func(v string, o time.Time) (bool, error) {
				assert(t, fmt.Sprintf("iterate todo #%v", i), v, testTodo[i].Value)
				assertTime(t, fmt.Sprintf("iterate todo #%v", i), o, testTodo[i].Time)
				i++

				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			todo := todoDefinition.List(tx)

			var i int
			next, err := todo.Iterate(nil, false, func(v string, o time.Time) (bool, error) {
				assert(t, fmt.Sprintf("iterate todo #%v", i), v, testTodo[i].Value)
				assertTime(t, fmt.Sprintf("iterate todo #%v", i), o, testTodo[i].Time)
				i++

				if i == 3 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next.Value, "Make a release")
			assertTime(t, "", next.OrderBy, time.Unix(1640782040, 0))

			next, err = todo.Iterate(next, false, func(v string, o time.Time) (bool, error) {
				assert(t, fmt.Sprintf("iterate todo #%v", i), v, testTodo[i].Value)
				assertTime(t, fmt.Sprintf("iterate todo #%v", i), o, testTodo[i].Time)
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			todo := todoDefinition.List(tx)

			var i int
			next, err := todo.Iterate(nil, true, func(v string, o time.Time) (bool, error) {
				assert(t, fmt.Sprintf("iterate todo #%v", i), v, testTodo[len(testTodo)-1-i].Value)
				assertTime(t, fmt.Sprintf("iterate todo #%v", i), o, testTodo[len(testTodo)-1-i].Time)
				i++

				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			todo := todoDefinition.List(tx)

			var i int
			next, err := todo.Iterate(nil, true, func(v string, o time.Time) (bool, error) {
				assert(t, fmt.Sprintf("iterate todo #%v", i), v, testTodo[len(testTodo)-1-i].Value)
				assertTime(t, fmt.Sprintf("iterate todo #%v", i), o, testTodo[len(testTodo)-1-i].Time)
				i++

				if i == 3 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next.Value, "Get feedback on API design")
			assertTime(t, "", next.OrderBy, time.Unix(1640782202, 0))

			next, err = todo.Iterate(next, true, func(v string, o time.Time) (bool, error) {
				assert(t, fmt.Sprintf("iterate todo #%v", i), v, testTodo[len(testTodo)-1-i].Value)
				assertTime(t, fmt.Sprintf("iterate todo #%v", i), o, testTodo[len(testTodo)-1-i].Time)
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
			todo := todoDefinition.List(tx)

			var count int
			next, err := todo.Iterate(nil, false, func(_ string, _ time.Time) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestList_iterateValues(t *testing.T) {
	db := newTodoDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			todo := todoDefinition.List(tx)

			var i int
			next, err := todo.IterateValues(nil, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate todo #%v", i), v, testTodo[i].Value)
				i++

				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			todo := todoDefinition.List(tx)

			var i int
			next, err := todo.IterateValues(nil, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate todo #%v", i), v, testTodo[i].Value)
				i++

				if i == 3 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next.Value, "Make a release")
			assertTime(t, "", next.OrderBy, time.Unix(1640782040, 0))

			next, err = todo.IterateValues(next, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate todo #%v", i), v, testTodo[i].Value)
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			todo := todoDefinition.List(tx)

			var i int
			next, err := todo.IterateValues(nil, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate todo #%v", i), v, testTodo[len(testTodo)-1-i].Value)
				i++

				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			todo := todoDefinition.List(tx)

			var i int
			next, err := todo.IterateValues(nil, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate todo #%v", i), v, testTodo[len(testTodo)-1-i].Value)
				i++

				if i == 3 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next.Value, "Get feedback on API design")
			assertTime(t, "", next.OrderBy, time.Unix(1640782202, 0))

			next, err = todo.IterateValues(next, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate todo #%v", i), v, testTodo[len(testTodo)-1-i].Value)
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
			todo := todoDefinition.List(tx)

			var count int
			next, err := todo.IterateValues(nil, false, func(_ string) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestList_size(t *testing.T) {
	db := newTodoDB(t)

	t.Run("full", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			todo := todoDefinition.List(tx)

			size, err := todo.Size()
			assertErrorFail(t, "", err, nil)
			assert(t, "", size, 9)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			todo := todoDefinition.List(tx)

			size, err := todo.Size()
			assertErrorFail(t, "", err, nil)
			assert(t, "", size, 0)
		})
	})
}

func TestList_page(t *testing.T) {
	db := newTodoDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			todo := todoDefinition.List(tx)

			_, _, _, err := todo.Page(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = todo.Page(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := todo.Page(1, 4, false)
			assertErrorFail(t, "", err, nil)
			assertListElements(t, page, todoElements(0, 1, 2, 3))
			assert(t, "", totalElements, 9)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = todo.Page(2, 4, false)
			assertErrorFail(t, "", err, nil)
			assertListElements(t, page, todoElements(4, 5, 6, 7))
			assert(t, "", totalElements, 9)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = todo.Page(3, 4, false)
			assertErrorFail(t, "", err, nil)
			assertListElements(t, page, todoElements(8))
			assert(t, "", totalElements, 9)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			todo := todoDefinition.List(tx)

			_, _, _, err := todo.Page(-1, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = todo.Page(0, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := todo.Page(1, 4, true)
			assertErrorFail(t, "", err, nil)
			assertListElements(t, page, todoElements(8, 7, 6, 5))
			assert(t, "", totalElements, 9)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = todo.Page(2, 4, true)
			assertErrorFail(t, "", err, nil)
			assertListElements(t, page, todoElements(4, 3, 2, 1))
			assert(t, "", totalElements, 9)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = todo.Page(3, 4, true)
			assertErrorFail(t, "", err, nil)
			assertListElements(t, page, todoElements(0))
			assert(t, "", totalElements, 9)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			todo := todoDefinition.List(tx)

			page, totalElements, totalPages, err := todo.Page(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestList_pageOfValues(t *testing.T) {
	db := newTodoDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			todo := todoDefinition.List(tx)

			_, _, _, err := todo.PageOfValues(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = todo.PageOfValues(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := todo.PageOfValues(1, 4, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, todoValues(0, 1, 2, 3))
			assert(t, "", totalElements, 9)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = todo.PageOfValues(2, 4, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, todoValues(4, 5, 6, 7))
			assert(t, "", totalElements, 9)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = todo.PageOfValues(3, 4, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, todoValues(8))
			assert(t, "", totalElements, 9)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			todo := todoDefinition.List(tx)

			_, _, _, err := todo.PageOfValues(-1, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = todo.PageOfValues(0, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := todo.PageOfValues(1, 4, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, todoValues(8, 7, 6, 5))
			assert(t, "", totalElements, 9)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = todo.PageOfValues(2, 4, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, todoValues(4, 3, 2, 1))
			assert(t, "", totalElements, 9)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = todo.PageOfValues(3, 4, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, todoValues(0))
			assert(t, "", totalElements, 9)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			todo := todoDefinition.List(tx)

			page, totalElements, totalPages, err := todo.PageOfValues(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestList_ErrNotFound(t *testing.T) {
	db := newDB(t)

	notFoundValue := "missing"

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		todo := todoDefinition.List(tx)

		v, err := todo.OrderBy(notFoundValue)
		assertError(t, "", err, boltron.ErrNotFound)
		assert(t, "", v, time.Time{})

		has, err := todo.Has(notFoundValue)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = todo.Remove(notFoundValue, true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = todo.Remove(notFoundValue, false)
		assertError(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		todo := todoDefinition.List(tx)

		err := todo.Add("v", time.Now())
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		todo := todoDefinition.List(tx)

		v, err := todo.OrderBy(notFoundValue)
		assertError(t, "", err, boltron.ErrNotFound)
		assert(t, "", v, time.Time{})

		err = todo.Remove(notFoundValue, true)
		assertError(t, "", err, boltron.ErrNotFound)
	})
}

func TestList_customErrNotFound(t *testing.T) {
	db := newDB(t)

	errNotFoundCustom := errors.New("custom not found error")

	customTodoDefinition := boltron.NewListDefinition(
		"todo",
		boltron.StringEncoding,
		boltron.TimeEncoding,
		&boltron.ListOptions{
			ErrValueNotFound: errNotFoundCustom,
		},
	)

	notFoundValue := "missing"

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		todo := customTodoDefinition.List(tx)

		v, err := todo.OrderBy(notFoundValue)
		assertError(t, "", err, errNotFoundCustom)
		assert(t, "", v, time.Time{})

		has, err := todo.Has(notFoundValue)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = todo.Remove(notFoundValue, true)
		assertError(t, "", err, errNotFoundCustom)

		err = todo.Remove(notFoundValue, false)
		assertError(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		todo := customTodoDefinition.List(tx)

		err := todo.Add("v", time.Now())
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		todo := customTodoDefinition.List(tx)

		v, err := todo.OrderBy(notFoundValue)
		assertError(t, "", err, errNotFoundCustom)
		assert(t, "", v, time.Time{})

		err = todo.Remove(notFoundValue, true)
		assertError(t, "", err, errNotFoundCustom)
	})
}

func newTodoDB(t *testing.T) *bolt.DB {
	t.Helper()

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		todo := todoDefinition.List(tx)

		for _, n := range testTodo {
			err := todo.Add(n.Value, n.Time)
			assertErrorFail(t, fmt.Sprintf("%+v", n), err, nil)
		}
	})

	return db
}

func todoElements(is ...int) []boltron.ListElement[string, time.Time] {
	s := make([]boltron.ListElement[string, time.Time], 0, len(is))
	for _, i := range is {
		s = append(s, boltron.ListElement[string, time.Time]{
			Value:   testTodo[i].Value,
			OrderBy: testTodo[i].Time,
		})
	}
	return s
}

func todoValues(is ...int) []string {
	s := make([]string, 0, len(is))
	for _, i := range is {
		s = append(s, testTodo[i].Value)
	}
	return s
}

func assertListElements[V any, O time.Time](t testing.TB, got, want []boltron.ListElement[V, time.Time]) {
	t.Helper()

	if len(got) != len(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	for i, l := 0, len(got); i < l; i++ {
		assert(t, fmt.Sprintf("element #%v value", i), got[i].Value, want[i].Value)
		assertTime(t, fmt.Sprintf("element #%v time", i), got[i].OrderBy, want[i].OrderBy)
	}
}
