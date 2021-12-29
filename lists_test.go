// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron_test

import (
	"fmt"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	"resenje.org/boltron"
)

var (
	projectDependenciesDefinition = boltron.NewListsDefinition(
		"project dependencies",
		boltron.StringEncoding,
		boltron.Uint64Base36Encoding, // depdency id in another collection
		boltron.TimeEncoding,
		nil,
	)

	testProjectDependencies = []struct {
		ProjectName string
		DependecyID uint64
		UpdateTime  time.Time
	}{
		{"resenje.org/boltron", 382, time.Unix(1640731016, 0)},
		{"resenje.org/boltron", 121, time.Unix(1640730983, 0)},
		{"resenje.org/boltron", 122, time.Unix(1640731310, 0)},
		{"resenje.org/schulze", 501, time.Unix(1640732181, 0)},
		{"resenje.org/schulze", 121, time.Unix(1640732188, 0)},
		{"resenje.org/schulze", 398, time.Unix(1640732192, 0)},
		{"resenje.org/schulze", 125, time.Unix(1640732205, 0)},
		{"resenje.org/schulze", 881, time.Unix(1640732216, 0)},
		{"resenje.org/web", 398, time.Unix(1640732358, 0)},
		{"resenje.org/web", 121, time.Unix(1640732362, 0)},
		{"resenje.org/web", 125, time.Unix(1640732381, 0)},
		{"resenje.org/web", 881, time.Unix(1640732390, 0)},
		{"resenje.org/pool", 121, time.Unix(1640732487, 0)},
		{"resenje.org/pool", 125, time.Unix(1640732500, 0)},
		{"resenje.org/pool", 881, time.Unix(1640732508, 0)},
	}

	testProjectDependenciesValues = []uint64{
		121,
		122,
		125,
		382,
		398,
		501,
		881,
	}

	testProjectDependenciesLists = []string{
		"resenje.org/boltron",
		"resenje.org/pool",
		"resenje.org/schulze",
		"resenje.org/web",
	}

	testProjectDependenciesListsWithValue125 = []string{
		"resenje.org/pool",
		"resenje.org/schulze",
		"resenje.org/web",
	}
)

func TestLists(t *testing.T) {
	db := projectsDependenciesDB(t)

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependenciesDefinition.Lists(tx)

		for _, d := range testProjectDependencies {
			has, err := projectDependencies.HasList(d.ProjectName)
			assertFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), has, true)

			_, exists, err := projectDependencies.List(d.ProjectName)
			assertFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), exists, true)

			has, err = projectDependencies.HasValue(d.DependecyID)
			assertFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), has, true)
		}

		has, err := projectDependencies.HasList("resenje.org/missing")
		assertFail(t, "", err, nil)
		assert(t, "", has, false)

		has, err = projectDependencies.HasValue(100)
		assertFail(t, "", err, nil)
		assert(t, "", has, false)

		_, exists, err := projectDependencies.List("resenje.org/missing")
		assertFail(t, "", err, nil)
		assert(t, "", exists, false)
	})

	deletedValue := uint64(121)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependenciesDefinition.Lists(tx)

		err := projectDependencies.DeleteValue(120, true)
		assertFail(t, "", err, boltron.ErrNotFound)

		err = projectDependencies.DeleteValue(deletedValue, true)
		assertFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependenciesDefinition.Lists(tx)

		for _, d := range testProjectDependenciesLists {
			has, err := projectDependencies.HasList(d)
			assertFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), has, true)

			list, exists, err := projectDependencies.List(d)
			assertFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), exists, true)

			has, err = list.Has(deletedValue)
			assertFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), has, false)
		}

		for _, d := range testProjectDependenciesValues {
			has, err := projectDependencies.HasValue(d)
			assertFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), has, d != deletedValue)
		}

		has, err := projectDependencies.HasValue(deletedValue)
		assertFail(t, "", err, nil)
		assert(t, "", has, false)
	})

	deletedList := "resenje.org/schulze"

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependenciesDefinition.Lists(tx)

		err := projectDependencies.DeleteList("resenje.org/missing", true)
		assertFail(t, "", err, boltron.ErrNotFound)

		err = projectDependencies.DeleteList(deletedList, true)
		assertFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependenciesDefinition.Lists(tx)

		for _, d := range testProjectDependencies {
			has, err := projectDependencies.HasList(d.ProjectName)
			assertFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), has, d.ProjectName != deletedList)

			_, exists, err := projectDependencies.List(d.ProjectName)
			assertFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), exists, d.ProjectName != deletedList)

			has, err = projectDependencies.HasValue(d.DependecyID)
			assertFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), has, d.DependecyID != deletedValue && d.DependecyID != 501)
		}
	})
}

func TestLists_iterateLists(t *testing.T) {
	db := projectsDependenciesDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			var i int
			next, err := projectDependencies.IterateLists(nil, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesLists[i])
				i++
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			var i int
			next, err := projectDependencies.IterateLists(nil, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesLists[i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", *next, "resenje.org/schulze")

			next, err = projectDependencies.IterateLists(next, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesLists[i])
				i++
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			var i int
			next, err := projectDependencies.IterateLists(nil, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesLists[len(testProjectDependenciesLists)-1-i])
				i++
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			var i int
			next, err := projectDependencies.IterateLists(nil, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesLists[len(testProjectDependenciesLists)-1-i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", *next, "resenje.org/pool")

			next, err = projectDependencies.IterateLists(next, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesLists[len(testProjectDependenciesLists)-1-i])
				i++
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})
}

func TestLists_pageOfLists(t *testing.T) {
	db := projectsDependenciesDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			_, _, _, err := projectDependencies.PageOfLists(-1, 3, false)
			assertFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = projectDependencies.PageOfLists(0, 3, false)
			assertFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := projectDependencies.PageOfLists(1, 2, false)
			assertFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesLists(0, 1))
			assert(t, "", totalElements, 4)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = projectDependencies.PageOfLists(2, 2, false)
			assertFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesLists(2, 3))
			assert(t, "", totalElements, 4)
			assert(t, "", totalPages, 2)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			_, _, _, err := projectDependencies.PageOfLists(-1, 3, false)
			assertFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = projectDependencies.PageOfLists(0, 3, false)
			assertFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := projectDependencies.PageOfLists(1, 2, true)
			assertFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesLists(3, 2))
			assert(t, "", totalElements, 4)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = projectDependencies.PageOfLists(2, 2, true)
			assertFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesLists(1, 0))
			assert(t, "", totalElements, 4)
			assert(t, "", totalPages, 2)
		})
	})
}

func TestLists_iterateListsWithValue(t *testing.T) {
	db := projectsDependenciesDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			var i int
			next, err := projectDependencies.IterateListsWithValue(125, nil, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesListsWithValue125[i])
				i++
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			var i int
			next, err := projectDependencies.IterateListsWithValue(125, nil, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesListsWithValue125[i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", *next, "resenje.org/web")

			next, err = projectDependencies.IterateListsWithValue(125, next, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesListsWithValue125[i])
				i++
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			var i int
			next, err := projectDependencies.IterateListsWithValue(125, nil, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesListsWithValue125[len(testProjectDependenciesListsWithValue125)-1-i])
				i++
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			var i int
			next, err := projectDependencies.IterateListsWithValue(125, nil, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesListsWithValue125[len(testProjectDependenciesListsWithValue125)-1-i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", *next, "resenje.org/pool")

			next, err = projectDependencies.IterateListsWithValue(125, next, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesListsWithValue125[len(testProjectDependenciesListsWithValue125)-1-i])
				i++
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})
}

func TestLists_pageOfListsWithValue(t *testing.T) {
	db := projectsDependenciesDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			_, _, _, err := projectDependencies.PageOfListsWithValue(125, -1, 3, false)
			assertFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = projectDependencies.PageOfListsWithValue(125, 0, 3, false)
			assertFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := projectDependencies.PageOfListsWithValue(125, 1, 2, false)
			assertFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesListsWithValue125(0, 1))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = projectDependencies.PageOfListsWithValue(125, 2, 2, false)
			assertFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesListsWithValue125(2))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			_, _, _, err := projectDependencies.PageOfListsWithValue(125, -1, 3, false)
			assertFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = projectDependencies.PageOfListsWithValue(125, 0, 3, false)
			assertFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := projectDependencies.PageOfListsWithValue(125, 1, 2, true)
			assertFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesListsWithValue125(2, 1))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = projectDependencies.PageOfListsWithValue(125, 2, 2, true)
			assertFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesListsWithValue125(0))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)
		})
	})
}

func TestLists_iterateValues(t *testing.T) {
	db := projectsDependenciesDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			var i int
			next, err := projectDependencies.IterateValues(nil, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesValues[i])
				i++
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			var i int
			next, err := projectDependencies.IterateValues(nil, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesValues[i])
				i++
				if i == 3 {
					return false, nil
				}
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", *next, 382)

			next, err = projectDependencies.IterateValues(next, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesValues[i])
				i++
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			var i int
			next, err := projectDependencies.IterateValues(nil, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesValues[len(testProjectDependenciesValues)-1-i])
				i++
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			var i int
			next, err := projectDependencies.IterateValues(nil, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesValues[len(testProjectDependenciesValues)-1-i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", *next, 398)

			next, err = projectDependencies.IterateValues(next, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesValues[len(testProjectDependenciesValues)-1-i])
				i++
				return true, nil
			})
			assertFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})
}

func TestLists_pageOfValues(t *testing.T) {
	db := projectsDependenciesDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			_, _, _, err := projectDependencies.PageOfValues(-1, 3, false)
			assertFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = projectDependencies.PageOfValues(0, 3, false)
			assertFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := projectDependencies.PageOfValues(1, 3, false)
			assertFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesValues(0, 1, 2))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = projectDependencies.PageOfValues(2, 3, false)
			assertFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesValues(3, 4, 5))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = projectDependencies.PageOfValues(3, 3, false)
			assertFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesValues(6))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			_, _, _, err := projectDependencies.PageOfValues(-1, 3, false)
			assertFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = projectDependencies.PageOfValues(0, 3, false)
			assertFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := projectDependencies.PageOfValues(1, 3, true)
			assertFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesValues(6, 5, 4))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = projectDependencies.PageOfValues(2, 3, true)
			assertFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesValues(3, 2, 1))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = projectDependencies.PageOfValues(3, 3, true)
			assertFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesValues(0))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})
}

func projectsDependenciesDB(t testing.TB) *bolt.DB {
	t.Helper()

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependenciesDefinition.Lists(tx)

		boltronProjectDependencies, exists, err := projectDependencies.List("resenje.org/boltron")
		assertFail(t, "", err, nil)
		assert(t, "", exists, false)
		schulzeProjectDependencies, exists, err := projectDependencies.List("resenje.org/schulze")
		assertFail(t, "", err, nil)
		assert(t, "", exists, false)
		webProjectDependencies, exists, err := projectDependencies.List("resenje.org/web")
		assertFail(t, "", err, nil)
		assert(t, "", exists, false)
		poolProjectDependencies, exists, err := projectDependencies.List("resenje.org/pool")
		assertFail(t, "", err, nil)
		assert(t, "", exists, false)

		for _, d := range testProjectDependencies {
			switch d.ProjectName {
			case "resenje.org/boltron":
				err := boltronProjectDependencies.Add(d.DependecyID, d.UpdateTime)
				assertFail(t, fmt.Sprintf("%+v", d), err, nil)
			case "resenje.org/schulze":
				err := schulzeProjectDependencies.Add(d.DependecyID, d.UpdateTime)
				assertFail(t, fmt.Sprintf("%+v", d), err, nil)
			case "resenje.org/web":
				err := webProjectDependencies.Add(d.DependecyID, d.UpdateTime)
				assertFail(t, fmt.Sprintf("%+v", d), err, nil)
			case "resenje.org/pool":
				err := poolProjectDependencies.Add(d.DependecyID, d.UpdateTime)
				assertFail(t, fmt.Sprintf("%+v", d), err, nil)
			}
		}
	})

	return db
}

func projectDependenciesValues(is ...int) []uint64 {
	s := make([]uint64, 0, len(is))
	for _, i := range is {
		s = append(s, testProjectDependenciesValues[i])
	}
	return s
}

func projectDependenciesLists(is ...int) []string {
	s := make([]string, 0, len(is))
	for _, i := range is {
		s = append(s, testProjectDependenciesLists[i])
	}
	return s
}

func projectDependenciesListsWithValue125(is ...int) []string {
	s := make([]string, 0, len(is))
	for _, i := range is {
		s = append(s, testProjectDependenciesListsWithValue125[i])
	}
	return s
}
