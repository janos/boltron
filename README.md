# BoltDB data containers

[![Go](https://github.com/janos/boltron/workflows/Go/badge.svg)](https://github.com/janos/boltron/actions)
[![PkgGoDev](https://pkg.go.dev/badge/resenje.org/boltron)](https://pkg.go.dev/resenje.org/boltron)
[![NewReleases](https://newreleases.io/badge.svg)](https://newreleases.io/github/janos/boltron)

Package boltron provides type safe generic constructs to design data models
using [BoltDB](go.etcd.io/bbolt) embedded key value store.

Definitions statically define encodings and options for other types to be
serialized and they provide methods to access and modify serialized data
within bolt transactions.

There are three basic types with their definitions:

- Collection
- Association
- List

One complex types provides methods to manage sets of basic types:

- Collections
- Associations
- Lists

## Installation

Run `go get -u resenje.org/boltron` from command line.

Boltron uses Type Parameters (Generics) introduced in Go 1.18.

## Collection

Collections is the most basic key value relation where key is an unique identifier. The whole collection can be iterated by the order of keys.

Define the Collection of Records:

```go
type Record struct {
	Message string
}

var recordsDefinition = boltron.NewCollectionDefinition(
	"records",
	boltron.Uint64BinaryEncoding,       // records are identified by their integer values
	boltron.NewJSONEncoding[*Record](), // JSON encoding of the Record type,
	nil,
)
```

Use it in transaction:

```go
db.Update(func(tx *bolt.Tx) error {
	records := recordsDefinition.Collection(tx)
	_, err := records.Save(42, &Record{
		Message: "Hello",
	}, false)
	return err
})
```

## Association

Association represents a simple one-to-one relation. It is useful to associate identifiers and quickly lookup relations from either lef ot right side, as well to iterate over them and paginate.

## List

List is a list of values, ordered by the provided order type. List values are unique, but the order by values are not. If the order is defined by the values encoding, or it is not important, order by encoding should be set to NullEncoding.

## Collections

Collections is a set of Collections, each identified by an unique collection key. All collections have the same key and value encodings.

Collections provide methods to access individual Collection by its collection key, as well methods to get information about all keys and remove a key from all Collections.

## Associations

Associations is a set of Associations, each identified by an unique key. All associations have the same left and right value encodings.

Associations provide methods to get individual Associations to manage relations by their Left values.

## Lists

Lists is a set of Lists, each identified by an unique key. All lists have the same value and order by encodings.

Lists provide methods to get individual lists by their keys and to manage values from all of them.

## License

This application is distributed under the BSD-style license found in the [LICENSE](LICENSE) file.
