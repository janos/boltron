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
- Lists

## Installation

Run `go get -u resenje.org/boltron` from command line.

Boltron uses Type Parameters (Generics) introduced in Go 1.18.

## License

This application is distributed under the BSD-style license found in the [LICENSE](LICENSE) file.
