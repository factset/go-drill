# go-drill

[![PkgGoDev](https://pkg.go.dev/badge/github.com/factset/go-drill)](https://pkg.go.dev/github.com/factset/go-drill)
[![codecov](https://codecov.io/gh/factset/go-drill/branch/master/graph/badge.svg)](https://codecov.io/gh/factset/go-drill)
[![Go Report Card](https://goreportcard.com/badge/github.com/factset/go-drill)](https://goreportcard.com/report/github.com/factset/go-drill)
[![CI Test](https://github.com/factset/go-drill/workflows/Go/badge.svg)](https://github.com/factset/go-drill/actions)
[![Smoke Test](https://github.com/factset/go-drill/workflows/SmokeTest/badge.svg)](https://github.com/factset/go-drill/actions)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

**go-drill** is a highly efficient Pure Go Client and Sql driver for [Apache Drill](https://drill.apache.org) and [Dremio](https://www.dremio.com). 
It differs from other clients / drivers by using the native Protobuf API to communicate instead of the REST API. The use of Protobuf
enables **zero-copy access to the returned data, resulting in greater efficiency.**


At the present time, the driver may be used without authentication or with
authentication via SASL gssapi-krb-5.

In typical use, the driver is initialized with a list of zookeeper hosts
to enable the driver to locate drillbits. It is also possible to connect
directly to a drillbit via the client.

## Install

#### Client

```bash
go get -u github.com/factset/go-drill
```

#### Driver

```bash
go get -u github.com/factset/go-drill/driver
```

## Usage

The driver can be used like a typical Golang SQL driver:

```go
import (
  "strings"
  "database/sql"

  _ "github.com/factset/go-drill/driver"
)

func main() {
  props := []string{
    "zk=zookeeper1,zookeeper2,zookeeper3",
    "auth=kerberos",
    "service=<krb_service_name>",
    "cluster=<clustername>",
  }

  db, err := sql.Open("drill", strings.Join(props, ";"))
}
```

Alternately, you can just use the client directly:

```go
import (
  "context"

  "github.com/factset/go-drill"
)

func main() {
  // create client, doesn't connect yet
  cl := drill.NewClient(drill.Options{/* fill out options */}, "zookeeper1", "zookeeper2", "zookeeper3")

  // connect the client
  err := cl.Connect(context.Background())
  // if there was any issue connecting, err will contain the error, otherwise will
  // be nil if successfully connected
}
```

## Developing

### Refreshing the Protobuf Definitions

A command is provided to easily refresh the protobuf definitions, provided you have
`protoc` already on your `PATH`. The source should be in a directory structure like
`.../github.com/factset/go-drill/` for development, allowing usage of `go generate`
which will run the command.

Alternatively, the provided command `drillProto` can be used manually via
`go run ./internal/cmd/drillProto` from the root of the source directory.

```bash
$ go run ./internal/cmd/drillProto -h
Drill Proto.

Usage:
        drillProto -h | --help
        drillProto download [-o PATH]
        drillProto fixup [-o PATH]
        drillProto gen [-o PATH] ROOTPATH
        drillProto runall [-o PATH] ROOTPATH

Arguments:
        ROOTPATH  location of the root output for the generated .go files

Options:
        -h --help           Show this screen.
        -o PATH --out PATH  .proto destination path [default: protobuf]
```

`drillProto download` will simply download the .proto files to the specified path
from the apache drill github repo.

`drillProto fixup` adds the `option go_package = "github.com/factset/go-drill/internal/rpc/proto/..."` to each file.

`drillProto gen` will generate the `.pb.go` files from the protobuf files, using the
provided `ROOTPATH` as the root output where it will write the files in the structure
of `<ROOTPATH>/github.com/factset/go-drill/internal/rpc/proto/...`.

`drillProto runall` does all of the steps in order as one command.

### Regenerate the data vector handling

Running `go generate ./internal/data` will regenerate the `.gen.go` files from their
templates.
