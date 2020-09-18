# go-drill

[![PkgGoDev](https://pkg.go.dev/badge/github.com/factset/go-drill)](https://pkg.go.dev/github.com/factset/go-drill)
[![codecov](https://codecov.io/gh/factset/go-drill/branch/master/graph/badge.svg)](https://codecov.io/gh/factset/go-drill)
[![Go Report Card](https://goreportcard.com/badge/github.com/factset/go-drill)](https://goreportcard.com/report/github.com/factset/go-drill)
[![CI Test](https://github.com/factset/go-drill/workflows/Go/badge.svg)](https://github.com/factset/go-drill/actions)
[![Smoke Test](https://github.com/factset/go-drill/workflows/SmokeTest/badge.svg)](https://github.com/factset/go-drill/actions)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

go-drill is a highly efficient Pure Go Client and Sql driver for [Apache Drill](https://drill.apache.org). It differs from other clients / drivers by using the
native Protobuf API to communicate with Drill instead of the REST API. As a result
this becomes significatly more performant when dealing with large amounts of data.
The raw bytes that are returned from Drill aren't copied, but are instead
interpreted and used in place via slices for efficiency.

Currently it only supports either no authentication or authentication via SASL
gssapi-krb5.

In addition, the sql driver expects to connect to a zookeeper quorum to find the
drillbits, though you can connect directly to a drillbit via the Client.

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

The driver can be used like any normal Golang SQL driver:

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
