# go-drill

[![codecov](https://codecov.io/gh/zeroshade/go-drill/branch/master/graph/badge.svg)](https://codecov.io/gh/zeroshade/go-drill)
[![CI Test](https://github.com/zeroshade/go-drill/workflows/Go/badge.svg)](https://github.com/zeroshade/go-drill/actions)
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
go get -u github.com/zeroshade/go-drill
```

#### Driver

```bash
go get -u github.com/zeroshade/go-drill/driver
```

## Usage

The driver can be used like any normal Golang SQL driver:

```go
import (
  "strings"
  "database/sql"

  _ "github.com/zeroshade/go-drill/driver"
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

  "github.com/zeroshade/go-drill"
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

