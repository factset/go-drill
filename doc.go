// Package drill is a highly efficient Pure Go client and driver for Apache Drill.
//
// A driver for the database/sql package is also provided via the driver subpackage.
// This can be used like so:
//
//   import (
//     "database/sql"
//
//     _ "github.com/factset/go-drill/driver"
//   )
//
//   func main() {
//     props := []string{
//       "zk=zookeeper1,zookeeper2,zookeeper3",
//       "auth=kerberos",
//       "service=<krb_service_name>",
//       "cluster=<clustername>",
//     }
//
//     db, err := sql.Open("drill", strings.Join(props, ";"))
//  }
//
// Also, currently logging of the internals can be turned on via the environment
// variable GO_DRILL_LOG_LEVEL. This uses github.com/rs/zerolog to do the logging
// so anything that is valid to pass to the zerolog.ParseLevel function is valid
// as a value for the environment variable.
package drill
