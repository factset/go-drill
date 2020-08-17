// Package drill is a highly efficient Pure Go client and driver for Apache Drill.
//
// A driver for the database/sql package is also provided via the driver subpackage.
// This can be used like so:
//
//   import (
//     "database/sql"
//
//     _ "github.com/zeroshade/go-drill/driver"
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
package drill
