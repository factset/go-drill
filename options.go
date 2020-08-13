package drill

import "time"

const drillRPCVersion int32 = 5
const clientName = "Apache Drill Golang Client"
const drillVersion = "1.17.0"
const drillMajorVersion = 1
const drillMinorVersion = 17
const drillPatchVersion = 0
const defaultHeartbeatFreq = 15 * time.Second

// Options for a Drill Connection
type Options struct {
	// the default Schema to use
	Schema string
	// true if expected to use encryption for communication
	SaslEncrypt bool
	// the HOST portion to use for the spn to authenticate with, if _HOST or
	// empty, will use the address of the drillbit that is connected to
	ServiceHost string
	// the krb service name to use for authentication
	ServiceName string
	// what authentication mechanism to use, currently only supports kerberos
	// or no auth
	Auth string
	// the Drill clusters name which is used by ZooKeeper to store the endpoint
	// information
	ClusterName string
	// whether or not the server should support complex types such as List
	SupportComplexTypes bool
	// what Application Name to use for connecting to the server
	ApplicationName string
	// the username to authenticate as
	User string
	// the heartbeatfrequency to use, if nil then will use the default (15 seconds)
	// set to 0 to disable it.
	HeartbeatFreq *time.Duration
}
