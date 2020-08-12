package drill

import "time"

const drillRpcVersion int32 = 5
const clientName = "Apache Drill Golang Client"
const drillVersion = "1.17.0"
const drillMajorVersion = 1
const drillMinorVersion = 17
const drillPatchVersion = 0
const defaultHeartbeatFreq = 15 * time.Second

type Options struct {
	Schema              string
	SaslEncrypt         bool
	ServiceHost         string
	ServiceName         string
	Auth                string
	ClusterName         string
	SupportComplexTypes bool
	ApplicationName     string
	User                string
	HeartbeatFreq       time.Duration
}
