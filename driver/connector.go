package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/zeroshade/go-drill"
)

type connector struct {
	base drill.Conn
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	dc, err := c.base.NewConnection(ctx)
	if err != nil {
		return nil, err
	}
	return &conn{dc}, nil
}

func (c *connector) Driver() driver.Driver {
	return drillDriver{}
}

func parseConnectStr(connectStr string) (driver.Connector, error) {
	opts := drill.Options{}

	var zknodes []string
	args := strings.Split(connectStr, ";")
	for _, kv := range args {
		parsed := strings.Split(kv, "=")
		if len(parsed) != 2 {
			return nil, fmt.Errorf("invalid format for connector string")
		}

		parsed[1] = strings.TrimSpace(parsed[1])

		switch strings.TrimSpace(parsed[0]) {
		case "zk":
			zknodes = strings.Split(parsed[1], ",")
			slash := strings.Index(zknodes[len(zknodes)-1], "/")
			if slash != -1 {
				addr := zknodes[len(zknodes)-1]
				zknodes[len(zknodes)-1] = addr[:slash]
				opts.ZKPath = addr[slash:]
			}
		case "auth":
			opts.Auth = parsed[1]
		case "schema":
			opts.Schema = parsed[1]
		case "service":
			opts.ServiceName = parsed[1]
		case "encrypt":
			val, err := strconv.ParseBool(parsed[1])
			if err != nil {
				return nil, err
			}
			opts.SaslEncrypt = val
		case "user":
			opts.User = parsed[1]
		case "pass":
			opts.Passwd = parsed[1]
		case "cluster":
			opts.ClusterName = parsed[1]
		case "heartbeat":
			hbsec, err := strconv.Atoi(parsed[1])
			if err != nil {
				return nil, err
			}
			opts.HeartbeatFreq = new(time.Duration)
			*opts.HeartbeatFreq = time.Duration(hbsec) * time.Second
		default:
			return nil, fmt.Errorf("invalid argument for connection string: %s", parsed[0])
		}
	}

	return &connector{base: drill.NewClient(opts, zknodes...)}, nil
}
