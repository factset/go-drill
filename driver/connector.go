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

type Connector struct {
	base drill.Conn
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	dc, err := c.base.NewConnection(ctx)
	if err != nil {
		return nil, err
	}
	return &conn{dc}, nil
}

func (c *Connector) Driver() driver.Driver {
	return Driver{}
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

		switch parsed[0] {
		case "zk":
			zknodes = strings.Split(parsed[1], ",")
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
		case "cluster":
			opts.ClusterName = parsed[1]
		case "heartbeat":
			hbsec, err := strconv.Atoi(parsed[1])
			if err != nil {
				return nil, err
			}
			opts.HeartbeatFreq = new(time.Duration)
			*opts.HeartbeatFreq = time.Duration(hbsec) * time.Second
		}
	}

	return &Connector{base: drill.NewDrillClient(opts, zknodes...)}, nil
}
