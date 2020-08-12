package drill

import (
	"context"
	"database/sql/driver"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
)

type Connector struct {
	zknodes []string
	opts    Options

	drillbits []string
	curbit    int
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	zook, err := newZKHandler(c.opts.ClusterName, c.zknodes...)
	if err != nil {
		return nil, err
	}
	defer zook.Close()

	if len(c.drillbits) == 0 {
		c.drillbits = zook.GetDrillBits()
		rand.Shuffle(len(c.drillbits), func(i, j int) {
			c.drillbits[i], c.drillbits[j] = c.drillbits[j], c.drillbits[i]
		})
	}

	endpoint := zook.GetEndpoint(c.drillbits[c.curbit])
	c.curbit++

	dc := &drillClient{opts: c.opts, zkNodes: c.zknodes}
	if err := dc.connectEndpoint(ctx, endpoint); err != nil {
		return nil, err
	}
	return dc, nil
}

func (c *Connector) Driver() driver.Driver {
	return Driver{}
}

func parseConnectStr(connectStr string) (driver.Connector, error) {
	opts := Options{}

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
		}
	}

	return &Connector{zknodes, opts, []string{}, 0}, nil
}
