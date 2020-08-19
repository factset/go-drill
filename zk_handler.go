package drill

import (
	"fmt"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/zeroshade/go-drill/internal/log"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec"
	"google.golang.org/protobuf/proto"
)

type zkconn interface {
	Get(path string) ([]byte, *zk.Stat, error)
	Children(path string) ([]string, *zk.Stat, error)
	Close()
}

type zkHandler struct {
	conn zkconn

	Nodes      []string
	Path       string
	Connecting bool
	Err        error
}

// newZKHandler attempts to connect to a zookeeper cluster made up of the provided nodes.
//
// The cluster passed in here would be the Drill cluster name which is used to form the path
// to the drill meta data information.
func newZKHandler(cluster string, nodes ...string) (*zkHandler, error) {
	hdlr := &zkHandler{Connecting: true, Nodes: zk.FormatServers(nodes), Path: "/drill/" + cluster}
	var err error
	hdlr.conn, _, err = zk.Connect(hdlr.Nodes, 30*time.Second, zk.WithLogger(&log.Logger), zk.WithEventCallback(func(ev zk.Event) {
		switch ev.Type {
		case zk.EventSession:
			switch ev.State {
			case zk.StateAuthFailed:
				hdlr.Err = fmt.Errorf("ZK Auth Failed: %w", zk.ErrAuthFailed)
				hdlr.conn.Close()
			case zk.StateExpired:
				hdlr.Err = fmt.Errorf("ZK Session Expired: %w", zk.ErrSessionExpired)
				hdlr.conn.Close()
			}
		}

		hdlr.Connecting = false
		if ev.State == zk.StateConnected {
			log.Print("Connected to Zookeeper.")
		}
	}))

	if err != nil {
		return nil, err
	}

	return hdlr, nil
}

// GetDrillBits returns the list of drillbit names that can in turn be passed to
// GetEndpoint to get the endpoint information to connect to them.
func (z *zkHandler) GetDrillBits() []string {
	children, stat, err := z.conn.Children(z.Path)
	if err != nil {
		z.Err = err
	}

	log.Printf("%+v %+v", children, stat)
	return children
}

// GetEndpoint returns the information necessary to connect to a given drillbit
// from its name.
func (z *zkHandler) GetEndpoint(drillbit string) Drillbit {
	data, _, err := z.conn.Get(z.Path + "/" + drillbit)
	if err != nil {
		z.Err = err
		return nil
	}

	drillServer := exec.DrillServiceInstance{}
	if err = proto.Unmarshal(data, &drillServer); err != nil {
		z.Err = err
		return nil
	}

	log.Printf("%+v", drillServer.String())

	return drillServer.GetEndpoint()
}

// Close closes the zookeeper connection and should be called when finished.
func (z *zkHandler) Close() {
	z.conn.Close()
}
