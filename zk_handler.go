package drill

import (
	"fmt"
	"log"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec"
	"google.golang.org/protobuf/proto"
)

type DrillZK interface {
	GetDrillBits() []string
	GetEndpoint(drillbit string) Drillbit
	Close()
}

type zkHandler struct {
	conn *zk.Conn

	Nodes      []string
	Path       string
	Connecting bool
	Err        error
}

func NewZKHandler(cluster string, nodes ...string) (DrillZK, error) {
	hdlr := &zkHandler{Connecting: true, Nodes: zk.FormatServers(nodes), Path: "/drill/" + cluster}
	cn, _, err := zk.Connect(hdlr.Nodes, 30*time.Second, zk.WithEventCallback(func(ev zk.Event) {
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
			log.Println("Connected to Zookeeper.")
		}
	}))

	if err != nil {
		return nil, err
	}

	hdlr.conn = cn
	return hdlr, nil
}

func (z *zkHandler) GetDrillBits() []string {
	children, stat, err := z.conn.Children(z.Path)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("%+v %+v\n", children, stat)
	return children
}

func (z *zkHandler) GetEndpoint(drillbit string) Drillbit {
	data, _, err := z.conn.Get(z.Path + "/" + drillbit)
	if err != nil {
		log.Fatal(err)
	}

	drillServer := exec.DrillServiceInstance{}
	if err = proto.Unmarshal(data, &drillServer); err != nil {
		log.Fatal(err)
	}

	log.Printf("%+v\n", drillServer.String())

	return drillServer.GetEndpoint()
}

func (z *zkHandler) Close() {
	z.conn.Close()
}
