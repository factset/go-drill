package drill

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/factset/go-drill/internal/rpc/proto/exec/rpc"
	"github.com/factset/go-drill/internal/rpc/proto/exec/shared"
	"github.com/factset/go-drill/internal/rpc/proto/exec/user"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var rbData = []struct {
	qd  string
	raw string
}{
	{
		qd:  "0a120905fed7bc6f65ca2011933559ea008ca27910091a9c01080912190a0408061001120d120b4e5f4e4154494f4e4b455920093848122d0a04081a1001120812064e5f4e414d451a170a04081f1001120b1209246f66667365747324200a38282009386312190a0408061001120d120b4e5f524547494f4e4b45592009384812310a04081a1001120b12094e5f434f4d4d454e541a170a04081f1001120b1209246f66667365747324200a38282009389a051800",
		raw: "000000000000000001000000000000000200000000000000030000000000000004000000000000000500000000000000060000000000000007000000000000000800000000000000000000000700000010000000160000001c00000021000000290000002f000000360000003b000000414c4745524941415247454e54494e414252415a494c43414e4144414547595054455448494f5049414652414e43454745524d414e59494e44494100000000000000000100000000000000010000000000000001000000000000000400000000000000000000000000000003000000000000000300000000000000020000000000000000000000330000007f000000ea0000004f010000b2010000d1010000f7010000310200007202000020686167676c652e206361726566756c6c792066696e616c206465706f736974732064657465637420736c796c792061676169616c20666f7865732070726f6d69736520736c796c79206163636f7264696e6720746f2074686520726567756c6172206163636f756e74732e20626f6c6420726571756573747320616c6f6e7920616c6f6e6773696465206f66207468652070656e64696e67206465706f736974732e206361726566756c6c79207370656369616c207061636b61676573206172652061626f7574207468652069726f6e696320666f726765732e20736c796c79207370656369616c206561732068616e672069726f6e69632c2073696c656e74207061636b616765732e20736c796c7920726567756c6172207061636b616765732061726520667572696f75736c79206f76657220746865207469746865732e20666c756666696c7920626f6c64792061626f766520746865206361726566756c6c7920756e757375616c207468656f646f6c697465732e2066696e616c206475676f7574732061726520717569636b6c79206163726f73732074686520667572696f75736c7920726567756c6172206476656e207061636b616765732077616b6520717569636b6c792e2072656775726566756c6c792066696e616c2072657175657374732e20726567756c61722c2069726f6e696c20706c6174656c6574732e20726567756c6172206163636f756e747320782d7261793a20756e757375616c2c20726567756c6172206163636f737320657863757365732063616a6f6c6520736c796c79206163726f737320746865207061636b616765732e206465706f73697473207072696e742061726f756e",
	},
	{
		qd:  "0a120905fed7bc6f65ca2011933559ea008ca27910091a9c01080912190a0408061001120d120b4e5f4e4154494f4e4b455920093848122d0a04081a1001120812064e5f4e414d451a170a04081f1001120b1209246f66667365747324200a38282009385e12190a0408061001120d120b4e5f524547494f4e4b45592009384812310a04081a1001120b12094e5f434f4d4d454e541a170a04081f1001120b1209246f66667365747324200a3828200938b7051800",
		raw: "09000000000000000a000000000000000b000000000000000c000000000000000d000000000000000e000000000000000f000000000000001000000000000000110000000000000000000000090000000d00000011000000160000001c00000021000000280000003200000036000000494e444f4e455349414952414e495241514a4150414e4a4f5244414e4b454e59414d4f524f43434f4d4f5a414d4249515545504552550200000000000000040000000000000004000000000000000200000000000000040000000000000000000000000000000000000000000000000000000000000001000000000000000000000072000000a4000000e60000000a010000410100009e010000f8010000250200008f02000020736c796c792065787072657373206173796d70746f7465732e20726567756c6172206465706f7369747320686167676c6520736c796c792e206361726566756c6c792069726f6e696320686f636b657920706c617965727320736c65657020626c697468656c792e206361726566756c6c6566756c6c7920616c6f6e6773696465206f662074686520736c796c792066696e616c20646570656e64656e636965732e206e6963206465706f7369747320626f6f73742061746f702074686520717569636b6c792066696e616c2072657175657374733f20717569636b6c7920726567756c616f75736c792e2066696e616c2c20657870726573732067696674732063616a6f6c6520616963206465706f736974732061726520626c697468656c792061626f757420746865206361726566756c6c7920726567756c61722070612070656e64696e67206578637573657320686167676c6520667572696f75736c79206465706f736974732e2070656e64696e672c20657870726573732070696e746f206265616e732077616b6520666c756666696c7920706173742074726e732e20626c697468656c7920626f6c6420636f7572747320616d6f6e672074686520636c6f73656c7920726567756c6172207061636b616765732075736520667572696f75736c7920626f6c6420706c6174656c6574733f732e2069726f6e69632c20756e757375616c206173796d70746f7465732077616b6520626c697468656c792072706c6174656c6574732e20626c697468656c792070656e64696e6720646570656e64656e636965732075736520666c756666696c79206163726f737320746865206576656e2070696e746f206265616e732e206361726566756c6c792073696c656e74206163636f756e",
	},
	{
		qd:  "0a120905fed7bc6f65ca2011933559ea008ca27910071a9c01080712190a0408061001120d120b4e5f4e4154494f4e4b455920073838122d0a04081a1001120812064e5f4e414d451a170a04081f1001120b1209246f66667365747324200838202007386012190a0408061001120d120b4e5f524547494f4e4b45592007383812310a04081a1001120b12094e5f434f4d4d454e541a170a04081f1001120b1209246f6666736574732420083820200738e0041800",
		raw: "120000000000000013000000000000001400000000000000150000000000000016000000000000001700000000000000180000000000000000000000050000000c000000180000001f0000002500000033000000400000004348494e41524f4d414e4941534155444920415241424941564945544e414d525553534941554e49544544204b494e47444f4d554e49544544205354415445530200000000000000030000000000000004000000000000000200000000000000030000000000000003000000000000000100000000000000000000005b000000ca000000180100004601000095010000d2010000400200006320646570656e64656e636965732e20667572696f75736c792065787072657373206e6f746f726e697320736c65657020736c796c7920726567756c6172206163636f756e74732e20696465617320736c6565702e206465706f73756c6172206173796d70746f746573206172652061626f75742074686520667572696f7573206d756c7469706c696572732e206578707265737320646570656e64656e63696573206e61672061626f7665207468652069726f6e6963616c6c792069726f6e6963206163636f756e7474732e2073696c656e7420726571756573747320686167676c652e20636c6f73656c792065787072657373207061636b6167657320736c656570206163726f73732074686520626c697468656c7968656c7920656e746963696e676c792065787072657373206163636f756e74732e206576656e2c2066696e616c2020726571756573747320616761696e73742074686520706c6174656c65747320757365206e65766572206163636f7264696e6720746f2074686520717569636b6c7920726567756c61722070696e7465616e7320626f6f7374206361726566756c6c79207370656369616c2072657175657374732e206163636f756e7473206172652e206361726566756c6c792066696e616c207061636b616765732e20736c6f7720666f7865732063616a6f6c6520717569636b6c792e20717569636b6c792073696c656e7420706c6174656c657473206272656163682069726f6e6963206163636f756e74732e20756e757375616c2070696e746f206265",
	},
}

const successfulQueryResult = "080212120905fed7bc6f65ca2011933559ea008ca279"

func getTestResultHandle() *ResultHandle {
	dc := make(chan *queryData)
	go func() {
		defer close(dc)

		for _, d := range rbData {
			b, _ := hex.DecodeString(d.qd)
			r, _ := hex.DecodeString(d.raw)

			qd := shared.QueryData{}
			proto.Unmarshal(b, &qd)
			dc <- &queryData{typ: int32(user.RpcType_QUERY_DATA), msg: &qd, raw: r}
		}

		qrbytes, _ := hex.DecodeString(successfulQueryResult)
		qr := shared.QueryResult{}
		proto.Unmarshal(qrbytes, &qr)
		dc <- &queryData{typ: int32(user.RpcType_QUERY_RESULT), msg: &qr}
	}()
	return &ResultHandle{
		dataChannel: dc,
	}
}

var failureResult = &shared.QueryResult{
	QueryId: &shared.QueryId{Part1: proto.Int64(2362793857957860714), Part2: proto.Int64(5294962225538573329)},
	Error: []*shared.DrillPBError{
		{
			ErrorId:   proto.String("d4f3fffc-b200-4d28-8a77-bb02c54fada0"),
			Endpoint:  nil,
			ErrorType: shared.DrillPBError_VALIDATION.Enum(),
			Message:   proto.String("Failure Error Test"),
		},
	},
	QueryState: shared.QueryResult_FAILED.Enum(),
}

var cancelledResult = &shared.QueryResult{
	QueryState: shared.QueryResult_CANCELED.Enum(),
}

func ExampleResultHandle_Next() {
	// using sample nation data set from Drill repo
	rh := getTestResultHandle()

	// iterate the batches
	batch, err := rh.Next()
	for ; err == nil; batch, err = rh.Next() {
		for i := int32(0); i < batch.NumRows(); i++ {
			for _, v := range batch.GetVectors() {
				val := v.Value(uint(i))
				switch t := val.(type) {
				case []byte:
					fmt.Print("|", string(t))
				default:
					fmt.Print("|", t)
				}
			}
			fmt.Println("|")
		}
	}

	fmt.Println(err == io.EOF)

	// Output:
	// |0|ALGERIA|0| haggle. carefully final deposits detect slyly agai|
	// |1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon|
	// |2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special |
	// |3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold|
	// |4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d|
	// |5|ETHIOPIA|0|ven packages wake quickly. regu|
	// |6|FRANCE|3|refully final requests. regular, ironi|
	// |7|GERMANY|3|l platelets. regular accounts x-ray: unusual, regular acco|
	// |8|INDIA|2|ss excuses cajole slyly across the packages. deposits print aroun|
	// |9|INDONESIA|2| slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull|
	// |10|IRAN|4|efully alongside of the slyly final dependencies. |
	// |11|IRAQ|4|nic deposits boost atop the quickly final requests? quickly regula|
	// |12|JAPAN|2|ously. final, express gifts cajole a|
	// |13|JORDAN|4|ic deposits are blithely about the carefully regular pa|
	// |14|KENYA|0| pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t|
	// |15|MOROCCO|0|rns. blithely bold courts among the closely regular packages use furiously bold platelets?|
	// |16|MOZAMBIQUE|0|s. ironic, unusual asymptotes wake blithely r|
	// |17|PERU|1|platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun|
	// |18|CHINA|2|c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos|
	// |19|ROMANIA|3|ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account|
	// |20|SAUDI ARABIA|4|ts. silent requests haggle. closely express packages sleep across the blithely|
	// |21|VIETNAM|2|hely enticingly express accounts. even, final |
	// |22|RUSSIA|3| requests against the platelets use never according to the quickly regular pint|
	// |23|UNITED KINGDOM|3|eans boost carefully special requests. accounts are. carefull|
	// |24|UNITED STATES|1|y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be|
	// true
}

func ExampleResultHandle_Next_queryfailed() {
	dc := make(chan *queryData)
	rh := &ResultHandle{dataChannel: dc}

	go func() {
		defer close(dc)
		dc <- &queryData{typ: int32(user.RpcType_QUERY_RESULT), msg: failureResult}
	}()

	_, err := rh.Next()
	if errors.Is(err, ErrQueryFailed) {
		fmt.Println(err.Error())
	}

	// calling Next again with the closed channel just returns the same error
	_, err2 := rh.Next()
	fmt.Println(err.Error() == err2.Error())

	// Output:
	// drill: query failed: Failure Error Test
	// true
}

func ExampleResultHandle_Next_cancelled() {
	dc := make(chan *queryData)
	rh := &ResultHandle{dataChannel: dc}

	go func() {
		defer close(dc)
		dc <- &queryData{typ: int32(user.RpcType_QUERY_RESULT), msg: cancelledResult}
	}()

	_, err := rh.Next()
	if err == ErrQueryCancelled {
		fmt.Println(err.Error())
	}

	// Output:
	// drill: query cancelled
}

func ExampleResultHandle_GetRecordBatch() {
	// using sample nation data set from Drill repo
	rh := getTestResultHandle()

	// at the start the record batch is nil, so the first call will grab from the channel
	rb := rh.GetRecordBatch()
	fmt.Printf("Num Cols: %d\n", rb.NumCols())
	fmt.Printf("Rows in this Batch: %d\n", rb.NumRows())
	// 0 affected rows since this wasn't an insert / update
	fmt.Printf("Affected Rows: %d\n", rb.AffectedRows())

	for idx, f := range rh.GetCols() {
		fmt.Printf("Col %d: %s\tNullable: %#v\tType: %s\n", idx,
			f, rb.IsNullable(idx), rb.TypeName(idx))
	}

	// we didn't call Next, so GetRecordBatch still returns the same batch we're on
	batch := rh.GetRecordBatch()
	fmt.Println(rb == batch)

	// Output:
	// Num Cols: 4
	// Rows in this Batch: 9
	// Affected Rows: 0
	// Col 0: N_NATIONKEY	Nullable: false	Type: BIGINT
	// Col 1: N_NAME	Nullable: false	Type: VARBINARY
	// Col 2: N_REGIONKEY	Nullable: false	Type: BIGINT
	// Col 3: N_COMMENT	Nullable: false	Type: VARBINARY
	// true
}

func ExampleResultHandle_GetCols() {
	// using sample nation data set from Drill repo
	rh := getTestResultHandle()

	cols := rh.GetCols()
	for _, c := range cols {
		fmt.Println(c)
	}

	// Output:
	// N_NATIONKEY
	// N_NAME
	// N_REGIONKEY
	// N_COMMENT
}

func TestClose(t *testing.T) {
	dataChannel := make(chan *queryData)
	queryID := shared.QueryId{Part1: proto.Int64(1020), Part2: proto.Int64(-12030)}

	testClient := &Client{}
	testClient.resultMap.Store(qid{queryID.GetPart1(), queryID.GetPart2()}, dataChannel)

	rh := ResultHandle{dataChannel: dataChannel, client: testClient, queryID: &queryID}

	assert.Nil(t, rh.Close())

	_, ok := testClient.resultMap.Load(qid{queryID.GetPart1(), queryID.GetPart2()})
	assert.False(t, ok)

	_, ok = <-dataChannel
	assert.False(t, ok)
}

func TestResultHandleNextNilResult(t *testing.T) {
	dc := make(chan *queryData)
	rh := &ResultHandle{dataChannel: dc}

	go func() {
		defer close(dc)
		dc <- &queryData{typ: int32(user.RpcType_QUERY_RESULT), msg: (*shared.QueryResult)(nil)}
	}()

	_, err := rh.Next()
	assert.Same(t, err, ErrQueryUnknownState)
}

func TestResultHandleNextUnknownState(t *testing.T) {
	dc := make(chan *queryData)
	rh := &ResultHandle{dataChannel: dc}

	go func() {
		defer close(dc)
		dc <- &queryData{typ: int32(user.RpcType_QUERY_RESULT), msg: &shared.QueryResult{}}
	}()

	_, err := rh.Next()
	assert.Same(t, err, ErrQueryUnknownState)
}

func TestResultHandleCancel(t *testing.T) {
	testDrillClient := &Client{outbound: make(chan []byte, 2)}

	rh := &ResultHandle{
		queryID: &shared.QueryId{Part1: proto.Int64(123456789), Part2: proto.Int64(9876543)},
		client:  testDrillClient,
	}

	rh.Cancel()

	encoded, ok := <-testDrillClient.outbound
	assert.True(t, ok)

	id := &shared.QueryId{}
	hdr, err := decodeRPCMessage(encoded, id)
	assert.NoError(t, err)
	assert.NotNil(t, hdr)

	assert.EqualValues(t, user.RpcType_CANCEL_QUERY, hdr.GetRpcType())
	assert.EqualValues(t, rpc.RpcMode_REQUEST, hdr.GetMode())
	assert.EqualValues(t, rh.queryID.GetPart1(), id.GetPart1())
	assert.EqualValues(t, rh.queryID.GetPart2(), id.GetPart2())
}
