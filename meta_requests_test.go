package drill

import (
	"testing"

	"github.com/factset/go-drill/internal/rpc/proto/exec/rpc"
	"github.com/factset/go-drill/internal/rpc/proto/exec/shared"
	"github.com/factset/go-drill/internal/rpc/proto/exec/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestClientGetServerMeta(t *testing.T) {
	cl := NewClient(Options{})
	cl.coordID = 0
	resp := &user.GetServerMetaResp{
		Status: user.RequestStatus_OK.Enum(),
		ServerMeta: &user.ServerMeta{
			CurrentSchema: proto.String("drill"),
		},
	}

	go func() {
		encoded := <-cl.outbound
		require.NotNil(t, encoded)

		msg := &user.GetServerMetaReq{}
		hdr, err := decodeRPCMessage(encoded, msg)
		require.NoError(t, err)

		assert.Equal(t, rpc.RpcMode_REQUEST, hdr.GetMode())
		assert.EqualValues(t, user.RpcType_GET_SERVER_META, hdr.GetRpcType())
		assert.EqualValues(t, 0, hdr.GetCoordinationId())

		ch, ok := cl.queryMap.Load(int32(0))
		assert.True(t, ok)

		body, _ := proto.Marshal(resp)
		ch.(chan *rpc.CompleteRpcMessage) <- &rpc.CompleteRpcMessage{
			ProtobufBody: body,
		}
	}()

	meta, err := cl.GetMetadata()
	assert.NoError(t, err)

	assert.EqualValues(t, resp.ServerMeta.GetCurrentSchema(), meta.GetCurrentSchema())
}

func TestClientGetServerMetaErr(t *testing.T) {
	cl := NewClient(Options{})
	cl.coordID = 0
	resp := &user.GetServerMetaResp{
		Status: user.RequestStatus_FAILED.Enum(),
		Error: &shared.DrillPBError{
			Message: proto.String("error fail"),
		},
	}

	go func() {
		encoded := <-cl.outbound
		require.NotNil(t, encoded)

		msg := &user.GetServerMetaReq{}
		_, err := decodeRPCMessage(encoded, msg)
		require.NoError(t, err)

		ch, ok := cl.queryMap.Load(int32(0))
		assert.True(t, ok)

		body, _ := proto.Marshal(resp)
		ch.(chan *rpc.CompleteRpcMessage) <- &rpc.CompleteRpcMessage{
			ProtobufBody: body,
		}
	}()

	meta, err := cl.GetMetadata()
	assert.Nil(t, meta)
	assert.EqualError(t, err, "get_meta error: error fail")
}

func TestClientGetCatalogs(t *testing.T) {
	cl := NewClient(Options{})
	cl.coordID = 0
	resp := &user.GetCatalogsResp{
		Status: user.RequestStatus_OK.Enum(),
		Catalogs: []*user.CatalogMetadata{
			{
				CatalogName: proto.String("DRILL"),
			},
		},
	}

	go func() {
		encoded := <-cl.outbound
		require.NotNil(t, encoded)

		msg := &user.GetCatalogsReq{}
		hdr, err := decodeRPCMessage(encoded, msg)
		require.NoError(t, err)

		assert.Equal(t, rpc.RpcMode_REQUEST, hdr.GetMode())
		assert.EqualValues(t, user.RpcType_GET_CATALOGS, hdr.GetRpcType())
		assert.EqualValues(t, 0, hdr.GetCoordinationId())

		assert.Equal(t, "%", msg.CatalogNameFilter.GetPattern())
		assert.Nil(t, msg.GetCatalogNameFilter().Escape)

		ch, ok := cl.queryMap.Load(int32(0))
		assert.True(t, ok)

		body, _ := proto.Marshal(resp)
		ch.(chan *rpc.CompleteRpcMessage) <- &rpc.CompleteRpcMessage{
			ProtobufBody: body,
		}
	}()

	meta, err := cl.GetCatalogs("%", nil)
	assert.NoError(t, err)

	assert.EqualValues(t, resp.GetCatalogs()[0].GetCatalogName(), meta[0].GetCatalogName())
}

func TestClientGetCatalogsErr(t *testing.T) {
	cl := NewClient(Options{})
	cl.coordID = 0
	resp := &user.GetCatalogsResp{
		Status: user.RequestStatus_FAILED.Enum(),
		Error: &shared.DrillPBError{
			Message: proto.String("error fail"),
		},
	}

	go func() {
		encoded := <-cl.outbound
		require.NotNil(t, encoded)

		msg := &user.GetCatalogsReq{}
		_, err := decodeRPCMessage(encoded, msg)
		require.NoError(t, err)

		ch, ok := cl.queryMap.Load(int32(0))
		assert.True(t, ok)

		body, _ := proto.Marshal(resp)
		ch.(chan *rpc.CompleteRpcMessage) <- &rpc.CompleteRpcMessage{
			ProtobufBody: body,
		}
	}()

	meta, err := cl.GetCatalogs("", nil)
	assert.Nil(t, meta)
	assert.EqualError(t, err, "get_catalogs error: error fail")
}

func TestClientGetSchemas(t *testing.T) {
	cl := NewClient(Options{})
	cl.coordID = 0
	resp := &user.GetSchemasResp{
		Status: user.RequestStatus_OK.Enum(),
		Schemas: []*user.SchemaMetadata{
			{
				SchemaName: proto.String("driller"),
			},
		},
	}

	go func() {
		encoded := <-cl.outbound
		require.NotNil(t, encoded)

		msg := &user.GetSchemasReq{}
		hdr, err := decodeRPCMessage(encoded, msg)
		require.NoError(t, err)

		assert.Equal(t, rpc.RpcMode_REQUEST, hdr.GetMode())
		assert.EqualValues(t, user.RpcType_GET_SCHEMAS, hdr.GetRpcType())
		assert.EqualValues(t, 0, hdr.GetCoordinationId())

		assert.Equal(t, "%", msg.CatalogNameFilter.GetPattern())
		assert.Equal(t, "`", msg.GetCatalogNameFilter().GetEscape())
		assert.Equal(t, "dr%", msg.GetSchemaNameFilter().GetPattern())
		assert.Equal(t, "`", msg.GetSchemaNameFilter().GetEscape())

		ch, ok := cl.queryMap.Load(int32(0))
		assert.True(t, ok)

		body, _ := proto.Marshal(resp)
		ch.(chan *rpc.CompleteRpcMessage) <- &rpc.CompleteRpcMessage{
			ProtobufBody: body,
		}
	}()

	meta, err := cl.GetSchemas("%", "dr%", proto.String("`"))
	assert.NoError(t, err)

	assert.EqualValues(t, resp.GetSchemas()[0].GetSchemaName(), meta[0].GetSchemaName())
}

func TestClientGetSchemasErr(t *testing.T) {
	cl := NewClient(Options{})
	cl.coordID = 0
	resp := &user.GetSchemasResp{
		Status: user.RequestStatus_FAILED.Enum(),
		Error: &shared.DrillPBError{
			Message: proto.String("error fail"),
		},
	}

	go func() {
		encoded := <-cl.outbound
		require.NotNil(t, encoded)

		msg := &user.GetSchemasReq{}
		_, err := decodeRPCMessage(encoded, msg)
		require.NoError(t, err)

		ch, ok := cl.queryMap.Load(int32(0))
		assert.True(t, ok)

		body, _ := proto.Marshal(resp)
		ch.(chan *rpc.CompleteRpcMessage) <- &rpc.CompleteRpcMessage{
			ProtobufBody: body,
		}
	}()

	meta, err := cl.GetSchemas("", "", nil)
	assert.Nil(t, meta)
	assert.EqualError(t, err, "get_schemas error: error fail")
}

func TestClientGetTables(t *testing.T) {
	cl := NewClient(Options{})
	cl.coordID = 0
	resp := &user.GetTablesResp{
		Status: user.RequestStatus_OK.Enum(),
		Tables: []*user.TableMetadata{
			{
				TableName: proto.String("drill_table_foo"),
			},
			{
				TableName: proto.String("drill_table_2"),
			},
		},
	}

	go func() {
		encoded := <-cl.outbound
		require.NotNil(t, encoded)

		msg := &user.GetTablesReq{}
		hdr, err := decodeRPCMessage(encoded, msg)
		require.NoError(t, err)

		assert.Equal(t, rpc.RpcMode_REQUEST, hdr.GetMode())
		assert.EqualValues(t, user.RpcType_GET_TABLES, hdr.GetRpcType())
		assert.EqualValues(t, 0, hdr.GetCoordinationId())

		assert.Equal(t, "%", msg.CatalogNameFilter.GetPattern())
		assert.Nil(t, msg.GetCatalogNameFilter().Escape)
		assert.Equal(t, "dr%", msg.GetSchemaNameFilter().GetPattern())
		assert.Nil(t, msg.GetSchemaNameFilter().Escape)
		assert.Equal(t, "drill_%", msg.GetTableNameFilter().GetPattern())
		assert.Nil(t, msg.GetTableNameFilter().Escape)
		assert.ElementsMatch(t, []string{"type1", "type2"}, msg.GetTableTypeFilter())

		ch, ok := cl.queryMap.Load(int32(0))
		assert.True(t, ok)

		body, _ := proto.Marshal(resp)
		ch.(chan *rpc.CompleteRpcMessage) <- &rpc.CompleteRpcMessage{
			ProtobufBody: body,
		}
	}()

	meta, err := cl.GetTables("%", "dr%", "drill_%", nil, "type1", "type2")
	assert.NoError(t, err)

	assert.Len(t, meta, len(resp.GetTables()))
	for idx, tbl := range meta {
		assert.EqualValues(t, resp.GetTables()[idx].GetTableName(), tbl.GetTableName())
	}
}

func TestClientGetTablesErr(t *testing.T) {
	cl := NewClient(Options{})
	cl.coordID = 0
	resp := &user.GetTablesResp{
		Status: user.RequestStatus_FAILED.Enum(),
		Error: &shared.DrillPBError{
			Message: proto.String("error fail"),
		},
	}

	go func() {
		encoded := <-cl.outbound
		require.NotNil(t, encoded)

		msg := &user.GetTablesReq{}
		_, err := decodeRPCMessage(encoded, msg)
		require.NoError(t, err)

		ch, ok := cl.queryMap.Load(int32(0))
		assert.True(t, ok)

		body, _ := proto.Marshal(resp)
		ch.(chan *rpc.CompleteRpcMessage) <- &rpc.CompleteRpcMessage{
			ProtobufBody: body,
		}
	}()

	meta, err := cl.GetTables("", "", "", nil)
	assert.Nil(t, meta)
	assert.EqualError(t, err, "get_tables error: error fail")
}

func TestClientGetColumns(t *testing.T) {
	cl := NewClient(Options{})
	cl.coordID = 0
	resp := &user.GetColumnsResp{
		Status: user.RequestStatus_OK.Enum(),
		Columns: []*user.ColumnMetadata{
			{
				ColumnName: proto.String("drill_table_foo"),
			},
		},
	}

	go func() {
		encoded := <-cl.outbound
		require.NotNil(t, encoded)

		msg := &user.GetColumnsReq{}
		hdr, err := decodeRPCMessage(encoded, msg)
		require.NoError(t, err)

		assert.Equal(t, rpc.RpcMode_REQUEST, hdr.GetMode())
		assert.EqualValues(t, user.RpcType_GET_COLUMNS, hdr.GetRpcType())
		assert.EqualValues(t, 0, hdr.GetCoordinationId())

		assert.Equal(t, "%", msg.CatalogNameFilter.GetPattern())
		assert.Nil(t, msg.GetCatalogNameFilter().Escape)
		assert.Equal(t, "dr%", msg.GetSchemaNameFilter().GetPattern())
		assert.Nil(t, msg.GetSchemaNameFilter().Escape)
		assert.Equal(t, "drill_%", msg.GetTableNameFilter().GetPattern())
		assert.Nil(t, msg.GetTableNameFilter().Escape)
		assert.Equal(t, "drill", msg.GetColumnNameFilter().GetPattern())
		assert.Nil(t, msg.GetColumnNameFilter().Escape)

		ch, ok := cl.queryMap.Load(int32(0))
		assert.True(t, ok)

		body, _ := proto.Marshal(resp)
		ch.(chan *rpc.CompleteRpcMessage) <- &rpc.CompleteRpcMessage{
			ProtobufBody: body,
		}
	}()

	meta, err := cl.GetColumns("%", "dr%", "drill_%", "drill", nil)
	assert.NoError(t, err)

	assert.Len(t, meta, len(resp.GetColumns()))
	for idx, tbl := range meta {
		assert.EqualValues(t, resp.GetColumns()[idx].GetColumnName(), tbl.GetColumnName())
	}
}

func TestClientGetColumnsErr(t *testing.T) {
	cl := NewClient(Options{})
	cl.coordID = 0
	resp := &user.GetColumnsResp{
		Status: user.RequestStatus_FAILED.Enum(),
		Error: &shared.DrillPBError{
			Message: proto.String("error fail"),
		},
	}

	go func() {
		encoded := <-cl.outbound
		require.NotNil(t, encoded)

		msg := &user.GetColumnsReq{}
		_, err := decodeRPCMessage(encoded, msg)
		require.NoError(t, err)

		ch, ok := cl.queryMap.Load(int32(0))
		assert.True(t, ok)

		body, _ := proto.Marshal(resp)
		ch.(chan *rpc.CompleteRpcMessage) <- &rpc.CompleteRpcMessage{
			ProtobufBody: body,
		}
	}()

	meta, err := cl.GetColumns("", "", "", "", nil)
	assert.Nil(t, meta)
	assert.EqualError(t, err, "get_columns error: error fail")
}
