package drill

import (
	"fmt"

	"github.com/factset/go-drill/internal/rpc/proto/exec/rpc"
	"github.com/factset/go-drill/internal/rpc/proto/exec/user"
)

func getLikeFilter(pattern string, escape *string) *user.LikeFilter {
	return &user.LikeFilter{
		Pattern: &pattern,
		Escape:  escape,
	}
}

// GetMetadata returns a structure consisting of all of the Drill Server metadata
// including what sql keywords are supported, escape characters, max lengths etc.
func (d *Client) GetMetadata() (*user.ServerMeta, error) {
	resp := &user.GetServerMetaResp{}
	if err := d.makeReqGetResp(rpc.RpcMode_REQUEST, user.RpcType_GET_SERVER_META, &user.GetServerMetaReq{}, resp); err != nil {
		return nil, err
	}

	if resp.GetStatus() != user.RequestStatus_OK {
		return nil, fmt.Errorf("get_meta error: %s", resp.Error.GetMessage())
	}

	return resp.ServerMeta, nil
}

// GetCatalogs uses the given pattern to search and return the catalogs available on
// the server. For drill, this is always only "DRILL". The syntax of the pattern is
// equivalent to using a LIKE sql expression. If there is no need to escape characters
// in the search filter, pass nil for the second argument, otherwise it should point
// to a string consisting of the characters used for escaping in the pattern.
func (d *Client) GetCatalogs(pattern string, escape *string) ([]*user.CatalogMetadata, error) {
	req := &user.GetCatalogsReq{
		CatalogNameFilter: getLikeFilter(pattern, escape),
	}

	resp := &user.GetCatalogsResp{}
	if err := d.makeReqGetResp(rpc.RpcMode_REQUEST, user.RpcType_GET_CATALOGS, req, resp); err != nil {
		return nil, err
	}

	if resp.GetStatus() != user.RequestStatus_OK {
		return nil, fmt.Errorf("get_catalogs error: %s", resp.Error.GetMessage())
	}

	return resp.Catalogs, nil
}

// GetSchemas returns all the schemas which fit the filter patterns provided.
//
// The syntax for the filter pattern is the same as for GetCatalogs.
func (d *Client) GetSchemas(catalogPattern, schemaPattern string, escape *string) ([]*user.SchemaMetadata, error) {
	req := &user.GetSchemasReq{
		CatalogNameFilter: getLikeFilter(catalogPattern, escape),
		SchemaNameFilter:  getLikeFilter(schemaPattern, escape),
	}

	resp := &user.GetSchemasResp{}
	if err := d.makeReqGetResp(rpc.RpcMode_REQUEST, user.RpcType_GET_SCHEMAS, req, resp); err != nil {
		return nil, err
	}

	if resp.GetStatus() != user.RequestStatus_OK {
		return nil, fmt.Errorf("get_schemas error: %s", resp.Error.GetMessage())
	}

	return resp.GetSchemas(), nil
}

// GetTables returns the metadata for all the tables which fit the filter patterns
// provided and are of the table types passed in.
//
// The syntax for the filter pattern is the same as for GetCatalogs.
func (d *Client) GetTables(catalogPattern, schemaPattern, tablePattern string, escape *string, tableTypes ...string) ([]*user.TableMetadata, error) {
	req := &user.GetTablesReq{
		CatalogNameFilter: getLikeFilter(catalogPattern, escape),
		SchemaNameFilter:  getLikeFilter(schemaPattern, escape),
		TableNameFilter:   getLikeFilter(tablePattern, escape),
		TableTypeFilter:   tableTypes,
	}

	resp := &user.GetTablesResp{}
	if err := d.makeReqGetResp(rpc.RpcMode_REQUEST, user.RpcType_GET_TABLES, req, resp); err != nil {
		return nil, err
	}

	if resp.GetStatus() != user.RequestStatus_OK {
		return nil, fmt.Errorf("get_tables error: %s", resp.Error.GetMessage())
	}

	return resp.GetTables(), nil
}

// GetColumns returns the metadata for all the columns from all the tables which fit the provided
// filter patterns.
//
// The syntax for the filter pattern is the same as for GetCatalogs.
func (d *Client) GetColumns(catalogPattern, schemaPattern, tablePattern, columnPattern string, escape *string) ([]*user.ColumnMetadata, error) {
	req := &user.GetColumnsReq{
		CatalogNameFilter: getLikeFilter(catalogPattern, escape),
		SchemaNameFilter:  getLikeFilter(schemaPattern, escape),
		TableNameFilter:   getLikeFilter(tablePattern, escape),
		ColumnNameFilter:  getLikeFilter(columnPattern, escape),
	}

	resp := &user.GetColumnsResp{}
	if err := d.makeReqGetResp(rpc.RpcMode_REQUEST, user.RpcType_GET_COLUMNS, req, resp); err != nil {
		return nil, err
	}

	if resp.GetStatus() != user.RequestStatus_OK {
		return nil, fmt.Errorf("get_columns error: %s", resp.Error.GetMessage())
	}

	return resp.GetColumns(), nil
}
