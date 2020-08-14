package driver

import (
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnImplements(t *testing.T) {
	// verify we implement the interfaces that aren't automatically going to
	// be enforced by the compiler so that we don't mess up any of the functions
	assert.Implements(t, (*driver.Pinger)(nil), new(conn))
	assert.Implements(t, (*driver.QueryerContext)(nil), new(conn))
	assert.Implements(t, (*driver.ExecerContext)(nil), new(conn))
}
