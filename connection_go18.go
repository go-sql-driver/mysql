// +build go1.8

package mysql

import (
	"context"
)

func (mc *mysqlConn) Ping(ctx context.Context) error {
	return mc.writeCommandPacket(comPing)
}
