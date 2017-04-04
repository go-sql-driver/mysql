// +build go1.8

package mysql

import (
	"context"
)

func (mc *mysqlConn) Ping(ctx context.Context) error {
	err := mc.writeCommandPacket(comPing)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		mc.Close()
		return ctx.Err()
	default:
	}

	_, err = mc.readResultOK()
	return err
}
