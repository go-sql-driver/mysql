// +build go1.8

package mysql

import (
	"context"
	"time"
)

func (mc *mysqlConn) Ping(ctx context.Context) error {
	err := mc.writeCommandPacket(comPing)
	if err != nil {
		return err
	}

	ch := make(chan error)
	go func() {
		_, err := mc.readResultOK()
		ch <- err
	}()
	select {
	case <-ctx.Done():
		mc.netConn.SetReadDeadline(time.Now())
		return ctx.Err()
	case err := <-ch:
		return err
	}
}
