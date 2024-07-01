package mysql

import (
	"time"
)

// MySQLConn exposes the usable methods on driverConn given to database/sql.Conn.Raw.
type MySQLConn interface {
	// Prevent other modules from implementing this interface so we can keep adding methods.
	isMySQLConn()

	// Location gets the Config.Loc of this connection. (This may differ from `time_zone` connection variable.)
	Location() *time.Location
}

func (mc *mysqlConn) isMySQLConn() {
}

func (mc *mysqlConn) Location() *time.Location {
	return mc.cfg.Loc
}
