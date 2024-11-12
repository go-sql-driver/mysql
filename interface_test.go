package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

var _ MySQLConn = &mysqlConn{}

func ExampleMySQLConn() {
	db, _ := sql.Open("mysql", "root:pw@unix(/tmp/mysql.sock)/myDatabase?parseTime=true&loc=Europe%2FAmsterdam")
	conn, _ := db.Conn(context.Background())
	var location *time.Location
	conn.Raw(func(dc any) error {
		mc := dc.(MySQLConn)
		location = mc.Location()
		return nil
	})
	fmt.Println(location)
}
