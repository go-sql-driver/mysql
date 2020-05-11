package mysql

import "database/sql"
import _ "github.com/go-sql-driver/mysql"

func Fuzz(data []byte) int {
	db, err := sql.Open("mysql", string(data))
	if err != nil {
		return 0
	}
	defer db.Close()
	return 1
}
