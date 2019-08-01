package mysql

import "database/sql/driver"

type Decimal string

func (d Decimal) Value() (driver.Value, error) {
	return d, nil
}
