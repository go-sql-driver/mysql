package mysql

import "testing"

type customString string

func TestConvertValueCustomTypes(t *testing.T) {
	var cstr customString = "string"
	c := converter{}
	if _, err := c.ConvertValue(cstr); err != nil {
		t.Errorf("custom string type should be valid")
	}
}
