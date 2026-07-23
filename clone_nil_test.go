package mysql

import "testing"

func TestConfigCloneNil(t *testing.T) {
	var c *Config
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Clone panicked: %v", r)
		}
	}()
	if c.Clone() != nil {
		t.Fatal("want nil")
	}
}
