package mysql

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestConnectorReturnsTimeout(t *testing.T) {
	connector := &connector{&Config{
		Net:     "tcp",
		Addr:    "1.1.1.1:1234",
		Timeout: 10 * time.Millisecond,
	}}

	_, err := connector.Connect(context.Background())
	if err == nil {
		t.Fatal("error expected")
	}

	if nerr, ok := err.(*net.OpError); ok {
		expected := "dial tcp 1.1.1.1:1234: i/o timeout"
		if nerr.Error() != expected {
			t.Fatalf("expected %q, got %q", expected, nerr.Error())
		}
	} else {
		t.Fatalf("expected %T, got %T", nerr, err)
	}
}
