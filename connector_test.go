package mysql

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestConnectorReturnsTimeout(t *testing.T) {
	connector := newConnector(&Config{
		Net:     "tcp",
		Addr:    "1.1.1.1:1234",
		Timeout: 10 * time.Millisecond,
	})

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

func TestBeforeConnectUsesEffectiveTimeout(t *testing.T) {
	dialErr := errors.New("stop after observing dial context")
	var remaining time.Duration

	cfg := NewConfig()
	cfg.Timeout = 2 * time.Hour
	cfg.DialFunc = func(ctx context.Context, _, _ string) (net.Conn, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("dial context has no deadline")
		}
		remaining = time.Until(deadline)
		return nil, dialErr
	}
	if err := cfg.Apply(BeforeConnect(func(_ context.Context, cfg *Config) error {
		cfg.Timeout = time.Hour
		return nil
	})); err != nil {
		t.Fatal(err)
	}

	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connector.Connect(context.Background()); !errors.Is(err, dialErr) {
		t.Fatalf("Connect() error = %v, want %v", err, dialErr)
	}
	if remaining < 59*time.Minute || remaining > 61*time.Minute {
		t.Fatalf("dial timeout = %v, want about 1h", remaining)
	}
}

func TestBeforeConnectUsesEffectiveDialerAndAttributes(t *testing.T) {
	serverHandshake := []byte(
		"\x48\x00\x00\x00" + // Packet header: 72-byte payload, sequence 0.
			"\x0a" + // Protocol version 10.
			"5.5.8\x00" + // NUL-terminated server version.
			"\xa5\x00\x00\x00" + // Connection ID 165.
			"<F?:Dh\"a" + // First 8 bytes of the authentication scramble.
			"\x00" + // Filler.
			"\xdf\xf7" + // Lower 2 bytes of the server capability flags.
			"\x21" + // utf8_general_ci character set.
			"\x02\x00" + // SERVER_STATUS_AUTOCOMMIT.
			"\x1f\x80" + // Upper 2 bytes of the server capability flags.
			"\x15" + // Authentication plugin data length: 21 bytes.
			"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" + // Reserved.
			"bxr/UKmc3M2@\x00" + // Remaining authentication scramble.
			"mysql_native_password", // Authentication plugin name.
	)
	okPacket := []byte(
		"\x07\x00\x00\x02" + // Packet header: 7-byte payload, sequence 2.
			"\x00" + // OK packet header.
			"\x00\x00" + // Zero affected rows and last insert ID.
			"\x02\x00" + // SERVER_STATUS_AUTOCOMMIT.
			"\x00\x00", // Zero warnings.
	)
	mock := &mockConn{
		data:          serverHandshake,
		queuedReplies: [][]byte{okPacket},
	}

	var (
		initialDialCalled bool
		dialNetwork       string
		dialAddress       string
	)
	cfg := NewConfig()
	cfg.Addr = "initial.example:3306"
	cfg.ConnectionAttributes = "phase:initial"
	cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		initialDialCalled = true
		return nil, errors.New("initial dialer must not be called")
	}
	if err := cfg.Apply(BeforeConnect(func(_ context.Context, cfg *Config) error {
		cfg.Addr = "callback.example:3306"
		cfg.ConnectionAttributes = "phase:callback"
		cfg.DialFunc = func(_ context.Context, network, address string) (net.Conn, error) {
			dialNetwork = network
			dialAddress = address
			return mock, nil
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}

	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if initialDialCalled {
		t.Fatal("Connect() used the pre-callback DialFunc")
	}
	if dialNetwork != "tcp" || dialAddress != "callback.example:3306" {
		t.Fatalf("dialed %s(%s), want tcp(callback.example:3306)", dialNetwork, dialAddress)
	}
	if !bytes.Contains(mock.written, []byte("phase\bcallback")) {
		t.Fatalf("handshake response does not contain callback attributes: %q", mock.written)
	}
	if bytes.Contains(mock.written, []byte("phase\x07initial")) {
		t.Fatalf("handshake response contains stale attributes: %q", mock.written)
	}
	if !bytes.Contains(mock.written, []byte("callback.example")) {
		t.Fatalf("handshake response does not contain callback server host: %q", mock.written)
	}
}
