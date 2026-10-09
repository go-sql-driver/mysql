package zstd

import (
	"bytes"
	"crypto/rand"
	"os/exec"
	"strings"
	"testing"
)

func TestCodecRoundtrip(t *testing.T) {
	random := make([]byte, 32768)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"small", []byte("hello world")},
		{"compressible", bytes.Repeat([]byte("mysql row contents"), 65536)},
		{"incompressible", random},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := codec{}
			encoded := c.Encode(tc.data, nil)
			decoded, err := c.Decode(encoded, make([]byte, 0, len(tc.data)))
			if err != nil || !bytes.Equal(decoded, tc.data) {
				t.Fatalf("roundtrip error = %v, output length = %d", err, len(decoded))
			}
		})
	}
}

func TestDriverDependencyBoundary(t *testing.T) {
	dependencies, err := exec.Command("go", "list", "-deps", "github.com/go-sql-driver/mysql").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, dependencies)
	}
	if strings.Contains(string(dependencies), "github.com/klauspost/compress") {
		t.Fatal("importing the driver alone must not build klauspost/compress")
	}
}

func TestCodecRejectsInvalidInput(t *testing.T) {
	c := codec{}
	data := bytes.Repeat([]byte("mysql"), 10000)
	encoded := c.Encode(data, nil)
	for _, tc := range []struct {
		name string
		src  []byte
		size int
	}{
		{"too much output", encoded, len(data) - 1},
		{"truncated frame", encoded[:len(encoded)-1], len(data)},
		{"invalid frame", []byte("invalid zstd frame"), len(data)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := c.Decode(tc.src, make([]byte, 0, tc.size)); err == nil {
				t.Fatal("invalid input was accepted")
			}
			// A decoder returned to the pool after failure must remain reusable.
			decoded, err := c.Decode(encoded, make([]byte, 0, len(data)))
			if err != nil || !bytes.Equal(decoded, data) {
				t.Fatalf("reuse after failure: %v", err)
			}
		})
	}
}
