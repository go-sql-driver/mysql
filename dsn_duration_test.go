package mysql

import "testing"

func TestParseDSNDurationTrimAndNonNeg(t *testing.T) {
	cfg, err := ParseDSN("user:pass@tcp(localhost:3306)/db?timeout= 5s &readTimeout=1s")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Timeout.Seconds() != 5 {
		t.Fatalf("timeout=%v", cfg.Timeout)
	}
	_, err = ParseDSN("user:pass@tcp(localhost:3306)/db?timeout=-1s")
	if err == nil {
		t.Fatal("expected error for negative timeout")
	}
}
