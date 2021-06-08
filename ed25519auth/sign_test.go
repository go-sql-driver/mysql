package ed25519auth

import "testing"

// https://github.com/MariaDB/server/blob/c0ac0b8/plugin/auth_ed25519/ed25519-t.c
func TestSign(t *testing.T) {
	challenge := [32]byte{
		'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A',
		'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A',
		'A', 'A', 'A', 'A', 'A', 'A',
	}
	password := []byte("foobar")
	expectedSignature := [64]byte{
		232, 61, 201, 63, 67, 63, 51, 53, 86, 73, 238, 35, 170, 117, 146,
		214, 26, 17, 35, 9, 8, 132, 245, 141, 48, 99, 66, 58, 36, 228, 48,
		84, 115, 254, 187, 168, 88, 162, 249, 57, 35, 85, 79, 238, 167, 106,
		68, 117, 56, 135, 171, 47, 20, 14, 133, 79, 15, 229, 124, 160, 176,
		100, 138, 14,
	}

	signature := Sign(challenge, password)

	if signature != expectedSignature {
		t.Fatal("Signature did not match expected value")
	}
}
