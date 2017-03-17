package mysql

import "testing"
import "bytes"

func TestAuthPlugin_Cleartext(t *testing.T) {
	cfg := &Config{
		Passwd: "funny",
	}

	plugin := authPluginFactories[mysqlClearPassword](cfg)

	_, err := plugin.Next(nil)
	if err == nil {
		t.Fatalf("expected error when AllowCleartextPasswords is false")
	}

	cfg.AllowCleartextPasswords = true

	actual, err := plugin.Next(nil)
	if err != nil {
		t.Fatalf("expected no error but got: %s", err)
	}

	expected := append([]byte("funny"), 0)
	if bytes.Compare(actual, expected) != 0 {
		t.Fatalf("expected data to be %v, but got: %v", expected, actual)
	}
}

func TestAuthPlugin_NativePassword(t *testing.T) {
	cfg := &Config{
		Passwd: "pass ",
	}

	plugin := authPluginFactories[mysqlNativePassword](cfg)

	actual, err := plugin.Next([]byte{9, 8, 7, 6, 5, 4, 3, 2})
	if err != nil {
		t.Fatalf("expected no error but got: %s", err)
	}

	expected := []byte{195, 146, 3, 213, 111, 95, 252, 192, 97, 226, 173, 176, 91, 175, 131, 138, 89, 45, 75, 179}
	if bytes.Compare(actual, expected) != 0 {
		t.Fatalf("expected data to be %v, but got: %v", expected, actual)
	}
}

func TestAuthPlugin_OldPassword(t *testing.T) {
	cfg := &Config{
		Passwd: "pass ",
	}

	plugin := authPluginFactories[mysqlOldPassword](cfg)

	_, err := plugin.Next(nil)
	if err == nil {
		t.Fatalf("expected error when AllowOldPasswords is false")
	}

	cfg.AllowOldPasswords = true

	actual, err := plugin.Next([]byte{9, 8, 7, 6, 5, 4, 3, 2})
	if err != nil {
		t.Fatalf("expected no error but got: %s", err)
	}

	expected := []byte{71, 87, 92, 90, 67, 91, 66, 81, 0}
	if bytes.Compare(actual, expected) != 0 {
		t.Fatalf("expected data to be %v, but got: %v", expected, actual)
	}
}
