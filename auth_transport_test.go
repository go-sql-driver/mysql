package mysql

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type authTestPlugin struct {
	init func(context.Context, []byte, *AuthContext) ([]byte, error)
	next func(context.Context, []byte, []byte, *AuthContext) ([]byte, error)
}

func (p *authTestPlugin) InitAuth(ctx context.Context, seed []byte, auth *AuthContext) ([]byte, error) {
	return p.init(ctx, seed, auth)
}

func (p *authTestPlugin) ContinuationAuth(ctx context.Context, packet, seed []byte, auth *AuthContext) ([]byte, error) {
	return p.next(ctx, packet, seed, auth)
}

func registerTestAuthPlugin(t *testing.T, name string, factory func() AuthPlugin) {
	t.Helper()
	globalPluginRegistry.mu.RLock()
	previous, existed := globalPluginRegistry.plugins[name]
	globalPluginRegistry.mu.RUnlock()
	RegisterAuthPlugin(name, factory)
	t.Cleanup(func() {
		globalPluginRegistry.mu.Lock()
		if existed {
			globalPluginRegistry.plugins[name] = previous
		} else {
			delete(globalPluginRegistry.plugins, name)
		}
		globalPluginRegistry.mu.Unlock()
	})
}

func authTestHandshake(plugin string, capabilities capabilityFlag) []byte {
	data := append([]byte{10}, "8.0.0\x00"...)
	data = binary.LittleEndian.AppendUint32(data, 1)
	data = append(data, "01234567\x00"...)
	data = binary.LittleEndian.AppendUint16(data, uint16(capabilities))
	data = append(data, defaultCollationID, 2, 0)
	data = binary.LittleEndian.AppendUint16(data, uint16(capabilities>>16))
	data = append(data, 21)
	data = append(data, make([]byte, 10)...)
	data = append(data, "89abcdefghij\x00"...)
	data = append(data, plugin...)
	data = append(data, 0)
	return makePacket(0, data)
}

func readAuthTestPacket(conn net.Conn, sequence byte) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, err
	}
	if header[3] != sequence {
		return nil, fmt.Errorf("sequence = %d, want %d", header[3], sequence)
	}
	size := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	data := make([]byte, size)
	_, err := io.ReadFull(conn, data)
	return data, err
}

func authTestCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func TestConnectorAuthTransport(t *testing.T) {
	certificate := authTestCertificate(t)
	for _, tc := range []struct {
		name          string
		serverTLS     bool
		clientTLS     bool
		fallback      bool
		untrustedCert bool
	}{
		{name: "plain"},
		{name: "TLS available but disabled", serverTLS: true},
		{name: "TLS", serverTLS: true, clientTLS: true},
		{name: "plaintext fallback", clientTLS: true, fallback: true},
		{name: "TLS required", clientTLS: true},
		{name: "TLS verification fails", serverTLS: true, clientTLS: true, untrustedCert: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useTLS := tc.serverTLS && tc.clientTLS
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			deadline, _ := ctx.Deadline()
			if err := server.SetDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			const pluginName = "test_auth_transport"
			var verified atomic.Bool
			var initial *AuthContext
			continued := false
			registerTestAuthPlugin(t, pluginName, func() AuthPlugin {
				return &authTestPlugin{
					init: func(gotCtx context.Context, seed []byte, auth *AuthContext) ([]byte, error) {
						initial = auth
						if gotCtx != ctx || auth.User() != "user" || auth.Password() != "password" || auth.UnixSocket() {
							return nil, errors.New("incorrect authentication context")
						}
						if auth.TLS() != useTLS || verified.Load() != useTLS {
							return nil, errors.New("InitAuth called before TLS was established")
						}
						if string(seed) != "0123456789abcdefghij" {
							return nil, errors.New("incorrect initial challenge")
						}
						return []byte{0xaa}, nil
					},
					next: func(gotCtx context.Context, packet, seed []byte, auth *AuthContext) ([]byte, error) {
						continued = true
						if gotCtx != ctx || auth != initial || !bytes.Equal(packet, []byte{7}) || string(seed) != "0123456789abcdefghij" {
							return nil, errors.New("continuation did not retain its context and challenge")
						}
						return []byte{}, nil // Send an empty packet rather than just reading.
					},
				}
			})

			serverResult := make(chan error, 1)
			go func() {
				defer server.Close()
				serverResult <- func() error {
					caps := clientMySQL | clientProtocol41 | clientSecureConn | clientPluginAuth | clientPluginAuthLenEncClientData
					if tc.serverTLS {
						caps |= clientSSL
					}
					if _, err := server.Write(authTestHandshake(pluginName, caps)); err != nil {
						return err
					}
					if tc.clientTLS && !tc.serverTLS && !tc.fallback {
						if _, err := readAuthTestPacket(server, 1); err == nil {
							return errors.New("received authentication despite missing TLS support")
						}
						return nil
					}
					var conn net.Conn = server
					seq := byte(1)
					var sslRequest []byte
					if useTLS {
						var err error
						sslRequest, err = readAuthTestPacket(server, seq)
						if err != nil {
							return err
						}
						if len(sslRequest) != 32 || capabilityFlag(binary.LittleEndian.Uint32(sslRequest))&clientSSL == 0 {
							return errors.New("invalid SSLRequest header")
						}
						tlsConn := tls.Server(server, &tls.Config{Certificates: []tls.Certificate{certificate}})
						err = tlsConn.HandshakeContext(ctx)
						if tc.untrustedCert {
							if err == nil {
								return errors.New("untrusted TLS handshake succeeded")
							}
							return nil
						}
						if err != nil {
							return err
						}
						conn = tlsConn
						seq++
					}
					response, err := readAuthTestPacket(conn, seq)
					if err != nil {
						return err
					}
					if len(response) < 32 || !bytes.HasPrefix(response[32:], []byte("user\x00\x01\xaa")) {
						return errors.New("invalid HandshakeResponse authentication payload")
					}
					if sslRequest != nil && !bytes.Equal(response[:32], sslRequest) {
						return errors.New("SSLRequest and HandshakeResponse headers differ")
					}
					if !useTLS && capabilityFlag(binary.LittleEndian.Uint32(response))&clientSSL != 0 {
						return errors.New("plaintext response advertised TLS")
					}
					if _, err := conn.Write(makePacket(seq+1, []byte{iAuthMoreData, 7})); err != nil {
						return err
					}
					response, err = readAuthTestPacket(conn, seq+2)
					if err != nil {
						return err
					}
					if len(response) != 0 {
						return errors.New("expected an empty continuation response")
					}
					if _, err := conn.Write(makePacket(seq+3, []byte{0, 0, 0, 2, 0, 0, 0})); err != nil {
						return err
					}
					_, err = readAuthTestPacket(conn, 0) // COM_QUIT
					return err
				}()
			}()

			cfg := NewConfig()
			cfg.User, cfg.Passwd = "user", "password"
			cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) { return client, nil }
			if tc.clientTLS {
				cfg.TLS = &tls.Config{
					ServerName:         "localhost",
					InsecureSkipVerify: !tc.untrustedCert,
					VerifyConnection: func(tls.ConnectionState) error {
						verified.Store(true)
						return nil
					},
				}
			}
			cfg.AllowFallbackToPlaintext = tc.fallback
			c, err := NewConnector(cfg)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := c.Connect(ctx)
			if tc.untrustedCert {
				var certErr *tls.CertificateVerificationError
				if !errors.As(err, &certErr) || initial != nil {
					t.Fatalf("Connect error = %v; plugin started = %v", err, initial != nil)
				}
			} else if tc.clientTLS && !tc.serverTLS && !tc.fallback {
				if err != ErrNoTLS || initial != nil {
					t.Fatalf("Connect error = %v; plugin started = %v", err, initial != nil)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err := conn.Close(); err != nil {
					t.Fatal(err)
				}
				if initial == nil || !continued {
					t.Fatal("authentication exchange was not completed")
				}
			}
			if tc.clientTLS && c.(*connector).cfg.TLS == nil {
				t.Error("Connect changed the connector's TLS configuration")
			}
			if err := <-serverResult; err != nil {
				t.Fatal(err)
			}
		})
	}
}
