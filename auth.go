// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2018 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.
//
// +build !windows

package mysql

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/openshift/gssapi"
)

// server pub keys registry
var (
	serverPubKeyLock     sync.RWMutex
	serverPubKeyRegistry map[string]*rsa.PublicKey
)

// RegisterServerPubKey registers a server RSA public key which can be used to
// send data in a secure manner to the server without receiving the public key
// in a potentially insecure way from the server first.
// Registered keys can afterwards be used adding serverPubKey=<name> to the DSN.
//
// Note: The provided rsa.PublicKey instance is exclusively owned by the driver
// after registering it and may not be modified.
//
//  data, err := ioutil.ReadFile("mykey.pem")
//  if err != nil {
//  	log.Fatal(err)
//  }
//
//  block, _ := pem.Decode(data)
//  if block == nil || block.Type != "PUBLIC KEY" {
//  	log.Fatal("failed to decode PEM block containing public key")
//  }
//
//  pub, err := x509.ParsePKIXPublicKey(block.Bytes)
//  if err != nil {
//  	log.Fatal(err)
//  }
//
//  if rsaPubKey, ok := pub.(*rsa.PublicKey); ok {
//  	mysql.RegisterServerPubKey("mykey", rsaPubKey)
//  } else {
//  	log.Fatal("not a RSA public key")
//  }
//
func RegisterServerPubKey(name string, pubKey *rsa.PublicKey) {
	serverPubKeyLock.Lock()
	if serverPubKeyRegistry == nil {
		serverPubKeyRegistry = make(map[string]*rsa.PublicKey)
	}

	serverPubKeyRegistry[name] = pubKey
	serverPubKeyLock.Unlock()
}

// DeregisterServerPubKey removes the public key registered with the given name.
func DeregisterServerPubKey(name string) {
	serverPubKeyLock.Lock()
	if serverPubKeyRegistry != nil {
		delete(serverPubKeyRegistry, name)
	}
	serverPubKeyLock.Unlock()
}

func getServerPubKey(name string) (pubKey *rsa.PublicKey) {
	serverPubKeyLock.RLock()
	if v, ok := serverPubKeyRegistry[name]; ok {
		pubKey = v
	}
	serverPubKeyLock.RUnlock()
	return
}

// Hash password using pre 4.1 (old password) method
// https://github.com/atcurtis/mariadb/blob/master/mysys/my_rnd.c
type myRnd struct {
	seed1, seed2 uint32
}

const myRndMaxVal = 0x3FFFFFFF

// Pseudo random number generator
func newMyRnd(seed1, seed2 uint32) *myRnd {
	return &myRnd{
		seed1: seed1 % myRndMaxVal,
		seed2: seed2 % myRndMaxVal,
	}
}

// Tested to be equivalent to MariaDB's floating point variant
// http://play.golang.org/p/QHvhd4qved
// http://play.golang.org/p/RG0q4ElWDx
func (r *myRnd) NextByte() byte {
	r.seed1 = (r.seed1*3 + r.seed2) % myRndMaxVal
	r.seed2 = (r.seed1 + r.seed2 + 33) % myRndMaxVal

	return byte(uint64(r.seed1) * 31 / myRndMaxVal)
}

// Generate binary hash from byte string using insecure pre 4.1 method
func pwHash(password []byte) (result [2]uint32) {
	var add uint32 = 7
	var tmp uint32

	result[0] = 1345345333
	result[1] = 0x12345671

	for _, c := range password {
		// skip spaces and tabs in password
		if c == ' ' || c == '\t' {
			continue
		}

		tmp = uint32(c)
		result[0] ^= (((result[0] & 63) + add) * tmp) + (result[0] << 8)
		result[1] += (result[1] << 8) ^ result[0]
		add += tmp
	}

	// Remove sign bit (1<<31)-1)
	result[0] &= 0x7FFFFFFF
	result[1] &= 0x7FFFFFFF

	return
}

// Hash password using insecure pre 4.1 method
func scrambleOldPassword(scramble []byte, password string) []byte {
	scramble = scramble[:8]

	hashPw := pwHash([]byte(password))
	hashSc := pwHash(scramble)

	r := newMyRnd(hashPw[0]^hashSc[0], hashPw[1]^hashSc[1])

	var out [8]byte
	for i := range out {
		out[i] = r.NextByte() + 64
	}

	mask := r.NextByte()
	for i := range out {
		out[i] ^= mask
	}

	return out[:]
}

// Hash password using 4.1+ method (SHA1)
func scramblePassword(scramble []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write([]byte(password))
	stage1 := crypt.Sum(nil)

	// scrambleHash = SHA1(scramble + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1)
	hash := crypt.Sum(nil)

	// outer Hash
	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(hash)
	scramble = crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}

// Hash password using MySQL 8+ method (SHA256)
func scrambleSHA256Password(scramble []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}

	// XOR(SHA256(password), SHA256(SHA256(SHA256(password)), scramble))

	crypt := sha256.New()
	crypt.Write([]byte(password))
	message1 := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1)
	message1Hash := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1Hash)
	crypt.Write(scramble)
	message2 := crypt.Sum(nil)

	for i := range message1 {
		message1[i] ^= message2[i]
	}

	return message1
}

func encryptPassword(password string, seed []byte, pub *rsa.PublicKey) ([]byte, error) {
	plain := make([]byte, len(password)+1)
	copy(plain, password)
	for i := range plain {
		j := i % len(seed)
		plain[i] ^= seed[j]
	}
	sha1 := sha1.New()
	return rsa.EncryptOAEP(sha1, rand.Reader, pub, plain, nil)
}

func (mc *mysqlConn) sendEncryptedPassword(seed []byte, pub *rsa.PublicKey) error {
	enc, err := encryptPassword(mc.cfg.Passwd, seed, pub)
	if err != nil {
		return err
	}
	return mc.writeAuthSwitchPacket(enc)
}

// Parse KRB5 authentication data.
//
// Get the SPN and REALM from the authentication data packet.
//
// Format:
//		SPN string length two bytes <B1> <B2> +
//		SPN string +
//		UPN realm string length two bytes <B1> <B2> +
//		UPN realm string
//
//Returns:
//		'spn' and 'realm'
func krb5ParseAuthData(authData []byte) (string, string) {
	buf := bytes.NewBuffer(authData[:2])
	spnLen := int16(0)
	binary.Read(buf, binary.LittleEndian, &spnLen)
	packet := authData[2:]
	spn := string(packet[:spnLen])
	// next realm
	packet = packet[spnLen:]
	buf = bytes.NewBuffer(packet[:2])
	realmLen := int16(0)
	binary.Read(buf, binary.LittleEndian, &realmLen)
	packet = packet[2:]
	realm := string(packet[:realmLen])
	// remove realm from SPN
	spn = strings.TrimSuffix(spn, "@"+realm)
	return spn, realm
}

func (mc *mysqlConn) auth(authData []byte, plugin string) ([]byte, error) {
	l := log.New(os.Stderr, "GOKRB5 Client: ", log.LstdFlags)
	l.Printf("auth plugin: %s", plugin)
	switch plugin {
	case "caching_sha2_password":
		authResp := scrambleSHA256Password(authData, mc.cfg.Passwd)
		return authResp, nil

	case "mysql_old_password":
		if !mc.cfg.AllowOldPasswords {
			return nil, ErrOldPassword
		}
		if len(mc.cfg.Passwd) == 0 {
			return nil, nil
		}
		// Note: there are edge cases where this should work but doesn't;
		// this is currently "wontfix":
		// https://github.com/go-sql-driver/mysql/issues/184
		authResp := append(scrambleOldPassword(authData[:8], mc.cfg.Passwd), 0)
		return authResp, nil

	case "mysql_clear_password":
		if !mc.cfg.AllowCleartextPasswords {
			return nil, ErrCleartextPassword
		}
		// http://dev.mysql.com/doc/refman/5.7/en/cleartext-authentication-plugin.html
		// http://dev.mysql.com/doc/refman/5.7/en/pam-authentication-plugin.html
		return append([]byte(mc.cfg.Passwd), 0), nil

	case "mysql_native_password":
		if !mc.cfg.AllowNativePasswords {
			return nil, ErrNativePassword
		}
		// https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
		// Native password authentication only need and will need 20-byte challenge.
		authResp := scramblePassword(authData[:20], mc.cfg.Passwd)
		return authResp, nil

	case "sha256_password":
		if len(mc.cfg.Passwd) == 0 {
			return []byte{0}, nil
		}
		if mc.cfg.tls != nil || mc.cfg.Net == "unix" {
			// write cleartext auth packet
			return append([]byte(mc.cfg.Passwd), 0), nil
		}

		pubKey := mc.cfg.pubKey
		if pubKey == nil {
			// request public key from server
			return []byte{1}, nil
		}

		// encrypted password
		enc, err := encryptPassword(mc.cfg.Passwd, authData, pubKey)
		return enc, err

	case "authentication_kerberos_client":
		krb5ConfigFilename := os.Getenv("KRB5_CONFIG")
		// try common location for config
		if krb5ConfigFilename == "" {
			krb5ConfigFilename = "/etc/krb5.conf"
		}
		krb5Config, err := ioutil.ReadFile(krb5ConfigFilename)
		if err != nil {
			log.Fatalf("could not read krb5.conf (%s): %v", krb5ConfigFilename, err)
		}
		log.Printf("%v", authData)
		// decode the SPN from authData
		spn, realm := krb5ParseAuthData(authData)
		log.Printf("SPN: %s", spn)
		log.Printf("Realm: %s", realm)
		conf, err := config.NewFromString(string(krb5Config))
		if err != nil {
			log.Fatalf("could not load krb5.conf: %v", err)
		}

		// load keytab from file
		keytabFilename := os.Getenv("KRB5_CLIENT_KTNAME")
		if keytabFilename == "" {
			// try default
			keytabFilename = conf.LibDefaults.DefaultClientKeytabName
		}
		log.Printf("Using keytab %s", keytabFilename)

		var cl *client.Client

		krb5keytabContent, err := ioutil.ReadFile(keytabFilename)
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}

		if os.IsNotExist(err) {
			ccacheFileDest, ok := os.LookupEnv("KRB5CCNAME")
			if !ok {
				// KRB5CCNAME is not set, return the error from loading the Keytab.
				return nil, err
			}

			ccache, err := credentials.LoadCCache(ccacheFileDest)
			if err != nil {
				return nil, err
			}

			cl, err = client.NewFromCCache(ccache, conf, client.Logger(l), client.DisablePAFXFAST(true), client.AssumePreAuthentication(false))
			if err != nil {
				return nil, err
			}
		} else {
			log.Printf("krb5 user: %s", mc.cfg.User)
			log.Printf("krb5 default realm: %s", conf.LibDefaults.DefaultRealm)
			log.Printf("krb5 kdc: %v", conf.Realms[0].KDC)

			kt := keytab.New()
			err = kt.Unmarshal(krb5keytabContent)
			if err != nil {
				return nil, err
			}
			cl = client.NewWithKeytab(mc.cfg.User, conf.LibDefaults.DefaultRealm, kt, conf, client.Logger(l), client.DisablePAFXFAST(true), client.AssumePreAuthentication(false))
		}

		// Log in the client
		err = cl.Login()
		if err != nil {
			log.Fatalf("could not login client: %v", err)
		}

		_, _, err = cl.GetServiceTicket(spn)
		if err != nil {
			log.Printf("failed to get service ticket: %v\n", err)
		}
		log.Println("ok got service ticket...")

		dl, err := gssapi.Load(nil)
		if err != nil {
			return nil, err
		}

		buf_name, err := dl.MakeBufferBytes([]byte(spn))
		if err != nil {
			return nil, err
		}
		name, err := buf_name.Name(dl.GSS_KRB5_NT_PRINCIPAL_NAME)
		input_buf, _ := dl.MakeBuffer(0)
		if err != nil {
			return nil, err
		}
		cname, _ := name.Canonicalize(dl.GSS_MECH_KRB5)
		//
		// TODO: need to implement mutual authentication to ensure both sides agree?
		//reqFlags := gssapi.GSS_C_DELEG_FLAG + gssapi.GSS_C_MUTUAL_FLAG
		//
		// allow delegation
		//
		reqFlags := gssapi.GSS_C_DELEG_FLAG

		//reqFlags = 0
		_, _, token, _, _, err := dl.InitSecContext(
			dl.GSS_C_NO_CREDENTIAL,
			nil,
			cname,
			dl.GSS_C_NO_OID,
			reqFlags,
			0,
			dl.GSS_C_NO_CHANNEL_BINDINGS,
			input_buf)

		if token == nil {
			return nil, err
		}
		log.Println("gssapi security context created")

		return token.Bytes(), err

	default:
		errLog.Print("unknown auth plugin:", plugin)
		return nil, ErrUnknownPlugin
	}
}

func (mc *mysqlConn) handleAuthResult(oldAuthData []byte, plugin string) error {
	// Read Result Packet
	authData, newPlugin, err := mc.readAuthResult()
	if err != nil {
		return err
	}

	// handle auth plugin switch, if requested
	if newPlugin != "" {
		// If CLIENT_PLUGIN_AUTH capability is not supported, no new cipher is
		// sent and we have to keep using the cipher sent in the init packet.
		if authData == nil {
			authData = oldAuthData
		} else {
			// copy data from read buffer to owned slice
			copy(oldAuthData, authData)
		}

		plugin = newPlugin

		authResp, err := mc.auth(authData, plugin)
		if err != nil {
			return err
		}
		if err = mc.writeAuthSwitchPacket(authResp); err != nil {
			return err
		}

		// Read Result Packet
		authData, newPlugin, err = mc.readAuthResult()
		if err != nil {
			return err
		}

		// Do not allow to change the auth plugin more than once
		if newPlugin != "" {
			return ErrMalformPkt
		}
	}

	switch plugin {

	// https://insidemysql.com/preparing-your-community-connector-for-mysql-8-part-2-sha256/
	case "caching_sha2_password":
		switch len(authData) {
		case 0:
			return nil // auth successful
		case 1:
			switch authData[0] {
			case cachingSha2PasswordFastAuthSuccess:
				if err = mc.readResultOK(); err == nil {
					return nil // auth successful
				}

			case cachingSha2PasswordPerformFullAuthentication:
				if mc.cfg.tls != nil || mc.cfg.Net == "unix" {
					// write cleartext auth packet
					err = mc.writeAuthSwitchPacket(append([]byte(mc.cfg.Passwd), 0))
					if err != nil {
						return err
					}
				} else {
					pubKey := mc.cfg.pubKey
					if pubKey == nil {
						// request public key from server
						data, err := mc.buf.takeSmallBuffer(4 + 1)
						if err != nil {
							return err
						}
						data[4] = cachingSha2PasswordRequestPublicKey
						mc.writePacket(data)

						// parse public key
						if data, err = mc.readPacket(); err != nil {
							return err
						}

						block, rest := pem.Decode(data[1:])
						if block == nil {
							return fmt.Errorf("No Pem data found, data: %s", rest)
						}
						pkix, err := x509.ParsePKIXPublicKey(block.Bytes)
						if err != nil {
							return err
						}
						pubKey = pkix.(*rsa.PublicKey)
					}

					// send encrypted password
					err = mc.sendEncryptedPassword(oldAuthData, pubKey)
					if err != nil {
						return err
					}
				}
				return mc.readResultOK()

			default:
				return ErrMalformPkt
			}
		default:
			return ErrMalformPkt
		}

	case "sha256_password":
		switch len(authData) {
		case 0:
			return nil // auth successful
		default:
			block, _ := pem.Decode(authData)
			pub, err := x509.ParsePKIXPublicKey(block.Bytes)
			if err != nil {
				return err
			}

			// send encrypted password
			err = mc.sendEncryptedPassword(oldAuthData, pub.(*rsa.PublicKey))
			if err != nil {
				return err
			}
			return mc.readResultOK()
		}

	default:
		return nil // auth successful
	}

	return err
}
