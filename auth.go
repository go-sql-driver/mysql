package mysql

import "bytes"

const (
	mysqlClearPassword    = "mysql_clear_password"
	mysqlNativePassword   = "mysql_native_password"
	mysqlOldPassword      = "mysql_old_password"
	defaultAuthPluginName = mysqlNativePassword
)

var authPluginFactories map[string]func(*Config) AuthPlugin

func init() {
	authPluginFactories = make(map[string]func(*Config) AuthPlugin)
	authPluginFactories[mysqlClearPassword] = func(cfg *Config) AuthPlugin {
		return &clearTextPlugin{cfg}
	}
	authPluginFactories[mysqlNativePassword] = func(cfg *Config) AuthPlugin {
		return &nativePasswordPlugin{cfg}
	}
	authPluginFactories[mysqlOldPassword] = func(cfg *Config) AuthPlugin {
		return &oldPasswordPlugin{cfg}
	}
}

// RegisterAuthPlugin registers an authentication plugin to be used during
// negotiation with the server. If a plugin with the given name already exists,
// it will be overwritten.
func RegisterAuthPlugin(name string, factory func(*Config) AuthPlugin) {
	authPluginFactories[name] = factory
}

// AuthPlugin handles authenticating a user.
type AuthPlugin interface {
	// Next takes a server's challenge and returns
	// the bytes to send back or an error.
	Next(challenge []byte) ([]byte, error)
}

type clearTextPlugin struct {
	cfg *Config
}

func (p *clearTextPlugin) Next(challenge []byte) ([]byte, error) {
	if !p.cfg.AllowCleartextPasswords {
		return nil, ErrCleartextPassword
	}

	// NUL-terminated
	return append([]byte(p.cfg.Passwd), 0), nil
}

type nativePasswordPlugin struct {
	cfg *Config
}

func (p *nativePasswordPlugin) Next(challenge []byte) ([]byte, error) {
	// NOTE: this seems to always be disabled...
	// if !p.cfg.AllowNativePasswords {
	// 	return nil, ErrNativePassword
	// }

	return scramblePassword(challenge, []byte(p.cfg.Passwd)), nil
}

type oldPasswordPlugin struct {
	cfg *Config
}

func (p *oldPasswordPlugin) Next(challenge []byte) ([]byte, error) {
	if !p.cfg.AllowOldPasswords {
		return nil, ErrOldPassword
	}

	// NUL-terminated
	return append(scrambleOldPassword(challenge, []byte(p.cfg.Passwd)), 0), nil
}

func handleAuthResult(mc *mysqlConn, plugin AuthPlugin, oldCipher []byte) error {
	data, err := mc.readPacket()
	if err != nil {
		return err
	}

	var authData []byte

	// packet indicator
	switch data[0] {
	case iOK:
		return mc.handleOkPacket(data)

	case iEOF: // auth switch
		if len(data) > 1 {
			pluginEndIndex := bytes.IndexByte(data, 0x00)
			pluginName := string(data[1:pluginEndIndex])
			if apf, ok := authPluginFactories[pluginName]; ok {
				plugin = apf(mc.cfg)
			} else {
				return ErrUnknownPlugin
			}

			if len(data) > pluginEndIndex+1 {
				authData = data[pluginEndIndex+1 : len(data)-1]
			}
		} else {
			// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::OldAuthSwitchRequest
			plugin = authPluginFactories[mysqlOldPassword](mc.cfg)
			authData = oldCipher
		}
	case iAuthContinue:
		// continue packet for a plugin.
		authData = data[1:] // strip off the continue flag
	default: // Error otherwise
		return mc.handleErrorPacket(data)
	}

	authData, err = plugin.Next(authData)
	if err != nil {
		return err
	}

	err = mc.writeAuthDataPacket(authData)
	if err != nil {
		return err
	}

	return handleAuthResult(mc, plugin, authData)
}
