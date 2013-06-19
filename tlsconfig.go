package mysql

import (
	"crypto/tls"
)

type TLSConfig interface {
	SetTLSConfig(key string, config *tls.Config)
}

var tlsConfigMap = make(map[string]*tls.Config)

func (d *mysqlDriver) SetTLSConfig(key string, config *tls.Config) {
	tlsConfigMap[key] = config
}
