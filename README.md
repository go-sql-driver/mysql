# Go-MySQL-Driver

A MySQL-Driver for Go's [database/sql](http://golang.org/pkg/database/sql) package

![Go-MySQL-Driver logo](https://raw.github.com/wiki/Go-SQL-Driver/MySQL/go-mysql-driver_m.jpg "Golang Gopher transporting the MySQL Dolphin in a wheelbarrow")

**Current tagged Release:** January 08, 2013 *(beta)*

[![Build Status](https://travis-ci.org/Go-SQL-Driver/MySQL.png?branch=master)](https://travis-ci.org/Go-SQL-Driver/MySQL) *(master branch)*

Note: `go get` doesn't install the master branch, but the tag `go1`, which is always checked before tagging!

---------------------------------------
  * [Features](#features)
  * [Requirements](#requirements)
  * [Installation](#installation)
  * [Usage](#usage)
  * [DSN (Data Source Name)](#dsn-data-source-name)
    * [Password](#password)
    * [Protocol](#protocol)
    * [Address](#address)
    * [Parameters](#parameters)
    * [Examples](#examples)
  * [License](#license)

---------------------------------------

## Features
  * Lightweight and fast
  * Native Go implementation. No C-bindings, just pure Go
  * No unsafe operations *(type-conversions etc.)*

## Requirements
  * Go 1 or higher (Go 1.0.3 or higher recommended)
  * MySQL (Version 4.1 or higher), MariaDB or Percona Server

---------------------------------------

## Installation
Simple install the package with the go tool from shell:
```bash
$ go get github.com/Go-SQL-Driver/MySQL
```
Make sure [Git is installed](http://git-scm.com/downloads) on your machine and in your system's `PATH`.

## Usage
_Go MySQL Driver_ is an implementation of Go's `database/sql/driver` interface, so all you need to do is import the driver and open a new Database-Connection with the given driver.

Use `"mysql"` as `driverName` and a valid [DSN](#dsn-data-source-name)  as `dataSourceName`
```go
import "database/sql"
import _ "code.google.com/p/go-mysql-driver/mysql"

db, e := sql.Open("mysql", "user:password@/dbname?charset=utf8")
```

All further methods are listed here: http://golang.org/pkg/database/sql


## DSN (Data Source Name)

The Data Source Name has a common format, like e.g. [PEAR DB](http://pear.php.net/manual/en/package.database.db.intro-dsn.php) uses it, but without type-prefix:
```
[username[:password]@][protocol[(address)]]/dbname[?param1=value1&paramN=valueN]
```

A DSN in its fullest form:
```
username:password@protocol(address)/dbname?param=value
```

Except of the databasename, all values are optional. So the minimal DSN is:
```
/dbname
```

If you do not want to preselect a database, leave `dbname` empty:
```
/
```

### Password
Passwords can consist of any character. Escaping is not necessary.

### Protocol
See [net.Dial](http://golang.org/pkg/net/#Dial) for more information which networks are available.
In general you should use an Unix-socket if available and TCP otherwise for best performance.

### Address
For TCP and UDP networks, addresses have the form `host:port`.
If `host` is a literal IPv6 address, it must be enclosed in square brackets.
The functions [net.JoinHostPort](http://golang.org/pkg/net/#JoinHostPort) and [net.SplitHostPort](http://golang.org/pkg/net/#SplitHostPort) manipulate addresses in this form.

For Unix-sockets the address is the absolute path to the MySQL-Server-socket, e.g. `/var/run/mysqld/mysqld.sock` or `/tmp/mysql.sock`.

### Parameters
**Parameters are case-sensitive!**

Possible Parameters are:
  * `charset`: *"SET NAMES `value`"*
  * _(deprecated)_ <s>`keepalive`: If `value` equals 1, the keepalive-time is set to [wait_timeout](https://dev.mysql.com/doc/refman/5.5/en/server-system-variables.html#sysvar_wait_timeout)-60, which pings the Server 60 seconds before the MySQL server would close the connection to avoid timeout. If the value is greater than 1, the server gets pinged every `value` seconds without a command. System variables are executed **before**, so it may be possible to change the *wait_timeout* value.</s> **With Go 1.0.3 this is not necessary anymore. Now closed connections can be automatically detected and handled.**
  * _(pending)_ <s>`tls`</s>: will enable SSL/TLS-Encryption 
  * _(pending)_ <s>`compress`</s>: will enable Compression 

All other parameters are interpreted as system variables:
  * `time_zone`: *"SET time_zone='`value`'"*
  * `tx_isolation`: *"SET [tx_isolation](https://dev.mysql.com/doc/refman/5.5/en/server-system-variables.html#sysvar_tx_isolation)='`value`'"*
  * `param`: *"SET `param`=`value`"*

### Examples
```
user@unix(/path/to/socket)/dbname?charset=utf8
```

```
user:password@tcp(localhost:5555)/dbname?charset=utf8
```

```
user:password@tcp([de:ad:be:ef::ca:fe]:80)/dbname
```

```
user:password@/dbname
```

No Database preselected:
```
user:password@/
```

---------------------------------------

## License
Go-MySQL-Driver is licensed under the [Mozilla Public License Version 2.0](https://raw.github.com/Go-SQL-Driver/MySQL/master/LICENSE)

Mozilla summarizes the license scope as follows:
> MPL: The copyleft applies to any files containing MPLed code.


That means:
  * You can **use** the **unchanged** source code both in private as also commercial
  * You **needn't publish** the source code of your library as long the files licensed under the MPL 2.0 are **unchanged**
  * You **must publish** the source code of any **changed files** licensed under the MPL 2.0 under a) the MPL 2.0 itself or b) a compatible license (e.g. GPL 3.0 or Apache License 2.0)

Please read the [MPL 2.0 FAQ](http://www.mozilla.org/MPL/2.0/FAQ.html) if you have further questions regarding the license. 

You can read the full terms here: [LICENSE](https://raw.github.com/Go-SQL-Driver/MySQL/master/LICENSE)
