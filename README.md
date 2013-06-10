# Go-MySQL-Driver

A MySQL-Driver for Go's [database/sql](http://golang.org/pkg/database/sql) package

![Go-MySQL-Driver logo](https://raw.github.com/wiki/go-sql-driver/mysql/gomysql_m.png "Golang Gopher holding the MySQL Dolphin")

**Current tagged Release:** May 14, 2013 (Version 1.0)

[![Build Status](https://travis-ci.org/go-sql-driver/mysql.png?branch=master)](https://travis-ci.org/go-sql-driver/mysql) *(master branch)*

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
    * [LOAD DATA LOCAL INFILE support](#load-data-local-infile-support)
    * [time.Time support](#timetime-support)
    * [Unicode support](#unicode-support)
  * [Testing / Development](#testing--development)
  * [License](#license)

---------------------------------------

## Features
  * Lightweight and [fast](https://github.com/go-sql-driver/sql-benchmark "golang MySQL-Driver performance")
  * Native Go implementation. No C-bindings, just pure Go
  * Connections over TCP/IPv4, TCP/IPv6 or Unix domain sockets
  * Automatic handling of broken connections
  * Automatic Connection Pooling *(by database/sql package)*
  * Supports queries larger than 16MB
  * Full [`sql.RawBytes`](http://golang.org/pkg/database/sql/#RawBytes) support.
  * Intelligent `LONG DATA` handling in prepared statements
  * Secure `LOAD DATA LOCAL INFILE` support with file Whitelisting and `io.Reader` support
  * Optional `time.Time` parsing

## Requirements
  * Go 1.1 or higher (use [v1.0](https://github.com/go-sql-driver/mysql/tags) for Go 1.0.x)
  * MySQL (Version 4.1 or higher), MariaDB or Percona Server

---------------------------------------

## Installation
Simple install the package to your [$GOPATH](http://code.google.com/p/go-wiki/wiki/GOPATH "GOPATH") with the [go tool](http://golang.org/cmd/go/ "go command") from shell:
```bash
$ go get github.com/go-sql-driver/mysql
```
Make sure [Git is installed](http://git-scm.com/downloads) on your machine and in your system's `PATH`.

## Usage
_Go MySQL Driver_ is an implementation of Go's `database/sql/driver` interface. You only need to import the driver and can use the full [`database/sql`](http://golang.org/pkg/database/sql) API then.

Use `mysql` as `driverName` and a valid [DSN](#dsn-data-source-name)  as `dataSourceName`:
```go
import "database/sql"
import _ "github.com/go-sql-driver/mysql"

db, e := sql.Open("mysql", "user:password@/dbname?charset=utf8")
```

[Examples are available in our Wiki](https://github.com/go-sql-driver/mysql/wiki/Examples "Go-MySQL-Driver Examples").


### DSN (Data Source Name)

The Data Source Name has a common format, like e.g. [PEAR DB](http://pear.php.net/manual/en/package.database.db.intro-dsn.php) uses it, but without type-prefix (optional parts marked by squared brackets):
```
[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
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

#### Password
Passwords can consist of any character. Escaping is **not** necessary.

#### Protocol
See [net.Dial](http://golang.org/pkg/net/#Dial) for more information which networks are available.
In general you should use an Unix domain socket if available and TCP otherwise for best performance.

#### Address
For TCP and UDP networks, addresses have the form `host:port`.
If `host` is a literal IPv6 address, it must be enclosed in square brackets.
The functions [net.JoinHostPort](http://golang.org/pkg/net/#JoinHostPort) and [net.SplitHostPort](http://golang.org/pkg/net/#SplitHostPort) manipulate addresses in this form.

For Unix domain sockets the address is the absolute path to the MySQL-Server-socket, e.g. `/var/run/mysqld/mysqld.sock` or `/tmp/mysql.sock`.

#### Parameters
***Parameters are case-sensitive!***

Possible Parameters are:
  * `allowAllFiles`: `allowAllFiles=true` disables the file Whitelist for `LOAD DATA LOCAL INFILE` and allows *all* files. *Might be insecure!*
  * `charset`: Sets the charset used for client-server interaction ("SET NAMES `value`"). If multiple charsets are set (separated by a comma), the following charset is used if setting the charset failes. This enables support for `utf8mb4` ([introduced in MySQL 5.5.3](http://dev.mysql.com/doc/refman/5.5/en/charset-unicode-utf8mb4.html)) with fallback to `utf8` for older servers (`charset=utf8mb4,utf8`).
  * `clientFoundRows`: `clientFoundRows=true` causes an UPDATE to return the number of matching rows instead of the number of rows changed.
  * `loc`: Sets the location for time.Time values (when using `parseTime=true`). The default is `UTC`. *"Local"* sets the system's location. See [time.LoadLocation](http://golang.org/pkg/time/#LoadLocation) for details.
  * `parseTime`: `parseTime=true` changes the output type of `DATE` and `DATETIME` values to `time.Time` instead of `[]byte` / `string`
  * `strict`: Enable strict mode. MySQL warnings are treated as errors.
  * `timeout`: **Driver** side connection timeout. The value must be a string of decimal numbers, each with optional fraction and a unit suffix ( *"ms"*, *"s"*, *"m"*, *"h"* ), such as *"30s"*, *"0.5m"* or *"1m30s"*. To set a server side timeout, use the parameter [`wait_timeout`](http://dev.mysql.com/doc/refman/5.6/en/server-system-variables.html#sysvar_wait_timeout).
  * `tls`: `true` enables TLS / SSL encrypted connection to the server. Use `skip-verify` if you want to use a self-signed or invalid certificate (server side)

All other parameters are interpreted as system variables:
  * `autocommit`: *"SET autocommit=`value`"*
  * `time_zone`: *"SET time_zone=`value`"*
  * `tx_isolation`: *"SET [tx_isolation](https://dev.mysql.com/doc/refman/5.5/en/server-system-variables.html#sysvar_tx_isolation)=`value`"*
  * `param`: *"SET `param`=`value`"*

#### Examples
```
user@unix(/path/to/socket)/dbname
```

```
user:password@tcp(localhost:5555)/dbname?autocommit=true
```

```
user:password@tcp([de:ad:be:ef::ca:fe]:80)/dbname?tls=skip-verify&charset=utf8mb4,utf8
```

```
user:password@/dbname
```

No Database preselected:
```
user:password@/
```

### `LOAD DATA LOCAL INFILE` support
For this feature you need direct access to the package. Therefore you must change the import path (no `_`):
```go
import "github.com/go-sql-driver/mysql"
```

Files must be whitelisted by registering them with `mysql.RegisterLocalFile(filepath)` (recommended) or the Whitelist check must be deactivated by using the DSN parameter `allowAllFiles=true` (might be insecure).

To use a `io.Reader` a handler function must be registered with `mysql.RegisterReaderHandler(name, handler)` which returns a `io.Reader` or `io.ReadCloser`. The Reader is available with the filepath `Reader::<name>` then.

See also the [godoc of Go-MySQL-Driver](http://godoc.org/github.com/go-sql-driver/mysql "golang mysql driver documentation")


### `time.Time` support
The default internal output type of MySQL `DATE` and `DATETIME` values is `[]byte` which allows you to scan the value into a `[]byte`, `string` or `sql.RawBytes` variable in your programm.

However, many want to scan MySQL `DATE` and `DATETIME` values into `time.Time` variables, which is the logical opposite in Go to `DATE` and `DATETIME` in MySQL. You can do that by changing the internal output type from `[]byte` to `time.Time` with the DSN parameter `parseTime=true`. You can set the default [`time.Time` location](http://golang.org/pkg/time/#Location) with the `loc` DSN parameter.

**Caution:** As of Go 1.1, this makes `time.Time` the only variable type you can scan `DATE` and `DATETIME` values into. This breaks for example [`sql.RawBytes` support](https://github.com/go-sql-driver/mysql/wiki/Examples#rawbytes).

Alternatively you can use the [`NullTime`](http://godoc.org/github.com/go-sql-driver/mysql#NullTime) type as the scan destination, which works with both `time.Time` and `string` / `[]byte`.


### Unicode support
Since version 1.1 Go-MySQL-Driver automatically uses the collation `utf8_general_ci` by default. Adding `&charset=utf8` (alias for `SET NAMES utf8`) to the DSN is not necessary anymore in most cases.

See http://dev.mysql.com/doc/refman/5.7/en/charset-unicode.html for more details on MySQL's Unicode support.


## Testing / Development
To run the driver tests you may need to adjust the configuration. See the [Testing Wiki-Page](https://github.com/go-sql-driver/mysql/wiki/Testing "Testing") for details.

Go-MySQL-Driver is not feature-complete yet. Your help is very appreciated.
If you want to contribute, you can work on an [open issue](https://github.com/go-sql-driver/mysql/issues?state=open) or review a [pull request](https://github.com/go-sql-driver/mysql/pulls).

Code changes must be proposed via a Pull Request and must be reviewed. Only *LGTM*-ed (" *Looks good to me* ") code may be committed to the master branch.

---------------------------------------

## License
Go-MySQL-Driver is licensed under the [Mozilla Public License Version 2.0](https://raw.github.com/go-sql-driver/mysql/master/LICENSE)

Mozilla summarizes the license scope as follows:
> MPL: The copyleft applies to any files containing MPLed code.


That means:
  * You can **use** the **unchanged** source code both in private as also commercial
  * You **needn't publish** the source code of your library as long the files licensed under the MPL 2.0 are **unchanged**
  * You **must publish** the source code of any **changed files** licensed under the MPL 2.0 under a) the MPL 2.0 itself or b) a compatible license (e.g. GPL 3.0 or Apache License 2.0)

Please read the [MPL 2.0 FAQ](http://www.mozilla.org/MPL/2.0/FAQ.html) if you have further questions regarding the license.

You can read the full terms here: [LICENSE](https://raw.github.com/go-sql-driver/mysql/master/LICENSE)

![Go Gopher and MySQL Dolphin](https://raw.github.com/wiki/go-sql-driver/mysql/go-mysql-driver_m.jpg "Golang Gopher transporting the MySQL Dolphin in a wheelbarrow")

