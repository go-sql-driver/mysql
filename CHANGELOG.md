## 1.1 (pending)

Changes:

  - Go-MySQL-Driver now requires Go 1.1
  - Connections now use the collation `utf8_general_ci` by default. Adding `&charset=UTF8` to the DSN should not be necessary anymore
  - Made closing rows and connections error tolerant. This allows for example deferring rows.Close() without checking for errors
  - `byte(nil)` is now treated as a NULL value. Before it was treated like an empty string / `[]byte("")`.
  - New Logo
  - Changed the copyright header to include all contributors
  - Optimized the buffer for reading
  - Use the buffer also for writing. This results in zero allocations (by the driver) for most queries
  - stmt.Query now caches column metadata
  - Improved the LOAD INFILE documentation
  - The driver struct is now exported to make the driver directly accessible
  - Refactored the driver tests
  - Added more benchmarks and moved all to a separate file
  - Other small refactoring
  - DSN parameter values must now be url.QueryEscape'ed. This allows text values to contain special characters, such as '&'.

New Features:

  - Added *old_passwords* support: Required in some cases, but must be enabled by adding `allowOldPasswords=true` to the DSN since it is insecure
  - Added a `clientFoundRows` parameter: Return the number of matching rows instead of the number of rows changed on UPDATEs
  - Added TLS/SSL support: Use a TLS/SSL encrypted connection to the server. Custom TLS configs can be registered and used

Bugfixes:

  - Fixed MySQL 4.1 support: MySQL 4.1 sends packets with lengths which differ from the specification
  - Convert to DB timezone when inserting time.Time
  - Splitted packets (more than 16MB) are now merged correctly


## 1.0 (2013-05-14)

Initial Release
