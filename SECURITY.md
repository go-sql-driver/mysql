# Security Policy

## Threat Model

go-sql-driver/mysql is designed to connect to trusted MySQL-compatible database servers.

The driver does not aim to protect applications against malicious or compromised database servers. Malformed or malicious responses from such servers are outside our security threat model.

**Only connect to database servers you trust.**

Issues that can only be exploited by a malicious or compromised database server are not considered security vulnerabilities. However, we may still address them as regular bugs.

## Reporting Security Vulnerabilities

If you believe you have found a security vulnerability within this threat model, please report it privately using [GitHub Security Advisories](https://github.com/go-sql-driver/mysql/security/advisories/new).

Please do not disclose security vulnerabilities publicly before the maintainers have had an opportunity to investigate them.
