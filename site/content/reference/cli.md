+++
title = "chronixd command line"
description = "Every chronixd flag, environment variable and subcommand: running the server, the demo, checking a config, inspect and the MCP stdio server."
weight = 60
+++

`chronixd` runs the server. With a subcommand it does one job and exits
instead. Precedence for every setting: flag, then environment variable, then
the config file, then the built-in default.

```text
chronixd [FLAGS]                 run the server
chronixd --demo                  run on generated data, loopback only
chronixd inspect <DIR> [FLAGS]   report what a data directory holds
chronixd mcp --data-dir <DIR>    serve MCP over stdio from a directory
```

## Server flags

| Flag | Environment | Meaning |
|---|---|---|
| `-c, --config PATH` | `CHRONIXD_CONFIG` | TOML configuration file ([Configuration](@/reference/configuration.md)) |
| `--data-dir PATH` | `CHRONIXD_DATA_DIR` | Data directory |
| `--bind ADDR` | `CHRONIXD_BIND` | HTTP address (default `0.0.0.0:8086`) |
| `--grpc-bind ADDR` | `CHRONIXD_GRPC_BIND` | gRPC address (default `0.0.0.0:8087`) |
| `--flight-bind ADDR` | `CHRONIXD_FLIGHT_BIND` | Flight SQL address (default `0.0.0.0:8817`) |
| `--tls-cert PATH` | `CHRONIXD_TLS_CERT` | TLS certificate; needs `--tls-key` |
| `--tls-key PATH` | `CHRONIXD_TLS_KEY` | TLS private key |
| `--tls-client-ca PATH` | `CHRONIXD_TLS_CLIENT_CA` | Require client certificates signed by this CA (mTLS) |
| `--tls-reload-interval-secs N` | `CHRONIXD_TLS_RELOAD_INTERVAL` | Reload the certificate and key every N seconds; `0` never. Default: the config file's `[tls] reload_interval_secs`, else 60 |
| `--log-format text\|json\|compact` | `CHRONIXD_LOG_FORMAT` | Log format; `text` by default, `json` for a log pipeline |
| `--log-level FILTER` | `CHRONIXD_LOG_LEVEL` | Log level filter, e.g. `info` or `chronixd=debug` |
| `--export-dashboards DIR` | — | Write the bundled Grafana dashboards to `DIR` and exit |
| `--check-config` | — | Validate the configuration, print what it resolves to, and exit without opening the database or binding a port |
| `--demo` | `CHRONIXD_DEMO` | Serve a generated week of gateway data from a temporary directory, on loopback, without authentication; deleted on exit |

`--demo` refuses `--data-dir` and `--config`. Pass `--bind 127.0.0.1:0` to pick
a free port; the banner prints the one it bound.

## `chronixd inspect`

Reports what a data directory holds without opening it: nothing is written,
locked or replayed, so it is safe on a directory a running server holds.

| Argument | Meaning |
|---|---|
| `DIR` | The data directory |
| `--json` | One JSON document (schema `chronix.inspect/1`) instead of the report |
| `--fast` | Skip the series sidecars: faster, without series counts |
| `-c, --config PATH` | Take the cardinality budget from this server configuration |

Exit status `0` when a report was printed, problems included; `1` when the
directory is not a Chronix data directory or the configuration cannot be read.
See [Operate a server](@/guides/operate.md) for reading the report.

## `chronixd mcp`

Serves the MCP tools over stdio from a data directory opened read-only — for a
directory copied off a device. A directory a running process holds is refused;
connect to that server's `/mcp` endpoint instead.

| Argument | Meaning |
|---|---|
| `--data-dir DIR` | The data directory |
| `-c, --config PATH` | Database settings the directory needs, such as field-encryption keys; listen addresses are ignored |

See [Use Chronix from an AI agent](@/guides/ai-agents.md).

## Cluster binary

Cluster mode is a separate, unpublished binary with extra role flags; see
[Cluster (frozen)](@/reference/cluster.md).
