+++
title = "Use Chronix from an AI agent"
description = "Connect Claude Code, Claude Desktop or any MCP client to Chronix over HTTP or stdio, and let an agent query, forecast and detect anomalies."
weight = 40
aliases = ["/docs/ai-agents/"]
+++

`chronixd` is an [MCP](https://modelcontextprotocol.io) server. An agent connected
to it can find the metrics, query them with PromQL or SQL, see why a query came
back empty, and **forecast and detect anomalies inside the database** — no data
leaves it to be analysed elsewhere. Every tool only reads.

## Connect

A running server serves MCP at `/mcp`, on the same port as everything else:

```bash
claude mcp add --transport http chronix http://127.0.0.1:8086/mcp
```

With authentication on, send the same key as for any request:

```bash
claude mcp add --transport http chronix https://chronix.example.com/mcp \
  --header "Authorization: Bearer $CHRONIX_API_KEY"
```

A data directory — one copied off a device, say — needs no server. `chronixd mcp`
opens it read-only and speaks MCP on stdin and stdout:

```bash
claude mcp add gateway-0042 -- chronixd mcp --data-dir ./gateway-0042
```

For Claude Desktop, or any client configured with JSON:

```json
{
  "mcpServers": {
    "gateway-0042": {
      "command": "chronixd",
      "args": ["mcp", "--data-dir", "/path/to/gateway-0042"]
    }
  }
}
```

## Tools

| Tool | What it does |
|---|---|
| `list_metrics` | The metrics, with type and the measurement and field each one reads |
| `label_values` | The values of one label, optionally for one metric |
| `query_promql` | A PromQL query at one time or over a range; an empty result explains itself |
| `query_sql` | A read-only SQL query, with the analytics functions |
| `explain` | How a PromQL or SQL query runs; with `analyze`, what every PromQL step produced |
| `cardinality` | Series counts by metric and label, and the budget left |
| `forecast` | A forecast of one series with a prediction interval, model chosen by cross-validation |
| `detect_anomalies` | The anomalous points of one series, strongest first |

Times are `"now"`, `"now-7d"`, RFC 3339 or Unix seconds. `forecast` and
`detect_anomalies` work on one series: when a metric has several, the error lists
them and the agent adds `labels`. For a window longer than a day, `forecast` takes a
`step` (`"15m"`): it fits on the series averaged into buckets that wide, so a week
is 672 points with the daily season in view, and `horizon` counts steps. `min`
(`0` for power) clamps the forecast and its interval at a floor the quantity cannot
go below, and the answer reports it as `clamped_below`.

## Try it on the demo

```bash
chronixd --demo
claude mcp add --transport http chronix http://127.0.0.1:8086/mcp
claude "Which device in chronix had anomalies this week, and when?"
```

The agent lists the metrics, runs `detect_anomalies` on each power series and
names the stuck heat-pump defrost the demo plants.

## What an agent can reach

- **The same as its key.** `/mcp` goes through the same authentication, namespace
  and authorization checks as `/api/v1/query`; every tool is a read. A key confined
  to one namespace sees only that namespace's series.
- **Only the tools you offer.** `[mcp] tools` lists them; a tool left out is neither
  listed nor callable. `enabled = false` turns `/mcp` off.
- **Bounded answers.** A result is cut at `[mcp] max_result_bytes` (256 KiB) and
  the server's row and series limits, and says it was cut.
- **Nothing written.** `chronixd mcp --data-dir` creates, locks and changes nothing
  in the directory, and refuses one a running process holds — connect to that
  server's `/mcp` instead.

Without authentication, `/mcp` accepts only requests addressed to `localhost`,
`127.0.0.1` or `::1`, so a web page cannot reach a local server through DNS
rebinding. Behind a proxy, set `[mcp] allowed_hosts` to the names it serves.
See [configuration](@/reference/configuration.md#mcp).
