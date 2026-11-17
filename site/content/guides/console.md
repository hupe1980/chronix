+++
title = "Use the console"
description = "The page built into chronixd: run PromQL and SQL, see forecasts and anomalies on the chart, inspect cardinality and explain a query."
weight = 30
aliases = ["/docs/console/"]
+++

`chronixd` serves a console at **`/console`**: a query editor, a chart, and the
views you need when something looks wrong. It is one static page compiled into the
binary — nothing to install, no state on the server.

```bash
chronixd --demo        # prints the console URL; open it
```

![The console: a week of solar power and a seasonal SARIMA forecast of the next two days, its interval drawn as a band clipped at zero and the model named above the chart](../../img/console-forecast.webp)

## What it shows

| View | What it is for |
|---|---|
| **Data** (left) | Every measurement with its series count, fields and tags. Click a field to chart it; click a tag to filter by it. |
| **Editor** | PromQL or SQL, with completion for metric names, labels, label values, functions, measurements and columns. |
| **Chart** | The result over the chosen range. Drag across it to zoom; click a legend entry to hide a series. |
| **Table** | The same result as rows, newest first, 500 to a page. SQL shows column types. |
| **Explain** | PromQL: each node of the query with the series and samples it produced. SQL: the plan with its execution metrics. |
| **Cardinality** | Series used against the budget, and the measurements and labels that account for them. |

{{ <screenshot src="img/console-cardinality.webp" alt="The console’s Cardinality tab: four series against a budget of one million, with series by measurement, values by label and series by label pair" caption="**Cardinality** — the budget, and what is using it." /> }}

{{ <screenshot src="img/console-explain.webp" alt="The console’s Explain tab for sum by (device) (rate(grid_import_w[5m])): the sum, rate and range-selector nodes, each with its series and sample counts" caption="**Explain** — each PromQL node with the series and samples it produced." /> }}

## Forecast and anomalies

For a query that selects one series — `pv_power_w{device="pv"}`, not a function of
it — two switches draw the analytics onto the chart:

- **Forecast** continues the line as a dashed median inside its prediction interval.
  The model is chosen by cross-validation on the visible range at the chart's step —
  at 7d and 15m, 672 points with the daily season in view. The chip names the model,
  the interval level and what it was fitted on. A series never negative in view is
  clamped at zero, and the chip says "clipped at 0".
- **Anomalies** shades each anomalous window and rings its strongest point. The chip
  names the method and the threshold.

Both use the same `forecast` and `detect_anomalies` tools that [AI agents](@/guides/ai-agents.md)
call, so they need the MCP server (on by default). Without it the switches say so.

{{ <screenshot src="img/console-anomalies.webp" alt="The console charting a week of heat pump power with the anomaly switch on: one spike to about 2 700 W is ringed and a chip reads 1 anomaly, z-score above 3" caption="**Anomalies** on the demo: the planted heat-pump spike, found in the engine and ringed." /> }}

## When a query goes wrong

- **A typo** in a metric, table, column or function: the error marks the place in
  the query and offers the closest name. **Apply** fixes it and runs again.
- **An empty result** explains itself: each label matcher with the number of series
  left after it, the one that removed the last series highlighted, and the values
  that label does have. Click a value to use it.

{{ <screenshot src="img/console-hint.webp" alt="The console showing the error Unknown function with name rat, the typo underlined in the query and a Did you mean rate? suggestion with an Apply button" caption="A typo, marked in place, with the fix one click away." /> }}

{{ <screenshot src="img/console-empty.webp" alt="The console explaining an empty result: 1 series of pv_power_w, then 0 after device equals inverter-9, and the values device does have: pv" caption="*Why is this empty?* — the matcher that removed the last series, and the values that label has." /> }}

## Share and reuse

- **The address is the view.** Query, range, step, tab and switches are all in the
  URL fragment; copy it to share exactly what you see. Back and forward step
  through your queries.
- **Copy as code** turns the last query into a Rust program against the embedded
  API, a Python script using `chronix-client`, or the `curl` request the page sent.

## Keyboard

| Key | Action |
|---|---|
| <kbd>Ctrl</kbd>/<kbd>⌘</kbd> <kbd>Enter</kbd> | Run (in PromQL, <kbd>Enter</kbd> alone runs; <kbd>Shift</kbd> <kbd>Enter</kbd> adds a line) |
| <kbd>/</kbd> | Focus the editor |
| <kbd>Esc</kbd> | Close suggestions, or cancel a running query |
| <kbd>[</kbd> <kbd>]</kbd> | Shift the range back or forward by its width |
| <kbd>1</kbd>–<kbd>4</kbd> | Chart, Table, Explain, Cardinality |
| <kbd>?</kbd> | All shortcuts |

## Authentication and privacy

The page itself is the same for everyone and contains no data, so it loads without
a key. Every request it makes is authenticated and authorised like any other client's.
With authentication on, enter the key under the key icon; it stays in that tab's
memory and is never stored, logged or put in the URL. The only thing the console
stores is your theme choice.

The page runs under a strict Content Security Policy: no inline script, no third-party
requests, no framing.

## Turning it off

```toml
[console]
enabled = false
```

Or build without it: `cargo install chronixd --no-default-features --features mcp`.
