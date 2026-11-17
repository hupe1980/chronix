// Chronix console — one module, no framework, no build step.
//
// Rendering goes through `el()`, which only ever sets text: no HTML string is
// parsed anywhere in this file (`scripts/check-console.sh` holds that). The
// whole view lives in the URL fragment; the API key lives in a variable.

"use strict";

/* ── Small helpers ────────────────────────────────────────────────────── */

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

/** Create an element. Attribute keys: `class`, `data-*`, `aria-*`, `on:<event>`,
 *  and DOM properties (`value`, `checked`, `disabled`, `hidden`, `title`…).
 *  Children are nodes or strings (always text). */
function el(tag, attrs, ...kids) {
  const node = tag === "svg" || tag === "path" || tag === "circle" || tag === "rect"
    ? document.createElementNS("http://www.w3.org/2000/svg", tag)
    : document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === undefined || v === null || v === false) continue;
    if (k.startsWith("on:")) node.addEventListener(k.slice(3), v);
    else if (k === "class") node.setAttribute("class", v);
    else if (k.startsWith("data-") || k.startsWith("aria-") || k === "role" || k === "viewBox" || k === "d"
      || k === "cx" || k === "cy" || k === "r" || k === "x" || k === "y" || k === "width" || k === "height"
      || k === "type" || k === "for" || k === "tabindex") node.setAttribute(k, v === true ? "" : String(v));
    else node[k] = v;
  }
  append(node, kids);
  return node;
}
function append(node, ...kids) {
  for (const kid of kids.flat(Infinity)) {
    if (kid === null || kid === undefined || kid === false) continue;
    node.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
  }
  return node;
}
function clear(node) { node.replaceChildren(); return node; }
function icon(d, extra) {
  return el("svg", { viewBox: "0 0 24 24", "aria-hidden": "true", class: extra }, el("path", { d }));
}
const ICONS = {
  error: "M12 8v5m0 3.5h.01M10.3 3.9 2.6 17.2A2 2 0 0 0 4.3 20h15.4a2 2 0 0 0 1.7-2.8L13.7 3.9a2 2 0 0 0-3.4 0z",
  warn: "M12 8v5m0 3.5h.01M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18z",
  info: "M12 11v5m0-8.5h.01M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18z",
  chart: "M3 3v18h18M7 15l4-4 3 3 5-6",
  search: "M11 4a7 7 0 1 0 0 14 7 7 0 0 0 0-14zm9 16-3.5-3.5",
  chev: "m9 6 6 6-6 6",
  db: "M4 6c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3zm0 0v12c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3",
  table: "M3 5h18v14H3zM3 10h18M9 5v14",
};

const NNBSP = "\u202f";
const isMac = /Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent);

function fmtInt(n) { return Number(n).toLocaleString("en-US").replace(/,/g, NNBSP); }
function fmtNum(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return "–";
  const a = Math.abs(v);
  if (a !== 0 && (a >= 1e6 || a < 1e-3)) return v.toExponential(2);
  if (a >= 1e4) return fmtInt(Math.round(v));
  return String(Number(v.toPrecision(5)));
}
function fmtAxis(v) {
  if (v === null) return "";
  const a = Math.abs(v);
  if (a >= 1e9) return `${+(v / 1e9).toFixed(1)}G`;
  if (a >= 1e6) return `${+(v / 1e6).toFixed(1)}M`;
  if (a >= 1e4) return `${+(v / 1e3).toFixed(1)}k`;
  return String(+v.toFixed(a < 1 ? 3 : a < 10 ? 2 : a < 100 ? 1 : 0));
}
function fmtMs(ms) { return ms < 1000 ? `${Math.round(ms)}${NNBSP}ms` : `${(ms / 1000).toFixed(ms < 10000 ? 2 : 1)}${NNBSP}s`; }
const DUR_UNITS = [["d", 86400], ["h", 3600], ["m", 60], ["s", 1]];
function fmtDur(secs) {
  for (const [u, s] of DUR_UNITS) if (secs >= s && secs % s === 0) return `${secs / s}${u}`;
  return `${Math.round(secs)}s`;
}
function parseDur(text) {
  const m = /^(\d+(?:\.\d+)?)(ms|s|m|h|d|w)$/.exec(String(text).trim());
  if (!m) return null;
  return Number(m[1]) * { ms: 0.001, s: 1, m: 60, h: 3600, d: 86400, w: 604800 }[m[2]];
}
const dtShort = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
const pad = (n) => String(n).padStart(2, "0");
/** Local time as `2026-11-17 07:06:20`, the same in every locale. */
function fmtTime(secs) {
  const d = new Date(secs * 1000);
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}
function fmtHM(secs) { const d = new Date(secs * 1000); return `${pad(d.getHours())}:${pad(d.getMinutes())}`; }
const fmtShort = (secs) => dtShort.format(new Date(secs * 1000));
const iso = (secs) => new Date(Math.round(secs * 1000)).toISOString().replace(".000Z", "Z");

function css(name) { return getComputedStyle(document.documentElement).getPropertyValue(name).trim(); }
function alpha(color, a) {
  const hex = color.replace("#", "");
  if (hex.length !== 6) return color;
  const n = parseInt(hex, 16);
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${a})`;
}

/* ── State: the URL fragment is the whole view ────────────────────────── */

const PRESETS = { "15m": 900, "1h": 3600, "6h": 21600, "24h": 86400, "7d": 604800 };
const TABS = ["chart", "table", "explain", "cardinality"];

const state = {
  q: "", lang: "promql", range: "1h", from: null, to: null, step: "auto",
  tab: "chart", fc: false, an: false, ns: "",
};
let lastPreset = "1h";

function readFragment() {
  const p = new URLSearchParams(location.hash.slice(1));
  const notes = [];
  state.q = p.get("q") || "";
  state.lang = p.get("l") === "sql" ? "sql" : "promql";
  const r = p.get("r") || "1h";
  if (PRESETS[r]) { state.range = r; state.from = state.to = null; lastPreset = r; }
  else {
    const [a, b] = r.split("..");
    const from = Date.parse(a) / 1000, to = Date.parse(b) / 1000;
    if (Number.isFinite(from) && Number.isFinite(to) && to > from) { state.range = "custom"; state.from = from; state.to = to; }
    else { state.range = "1h"; state.from = state.to = null; notes.push(`The range "${r}" in the address is not a preset or "from..to"; showing the last hour.`); }
  }
  const s = p.get("s") || "auto";
  state.step = s === "auto" || parseDur(s) ? s : "auto";
  const t = p.get("t");
  state.tab = TABS.includes(t) ? t : "chart";
  state.fc = p.get("f") === "1";
  state.an = p.get("a") === "1";
  state.ns = p.get("ns") || session.ns || "";
  if (state.ns) session.ns = state.ns;
  return notes;
}
function fragment() {
  const p = new URLSearchParams();
  if (state.q) p.set("q", state.q);
  p.set("l", state.lang);
  p.set("r", state.range === "custom" ? `${iso(state.from)}..${iso(state.to)}` : state.range);
  if (state.step !== "auto") p.set("s", state.step);
  if (state.tab !== "chart") p.set("t", state.tab);
  if (state.fc) p.set("f", "1");
  if (state.an) p.set("a", "1");
  if (session.ns) p.set("ns", session.ns);
  return `#${p.toString().replace(/\+/g, "%20")}`;
}
function writeFragment(push) {
  const next = fragment();
  if (next === location.hash) return;
  if (push) history.pushState(null, "", next); else history.replaceState(null, "", next);
}

/** The window the next query covers, in seconds. */
function window_() {
  if (state.range === "custom") return { start: state.from, end: state.to };
  // Whole seconds: the range ends up in the URL and in copied code.
  const end = Math.floor(Date.now() / 1000);
  return { start: end - PRESETS[state.range], end };
}
const NICE = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 10800, 21600, 43200, 86400];
function stepFor(start, end) {
  if (state.step !== "auto") return parseDur(state.step);
  const width = $("#panel-chart").clientWidth || 900;
  const target = (end - start) / Math.max(240, Math.min(1400, width / 1.3));
  return NICE.find((n) => n >= target) || 86400;
}

/* ── Session and the API client ───────────────────────────────────────── */

const session = { key: "", ns: "" };
let inflight = null;

class ApiError extends Error {
  constructor(status, body, text) {
    super((body && (body.error || body.message)) || text || `HTTP ${status}`);
    this.status = status; this.body = body;
  }
}

async function api(path, { method = "GET", params, body, signal, accept } = {}) {
  const url = new URL(path, location.href);
  for (const [k, v] of Object.entries(params || {})) {
    if (Array.isArray(v)) v.forEach((x) => url.searchParams.append(k, x));
    else if (v !== undefined && v !== null) url.searchParams.set(k, v);
  }
  const headers = { accept: accept || "application/json" };
  if (session.key) headers.authorization = `Bearer ${session.key}`;
  if (session.ns) headers["x-namespace"] = session.ns;
  if (body !== undefined) headers["content-type"] = "application/json";
  const res = await fetch(url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal });
  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch { json = null; }
  if (res.status === 401) needKey();
  if (!res.ok && !(json && json.status === "error")) throw new ApiError(res.status, json, text);
  return json;
}

/** Call an MCP tool on `/mcp` — the route agents use, read-only, same gates. */
async function tool(name, args, signal) {
  const body = { jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: args } };
  const res = await api("mcp", { method: "POST", body, signal, accept: "application/json, text/event-stream" });
  if (!res || res.error) throw new Error((res && res.error && res.error.message) || "no answer from /mcp");
  const r = res.result || {};
  const text = (r.content || []).map((c) => c.text).filter(Boolean).join(" ");
  if (r.isError) throw new Error(text || "the tool refused");
  return { data: r.structuredContent || {}, summary: text };
}

/* ── Catalogue: measurements, metric names, labels ────────────────────── */

const catalogue = { measurements: [], counts: new Map(), metrics: [], labels: null, values: new Map() };

async function loadCatalogue() {
  const [m, tsdb, names] = await Promise.allSettled([
    api("api/v1/measurements"),
    api("api/v1/status/tsdb", { params: { limit: 10000 } }),
    api("api/v1/label/__name__/values"),
  ]);
  catalogue.measurements = (m.status === "fulfilled" && m.value && m.value.items ? m.value.items : [])
    .sort((a, b) => a.name.localeCompare(b.name));
  catalogue.counts = new Map();
  if (tsdb.status === "fulfilled" && tsdb.value && tsdb.value.data) {
    for (const { name, value } of tsdb.value.data.seriesCountByMetricName || []) catalogue.counts.set(name, value);
  }
  catalogue.metrics = (names.status === "fulfilled" && names.value && names.value.data ? names.value.data : []).sort();
  renderRail();
  if (!state.q) renderWelcome();
}

/** The PromQL metric a measurement's field is read as. */
function metricOf(measurement, field) { return field === "value" ? measurement : `${measurement}_${field}`; }

/* ── Rail ─────────────────────────────────────────────────────────────── */

const expanded = new Set();

function renderRail() {
  const list = clear($("#measurements"));
  const filter = $("#filter").value.trim().toLowerCase();
  const items = catalogue.measurements.filter((m) => !filter || m.name.toLowerCase().includes(filter)
    || m.columns.some((c) => c.name.toLowerCase().includes(filter)));
  $("#rail-count").textContent = catalogue.measurements.length ? fmtInt(catalogue.measurements.length) : "";
  if (!catalogue.measurements.length) {
    list.append(el("li", { class: "rail-empty" },
      el("p", {}, "No measurements yet."),
      el("p", {}, "Start with generated data:"),
      copyLine("chronixd --demo"),
      el("p", {}, "or write a point:"),
      copyLine(`curl ${baseUrl()}/write --data-binary 'cpu,host=a usage=42'`)));
    return;
  }
  if (!items.length) { list.append(el("li", { class: "rail-empty" }, `Nothing matches “${filter}”.`)); return; }
  for (const m of items) {
    const open = expanded.has(m.name) || (filter && items.length <= 3);
    const fields = m.columns.filter((c) => c.role === "field");
    const tags = m.columns.filter((c) => c.role === "tag");
    const count = catalogue.counts.get(m.name);
    const li = el("li", { "aria-expanded": open ? "true" : "false" },
      el("button", {
        type: "button", title: `${m.name}: ${fields.length} field${fields.length === 1 ? "" : "s"}, ${tags.length} tag${tags.length === 1 ? "" : "s"}`,
        "on:click": () => { if (expanded.has(m.name)) expanded.delete(m.name); else expanded.add(m.name); renderRail(); },
      }, icon(ICONS.chev, "chev"), el("span", { class: "m-name" }, m.name),
      count !== undefined ? el("span", { class: "count", title: `${count} series` }, fmtInt(count)) : null));
    if (open) {
      li.append(el("ul", {},
        fields.map((f) => el("li", {}, el("button", {
          type: "button", title: state.lang === "sql" ? `Select ${f.name} from ${m.name}` : `Chart ${metricOf(m.name, f.name)}`,
          "on:click": () => pickField(m, f),
        }, el("span", {}, f.name), el("span", { class: "kind" }, typeBadge(f.data_type))))),
        tags.map((t) => el("li", {}, el("button", {
          type: "button", title: `Filter by ${t.name}`, "on:click": () => pickTag(m, t),
        }, el("span", {}, t.name), el("span", { class: "kind tag" }, "tag"))))));
    }
    list.append(li);
  }
}
function typeBadge(t) {
  return ({ float64: "f64", int64: "i64", uint64: "u64", string: "str", boolean: "bool", decimal: "dec", histogram: "hist" })[t] || t;
}
function copyLine(text) {
  return el("button", {
    type: "button", class: "chip", title: "Copy", "on:click": () => copyText(text),
  }, text);
}
function pickField(m, f) {
  if (state.lang === "sql") {
    const tags = m.columns.filter((c) => c.role === "tag").map((c) => c.name);
    setQuery(`SELECT _time, ${[...tags, f.name].join(", ")}\nFROM ${m.name}\nORDER BY _time DESC\nLIMIT 100`);
  } else setQuery(metricOf(m.name, f.name));
  run(true);
}
function pickTag(m, t) {
  const ed = $("#editor");
  const sel = plainSelector(ed.value);
  if (state.lang === "promql" && sel) {
    const inner = sel.matchers ? `${sel.matchers.slice(1, -1).trim()}${sel.matchers.length > 2 ? ", " : ""}` : "";
    const text = `${sel.name}{${inner}${t.name}=""}`;
    setQuery(text);
    ed.focus(); ed.setSelectionRange(text.length - 2, text.length - 2);
    complete(true);
  } else insertAtCaret(t.name);
}

/* ── Editor ───────────────────────────────────────────────────────────── */

function setQuery(text) { const ed = $("#editor"); ed.value = text; autosize(); }
function autosize() { const ed = $("#editor"); ed.style.height = "auto"; ed.style.height = `${Math.min(ed.scrollHeight + 2, 260)}px`; }
function insertAtCaret(text) {
  const ed = $("#editor");
  ed.setRangeText(text, ed.selectionStart, ed.selectionEnd, "end");
  ed.focus(); autosize();
}
function setLang(lang) {
  if (state.lang === lang) return;
  state.lang = lang;
  $$('.editor-bar [data-lang]').forEach((b) => b.setAttribute("aria-selected", String(b.dataset.lang === lang)));
  $("#editor").placeholder = lang === "sql"
    ? "SELECT _time, power_w FROM pv ORDER BY _time DESC LIMIT 100"
    : "rate(http_requests_total[5m])   — Enter runs, Shift+Enter adds a line";
  document.body.classList.toggle("lang-sql", lang === "sql");
  $(".range").title = lang === "sql" ? "The range applies to PromQL. In SQL, choose rows with WHERE _time …" : "";
  renderRail(); syncOverlayToggles();
}

/** `name` or `name{…}` and nothing else — the shape the overlays understand. */
function plainSelector(q) {
  const m = /^\s*([A-Za-z_:][A-Za-z0-9_:]*)\s*(\{[^{}]*\})?\s*$/.exec(q || "");
  return m ? { name: m[1], matchers: m[2] || "" } : null;
}

/* ── Completion ───────────────────────────────────────────────────────── */

const PROM_FUNCS = [
  ["rate", "(v range-vector)", "per-second rate of a counter"], ["irate", "(v range-vector)", "instant rate"],
  ["increase", "(v range-vector)", "increase of a counter"], ["delta", "(v range-vector)", "difference first to last"],
  ["deriv", "(v range-vector)", "per-second derivative"], ["resets", "(v range-vector)", "counter resets"],
  ["changes", "(v range-vector)", "value changes"], ["avg_over_time", "(v range-vector)", ""],
  ["min_over_time", "(v range-vector)", ""], ["max_over_time", "(v range-vector)", ""],
  ["sum_over_time", "(v range-vector)", ""], ["count_over_time", "(v range-vector)", ""],
  ["last_over_time", "(v range-vector)", ""], ["first_over_time", "(v range-vector)", ""],
  ["quantile_over_time", "(φ scalar, v range-vector)", ""], ["stddev_over_time", "(v range-vector)", ""],
  ["predict_linear", "(v range-vector, t scalar)", "linear prediction"],
  ["histogram_quantile", "(φ scalar, b instant-vector)", "quantile of a histogram"],
  ["histogram_count", "(v instant-vector)", ""], ["histogram_sum", "(v instant-vector)", ""],
  ["abs", "(v)", ""], ["ceil", "(v)", ""], ["floor", "(v)", ""], ["round", "(v, to_nearest)", ""],
  ["clamp", "(v, min, max)", ""], ["clamp_min", "(v, min)", ""], ["clamp_max", "(v, max)", ""],
  ["label_replace", "(v, dst, repl, src, regex)", ""], ["label_join", "(v, dst, sep, src…)", ""],
  ["absent", "(v instant-vector)", ""], ["absent_over_time", "(v range-vector)", ""],
  ["sort", "(v)", ""], ["sort_desc", "(v)", ""], ["time", "()", "evaluation time"], ["vector", "(s scalar)", ""],
  ["scalar", "(v instant-vector)", ""], ["timestamp", "(v instant-vector)", ""],
];
const PROM_AGGS = ["sum", "avg", "min", "max", "count", "stddev", "stdvar", "topk", "bottomk", "quantile", "count_values", "group"];
const PROM_KW = ["by", "without", "on", "ignoring", "group_left", "group_right", "offset", "bool", "and", "or", "unless"];
const SQL_KW = ["SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "AND", "OR", "AS", "DESC", "ASC", "BETWEEN", "INTERVAL", "JOIN", "ON", "HAVING", "DISTINCT", "OVER", "PARTITION BY"];
const SQL_FUNCS = [
  ["time_bucket", "('5m', _time)", "calendar bucket"], ["avg", "(x)", ""], ["sum", "(x)", ""], ["min", "(x)", ""],
  ["max", "(x)", ""], ["count", "(x)", ""], ["first", "(x, _time)", ""], ["last", "(x, _time)", ""],
  ["rate", "(x, _time)", ""], ["forecast", "(x, _time, horizon)", "SES forecast, a list"],
  ["auto_forecast", "(x, _time, horizon)", "best model by cross-validation"],
  ["anomaly_score", "(x, threshold) OVER (…)", "0–1, 0.5 at the threshold"], ["zscore", "(x) OVER (…)", ""],
  ["rolling_mean", "(x, n) OVER (…)", ""], ["rolling_std", "(x, n) OVER (…)", ""], ["ewm", "(x, α) OVER (…)", ""],
  ["stl_trend", "(x, period) OVER (…)", ""], ["unnest", "(list)", ""],
];

const comp = { items: [], index: 0, from: 0, to: 0, open: false, seq: 0 };
let compTimer = 0;

async function labelNames(metric) {
  const key = `labels:${metric}`;
  if (!catalogue.values.has(key)) {
    const r = await api("api/v1/labels", { params: metric ? { "match[]": metric } : {} }).catch(() => null);
    catalogue.values.set(key, (r && r.data) || []);
  }
  return catalogue.values.get(key).filter((l) => l !== "__name__");
}
async function labelValues(label, metric) {
  const key = `values:${label}:${metric}`;
  if (!catalogue.values.has(key)) {
    const r = await api(`api/v1/label/${encodeURIComponent(label)}/values`, { params: metric ? { "match[]": metric } : {} }).catch(() => null);
    catalogue.values.set(key, (r && r.data) || []);
  }
  return catalogue.values.get(key);
}

async function suggestions(text, caret) {
  const before = text.slice(0, caret);
  const word = /[A-Za-z0-9_:.]*$/.exec(before)[0];
  const from = caret - word.length;
  if (state.lang === "promql") {
    const open = before.lastIndexOf("{"), close = before.lastIndexOf("}");
    if (open > close) {
      const metric = (/([A-Za-z_:][A-Za-z0-9_:]*)\s*$/.exec(before.slice(0, open)) || [])[1] || "";
      const inside = before.slice(open + 1);
      const val = /([A-Za-z_][A-Za-z0-9_]*)\s*(=~|!~|!=|=)\s*"([^"]*)$/.exec(inside);
      if (val) {
        const values = await labelValues(val[1], metric);
        return { from: caret - val[3].length, to: caret, items: rank(values.map((v) => ({ label: v, insert: v, detail: val[1], close: '"' })), val[3]) };
      }
      const names = await labelNames(metric);
      return { from, to: caret, items: rank(names.map((n) => ({ label: n, insert: `${n}="`, detail: "label" })), word) };
    }
    if (!word) return null;
    const items = [
      ...catalogue.metrics.map((m) => ({ label: m, insert: m, detail: "metric" })),
      ...PROM_FUNCS.map(([n, sig, d]) => ({ label: n, insert: `${n}(`, detail: d || sig, sig })),
      ...PROM_AGGS.map((n) => ({ label: n, insert: `${n}(`, detail: "aggregation" })),
      ...PROM_KW.map((n) => ({ label: n, insert: n, detail: "keyword" })),
    ];
    return { from, to: caret, items: rank(items, word) };
  }
  if (!word) return null;
  const afterTable = /\b(FROM|JOIN)\s+[A-Za-z0-9_]*$/i.test(before);
  const tables = catalogue.measurements.map((m) => ({ label: m.name, insert: m.name, detail: "measurement" }));
  if (afterTable) return { from, to: caret, items: rank(tables, word) };
  const used = new Set((text.match(/\b(?:FROM|JOIN)\s+([A-Za-z0-9_]+)/gi) || []).map((s) => s.split(/\s+/).pop()));
  const cols = catalogue.measurements.filter((m) => used.has(m.name))
    .flatMap((m) => m.columns.map((c) => ({ label: c.name, insert: c.name, detail: `${c.role} · ${m.name}` })));
  const items = [
    ...cols, ...tables,
    ...SQL_FUNCS.map(([n, sig, d]) => ({ label: n, insert: `${n}(`, detail: d || sig, sig })),
    ...SQL_KW.map((k) => ({ label: k, insert: k, detail: "keyword" })),
  ];
  return { from, to: caret, items: rank(items, word) };
}
function rank(items, prefix) {
  const p = prefix.toLowerCase();
  const seen = new Set();
  const scored = [];
  for (const it of items) {
    if (seen.has(it.label)) continue;
    const l = it.label.toLowerCase();
    const at = p ? l.indexOf(p) : 0;
    if (at < 0 || (p && l === p)) continue;
    seen.add(it.label);
    scored.push({ ...it, at, score: at === 0 ? 0 : 1 });
  }
  scored.sort((a, b) => a.score - b.score || a.label.length - b.label.length || a.label.localeCompare(b.label));
  return scored.slice(0, 50).map((it) => ({ ...it, match: [it.at, it.at + p.length] }));
}

async function complete(force) {
  const ed = $("#editor");
  const seq = ++comp.seq;
  const res = await suggestions(ed.value, ed.selectionStart).catch(() => null);
  if (seq !== comp.seq || document.activeElement !== ed) return;
  if (!res || !res.items.length || (!force && res.to - res.from < 1 && !res.items[0].close && res.items[0].detail !== "label")) { closeComplete(); return; }
  Object.assign(comp, { items: res.items, index: 0, from: res.from, to: res.to, open: true });
  renderComplete();
}
function renderComplete() {
  const list = clear($("#complete"));
  comp.items.forEach((it, i) => {
    const [a, b] = it.match;
    list.append(el("li", {
      role: "option", id: `c-${i}`, "aria-selected": String(i === comp.index),
      "on:mousedown": (e) => { e.preventDefault(); comp.index = i; accept(); },
    }, el("span", { class: "c-label" }, it.label.slice(0, a), el("mark", {}, it.label.slice(a, b)), it.label.slice(b)),
    el("span", { class: "c-detail" }, it.sig && it.detail !== it.sig ? `${it.sig}${it.detail ? ` — ${it.detail}` : ""}` : it.detail)));
  });
  list.hidden = false;
  $("#editor").setAttribute("aria-activedescendant", `c-${comp.index}`);
  const pos = caretXY($("#editor"), comp.from);
  list.style.left = `${Math.max(10, Math.min(pos.x, $(".editor-wrap").clientWidth - 280))}px`;
  list.style.top = `${pos.y}px`;
  const cur = list.children[comp.index];
  if (cur) cur.scrollIntoView({ block: "nearest" });
}
function closeComplete() { comp.open = false; comp.seq++; $("#complete").hidden = true; $("#editor").removeAttribute("aria-activedescendant"); }
function accept() {
  const it = comp.items[comp.index];
  if (!it) return;
  const ed = $("#editor");
  let text = it.insert;
  if (it.close && ed.value[comp.to] !== it.close) text += it.close;
  ed.setRangeText(text, comp.from, comp.to, "end");
  if (it.close) ed.setSelectionRange(comp.from + it.insert.length + 1, comp.from + it.insert.length + 1);
  closeComplete(); autosize();
  if (it.insert.endsWith('="')) complete(true);
}

/** Caret pixel position inside the textarea, relative to `.editor-wrap`. */
function caretXY(ta, index) {
  const mirror = el("div");
  const cs = getComputedStyle(ta);
  for (const p of ["boxSizing", "width", "fontFamily", "fontSize", "fontWeight", "lineHeight", "letterSpacing",
    "paddingTop", "paddingRight", "paddingBottom", "paddingLeft", "borderTopWidth", "borderLeftWidth", "tabSize", "whiteSpace"]) {
    mirror.style[p] = cs[p];
  }
  mirror.style.position = "absolute"; mirror.style.visibility = "hidden"; mirror.style.whiteSpace = "pre-wrap";
  mirror.style.borderStyle = "solid"; mirror.style.borderColor = "transparent";
  mirror.style.overflowWrap = "break-word"; mirror.style.top = "0"; mirror.style.left = "0";
  mirror.append(document.createTextNode(ta.value.slice(0, index)));
  const mark = el("span", {}, "\u200b");
  mirror.append(mark);
  ta.parentElement.append(mirror);
  const x = ta.offsetLeft + mark.offsetLeft - 4;
  const y = ta.offsetTop + mark.offsetTop - ta.scrollTop + parseFloat(cs.lineHeight || "20") + 6;
  mirror.remove();
  return { x, y };
}

/* ── Running a query ──────────────────────────────────────────────────── */

let last = null;          // the last result, for re-rendering
let lastRequest = null;   // the last request, for "copy as code"
let firstChartMarked = false;

async function run(push) {
  const ed = $("#editor");
  state.q = ed.value.trim();
  closeComplete();
  if (!state.q) { renderWelcome(); return; }
  writeFragment(push);
  if (inflight) inflight.abort();
  const ctl = new AbortController();
  inflight = ctl;
  const { start, end } = window_();
  const step = stepFor(start, end);
  lastRequest = { lang: state.lang, query: state.q, start, end, step };
  explainCache = null;
  busy(true);
  const t0 = performance.now();
  const timer = setTimeout(() => setStatus([el("span", {}, "Running…"), el("span", { class: "right" }, "Esc to cancel")]), 300);
  try {
    if (state.lang === "promql") {
      const res = await api("api/v1/query_range", {
        params: { query: state.q, start: start.toFixed(3), end: end.toFixed(3), step: String(step), explain_empty: "true" },
        signal: ctl.signal,
      });
      const ms = performance.now() - t0;
      if (res.status === "error") { showError({ message: res.error, kind: res.errorType }); last = null; renderEmptyResult(); setStatus([]); return; }
      last = fromPromql(res, { start, end, step, ms });
    } else {
      const res = await api("api/v1/chronix/sql", { method: "POST", body: { query: state.q }, signal: ctl.signal });
      const ms = performance.now() - t0;
      last = fromSql(res, { start, end, step, ms });
    }
    renderDiagnostics(last.warnings);
    renderResult();
  } catch (err) {
    if (err.name === "AbortError") { setStatus([el("span", {}, "Cancelled.")]); return; }
    last = null;
    if (err instanceof ApiError && err.body) showError({ message: err.body.error, hint: err.body.hint, span: err.body.span, kind: err.body.code, status: err.status });
    else showError({ message: err.message });
    renderEmptyResult(); setStatus([]);
  } finally {
    clearTimeout(timer);
    if (inflight === ctl) { inflight = null; busy(false); }
  }
}
function busy(on) { $("#progress").hidden = !on; $("#run").disabled = false; }

function fromPromql(res, meta) {
  const data = res.data || {};
  const series = (data.result || []).map((s) => ({
    labels: s.metric || {},
    points: (s.values || (s.value ? [s.value] : [])).map(([t, v]) => {
      const n = Number(v);
      return [Number(t), Number.isFinite(n) ? n : null];
    }),
  }));
  return { kind: "promql", series, warnings: res.warnings || [], ...meta, points: series.reduce((n, s) => n + s.points.length, 0) };
}
function fromSql(res, meta) {
  const columns = res.columns || [];
  const rows = res.rows || [];
  return { kind: "sql", columns, rows, truncated: !!res.truncated, warnings: [], ...meta, points: rows.length };
}

/* ── Diagnostics: errors, hints, "why is this empty?" ─────────────────── */

function note(kind, ...body) {
  return el("div", { class: `note ${kind}`, role: kind === "error" ? "alert" : undefined },
    icon(ICONS[kind === "error" ? "error" : kind === "warn" ? "warn" : "info"]), el("div", { class: "body" }, body));
}

function showError({ message = "", hint, span, kind, status }) {
  const diag = clear($("#diag"));
  let msg = message;
  let at = null;
  const pos = /^(\d+):(\d+):\s*(.*)$/s.exec(msg);
  if (pos) {
    msg = pos[3];
    const col = Number(pos[2]) - 1;
    const tail = /^[A-Za-z0-9_:]+/.exec(state.q.slice(col));
    at = { start: col, end: col + Math.max(1, tail ? tail[0].length : 1) };
  } else if (span) at = byteSpan(state.q, span);
  msg = msg.replace(/^parse error:\s*/i, "");
  let suggestion = null;
  const dym = /\s+—\s+did you mean "([^"]+)"\?$/.exec(msg);
  if (dym) { suggestion = dym[1]; msg = msg.slice(0, dym.index); }
  if (!suggestion && hint) { const h = /did you mean "([^"]+)"/.exec(hint); if (h) suggestion = h[1]; }
  if (status === 403) msg = `${msg} — check the key and namespace in Connection.`;
  const parts = [el("div", { class: "msg" }, capital(msg))];
  if (at) {
    const q = state.q;
    parts.push(el("pre", { class: "where", "aria-label": "Where" }, q.slice(0, at.start), el("mark", {}, q.slice(at.start, at.end) || " "), q.slice(at.end)));
  }
  if (suggestion) {
    parts.push(el("div", { class: "hint" }, `Did you mean `, el("code", {}, suggestion), "?",
      at ? el("button", { type: "button", class: "apply", "on:click": () => { replaceRange(at, suggestion); run(true); } }, "Apply") : null));
  } else if (hint) parts.push(el("div", { class: "hint" }, capital(hint)));
  diag.append(note("error", parts));
}
function capital(s) { return s ? s[0].toUpperCase() + s.slice(1) : s; }
function byteSpan(text, span) {
  const enc = new TextEncoder();
  let bytes = 0, start = null, end = null;
  for (let i = 0; i <= text.length; i++) {
    if (start === null && bytes >= span.start) start = i;
    if (end === null && bytes >= span.end) { end = i; break; }
    if (i < text.length) bytes += enc.encode(text[i]).length;
  }
  return start === null ? null : { start, end: end === null ? text.length : end };
}
function replaceRange(at, text) {
  const q = state.q;
  setQuery(q.slice(0, at.start) + text + q.slice(at.end));
}

function renderDiagnostics(warnings) {
  const diag = clear($("#diag"));
  const typos = new Set();
  for (const w of warnings) {
    const typo = /^metric "(.+?)" matches no series — did you mean "(.+?)"\?$/.exec(w);
    if (typo) {
      typos.add(typo[1]);
      diag.append(note("warn", el("div", { class: "msg" }, el("code", {}, typo[1]), " matches no series. Did you mean ",
        el("code", {}, typo[2]), "?", el("button", {
          type: "button", class: "apply", "on:click": () => { setQuery(replaceToken(state.q, typo[1], typo[2])); run(true); },
        }, "Apply"))));
      continue;
    }
    const funnel = parseFunnel(w);
    if (funnel) { diag.append(renderFunnel(funnel)); continue; }
    const quiet = /^(.+?): no series of "(.+?)" in this window$/.exec(w);
    if (quiet && typos.has(quiet[2])) continue;
    diag.append(note("info", el("div", { class: "msg" }, w)));
  }
}
function replaceToken(text, from, to) {
  const re = new RegExp(`(^|[^A-Za-z0-9_:])${from.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?![A-Za-z0-9_:])`);
  return text.replace(re, (m, pre) => `${pre}${to}`);
}

/** `sel: N series of "m", M after k="v" — k has: a, b` → stages and values. */
function parseFunnel(w) {
  const m = /^(.+?): (\S+ series of "[^"]+"(?:, \S+ after .+?)*)(?: — ([A-Za-z_][A-Za-z0-9_]*) has: (.*))?$/.exec(w);
  if (!m) return null;
  const stages = [];
  const first = /^(\S+) series of "([^"]+)"/.exec(m[2]);
  if (!first) return null;
  stages.push({ count: first[1], label: `series of ${first[2]}` });
  const re = /, (\S+) after (.+?)(?=, \S+ after |$)/g;
  let s;
  while ((s = re.exec(m[2].slice(first[0].length)))) stages.push({ count: s[1], matcher: s[2] });
  let values = null, more = null;
  if (m[4] !== undefined) {
    const parts = m[4].split(", ");
    const tail = /^\+(\d+) more$/.exec(parts[parts.length - 1] || "");
    if (tail) { more = Number(tail[1]); parts.pop(); }
    values = parts.filter(Boolean);
  }
  return { selector: m[1], stages, label: m[3], values, more };
}
function renderFunnel(f) {
  const kids = [];
  f.stages.forEach((st, i) => {
    if (i) kids.push(el("span", { class: "arrow", "aria-hidden": "true" }, "→"));
    const zero = st.count === "0";
    kids.push(el("span", { class: `stage${zero ? " zero" : ""}` }, el("b", {}, st.count),
      st.matcher ? ["after ", el("code", {}, st.matcher)] : st.label));
  });
  const body = [el("div", { class: "msg" }, "Why is this empty? ", el("code", {}, f.selector)), el("div", { class: "funnel" }, kids)];
  if (f.label && f.values) {
    const zero = f.stages.find((s) => s.count === "0" && s.matcher);
    body.push(el("div", { class: "chips" }, el("span", { class: "muted" }, `${f.label} has:`),
      f.values.map((v) => el("button", {
        type: "button", class: "chip", title: `Use ${f.label}="${v}"`,
        "on:click": () => {
          if (zero) {
            const re = new RegExp(`\\b${f.label}\\s*(=~|!~|!=|=)\\s*"(?:[^"\\\\]|\\\\.)*"`);
            setQuery(state.q.replace(re, `${f.label}=${JSON.stringify(v)}`));
          }
          run(true);
        },
      }, v)), f.more ? el("span", { class: "chip static" }, `+${f.more} more`) : null));
  }
  return note("warn", body);
}

/* ── Result views ─────────────────────────────────────────────────────── */

function setTab(tab, push) {
  state.tab = tab;
  $$(".tabs [data-tab]").forEach((b) => b.setAttribute("aria-selected", String(b.dataset.tab === tab)));
  for (const t of TABS) $(`#panel-${t}`).hidden = t !== tab;
  writeFragment(push);
  renderResult();
}

function renderResult() {
  if (state.tab === "cardinality") { renderCardinality(); return; }
  if (!last) { if (!state.q) renderWelcome(); return; }
  syncOverlayToggles();
  renderStatus();
  if (state.tab === "chart") renderChart();
  else if (state.tab === "table") renderTable();
  else if (state.tab === "explain") renderExplain();
}

function renderStatus() {
  if (!last) { setStatus([]); return; }
  const parts = [];
  if (last.kind === "promql") {
    parts.push(el("span", {}, `${fmtInt(last.series.length)} series`), el("span", { class: "sep" }, "·"),
      el("span", {}, `${fmtInt(last.points)} points`), el("span", { class: "sep" }, "·"),
      el("span", {}, `step ${fmtDur(last.step)}`));
  } else {
    parts.push(el("span", {}, `${fmtInt(last.rows.length)} row${last.rows.length === 1 ? "" : "s"}`));
    if (last.truncated) parts.push(el("span", { class: "sep" }, "·"), el("span", { class: "warn-text", title: "The server's sql_max_rows cut this result; aggregates over it are wrong, not partial." }, "truncated — add a LIMIT or a narrower WHERE"));
  }
  parts.push(el("span", { class: "sep" }, "·"), el("span", {}, fmtMs(last.ms)));
  if (last.kind === "promql") parts.push(el("span", { class: "right" }, `${fmtShort(last.start)} – ${fmtShort(last.end)}`));
  setStatus(parts);
}
function setStatus(kids) { append(clear($("#status")), kids); }

function emptyState(panel, iconPath, title, text, extra) {
  append(clear(panel), el("div", { class: "empty" },
    el("div", { class: "icon" }, icon(iconPath)), el("h3", {}, title), text ? el("p", {}, text) : null, extra || null));
}
function renderWelcome() {
  const panel = $(`#panel-${state.tab === "cardinality" ? "chart" : state.tab}`);
  const examples = catalogue.metrics.slice(0, 4);
  const chips = examples.length
    ? el("div", { class: "chips" }, examples.map((m) => el("button", {
      type: "button", class: "chip", "on:click": () => { setLang("promql"); setQuery(m); run(true); },
    }, m)))
    : null;
  emptyState(panel, ICONS.chart, "Ask a question of your data",
    catalogue.measurements.length
      ? "Pick a field on the left, type a PromQL or SQL query, or start from one of these:"
      : "There is nothing stored yet. Run chronixd --demo for a generated week of gateway data.",
    chips);
  if (state.tab === "cardinality") return;
  setStatus([el("span", {}, isMac ? "⌘↵ runs · / focuses the editor · ? lists every shortcut" : "Ctrl+↵ runs · / focuses the editor · ? lists every shortcut")]);
}
function renderEmptyResult() {
  for (const t of ["chart", "table", "explain"]) {
    emptyState($(`#panel-${t}`), ICONS.error, "Nothing to show", "Fix the query above and run it again.");
  }
  if (state.tab === "cardinality") renderCardinality();
}

/* ── Chart ────────────────────────────────────────────────────────────── */

let plot = null;
let overlay = { forecast: null, anomalies: null, status: {} };
let resizeObs = null;

function seriesColors() { return [1, 2, 3, 4, 5, 6, 7, 8].map((i) => css(`--s${i}`)); }

/** Labels that differ between series name each one; the rest are noise. */
function seriesNames(series) {
  if (series.length === 1) return [selectorText(series[0].labels)];
  const keys = new Set(series.flatMap((s) => Object.keys(s.labels)));
  const varying = [...keys].filter((k) => new Set(series.map((s) => s.labels[k])).size > 1);
  const names = new Set(series.map((s) => s.labels.__name__));
  return series.map((s) => {
    const shown = varying.filter((k) => k !== "__name__");
    const inner = shown.map((k) => `${k}="${s.labels[k] ?? ""}"`).join(", ");
    const name = names.size > 1 ? (s.labels.__name__ || "") : "";
    return `${name}${inner ? `{${inner}}` : ""}` || selectorText(s.labels);
  });
}
function selectorText(labels) {
  const name = labels.__name__ || "";
  const inner = Object.entries(labels).filter(([k]) => k !== "__name__").map(([k, v]) => `${k}="${v}"`).join(", ");
  return `${name}${inner ? `{${inner}}` : ""}` || "{}";
}

/** Series for the chart, from either query language. */
function chartSeries() {
  if (!last) return [];
  if (last.kind === "promql") {
    const names = seriesNames(last.series);
    return last.series.map((s, i) => ({ name: names[i], labels: s.labels, points: s.points }));
  }
  return sqlSeries(last);
}
function hasTime(r) { return r.kind === "sql" && timeColumn(r) >= 0; }
function timeColumn(r) { return r.columns.findIndex((c) => c.name === "_time" || /^Timestamp/.test(c.data_type)); }
function isNumeric(t) { return /^(Float|Int|UInt|Decimal)/.test(t); }
function sqlSeries(r) {
  const ti = timeColumn(r);
  if (ti < 0) return [];
  const nums = r.columns.map((c, i) => [c, i]).filter(([c, i]) => i !== ti && isNumeric(c.data_type));
  const key = r.columns.findIndex((c, i) => i !== ti && /Utf8|String/.test(c.data_type));
  if (!nums.length) return [];
  const groups = new Map();
  for (const row of r.rows) {
    const t = toSeconds(row[ti]);
    if (t === null) continue;
    for (const [c, i] of nums) {
      const g = `${key >= 0 ? `${row[key]} · ` : ""}${c.name}`;
      if (!groups.has(g)) { if (groups.size >= 12) continue; groups.set(g, []); }
      groups.get(g).push([t, row[i] === null ? null : Number(row[i])]);
    }
  }
  return [...groups].map(([name, points]) => ({ name, labels: {}, points: points.sort((a, b) => a[0] - b[0]) }));
}
function toSeconds(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === "number") return v > 1e14 ? v / 1e9 : v > 1e11 ? v / 1e3 : v;
  const d = Date.parse(v);
  return Number.isFinite(d) ? d / 1000 : null;
}

function renderChart() {
  const panel = $("#panel-chart");
  const series = chartSeries();
  if (plot) { plot.destroy(); plot = null; }
  if (!series.length || series.every((s) => !s.points.length)) {
    if (last.kind === "sql" && !hasTime(last)) {
      emptyState(panel, ICONS.table, "Nothing to plot", "This result has no time column. The table shows it as it is.",
        el("button", { type: "button", class: "btn", "on:click": () => setTab("table", true) }, "Show the table"));
    } else {
      emptyState(panel, ICONS.search, "No data in this range",
        $("#diag").childElementCount ? "The notes above say why." : "Try a wider range, or check the selector.");
    }
    return;
  }
  if (typeof window.uPlot !== "function") { emptyState(panel, ICONS.chart, "Loading the chart…", ""); setTimeout(renderChart, 50); return; }

  // One shared x axis: the union of every series' timestamps.
  const xs = [...new Set(series.flatMap((s) => s.points.map((p) => p[0])))].sort((a, b) => a - b);
  const index = new Map(xs.map((x, i) => [x, i]));
  const colors = seriesColors();
  const cols = series.map((s) => { const a = new Array(xs.length).fill(null); for (const [t, v] of s.points) a[index.get(t)] = v; return a; });

  const fc = overlay.forecast;
  const extra = [];
  let fcIdx = -1;
  if (fc && series.length === 1) {
    const lastX = xs[xs.length - 1];
    const lastY = cols[0][cols[0].length - 1];
    const ahead = fc.points.filter((p) => p.t > lastX);
    for (const p of ahead) { xs.push(p.t); cols[0].push(null); }
    const pad = (arr) => [...new Array(xs.length - ahead.length - 1).fill(null), lastY, ...arr];
    extra.push(pad(ahead.map((p) => p.value)), pad(ahead.map((p) => p.upper)), pad(ahead.map((p) => p.lower)));
    fcIdx = series.length + 1;
  }
  const data = [xs, ...cols, ...extra];

  const muted = css("--muted"), grid = css("--grid"), font = `11px ${getComputedStyle(document.body).fontFamily}`;
  const opts = {
    width: Math.max(320, panel.clientWidth - 28),
    height: chartHeight(),
    padding: [10, 14, 0, 2],
    cursor: { drag: { x: true, y: false, setScale: false }, points: { size: 7, width: 2 }, focus: { prox: 24 } },
    focus: { alpha: 0.35 },
    legend: { show: false },
    scales: { x: { time: true } },
    axes: [
      { stroke: muted, font, grid: { stroke: grid, width: 1 }, ticks: { stroke: grid, width: 1, size: 4 }, gap: 6, values: timeTicks },
      { stroke: muted, font, grid: { stroke: grid, width: 1 }, ticks: { show: false }, size: 52, gap: 6, values: (u, vals) => vals.map(fmtAxis) },
    ],
    series: [
      {},
      ...series.map((s, i) => ({ label: s.name, stroke: colors[i % colors.length], width: 1.6, points: { show: false }, spanGaps: false })),
    ],
    hooks: {
      drawClear: [drawAnomalyWindows],
      draw: [drawOverlayMarks],
      setCursor: [showTooltip],
      setSelect: [zoomToSelection],
    },
  };
  if (fcIdx > 0) {
    const c = colors[0];
    opts.series.push(
      { label: "forecast", stroke: c, width: 1.6, dash: [6, 5], points: { show: false } },
      { label: "upper", stroke: alpha(c, 0.35), width: 1, points: { show: false } },
      { label: "lower", stroke: alpha(c, 0.35), width: 1, points: { show: false } },
    );
    opts.bands = [{ series: [fcIdx + 1, fcIdx + 2], fill: alpha(c, 0.14) }];
  }

  clear(panel);
  const head = el("div", { class: "chart-head" }, renderLegend(series, colors, fcIdx));
  const badges = overlayBadges();
  if (badges.length) append(head, badges);
  const box = el("div", { class: "chart" });
  panel.append(head, box);
  plot = new window.uPlot(opts, data, box);
  plot.meta = { series, fcIdx };
  if (!resizeObs) {
    resizeObs = new ResizeObserver(() => { if (plot) plot.setSize({ width: Math.max(320, $("#panel-chart").clientWidth - 28), height: chartHeight() }); });
    resizeObs.observe(panel);
  }
  if (!firstChartMarked) { firstChartMarked = true; document.body.dataset.firstChartMs = String(Math.round(performance.now())); }
}
/** Fill the card down to the status line, within sensible bounds. */
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
/** Time ticks in 24-hour time, with the date under the first tick of each day. */
function timeTicks(u, splits, axis, space, incr) {
  let prevDay = null;
  return splits.map((t) => {
    const d = new Date(t * 1000);
    const day = `${MONTHS[d.getMonth()]} ${d.getDate()}`;
    const showDay = day !== prevDay;
    prevDay = day;
    if (incr >= 86400) return day;
    const hm = incr < 60 ? `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}` : `${pad(d.getHours())}:${pad(d.getMinutes())}`;
    return showDay ? `${hm}\n${day}` : hm;
  });
}
function chartHeight() {
  const panel = $("#panel-chart");
  const top = panel.getBoundingClientRect().top + window.scrollY;
  const head = 40, status = 34, padding = 24 + 16;
  return Math.max(260, Math.min(640, window.innerHeight - top - head - status - padding));
}

function renderLegend(series, colors, fcIdx) {
  const box = el("div", { class: "legend", role: "group", "aria-label": "Series" });
  series.slice(0, 40).forEach((s, i) => {
    const btn = el("button", {
      type: "button", "aria-pressed": "true", title: `${s.name} — click to hide, again to show`,
      "on:click": () => { const on = btn.getAttribute("aria-pressed") !== "true"; btn.setAttribute("aria-pressed", String(on)); plot.setSeries(i + 1, { show: on }); },
    }, el("span", { class: "swatch" }), el("span", { class: "lbl" }, legendLabel(s.name)));
    $(".swatch", btn).style.background = colors[i % colors.length];
    box.append(btn);
  });
  if (series.length > 40) box.append(el("span", { class: "badge" }, `+${series.length - 40} more`));
  if (fcIdx > 0) {
    const btn = el("button", {
      type: "button", "aria-pressed": "true", title: "Forecast and its interval",
      "on:click": () => { const on = btn.getAttribute("aria-pressed") !== "true"; btn.setAttribute("aria-pressed", String(on)); for (const k of [0, 1, 2]) plot.setSeries(fcIdx + k, { show: on }); },
    }, el("span", { class: "swatch dash" }), el("span", { class: "lbl" }, "forecast"));
    $(".swatch", btn).style.color = colors[0];
    box.append(btn);
  }
  if (overlay.anomalies && overlay.anomalies.windows.length) {
    box.append(el("button", { type: "button", "aria-pressed": "true", tabindex: "-1", title: "Anomaly windows" },
      el("span", { class: "swatch ring" }), el("span", { class: "lbl" }, "anomalies")));
  }
  return box;
}
function legendLabel(name) {
  const m = /^([^{]*)\{(.*)\}$/.exec(name);
  if (!m) return name;
  const pairs = m[2].split(/, (?=[A-Za-z_][A-Za-z0-9_]*=)/).map((p) => {
    const [k, ...v] = p.split("=");
    return [el("span", { class: "k" }, `${k}=`), v.join("=")];
  });
  const out = [m[1], m[1] ? "{" : ""];
  pairs.forEach((p, i) => { if (i) out.push(", "); out.push(p); });
  if (m[1]) out.push("}");
  return out;
}

function overlayBadges() {
  const out = [];
  const st = overlay.status;
  if (st.forecast === "busy") out.push(el("span", { class: "badge busy" }, "Forecasting…"));
  else if (overlay.forecast) {
    const f = overlay.forecast;
    const b = el("span", { class: "badge fc", title: f.summary }, el("span", { class: "b" }),
      `${f.model} · ${Math.round((f.level || 0.95) * 100)}${NNBSP}% interval · fit on ${fmtDur(f.fit)} at ${fmtDur(f.step)}`
        + (typeof f.floor === "number" ? ` · clipped at ${f.floor}` : ""));
    $(".b", b).style.background = seriesColors()[0];
    out.push(b);
  } else if (st.forecast && st.forecast !== "off") out.push(el("span", { class: "badge", title: st.forecast }, "Forecast unavailable"));
  if (st.anomalies === "busy") out.push(el("span", { class: "badge busy" }, "Detecting…"));
  else if (overlay.anomalies) {
    const a = overlay.anomalies;
    out.push(el("span", { class: "badge an", title: a.summary }, el("span", { class: "b" }),
      a.windows.length ? `${a.windows.length} anomal${a.windows.length === 1 ? "y" : "ies"} · ${a.method} > ${a.threshold}` : `no anomalies · ${a.method} > ${a.threshold}`));
  } else if (st.anomalies && st.anomalies !== "off") out.push(el("span", { class: "badge", title: st.anomalies }, "Anomalies unavailable"));
  return out;
}

function drawAnomalyWindows(u) {
  const a = overlay.anomalies;
  if (!a || !a.windows.length) return;
  const { ctx } = u;
  const { left, top, width, height } = u.bbox;
  ctx.save();
  ctx.beginPath(); ctx.rect(left, top, width, height); ctx.clip();
  ctx.fillStyle = alpha(css("--anomaly"), 0.12);
  const minW = 4 * devicePixelRatio;
  for (const w of a.windows) {
    const x0 = u.valToPos(w.from, "x", true), x1 = u.valToPos(w.to, "x", true);
    const wd = Math.max(minW, x1 - x0);
    ctx.fillRect(x0 - (wd - (x1 - x0)) / 2, top, wd, height);
  }
  ctx.restore();
}
function drawOverlayMarks(u) {
  const { ctx } = u;
  const { left, top, width, height } = u.bbox;
  ctx.save();
  ctx.beginPath(); ctx.rect(left, top, width, height); ctx.clip();
  const dpr = devicePixelRatio;
  if (overlay.forecast && u.meta && u.meta.fcIdx > 0) {
    const x = u.valToPos(overlay.forecast.now, "x", true);
    ctx.strokeStyle = css("--border-strong"); ctx.lineWidth = dpr; ctx.setLineDash([3 * dpr, 3 * dpr]);
    ctx.beginPath(); ctx.moveTo(x, top); ctx.lineTo(x, top + height); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = css("--muted"); ctx.font = `${11 * dpr}px ${getComputedStyle(document.body).fontFamily}`;
    ctx.fillText("now", x + 4 * dpr, top + 12 * dpr);
  }
  const a = overlay.anomalies;
  if (a && a.windows.length) {
    ctx.strokeStyle = css("--anomaly"); ctx.lineWidth = 2 * dpr; ctx.fillStyle = css("--surface");
    for (const w of a.windows) {
      const x = u.valToPos(w.peak.t, "x", true), y = u.valToPos(w.peak.value, "y", true);
      ctx.beginPath(); ctx.arc(x, y, 6 * dpr, 0, 2 * Math.PI); ctx.fill(); ctx.stroke();
      ctx.beginPath(); ctx.arc(x, y, 2 * dpr, 0, 2 * Math.PI); ctx.fillStyle = css("--anomaly"); ctx.fill(); ctx.fillStyle = css("--surface");
    }
  }
  ctx.restore();
}

function showTooltip(u) {
  const tip = $("#tooltip");
  const i = u.cursor.idx;
  if (i === null || i === undefined || u.cursor.left < 0) { tip.hidden = true; return; }
  const x = u.data[0][i];
  const { series, fcIdx } = u.meta;
  const colors = seriesColors();
  const rows = [];
  const ranked = series.map((s, k) => ({ k, s, v: u.data[k + 1][i] }))
    .filter((r) => r.v !== null && r.v !== undefined && u.series[r.k + 1].show)
    .sort((a, b) => b.v - a.v);
  for (const r of ranked.slice(0, 10)) {
    const sw = el("span", { class: "swatch" });
    sw.style.background = colors[r.k % colors.length];
    rows.push(el("div", { class: "t-row" }, sw, el("span", { class: "name" }, r.s.name), el("span", { class: "val" }, fmtNum(r.v))));
  }
  if (ranked.length > 10) rows.push(el("div", { class: "t-row muted" }, `+${ranked.length - 10} more`));
  if (fcIdx > 0 && u.data[fcIdx][i] !== null && u.data[fcIdx][i] !== undefined && x > overlay.forecast.now) {
    const sw = el("span", { class: "swatch" });
    sw.style.background = alpha(colors[0], 0.5);
    rows.push(el("div", { class: "t-row" }, sw, el("span", { class: "name" }, "forecast"),
      el("span", { class: "val" }, `${fmtNum(u.data[fcIdx][i])} (${fmtNum(u.data[fcIdx + 2][i])} – ${fmtNum(u.data[fcIdx + 1][i])})`)));
  }
  const a = overlay.anomalies;
  const hit = a && a.windows.find((w) => x >= w.from - a.slack && x <= w.to + a.slack);
  if (hit) {
    rows.push(el("div", { class: "t-an" },
      `Anomaly ${fmtShort(hit.from)}${hit.to > hit.from ? ` – ${fmtHM(hit.to)}` : ""} · peak ${fmtNum(hit.peak.value)} · score ${hit.peak.score.toFixed(2)}`));
  }
  if (!rows.length) { tip.hidden = true; return; }
  append(clear(tip), el("div", { class: "t-time" }, fmtTime(x)), rows);
  tip.hidden = false;
  const r = u.over.getBoundingClientRect();
  const px = r.left + u.cursor.left, py = r.top + u.cursor.top;
  const w = tip.offsetWidth, h = tip.offsetHeight;
  tip.style.left = `${px + 16 + w > window.innerWidth - 8 ? px - w - 16 : px + 16}px`;
  tip.style.top = `${Math.min(window.innerHeight - h - 8, Math.max(8, py - h / 2))}px`;
}
function zoomToSelection(u) {
  if (u.select.width < 4) return;
  const from = u.posToVal(u.select.left, "x"), to = u.posToVal(u.select.left + u.select.width, "x");
  u.setSelect({ left: 0, top: 0, width: 0, height: 0 }, false);
  $("#tooltip").hidden = true;
  setRange("custom", from, to);
}

/* ── Overlays: forecast and anomalies, from the MCP tools ─────────────── */

function overlayTarget() {
  if (!last || last.kind !== "promql" || last.series.length !== 1) return null;
  const sel = plainSelector(state.q);
  if (!sel) return null;
  const labels = { ...last.series[0].labels };
  delete labels.__name__;
  return { metric: last.series[0].labels.__name__ || sel.name, labels };
}
function syncOverlayToggles() {
  const target = overlayTarget();
  const why = state.lang !== "promql"
    ? "Forecast and anomalies apply to a PromQL selector."
    : !last ? "Run a query first."
      : last.kind === "promql" && last.series.length !== 1
        ? `They apply to one series; this query returns ${last.series.length}. Add a label matcher.`
        : "They apply to a plain selector such as pv_power_w{device=\"pv\"}, not to a function of one.";
  if (!target) { overlay = { forecast: null, anomalies: null, status: {} }; overlayKey = ""; }
  for (const [id, on] of [["fc", state.fc], ["an", state.an]]) {
    const box = $(`#${id}`);
    box.checked = on;
    box.disabled = !target;
    $(`#${id}-wrap`).classList.toggle("disabled", !target);
    $(`#${id}-wrap`).title = target ? (id === "fc" ? "Forecast this series, with a prediction interval" : "Mark anomalous windows") : why;
  }
  if (target) refreshOverlays(target);
}

let overlayKey = "";
async function refreshOverlays(target) {
  const key = JSON.stringify([target, last.start, last.end, state.fc, state.an]);
  if (key === overlayKey) return;
  overlayKey = key;
  overlay = { forecast: null, anomalies: null, status: {} };
  const jobs = [];
  if (state.fc) { overlay.status.forecast = "busy"; jobs.push(loadForecast(target, key)); }
  if (state.an) { overlay.status.anomalies = "busy"; jobs.push(loadAnomalies(target, key)); }
  if (jobs.length && state.tab === "chart") renderChart();
  await Promise.all(jobs);
}
async function rawInterval(target) {
  const inner = Object.entries(target.labels).map(([k, v]) => `${k}=${JSON.stringify(v)}`).join(",");
  try {
    const r = await api("api/v1/query", { params: { query: `count_over_time(${target.metric}{${inner}}[1h])`, time: last.end.toFixed(3) } });
    const n = Number(r.data.result[0].value[1]);
    return n > 0 ? 3600 / n : last.step;
  } catch { return last.step; }
}
async function loadForecast(target, key) {
  try {
    // Fit on what the chart shows, at the chart's own step: a week at 15m is
    // 672 points with the daily season in view, where raw samples would be
    // tens of thousands. The step only grows past the chart's to stay under
    // the tool's 5000-bucket cap.
    const fit = last.end - last.start;
    const step = Math.ceil(Math.max(last.step, fit / 4000));
    const horizon = Math.max(12, Math.min(2000, Math.round((fit * 0.25) / step)));
    // A series that has never gone below zero in view is clamped at zero, and
    // the chip says so: the floor is the console's statement, not the server's.
    const nonNegative = last.series[0].points.every((p) => !(Number(p[1]) < 0));
    const args = {
      metric: target.metric, labels: target.labels, start: iso(last.start), end: iso(last.end),
      step: `${step}s`, horizon,
    };
    if (nonNegative) args.min = 0;
    const { data, summary } = await tool("forecast", args);
    if (key !== overlayKey) return;
    const points = (data.points || []).map((p) => ({ t: Date.parse(p.t) / 1000, value: p.value, lower: p.lower, upper: p.upper }));
    const now = Math.max(...last.series[0].points.map((p) => p[0]));
    overlay.forecast = { points, model: data.model || "model", level: data.interval_level, fit, step, now, summary, floor: data.clamped_below };
    overlay.status.forecast = "ok";
  } catch (e) {
    if (key !== overlayKey) return;
    overlay.status.forecast = e.status === 404 ? "This server does not serve /mcp, which the forecast uses." : e.message;
  }
  if (state.tab === "chart") renderChart();
}
async function loadAnomalies(target, key) {
  try {
    const raw = await rawInterval(target);
    const threshold = 3;
    const { data, summary } = await tool("detect_anomalies", {
      metric: target.metric, labels: target.labels, start: iso(last.start), end: iso(last.end), threshold,
    });
    if (key !== overlayKey) return;
    const pts = (data.anomalies || []).map((a) => ({ t: Date.parse(a.t) / 1000, value: a.value, score: a.score })).sort((a, b) => a.t - b.t);
    const gap = Math.max(raw * 3, last.step);
    const windows = [];
    for (const p of pts) {
      const w = windows[windows.length - 1];
      if (w && p.t - w.to <= gap) { w.to = p.t; if (p.score > w.peak.score || (p.score === w.peak.score && Math.abs(p.value) > Math.abs(w.peak.value))) w.peak = p; }
      else windows.push({ from: p.t, to: p.t, peak: p });
    }
    overlay.anomalies = { windows, method: data.method === "zscore" ? "z-score" : data.method || "z-score", threshold, slack: last.step, summary };
    overlay.status.anomalies = "ok";
  } catch (e) {
    if (key !== overlayKey) return;
    overlay.status.anomalies = e.status === 404 ? "This server does not serve /mcp, which detection uses." : e.message;
  }
  if (state.tab === "chart") renderChart();
}

/* ── Table ────────────────────────────────────────────────────────────── */

let tablePage = 0;
const PAGE = 500;

function renderTable() {
  const panel = clear($("#panel-table"));
  let head, rows;
  if (last.kind === "sql") {
    const ti = timeColumn(last);
    head = last.columns.map((c, i) => ({ name: c.name, type: c.data_type, num: isNumeric(c.data_type) && i !== ti, time: i === ti }));
    rows = last.rows;
  } else {
    const series = chartSeries();
    if (series.length <= 8) {
      const xs = [...new Set(series.flatMap((s) => s.points.map((p) => p[0])))].sort((a, b) => b - a);
      const maps = series.map((s) => new Map(s.points));
      head = [{ name: "time", type: "local", time: true }, ...series.map((s) => ({ name: s.name, type: "float64", num: true }))];
      rows = xs.map((x) => [x, ...maps.map((m) => (m.has(x) ? m.get(x) : null))]);
    } else {
      head = [{ name: "time", type: "local", time: true }, { name: "series", type: "labels" }, { name: "value", type: "float64", num: true }];
      rows = series.flatMap((s) => s.points.map((p) => [p[0], s.name, p[1]])).sort((a, b) => b[0] - a[0]);
    }
  }
  if (!rows.length) { emptyState(panel, ICONS.table, "No rows", "The query ran and returned nothing."); return; }
  const pages = Math.ceil(rows.length / PAGE);
  tablePage = Math.min(tablePage, pages - 1);
  const slice = rows.slice(tablePage * PAGE, (tablePage + 1) * PAGE);
  const table = el("table", { class: "grid" },
    el("thead", {}, el("tr", {}, head.map((h) => el("th", { class: h.num ? "num" : "", scope: "col" }, h.name, el("span", { class: "ty" }, h.type))))),
    el("tbody", {}, slice.map((row) => el("tr", {}, row.map((v, i) => cell(v, head[i]))))));
  panel.append(el("div", { class: "table-wrap" }, table));
  if (pages > 1) {
    panel.append(el("div", { class: "pager" },
      `${fmtInt(tablePage * PAGE + 1)}–${fmtInt(Math.min(rows.length, (tablePage + 1) * PAGE))} of ${fmtInt(rows.length)}`,
      el("button", { type: "button", class: "btn", disabled: tablePage === 0, "on:click": () => { tablePage--; renderTable(); } }, "Previous"),
      el("button", { type: "button", class: "btn", disabled: tablePage >= pages - 1, "on:click": () => { tablePage++; renderTable(); } }, "Next")));
  }
}
function cell(v, h) {
  if (v === null || v === undefined) return el("td", { class: "null" }, "null");
  if (h && h.time) { const s = toSeconds(v); return el("td", { class: "time" }, s === null ? String(v) : fmtTime(s)); }
  if (h && h.num) return el("td", { class: "num" }, String(v));
  if (typeof v === "object") return el("td", {}, JSON.stringify(v));
  return el("td", {}, String(v));
}

/* ── Explain ──────────────────────────────────────────────────────────── */

let explainCache = null;

async function renderExplain() {
  const panel = $("#panel-explain");
  if (!lastRequest) return;
  if (explainCache && explainCache.req === lastRequest) { drawExplain(panel, explainCache); return; }
  emptyState(panel, ICONS.search, "Explaining…", "");
  const req = lastRequest;
  try {
    if (req.lang === "promql") {
      const t0 = performance.now();
      const res = await api("api/v1/chronix/promql/explain", {
        method: "POST", body: { query: req.query, start: req.start, end: req.end, step: req.step, analyze: true },
      });
      explainCache = { req, tree: res.tree, ms: performance.now() - t0 };
    } else {
      const res = await api("api/v1/chronix/sql", { method: "POST", body: { query: `EXPLAIN ANALYZE ${req.query}` } });
      explainCache = { req, plan: (res.rows || []).map((r) => r[r.length - 1]).join("\n\n") };
    }
    if (state.tab === "explain" && lastRequest === req) drawExplain(panel, explainCache);
  } catch (e) {
    emptyState(panel, ICONS.error, "No plan", e.body && e.body.error ? capital(e.body.error) : e.message);
  }
}
function drawExplain(panel, x) {
  clear(panel);
  if (x.plan !== undefined) {
    panel.append(el("div", { class: "explain-head" }, el("span", {}, "DataFusion plan, with the metrics of one execution")),
      el("pre", { class: "plan" }, highlightPlan(x.plan)));
    return;
  }
  // Each column against its own maximum: a node's series and its samples
  // differ by orders of magnitude.
  const max = { series: 1, samples: 1 };
  (function walk(n) {
    max.series = Math.max(max.series, n.series || 0);
    max.samples = Math.max(max.samples, n.samples || 0);
    (n.children || []).forEach(walk);
  })(x.tree);
  const node = (n) => {
    const zero = n.series === 0;
    const metric = (v, what) => el("div", { class: "metric" }, el("div", {}, fmtInt(v ?? 0), " ", el("span", {}, what)),
      el("div", { class: "bar" }, barFill(v, max[what])));
    return el("li", {}, el("div", { class: `xnode${zero ? " zero" : ""}`, title: zero ? "This node produced no series." : "" },
      el("div", { class: "what" }, el("div", { class: "op" }, n.node), el("div", { class: "src", title: n.text }, n.text)),
      metric(n.series, "series"), metric(n.samples, "samples")),
    n.children && n.children.length ? el("ul", {}, n.children.map(node)) : null);
  };
  panel.append(el("div", { class: "explain-head" }, el("span", {}, "Each node with the series and samples it produced over this range"),
    el("span", {}, fmtMs(x.ms))), el("ul", { class: "explain-tree" }, node(x.tree)));
}
function barFill(v, max) { const i = el("i"); i.style.width = `${Math.max(0, Math.min(100, ((v || 0) / max) * 100))}%`; return i; }
function highlightPlan(text) {
  const out = [];
  let at = 0;
  for (const m of text.matchAll(/(elapsed_compute|output_rows)=([^,\]]+)/g)) {
    out.push(text.slice(at, m.index), el("span", { class: "metric-hl" }, m[0]));
    at = m.index + m[0].length;
  }
  out.push(text.slice(at));
  return out;
}

/* ── Cardinality ──────────────────────────────────────────────────────── */

async function renderCardinality() {
  const panel = $("#panel-cardinality");
  emptyState(panel, ICONS.db, "Counting series…", "");
  try {
    const t0 = performance.now();
    const res = await api("api/v1/status/tsdb", { params: { limit: 10 } });
    if (state.tab !== "cardinality") return;
    drawCardinality(panel, res.data || {});
    setStatus([el("span", {}, `${fmtInt((res.data.headStats || {}).numSeries || 0)} series`), el("span", { class: "sep" }, "·"), el("span", {}, fmtMs(performance.now() - t0))]);
  } catch (e) {
    emptyState(panel, ICONS.error, "Cardinality is not available", e.message);
  }
}
function drawCardinality(panel, d) {
  clear(panel);
  const c = d.chronix || {};
  const head = d.headStats || {};
  const used = c.used ?? head.numSeries ?? 0;
  const budget = c.budget;
  const pct = budget ? used / budget : 0;
  const fill = el("i", { class: pct > 0.9 ? "full" : pct > 0.7 ? "hot" : "" });
  fill.style.width = `${Math.min(100, pct * 100)}%`;
  const span = head.minTime && head.maxTime ? `${fmtShort(head.minTime / 1000)} – ${fmtShort(head.maxTime / 1000)}` : "no data yet";
  const grid = el("div", { class: "card-grid" },
    el("div", { class: "meter-card" },
      el("div", {}, el("div", { class: "big" }, fmtInt(used), el("small", {}, "series")), el("div", { class: "lbl" }, c.scope === "namespace" ? "in this namespace" : "in the database")),
      el("div", { class: "meter" },
        el("div", { class: "legend-line" }, el("span", {}, budget ? `${pct > 0 && pct < 0.0001 ? "< 0.01" : (pct * 100).toFixed(pct < 0.01 ? 2 : 1)}${NNBSP}% of the budget` : "No series budget"),
          el("span", {}, budget ? `${fmtInt(c.headroom)} left of ${fmtInt(budget)}` : "")),
        el("div", { class: "track", title: budget ? `${fmtInt(used)} of ${fmtInt(budget)}` : "unlimited" }, fill),
        el("div", { class: "legend-line" }, el("span", {}, c.enforced === false ? "not enforced" : "enforced on every write"), el("span", {}, `${head.chunkCount ? `${fmtInt(head.chunkCount)} segments` : "all in memory"} · ${span}`)))),
    rankList("Series by measurement", d.seriesCountByMetricName),
    rankList("Values by label", d.labelValueCountByLabelName),
    rankList("Series by label pair", d.seriesCountByLabelValuePair));
  panel.append(grid);
}
function rankList(title, list) {
  const items = list || [];
  const max = Math.max(1, ...items.map((i) => i.value));
  return el("section", { class: "rank" }, el("h3", {}, title),
    items.length ? el("ol", {}, items.map((i) => el("li", {},
      el("span", { class: "n", title: i.name }, i.name), el("span", { class: "v" }, fmtInt(i.value)),
      el("span", { class: "bar" }, barFill(i.value, max)))))
      : el("p", { class: "muted" }, "Nothing yet."));
}

/* ── Copy as code ─────────────────────────────────────────────────────── */

let snippets = null;
let codeLang = "rust";

function baseUrl() { return `${location.origin}${location.pathname.replace(/\/?console\/?$/, "")}`; }
function rustStr(s) {
  if (!/["\\]/.test(s)) return `"${s}"`;
  let h = "#";
  while (s.includes(`"${h}`)) h += "#";
  return `r${h}"${s}"${h}`;
}
const shq = (s) => `'${s.replace(/'/g, `'\\''`)}'`;

/** Every placeholder a template may use, for one request. */
function codeVars(req) {
  const ns = BigInt(1e9);
  // Rust-style digit groups, as the literal would be written by hand.
  const toNs = (secs) => (BigInt(Math.round(secs * 1000)) * ns / 1000n).toString().replace(/\B(?=(\d{3})+(?!\d))/g, "_");
  const vars = {
    query_rust: rustStr(req.query), query_py: JSON.stringify(req.query), base_url: baseUrl(),
    base_url_py: JSON.stringify(baseUrl()),
    start: iso(req.start), end: iso(req.end), step: fmtDur(req.step),
    start_ns: toNs(req.start), end_ns: toNs(req.end), step_ns: toNs(req.step),
    range: `${iso(req.start)} to ${iso(req.end)}, one point every ${fmtDur(req.step)}`,
    query_param_shell: shq(`query=${req.query}`), sql_body_shell: shq(JSON.stringify({ query: req.query })),
    py_os: "", auth_py: "", auth_curl: "",
  };
  if (session.key) {
    vars.py_os = "\nimport os";
    vars.auth_py = ', api_key=os.environ["CHRONIX_TOKEN"]';
    vars.auth_curl = " \\\n  -H \"Authorization: Bearer $CHRONIX_TOKEN\"";
  }
  if (session.ns) {
    vars.auth_py += `, namespace=${JSON.stringify(session.ns)}`;
    vars.auth_curl += ` \\\n  -H ${shq(`X-Namespace: ${session.ns}`)}`;
  }
  return vars;
}
function render(template, vars) { return template.replace(/\{\{(\w+)\}\}/g, (m, k) => (k in vars ? vars[k] : m)); }

async function openCode() {
  if (!lastRequest) { toast("Run a query first."); return; }
  if (!snippets) snippets = await fetch(new URL("console/snippets.json", location.href)).then((r) => r.json()).catch(() => null);
  if (!snippets) { toast("The snippet templates did not load."); return; }
  drawCode();
  $("#code-dialog").showModal();
}
function drawCode() {
  $$("#code-dialog [data-code]").forEach((b) => b.setAttribute("aria-selected", String(b.dataset.code === codeLang)));
  const req = lastRequest;
  const lang = req.lang === "sql" ? "sql" : "promql";
  $("#code-out").textContent = render(snippets[codeLang][lang], codeVars(req));
  $("#code-note").textContent = {
    rust: "The embedded API, reading a data directory in your own process — no server.",
    python: "The chronix-client package, against this server.",
    curl: session.key ? "The same request this page sent. Set CHRONIX_TOKEN to your key first." : "The same request this page sent.",
  }[codeLang];
}

/* ── Range, theme, dialogs, toast ─────────────────────────────────────── */

function setRange(range, from, to) {
  if (range === "custom") { state.range = "custom"; state.from = from; state.to = to; }
  else { state.range = range; lastPreset = range; state.from = state.to = null; }
  syncRange();
  if (state.q) run(true); else writeFragment(true);
}
function syncRange() {
  $$(".range [data-range]").forEach((b) => b.setAttribute("aria-pressed", String(b.dataset.range === state.range)));
  const custom = $("#range-custom");
  custom.hidden = state.range !== "custom";
  if (state.range === "custom") custom.textContent = `${fmtShort(state.from)} – ${fmtShort(state.to)}`;
  $("#step").value = state.step;
}
function shiftRange(dir) {
  const { start, end } = window_();
  const w = end - start;
  setRange("custom", start + dir * w, end + dir * w);
}

const THEMES = ["system", "light", "dark"];
function applyTheme(choice) {
  if (choice === "system") delete document.documentElement.dataset.theme;
  else document.documentElement.dataset.theme = choice;
  $("#theme-btn").setAttribute("aria-label", `Theme: ${choice}`);
  $("#theme-btn").title = `Theme: ${choice}`;
  if (plot) renderChart();
}
function loadTheme() { try { return localStorage.getItem("chronix.theme") || "system"; } catch { return "system"; } }
function saveTheme(t) { try { localStorage.setItem("chronix.theme", t); } catch { /* private mode */ } }

function needKey(message) {
  const d = $("#key-dialog");
  const err = $("#key-error");
  err.textContent = message || (session.key ? "The server refused this key." : "This server needs an API key.");
  err.hidden = false;
  if (!d.open) openKey();
}
function openKey() {
  $("#key-input").value = session.key;
  $("#ns-input").value = session.ns;
  $("#key-dialog").showModal();
  $("#key-input").focus();
}
function saveKey() {
  session.key = $("#key-input").value.trim();
  session.ns = $("#ns-input").value.trim();
  $("#key-dot").hidden = !session.key;
  $("#key-error").hidden = true;
  $("#key-dialog").close();
  catalogue.values.clear();
  writeFragment(false);
  loadCatalogue();
  if (state.q) run(false);
}

let toastTimer = 0;
function toast(text) {
  const t = $("#toast");
  t.textContent = text; t.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { t.hidden = true; }, 1800);
}
async function copyText(text) {
  try { await navigator.clipboard.writeText(text); toast("Copied"); } catch { toast("Copying is blocked here; select the text instead."); }
}

/* ── Wiring ───────────────────────────────────────────────────────────── */

function typing(e) {
  const t = e.target;
  return t instanceof HTMLElement && (t.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName));
}

function wire() {
  const ed = $("#editor");
  ed.addEventListener("input", () => {
    autosize();
    clearTimeout(compTimer);
    compTimer = setTimeout(() => complete(false), 90);
  });
  ed.addEventListener("keydown", (e) => {
    const mod = e.metaKey || e.ctrlKey;
    if (mod && e.key === "Enter") { e.preventDefault(); run(true); return; }
    if (mod && e.key === " ") { e.preventDefault(); complete(true); return; }
    if (comp.open) {
      if (e.key === "ArrowDown" || e.key === "ArrowUp") {
        e.preventDefault();
        comp.index = (comp.index + (e.key === "ArrowDown" ? 1 : comp.items.length - 1)) % comp.items.length;
        renderComplete(); return;
      }
      if (e.key === "Tab" || e.key === "Enter") { e.preventDefault(); accept(); return; }
      if (e.key === "Escape") { e.preventDefault(); closeComplete(); return; }
    }
    if (e.key === "Escape" && inflight) { e.preventDefault(); inflight.abort(); return; }
    if (e.key === "Enter" && !e.shiftKey && state.lang === "promql") { e.preventDefault(); run(true); }
  });
  ed.addEventListener("blur", () => setTimeout(closeComplete, 120));

  $("#run").addEventListener("click", () => run(true));
  $$(".editor-bar [data-lang]").forEach((b) => b.addEventListener("click", () => { setLang(b.dataset.lang); ed.focus(); }));
  $$(".tabs [data-tab]").forEach((b) => b.addEventListener("click", () => setTab(b.dataset.tab, true)));
  $$(".range [data-range]").forEach((b) => b.addEventListener("click", () => setRange(b.dataset.range)));
  $("#range-custom").addEventListener("click", () => setRange(lastPreset));
  $("#step").addEventListener("change", (e) => { state.step = e.target.value; if (state.q) run(true); else writeFragment(true); });
  $("#filter").addEventListener("input", renderRail);
  for (const [id, keyName] of [["fc", "fc"], ["an", "an"]]) {
    $(`#${id}`).addEventListener("change", (e) => {
      state[keyName] = e.target.checked; writeFragment(false);
      const target = overlayTarget();
      if (target) { overlayKey = ""; refreshOverlays(target); if (state.tab !== "chart") setTab("chart", false); }
      if (!state.fc && !state.an && plot) renderChart();
    });
  }
  $("#code-btn").addEventListener("click", openCode);
  $$("#code-dialog [data-code]").forEach((b) => b.addEventListener("click", () => { codeLang = b.dataset.code; drawCode(); }));
  $("#code-copy").addEventListener("click", () => copyText($("#code-out").textContent));
  $("#code-close").addEventListener("click", () => $("#code-dialog").close());
  $("#key-btn").addEventListener("click", () => { $("#key-error").hidden = true; openKey(); });
  $("#key-cancel").addEventListener("click", () => $("#key-dialog").close());
  $("#key-clear").addEventListener("click", () => { $("#key-input").value = ""; saveKey(); });
  $("#key-form").addEventListener("submit", (e) => { e.preventDefault(); saveKey(); });
  $("#help-btn").addEventListener("click", () => $("#help-dialog").showModal());
  $("#help-close").addEventListener("click", () => $("#help-dialog").close());
  $("#theme-btn").addEventListener("click", () => {
    const cur = loadTheme();
    const next = THEMES[(THEMES.indexOf(cur) + 1) % THEMES.length];
    saveTheme(next); applyTheme(next);
  });
  window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => { if (plot) renderChart(); });
  window.addEventListener("popstate", () => { readFragment(); syncAll(); if (state.q) run(false); });

  document.addEventListener("keydown", (e) => {
    if (document.querySelector("dialog[open]")) return;
    if (e.key === "Escape" && inflight) { inflight.abort(); return; }
    if (typing(e) || e.metaKey || e.ctrlKey || e.altKey) return;
    if (e.key === "/") { e.preventDefault(); ed.focus(); }
    else if (e.key === "?") { e.preventDefault(); $("#help-dialog").showModal(); }
    else if (e.key === "[") shiftRange(-1);
    else if (e.key === "]") shiftRange(1);
    else if (/^[1-4]$/.test(e.key)) setTab(TABS[Number(e.key) - 1], true);
  });
  document.addEventListener("pointerleave", () => { $("#tooltip").hidden = true; });
  $("#panel-chart").addEventListener("pointerleave", () => { $("#tooltip").hidden = true; });
}

function syncAll() {
  setLang(state.lang);
  $$('.editor-bar [data-lang]').forEach((b) => b.setAttribute("aria-selected", String(b.dataset.lang === state.lang)));
  setQuery(state.q);
  syncRange();
  $$(".tabs [data-tab]").forEach((b) => b.setAttribute("aria-selected", String(b.dataset.tab === state.tab)));
  for (const t of TABS) $(`#panel-${t}`).hidden = t !== state.tab;
  $("#key-dot").hidden = !session.key;
}

function init() {
  applyTheme(loadTheme());
  for (const k of $$("kbd.mod, #mod-key")) k.textContent = isMac ? "⌘" : "Ctrl";
  const notes = readFragment();
  state.lang = state.lang === "sql" ? "sql" : "promql";
  const lang = state.lang;
  state.lang = "";
  setLang(lang);
  syncAll();
  wire();
  if (notes.length) append($("#diag"), notes.map((n) => note("info", el("div", { class: "msg" }, n))));
  loadCatalogue();
  if (state.q) run(false);
  else if (state.tab === "cardinality") renderCardinality();
  else renderWelcome();
}

init();
