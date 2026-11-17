#!/usr/bin/env bash
# Regenerate every screenshot on the documentation site from a real chronixd.
#
#   scripts/screenshots.sh              # all shots in scripts/screenshots/shots.tsv
#   scripts/screenshots.sh --only console-   # shots whose name starts with the prefix
#   scripts/screenshots.sh --skip console-forecast   # all but the shots with that prefix
#   scripts/screenshots.sh --keep       # leave everything running and print the URLs
#
# Starts `chronixd --demo`, a Prometheus scraping its /metrics, and a Grafana
# provisioned with the bundled dashboards (as files, the way the guide says), drives
# real traffic for the self-monitoring panels, captures with headless Chrome, and
# writes 2× WebP under 200 KB to site/static/img/. The images are committed; re-run this
# when a UI changes. Needs: docker, Google Chrome, cwebp, curl, python3.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD

GRAFANA_IMAGE=grafana/grafana:13.3.1
PROMETHEUS_IMAGE=prom/prometheus:v3.16.0
MAX_BYTES=$((200 * 1024))
OUT=site/static/img

ONLY=""; SKIP=""; KEEP=0
while [ $# -gt 0 ]; do
  case "$1" in
    --only) ONLY="$2"; shift 2 ;;
    --skip) SKIP="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    *) echo "usage: $0 [--only PREFIX] [--skip PREFIX] [--keep]" >&2; exit 2 ;;
  esac
done

CHROME=${CHROME:-"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"}
[ -x "$CHROME" ] || CHROME=$(command -v google-chrome || command -v chromium || true)
for tool in docker cwebp curl python3; do
  command -v "$tool" >/dev/null || { echo "screenshots: $tool is required" >&2; exit 2; }
done
[ -n "$CHROME" ] && [ -x "$CHROME" ] || { echo "screenshots: Google Chrome is required (set CHROME)" >&2; exit 2; }
docker info >/dev/null 2>&1 || { echo "screenshots: docker is not running" >&2; exit 2; }

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])'; }
wait_for() { # url, what
  for _ in $(seq 1 120); do curl -sf -o /dev/null "$1" && return 0; sleep 0.5; done
  echo "screenshots: $2 did not come up at $1" >&2; return 1
}

WORK=$(mktemp -d)
RUN=chronix-shots-$$
DEMO_PID=""; LOAD_PID=""
cleanup() {
  [ "$KEEP" = 1 ] && return
  [ -n "$LOAD_PID" ] && kill "$LOAD_PID" 2>/dev/null || true
  [ -n "$DEMO_PID" ] && kill "$DEMO_PID" 2>/dev/null && wait "$DEMO_PID" 2>/dev/null || true
  docker rm -f "$RUN-grafana" "$RUN-prometheus" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

# ── chronixd --demo ─────────────────────────────────────────────────────────
if [ -z "${CHRONIXD:-}" ]; then
  echo "building chronixd…"
  cargo build -q -p chronixd
  CHRONIXD=$ROOT/target/debug/chronixd
fi
DEMO_PORT=$(free_port)
"$CHRONIXD" --demo --bind "127.0.0.1:$DEMO_PORT" >"$WORK/demo.log" 2>&1 &
DEMO_PID=$!
wait_for "http://127.0.0.1:$DEMO_PORT/health" "chronixd --demo"
CHRONIX=http://127.0.0.1:$DEMO_PORT

# Containers reach the loopback-bound demo through Docker Desktop's host gateway; on
# Linux they share the host network instead, so both see 127.0.0.1.
if [ "$(uname)" = Linux ]; then
  NET=(--network host); HOSTNAME_FOR_CONTAINERS=127.0.0.1
else
  NET=(); HOSTNAME_FOR_CONTAINERS=host.docker.internal
fi

# ── Prometheus, scraping chronix's own metrics ──────────────────────────────
PROM_PORT=$(free_port)
sed "s|__CHRONIX__|$HOSTNAME_FOR_CONTAINERS:$DEMO_PORT|" scripts/screenshots/prometheus.yml >"$WORK/prometheus.yml"
if [ "$(uname)" = Linux ]; then PROM_PUBLISH=(); PROM_ARGS=(--web.listen-address=":$PROM_PORT"); else PROM_PUBLISH=(-p "127.0.0.1:$PROM_PORT:9090"); PROM_ARGS=(); fi
docker run -d --name "$RUN-prometheus" ${NET[@]+"${NET[@]}"} ${PROM_PUBLISH[@]+"${PROM_PUBLISH[@]}"} \
  -v "$WORK/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  "$PROMETHEUS_IMAGE" --config.file=/etc/prometheus/prometheus.yml ${PROM_ARGS[@]+"${PROM_ARGS[@]}"} >/dev/null
wait_for "http://127.0.0.1:$PROM_PORT/-/ready" "Prometheus"

# ── Grafana, provisioned from files ─────────────────────────────────────────
GRAFANA_PORT=$(free_port)
mkdir -p "$WORK/provisioning/datasources" "$WORK/provisioning/dashboards" "$WORK/dashboards"
PROM_FROM_GRAFANA=$HOSTNAME_FOR_CONTAINERS:$PROM_PORT
sed -e "s|__CHRONIX__|$HOSTNAME_FOR_CONTAINERS:$DEMO_PORT|" -e "s|__PROMETHEUS__|$PROM_FROM_GRAFANA|" \
  scripts/screenshots/grafana/datasources.yaml >"$WORK/provisioning/datasources/chronix.yaml"
cp scripts/screenshots/grafana/dashboards.yaml "$WORK/provisioning/dashboards/chronix.yaml"
cp dashboards/*.json scripts/screenshots/grafana/demo-dashboard.json "$WORK/dashboards/"
if [ "$(uname)" = Linux ]; then GF_PUBLISH=(); GF_ENV=(-e "GF_SERVER_HTTP_PORT=$GRAFANA_PORT"); else GF_PUBLISH=(-p "127.0.0.1:$GRAFANA_PORT:3000"); GF_ENV=(); fi
docker run -d --name "$RUN-grafana" ${NET[@]+"${NET[@]}"} ${GF_PUBLISH[@]+"${GF_PUBLISH[@]}"} ${GF_ENV[@]+"${GF_ENV[@]}"} \
  -e GF_AUTH_ANONYMOUS_ENABLED=true -e GF_AUTH_ANONYMOUS_ORG_ROLE=Admin \
  -e GF_AUTH_DISABLE_LOGIN_FORM=true -e GF_NEWS_NEWS_FEED_ENABLED=false \
  -e GF_ANALYTICS_REPORTING_ENABLED=false -e GF_ANALYTICS_CHECK_FOR_UPDATES=false \
  -e GF_USERS_DEFAULT_THEME=light \
  -v "$WORK/provisioning:/etc/grafana/provisioning:ro" \
  -v "$WORK/dashboards:/var/lib/grafana/dashboards:ro" \
  "$GRAFANA_IMAGE" >/dev/null
GRAFANA=http://127.0.0.1:$GRAFANA_PORT
wait_for "$GRAFANA/api/health" "Grafana"

# ── Capture helpers ─────────────────────────────────────────────────────────
mkdir -p "$OUT"
capture() { # url theme WxH out.png
  local scheme=(--blink-settings=preferredColorScheme=1)
  [ "$2" = dark ] && scheme=(--force-dark-mode --blink-settings=preferredColorScheme=0)
  "$CHROME" --headless=new --disable-gpu --hide-scrollbars --no-first-run \
    --user-data-dir="$WORK/chrome" "${scheme[@]}" --window-size="$3" --force-device-scale-factor=2 \
    --virtual-time-budget=20000 --screenshot="$4" "$1" >/dev/null 2>&1
}
explore_url() { # expr theme
  python3 - "$1" "$2" <<'PY'
import json, sys, urllib.parse
expr, theme = sys.argv[1], sys.argv[2]
panes = {"a": {"datasource": "chronix", "queries": [{"refId": "A", "expr": expr,
         "datasource": {"type": "prometheus", "uid": "chronix"}}],
         "range": {"from": "now-2d", "to": "now"}}}
print("/explore?schemaVersion=1&orgId=1&theme=" + theme + "&panes=" + urllib.parse.quote(json.dumps(panes, separators=(",", ":"))))
PY
}
wanted() { # name
  case "$1" in ''|'#'*) return 1 ;; esac
  [ -n "$ONLY" ] && [ "${1#"$ONLY"}" = "$1" ] && return 1
  [ -n "$SKIP" ] && [ "${1#"$SKIP"}" != "$1" ] && return 1
  return 0
}
fail=0
shoot() { # target: capture every wanted row for one target
  while IFS=$'\t' read -r name target path theme size; do
    wanted "$name" || continue
    [ "$target" = "$1" ] || continue
    if [ "$target" = grafana ]; then
      if [ "${path#EXPLORE:}" != "$path" ]; then
        url=$GRAFANA$(explore_url "${path#EXPLORE:}" "$theme")
      else
        sep='?'; [ "${path#*\?}" != "$path" ] && sep='&'
        url="$GRAFANA$path${sep}orgId=1&theme=$theme&kiosk"
      fi
    else
      url="$CHRONIX/console#$path"
    fi
    png=$WORK/$name.png
    capture "$url" "$theme" "${size/x/,}" "$png"
    cwebp -quiet -q 82 -m 6 "$png" -o "$OUT/$name.webp"
    bytes=$(wc -c <"$OUT/$name.webp" | tr -d ' ')
    if [ "$bytes" -ge "$MAX_BYTES" ]; then
      cwebp -quiet -q 70 -m 6 "$png" -o "$OUT/$name.webp"; bytes=$(wc -c <"$OUT/$name.webp" | tr -d ' ')
    fi
    if [ "$bytes" -ge "$MAX_BYTES" ]; then echo "  FAIL  $name: $bytes bytes ≥ 200 KB" >&2; fail=1
    else printf '  %-28s %6s KB\n' "$name.webp" "$((bytes / 1024))"; fi
  done <scripts/screenshots/shots.tsv
}

# ── The console first, on the demo's data alone ─────────────────────────────
if [ "$KEEP" = 0 ]; then
  if curl -sf -o /dev/null "$CHRONIX/console"; then shoot console
  else echo "  skip  console shots (this chronixd has no console)"; fi
fi

# ── Then real traffic, so the self-monitoring rate() panels have something ──
# It writes its own `traffic` measurement, which is why the console is captured
# before it starts.
(
  i=0
  while :; do
    i=$((i + 1))
    curl -s -o /dev/null "$CHRONIX/write" --data-binary "traffic,source=screenshots value=$i" || true
    curl -s -o /dev/null "$CHRONIX/api/v1/query" --data-urlencode 'query=rate(pv_power_w[5m])' || true
    curl -s -o /dev/null "$CHRONIX/api/v1/chronix/sql" -H 'content-type: application/json' \
      -d '{"query":"SELECT device, avg(power_w) FROM pv GROUP BY device"}' || true
    sleep 0.5
  done
) &
LOAD_PID=$!

if [ "$KEEP" = 1 ]; then
  echo "chronix    $CHRONIX/console"
  echo "grafana    $GRAFANA"
  echo "prometheus http://127.0.0.1:$PROM_PORT"
  echo "--keep: left running; stop with: docker rm -f $RUN-grafana $RUN-prometheus; kill $DEMO_PID $LOAD_PID"
  exit 0
fi

# Let Prometheus collect a few minutes of scrapes before the dashboards are drawn.
dashboards=0
while IFS=$'\t' read -r name target path _; do
  wanted "$name" && [ "$target" = grafana ] && [ "${path#/d/chronix-}" != "$path" ] \
    && [ "${path#/d/chronix-demo}" = "$path" ] && dashboards=1
done <scripts/screenshots/shots.tsv
if [ "$dashboards" = 1 ]; then
  echo "collecting 150 s of traffic for the self-monitoring dashboards…"
  sleep 150
fi
shoot grafana

# ── Every captured dashboard must have drawn data ───────────────────────────
for uid in $(grep -v '^#' scripts/screenshots/shots.tsv | grep -o '/d/chronix-[a-z-]*' | sort -u | sed 's|/d/||'); do
  name=$(grep -m1 "/d/$uid" scripts/screenshots/shots.tsv | cut -f1)
  [ -n "$ONLY" ] && [ "${name#"$ONLY"}" = "$name" ] && continue
  python3 - "$GRAFANA" "$uid" <<'PY' || fail=1
import json, sys, urllib.request
base, uid = sys.argv[1], sys.argv[2]
dash = json.load(urllib.request.urlopen(f"{base}/api/dashboards/uid/{uid}"))["dashboard"]
with_data, empty = 0, []
for p in dash.get("panels", []):
    for t in p.get("targets", [])[:1]:
        ds = (t.get("datasource") or p.get("datasource") or {}).get("uid", "")
        ds = "prometheus" if ds in ("${datasource}", "") else ds
        body = {"from": "now-5m" if uid != "chronix-demo" else "now-2d", "to": "now",
                "queries": [{"refId": "A", "expr": t.get("expr", ""), "datasource": {"uid": ds},
                             "intervalMs": 15000, "maxDataPoints": 200}]}
        req = urllib.request.Request(f"{base}/api/ds/query", json.dumps(body).encode(),
                                     {"content-type": "application/json"})
        frames = json.load(urllib.request.urlopen(req))["results"]["A"].get("frames", [])
        rows = sum(len((f.get("data", {}).get("values") or [[]])[0]) for f in frames)
        if rows: with_data += 1
        else: empty.append(p.get("title", "?"))
for title in empty: print(f"  warn  {uid}: panel \"{title}\" has no data under the demo")
if with_data == 0:
    print(f"  FAIL  {uid}: no panel has data", file=sys.stderr); sys.exit(1)
PY
done

[ "$fail" = 0 ] || { echo "screenshots: failed" >&2; exit 1; }
echo "screenshots: done"
