// Documentation search.
//
// Progressive enhancement: the markup ships hidden and this script reveals it,
// so a reader without JavaScript never sees a control that cannot work.
//
// ## The index is loaded on first use, not on every page view
//
// `search_index.en.js` is **3.4 MB** — elasticlunr indexes the full text of
// every page, and this is a reference site. It used to be a `<script defer>`
// in the document head, so every visitor to every page downloaded and parsed
// it whether or not they ever typed anything: 210× the weight of the page
// they came for, and a main-thread parse that shows up in interaction
// latency. `defer` keeps it off the critical rendering path; it does not make
// it free.
//
// So the box is revealed immediately — it must look usable — and the index is
// fetched the first time the reader focuses it. The fetch is also started on
// the intent signals that precede focus (a pointer over the box, the `/`
// shortcut), so by the time the first keystroke lands it is usually there.
(function () {
  "use strict";

  var box = document.getElementById("search-box");
  var input = document.getElementById("search-input");
  var out = document.getElementById("search-results");
  if (!box || !input || !out) return;

  // The index URL is stamped on the container by the template, because a
  // script in `static/` cannot know the site's base URL.
  var indexUrl = box.getAttribute("data-index");
  var lunrUrl = box.getAttribute("data-lunr");
  if (!indexUrl || !lunrUrl) return;

  box.hidden = false;

  var index = null;
  var loading = null;
  var MAX = 8;

  function loadScript(src) {
    return new Promise(function (resolve, reject) {
      var el = document.createElement("script");
      el.src = src;
      el.onload = resolve;
      el.onerror = function () { reject(new Error("failed to load " + src)); };
      document.head.appendChild(el);
    });
  }

  // Resolves when the index is ready. Called more than once; the promise is
  // memoised so the 3.4 MB is fetched exactly once.
  function ensureIndex() {
    if (loading) return loading;
    input.setAttribute("aria-busy", "true");
    loading = loadScript(lunrUrl)
      .then(function () { return loadScript(indexUrl); })
      .then(function () {
        index = elasticlunr.Index.load(window.searchIndex);
        input.removeAttribute("aria-busy");
      })
      .catch(function (e) {
        input.removeAttribute("aria-busy");
        out.innerHTML = '<p class="search-empty">Search is unavailable.</p>';
        out.classList.add("open");
        throw e;
      });
    return loading;
  }

  function snippet(body, terms) {
    if (!body) return "";
    var lower = body.toLowerCase();
    var at = -1;
    for (var i = 0; i < terms.length && at < 0; i++) at = lower.indexOf(terms[i]);
    if (at < 0) at = 0;
    var from = Math.max(0, at - 60);
    var text = body.slice(from, from + 180).replace(/\s+/g, " ").trim();
    return (from > 0 ? "…" : "") + text + "…";
  }

  function esc(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function render(results, terms) {
    if (!results.length) {
      out.innerHTML = '<p class="search-empty">No matches.</p>';
      out.classList.add("open");
      return;
    }
    var html = results.slice(0, MAX).map(function (r) {
      var d = r.doc;
      return (
        '<a class="search-hit" role="option" href="' + esc(d.id) + '">' +
        "<strong>" + esc(d.title) + "</strong>" +
        "<span>" + esc(snippet(d.body, terms)) + "</span></a>"
      );
    }).join("");
    out.innerHTML = html;
    out.classList.add("open");
  }

  function close() {
    out.classList.remove("open");
    out.innerHTML = "";
  }

  function run() {
    var q = input.value.trim();
    if (q.length < 2) return close();
    ensureIndex().then(function () {
      // The reader may have typed on while the index loaded.
      var live = input.value.trim();
      if (live.length < 2) return close();
      var terms = live.toLowerCase().split(/\s+/);
      render(
        index.search(live, {
          bool: "AND",
          expand: true,
          fields: { title: { boost: 3 }, description: { boost: 2 }, body: { boost: 1 } },
        }),
        terms
      );
    }, function () { /* the catch above has already reported it */ });
  }

  // Warm the index on intent, so the first keystroke rarely waits for it.
  ["focus", "pointerenter"].forEach(function (ev) {
    box.addEventListener(ev, function () { ensureIndex().catch(function () {}); }, { once: true, capture: true });
  });

  var timer;
  input.addEventListener("input", function () {
    clearTimeout(timer);
    timer = setTimeout(run, 90);
  });

  // Escape closes; "/" focuses, the shortcut every docs site has.
  input.addEventListener("keydown", function (e) {
    if (e.key === "Escape") { close(); input.blur(); }
  });
  document.addEventListener("keydown", function (e) {
    if (e.key === "/" && document.activeElement !== input) {
      e.preventDefault();
      ensureIndex().catch(function () {});
      input.focus();
    }
  });
  document.addEventListener("click", function (e) {
    if (!box.contains(e.target)) close();
  });
})();
