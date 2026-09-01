// Documentation search.
//
// Progressive enhancement: the markup ships hidden and this script reveals it,
// so a reader without JavaScript never sees a control that cannot work. The
// index and elasticlunr are loaded with `defer`, so this runs after both.
(function () {
  "use strict";

  var box = document.getElementById("search-box");
  var input = document.getElementById("search-input");
  var out = document.getElementById("search-results");
  if (!box || !input || !out) return;
  if (typeof elasticlunr === "undefined" || typeof window.searchIndex === "undefined") return;

  var index = elasticlunr.Index.load(window.searchIndex);
  box.hidden = false;

  var MAX = 8;

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

  var timer;
  input.addEventListener("input", function () {
    clearTimeout(timer);
    var q = input.value.trim();
    if (q.length < 2) return close();
    timer = setTimeout(function () {
      var terms = q.toLowerCase().split(/\s+/);
      var results = index.search(q, {
        bool: "AND",
        expand: true,
        fields: { title: { boost: 3 }, description: { boost: 2 }, body: { boost: 1 } },
      });
      render(results, terms);
    }, 90);
  });

  // Escape closes; "/" focuses, the shortcut every docs site has.
  input.addEventListener("keydown", function (e) {
    if (e.key === "Escape") { close(); input.blur(); }
  });
  document.addEventListener("keydown", function (e) {
    if (e.key === "/" && document.activeElement !== input) {
      e.preventDefault();
      input.focus();
    }
  });
  document.addEventListener("click", function (e) {
    if (!box.contains(e.target)) close();
  });
})();
