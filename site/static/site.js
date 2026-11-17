// Chronix site: theme, copy buttons, code tabs, TOC scroll-spy, search.
// Everything here is progressive: without it the site reads and navigates fine.
(function () {
  "use strict";
  var doc = document.documentElement;
  function store(k, v) { try { if (v === null) localStorage.removeItem(k); else localStorage.setItem(k, v); } catch (e) {} }
  function load(k) { try { return localStorage.getItem(k); } catch (e) { return null; } }

  // ── Sidebar: open in the HTML (works without JS), collapsed on phones so
  // the page's content comes before twelve navigation links.
  if (window.matchMedia("(max-width: 899px)").matches) {
    document.querySelectorAll(".sidebar details[open], nav details[open]").forEach(function (d) {
      d.removeAttribute("open");
    });
  }

  // ── Theme: system → light → dark ──────────────────────────────────────
  var order = ["system", "light", "dark"];
  var themeBtn = document.querySelector("[data-theme-toggle]");
  function applyTheme(choice) {
    doc.setAttribute("data-theme-choice", choice);
    if (choice === "system") doc.removeAttribute("data-theme"); else doc.setAttribute("data-theme", choice);
    if (window.__hl) window.__hl();
    if (themeBtn) {
      var next = order[(order.indexOf(choice) + 1) % 3];
      themeBtn.setAttribute("aria-label", "Theme: " + choice + ". Switch to " + next);
      themeBtn.title = "Theme: " + choice;
    }
  }
  if (themeBtn) {
    applyTheme(doc.getAttribute("data-theme-choice") || "system");
    themeBtn.addEventListener("click", function () {
      var next = order[(order.indexOf(doc.getAttribute("data-theme-choice") || "system") + 1) % 3];
      store("chronix.theme", next === "system" ? null : next);
      applyTheme(next);
    });
  }

  // ── Copy ──────────────────────────────────────────────────────────────
  function copy(text, btn) {
    var done = function () {
      var old = btn.textContent; btn.textContent = "Copied"; btn.classList.add("done");
      setTimeout(function () { btn.textContent = old; btn.classList.remove("done"); }, 1400);
    };
    if (navigator.clipboard) navigator.clipboard.writeText(text).then(done, function () {});
  }
  document.querySelectorAll("pre > code").forEach(function (code) {
    var pre = code.parentNode;
    var wrap = document.createElement("div");
    wrap.className = "code-wrap";
    pre.parentNode.insertBefore(wrap, pre);
    wrap.appendChild(pre);
    var b = document.createElement("button");
    b.type = "button"; b.className = "copy-btn"; b.textContent = "Copy";
    b.setAttribute("aria-label", "Copy code");
    b.addEventListener("click", function () { copy(code.innerText.replace(/\n$/, ""), b); });
    wrap.appendChild(b);
  });
  document.querySelectorAll("[data-copy]").forEach(function (b) {
    b.addEventListener("click", function () { copy(b.getAttribute("data-copy"), b); });
  });

  // ── Tabs: label from the language, choice remembered across pages ─────
  var names = { rust: "Rust", python: "Python", shellscript: "Shell", bash: "Shell", sh: "Shell", shell: "Shell",
                sql: "SQL", promql: "PromQL", toml: "TOML", json: "JSON", yaml: "YAML" };
  var groups = [];
  function select(group, label, focus) {
    group.tabs.forEach(function (t, i) {
      var on = t.textContent === label;
      t.setAttribute("aria-selected", on); t.tabIndex = on ? 0 : -1;
      group.panels[i].hidden = !on;
      if (on && focus) t.focus();
    });
  }
  document.querySelectorAll("[data-tabs]").forEach(function (box, n) {
    var panels = Array.prototype.slice.call(box.querySelectorAll(":scope > .code-wrap"));
    if (panels.length < 2) return;
    var list = document.createElement("div");
    list.setAttribute("role", "tablist");
    var group = { tabs: [], panels: panels };
    panels.forEach(function (p, i) {
      var code = p.querySelector("code[data-lang]");
      var lang = code ? code.getAttribute("data-lang") : "text";
      var t = document.createElement("button");
      t.type = "button"; t.setAttribute("role", "tab");
      t.id = "tab-" + n + "-" + i; p.id = "panel-" + n + "-" + i;
      t.setAttribute("aria-controls", p.id); p.setAttribute("role", "tabpanel"); p.setAttribute("aria-labelledby", t.id);
      t.textContent = names[lang] || lang;
      t.addEventListener("click", function () {
        store("chronix.lang", t.textContent);
        groups.forEach(function (g) { if (g.tabs.some(function (x) { return x.textContent === t.textContent; })) select(g, t.textContent); });
      });
      t.addEventListener("keydown", function (e) {
        var d = e.key === "ArrowRight" ? 1 : e.key === "ArrowLeft" ? -1 : 0;
        if (!d) return;
        e.preventDefault();
        select(group, group.tabs[(i + d + group.tabs.length) % group.tabs.length].textContent, true);
      });
      group.tabs.push(t); list.appendChild(t);
    });
    box.insertBefore(list, box.firstChild);
    box.classList.add("ready");
    groups.push(group);
    var want = load("chronix.lang");
    var has = want && group.tabs.some(function (t) { return t.textContent === want; });
    select(group, has ? want : group.tabs[0].textContent);
  });

  // ── TOC scroll-spy ────────────────────────────────────────────────────
  var toc = document.querySelector("[data-toc]");
  if (toc && "IntersectionObserver" in window) {
    var links = {};
    toc.querySelectorAll("a[href*='#']").forEach(function (a) { links[decodeURIComponent(a.hash.slice(1))] = a; });
    var heads = Object.keys(links).map(function (id) { return document.getElementById(id); }).filter(Boolean);
    var current = null;
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) { if (e.isIntersecting) current = e.target.id; });
      if (!current) return;
      Object.keys(links).forEach(function (id) { links[id].classList.toggle("active", id === current); });
    }, { rootMargin: "-70px 0px -70% 0px" });
    heads.forEach(function (h) { io.observe(h); });
  }

  // ── Search: Pagefind, loaded on first open ────────────────────────────
  var dialog = document.getElementById("search");
  if (!dialog || !dialog.showModal) return;
  var input = dialog.querySelector("input");
  var results = dialog.querySelector(".search-results");
  var pf = null, active = -1, seq = 0;
  function msg(text) { results.innerHTML = ""; var li = document.createElement("li"); li.className = "search-empty"; li.textContent = text; results.appendChild(li); }
  function open() {
    if (dialog.open) return;
    dialog.showModal(); input.select();
    if (!pf) {
      pf = import(dialog.getAttribute("data-pagefind")).then(function (m) { return m; }, function () {
        msg("The search index is built at deploy time (site/build.sh). Browse the sections instead.");
        return null;
      });
    }
  }
  function mark(i) {
    var items = results.querySelectorAll("a");
    items.forEach(function (a, j) { a.setAttribute("aria-selected", j === i); });
    active = i;
    if (items[i]) items[i].scrollIntoView({ block: "nearest" });
  }
  input.addEventListener("input", function () {
    var q = input.value.trim(), mine = ++seq;
    if (!q) { results.innerHTML = ""; return; }
    pf.then(function (m) {
      if (!m) return;
      return m.debouncedSearch(q, {}, 120).then(function (r) {
        if (!r || mine !== seq) return;
        return Promise.all(r.results.slice(0, 8).map(function (x) { return x.data(); })).then(function (data) {
          if (mine !== seq) return;
          if (!data.length) { msg("No results for “" + q + "”."); return; }
          results.innerHTML = "";
          data.forEach(function (d, i) {
            var li = document.createElement("li");
            var a = document.createElement("a");
            a.href = d.url; a.setAttribute("role", "option"); a.id = "sr-" + i;
            var t = document.createElement("span"); t.className = "r-title"; t.textContent = d.meta.title || d.url;
            var ex = document.createElement("span"); ex.className = "r-excerpt";
            ex.innerHTML = d.excerpt; // Pagefind escapes the text and adds only <mark>
            a.appendChild(t); a.appendChild(ex); li.appendChild(a); results.appendChild(li);
          });
          mark(0);
        });
      });
    });
  });
  input.addEventListener("keydown", function (e) {
    var n = results.querySelectorAll("a").length;
    if (e.key === "ArrowDown" && n) { e.preventDefault(); mark((active + 1) % n); }
    else if (e.key === "ArrowUp" && n) { e.preventDefault(); mark((active - 1 + n) % n); }
    else if (e.key === "Enter") { var a = results.querySelectorAll("a")[active]; if (a) { e.preventDefault(); location.href = a.href; } }
  });
  dialog.addEventListener("click", function (e) { if (e.target === dialog) dialog.close(); });
  document.querySelectorAll("[data-search-open]").forEach(function (b) { b.addEventListener("click", open); });
  document.addEventListener("keydown", function (e) {
    var typing = /^(INPUT|TEXTAREA|SELECT)$/.test(e.target.tagName) || e.target.isContentEditable;
    if ((e.key === "k" && (e.metaKey || e.ctrlKey)) || (e.key === "/" && !typing)) { e.preventDefault(); open(); }
  });
})();
