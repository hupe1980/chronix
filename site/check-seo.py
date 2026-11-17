#!/usr/bin/env python3
"""Check the built site for what search engines and readers depend on.

    python3 check-seo.py public            # every built page
    python3 check-seo.py --strict public   # description length is an error on every page
    python3 check-seo.py --contrast        # WCAG contrast of the colour tokens

Every page: a <title>, unique across the site; a meta description; a canonical
link; JSON-LD that parses. The landing page: the hero comes first and makes its
case (tagline, lede, primary action, install pills) before the first code block,
with no image in it. Description length (50-160 characters, what a search
result shows) is an error on the landing page and section indexes, and on every
page with --strict, as are duplicate titles.
"""
import json
import re
import sys
from html.parser import HTMLParser
from pathlib import Path

HERE = Path(__file__).resolve().parent


class Page(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.title, self.description, self.canonical = None, None, None
        self.redirect, self.jsonld, self._in = False, [], None
        self._buf = []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "title":
            self._in, self._buf = "title", []
        elif tag == "meta" and a.get("name") == "description":
            self.description = a.get("content")
        elif tag == "meta" and a.get("http-equiv", "").lower() == "refresh":
            self.redirect = True
        elif tag == "link" and a.get("rel") == "canonical":
            self.canonical = a.get("href")
        elif tag == "script" and a.get("type") == "application/ld+json":
            self._in, self._buf = "ld", []

    def handle_data(self, data):
        if self._in:
            self._buf.append(data)

    def handle_endtag(self, tag):
        if self._in == "title" and tag == "title":
            self.title = "".join(self._buf).strip()
            self._in = None
        elif self._in == "ld" and tag == "script":
            self.jsonld.append("".join(self._buf))
            self._in = None


def check_pages(public: Path, strict: bool) -> int:
    errors, warnings, titles = [], [], {}
    pages = sorted(public.rglob("*.html"))
    checked = 0
    for path in pages:
        rel = path.relative_to(public).as_posix()
        if rel.startswith("pagefind/"):
            continue
        p = Page()
        p.feed(path.read_text(encoding="utf-8"))
        if p.redirect:
            continue  # an alias stub: a meta refresh to the page that moved
        checked += 1
        is_index = rel == "index.html" or (rel.endswith("/index.html") and _is_section(public, rel))
        if not p.title:
            errors.append(f"{rel}: no <title>")
        else:
            titles.setdefault(p.title, []).append(rel)
        if not p.description:
            errors.append(f"{rel}: no meta description")
        elif not 50 <= len(p.description) <= 160:
            msg = f"{rel}: description is {len(p.description)} characters (50-160)"
            (errors if strict or is_index else warnings).append(msg)
        if rel != "404.html" and not p.canonical:
            errors.append(f"{rel}: no canonical link")
        for block in p.jsonld:
            try:
                json.loads(block)
            except json.JSONDecodeError as e:
                errors.append(f"{rel}: JSON-LD does not parse ({e})")
    for title, where in titles.items():
        if len(where) > 1:
            msg = f"duplicate title {title!r}: {', '.join(where)}"
            (errors if strict else warnings).append(msg)
    errors += check_hero(public / "index.html")
    for w in warnings:
        print(f"  warning: {w}")
    for e in errors:
        print(f"  error: {e}")
    print(f"{checked} pages checked, {len(errors)} errors, {len(warnings)} warnings")
    return 1 if errors else 0


def _is_section(public: Path, rel: str) -> bool:
    # A section index lists pages; Zola's section template marks it.
    text = (public / rel).read_text(encoding="utf-8")
    return 'class="index-list"' in text


def check_hero(index: Path) -> list:
    html = index.read_text(encoding="utf-8")
    errors = []
    hero = html.find('class="hero')
    first_section = html.find("<section")
    if hero < 0 or first_section < 0 or html.rfind("<section", 0, hero + 1) != first_section:
        return ["index.html: the hero is not the first section"]
    end = html.find("</section>", hero)
    block = html[hero:end]
    first_pre = html.find("<pre", hero)
    for marker, what in (("<h1", "tagline"), ('class="lede', "lede"),
                         ('btn-primary', "primary call to action"), ('class="pills', "install pills")):
        at = html.find(marker, hero)
        if at < 0 or at > end:
            errors.append(f"index.html: the hero has no {what}")
        elif first_pre >= 0 and at > first_pre:
            errors.append(f"index.html: the {what} comes after the first code block")
    if "<img" in block:
        errors.append("index.html: the hero contains an image")
    return errors


# ── contrast ──────────────────────────────────────────────────────────────

PAIRS = [  # (foreground, background, minimum)
    ("fg", "bg", 4.5), ("fg", "bg-alt", 4.5), ("fg", "bg-code", 4.5),
    ("fg-muted", "bg", 4.5), ("fg-muted", "bg-alt", 4.5),
    ("accent", "bg", 4.5), ("accent", "bg-alt", 4.5), ("accent", "accent-soft", 4.5),
    ("accent-fg", "accent", 4.5), ("warn", "bg", 4.5), ("warn", "warn-soft", 4.5),
    ("border-strong", "bg", 3.0), ("accent", "bg-code", 3.0),
]


def _lum(hex_):
    h = hex_.lstrip("#")
    r, g, b = (int(h[i:i + 2], 16) / 255 for i in (0, 2, 4))
    lin = [c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4 for c in (r, g, b)]
    return 0.2126 * lin[0] + 0.7152 * lin[1] + 0.0722 * lin[2]


def ratio(a, b):
    la, lb = sorted((_lum(a), _lum(b)), reverse=True)
    return (la + 0.05) / (lb + 0.05)


def tokens(scss: str, name: str) -> dict:
    m = re.search(r"\$" + name + r":\s*\((.*?)\);", scss, re.S)
    if not m:
        sys.exit(f"no ${name} map in main.scss")
    return dict(re.findall(r"([a-z-]+):\s*(#[0-9a-fA-F]{6})", m.group(1)))


def check_contrast() -> int:
    scss = (HERE / "sass" / "main.scss").read_text(encoding="utf-8")
    bad = 0
    print(f"{'pair':28} {'light':>7} {'dark':>7}  min")
    themes = {"light": tokens(scss, "light"), "dark": tokens(scss, "dark")}
    for fg, bg, minimum in PAIRS:
        row = []
        for t in ("light", "dark"):
            r = ratio(themes[t][fg], themes[t][bg])
            row.append(r)
            bad += r < minimum
        flag = "" if min(row) >= minimum else "  FAIL"
        print(f"{fg + ' on ' + bg:28} {row[0]:7.2f} {row[1]:7.2f}  {minimum}{flag}")
    return 1 if bad else 0


def main(argv):
    if "--contrast" in argv:
        return check_contrast()
    strict = "--strict" in argv
    args = [a for a in argv if not a.startswith("--")]
    public = Path(args[0]) if args else HERE / "public"
    return check_pages(public, strict)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
