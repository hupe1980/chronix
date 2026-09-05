# Chronix documentation site

The source for <https://hupe1980.github.io/chronix>, built with
[Zola](https://www.getzola.org/) (a single static binary, no Node toolchain).

```bash
zola serve        # http://127.0.0.1:1111, live reload
zola build        # → public/
zola check        # link and anchor check; CI runs this
```

**Use Zola 0.23.4** — the version CI pins in `ci.yml` and `site.yml`. Tera's
accepted syntax has widened between releases, so a newer Zola parses templates
an older one rejects: a map literal in `{{ … }}` builds locally on 0.23 and
fails CI on 0.21. Matching the pin is what makes a local `zola build` mean
anything.

## Layout

| Path | What it holds |
|------|---------------|
| `content/_index.md` | Landing page. Copy and code samples live in its front matter, because Zola highlights Markdown, not templates |
| `content/docs/` | **Guide** — task-oriented: install, API, analytics, operations, performance, security, SDKs, Grafana |
| `content/internals/` | **Internals** — how the engine works, and why each algorithm was chosen |
| `content/reference/` | **Reference** — the implementation, subsystem by subsystem |
| `templates/` | Tera templates. `base.html` carries every piece of page metadata |
| `sass/main.scss` | The whole stylesheet |
| `static/` | Favicon, `robots.txt`, the search controller, and the social card — `og-image.svg` is the source, `og-image.png` is what ships |

Ordering inside a section comes from each page's `weight`. Adding a page needs
nothing else — the sidebar, the section index, the previous/next pager, the
sitemap and the search index all derive from the content tree.

## Two things the deploy does that a local build does not

**`scripts/stamp-lastmod.sh` writes each page's `updated` date** from the last
commit that touched it, so the sitemap carries `<lastmod>`. It runs in CI
against an ephemeral checkout and rewrites files in place — never commit its
output. A hand-kept date would go stale, and a `lastmod` a crawler learns to
distrust is worse than none, which is why it is not front matter you maintain.

**The social card is a PNG.** Facebook, LinkedIn, Slack, Discord and X all
refuse an SVG `og:image` and render the card with no picture at all, silently.
`og-image.svg` is the source; regenerate the PNG with
`rsvg-convert -w 1200 -h 630 -f png -o og-image.png og-image.svg` when it
changes.

## Conventions

**Every page needs a `description`.** It is the meta description, the Open
Graph and Twitter description, and the text under the link on the section
index. Aim for one sentence under about 155 characters, since that is roughly
where search results truncate. A page without one silently inherits the
site-wide description, which makes every result look identical.

**One `<h1>` per page, and the template renders it** from `title`. Body content
starts at `##`. Two `<h1>`s is an accessibility problem and a ranking one.

**Link between pages with `@/`** — `[Operations](@/docs/operations.md)` —
rather than with relative paths. Zola resolves and *verifies* those, so a
rename breaks the build instead of shipping a 404. `zola check` validates
anchors too, which is how the site catches a heading being renamed out from
under a deep link.

**The search index is loaded on demand.** It is 3.4 MB — elasticlunr indexes
the full text of every page — and it used to be a `<script defer>` in the head,
so every visitor to every page downloaded and parsed it whether or not they
ever searched. `search.js` now fetches it on the first sign that someone means
to search. Keep it that way: `defer` keeps a script off the critical rendering
path, it does not make it free.

**Numbers name their evidence.** A performance or compression figure in these
pages should say which test or benchmark pins it. Unsourced numbers rot: this
documentation previously claimed "up to 55× compression" (a per-column
timestamp ratio quoted as a whole-database one) and documented a GPU backend
that had been deleted.

## Why not mdBook

The site was an mdBook whose chapters were stubs `{{#include}}`-ing Markdown
from the repository root. That kept two copies in sync but left nowhere to put
per-page titles, descriptions, canonical URLs or structured data — so every
page shared one description and search engines had nothing to distinguish them.
Zola gives each page its own metadata and fails the build on a broken internal
link, which is what a documentation site needs.
