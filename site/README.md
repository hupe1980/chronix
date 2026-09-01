# Chronix documentation site

The source for <https://hupe1980.github.io/chronix>, built with
[Zola](https://www.getzola.org/) (a single static binary, no Node toolchain).

```bash
zola serve        # http://127.0.0.1:1111, live reload
zola build        # → public/
zola check        # link and anchor check; CI runs this
```

## Layout

| Path | What it holds |
|------|---------------|
| `content/_index.md` | Landing page. Copy and code samples live in its front matter, because Zola highlights Markdown, not templates |
| `content/docs/` | **Guide** — task-oriented: install, API, analytics, operations, performance, security, SDKs, Grafana |
| `content/internals/` | **Internals** — how the engine works, and why each algorithm was chosen |
| `content/reference/` | **Reference** — the implementation, subsystem by subsystem |
| `templates/` | Tera templates. `base.html` carries every piece of page metadata |
| `sass/main.scss` | The whole stylesheet |
| `static/` | Favicon, social image, `robots.txt`, search script |

Ordering inside a section comes from each page's `weight`. Adding a page needs
nothing else — the sidebar, the section index, the previous/next pager, the
sitemap and the search index all derive from the content tree.

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
