# Chronix documentation site

The source for <https://hupe1980.github.io/chronix>: [Zola](https://www.getzola.org/)
0.23 (Tera 2 templates), one stylesheet, one small script, and
[Pagefind](https://pagefind.app/) for search. No frontend build step.

```bash
zola serve                 # http://127.0.0.1:1111, live reload, no search index
./build.sh                 # the deploy build: zola + llms files + Pagefind → public/
NO_SEARCH=1 ./build.sh     # the same without Node
zola check                 # links and anchors
python3 check-seo.py public       # titles, descriptions, canonical, JSON-LD, the hero
python3 check-seo.py --contrast   # WCAG contrast of the colour tokens
```

CI pins **Zola 0.23.6** (`ci.yml`, `site.yml`); build with that version before trusting a
template change.

## Layout

| Path | What it holds |
|---|---|
| `content/_index.md` | Landing page. Copy, cards and code live in `[extra]`, because Zola highlights Markdown, not templates |
| `content/learn/` | Tutorials — first steps, start to finish |
| `content/guides/` | How-to guides — one task each |
| `content/reference/` | Reference — every API, setting, flag and format |
| `content/explanation/` | How it works — the design and its trade-offs |
| `content/llms*.md` | Front matter only; render `/llms.txt` and `/llms-full.txt` |
| `templates/base.html` | Every page's head: title, description, canonical, Open Graph, the pre-paint theme script |
| `templates/index.html` | Landing page and its JSON-LD (`WebSite`, `SoftwareSourceCode`, `SoftwareApplication`) |
| `templates/docs-page.html` | Docs page: sidebar, prose, on-page TOC, `TechArticle` + `BreadcrumbList` |
| `templates/components.html` | Tera 2 components, called from Markdown (global once defined; they see only their arguments) |
| `sass/main.scss` | Design tokens (`$light`, `$dark`) and every component |
| `static/site.js` | Theme toggle, copy buttons, code tabs, TOC scroll-spy, search dialog |

The top navigation comes from `extra.nav` in `config.toml`; the sidebar, section index,
pager, sitemap and search index come from the content tree and each page's `weight`.

## Writing a page

- **`title` and `description` are required.** The description is the search-result
  snippet: one sentence, 50–160 characters, saying what the page answers.
  `check-seo.py --strict` enforces it.
- **The answer first, background after.** Present tense, current behaviour only: no
  plans, no history — that is what the changelog is for.
- **Body starts at `##`**; the template renders the `<h1>`.
- **Link with `@/`** (`[Operate](@/guides/operate.md)`) so a rename breaks the build,
  not a reader's click.
- **Numbers name their evidence** — the test or benchmark that measures them.
- **Components** are called from Markdown as `{{ <name arg="…" /> }}`; the available
  ones and their arguments are in `templates/components.html`. Components with a body
  use `{% <callout kind="note|warning" title="…"> %} … {% </callout> %}` and
  `{% <tabs> %} … {% </tabs> %}`. Zola 0.23 has no shortcodes.
- **A moved page keeps its old URL** in `aliases`, so links and search results survive.
- **`scripts/check-docs.sh` fails on history or backlog phrasing** ("used to", "planned",
  "not yet", …): a page describes what is, and the changelog what changed.

## Screenshots

`scripts/screenshots.sh` regenerates every image under `static/img/` from a real
`chronixd --demo`, a Prometheus scraping it and a Grafana provisioned with the bundled
dashboards (Docker, Google Chrome and `cwebp` required; image tags are pinned in the
script). The images are committed build artefacts: re-run the script when a UI changes,
add a shot as one row in `scripts/screenshots/shots.tsv`, and place it with
`{{ <screenshot src="img/name.webp" alt="…" caption="…" /> }}` (the component in
`templates/components.html` sets the image's width and height from the file).

## What the deploy adds

- `scripts/stamp-lastmod.sh` writes each page's `updated` from git for the sitemap's
  `<lastmod>`. It rewrites files in an ephemeral CI checkout — never commit its output.
- The social card is `static/og-image.png`, rendered from `og-image.svg`
  (`rsvg-convert -w 1200 -h 630 -f png -o og-image.png og-image.svg`); platforms do not
  render an SVG card.
