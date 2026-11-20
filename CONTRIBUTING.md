# Contributing to Chronix

## Setup

Rust 1.94 or newer (`rust-version` in `Cargo.toml`), via [rustup](https://rustup.rs/).

```bash
git clone https://github.com/hupe1980/chronix.git
cd chronix
cargo build              # the product; the frozen cluster tier is not a default member
cargo test               # the product
cargo test --workspace   # adds the frozen cluster tier
```

## Before you push

```bash
./scripts/preflight.sh --quick   # ~3 min: fmt --check, clippy, rustdoc, guards, both test builds
./scripts/preflight.sh           # ~15 min: adds the examples and the frozen tier
```

Preflight runs what CI gates on. `cargo fmt --all` formats and always succeeds; only
`--check` is a check.

## Code and tests

- `cargo clippy --all-targets -- -D warnings` must be clean.
- Every public item has a `///` doc comment: one line saying what it does, then an example
  where one helps.
- Unit tests sit in a `#[cfg(test)] mod tests` block; integration tests in the crate's
  `tests/`; `proptest` for codecs and parsers.
- Run tests in both the default and the feature builds (preflight does): an assertion can
  hold with a feature on and fail without it.

## Documentation

The site at <https://hupe1980.github.io/chronix> is built from `site/` with
[Zola](https://www.getzola.org/) 0.23.6 — the version CI pins.

```bash
zola --root site serve    # http://127.0.0.1:1111, live reload
site/build.sh             # the full build: Zola, llms.txt, the Pagefind index
./scripts/check-docs.sh   # the prose against the code; CI runs it
./scripts/screenshots.sh  # regenerate every screenshot (Docker, headless Chrome)
```

`site/README.md` has the page conventions. Link pages with `@/` so a rename breaks the build,
and give every page a one-sentence `description`. Pages describe current behaviour only.

## Commits and pull requests

- Imperative subject under 72 characters; reference issues with `#NNN`.
- One logical change per pull request, branched from `main`; say what changed and why.
- CI runs fmt, clippy, the tests, `cargo deny`, the docs checks and the site build.

## Releasing

| Artifact | Destination |
|---|---|
| `chronix` and its seven library crates | crates.io |
| `chronixd` | crates.io, and prebuilt binaries on GitHub Releases |
| `chronix-meta`, `chronix-cluster`, `chronix-dsim`, `chronixd-cluster` | not published (frozen cluster tier) |

All crates share one version. Before 1.0 a breaking change bumps the minor and a fix the patch.
The on-disk formats are frozen from 0.8: no release changes a data directory.

```bash
# [workspace.package] version, and the version of every chronix* entry in
# [workspace.dependencies] — cargo publish refuses a mismatch.
$EDITOR Cargo.toml
cargo update --workspace
git commit -am "Release 0.9.0"
git tag -a v0.9.0 -m "chronix 0.9.0"
git push origin v0.9.0
```

`.github/workflows/release.yml` checks the tag against the manifest, waits for CI on that
commit, publishes the crates in the order `scripts/publish-order.sh` derives, and attaches
`chronixd` binaries for `x86_64`/`aarch64` Linux (gnu), `aarch64` Linux musl and `aarch64`
macOS. Tag a commit on `main` that has passed CI. The workflow needs `CARGO_REGISTRY_TOKEN`
in the `crates-io` environment; run it manually with `dry_run` to rehearse.

After a release, check that every crate rendered on docs.rs.

## Reporting issues

Search existing issues first, include a minimal reproduction, and say what you expected and
what happened.

## License

Unless you state otherwise, any contribution you submit is dual licensed as MIT OR
Apache-2.0 ([LICENSE-MIT](LICENSE-MIT) / [LICENSE-APACHE](LICENSE-APACHE)), without
additional terms.
