# Contributing to Chronix

Thank you for your interest in contributing! This document provides guidelines
to help you get started.

## Development Setup

1. **Install Rust** — Chronix requires Rust 1.94+ (see `rust-version` in
   `Cargo.toml`). Install via [rustup](https://rustup.rs/).

2. **Clone the repository**:
   ```bash
   git clone https://github.com/hupe1980/chronix.git
   cd chronix
   ```

3. **Build the product** (default members — the frozen cluster tier is
   excluded from the default build):
   ```bash
   cargo build
   ```

4. **Run the test suite**:
   ```bash
   cargo test              # the product
   cargo test --workspace  # additionally the frozen cluster tier
   ```

## Documentation

The site at <https://hupe1980.github.io/chronix> is built from `site/` with
[Zola](https://www.getzola.org/) — a single static binary, no Node toolchain:

```bash
zola --root site serve   # http://127.0.0.1:1111, live reload
zola --root site check   # links and anchors; CI runs this
./scripts/check-docs.sh  # prose against the code; CI runs this too
```

Install **Zola 0.23.4**, the version CI pins. A newer Zola accepts Tera syntax
an older one rejects, so a mismatched local build passes on templates CI
refuses.

`site/README.md` covers the conventions. Two are worth knowing before you
write a page: link between pages with `@/` (`[Operations](@/docs/operations.md)`)
so a rename breaks the build instead of shipping a 404, and give every page a
one-sentence `description` — it is the search-result snippet.

`check-docs.sh` compares the documentation to the tree: crate names, default
ports, example paths, and subsystems that were removed. It exists because all
four had drifted — the site documented three different sets of listen ports,
none of which the server bound.

## Code Style

- Run `cargo fmt --all` before committing. CI enforces `rustfmt` with the
  project's [rustfmt.toml](rustfmt.toml).
- Run `cargo clippy --all-targets -- -D warnings` and fix all
  warnings. CI treats Clippy warnings as errors.
- Follow the existing naming conventions in the crate you're modifying.
- Add doc-comments (`///`) on all public items.

## Testing Conventions

- Every public function should have unit tests in a `#[cfg(test)] mod tests`
  block at the bottom of the file.
- Use `proptest` for property-based testing where appropriate (see existing
  tests in `chronix-encoding` and `chronix-engine`).
- Integration tests go in the crate's `tests/` directory.
- Run the full suite before opening a PR:
  ```bash
  cargo test
  ```

## Commit Messages

- Use the imperative mood: "Add feature" not "Added feature".
- Keep the subject line under 72 characters.
- Reference related issues with `#NNN` when applicable.

## Pull Request Process

1. **Fork & branch** — Create a feature branch from `main`
   (e.g. `feat/my-feature` or `fix/issue-42`).
2. **Keep PRs focused** — One logical change per PR.
3. **Ensure CI passes** — The GitHub Actions pipeline runs fmt, clippy, tests,
   `cargo deny`, security audit, and documentation checks.
4. **Write a clear description** — Explain *what* changed and *why*.
5. **Be responsive** — Address review feedback promptly.

## Releasing

Nine crates, three destinations:

| Artifact | Where |
|---|---|
| `chronix` and its seven library crates | crates.io |
| `chronixd` | GitHub Releases, as a binary — its `cluster` feature depends on `publish = false` crates, and cargo requires a version for every packaged dependency, optional ones included |
| `chronix-meta`, `chronix-cluster`, `chronix-dsim` | nowhere; the frozen cluster tier |

**Version policy.** Every crate shares one version and they are bumped
together — a mixed set has never been tested. Within 0.x a breaking change
bumps the minor (`0.1.0` → `0.2.0`) and a fix bumps the patch. The on-disk
`.csx` and WAL formats are not stable before 1.0; a format change is a minor
bump called out in the release notes, and there is no migration tooling before
then. Sealing the facade API is a pre-1.0 job, not a pre-0.1 one.

**Cutting a release** is a version bump and a tag:

```bash
$EDITOR Cargo.toml        # workspace [package] version — one version, one commit
cargo update --workspace  # refresh Cargo.lock to match
git commit -am "Release 0.1.0"
git tag -a v0.1.0 -m "chronix 0.1.0"
git push origin v0.1.0
```

`.github/workflows/release.yml` does the rest: it checks the tag against the
manifest, waits for CI's verdict on that commit, builds `chronixd`, publishes
the library crates to crates.io, then attaches the archives to the GitHub
release.

It does not re-run the test suite. CI has already run every check on that
commit and runs *more* than a release-time re-run would — the aarch64 targets,
the feature matrix, miri, the MSRV floor, `cargo deny` and the site build are
all CI jobs — so the release waits for that verdict instead of re-deriving a
weaker one. **Tag a commit that is on `main` and has passed CI**; a tag on a
commit CI never saw fails rather than publishing on trust.

Publishing waits for that verdict because it cannot be undone. It does not
wait for the binaries, which are a separate artifact — `chronixd` is not
published to crates.io at all.

Binaries are built for four targets: `x86_64` and `aarch64` Linux (gnu),
`aarch64` Linux musl for static deployment on a gateway, and `aarch64` macOS.
Each is exercised by a CI job, and the `verify` job fails the release if that
stops being true. Adding a target means adding it to CI in the same commit.

Crates go up in dependency order, which `scripts/publish-order.sh` derives
from the manifests. The workflow needs a crates.io API token with publish
rights in the `crates-io` environment as `CARGO_REGISTRY_TOKEN`; run the
workflow manually with `dry_run` to exercise the path without uploading.

Two things still need a person before a release: the Grafana walkthrough
against a running `chronixd`, and checking that every crate rendered on
docs.rs — a crate that fails there fails silently.

## Reporting Issues

- Search existing issues before opening a new one.
- Include a minimal reproducible example when reporting bugs.
- Clearly describe expected vs. actual behaviour.

## License

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you shall be dual licensed as
MIT OR Apache-2.0 ([LICENSE-MIT](LICENSE-MIT) /
[LICENSE-APACHE](LICENSE-APACHE)), without any additional terms or
conditions.
