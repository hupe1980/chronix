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
