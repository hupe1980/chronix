"""Every documented call must be one the client can actually take.

The Python SDK's documentation was never executed. Three files showed
``client.query(..., tag_filters={...})`` — a keyword the method has not had
since the request body was corrected — so the first snippet a new user copied
raised ``TypeError``. The same files carried ``localhost:5555`` and Flight SQL
port ``5557`` while ``chronixd`` listens on 8086 and 8817, which is a
``ConnectionRefused`` before any of it runs.

Neither was visible to anything. The mocked unit suite builds its own
requests, ``tests/smoke_live.py`` is handed ``CHRONIX_URL`` so it never uses
the default, and ``scripts/check-docs.sh`` — which exists *because* the ports
drifted once before — scans ``site/content`` and the top-level README only.

So this asserts on the documentation itself: parse every Python block the SDK
ships, and check each call against the real signature.
"""

from __future__ import annotations

import ast
import inspect
import pathlib
import re

import pytest

from chronix_client import ChronixClient
from chronix_client.client import DEFAULT_FLIGHT_PORT, DEFAULT_HTTP_PORT

_SDK = pathlib.Path(__file__).resolve().parent.parent
_REPO = _SDK.parent.parent

#: Everything that shows a reader how to call this client.
_DOCS = [
    _SDK / "README.md",
    _SDK / "examples" / "basic_usage.py",
    _REPO / "site" / "content" / "docs" / "client-sdks.md",
]


def _python_blocks(path: pathlib.Path) -> list[tuple[str, str]]:
    """`(label, source)` for every Python snippet in `path`."""
    text = path.read_text()
    if path.suffix == ".py":
        return [(path.name, text)]
    return [
        (f"{path.name} block {i + 1}", block)
        for i, block in enumerate(re.findall(r"```python\n(.*?)```", text, re.S))
    ]


def _parsed() -> list[tuple[str, ast.AST]]:
    out = []
    for path in _DOCS:
        for label, source in _python_blocks(path):
            # Snippets are written as fragments of an `async def`; wrapping
            # them makes a top-level `await` parse. Indentation is uniform
            # inside a block, so a plain re-indent is enough.
            body = "\n".join("    " + line for line in source.splitlines())
            try:
                tree = ast.parse(f"async def _doc():\n{body}\n")
            except SyntaxError:
                tree = ast.parse(source)
            out.append((label, tree))
    return out


def _client_calls(tree: ast.AST):
    """Yield `(method, call)` for every `client.<method>(...)` in `tree`."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            if func.value.id in {"client", "c", "ChronixClient"}:
                yield func.attr, node
        elif isinstance(func, ast.Name) and func.id == "ChronixClient":
            yield "__init__", node


def test_every_documented_call_matches_the_signature() -> None:
    problems: list[str] = []
    for label, tree in _parsed():
        for method, call in _client_calls(tree):
            attr = getattr(ChronixClient, method, None)
            if attr is None:
                problems.append(f"{label}: ChronixClient has no `{method}`")
                continue
            sig = inspect.signature(attr)
            accepted = set(sig.parameters)
            has_kwargs = any(
                p.kind is inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
            )
            for kw in call.keywords:
                if kw.arg is None or has_kwargs:
                    continue
                if kw.arg not in accepted:
                    problems.append(
                        f"{label}: `{method}(…, {kw.arg}=…)` — the signature is "
                        f"({', '.join(k for k in accepted if k != 'self')})"
                    )
    assert not problems, "documented calls the client cannot take:\n" + "\n".join(problems)


def test_documented_ports_are_the_servers_defaults() -> None:
    """A copy-pasted port that is not the server's is a connection refused.

    Read from `config.rs` rather than restated: the pair drifted once already,
    and a number written down twice is a number that will disagree.
    """
    cfg = (_REPO / "crates" / "chronixd" / "src" / "config.rs").read_text()

    def default(fn: str) -> int:
        m = re.search(rf"fn {fn}\(\).*?0\.0\.0\.0:(\d+)", cfg, re.S)
        assert m, f"{fn}() not found in config.rs"
        return int(m.group(1))

    assert DEFAULT_HTTP_PORT == default("default_http_addr")
    assert DEFAULT_FLIGHT_PORT == default("default_flight_addr")

    allowed = {DEFAULT_HTTP_PORT, DEFAULT_FLIGHT_PORT, default("default_grpc_addr")}
    stale: list[str] = []
    for path in _DOCS:
        for port in re.findall(r"localhost[\"']?,?\s*[:,]\s*(\d{4,5})", path.read_text()):
            if int(port) not in allowed:
                stale.append(f"{path.name}: port {port}")
    assert not stale, "documented ports the server does not listen on:\n" + "\n".join(stale)


@pytest.mark.parametrize("path", _DOCS, ids=lambda p: p.name)
def test_documented_python_parses(path: pathlib.Path) -> None:
    """A snippet that does not parse cannot have been run by anyone."""
    assert _python_blocks(path), f"{path.name} shows no Python"
