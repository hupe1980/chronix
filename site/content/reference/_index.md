+++
title = "Reference"
description = "Subsystem-by-subsystem implementation reference for Chronix: crate layout, storage engine, encoding, indexing and query, embedded API, server, analytics, security and cold tiering."
sort_by = "weight"
weight = 30
template = "docs-section.html"
page_template = "docs-page.html"
+++

The implementation, subsystem by subsystem: types, formats, invariants and the
call paths between them. This is the level of detail you need to modify the
engine or to reason about a failure, and it assumes the concepts from
[Internals](/internals/).

It was one 3,400-line document. It is now one page per subsystem, because a
single page that answers forty questions answers none of them well.
