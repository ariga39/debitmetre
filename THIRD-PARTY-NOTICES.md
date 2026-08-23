# Third-party notices

## ariga39/orihsus (MIT)

This project selectively ports and adapts compatible building blocks from the
independent Codex gateway [ariga39/orihsus](https://github.com/ariga39/orihsus)
at the pinned revision `7285dd5c6a7ec5f1c0e521c6ee71f70e659d6220`:

- `StreamUsageParser` / `SseUsageParser` (the streaming SSE usage extractor,
  with its non-streaming JSON fallback) are adapted from `src/gateway.rs`
  (lines around 2261, 2294, and 2336 respectively).
- The bounded single-writer JSONL audit writer is adapted from `src/audit.rs`
  (lines around 291, 504, and 527 respectively).
- `DropNotifyStream` and its response-pump select pattern (client-cancel
  detection without another upstream chunk) are adapted from `src/gateway.rs`
  (lines around 2413 and 1966–1995 respectively).

orihsus's key-pool/retry/quota/product semantics are not imported; only the
stream pump, SSE/JSON usage parsing, and audit writer building blocks are used,
adapted to debitmetre's canonical allowlisted audit schema (DESIGN.md §5).

The upstream source is distributed under the MIT and Apache-2.0 licenses. The
full MIT notice follows:

```
MIT License

Copyright (c) 2026 Kagami

Permission is hereby granted, free of charge, to any
person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the
Software without restriction, including without
limitation the rights to use, copy, modify, merge,
publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software
is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice
shall be included in all copies or substantial portions
of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.
```