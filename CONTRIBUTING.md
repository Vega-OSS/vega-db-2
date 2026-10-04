# Contributing to VegaDB

Thank you for contributing to VegaDB!

## Engineering Standards

All Go code in this repository adheres to current Go idioms (Go 1.22+):
1. **Structure & Dependencies:**
   - `cmd/*`: thin main packages for flags and lifecycle wiring only.
   - `internal/*`: internal implementation details.
   - `pkg/client`: public client library.
   - Strict dependency flow: `transports -> service -> engine -> lsm -> sstable, vlog, memtable -> record, encoding, bufpool, arena`.
2. **Code Style & Documentation:**
   - Formatted with `gofmt` and `goimports`.
   - Every package contains a `doc.go`.
   - Every exported symbol has a doc comment starting with its name.
3. **Concurrency & Memory:**
   - Every goroutine has an owner and joins on shutdown via `sync.WaitGroup`.
   - All locks are documented with `// protects: ...` and never held across I/O.
   - Zero-copy discipline: use `bufpool` for pooled byte buffers; decoders return sub-slices.
4. **Error Handling:**
   - Sentinel errors (`ErrNotFound`, `ErrClosed`).
   - Wrap with `%w` for context.
   - Never ignore unchecked errors.

## Git Workflow & Commit Rules

1. Work on feature branches (e.g. `feat/wisckey-engine`).
2. Commit message length: **at most 9 words** in the format `type: summary` (lowercase, no trailing period, imperative mood).
3. Types allowed: `feat`, `fix`, `test`, `docs`, `build`, `ci`, `chore`, `refactor`, `perf`.
4. Validate commits using `./scripts/check_commits.sh`.
5. Run `make check` prior to every commit.

## Running Tests and Verification

- Quick gate: `make check` (runs format check, vet, and race tests).
- Buffer safety checks: `make tagtest` (runs with `-tags bufdebug`).
- Full gate: `make verify` (runs linter, race tests, tag tests, builds, and container smoke tests).
