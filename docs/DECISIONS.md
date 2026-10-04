# Architectural & Implementation Decisions

This log documents decisions made during implementation where the specification was ambiguous or required design clarification. Format: What, Why, Date.

- **Initial Git & Branch Setup:** Created empty root commit on `main` to provide a valid merge-base for `feat/wisckey-engine` commit inspection scripts (2026-10-04).
- **Gosec G115 Compliance in DecodeEntry:** Verified non-negative bounds and converted unsigned uvarint length after comparison against max integer constraints to safely eliminate integer overflow warnings without sacrificing parsing performance (2026-10-04).
