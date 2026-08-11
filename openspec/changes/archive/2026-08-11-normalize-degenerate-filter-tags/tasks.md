## 1. Tests first

- [x] 1.1 Add rejection cases to `lib/flow/filter_test.exs`: `tags: []`, repeated tags, and a non-list `tags`, each asserting `ArgumentError` with a distinguishing message
- [x] 1.2 Add an acceptance case to `lib/flow/filter_test.exs`: a non-empty repeat-free tag list constructs and retains its tags exactly as given
- [x] 1.3 Add a conformance case to `lib/flow/store_test.exs` (parameterised over both backends): a tag-constrained read returns the same events in the same order
- [x] 1.4 Run the suite and confirm 1.1 fails for the reason expected — not for a `MapSet`/`Protocol` error standing in for the raise

## 2. Implementation

- [x] 2.1 Add tag hints alongside the existing `@filter_hint_*` module attributes in `lib/flow/filter.ex`
- [x] 2.2 Add `validate_tags!/1` and call it from `validate!/1`, clause-per-rule: `nil` passes, `[]` raises, a list with repeats raises, any other value raises

## 3. Fallout

- [x] 3.1 Check `lib/flow/query/optimizer_test.exs` for any case that reaches `build_filters/1` with an empty-tag constraint; confirm the absorb rule's cases still hold
- [x] 3.2 Rewrite the ENA-135 sentence in `CLAUDE.md:91` — the divergence is now unreachable rather than open

## 4. Verify

- [x] 4.1 `mix test` green, including the Postgres-parameterised store suite
- [x] 4.2 `mix check` clean (credo `--strict`, formatter, sobelow)
