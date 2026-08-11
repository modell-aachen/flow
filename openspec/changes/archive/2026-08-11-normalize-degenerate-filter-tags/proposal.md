## Why

`Filter.new/1` accepts two degenerate tag lists — an empty list and a list with repeats — and stores them unchanged. `Filter.matches?/2` reads both as a subset test, so `InMemory` treats them as "no tag constraint"; the Postgres builder counts them, so `having count(t.tag) == ^length(tags)` makes both match nothing. The same query returns different events depending on the backend, which is exactly what the store-conformance contract exists to prevent (ENA-135).

The empty case is worse than a wrong result on its own filter. `Query.Optimizer` already agrees with `Filter.matches?/2` that an empty tag set is the *broadest* constraint, so `remove_supersets/1` lets an empty-tag filter absorb every tag-restricted filter for the same type. One degenerate filter silently deletes a correct sibling, and the whole read comes back empty on Postgres.

Neither form has a meaning a caller could want. "Constrain to zero tags" is not a constraint — the way to ask for no tag constraint is to omit `tags`, which is already `nil`. So the fix is to reject them at construction rather than to translate them, keeping one representation per meaning.

## What Changes

- **BREAKING** `Filter.new/1` raises `ArgumentError` on `tags: []`, matching the existing rule for `types: []`.
- **BREAKING** `Filter.new/1` raises `ArgumentError` on a tag list containing repeats.
- `Filter.new/1` raises `ArgumentError` on a `tags` value that is not a list or `nil` — this falls out of the clause structure the two rules above need, and replaces a downstream `Protocol.UndefinedError` from `MapSet.new/1`.
- No change to `Query.Optimizer`, to either query builder, or to `Filter.matches?/2`. Rejecting at construction means neither backend can be reached with a degenerate form.
- No new normalisation: `nil` stays the only representation of "no tag constraint", and a tag list is stored exactly as given.

## Capabilities

### New Capabilities

- `event-filtering`: what a `Filter` accepts at construction, what it rejects, and the guarantee that a constructible filter selects the same events on every store backend.

### Modified Capabilities

None — `openspec/specs/` is empty; this is the first capability in the store.

## Impact

- `lib/flow/filter.ex`: `validate!/1` gains a tags clause.
- `lib/flow/filter_test.exs`: rejection cases for all three invalid forms.
- `lib/flow/store_test.exs`: a conformance case pinning that a *valid* tag filter selects identically on both backends, so the guarantee is tested against the stores and not only against the struct.
- `lib/flow/query/optimizer_test.exs`: confirm no case relies on an empty-tag constraint reaching `build_filters/1`.
- `CLAUDE.md:91`: the sentence recording ENA-135 as an open divergence stops being true and needs rewriting to say the forms are unconstructible.
- No dependency, migration, or public-API signature change. No internal caller constructs either form, so nothing in `lib/` or `dev/` changes besides `filter.ex`.
