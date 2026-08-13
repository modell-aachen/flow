## Context

See proposal.md — Why, for the motivation. What shapes the approach is where the degenerate forms can reach a backend from:

- `Query.new/1` maps `Filter.new/1` over its input, then runs `Optimizer.optimize/1`.
- `Optimizer.optimize/1` splits on `only_last_event`. Only the `false` branch goes through `expand_to_type_constraint_pairs/1`, whose `normalize_constraint/1` turns a tag list into a `MapSet` and thereby deduplicates it as a side effect. The `true` branch goes through `dedupe_last_event_filters/1`, which deduplicates whole filters and leaves each filter's tag list untouched.
- `Optimizer.build_filters/1` calls `Filter.new/1` again on the filters it emits.

Two consequences follow. The optimizer is not a chokepoint — a repeated tag survives it on the `only_last_event: true` path — so validation belongs upstream of it. And whatever `Filter.new/1` accepts must include everything `Filter.new/1` produces, because the optimizer feeds its own output back through it.

## Goals / Non-Goals

**Goals:**

- One representation per meaning: `nil` is the only way to say "no tag constraint".
- Validation at a single point that every filter passes through exactly once before any backend sees it.
- `Filter.new/1` remains idempotent over its own output.

**Non-Goals:**

- Validating the *element* type of a tag list. `tags: [:foo]` still passes construction and still fails later inside Ecto. That is a separate gap, worth its own change; widening this one would blur what the breaking bump is for.
- Changing the Postgres multi-tag query. Its `having count(t.tag) == ^length(tags)` shape and the two deliberately repeated predicates in `positions_with_all_tags/3` are load-bearing for the measurements recorded in CLAUDE.md.
- Changing `Filter.matches?/2`.
- Checking `types` for repeats.

## Decisions

### Reject the degenerate forms rather than normalise them

Normalising would mean `[] → nil` and `Enum.uniq/1` on the tag list. Rejected because it introduces a second spelling of "no tag constraint" that a reader has to learn, and because it silently repairs what is a caller bug in both cases — there is no input for which a caller means "constrain to zero tags" or "require this tag twice". Rejecting is also less code: three clauses in `validate!/1` and nothing in the construction path.

The precedent is already in the module: `types: []` raises rather than being read as "any type". Commit c2bbec6 (`refactor!: raise ArgumentError on bad construction`) established this across the construction surface.

### Validate in `Filter.new/1`, not in the query builders

`Store.read/3` and `AppendCondition` only ever see `Query.new/1` output, and `Query.new/1` is the only builder of a normalised query — so a check inside `Filter.new/1` covers every path into every backend, including the `only_last_event: true` path the optimizer does not launder. Putting the check in `Store.Postgres` instead would leave `InMemory` accepting what Postgres rejects, which is the divergence this change exists to close, one level down.

Idempotence holds after the change: `build_filters/1` derives `tags` from a `MapSet`, so it emits either `nil` or a non-empty repeat-free list, and with `tags: []` rejected upstream the constraint can never be an empty `MapSet` in the first place.

### Leave `types` unchecked for repeats

Duplicate types are harmless — they reach Postgres as `e.type in ["A", "A"]`, which is a set membership test, and reach `Filter.matches?/2` as `type in types`. Duplicate tags corrupt a `count(...)`. The asymmetry is in the queries, so the asymmetry in the validation is correct rather than an oversight, and belongs in the code as structure rather than as a comment.

## Risks / Trade-offs

- A consumer constructing `tags: []` today gets an `ArgumentError` where they used to get a result → their Postgres result was already the empty set and their `InMemory` result already disagreed with it, so the crash replaces a silent wrong answer. It ships as a breaking change (`!`), which pre-1.0 is a minor bump.
- A caller composing tag lists (`base ++ [tag]`) can now crash on a duplicate → the same composition is already a silent wrong-result bug on Postgres today. A caller who expects duplicates can `Enum.uniq/1` at their own boundary, where they know whether one is expected.
- The error is raised at construction, which for a filter built at compile time (`@filter Filter.new(...)`) means a compile failure rather than a runtime one → this is the desired direction, and matches how `types: []` already behaves.

## Migration Plan

No data migration, no schema change. The commit is a breaking fix (`fix!:`), so release-please cuts a minor bump. A consumer hitting the new raise fixes it by deleting the `tags:` key (empty list) or by deduplicating their list (repeats); both are local edits at the call site with no store-side consequence.
