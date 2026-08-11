## Purpose

Defines how a caller declares which stored events a read or an append condition selects, and guarantees that one such declaration selects the same events regardless of which store backend answers it.

## ADDED Requirements

### Requirement: A filter selects events by type and optionally by tags

An event filter SHALL constrain events by a non-empty set of event types, and MAY additionally constrain them by a set of tags. A filter with a tag constraint SHALL select an event only when the event carries every tag in the constraint; the event MAY carry further tags.

#### Scenario: Type constraint alone

- **WHEN** a filter declares types and declares no tags
- **THEN** it selects every event of those types
- **AND** it selects them irrespective of what tags those events carry, including none

#### Scenario: Tag constraint is a subset test

- **WHEN** a filter declares a tag constraint
- **THEN** it selects an event carrying every constrained tag plus further tags
- **AND** it does not select an event missing any constrained tag

### Requirement: A filter rejects a tag constraint that cannot express a constraint

An empty tag list does not constrain anything and a repeated tag does not constrain twice, so neither carries a meaning distinct from an already-expressible filter. A filter SHALL therefore reject both at construction with an `ArgumentError` rather than accept them and assign them a meaning. Absence of a tag constraint SHALL remain the single way to express "no tag constraint".

#### Scenario: Empty tag list is rejected

- **WHEN** a caller constructs a filter with an empty tag list
- **THEN** construction fails with an `ArgumentError`
- **AND** the message directs the caller to omit the tags instead

#### Scenario: Repeated tag is rejected

- **WHEN** a caller constructs a filter whose tag list contains the same tag more than once
- **THEN** construction fails with an `ArgumentError`

#### Scenario: Tags that are not a list are rejected

- **WHEN** a caller constructs a filter whose tags are neither a list nor absent
- **THEN** construction fails with an `ArgumentError`
- **AND** the failure occurs at construction rather than at the point the filter is applied

#### Scenario: A tag constraint that can be satisfied is accepted

- **WHEN** a caller constructs a filter with a non-empty tag list holding no repeats
- **THEN** construction succeeds
- **AND** the filter retains the tags exactly as given

### Requirement: A constructible filter selects identically on every backend

Every store backend SHALL select the same events for the same filter. A filter that can be constructed SHALL NOT depend on the backend for its meaning.

#### Scenario: Tag-constrained read agrees across backends

- **WHEN** the same events are appended to two different store backends
- **AND** each is read with the same tag-constrained filter
- **THEN** both return the same events in the same order

#### Scenario: No filter can reach a backend with a degenerate tag list

- **WHEN** a filter is rejected at construction
- **THEN** no query carrying it reaches any backend
- **AND** the backends' differing treatment of such a list is unreachable
