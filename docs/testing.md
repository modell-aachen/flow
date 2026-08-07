# Testing with GWT

An event reducer is a pure description: given a list of past events, `reduce/2` always produces the same result. Tests for reducers therefore need no store, no database, and no setup — they are plain function calls.

`Ariadne.Flow.Test.Gwt` provides a small Given-When-Then DSL that makes those function calls read naturally.

## Setup

```elixir
defmodule MyProjectionsTest do
  use Ariadne.Flow.Test.Gwt, async: true

  # tests go here
end
```

`use Ariadne.Flow.Test.Gwt, async: true` imports the DSL and sets up ExUnit. Tests can run concurrently because reducers are pure.

## `res`

`res` asserts that the reducer returns a specific value:

```elixir
gwt "course_capacity" do
  res("is 0 with no events",
    given: [],
    when: course_capacity(42),
    then: 0
  )

  res("reflects CourseDefined",
    given: [%CourseDefined{course_id: 42, capacity: 30}],
    when: course_capacity(42),
    then: 30
  )

  res("follows the latest CourseCapacityChanged",
    given: [
      %CourseDefined{course_id: 42, capacity: 30},
      %CourseCapacityChanged{course_id: 42, new_capacity: 40}
    ],
    when: course_capacity(42),
    then: 40
  )
end
```

- `gwt "..."` groups related cases under a description.
- `given` is the list of events to feed the reducer, in the order they would have been appended.
- `when` is the reducer under test.
- `then` is the expected reduced value.

Use `res` whenever the reducer's result is a plain value — typically a projection or a read-only composite, but anything with that shape works.

## `ok` and `err`

`ok` asserts the reducer returned `{:ok, then}`; `err` asserts it returned `{:error, then}`:

```elixir
gwt "subscribe_student" do
  ok("emits StudentSubscribedToCourse when there is room",
    given: [%CourseDefined{course_id: 42, capacity: 2}],
    when: subscribe_student(42, 7),
    then: [%StudentSubscribedToCourse{course_id: 42, student_id: 7}]
  )

  err("rejects when the course is full",
    given: [
      %CourseDefined{course_id: 42, capacity: 2},
      %StudentSubscribedToCourse{course_id: 42, student_id: 1},
      %StudentSubscribedToCourse{course_id: 42, student_id: 2}
    ],
    when: subscribe_student(42, 7),
    then: :course_full
  )
end
```

Use `ok` and `err` whenever the reducer returns a tagged tuple — commands are the typical case, since their `map_fn` returns `{:ok, events}` or `{:error, reason}`.

## Supplying metadata

When a reducer's handler reads from the `metadata` argument, the test needs to control what metadata each event carries. `given` accepts two forms for this:

- Plain event structs (`%CourseDefined{...}`) — the DSL wraps each in an envelope with `metadata: %{created_at: ~U[2000-01-01 12:00:00Z]}` automatically. Use this form when the handler ignores metadata.
- Pre-wrapped sequenced events (`%{event: ..., metadata: %{...}}`) — the metadata is kept as given. Use this form when the handler reads metadata fields, for example a custom timestamp, a user id, or a trace id.

Either way the DSL builds the same `Ariadne.Flow.Envelope` an event read from the store arrives in: the decoded event and its metadata, plus the stored type and tags the reducer's filter matches against. It also fills in the `:position` a store would have assigned, numbering the given events from 1 in the order you list them, so a handler that reads `metadata.position` can be tested without a store. Pass your own `:position` to override it.

An `%Ariadne.Flow.Envelope{}` passes through `given` untouched. That is the form to use when the point of the test is an event stored under a type its module no longer carries — building it by hand keeps the stored `type` and `tags` out of the current encoder's hands.

Each given event still passes through the reducer's filter just like an event read from the store, so only events whose types and tags match will affect the result.
