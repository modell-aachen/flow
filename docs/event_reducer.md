# Event Reducer

An **event reducer** in `Ariadne.Flow` describes how to turn a sequence of events into a value. State is never stored directly — whenever a value is needed (the current capacity of a course, whether a course is full, whether a subscription is allowed), the relevant events are read from the store and reduced.

An event reducer is purely a description: a plain struct that declares which events it cares about and how to fold them into a result. It holds no state of its own, performs no I/O, and does not talk to the store. Other parts of `Ariadne.Flow` read the events and drive the reducer.

Two concrete event reducers ship with the library: `Ariadne.Flow.Projection` is the basic case, and `Ariadne.Flow.Composite` combines several reducers into one.

The examples on this page reference three events:

```elixir
defmodule CourseDefined do
  @derive Ariadne.Flow.Store.Event.Encoder
  defstruct [:course_id, :capacity]

  def tags(e), do: ["course:#{e.course_id}"]
end

defmodule CourseCapacityChanged do
  @derive Ariadne.Flow.Store.Event.Encoder
  defstruct [:course_id, :new_capacity]

  def tags(e), do: ["course:#{e.course_id}"]
end

defmodule StudentSubscribedToCourse do
  @derive Ariadne.Flow.Store.Event.Encoder
  defstruct [:student_id, :course_id]

  def tags(e), do: ["student:#{e.student_id}", "course:#{e.course_id}"]
end
```

## Projection

Here is a projection that tracks whether course 42 has been defined:

```elixir
course_42_exists =
  Projection.new(%{
    filter: %{
      types: [CourseDefined],
      tags: ["course:42"]
    },
    initial_state: false,
    handler: fn _state, %CourseDefined{}, _metadata -> true end
  })
```

A projection consists of the following essential parts:
- The `filter` decides which events reach the handler. An event passes the filter when its type is listed in `types` **and** every tag in the filter's `tags` list is present on the event. The event may carry additional tags beyond those listed — the filter's tags only need to be a subset. Here the projection only sees `CourseDefined` events tagged with `"course:42"` — events for other courses are skipped.
- The `initial_state` is the starting value used before any events are applied. Here it is `false`.
- The `handler` is a function of `(state, event, metadata)` that returns the next state. Every event comes with additional metadata which can also be accessed. Event metadata is explained in the next sections.  Here every `CourseDefined` event replaces the state with `true`.

`types` is required and must be a non-empty list. `tags` is optional — omit it to match every event of the listed types regardless of tags.

The projection above is tied to course 42. To make it reusable for any course, wrap the construction in a function:

```elixir
def course_exists(course_id) do
  Projection.new(%{
    filter: %{
      types: [CourseDefined],
      tags: ["course:#{course_id}"]
    },
    initial_state: false,
    handler: fn _state, %CourseDefined{}, _metadata -> true end
  })
end
```

`course_exists(42)` now produces the same projection as before, and `course_exists(7)` produces one for a different course.

A projection can fold multiple event types into a single state. The handler simply pattern-matches on event structs:

```elixir
def course_capacity(course_id) do
  Projection.new(%{
    filter: %{
      types: [CourseDefined, CourseCapacityChanged],
      tags: ["course:#{course_id}"]
    },
    initial_state: 0,
    handler: fn
      _state, %CourseDefined{capacity: capacity}, _metadata -> capacity
      _state, %CourseCapacityChanged{new_capacity: new}, _metadata -> new
    end
  })
end
```

A projection can also accumulate a running value by using the current state in the handler:

```elixir
def course_subscriptions(course_id) do
  Projection.new(%{
    filter: %{
      types: [StudentSubscribedToCourse],
      tags: ["course:#{course_id}"]
    },
    initial_state: 0,
    handler: fn count, %StudentSubscribedToCourse{}, _metadata -> count + 1 end
  })
end
```

### Reducing the last event only

Those two projections differ in more than their handlers. `course_subscriptions/1` genuinely needs every event it matches — take one away and the count is wrong. `course_capacity/1` does not: every clause of its handler ignores the state it was given and returns a value read off the event, so the newest matching event alone decides the result. The earlier ones are the same — `course_exists/1` answers `true` from any one match.

A filter that only needs its newest match can say so with `only_last_event: true`:

```elixir
def course_capacity(course_id) do
  Projection.new(%{
    filter: %{
      types: [CourseDefined, CourseCapacityChanged],
      tags: ["course:#{course_id}"],
      only_last_event: true
    },
    initial_state: 0,
    handler: fn
      _state, %CourseDefined{capacity: capacity}, _metadata -> capacity
      _state, %CourseCapacityChanged{new_capacity: new}, _metadata -> new
    end
  })
end
```

The filter still matches what it matched before, but of all the events it matches only the last one reaches the handler — one event in total, not one per listed type. Here that is the newest of the `CourseDefined` and `CourseCapacityChanged` events together, which is exactly what a current capacity is. The superseded capacity changes never leave the store. This is the form the course example in `lib/flow/examples/` ships.

What the option saves for certain is rows fetched and reduced, not necessarily rows scanned. The Postgres store puts the selection inside the item's own query as an `ORDER BY position DESC LIMIT 1`, which for a filter without `tags` is a backward walk of the position index that stops at the first hit. A tag-restricted filter joins the tag table and aggregates per position to enforce its AND-semantics, so whether the database can stop at the first group is the query planner's decision rather than a guarantee.

`only_last_event` applies per filter, not per query. Each filter of a composite picks the last of *its own* matches, so `course_exists/1` and `course_capacity/1` in one composite yield one event each — the last `CourseDefined`, and the last event of either type — and a filter without the option beside them still sees all of its matches.

A reducer's query is also what its command's append condition is built from ([Concurrency](application.html#concurrency)), and there the option changes nothing: an event matching the filter has appeared since the read exactly when the last matching event has. The capacity projection above conflicts on the same events with the option as without it.

## Composite

A composite combines several reducers into one. Use it when the value you need depends on state built by more than one projection.

Here is a composite that answers whether a course still has room for subscriptions:

```elixir
def has_room?(course_id) do
  Composite.new(
    %{
      capacity: course_capacity(course_id),
      subscriptions: course_subscriptions(course_id)
    },
    fn %{capacity: capacity, subscriptions: subscriptions} ->
      subscriptions < capacity
    end
  )
end
```

A composite consists of the following essential parts:
- The `read_model` is a map from user-chosen keys to child reducers. Each child is reduced independently against the same event stream.
- The `map_fn` receives a map with the same keys holding the reduced states, and returns whatever the composite is meant to produce. Here it returns a boolean.

Child reducers can themselves be composites, so you can stack them to any depth.

## The EventReducer behaviour

Both `Ariadne.Flow.Projection` and `Ariadne.Flow.Composite` implement the `Ariadne.Flow.EventReducer` behaviour:

```elixir
@callback reduce(reducer :: struct(), events :: list()) :: any()
@callback query(reducer :: struct()) :: list()
```

- `reduce/2` folds a list of events into the reducer's result.
- `query/1` returns a list of query items describing which events the reducer needs. For a projection this is just its filter; for a composite it is the union of its children's queries.

This shared behaviour is what lets the rest of `Ariadne.Flow` treat projections and composites uniformly.
