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

### Reducing the last event only

A filter can also carry `only_last_event: true`. The filter then matches as it otherwise would, but of all the events it matches only the last one — the one appended most recently — reaches the handler:

```elixir
def current_capacity(course_id) do
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

This is the same result as `course_capacity/1` above, reached without reading the capacity changes that have since been superseded. Use it where the state is decided by the newest matching event alone; a projection that counts, sums or otherwise accumulates needs every event and must leave the option out.

`only_last_event` applies per filter, not per query. Each filter of a composite picks the last of *its own* matches, so `%{types: [CourseCapacityChanged], only_last_event: true}` and `%{types: [StudentSubscribedToCourse], only_last_event: true}` in one composite yield one event each, and a filter without the option beside them still sees all of its matches.

A reducer's query is also what its command's append condition is built from ([Concurrency](application.html#concurrency)), and there the option changes nothing: an event matching the filter has appeared since the read exactly when the last matching event has. A command reducing `current_capacity/1` conflicts on the same events as one reducing `course_capacity/1`.

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
