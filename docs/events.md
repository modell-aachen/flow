# Events

An **event** in `Ariadne.Flow` records a business-relevant fact that has already happened, such as a course being defined. Events are the only source of truth in a Flow-based service.

## Anatomy of an Event

Here is an example of such an event:

```elixir
defmodule CourseDefined do
  @derive {Ariadne.Flow.Store.Event.Encoder, type: "course-defined"}
  defstruct [:course_id, :capacity]

  def tags(e), do: ["course:#{e.course_id}"]
end
```

Each event consists of the following essential parts:
- The **event type**. It is the name the event is stored under, declared with `type:` on the
  derive. Here the event type is `"course-defined"`. Left out, it defaults to the module name —
  `"CourseDefined"` — which ties the stored history to what the module is called; see
  [Renaming an event module](#renaming-an-event-module) for why declaring it is worth the one line.
- The `Ariadne.Flow.Store.Event.Encoder` protocol implementation. Each event needs to implement
  this protocol, which allows us to append and read the event in the `Ariadne.Flow.Store`.
  With `@derive Ariadne.Flow.Store.Event.Encoder` the event gets the default encoder implementation.
- The `defstruct`. It carries the data of the event. Here it has a course id and a capacity.
- The `tags/1` function. It receives the event struct and returns a list of tag strings. Tags are
  saved alongside the event in the store. Their role is explained on the Event Reducer page. Here
  each `CourseDefined` event is stored with one tag: `"course:<course_id>"`.

## Evolving events

Events recorded in the store are immutable history — they are read back forever, by every future version of the code. The three parts that identify and shape an event must therefore stay stable across versions:

- **Event type** — the string stored alongside the data, and what every read selects on. A type that changes is history the new code no longer finds: projections start over from the change, and a consistency boundary guarding an invariant reads none of the facts it was protecting. Since the type is declared, it stays put while the module around it moves.
- **Payload** — existing fields and their meaning are part of the event's contract. Don't rename, repurpose, or drop a field. Adding a new field is the one exception, and it requires a [custom encoder](encoder.md) that supplies a default for older events that don't carry it.
- **Tags** — filters in [event reducers](event_reducer.md) pin to specific tag strings. Changing how an event's tags are built breaks every reducer that already depends on them. Adding a new tag in `tags/1` only affects events written from that point on — events already in the store keep the tags they were saved with and will not match a filter that requires the new tag.

When the model genuinely needs to change, introduce a new event type rather than rewriting an existing one.

## Renaming an event module

An event that declares its type can be renamed and moved between namespaces freely — the store never knew the module name to begin with. That is why declaring `type:` from day one is worth the line: it is the only starting point a rename cannot break, and it lets the type read as the store's own name for the fact (`"course-defined"`) rather than as an Elixir module path.

An event that does not declare one is stored under its module name, so renaming the module is renaming the type. Suppose this event has been writing history as `"LessonScheduled"`:

```elixir
defmodule LessonScheduled do
  @derive Ariadne.Flow.Store.Event.Encoder
  defstruct [:course_id, :lesson_id]

  def tags(e), do: ["course:#{e.course_id}"]
end
```

The remedy is to declare that string on the module that replaced it:

```elixir
defmodule Scheduling.LessonScheduled do
  @derive {Ariadne.Flow.Store.Event.Encoder, type: "LessonScheduled"}
  defstruct [:course_id, :lesson_id]

  def tags(e), do: ["course:#{e.course_id}"]
end
```

Old events keep matching every filter, and new ones are written under the same type as before. The type is history from the moment the first event carries it; the module name never was.
