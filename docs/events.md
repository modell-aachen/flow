# Events

An **event** in `Ariadne.Flow` records a business-relevant fact that has already happened, such as a course being defined. Events are the only source of truth in a Flow-based service.

## Anatomy of an Event

Here is an example of such an event:

```elixir
defmodule CourseDefined do
  @derive Ariadne.Flow.Store.Event.Encoder
  defstruct [:course_id, :capacity]

  def tags(e), do: ["course:#{e.course_id}"]
end
```

Each event consists of the following essential parts:
- The **module name**. It represents the **event type**. Here the event type is `CourseDefined`.
- The `Ariadne.Flow.Store.Event.Encoder` protocol implementation. Each event needs to implement
  this protocol, which allows us to append and read the event in the `Ariadne.Flow.Store`.
  With `@derive Ariadne.Flow.Store.Event.Encoder` the event gets the default encoder implementation.
- The `defstruct`. It carries the data of the event. Here it has a course id and a capacity.
- The `tags/1` function. It receives the event struct and returns a list of tag strings. Tags are
  saved alongside the event in the store. Their role is explained on the Event Reducer page. Here
  each `CourseDefined` event is stored with one tag: `"course:<course_id>"`.

## Evolving events

Events recorded in the store are immutable history — they are read back forever, by every future version of the code. The three parts that identify and shape an event must therefore stay stable across versions:

- **Module name** — it is the event type stored alongside the data. Renaming the module makes old events unreadable.
- **Payload** — existing fields and their meaning are part of the event's contract. Don't rename, repurpose, or drop a field. Adding a new field is the one exception, and it requires a [custom encoder](encoder.md) that supplies a default for older events that don't carry it.
- **Tags** — filters in [event reducers](event_reducer.md) pin to specific tag strings. Changing how an event's tags are built breaks every reducer that already depends on them. Adding a new tag in `tags/1` only affects events written from that point on — events already in the store keep the tags they were saved with and will not match a filter that requires the new tag.

When the model genuinely needs to change, introduce a new event type rather than rewriting an existing one.
