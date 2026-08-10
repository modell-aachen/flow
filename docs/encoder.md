# Encoder

The encoder describes how an event struct is turned into data for the store and back. Every event opts in via `@derive Ariadne.Flow.Event` — without it, the store cannot persist the event.

## Stored type

The encoder is also where an event declares the type it is stored under:

```elixir
@derive {Ariadne.Flow.Event, type: "course-defined"}
```

The type is the event's identity in the store — filters select on it, and reading an event back resolves it to the module that declares it. It defaults to the module name, so an event without `type:` is stored as `"CourseDefined"`; [evolving events](events.md#renaming-an-event-module) covers why declaring it anyway is what keeps a module free to be renamed.

Two event modules cannot declare the same type: the first read that has to resolve a type raises instead of guessing between them. So does a stored type no event module declares any more.

## Default encoder

The default encoder serialises an event by taking its struct fields as a map with string keys and collecting the event's tags via `tags/1`. On read, the stored data is turned back into the event struct.

For

```elixir
%CourseDefined{course_id: 42, capacity: 30}
```

the default produces

```elixir
%{
  data: %{"course_id" => 42, "capacity" => 30},
  tags: ["course:42"]
}
```

and reverses that on decode. The default fits any event whose fields are JSON-serialisable (strings, numbers, booleans, maps, lists, `nil`).

## Custom encoder

A custom encoder is useful when the default does not fit — for example:

- An event has been extended with a new field, and older events in the store need a default value for it.
- A field needs a non-default serialisation (a custom value object, a formatted timestamp).
- A field should be redacted or transformed before storage.

Instead of `@derive`, write a `defimpl` inside the event module and implement `encode/1` and `decode/3` directly. Delegate to `Ariadne.Flow.Event.DefaultEncoder` for the parts you do not need to change:

```elixir
defmodule CourseDefined do
  defstruct [:course_id, :capacity]

  def tags(e), do: ["course:#{e.course_id}"]

  defimpl Ariadne.Flow.Event do
    alias Ariadne.Flow.Event.DefaultEncoder

    def type, do: "course-defined"

    def encode(event), do: DefaultEncoder.encode(event)

    def decode(event, store_data, metadata) do
      store_data = Map.put_new(store_data, "capacity", 0)
      DefaultEncoder.decode(event, store_data, metadata)
    end
  end
end
```

The `defimpl` replaces the `@derive` line — you don't need both. Each event has its own implementation, so different events can use different encoders.

`def type` takes the place of the derive's `type:` option; an implementation without it stores the event under its module name, exactly as a derive without `type:` does.
