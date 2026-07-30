# Encoder

The encoder describes how an event struct is turned into data for the store and back. Every event opts in via `@derive Ariadne.Flow.Store.Event.Encoder` — without it, the store cannot persist the event.

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

Instead of `@derive`, write a `defimpl` inside the event module and implement `encode/1` and `decode/3` directly. Delegate to `Ariadne.Flow.Store.Event.Encoder.Default` for the parts you do not need to change:

```elixir
defmodule CourseDefined do
  defstruct [:course_id, :capacity]

  def tags(e), do: ["course:#{e.course_id}"]

  defimpl Ariadne.Flow.Store.Event.Encoder do
    alias Ariadne.Flow.Store.Event.Encoder.Default

    def encode(event), do: Default.encode(event)

    def decode(event, store_data, metadata) do
      store_data = Map.put_new(store_data, "capacity", 0)
      Default.decode(event, store_data, metadata)
    end
  end
end
```

The `defimpl` replaces the `@derive` line — you don't need both. Each event has its own implementation, so different events can use different encoders.
