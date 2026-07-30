defmodule Ariadne.Flow.ProjectionTest do
  use Ariadne.Flow.Test.Gwt, async: true
  alias Ariadne.Flow.Projection
  alias Ariadne.Flow.Query

  defmodule ExampleEvent do
    @derive Ariadne.Flow.Store.Event.Encoder
    defstruct count: 0

    def tags(%{count: count}) do
      ["count:#{count}"]
    end
  end

  defmodule ExampleEvent2 do
    @derive Ariadne.Flow.Store.Event.Encoder
    defstruct []

    def tags(_) do
      []
    end
  end

  test "Projection projects events to state via type filter" do
    projection =
      Projection.new(
        %{
          initial_state: 0,
          filter: %{
            types: [ExampleEvent]
          }
        },
        fn state, %ExampleEvent{count: count}, _metadata -> state + count end
      )

    assert 0 == Projection.reduce(projection, [])

    assert 1 == Projection.reduce(projection, given([%ExampleEvent{count: 1}]))

    assert 3 ==
             Projection.reduce(
               projection,
               given([%ExampleEvent{count: 1}, %ExampleEvent{count: 2}])
             )

    assert 0 == Projection.reduce(projection, given([%ExampleEvent2{}]))
  end

  test "Projection projects events to state via type and tags filter" do
    projection =
      Projection.new(
        %{
          initial_state: 0,
          filter: %{
            types: [ExampleEvent],
            tags: ["count:1"]
          }
        },
        fn state, %ExampleEvent{count: count}, _metadata -> state + count end
      )

    assert 2 ==
             Projection.reduce(
               projection,
               given([
                 %ExampleEvent{count: 1},
                 %ExampleEvent{count: 1},
                 %ExampleEvent{count: 3}
               ])
             )
  end

  test "Projection projects events with metadata to state" do
    projection =
      Projection.new(
        %{
          initial_state: 0,
          filter: %{
            types: [ExampleEvent]
          }
        },
        fn state, %ExampleEvent{}, %{offset: offset} -> state + offset end
      )

    assert 6 ==
             Projection.reduce(
               projection,
               given([
                 %{event: %ExampleEvent{}, metadata: %{offset: 1}},
                 %{event: %ExampleEvent{}, metadata: %{offset: 2}},
                 %{event: %ExampleEvent{}, metadata: %{offset: 3}}
               ])
             )
  end

  test "Projection can give its query" do
    projection =
      Projection.new(
        %{
          initial_state: 0,
          filter: %{
            types: [ExampleEvent]
          }
        },
        fn state, _, _ -> state end
      )

    assert [%Query.Item{}] = Projection.query(projection)
  end
end
