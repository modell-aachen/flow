defmodule Ariadne.Flow.CompositeTest do
  use Ariadne.Flow.Test.Gwt, async: true
  alias Ariadne.Flow.Composite
  alias Ariadne.Flow.Projection

  defmodule CountEvent do
    @derive Ariadne.Flow.Store.Event.Encoder
    defstruct []

    def tags(_), do: []
  end

  defmodule ValueEvent do
    @derive Ariadne.Flow.Store.Event.Encoder
    defstruct [:value]

    def tags(_), do: []
  end

  defp value_projection(only_last_event) do
    Projection.new(
      %{
        initial_state: 0,
        filter: %{types: [ValueEvent], only_last_event: only_last_event}
      },
      fn state, %ValueEvent{value: value}, _ -> state + value end
    )
  end

  defp count_exists_projection do
    Projection.new(
      %{
        initial_state: false,
        filter: %{types: [CountEvent]}
      },
      fn _, %CountEvent{}, _ -> true end
    )
  end

  test "A composite can build state via projections or other composites and give the query" do
    composite =
      Composite.new(
        %{
          count_exists?: count_exists_projection(),
          model:
            Composite.new(
              %{
                count_exists?: count_exists_projection()
              },
              & &1
            )
        },
        & &1
      )

    assert %{count_exists?: false, model: %{count_exists?: false}} =
             Composite.reduce(composite, given([]))

    assert %{count_exists?: true, model: %{count_exists?: true}} =
             Composite.reduce(composite, given([%CountEvent{}]))

    assert [%{types: [count_event]}, %{types: [count_event]}] = Composite.query(composite)
    assert count_event == "Ariadne.Flow.CompositeTest.CountEvent"
  end

  test "A composite lets each child pick the events its own filter asks for" do
    composite =
      Composite.new(
        %{sum: value_projection(false), latest: value_projection(true)},
        & &1
      )

    assert %{sum: 3, latest: 2} =
             Composite.reduce(composite, given([%ValueEvent{value: 1}, %ValueEvent{value: 2}]))
  end

  test "A composite maps its state" do
    composite =
      Composite.new(
        %{
          count_exists?: count_exists_projection()
        },
        fn state -> state.count_exists? end
      )

    assert false ==
             Composite.reduce(composite, given([]))

    assert true =
             Composite.reduce(composite, given([%CountEvent{}]))
  end
end
