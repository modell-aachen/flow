defmodule Ariadne.Flow.CompositeTest do
  use Ariadne.Flow.Test.Gwt, async: true
  alias Ariadne.Flow.Composite
  alias Ariadne.Flow.Projection

  defmodule CountEvent do
    @derive Ariadne.Flow.Store.Event.Encoder
    defstruct []

    def tags(_), do: []
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

    assert [%{types: [CountEvent]}, %{types: [CountEvent]}] = Composite.query(composite)
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
