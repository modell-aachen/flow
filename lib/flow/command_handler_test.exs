defmodule Ariadne.Flow.CommandHandlerTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.CommandHandler
  alias Ariadne.Flow.Composite
  alias Ariadne.Flow.Projection
  alias Ariadne.Flow.Store

  @created_at ~U[2025-09-10 11:09:49.647209Z]

  defmodule CountEvent do
    @derive Ariadne.Flow.Store.Event.Encoder
    defstruct count: 1

    def tags(%{count: count}), do: ["count:#{count}"]
  end

  defmodule FailingAppendStore do
    def new(reason), do: %Store{module: __MODULE__, config: reason}
    def read(_reason, _query, _opts), do: %{events: []}
    def append(reason, _events, _opts), do: {:error, reason}
    def telemetry_metadata(_reason), do: %{}
  end

  defp num_counts_projection do
    Projection.new(
      %{initial_state: 0, filter: %{types: [CountEvent]}},
      fn state, %CountEvent{}, _ -> state + 1 end
    )
  end

  defp count_command(increase) do
    Composite.new(
      %{count: num_counts_projection()},
      fn
        %{count: 3} -> {:error, :count_too_high}
        %{count: count} -> {:ok, [%CountEvent{count: count + increase}]}
      end
    )
  end

  defp metadata_projection do
    Projection.new(
      %{initial_state: 0, filter: %{types: [CountEvent]}},
      fn state, _, %{"count" => count, :created_at => %DateTime{}, :position => position}
         when is_integer(position) ->
        state + count
      end
    )
  end

  defp metadata_command do
    Composite.new(
      %{count: metadata_projection()},
      &{:ok, [%CountEvent{count: &1.count}, %CountEvent{count: &1.count}]}
    )
  end

  describe "handle/3" do
    test "appends the command's events and returns them, given only a store" do
      store = Store.InMemory.init()

      assert {:ok, %{events: [%{event: %CountEvent{count: 1}}]}} =
               CommandHandler.handle(count_command(1), store)

      assert {:ok, %{events: [%{event: %CountEvent{count: 2}}]}} =
               CommandHandler.handle(count_command(1), store)
    end

    test "serializes appended events into the store unchanged" do
      store = Store.InMemory.init()

      assert {:ok, _} = CommandHandler.handle(count_command(1), store)

      assert %{
               events: [
                 %{
                   event: %{
                     type: "Ariadne.Flow.CommandHandlerTest.CountEvent",
                     data: %{"count" => 1},
                     tags: ["count:1"]
                   }
                 }
               ]
             } = Store.read(store)
    end

    test "stamps the given metadata on every appended event and folds it back into the decision" do
      store = Store.InMemory.init()

      assert {:ok, _} =
               CommandHandler.handle(metadata_command(), store, metadata: %{"count" => 1})

      assert %{events: [%{metadata: %{"count" => 1}}, %{metadata: %{"count" => 1}}]} =
               Store.read(store)

      assert {:ok, _} =
               CommandHandler.handle(metadata_command(), store, metadata: %{"count" => 1})

      assert %{
               events: [
                 _,
                 _,
                 %{event: %{data: %{"count" => 2}}},
                 %{event: %{data: %{"count" => 2}}}
               ]
             } = Store.read(store)
    end

    test "stamps the given created_at on every appended event" do
      store = Store.InMemory.init()

      assert {:ok, _} = CommandHandler.handle(count_command(1), store, created_at: @created_at)

      assert %{events: [%{created_at: @created_at}]} = Store.read(store)
    end

    test "returns the command error unchanged without appending" do
      store = Store.InMemory.init()
      {:ok, _} = CommandHandler.handle(count_command(1), store)
      {:ok, _} = CommandHandler.handle(count_command(1), store)
      {:ok, _} = CommandHandler.handle(count_command(1), store)

      assert {:error, :count_too_high} = CommandHandler.handle(count_command(1), store)

      assert %{events: [_, _, _]} = Store.read(store)
    end

    test "returns the append error unchanged" do
      store = FailingAppendStore.new(:append_boom)

      assert {:error, :append_boom} = CommandHandler.handle(count_command(1), store)
    end
  end
end
