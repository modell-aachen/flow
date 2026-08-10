defmodule Ariadne.Flow.CommandHandlerTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.AppendConditionError
  alias Ariadne.Flow.CommandHandler
  alias Ariadne.Flow.Composite
  alias Ariadne.Flow.Envelope
  alias Ariadne.Flow.Projection
  alias Ariadne.Flow.Store

  @created_at ~U[2025-09-10 11:09:49.647209Z]

  defmodule CountEvent do
    @derive Ariadne.Flow.Event
    defstruct count: 1

    def tags(%{count: count}), do: ["count:#{count}"]
  end

  defmodule RenamedCountEvent do
    @derive {Ariadne.Flow.Event, type: "count-event"}
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

  defp conflicting_count_command(store) do
    Composite.new(%{count: num_counts_projection()}, fn %{count: count} ->
      {:ok, _} = handle(count_command(1), store)
      {:ok, [%CountEvent{count: count + 1}]}
    end)
  end

  defp last_count_command do
    Composite.new(
      %{count: last_count_projection()},
      &{:ok, [%CountEvent{count: &1.count + 1}]}
    )
  end

  defp conflicting_last_count_command(store) do
    Composite.new(%{count: last_count_projection()}, fn %{count: count} ->
      {:ok, _} = handle(count_command(1), store)
      {:ok, [%CountEvent{count: count + 1}]}
    end)
  end

  defp last_count_projection do
    Projection.new(
      %{initial_state: 0, filter: %{types: [CountEvent], only_last_event: true}},
      fn _state, %CountEvent{count: count}, _ -> count end
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

  defp renamed_count_projection do
    Projection.new(
      %{initial_state: 0, filter: %{types: [RenamedCountEvent], only_last_event: true}},
      fn _state, %RenamedCountEvent{count: count}, _ -> count end
    )
  end

  defp renamed_count_command do
    Composite.new(
      %{count: renamed_count_projection()},
      &{:ok, [%RenamedCountEvent{count: &1.count + 1}]}
    )
  end

  defp handle(command, store, attrs \\ %{}) do
    attrs
    |> Map.put(:command, command)
    |> CommandHandler.new()
    |> CommandHandler.handle(store)
  end

  describe "new/1" do
    test "keeps the command, defaulting the metadata to an empty map and created_at to nil" do
      command = count_command(1)

      assert %CommandHandler{command: ^command, metadata: %{}, created_at: nil} =
               CommandHandler.new(%{command: command})
    end

    test "keeps the given metadata and created_at" do
      assert %CommandHandler{metadata: %{"count" => 1}, created_at: @created_at} =
               CommandHandler.new(%{
                 command: count_command(1),
                 metadata: %{"count" => 1},
                 created_at: @created_at
               })
    end
  end

  describe "handle/2" do
    test "appends the command's events and returns them, given only a store" do
      store = Store.InMemory.init()

      assert {:ok, %{events: [%{event: %CountEvent{count: 1}}]}} =
               handle(count_command(1), store)

      assert {:ok, %{events: [%{event: %CountEvent{count: 2}}]}} =
               handle(count_command(1), store)
    end

    test "returns each appended event as an Envelope carrying the form it was stored under" do
      store = Store.InMemory.init()

      assert {:ok,
              %{
                events: [
                  %Envelope{
                    event: %CountEvent{count: 1},
                    type: "Ariadne.Flow.CommandHandlerTest.CountEvent",
                    tags: ["count:1"],
                    metadata: %{position: 1}
                  }
                ]
              }} = handle(count_command(1), store)
    end

    test "serializes appended events into the store unchanged" do
      store = Store.InMemory.init()

      assert {:ok, _} = handle(count_command(1), store)

      assert %{
               events: [
                 %{
                   record: %{
                     type: "Ariadne.Flow.CommandHandlerTest.CountEvent",
                     data: %{"count" => 1},
                     tags: ["count:1"]
                   }
                 }
               ]
             } = Store.read(store)
    end

    test "reads and appends the type an event declares, not the name of its module" do
      store = Store.InMemory.init()

      {:ok, _} =
        Store.append(store, [
          %Store.Record{type: "count-event", data: %{"count" => 7}, tags: ["count:7"]}
        ])

      assert {:ok, %{events: [%{event: %RenamedCountEvent{count: 8}}]}} =
               handle(renamed_count_command(), store)

      assert %{events: [_, %{record: %{type: "count-event", data: %{"count" => 8}}}]} =
               Store.read(store)
    end

    test "stamps the given metadata on every appended event and folds it back into the decision" do
      store = Store.InMemory.init()

      assert {:ok, _} =
               handle(metadata_command(), store, %{metadata: %{"count" => 1}})

      assert %{events: [%{metadata: %{"count" => 1}}, %{metadata: %{"count" => 1}}]} =
               Store.read(store)

      assert {:ok, _} =
               handle(metadata_command(), store, %{metadata: %{"count" => 1}})

      assert %{
               events: [
                 _,
                 _,
                 %{record: %{data: %{"count" => 2}}},
                 %{record: %{data: %{"count" => 2}}}
               ]
             } = Store.read(store)
    end

    test "stamps the given created_at on every appended event" do
      store = Store.InMemory.init()

      assert {:ok, _} = handle(count_command(1), store, %{created_at: @created_at})

      assert %{events: [%{created_at: @created_at}]} = Store.read(store)
    end

    test "returns the command error unchanged without appending" do
      store = Store.InMemory.init()
      {:ok, _} = handle(count_command(1), store)
      {:ok, _} = handle(count_command(1), store)
      {:ok, _} = handle(count_command(1), store)

      assert {:error, :count_too_high} = handle(count_command(1), store)

      assert %{events: [_, _, _]} = Store.read(store)
    end

    test "fails the append when an event matching the command's query arrives while it decides" do
      store = Store.InMemory.init()

      assert {:error, %AppendConditionError{}} =
               handle(conflicting_count_command(store), store)

      assert %{events: [%{record: %{data: %{"count" => 1}}}]} = Store.read(store)
    end

    test "decides from the last matching event when the command's filter asks for it" do
      store = Store.InMemory.init()

      assert {:ok, %{events: [%{event: %CountEvent{count: 1}}]}} =
               handle(last_count_command(), store)

      assert {:ok, %{events: [%{event: %CountEvent{count: 2}}]}} =
               handle(last_count_command(), store)
    end

    test "fails the append of a command reading only the last matching event just the same" do
      store = Store.InMemory.init()

      assert {:error, %AppendConditionError{}} =
               handle(conflicting_last_count_command(store), store)

      assert %{events: [%{record: %{data: %{"count" => 1}}}]} = Store.read(store)
    end

    test "returns the append error unchanged" do
      store = FailingAppendStore.new(:append_boom)

      assert {:error, :append_boom} = handle(count_command(1), store)
    end
  end
end
