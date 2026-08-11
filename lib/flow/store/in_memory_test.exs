defmodule Ariadne.Flow.Store.InMemoryTest do
  use ExUnit.Case, async: true

  alias Ariadne.Flow.ConsumeResult
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.InMemory
  alias Ariadne.Flow.Store.Record
  alias Ariadne.Flow.Store.StoredEventReactor

  @item_added [%{types: ["ItemAdded"]}]

  # The handler runs in the calling process, so what the Postgres backend gets from its
  # per-reactor advisory lock has to come from the lock the agent's state holds.
  describe "consume/2 serialises a reactor" do
    test "makes a consume wait for the one already running under the same name" do
      store = InMemory.init()
      position = append!(store)
      test_pid = self()

      holder =
        Task.async(fn ->
          consume(store, "echo", fn events ->
            send(test_pid, :holding)

            receive do
              :release -> {:ok, length(events)}
            end
          end)
        end)

      assert_receive :holding

      waiter =
        Task.async(fn ->
          consume(store, "echo", fn events ->
            send(test_pid, {:waiter_saw, Enum.map(events, & &1.position)})
            {:ok, length(events)}
          end)
        end)

      refute_receive {:waiter_saw, _}, 50
      send(holder.pid, :release)

      assert %ConsumeResult{processed: 1, last_position: ^position} = Task.await(holder)
      assert %ConsumeResult{processed: 0} = Task.await(waiter)
      assert_received {:waiter_saw, []}
    end

    test "takes the reactor over from a consumer that died holding it" do
      store = InMemory.init()
      position = append!(store)
      test_pid = self()

      holder =
        spawn(fn ->
          consume(store, "echo", fn _events ->
            send(test_pid, :holding)
            Process.sleep(:infinity)
          end)
        end)

      assert_receive :holding

      waiter = Task.async(fn -> consume(store, "echo", &{:ok, length(&1)}) end)
      Process.exit(holder, :kill)

      assert %ConsumeResult{processed: 1, last_position: ^position} = Task.await(waiter)
    end

    test "lets a handler consume its own reactor again rather than wait on itself" do
      store = InMemory.init()
      append!(store)
      test_pid = self()

      nesting_handler = fn events ->
        if Process.put(:nested, true) == nil do
          send(test_pid, {:nested, consume(store, "echo", &{:ok, length(&1)})})
        end

        {:ok, length(events)}
      end

      assert %ConsumeResult{processed: 1} = consume(store, "echo", nesting_handler)
      assert_received {:nested, %ConsumeResult{}}
    end
  end

  defp append!(store) do
    {:ok, %{events: [%{position: position}]}} =
      Store.append(store, [%Record{type: "ItemAdded", data: %{}, tags: []}])

    position
  end

  defp consume(store, name, handler) do
    Store.consume(
      store,
      StoredEventReactor.new(%{name: name, query: @item_added, handler: handler})
    )
  end
end
