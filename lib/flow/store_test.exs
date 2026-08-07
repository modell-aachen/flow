defmodule Ariadne.Flow.StoreTest do
  use ExUnit.Case,
    async: true,
    parameterize:
      Enum.map([Ariadne.Flow.Store.InMemory, Ariadne.Flow.Store.Postgres], &%{store_module: &1})

  alias Ariadne.Flow.ConsumeResult
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Event
  alias Ariadne.Flow.Store.SequencedEvent
  alias Ariadne.Flow.Store.StoredEventReactor
  alias Ecto.Adapters.SQL.Sandbox
  @created_at ~U[2025-09-10 11:09:49.647209Z]

  setup %{store_module: store_module} do
    store =
      case store_module do
        Ariadne.Flow.Store.InMemory ->
          store_module.init()

        Ariadne.Flow.Store.Postgres ->
          repo = Ariadne.Flow.Test.Repo
          :ok = Sandbox.checkout(repo)
          store_module.init(repo: repo, prefix: "postgres_store_test_schema")
      end

    %{store: store}
  end

  describe "The event store" do
    test "starts with no events", %{store: store} do
      assert %{events: []} = Store.read(store)
    end

    test "counts the events it holds", %{store: store} do
      assert 0 == Store.count(store)

      Store.append(store, [
        %Event{type: "PageCreated", data: %{"title" => "Page 1"}, tags: []},
        %Event{type: "PageCreated", data: %{"title" => "Page 2"}, tags: []}
      ])

      assert 2 == Store.count(store)
    end

    test "appends events and reads all", %{store: store} do
      Store.append(
        store,
        [
          %Event{type: "PageCreated", data: %{"title" => "Page 1"}, tags: []},
          %Event{type: "PageCreated", data: %{"title" => "Page 2"}, tags: []}
        ],
        created_at: @created_at
      )

      assert %{
               events: [
                 %Ariadne.Flow.Store.SequencedEvent{
                   event: %Event{type: "PageCreated", data: %{"title" => "Page 1"}, tags: []},
                   created_at: @created_at,
                   position: position_1,
                   metadata: %{}
                 },
                 %Ariadne.Flow.Store.SequencedEvent{
                   event: %Event{type: "PageCreated", data: %{"title" => "Page 2"}, tags: []},
                   created_at: @created_at,
                   position: position_2,
                   metadata: %{}
                 }
               ]
             } = Store.read(store)

      assert position_1 < position_2

      Store.append(store, %Event{type: "PageCreated", data: %{"title" => "Page 3"}, tags: []})

      assert %{
               events: [
                 %{event: %Event{type: "PageCreated", data: %{"title" => "Page 1"}}},
                 %{
                   event: %Event{type: "PageCreated", data: %{"title" => "Page 2"}},
                   position: position_2
                 },
                 %{
                   event: %Event{type: "PageCreated", data: %{"title" => "Page 3"}},
                   position: position_3,
                   created_at: %DateTime{}
                 }
               ]
             } = Store.read(store)

      assert position_2 < position_3
    end

    test "appends events with additional metadata", %{store: store} do
      Store.append(
        store,
        [
          %Event{type: "PageCreated", data: %{}, tags: []},
          %Event{type: "PageDeleted", data: %{}, tags: []}
        ],
        metadata: %{"user_id" => "user-123", "trace_id" => "trace-456"}
      )

      assert %{
               events: [
                 %{
                   event: %Event{type: "PageCreated"},
                   metadata: %{"user_id" => "user-123", "trace_id" => "trace-456"}
                 },
                 %{
                   event: %Event{type: "PageDeleted"},
                   metadata: %{"user_id" => "user-123", "trace_id" => "trace-456"}
                 }
               ]
             } = Store.read(store)
    end

    test "reads into a Read carrying the normalised query and the last position seen",
         %{store: store} do
      Store.append(store, [
        %Event{type: "PageCreated", data: %{}, tags: []},
        %Event{type: "PageCreated", data: %{}, tags: []}
      ])

      assert %Store.Read{
               query: %Query{items: [%Query.Item{types: ["PageCreated"]}]},
               events: [_, %{position: last_position}],
               last_position: last_position
             } = Store.read(store, [%{types: ["PageCreated"]}, %{types: ["PageCreated"]}])

      assert %Store.Read{query: %Query{items: :all}, events: [_, _]} = Store.read(store)

      assert %Store.Read{query: %Query{items: []}, events: [], last_position: 0} =
               Store.read(store, [])
    end

    test "queries events without query items", %{store: store} do
      Store.append(store, [
        %Event{type: "PageCreated", data: %{}, tags: []},
        %Event{type: "PageDeleted", data: %{}, tags: []}
      ])

      assert %{events: []} = Store.read(store, [])
    end

    test "queries events by type", %{store: store} do
      Store.append(store, [
        %Event{type: "PageCreated", data: %{}, tags: []},
        %Event{type: "PageDeleted", data: %{}, tags: []}
      ])

      assert %{events: [%{event: %Event{type: "PageCreated"}}]} =
               Store.read(store, [%{types: ["PageCreated"]}])

      assert %{
               events: [
                 %{event: %Event{type: "PageCreated"}},
                 %{event: %Event{type: "PageDeleted"}}
               ]
             } = Store.read(store, [%{types: ["PageCreated", "PageDeleted"]}])

      assert %{
               events: [
                 %{event: %Event{type: "PageCreated"}},
                 %{event: %Event{type: "PageDeleted"}}
               ]
             } = Store.read(store, [%{types: ["PageCreated"]}, %{types: ["PageDeleted"]}])

      assert_raise(RuntimeError, fn ->
        Store.read(store, [%{types: []}])
      end)
    end

    test "does not support queries only by tags", %{store: store} do
      Store.append(store, [
        %Event{type: "PageCreated", data: %{}, tags: ["page:Page 1", "type:page"]}
      ])

      assert_raise(RuntimeError, fn ->
        Store.read(store, [%{tags: ["page:Page 1"]}])
      end)
    end

    test "queries by types and tags", %{store: store} do
      Store.append(store, [
        %Event{
          type: "PageCreated",
          data: %{"title" => "Page 1"},
          tags: ["page:Page 1", "type:page"]
        },
        %Event{type: "PageDeleted", data: %{}, tags: ["page:deleted", "type:page"]},
        %Event{
          type: "PageCreated",
          data: %{"title" => "Page 2"},
          tags: ["page:Page 2", "type:page"]
        }
      ])

      assert %{
               events: [
                 %{event: %Event{type: "PageCreated", data: %{"title" => "Page 2"}}}
               ]
             } = Store.read(store, [%{types: ["PageCreated"], tags: ["page:Page 2"]}])
    end

    test "reads only the last event matching an item that asks for it", %{store: store} do
      append!(store, "ItemAdded")

      {:ok, %{events: [%SequencedEvent{position: last_added}]}} =
        Store.append(
          store,
          %Event{type: "ItemAdded", data: %{"title" => "Item 2"}, tags: ["item:2"]},
          created_at: @created_at,
          metadata: %{"user_id" => "user-123"}
        )

      removed = append_position!(store, "ItemRemoved")

      assert %{
               events: [
                 %SequencedEvent{
                   event: %Event{
                     type: "ItemAdded",
                     data: %{"title" => "Item 2"},
                     tags: ["item:2"]
                   },
                   position: ^last_added,
                   created_at: @created_at,
                   metadata: %{"user_id" => "user-123"}
                 }
               ]
             } = Store.read(store, [%{types: ["ItemAdded"], only_last_event: true}])

      assert %{events: []} =
               Store.read(store, [%{types: ["ItemRenamed"], only_last_event: true}])

      assert %{events: [%{position: ^last_added}, %{position: ^removed}]} =
               Store.read(store, [
                 %{types: ["ItemAdded"], only_last_event: true},
                 %{types: ["ItemRemoved"], only_last_event: true}
               ])
    end

    test "reads the last event of each item, not of the query", %{store: store} do
      tagged = append_position!(store, "ItemAdded", ["item:1"])
      untagged = append_position!(store, "ItemAdded")

      assert %{events: [%{position: ^tagged}, %{position: ^untagged}]} =
               Store.read(store, [
                 %{types: ["ItemAdded"], tags: ["item:1"], only_last_event: true},
                 %{types: ["ItemAdded"], only_last_event: true}
               ])
    end

    test "reads every match of the items beside one that asks for its last event only",
         %{store: store} do
      first_added = append_position!(store, "ItemAdded")
      second_added = append_position!(store, "ItemAdded")
      append!(store, "ItemRemoved")
      last_removed = append_position!(store, "ItemRemoved")

      assert %{
               events: [
                 %{position: ^first_added},
                 %{position: ^second_added},
                 %{position: ^last_removed}
               ]
             } =
               Store.read(store, [
                 %{types: ["ItemAdded"]},
                 %{types: ["ItemRemoved"], only_last_event: true}
               ])
    end

    test "reads the last event after the position read after", %{store: store} do
      first = append_position!(store, "ItemAdded")
      second = append_position!(store, "ItemAdded")

      only_last = [%{types: ["ItemAdded"], only_last_event: true}]

      assert %{events: [%{position: ^second}]} = Store.read(store, only_last, after: first)
      assert %{events: []} = Store.read(store, only_last, after: second)
    end

    test "fails an append condition on an item that asks for its last event only",
         %{store: store} do
      condition = %{fail_if_events_match: [%{types: ["ItemAdded"], only_last_event: true}]}
      first = append_position!(store, "ItemAdded")

      assert {:error, :append_condition_failed} ==
               Store.append(store, %Event{type: "ItemAdded", data: %{}, tags: []},
                 condition: condition
               )

      assert {:ok, _} =
               Store.append(store, %Event{type: "ItemAdded", data: %{}, tags: []},
                 condition: Map.put(condition, :after, first)
               )
    end

    test "skips events up to the position read after", %{store: store} do
      first = append_position!(store, "ItemAdded")
      second = append_position!(store, "ItemAdded")

      assert %{events: [%{position: ^second}]} = Store.read(store, :all, after: first)

      assert %{events: [%{position: ^second}]} =
               Store.read(store, [%{types: ["ItemAdded"]}], after: first)

      assert %{events: []} = Store.read(store, :all, after: second)
    end

    test "reads at most as many events as the limit", %{store: store} do
      first = append_position!(store, "ItemAdded")
      append_position!(store, "ItemAdded")

      assert %{events: [%{position: ^first}]} = Store.read(store, :all, limit: 1)

      assert %{events: [%{position: ^first}]} =
               Store.read(store, [%{types: ["ItemAdded"]}], limit: 1)
    end

    test "appends events with an append condition", %{store: store} do
      {:ok, %{events: [%SequencedEvent{position: position, created_at: created_at}]}} =
        result =
        Store.append(store, %Event{type: "PageCreated", data: %{}, tags: ["page:Page 1"]},
          created_at: @created_at
        )

      assert result ==
               {:ok,
                %{
                  events: [
                    %SequencedEvent{
                      event: %Event{type: "PageCreated", data: %{}, tags: ["page:Page 1"]},
                      position: position,
                      created_at: created_at,
                      metadata: %{}
                    }
                  ]
                }}

      assert {:error, :append_condition_failed} ==
               Store.append(store, %Event{type: "PageCreated", data: %{}, tags: []},
                 condition: %{
                   fail_if_events_match: [%{types: ["PageCreated"], tags: ["page:Page 1"]}]
                 }
               )

      assert {:ok, _} =
               Store.append(store, %Event{type: "PageCreated", data: %{}, tags: ["page:Page 2"]},
                 condition: %{
                   fail_if_events_match: [%{types: ["PageCreated"], tags: ["page:Page 2"]}]
                 }
               )

      assert {:error, :append_condition_failed} ==
               Store.append(store, %Event{type: "PageCreated", data: %{}, tags: []},
                 condition: %{
                   fail_if_events_match: [%{types: ["PageCreated"], tags: ["page:Page 2"]}],
                   after: 1
                 }
               )

      assert {:ok, _} =
               Store.append(
                 store,
                 %Event{type: "PageCreated", data: %{}, tags: []},
                 condition: %{
                   fail_if_events_match: [%{types: ["PageCreated"], tags: ["page:Page 2"]}],
                   after: 1000
                 }
               )

      assert_raise RuntimeError, fn ->
        Store.append(store, %Event{type: "PageCreated", data: %{}, tags: []},
          condition: %{after: 1}
        )
      end
    end

    test "emits telemetry spans on read and append", %{store: store, store_module: backend} do
      attach_telemetry([
        [:ariadne, :flow, :store, :read, :start],
        [:ariadne, :flow, :store, :read, :stop],
        [:ariadne, :flow, :store, :append, :start],
        [:ariadne, :flow, :store, :append, :stop]
      ])

      Store.append(store, [
        %Event{type: "PageCreated", data: %{}, tags: []},
        %Event{type: "PageDeleted", data: %{}, tags: []}
      ])

      assert_receive {:telemetry, [:ariadne, :flow, :store, :append, :start], _,
                      %{backend: ^backend} = append_start_meta}

      assert_receive {:telemetry, [:ariadne, :flow, :store, :append, :stop],
                      %{duration: duration, event_count: 2}, %{backend: ^backend, result: :ok}}

      assert is_integer(duration) and duration >= 0

      Store.read(store)

      assert_receive {:telemetry, [:ariadne, :flow, :store, :read, :start], _,
                      %{backend: ^backend}}

      assert_receive {:telemetry, [:ariadne, :flow, :store, :read, :stop], %{event_count: 2},
                      %{backend: ^backend}}

      assert_backend_metadata(backend, append_start_meta)
    end

    test "emits telemetry stop with result :error on append condition conflict",
         %{store: store, store_module: backend} do
      Store.append(store, %Event{type: "PageCreated", data: %{}, tags: ["page:Page 1"]})

      attach_telemetry([[:ariadne, :flow, :store, :append, :stop]])

      assert {:error, :append_condition_failed} =
               Store.append(store, %Event{type: "PageCreated", data: %{}, tags: []},
                 condition: %{
                   fail_if_events_match: [%{types: ["PageCreated"], tags: ["page:Page 1"]}]
                 }
               )

      assert_receive {:telemetry, [:ariadne, :flow, :store, :append, :stop], _,
                      %{backend: ^backend, result: :error, error: :append_condition_failed}}
    end
  end

  describe "consume" do
    @item_added [%{types: ["ItemAdded"]}]

    test "returns empty result when there are no matching events", %{store: store} do
      handler = fn _events -> {:ok, 0} end

      assert %ConsumeResult{
               status: :ok,
               processed: 0,
               last_position: 0,
               more?: false,
               failure: nil
             } =
               Store.consume(
                 store,
                 StoredEventReactor.new(%{name: "echo", query: @item_added, handler: handler})
               )
    end

    test "delivers all matching events to the handler in order", %{store: store} do
      append!(store, "ItemAdded")
      append!(store, "ItemAdded")
      append!(store, "ItemAdded")

      handler = recording_handler(self(), "echo")

      assert %ConsumeResult{
               status: :ok,
               processed: 3,
               last_position: last_position,
               more?: false
             } =
               Store.consume(
                 store,
                 StoredEventReactor.new(%{name: "echo", query: @item_added, handler: handler})
               )

      assert last_position > 0

      assert_received {:got, "echo", %SequencedEvent{position: p1}}
      assert_received {:got, "echo", %SequencedEvent{position: p2}}
      assert_received {:got, "echo", %SequencedEvent{position: p3}}
      assert p1 < p2
      assert p2 < p3
    end

    test "normalises the reactor's query before dispatching to the backend", %{store: store} do
      append!(store, "ItemAdded")

      handler = recording_handler(self(), "echo")

      raw_query_reactor = %StoredEventReactor{
        name: "echo",
        query: [%{types: ["ItemAdded"]}],
        handler: handler
      }

      assert %ConsumeResult{status: :ok, processed: 1} = Store.consume(store, raw_query_reactor)

      assert_received {:got, "echo", %SequencedEvent{event: %Event{type: "ItemAdded"}}}
    end

    test "skips events that do not match the query", %{store: store} do
      append!(store, "ItemAdded")
      append!(store, "ItemRemoved")
      append!(store, "ItemAdded")

      handler = recording_handler(self(), "echo")

      assert %ConsumeResult{status: :ok, processed: 2, more?: false} =
               Store.consume(
                 store,
                 StoredEventReactor.new(%{name: "echo", query: @item_added, handler: handler})
               )

      assert_received {:got, "echo", %SequencedEvent{event: %Event{type: "ItemAdded"}}}
      assert_received {:got, "echo", %SequencedEvent{event: %Event{type: "ItemAdded"}}}
      refute_received {:got, "echo", %SequencedEvent{event: %Event{type: "ItemRemoved"}}}
    end

    test "resumes from the last position on subsequent advances", %{store: store} do
      append!(store, "ItemAdded")
      append!(store, "ItemAdded")

      handler = recording_handler(self(), "echo")

      assert %ConsumeResult{processed: 2, last_position: first_last} =
               Store.consume(
                 store,
                 StoredEventReactor.new(%{name: "echo", query: @item_added, handler: handler})
               )

      append!(store, "ItemAdded")

      assert %ConsumeResult{processed: 1, last_position: second_last} =
               Store.consume(
                 store,
                 StoredEventReactor.new(%{name: "echo", query: @item_added, handler: handler})
               )

      assert second_last > first_last
    end

    test "consuming again after draining returns nothing", %{store: store} do
      append!(store, "ItemAdded")

      handler = recording_handler(self(), "echo")

      assert %ConsumeResult{processed: 1} =
               Store.consume(
                 store,
                 StoredEventReactor.new(%{name: "echo", query: @item_added, handler: handler})
               )

      assert %ConsumeResult{status: :ok, processed: 0, more?: false} =
               Store.consume(
                 store,
                 StoredEventReactor.new(%{name: "echo", query: @item_added, handler: handler})
               )
    end

    test "starts after the reactor's initialized checkpoint", %{store: store} do
      append_position!(store, "ItemAdded")
      second = append_position!(store, "ItemAdded")
      third = append_position!(store, "ItemAdded")

      Store.init_checkpoints(store, [%{name: "echo", position: second}])

      handler = recording_handler(self(), "echo")

      assert %ConsumeResult{status: :ok, processed: 1, more?: false} =
               Store.consume(
                 store,
                 StoredEventReactor.new(%{name: "echo", query: @item_added, handler: handler})
               )

      assert_received {:got, "echo", %SequencedEvent{position: ^third}}
      refute_received {:got, "echo", %SequencedEvent{position: ^second}}
    end

    test "starts at the origin for a reactor nobody ever initialized", %{store: store} do
      first = append_position!(store, "ItemAdded")

      handler = recording_handler(self(), "echo")

      assert %ConsumeResult{processed: 1} =
               Store.consume(
                 store,
                 StoredEventReactor.new(%{name: "echo", query: @item_added, handler: handler})
               )

      assert_received {:got, "echo", %SequencedEvent{position: ^first}}
    end

    test "isolates positions across reactor names", %{store: store} do
      append!(store, "ItemAdded")
      append!(store, "ItemAdded")

      handler = recording_handler(self(), "any")

      assert %ConsumeResult{processed: 2} =
               Store.consume(
                 store,
                 StoredEventReactor.new(%{name: "alpha", query: @item_added, handler: handler})
               )

      assert %ConsumeResult{processed: 2} =
               Store.consume(
                 store,
                 StoredEventReactor.new(%{name: "beta", query: @item_added, handler: handler})
               )
    end

    test "halts at the first error and persists position up to the last successful event",
         %{store: store} do
      append!(store, "ItemAdded", ["id:1"])
      append!(store, "ItemAdded", ["id:0"])
      append!(store, "ItemAdded", ["id:2"])

      failing_handler = fn events ->
        Enum.reduce_while(events, {:ok, 0}, fn seq, {:ok, count} ->
          if "id:0" in seq.event.tags do
            {:halt, {:error, count, %{event: seq, reason: :zero_not_allowed}}}
          else
            {:cont, {:ok, count + 1}}
          end
        end)
      end

      assert %ConsumeResult{
               status: :error,
               processed: 1,
               last_position: last_position,
               more?: false,
               failure: %{
                 event: %SequencedEvent{event: %Event{tags: ["id:0"]}},
                 reason: :zero_not_allowed
               }
             } =
               Store.consume(
                 store,
                 StoredEventReactor.new(%{
                   name: "rejects-zero",
                   query: @item_added,
                   handler: failing_handler
                 })
               )

      assert last_position > 0

      success_handler = fn events -> {:ok, length(events)} end

      assert %ConsumeResult{status: :ok, processed: 2, more?: false} =
               Store.consume(
                 store,
                 StoredEventReactor.new(%{
                   name: "rejects-zero",
                   query: @item_added,
                   handler: success_handler
                 })
               )
    end

    test "consumes in batches and reports more? when the batch is full", %{store: store} do
      batch_size = 100
      Enum.each(1..(batch_size + 5), fn _ -> append!(store, "ItemAdded") end)

      handler = fn events -> {:ok, length(events)} end

      assert %ConsumeResult{status: :ok, processed: ^batch_size, more?: true} =
               Store.consume(
                 store,
                 StoredEventReactor.new(%{name: "batched", query: @item_added, handler: handler})
               )

      assert %ConsumeResult{status: :ok, processed: 5, more?: false} =
               Store.consume(
                 store,
                 StoredEventReactor.new(%{name: "batched", query: @item_added, handler: handler})
               )
    end

    test "passes the matching events to the handler only", %{store: store} do
      append!(store, "ItemRemoved")
      append!(store, "ItemAdded")
      append!(store, "ItemRemoved")
      append!(store, "ItemAdded")

      capturing_handler = fn events ->
        Enum.each(events, fn seq -> send(self(), {:saw, seq.event.type}) end)
        {:ok, length(events)}
      end

      assert %ConsumeResult{processed: 2} =
               Store.consume(
                 store,
                 StoredEventReactor.new(%{
                   name: "filtered",
                   query: @item_added,
                   handler: capturing_handler
                 })
               )

      refute_received {:saw, "ItemRemoved"}
    end
  end

  describe "checkpoint/2" do
    @checkpoint_query [%{types: ["ItemAdded"]}]

    test "is nil for a reactor that has never run", %{store: store} do
      assert Store.checkpoint(store, "echo") == nil
    end

    test "is the position the reactor last consumed up to", %{store: store} do
      append!(store, "ItemAdded")
      position = append_position!(store, "ItemAdded")

      consume!(store, "echo", @checkpoint_query)

      assert Store.checkpoint(store, "echo") == position
    end

    test "advances as the reactor consumes newly appended events", %{store: store} do
      first = append_position!(store, "ItemAdded")
      consume!(store, "echo", @checkpoint_query)
      assert Store.checkpoint(store, "echo") == first

      second = append_position!(store, "ItemAdded")
      consume!(store, "echo", @checkpoint_query)

      assert Store.checkpoint(store, "echo") == second
    end

    test "stays put on events the reactor's query does not match", %{store: store} do
      position = append_position!(store, "ItemAdded")
      consume!(store, "echo", @checkpoint_query)

      append!(store, "ItemRemoved")
      consume!(store, "echo", @checkpoint_query)

      assert Store.checkpoint(store, "echo") == position
    end

    test "is kept per reactor name", %{store: store} do
      append!(store, "ItemAdded")
      consume!(store, "echo", @checkpoint_query)

      assert Store.checkpoint(store, "other") == nil
    end

    test "is where a reactor stands even when its run consumed nothing", %{store: store} do
      consume!(store, "echo", @checkpoint_query)

      assert Store.checkpoint(store, "echo") == 0
    end
  end

  describe "init_checkpoints/2" do
    @checkpoint_query [%{types: ["ItemAdded"]}]

    test "gives a reactor that has none the checkpoint it was declared at", %{store: store} do
      assert :ok = Store.init_checkpoints(store, [%{name: "echo", position: 7}])

      assert Store.checkpoint(store, "echo") == 7
    end

    test "initializes every reactor of the set in one call", %{store: store} do
      assert :ok =
               Store.init_checkpoints(store, [
                 %{name: "echo", position: 7},
                 %{name: "other", position: 0}
               ])

      assert Store.checkpoint(store, "echo") == 7
      assert Store.checkpoint(store, "other") == 0
    end

    test "accepts an empty set without touching the store", %{store: store} do
      assert :ok = Store.init_checkpoints(store, [])
    end

    # The declaration only ever says where a reactor that never ran begins, so an init
    # arriving after the reactor has moved on must not drag it back.
    test "leaves a reactor that already has a checkpoint where it stands", %{store: store} do
      position = append_position!(store, "ItemAdded")
      consume!(store, "echo", @checkpoint_query)

      assert :ok = Store.init_checkpoints(store, [%{name: "echo", position: 0}])

      assert Store.checkpoint(store, "echo") == position
    end

    test "rolls back with the transaction it was made in", %{store: store} do
      assert_raise RuntimeError, "boom", fn ->
        Store.transaction(store, fn ->
          Store.init_checkpoints(store, [%{name: "echo", position: 7}])
          raise "boom"
        end)
      end

      assert Store.checkpoint(store, "echo") == nil
    end
  end

  describe "in_transaction?/1" do
    test "is false outside a transaction", %{store: store} do
      refute Store.in_transaction?(store)
    end

    test "is true inside one", %{store: store} do
      assert :ok =
               Store.transaction(store, fn ->
                 assert Store.in_transaction?(store)
                 :ok
               end)
    end

    test "is true inside a transaction nested in another", %{store: store} do
      assert :ok =
               Store.transaction(store, fn ->
                 Store.transaction(store, fn ->
                   assert Store.in_transaction?(store)
                   :ok
                 end)
               end)
    end

    test "is false again after the transaction returns", %{store: store} do
      assert :ok = Store.transaction(store, fn -> :ok end)

      refute Store.in_transaction?(store)
    end

    test "is false again after the transaction raises", %{store: store} do
      assert_raise RuntimeError, "boom", fn ->
        Store.transaction(store, fn -> raise "boom" end)
      end

      refute Store.in_transaction?(store)
    end
  end

  describe "transaction/2" do
    @item_added_query [%{types: ["ItemAdded"]}]

    test "returns the function's value", %{store: store} do
      assert {:ok, :done} = Store.transaction(store, fn -> {:ok, :done} end)
      assert :ok = Store.transaction(store, fn -> :ok end)
    end

    test "keeps events appended inside a successful transaction", %{store: store} do
      assert :ok =
               Store.transaction(store, fn ->
                 append!(store, "ItemAdded")
                 :ok
               end)

      assert %{events: [%SequencedEvent{}]} = Store.read(store, @item_added_query)
    end

    test "keeps events appended before the function returns an error", %{store: store} do
      assert {:error, :nope} =
               Store.transaction(store, fn ->
                 append!(store, "ItemAdded")
                 {:error, :nope}
               end)

      assert %{events: [%SequencedEvent{}]} = Store.read(store, @item_added_query)
    end

    test "rolls back events appended before the function raises", %{store: store} do
      assert_raise RuntimeError, "boom", fn ->
        Store.transaction(store, fn ->
          append!(store, "ItemAdded")
          raise "boom"
        end)
      end

      assert %{events: []} = Store.read(store, @item_added_query)
    end

    test "rolls back a reactor checkpoint advanced before the function raises", %{store: store} do
      append!(store, "ItemAdded")
      handler = recording_handler(self(), "echo")

      reactor =
        StoredEventReactor.new(%{name: "echo", query: @item_added_query, handler: handler})

      assert_raise RuntimeError, "boom", fn ->
        Store.transaction(store, fn ->
          %ConsumeResult{processed: 1} = Store.consume(store, reactor)
          raise "boom"
        end)
      end

      assert %ConsumeResult{processed: 1} = Store.consume(store, reactor)
    end

    test "joins an ambient transaction instead of committing on its own", %{store: store} do
      assert_raise RuntimeError, "boom", fn ->
        Store.transaction(store, fn ->
          :ok =
            Store.transaction(store, fn ->
              append!(store, "ItemAdded")
              :ok
            end)

          raise "boom"
        end)
      end

      assert %{events: []} = Store.read(store, @item_added_query)
    end
  end

  defp append!(store, type, tags \\ []) do
    Store.append(store, [%Event{type: type, data: %{}, tags: tags}])
  end

  defp append_position!(store, type, tags \\ []) do
    {:ok, %{events: [%SequencedEvent{position: position}]}} = append!(store, type, tags)
    position
  end

  defp consume!(store, name, query) do
    %ConsumeResult{} =
      Store.consume(
        store,
        StoredEventReactor.new(%{
          name: name,
          query: query,
          handler: fn events -> {:ok, length(events)} end
        })
      )
  end

  defp recording_handler(target_pid, name) do
    fn events ->
      Enum.reduce_while(events, {:ok, 0}, fn seq, {:ok, count} ->
        send(target_pid, {:got, name, seq})
        {:cont, {:ok, count + 1}}
      end)
    end
  end

  describe "dump/1 and load/1" do
    test "round-trip a store back to one that reads the same events", %{store: store} do
      Store.append(store, [
        %Event{type: "PageCreated", data: %{"title" => "Page 1"}, tags: []},
        %Event{type: "PageCreated", data: %{"title" => "Page 2"}, tags: []}
      ])

      reloaded =
        store
        |> Store.dump()
        |> Store.load()

      assert Store.read(reloaded) == Store.read(store)
    end

    test "dump the backend module name", %{store: store} do
      %{"module" => module} = Store.dump(store)

      assert module == "Elixir.Ariadne.Flow.Store." <> module_suffix(store)
    end
  end

  defp module_suffix(%Store{module: module}) do
    module
    |> Module.split()
    |> List.last()
  end

  defp attach_telemetry(events) do
    parent = self()
    handler_id = "store-telemetry-test-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      events,
      fn name, measurements, metadata, _ ->
        send(parent, {:telemetry, name, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp assert_backend_metadata(Ariadne.Flow.Store.Postgres, metadata) do
    assert %{prefix: "postgres_store_test_schema", context: "default"} = metadata
  end

  defp assert_backend_metadata(_backend, _metadata), do: :ok
end
