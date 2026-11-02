defmodule Ariadne.Flow.ReactorTest do
  use Ariadne.Flow.Test.Gwt, async: true
  alias Ariadne.Flow.Envelope
  alias Ariadne.Flow.Filter
  alias Ariadne.Flow.Reactor
  alias Ariadne.Flow.Test.Gwt.Helpers

  defmodule ExampleEvent do
    @derive Ariadne.Flow.Event
    defstruct [:id]

    def tags(%{id: id}), do: ["thing:#{id}"]
  end

  defmodule UnrelatedEvent do
    @derive Ariadne.Flow.Event
    defstruct []

    def tags(_), do: []
  end

  defp build_reactor(handler) do
    Reactor.new(%{name: "example", filter: %{types: [ExampleEvent]}}, handler)
  end

  defp echo_reactor do
    test_pid = self()

    build_reactor(fn
      %ExampleEvent{id: 0}, _metadata ->
        {:error, :zero_not_allowed}

      event, metadata ->
        send(test_pid, {:got, event, metadata})
        :ok
    end)
  end

  defp tagging_reactor do
    build_reactor(fn %ExampleEvent{id: id}, _metadata -> {:ok, {:seen, id}} end)
  end

  gwt "echo reactor" do
    ok("forwards a single event to the test process",
      given: [%ExampleEvent{id: 1}],
      when: echo_reactor(),
      then: fn -> assert_received {:got, %ExampleEvent{id: 1}, _} end
    )

    ok("forwards multiple events in order",
      given: [%ExampleEvent{id: 1}, %ExampleEvent{id: 2}],
      when: echo_reactor(),
      then: fn ->
        assert_received {:got, %ExampleEvent{id: 1}, _}
        assert_received {:got, %ExampleEvent{id: 2}, _}
      end
    )

    err("rejects the sentinel id zero",
      given: [%ExampleEvent{id: 0}],
      when: echo_reactor(),
      then: :zero_not_allowed
    )
  end

  gwt "results handed to then" do
    ok("carries a handler's result with the event's envelope and position",
      given: [%ExampleEvent{id: 1}],
      when: tagging_reactor(),
      then: fn %{results: results} ->
        assert [
                 %{
                   envelope: %Envelope{event: %ExampleEvent{id: 1}},
                   position: 1,
                   result: {:seen, 1}
                 }
               ] = results
      end
    )

    ok("keeps each event's result apart, in position order",
      given: [%ExampleEvent{id: 1}, %ExampleEvent{id: 2}],
      when: tagging_reactor(),
      then: fn %{results: results} ->
        assert [
                 %{position: 1, result: {:seen, 1}},
                 %{position: 2, result: {:seen, 2}}
               ] = results
      end
    )

    ok("reports nil for a handler that answered :ok",
      given: [%ExampleEvent{id: 1}],
      when: build_reactor(fn _, _ -> :ok end),
      then: fn %{results: results} ->
        assert [%{position: 1, result: nil}] = results
      end
    )

    ok("leaves out events the filter does not match",
      given: [%ExampleEvent{id: 1}, %UnrelatedEvent{}, %ExampleEvent{id: 3}],
      when: tagging_reactor(),
      then: fn %{results: results} ->
        assert [
                 %{position: 1, result: {:seen, 1}},
                 %{position: 3, result: {:seen, 3}}
               ] = results
      end
    )
  end

  test "an ok case still calls a then of arity 0 when the handler returns a result" do
    Helpers.run_ok(tagging_reactor(), given([%ExampleEvent{id: 1}]), fn ->
      send(self(), :then_called)
    end)

    assert_received :then_called
  end

  test "an ok case reports no position for a hand-built envelope that carries none" do
    [%Envelope{} = numbered] = given([%ExampleEvent{id: 1}])
    unnumbered = %Envelope{numbered | metadata: %{}}

    Helpers.run_ok(tagging_reactor(), given([unnumbered]), fn %{results: results} ->
      send(self(), {:results, results})
    end)

    assert_received {:results, [%{position: nil, result: {:seen, 1}}]}
  end

  test "handle/2 returns the user function's result" do
    reactor = build_reactor(fn _, _ -> {:error, :boom} end)

    assert {:error, :boom} == Reactor.handle(reactor, envelope(%ExampleEvent{id: 1}))
  end

  test "handle/2 skips events whose type does not match the filter" do
    reactor = build_reactor(fn _, _ -> raise "should not be called" end)

    assert :ok == Reactor.handle(reactor, envelope(%UnrelatedEvent{}))
  end

  test "handle/2 skips events whose tags do not satisfy the filter" do
    reactor =
      Reactor.new(
        %{name: "selective", filter: %{types: [ExampleEvent], tags: ["thing:special"]}},
        fn _, _ -> raise "should not be called" end
      )

    assert :ok == Reactor.handle(reactor, envelope(%ExampleEvent{id: 1}))
  end

  test "matches?/2 answers whether the reactor reacts to an event at all" do
    reactor = build_reactor(fn _, _ -> :ok end)

    assert Reactor.matches?(reactor, envelope(%ExampleEvent{id: 1}))
    refute Reactor.matches?(reactor, envelope(%UnrelatedEvent{}))
  end

  test "matches?/2 takes the filter's tags into account" do
    reactor =
      Reactor.new(
        %{name: "selective", filter: %{types: [ExampleEvent], tags: ["thing:special"]}},
        fn _, _ -> :ok end
      )

    refute Reactor.matches?(reactor, envelope(%ExampleEvent{id: 1}))
    assert Reactor.matches?(reactor, envelope(%ExampleEvent{id: "special"}))
  end

  test "new/2 defaults start_after_position to :head, so history is opt-in" do
    assert %Reactor{start_after_position: :head} = build_reactor(fn _, _ -> :ok end)
  end

  test "new/2 keeps a declared position, 0 asking for the store's whole history" do
    assert %Reactor{start_after_position: 42} = start_after(42)
    assert %Reactor{start_after_position: 0} = start_after(0)
  end

  test "new/2 rejects a start_after_position that is neither :head nor a position" do
    assert_raise ArgumentError, ~r/:head or a non-negative position/, fn -> start_after(-1) end
    assert_raise ArgumentError, ~r/:head or a non-negative position/, fn -> start_after(:tail) end
  end

  test "new/2 defaults sync to false" do
    assert %Reactor{sync: false} = build_reactor(fn _, _ -> :ok end)
  end

  test "new/2 keeps the provided sync flag" do
    reactor =
      Reactor.new(
        %{name: "example", filter: %{types: [ExampleEvent]}, sync: true},
        fn _, _ -> :ok end
      )

    assert %Reactor{sync: true} = reactor
  end

  test "query/1 exposes the filter as a one-element list holding serialized types" do
    reactor =
      Reactor.new(
        %{name: "example", filter: %{types: [ExampleEvent], tags: ["thing:1"]}},
        fn _, _ -> :ok end
      )

    assert [%Filter{types: ["Ariadne.Flow.ReactorTest.ExampleEvent"], tags: ["thing:1"]}] =
             Reactor.query(reactor)
  end

  test "new/2 validates the filter" do
    assert_raise ArgumentError, ~r/non-empty list/, fn ->
      Reactor.new(%{name: "example", filter: %{types: []}}, fn _, _ -> :ok end)
    end
  end

  test "new/2 rejects a filter asking for only the last event" do
    assert_raise ArgumentError, ~r/only_last_event/, fn ->
      Reactor.new(
        %{name: "example", filter: %{types: [ExampleEvent], only_last_event: true}},
        fn _, _ -> :ok end
      )
    end
  end

  test "new/2 requires a 2-arity handler function" do
    assert_raise FunctionClauseError, fn ->
      new(%{name: "example", filter: %{types: [ExampleEvent]}}, fn _ -> :ok end)
    end
  end

  test "new/2 requires a binary name" do
    assert_raise FunctionClauseError, fn ->
      new(%{name: :example, filter: %{types: [ExampleEvent]}}, fn _, _ -> :ok end)
    end
  end

  defp new(params, handler) do
    args = [params, handler]
    apply(Reactor, :new, args)
  end

  defp start_after(position) do
    Reactor.new(
      %{name: "example", filter: %{types: [ExampleEvent]}, start_after_position: position},
      fn _, _ -> :ok end
    )
  end

  defp envelope(event), do: hd(given([event]))
end
