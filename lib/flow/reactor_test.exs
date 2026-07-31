defmodule Ariadne.Flow.ReactorTest do
  use Ariadne.Flow.Test.Gwt, async: true
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Reactor

  defmodule ExampleEvent do
    @derive Ariadne.Flow.Store.Event.Encoder
    defstruct [:id]

    def tags(%{id: id}), do: ["thing:#{id}"]
  end

  defmodule UnrelatedEvent do
    @derive Ariadne.Flow.Store.Event.Encoder
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

  test "handle/2 returns the user function's result" do
    reactor = build_reactor(fn _, _ -> {:error, :boom} end)

    assert {:error, :boom} ==
             Reactor.handle(reactor, %{event: %ExampleEvent{id: 1}, metadata: %{}})
  end

  test "handle/2 skips events whose type does not match the filter" do
    reactor = build_reactor(fn _, _ -> raise "should not be called" end)

    assert :ok == Reactor.handle(reactor, %{event: %UnrelatedEvent{}, metadata: %{}})
  end

  test "handle/2 skips events whose tags do not satisfy the filter" do
    reactor =
      Reactor.new(
        %{name: "selective", filter: %{types: [ExampleEvent], tags: ["thing:special"]}},
        fn _, _ -> raise "should not be called" end
      )

    assert :ok == Reactor.handle(reactor, %{event: %ExampleEvent{id: 1}, metadata: %{}})
  end

  test "new/2 defaults start_after_position to 0" do
    assert %Reactor{start_after_position: 0} = build_reactor(fn _, _ -> :ok end)
  end

  test "new/2 keeps the provided start_after_position" do
    reactor =
      Reactor.new(
        %{name: "example", filter: %{types: [ExampleEvent]}, start_after_position: 42},
        fn _, _ -> :ok end
      )

    assert %Reactor{start_after_position: 42} = reactor
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

  test "query/1 exposes the filter as a Query.Item list" do
    reactor =
      Reactor.new(
        %{name: "example", filter: %{types: [ExampleEvent], tags: ["thing:1"]}},
        fn _, _ -> :ok end
      )

    assert [%Query.Item{types: [ExampleEvent], tags: ["thing:1"]}] = Reactor.query(reactor)
  end

  test "new/2 validates the filter via Query.Item" do
    assert_raise RuntimeError, fn ->
      Reactor.new(%{name: "example", filter: %{types: []}}, fn _, _ -> :ok end)
    end
  end

  test "new/2 rejects a filter asking for only the last event" do
    assert_raise RuntimeError, ~r/only_last_event/, fn ->
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
end
