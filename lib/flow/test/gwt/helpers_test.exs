defmodule Ariadne.Flow.Test.Gwt.HelpersTest do
  use Ariadne.Flow.Test.Gwt, async: true
  alias Ariadne.Flow.Envelope

  defmodule ExampleEvent do
    @derive {Ariadne.Flow.Event, type: "example-event"}
    defstruct [:id]

    def tags(%{id: id}), do: ["thing:#{id}"]
  end

  test "given/1 builds the Envelope a stored event is deserialized into" do
    assert [
             %Envelope{
               event: %ExampleEvent{id: 1},
               type: "example-event",
               tags: ["thing:1"],
               metadata: %{created_at: ~U[2000-01-01 12:00:00Z], position: 1}
             }
           ] = given([%ExampleEvent{id: 1}])
  end

  test "given/1 positions events by the order they are given, as the store would" do
    assert [%Envelope{metadata: %{position: 1}}, %Envelope{metadata: %{position: 2}}] =
             given([%ExampleEvent{id: 1}, %ExampleEvent{id: 2}])
  end

  test "given/1 keeps supplied metadata, filling in only the position it did not supply" do
    assert [%Envelope{metadata: %{offset: 7, position: 1}}] =
             given([%{event: %ExampleEvent{id: 1}, metadata: %{offset: 7}}])

    assert [%Envelope{metadata: %{position: 42}}] =
             given([%{event: %ExampleEvent{id: 1}, metadata: %{position: 42}}])
  end

  test "given/1 passes an Envelope through, so a stored type survives being fed back in" do
    legacy = %Envelope{
      event: %ExampleEvent{id: 1},
      metadata: %{position: 9},
      type: "the-type-it-was-written-under",
      tags: ["thing:legacy"]
    }

    assert [^legacy] = given([legacy])
  end
end
