defmodule Ariadne.Flow.Event.DefaultEncoderTest do
  use ExUnit.Case, async: true

  alias Ariadne.Flow.Event

  defmodule Nested do
    @derive Event
    defstruct [:label]

    def tags(_), do: []
  end

  defmodule Sample do
    @derive Event
    defstruct [:title, :count, :ratio, :active, :missing, :value]

    def tags(_), do: ["event"]
  end

  defp round_trip(%module{} = event) do
    %{data: data} = Event.encode(event)

    store_data =
      data
      |> Jason.encode!()
      |> Jason.decode!()

    Event.decode(struct(module), store_data, %{})
  end

  describe "primitive fields" do
    test "round-trip unchanged" do
      event = %Sample{title: "Onboarding", count: 3, ratio: 1.5, active: true, missing: nil}

      assert round_trip(event) == event
    end

    test "lists of primitives round-trip unchanged" do
      event = %Sample{value: ["a", 1, true, nil]}

      assert round_trip(event) == event
    end

    test "string-keyed maps of primitives round-trip unchanged" do
      event = %Sample{value: %{"role" => "admin", "level" => 2}}

      assert round_trip(event) == event
    end
  end

  describe "non-primitive fields are rejected" do
    test "atoms" do
      assert_raise ArgumentError, ~r/:value/, fn -> Event.encode(%Sample{value: :published}) end
    end

    test "DateTime" do
      assert_raise ArgumentError, fn ->
        Event.encode(%Sample{value: ~U[2025-09-10 11:09:49.647209Z]})
      end
    end

    test "Date" do
      assert_raise ArgumentError, fn -> Event.encode(%Sample{value: ~D[2025-09-10]}) end
    end

    test "nested structs" do
      assert_raise ArgumentError, fn ->
        Event.encode(%Sample{value: %Nested{label: "ready"}})
      end
    end

    test "tuples" do
      assert_raise ArgumentError, fn -> Event.encode(%Sample{value: {1, 2}}) end
    end

    test "maps with non-string keys" do
      assert_raise ArgumentError, fn -> Event.encode(%Sample{value: %{count: 3}}) end
    end

    test "lists containing a non-primitive value" do
      assert_raise ArgumentError, fn -> Event.encode(%Sample{value: ["ok", :nope]}) end
    end

    test "maps with a non-primitive value" do
      assert_raise ArgumentError, fn ->
        Event.encode(%Sample{value: %{"at" => ~D[2025-09-10]}})
      end
    end
  end
end
