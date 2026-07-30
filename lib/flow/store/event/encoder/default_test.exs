defmodule Ariadne.Flow.Store.Event.Encoder.DefaultTest do
  use ExUnit.Case, async: true

  alias Ariadne.Flow.Store.Event.Encoder

  defmodule Nested do
    @derive Encoder
    defstruct [:label]

    def tags(_), do: []
  end

  defmodule Event do
    @derive Encoder
    defstruct [:title, :count, :ratio, :active, :missing, :value]

    def tags(_), do: ["event"]
  end

  defp round_trip(%module{} = event) do
    %{data: data} = Encoder.encode(event)

    store_data =
      data
      |> Jason.encode!()
      |> Jason.decode!()

    Encoder.decode(struct(module), store_data, %{})
  end

  describe "primitive fields" do
    test "round-trip unchanged" do
      event = %Event{title: "Onboarding", count: 3, ratio: 1.5, active: true, missing: nil}

      assert round_trip(event) == event
    end

    test "lists of primitives round-trip unchanged" do
      event = %Event{value: ["a", 1, true, nil]}

      assert round_trip(event) == event
    end

    test "string-keyed maps of primitives round-trip unchanged" do
      event = %Event{value: %{"role" => "admin", "level" => 2}}

      assert round_trip(event) == event
    end
  end

  describe "non-primitive fields are rejected" do
    test "atoms" do
      assert_raise ArgumentError, ~r/:value/, fn -> Encoder.encode(%Event{value: :published}) end
    end

    test "DateTime" do
      assert_raise ArgumentError, fn ->
        Encoder.encode(%Event{value: ~U[2025-09-10 11:09:49.647209Z]})
      end
    end

    test "Date" do
      assert_raise ArgumentError, fn -> Encoder.encode(%Event{value: ~D[2025-09-10]}) end
    end

    test "nested structs" do
      assert_raise ArgumentError, fn ->
        Encoder.encode(%Event{value: %Nested{label: "ready"}})
      end
    end

    test "tuples" do
      assert_raise ArgumentError, fn -> Encoder.encode(%Event{value: {1, 2}}) end
    end

    test "maps with non-string keys" do
      assert_raise ArgumentError, fn -> Encoder.encode(%Event{value: %{count: 3}}) end
    end

    test "lists containing a non-primitive value" do
      assert_raise ArgumentError, fn -> Encoder.encode(%Event{value: ["ok", :nope]}) end
    end

    test "maps with a non-primitive value" do
      assert_raise ArgumentError, fn ->
        Encoder.encode(%Event{value: %{"at" => ~D[2025-09-10]}})
      end
    end
  end
end
