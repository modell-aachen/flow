defmodule Ariadne.Flow.Event.TypeTest do
  use ExUnit.Case, async: true

  alias Ariadne.Flow.Event
  alias Ariadne.Flow.Event.Type

  defmodule DefaultTypeEvent do
    @derive Event
    defstruct [:label]

    def tags(_), do: []
  end

  defmodule PinnedTypeEvent do
    @derive {Event, type: "type-test-pinned"}
    defstruct [:label]

    def tags(_), do: []
  end

  defmodule PinnedCustomEncoderEvent do
    @derive {Event, to: Event.DefaultEncoder, type: "type-test-custom-encoder"}
    defstruct [:label]

    def tags(_), do: []
  end

  defmodule PinnedByImplEvent do
    defstruct [:label]

    def tags(_), do: []

    defimpl Event do
      alias Ariadne.Flow.Event.DefaultEncoder

      def type, do: "type-test-impl"

      def encode(event), do: DefaultEncoder.encode(event)

      def decode(event, store_data, metadata),
        do: DefaultEncoder.decode(event, store_data, metadata)
    end
  end

  defmodule UnpinnedByImplEvent do
    defstruct [:label]

    def tags(_), do: []

    defimpl Event do
      alias Ariadne.Flow.Event.DefaultEncoder

      def encode(event), do: DefaultEncoder.encode(event)

      def decode(event, store_data, metadata),
        do: DefaultEncoder.decode(event, store_data, metadata)
    end
  end

  defmodule MistypedEvent do
    defstruct [:label]
  end

  defmodule Module.concat(Event, MistypedEvent) do
    def type, do: 42
  end

  describe "the type an event is stored under" do
    test "is the module name unless the event declares one" do
      assert "Ariadne.Flow.Event.TypeTest.DefaultTypeEvent" == Type.of(DefaultTypeEvent)
    end

    test "is the type pinned on the derive" do
      assert "type-test-pinned" == Type.of(PinnedTypeEvent)
    end

    test "is pinned independently of the encoder the derive delegates to" do
      assert "type-test-custom-encoder" == Type.of(PinnedCustomEncoderEvent)
    end

    test "is the type a hand-written implementation declares" do
      assert "type-test-impl" == Type.of(PinnedByImplEvent)
    end

    test "falls back to the module name for a hand-written implementation declaring none" do
      assert "Ariadne.Flow.Event.TypeTest.UnpinnedByImplEvent" ==
               Type.of(UnpinnedByImplEvent)
    end

    test "is the module name for a module with no encoder at all" do
      assert "Ariadne.Flow.Event.TypeTest" == Type.of(__MODULE__)
    end

    test "has to be a string" do
      assert_raise ArgumentError, ~r/declares the stored event type 42/, fn ->
        Type.of(MistypedEvent)
      end
    end
  end

  describe "the module a stored type belongs to" do
    test "is found by module name for an event declaring no type" do
      assert DefaultTypeEvent ==
               Type.module!("Ariadne.Flow.Event.TypeTest.DefaultTypeEvent")
    end

    test "is found by the type the event declares" do
      assert PinnedTypeEvent == Type.module!("type-test-pinned")
      assert PinnedByImplEvent == Type.module!("type-test-impl")
    end

    test "is found for an event module compiled after the type was first resolved" do
      Type.module!("type-test-pinned")

      Code.compile_string("""
      defmodule #{inspect(__MODULE__)}.LateEvent do
        @derive {Ariadne.Flow.Event, type: "type-test-late"}
        defstruct [:label]

        def tags(_), do: []
      end
      """)

      assert Module.concat(__MODULE__, LateEvent) == Type.module!("type-test-late")
    end

    test "is missing for a type no event module declares" do
      assert_raise ArgumentError, ~r/No event module declares the stored event type/, fn ->
        Type.module!("type-test-nobody-declares-this")
      end
    end
  end

  describe "the index of stored types" do
    test "maps every declared type to its event module" do
      assert %{"a" => DefaultTypeEvent, "b" => PinnedTypeEvent} ==
               Type.index([{"a", DefaultTypeEvent}, {"b", PinnedTypeEvent}])
    end

    test "takes the same event module declaring a type more than once" do
      assert %{"a" => DefaultTypeEvent} ==
               Type.index([{"a", DefaultTypeEvent}, {"a", DefaultTypeEvent}])
    end

    test "rejects two event modules declaring the same type" do
      assert_raise ArgumentError, ~r/DefaultTypeEvent and .*PinnedTypeEvent both declare/s, fn ->
        Type.index([{"a", DefaultTypeEvent}, {"a", PinnedTypeEvent}])
      end
    end
  end
end
