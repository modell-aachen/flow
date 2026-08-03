defmodule Ariadne.Flow.ReactorRun do
  @moduledoc false
  alias Ariadne.Flow.ConsumeResult
  alias Ariadne.Flow.Reactor
  alias Ariadne.Flow.ReactorError
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Event.Codec
  alias Ariadne.Flow.Store.StoredEventReactor

  @enforce_keys [:reactor, :start_after_position]
  defstruct [:reactor, :start_after_position, metadata: %{}]

  def new(%{reactor: reactor_module} = attrs) do
    reactor = reactor_module.reactor()

    %__MODULE__{
      reactor: reactor_module,
      start_after_position: Map.get(attrs, :start_after_position, reactor.start_after_position),
      metadata: Map.get(attrs, :metadata, %{})
    }
  end

  def sync?(%__MODULE__{reactor: reactor_module}), do: reactor_module.reactor().sync

  def name(%__MODULE__{reactor: reactor_module}), do: reactor_module.reactor().name

  def dump(%__MODULE__{reactor: reactor, start_after_position: position, metadata: metadata}) do
    %{
      "reactor" => Atom.to_string(reactor),
      "start_after_position" => position,
      "metadata" => metadata
    }
  end

  def load(%{"reactor" => reactor, "start_after_position" => position} = payload) do
    new(%{
      reactor: String.to_existing_atom(reactor),
      start_after_position: position,
      metadata: Map.get(payload, "metadata", %{})
    })
  end

  def execute(
        %__MODULE__{reactor: reactor_module, start_after_position: position},
        %Store{} = store
      ) do
    reactor = reactor_module.reactor()

    stored_event_reactor =
      StoredEventReactor.new(%{
        name: reactor.name,
        query: Reactor.query(reactor),
        handler: fn events -> run_handler(reactor, events) end,
        start_after_position: position
      })

    catch_up(store, stored_event_reactor)
  end

  defp catch_up(store, %StoredEventReactor{name: name} = stored_event_reactor) do
    case Store.consume(store, stored_event_reactor) do
      %ConsumeResult{status: :error, last_position: position, failure: %{reason: reason}} ->
        {:error, %ReactorError{failures: [%{name: name, position: position, reason: reason}]}}

      %ConsumeResult{more?: true} ->
        catch_up(store, stored_event_reactor)

      %ConsumeResult{} ->
        :ok
    end
  end

  defp run_handler(reactor, events) do
    Enum.reduce_while(events, {:ok, 0}, fn seq, {:ok, count} ->
      case Reactor.handle(reactor, Codec.deserialize(seq)) do
        :ok ->
          {:cont, {:ok, count + 1}}

        {:error, reason} ->
          {:halt, {:error, count, %{event: seq, reason: reason}}}
      end
    end)
  end
end
