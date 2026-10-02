defmodule Ariadne.Flow.ReactorRun do
  @moduledoc false
  alias Ariadne.Flow.ConsumeResult
  alias Ariadne.Flow.Event.Codec
  alias Ariadne.Flow.Reactor
  alias Ariadne.Flow.ReactorError
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.StoredEventReactor

  @enforce_keys [:reactor]
  defstruct [:reactor, metadata: %{}]

  @around_event_hint ":around_event must be a function of the event's envelope and a zero-arity function handling it, got: "

  def new(%{reactor: reactor_module} = attrs) do
    %__MODULE__{reactor: reactor_module, metadata: Map.get(attrs, :metadata, %{})}
  end

  def sync?(%__MODULE__{reactor: reactor_module}), do: reactor_module.reactor().sync

  def name(%__MODULE__{reactor: reactor_module}), do: reactor_module.reactor().name

  def dump(%__MODULE__{reactor: reactor, metadata: metadata}) do
    %{"reactor" => Atom.to_string(reactor), "metadata" => metadata}
  end

  def load(%{"reactor" => reactor} = payload) do
    new(%{
      reactor: String.to_existing_atom(reactor),
      metadata: Map.get(payload, "metadata", %{})
    })
  end

  def execute(%__MODULE__{reactor: reactor_module}, %Store{} = store, opts \\ []) do
    reactor = reactor_module.reactor()
    around_event = around_event!(Keyword.get(opts, :around_event))

    stored_event_reactor =
      StoredEventReactor.new(%{
        name: reactor.name,
        query: Reactor.query(reactor),
        handler: fn events -> run_handler(reactor, events, around_event) end
      })

    catch_up(store, stored_event_reactor)
  end

  def around_event!(nil), do: &handle_directly/2
  def around_event!(around_event) when is_function(around_event, 2), do: around_event

  def around_event!(around_event),
    do: raise(ArgumentError, @around_event_hint <> inspect(around_event))

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

  defp handle_directly(_envelope, handle), do: handle.()

  defp run_handler(reactor, events, around_event) do
    Enum.reduce_while(events, {:ok, 0}, fn seq, {:ok, count} ->
      case handle(reactor, seq, around_event) do
        :ok ->
          {:cont, {:ok, count + 1}}

        {:error, reason} ->
          {:halt, {:error, count, %{event: seq, reason: reason}}}
      end
    end)
  end

  defp handle(reactor, seq, around_event) do
    envelope = Codec.deserialize(seq)

    around_event.(envelope, fn -> Reactor.handle(reactor, envelope) end)
  end
end
