defmodule Ariadne.Flow.ReactorRun do
  @moduledoc false
  alias Ariadne.Flow.ConsumeResult
  alias Ariadne.Flow.Event.Codec
  alias Ariadne.Flow.Reactor
  alias Ariadne.Flow.ReactorError
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.StoredEventReactor
  require Logger

  @enforce_keys [:reactor]
  defstruct [:reactor, metadata: %{}]

  @around_event_hint ":around_event must be a function of the event's envelope and a zero-arity function handling it, got: "
  @after_reactor_run_hint ":after_reactor_run must be a function of the reactor module, the checkpoint its run saved and the results of the events it committed, got: "

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
    after_run = after_run(reactor_module, Keyword.get(opts, :after_reactor_run), store)

    stored_event_reactor =
      StoredEventReactor.new(%{
        name: reactor.name,
        query: Reactor.query(reactor),
        handler: fn events -> run_handler(reactor, events, around_event) end
      })

    catch_up(store, stored_event_reactor, after_run)
  end

  def around_event!(nil), do: &handle_directly/2
  def around_event!(around_event) when is_function(around_event, 2), do: around_event

  def around_event!(around_event),
    do: raise(ArgumentError, @around_event_hint <> inspect(around_event))

  def after_reactor_run!(nil), do: nil

  def after_reactor_run!(after_reactor_run) when is_function(after_reactor_run, 3),
    do: after_reactor_run

  def after_reactor_run!(after_reactor_run),
    do: raise(ArgumentError, @after_reactor_run_hint <> inspect(after_reactor_run))

  defp after_run(reactor_module, after_reactor_run, store) do
    case after_reactor_run!(after_reactor_run) do
      nil ->
        fn _consume_result -> :ok end

      hook ->
        Store.ensure_after_commit!(store)
        &report_run(store, reactor_module, hook, &1)
    end
  end

  defp report_run(_store, _reactor_module, _hook, %ConsumeResult{processed: 0}), do: :ok

  defp report_run(store, reactor_module, hook, %ConsumeResult{} = result) do
    Store.after_commit(store, fn ->
      call_hook(hook, reactor_module, result.last_position, result.results)
    end)
  end

  defp call_hook(hook, reactor_module, position, results) do
    hook.(reactor_module, position, results)
  catch
    kind, value ->
      Logger.error(fn ->
        "[ariadne.flow] after_reactor_run for #{inspect(reactor_module)} at position " <>
          "#{position} failed: #{Exception.format(kind, value, __STACKTRACE__)}"
      end)
  end

  defp catch_up(store, %StoredEventReactor{name: name} = stored_event_reactor, after_run) do
    result = Store.consume(store, stored_event_reactor)
    after_run.(result)

    case result do
      %ConsumeResult{status: :error, last_position: position, failure: %{reason: reason}} ->
        {:error, %ReactorError{failures: [%{name: name, position: position, reason: reason}]}}

      %ConsumeResult{more?: true} ->
        catch_up(store, stored_event_reactor, after_run)

      %ConsumeResult{} ->
        :ok
    end
  end

  defp handle_directly(_envelope, handle), do: handle.()

  defp run_handler(reactor, events, around_event) do
    events
    |> Enum.reduce_while({:ok, []}, fn seq, {:ok, results} ->
      case handle(reactor, seq, around_event) do
        :ok ->
          {:cont, {:ok, [nil | results]}}

        {:ok, result} ->
          {:cont, {:ok, [result | results]}}

        {:error, reason} ->
          {:halt, {:error, results, %{event: seq, reason: reason}}}
      end
    end)
    |> in_position_order()
  end

  defp in_position_order({:ok, results}), do: {:ok, Enum.reverse(results)}

  defp in_position_order({:error, results, failure}),
    do: {:error, Enum.reverse(results), failure}

  defp handle(reactor, seq, around_event) do
    envelope = Codec.deserialize(seq)

    around_event.(envelope, fn -> Reactor.handle(reactor, envelope) end)
  end
end
