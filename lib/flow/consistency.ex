defmodule Ariadne.Flow.Consistency do
  @moduledoc false
  alias Ariadne.Flow.ConsistencyTimeoutError
  alias Ariadne.Flow.Reactor
  alias Ariadne.Flow.Store

  @default_timeout 5_000
  @first_interval 2
  @max_interval 100

  @enforce_keys [:reactors, :timeout]
  defstruct @enforce_keys

  @type t :: %__MODULE__{reactors: [%Reactor{}], timeout: non_neg_integer()}

  def new(%{reactors: reactors} = attrs) do
    %__MODULE__{
      reactors: awaited(reactors, Map.get(attrs, :nested, false)),
      timeout: validated_timeout(Map.get(attrs, :await_timeout))
    }
  end

  def await(%__MODULE__{reactors: []}, %Store{}, _events), do: :ok

  def await(%__MODULE__{}, %Store{}, []), do: :ok

  def await(%__MODULE__{reactors: reactors, timeout: timeout}, %Store{} = store, events) do
    case Enum.flat_map(reactors, &target(&1, events)) do
      [] -> :ok
      targets -> await_targets(store, targets, timeout)
    end
  end

  defp target(%Reactor{name: name} = reactor, events) do
    case Enum.filter(events, &Reactor.matches?(reactor, &1)) do
      [] -> []
      matching -> [%{name: name, position: highest_position(matching)}]
    end
  end

  defp await_targets(store, targets, timeout) do
    metadata = %{awaited: targets, timeout: timeout}

    :telemetry.span([:ariadne, :flow, :dispatch, :await], metadata, fn ->
      wait = %{deadline: now() + timeout, interval: @first_interval, polls: 0}

      store
      |> poll(targets, wait)
      |> report(metadata)
    end)
  end

  defp poll(store, targets, %{deadline: deadline, interval: interval} = wait) do
    pending = Enum.reject(targets, &confirmed?(store, &1))
    wait = %{wait | polls: wait.polls + 1}

    cond do
      pending == [] ->
        {:ok, wait}

      now() >= deadline ->
        {:timeout, pending, wait}

      true ->
        Process.sleep(min(interval, max(deadline - now(), 0)))
        poll(store, pending, %{wait | interval: min(interval * 2, @max_interval)})
    end
  end

  defp report({:ok, %{polls: polls}}, metadata) do
    {:ok, %{polls: polls}, Map.put(metadata, :result, :confirmed)}
  end

  defp report({:timeout, pending, %{polls: polls}}, %{timeout: timeout} = metadata) do
    error = ConsistencyTimeoutError.exception(unconfirmed: pending, timeout: timeout)

    {{:error, error}, %{polls: polls},
     Map.merge(metadata, %{result: :timeout, unconfirmed: pending})}
  end

  defp confirmed?(store, %{name: name, position: position}) do
    case Store.checkpoint(store, name) do
      nil -> false
      checkpoint -> checkpoint >= position
    end
  end

  defp awaited(_reactors, true), do: []

  defp awaited(reactors, false) do
    Enum.flat_map(reactors, fn reactor_module ->
      case reactor_module.reactor() do
        %Reactor{sync: true} = reactor -> [reactor]
        %Reactor{} -> []
      end
    end)
  end

  defp validated_timeout(nil), do: @default_timeout
  defp validated_timeout(timeout) when is_integer(timeout) and timeout >= 0, do: timeout

  defp validated_timeout(timeout) do
    raise ArgumentError,
          ":await_timeout must be a non-negative number of milliseconds, got: #{inspect(timeout)}"
  end

  defp highest_position(events) do
    events
    |> Enum.map(& &1.metadata.position)
    |> Enum.max()
  end

  defp now, do: System.monotonic_time(:millisecond)
end
