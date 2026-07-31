defmodule Ariadne.Flow.Test.Gwt.Helpers do
  import ExUnit.Assertions

  alias Ariadne.Flow.Reactor
  alias Ariadne.Flow.Store.Event.Codec
  alias Ariadne.Flow.Store.Event.Encoder

  def given(events) do
    Enum.map(events, fn
      %{event: event, metadata: metadata} -> envelope(event, metadata)
      event -> envelope(event, %{created_at: ~U[2000-01-01 12:00:00Z]})
    end)
  end

  # The same envelope `Codec.deserialize/1` yields for a stored event: the struct after
  # a JSON round-trip, plus the stored type and tags that filters match against.
  defp envelope(%type{} = event, metadata) do
    %{data: data, tags: tags} = Encoder.encode(event)

    store_data =
      data
      |> Jason.encode!()
      |> Jason.decode!()

    %{
      event: Encoder.decode(event, store_data, %{}),
      metadata: metadata,
      type: Codec.serialize_type(type),
      tags: tags
    }
  end

  def run_ok(%Reactor{} = reactor, given, expected) do
    for entry <- given do
      assert :ok == Reactor.handle(reactor, entry)
    end

    maybe_call(expected)
  end

  def run_ok(%module{} = reducer, given, expected) do
    assert_ok(module.reduce(reducer, given), expected)
  end

  def run_err(%Reactor{} = reactor, given, expected) do
    for entry <- given do
      assert_err(Reactor.handle(reactor, entry), expected)
    end
  end

  def run_err(%module{} = reducer, given, expected) do
    assert_err(module.reduce(reducer, given), expected)
  end

  def run_res(%module{} = reducer, given, expected) do
    assert_res(module.reduce(reducer, given), expected)
  end

  def assert_ok(result, expected) when is_function(expected, 1) do
    assert {:ok, value} = result
    expected.(value)
  end

  def assert_ok(result, expected), do: assert({:ok, expected} == result)

  def assert_err(result, expected) when is_function(expected, 1) do
    assert {:error, reason} = result
    expected.(reason)
  end

  def assert_err(result, expected), do: assert({:error, expected} == result)

  def assert_res(result, expected) when is_function(expected, 1), do: expected.(result)
  def assert_res(result, expected), do: assert(expected == result)

  def maybe_call(fun) when is_function(fun, 0), do: fun.()
  def maybe_call(_), do: :ok

  defp extract_test_cases(maybe_block) do
    case maybe_block do
      {:__block__, _, test_cases} -> test_cases
      single_case -> [single_case]
    end
  end

  defmacro gwt(description, do: block) do
    test_quotes = Enum.map(extract_test_cases(block), &build_case/1)

    quote do
      describe unquote(description) do
        (unquote_splicing(test_quotes))
      end
    end
  end

  defp build_case({kind, _, [label, opts]}) when kind in [:ok, :err, :res] do
    runner =
      case kind do
        :ok -> :run_ok
        :err -> :run_err
        :res -> :run_res
      end

    quote do
      test unquote(label) do
        given = given(unquote(opts[:given] || []))

        unquote(__MODULE__).unquote(runner)(
          unquote(opts[:when]),
          given,
          unquote(opts[:then])
        )
      end
    end
  end
end
