defmodule Ariadne.Flow.AttemptsTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.AppendConditionError
  alias Ariadne.Flow.Attempts

  # A dispatch stand-in: it returns the given results in order, one per call, so what
  # `run/2` retried is visible in how many of them were consumed.
  defp results(results) do
    {:ok, agent} = Agent.start_link(fn -> results end)

    fn -> Agent.get_and_update(agent, fn [result | rest] -> {result, rest} end) end
  end

  defp conflict, do: {:error, %AppendConditionError{}}

  describe "new/1" do
    test "defaults to three attempts, so a loser can lose twice before giving up" do
      assert %Attempts{limit: 3} = Attempts.new()
    end

    test "takes the bound from the :attempts option, one meaning no retry" do
      assert %Attempts{limit: 5} = Attempts.new(%{attempts: 5})
      assert %Attempts{limit: 1} = Attempts.new(%{attempts: 1})
    end

    test "allows a single attempt only when the dispatch is nested in an outer transaction" do
      assert %Attempts{limit: 1} = Attempts.new(%{nested: true})
      assert %Attempts{limit: 1} = Attempts.new(%{attempts: 5, nested: true})
    end

    test "raises on a bound that is not a positive integer, nesting or not" do
      for attempts <- [0, -1, 2.5, :infinity, "3"] do
        assert_raise ArgumentError, ~r/:attempts must be a positive integer/, fn ->
          Attempts.new(%{attempts: attempts})
        end

        assert_raise ArgumentError, fn -> Attempts.new(%{attempts: attempts, nested: true}) end
      end
    end
  end

  describe "run/2" do
    test "returns a first-time success and the one attempt it took" do
      assert {{:ok, :appended}, 1} = Attempts.run(Attempts.new(), results([{:ok, :appended}]))
    end

    test "retries a conflict and returns the attempt that succeeded" do
      fun = results([conflict(), {:ok, :appended}])

      assert {{:ok, :appended}, 2} = Attempts.run(Attempts.new(), fun)
    end

    test "returns the conflict once the attempts are exhausted" do
      fun = results([conflict(), conflict(), conflict()])

      assert {{:error, %AppendConditionError{}}, 3} = Attempts.run(Attempts.new(), fun)
    end

    test "makes no more attempts than the bound allows" do
      fun = results([conflict(), {:ok, :appended}])

      assert {{:error, %AppendConditionError{}}, 1} =
               Attempts.run(Attempts.new(%{attempts: 1}), fun)
    end

    test "does not retry a refusal the command decided on its own" do
      fun = results([{:error, :course_full}, {:ok, :appended}])

      assert {{:error, :course_full}, 1} = Attempts.run(Attempts.new(), fun)
    end

    test "lets a raise out instead of retrying it" do
      fun = fn -> raise "kaboom" end

      assert_raise RuntimeError, "kaboom", fn -> Attempts.run(Attempts.new(), fun) end
    end
  end
end
