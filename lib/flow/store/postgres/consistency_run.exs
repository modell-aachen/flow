# credo:disable-for-this-file Credo.Check.Refactor.IoPuts

defmodule Ariadne.Flow.Store.Postgres.ConsistencyRun do
  alias Ariadne.Flow.Store.Postgres
  alias Ariadne.Flow.Store.Speedrun
  alias __MODULE__
  alias Ecto.UUID

  def run do
    {:ok, _} = Speedrun.Repo.start_link()
    store = Postgres.init(repo: Speedrun.Repo)

    iterations = 500
    IO.puts("Check consistency for #{iterations} iterations...")

    tests = [
      ConsistencyRun.TypeExclusivity,
      ConsistencyRun.TagExclusivity
    ]

    run_all_tests(store, iterations, tests)

    IO.puts("✔️ All iterations completed successfully. No inconsistencies detected.")
  end

  defp run_all_tests(store, iterations, tests) do
    Enum.each(1..iterations, fn _ -> run_tests_iteration(store, tests) end)
  end

  defp run_tests_iteration(store, tests) do
    Enum.each(tests, fn test ->
      run_single_test(store, test)
    end)
  end

  defp run_single_test(store, test) do
    id = UUID.generate()

    [result_a, result_b] =
      [:a, :b]
      |> Enum.map(&run_worker(&1, test, id))
      |> Task.await_many()

    case test.evaluate(store, id, result_a, result_b) do
      {:error, reason} ->
        IO.puts(:stderr, "❌ Error in #{test}: #{reason}")
        System.halt(1)

      :ok ->
        :ok
    end
  end

  defp run_worker(run, module, id) do
    Task.async(fn ->
      store = Postgres.init(repo: Speedrun.Repo)
      module.run(run, store, id)
    end)
  end
end

Ariadne.Flow.Store.Postgres.ConsistencyRun.run()
