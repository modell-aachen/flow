# credo:disable-for-this-file Credo.Check.Refactor.IoPuts

alias Ariadne.Flow.Store.Speedrun

num_workers = 10
duration_s = 10
module = Ariadne.Flow.Store.Speedrun.Courses
adapter = Ariadne.Flow.Store.Postgres.SpeedrunAdapter

adapter.init()

IO.puts(module.description())
IO.puts("Filling store...")

Speedrun.fill_events(module, adapter, fn progress ->
  IO.write("\r#{progress}%")
end)

IO.puts("")

{:ok, _} = Speedrun.Reporter.start_link()
IO.puts("Running #{num_workers} workers for #{duration_s} seconds...")

tasks = Enum.map(1..num_workers, fn _ -> Speedrun.run_worker(module, adapter) end)

Process.sleep(duration_s * 1000)

Enum.each(tasks, fn task ->
  Task.shutdown(task, :brutal_kill)
end)

%{
  average_read_time_ms: average_read_time,
  read_ops_per_sec: read_ops_per_sec,
  average_append_time_ms: average_append_time,
  append_ops_per_sec: append_ops_per_sec
} = Speedrun.Reporter.stats()

[
  "Query:",
  "\tavg ops time: #{:erlang.float_to_binary(average_read_time, [{:decimals, 2}])}ms",
  "\tops/sec: #{:erlang.float_to_binary(read_ops_per_sec, [{:decimals, 2}])}",
  "Append:",
  "\tavg ops time: #{:erlang.float_to_binary(average_append_time, [{:decimals, 2}])}ms",
  "\tops/sec: #{:erlang.float_to_binary(append_ops_per_sec, [{:decimals, 2}])}"
]
|> Enum.join("\n")
|> IO.puts()
