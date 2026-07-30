import Config

config :flow, Ariadne.Flow.Test.Repo,
  username: System.get_env("POSTGRESQL_USER", "postgres"),
  password: System.get_env("POSTGRESQL_PASSWORD", "postgres"),
  hostname: System.get_env("POSTGRESQL_HOST", "localhost"),
  port: System.get_env("POSTGRESQL_PORT", "5544"),
  database: "ariadne_flow",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 50,
  stacktrace: false,
  show_sensitive_data_on_connection_error: true,
  log: false,
  priv: "priv/flow_test_repo"

config :flow, Ariadne.Flow.Store.Speedrun.Repo,
  username: System.get_env("POSTGRESQL_USER", "postgres"),
  password: System.get_env("POSTGRESQL_PASSWORD", "postgres"),
  hostname: System.get_env("POSTGRESQL_HOST", "localhost"),
  port: System.get_env("POSTGRESQL_PORT", "5544"),
  database: "ariadne_speedrun",
  pool_size: 10,
  stacktrace: false,
  show_sensitive_data_on_connection_error: true,
  log: false,
  ownership_timeout: 300_000,
  priv: "priv/speedrun_repo"

config :flow, ecto_repos: [Ariadne.Flow.Test.Repo]

config :logger, level: :warning

Code.compiler_options(ignore_module_conflict: true)
