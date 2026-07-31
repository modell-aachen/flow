defmodule Flow.MixProject do
  use Mix.Project

  def project do
    [
      app: :flow,
      version: "0.2.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps(),
      docs: [
        extras: [
          "docs/introduction.md",
          "docs/events.md",
          "docs/event_reducer.md",
          "docs/application.md",
          "docs/store.md",
          "docs/encoder.md",
          "docs/testing.md"
        ],
        main: "introduction",
        filter_modules: ~r/^Elixir\.Ariadne\.Flow\.(?!Examples)/,
        nest_modules_by_prefix: [Ariadne.Flow]
      ],
      test_paths: ["./lib"],
      test_pattern: "*_test.ex*",
      consolidate_protocols: Mix.env() != :test
    ]
  end

  def application do
    [extra_applications: [:crypto, :logger]]
  end

  defp deps do
    [
      {:credo, "~> 1.6"},
      {:ecto_sql, "~> 3.10"},
      {:jason, "~> 1.4"},
      {:postgrex, ">= 0.0.0"},
      {:telemetry, "~> 1.0"},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:mix_test_interactive, "~> 5.0", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.14", only: [:dev, :test], runtime: false}
    ]
  end

  defp aliases do
    [
      test: [
        "ecto.create --quiet",
        "ecto.rollback -r Ariadne.Flow.Test.Repo --all --quiet",
        "ecto.migrate -r Ariadne.Flow.Test.Repo",
        "test --warnings-as-errors"
      ],
      speedrun: speedrun_repo_setup() ++ ["run lib/flow/store/speedrun_cli.exs"],
      consistency: speedrun_repo_setup() ++ ["run lib/flow/store/postgres/consistency_run.exs"],
      check: [
        "credo --strict",
        "format --check-formatted",
        "sobelow --skip --exit"
      ],
      ci: [
        "compile --force --warnings-as-errors",
        "test --max-cases=8",
        "check"
      ]
    ]
  end

  defp speedrun_repo_setup do
    [
      "ecto.create -r Ariadne.Flow.Store.Speedrun.Repo --quiet",
      "ecto.migrate -r Ariadne.Flow.Store.Speedrun.Repo"
    ]
  end
end
