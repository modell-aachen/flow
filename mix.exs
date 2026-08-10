defmodule AriadneFlow.MixProject do
  use Mix.Project

  @source_url "https://github.com/modell-aachen/flow"

  @description "Event sourcing for Elixir built on Dynamic Consistency Boundaries: an " <>
                 "append-only event store, event reducers that fold events into values, " <>
                 "and command dispatch with reactors."

  def project do
    [
      app: :ariadne_flow,
      version: "0.7.0",
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      name: "Ariadne Flow",
      description: @description,
      source_url: @source_url,
      package: package(),
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
      test_paths: ["./lib", "./dev"],
      test_pattern: "*_test.ex*",
      consolidate_protocols: Mix.env() != :test
    ]
  end

  def application do
    [extra_applications: [:crypto, :logger]]
  end

  defp elixirc_paths(:prod), do: ["lib"]
  defp elixirc_paths(_env), do: ["lib", "dev"]

  defp package do
    [
      files: ~w(lib mix.exs .formatter.exs README.md CHANGELOG.md LICENSE),
      exclude_patterns: [
        ~r/_test\.exs$/,
        ~r{^lib/test_helper\.exs$}
      ],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md"
      }
    ]
  end

  defp deps do
    [
      {:ecto_sql, "~> 3.10"},
      {:jason, "~> 1.4"},
      {:postgrex, ">= 0.0.0"},
      {:telemetry, "~> 1.0"},
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
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
      speedrun: speedrun_repo_setup() ++ ["run dev/flow/store/speedrun_cli.exs"],
      consistency: speedrun_repo_setup() ++ ["run dev/flow/store/postgres/consistency_run.exs"],
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
