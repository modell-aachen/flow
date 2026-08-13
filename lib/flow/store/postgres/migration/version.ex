defmodule Ariadne.Flow.Store.Postgres.Migration.Version do
  @moduledoc false
  @callback up(Ariadne.Flow.Store.Postgres.Migration.opts()) :: :ok
  @callback down(Ariadne.Flow.Store.Postgres.Migration.opts()) :: :ok
end
