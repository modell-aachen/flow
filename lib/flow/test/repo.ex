defmodule Ariadne.Flow.Test.Repo do
  @moduledoc false
  use Ecto.Repo,
    otp_app: :ariadne_flow,
    adapter: Ecto.Adapters.Postgres
end
