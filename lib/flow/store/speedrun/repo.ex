defmodule Ariadne.Flow.Store.Speedrun.Repo do
  use Ecto.Repo,
    otp_app: :ariadne_flow,
    adapter: Ecto.Adapters.Postgres
end
