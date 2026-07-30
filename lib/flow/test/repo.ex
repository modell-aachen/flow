defmodule Ariadne.Flow.Test.Repo do
  @moduledoc false
  use Ecto.Repo,
    otp_app: :flow,
    adapter: Ecto.Adapters.Postgres
end
