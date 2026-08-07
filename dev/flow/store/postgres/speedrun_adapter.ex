defmodule Ariadne.Flow.Store.Postgres.SpeedrunAdapter do
  alias Ariadne.Flow.Store.Postgres
  alias Ariadne.Flow.Store.Speedrun

  def init do
    {:ok, _} = Speedrun.Repo.start_link()
  end

  def start_store do
    Postgres.init(repo: Speedrun.Repo)
  end
end
