defmodule Ariadne.Flow.Examples.UniqueUsername.RegisterAccount do
  alias Ariadne.Flow.Composite
  alias Ariadne.Flow.Examples.UniqueUsername.Events
  alias Ariadne.Flow.Projection

  defp in_retention_period?(created_at, now) do
    DateTime.diff(now, created_at, :day) <= 3
  end

  defp username_claimed(username, now) do
    Projection.new(
      %{
        filter: %{
          types: [Events.AccountRegistered, Events.UsernameChanged, Events.AccountClosed],
          tags: [Events.username_tag(username)]
        },
        initial_state: false
      },
      fn
        _state, %Events.AccountRegistered{}, _meta ->
          true

        _state, %Events.AccountClosed{}, %{created_at: created_at} ->
          in_retention_period?(created_at, now)

        _state, %Events.UsernameChanged{new_username: ^username}, _meta ->
          true

        _state, %Events.UsernameChanged{old_username: ^username}, %{created_at: created_at} ->
          in_retention_period?(created_at, now)
      end
    )
  end

  def command(%{username: username, now: now}) do
    Composite.new(
      %{
        username_claimed?: username_claimed(username, now)
      },
      fn
        %{username_claimed?: true} -> {:error, :username_taken}
        _ -> {:ok, [%Events.AccountRegistered{username: username}]}
      end
    )
  end
end
