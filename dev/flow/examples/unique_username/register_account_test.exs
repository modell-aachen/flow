defmodule Ariadne.Flow.Examples.UniqueUsername.RegisterAccountTest do
  use Ariadne.Flow.Test.Gwt, async: true
  alias Ariadne.Flow.Examples.UniqueUsername.Events
  alias Ariadne.Flow.Examples.UniqueUsername.RegisterAccount

  gwt "Registering an account" do
    ok("with an available username",
      given: [],
      when: RegisterAccount.command(%{username: "alice", now: ~U[2025-01-01 12:00:00Z]}),
      then: [%Events.AccountRegistered{username: "alice"}]
    )

    err("with a taken username",
      given: [%Events.AccountRegistered{username: "alice"}],
      when: RegisterAccount.command(%{username: "alice", now: ~U[2025-01-01 12:00:00Z]}),
      then: :username_taken
    )

    err("with a released username due to account closure within 3 day retention period",
      given: [
        %{
          event: %Events.AccountRegistered{username: "alice"},
          metadata: %{created_at: ~U[2024-01-01 12:00:00Z]}
        },
        %{
          event: %Events.AccountClosed{username: "alice"},
          metadata: %{created_at: ~U[2024-01-02 12:00:00Z]}
        }
      ],
      when: RegisterAccount.command(%{username: "alice", now: ~U[2024-01-05 12:00:00Z]}),
      then: :username_taken
    )

    ok("with a released username due to account closure after 3 day retention period",
      given: [
        %{
          event: %Events.AccountRegistered{username: "alice"},
          metadata: %{created_at: ~U[2024-01-01 12:00:00Z]}
        },
        %{
          event: %Events.AccountClosed{username: "alice"},
          metadata: %{created_at: ~U[2024-01-02 12:00:00Z]}
        }
      ],
      when: RegisterAccount.command(%{username: "alice", now: ~U[2024-01-06 12:00:00Z]}),
      then: [%Events.AccountRegistered{username: "alice"}]
    )

    err("with a released username due to name change within 3 day retention period",
      given: [
        %{
          event: %Events.AccountRegistered{username: "alice"},
          metadata: %{created_at: ~U[2024-01-01 12:00:00Z]}
        },
        %{
          event: %Events.UsernameChanged{old_username: "alice", new_username: "bob"},
          metadata: %{created_at: ~U[2024-01-02 12:00:00Z]}
        }
      ],
      when: RegisterAccount.command(%{username: "alice", now: ~U[2024-01-05 12:00:00Z]}),
      then: :username_taken
    )

    ok("with a released username due to name change after 3 day retention period",
      given: [
        %{
          event: %Events.AccountRegistered{username: "alice"},
          metadata: %{created_at: ~U[2024-01-01 12:00:00Z]}
        },
        %{
          event: %Events.UsernameChanged{old_username: "alice", new_username: "bob"},
          metadata: %{created_at: ~U[2024-01-02 12:00:00Z]}
        }
      ],
      when: RegisterAccount.command(%{username: "alice", now: ~U[2024-01-06 12:00:00Z]}),
      then: [%Events.AccountRegistered{username: "alice"}]
    )
  end
end
