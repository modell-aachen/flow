defmodule Ariadne.Flow.Examples.UniqueUsername.Events do
  alias __MODULE__
  def username_tag(username), do: "username:#{username}"

  defmodule AccountRegistered do
    @derive {Ariadne.Flow.Store.Event.Encoder, type: "account-registered"}
    defstruct [:username]

    def tags(e), do: [Events.username_tag(e.username)]
  end

  defmodule AccountClosed do
    @derive {Ariadne.Flow.Store.Event.Encoder, type: "account-closed"}
    defstruct [:username]

    def tags(e), do: [Events.username_tag(e.username)]
  end

  defmodule UsernameChanged do
    @derive {Ariadne.Flow.Store.Event.Encoder, type: "username-changed"}
    defstruct [:old_username, :new_username]

    def tags(e), do: [Events.username_tag(e.old_username), Events.username_tag(e.new_username)]
  end
end
