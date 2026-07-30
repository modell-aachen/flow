defmodule Ariadne.Flow.Examples.PageResponsible.Events do
  alias Ariadne.Flow.Store
  alias __MODULE__

  def page_tags(id), do: ["page:#{id}"]

  def module_tags(id), do: ["module:#{id}"]

  defmodule PageCreated do
    @derive Store.Event.Encoder
    @enforce_keys [:id, :module_id, :responsible_id]
    defstruct @enforce_keys

    def tags(e), do: Events.page_tags(e.id)
  end

  defmodule PageDeleted do
    @derive Store.Event.Encoder
    @enforce_keys [:id]
    defstruct @enforce_keys

    def tags(e), do: Events.page_tags(e.id)
  end

  defmodule ModuleAccessChanged do
    @derive Store.Event.Encoder
    @enforce_keys [:id, :roles]
    defstruct @enforce_keys

    def tags(e), do: Events.module_tags(e.id)
  end

  defmodule PageResponsibleChanged do
    @derive Store.Event.Encoder
    @enforce_keys [:id, :responsible_id]
    defstruct @enforce_keys

    def tags(e), do: Events.page_tags(e.id)
  end
end
