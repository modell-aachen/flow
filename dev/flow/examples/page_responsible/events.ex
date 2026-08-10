defmodule Ariadne.Flow.Examples.PageResponsible.Events do
  alias Ariadne.Flow.Event
  alias __MODULE__

  def page_tags(id), do: ["page:#{id}"]

  def module_tags(id), do: ["module:#{id}"]

  defmodule PageCreated do
    @derive {Event, type: "page-created"}
    @enforce_keys [:id, :module_id, :responsible_id]
    defstruct @enforce_keys

    def tags(e), do: Events.page_tags(e.id)
  end

  defmodule PageDeleted do
    @derive {Event, type: "page-deleted"}
    @enforce_keys [:id]
    defstruct @enforce_keys

    def tags(e), do: Events.page_tags(e.id)
  end

  defmodule ModuleAccessChanged do
    @derive {Event, type: "module-access-changed"}
    @enforce_keys [:id, :roles]
    defstruct @enforce_keys

    def tags(e), do: Events.module_tags(e.id)
  end

  defmodule PageResponsibleChanged do
    @derive {Event, type: "page-responsible-changed"}
    @enforce_keys [:id, :responsible_id]
    defstruct @enforce_keys

    def tags(e), do: Events.page_tags(e.id)
  end
end
