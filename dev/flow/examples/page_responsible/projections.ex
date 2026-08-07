defmodule Ariadne.Flow.Examples.PageResponsible.ReadModels do
  alias Ariadne.Flow.Composite
  alias Ariadne.Flow.Examples.PageResponsible.Events
  alias Ariadne.Flow.Projection

  def page_exists(id) do
    Projection.new(
      %{
        initial_state: false,
        filter: %{
          types: [Events.PageCreated, Events.PageDeleted],
          tags: Events.page_tags(id),
          only_last_event: true
        }
      },
      fn
        _state, %Events.PageCreated{}, _ -> true
        _state, %Events.PageDeleted{}, _ -> false
      end
    )
  end

  def page_responsible(id) do
    Projection.new(
      %{
        initial_state: nil,
        filter: %{
          types: [Events.PageCreated, Events.PageResponsibleChanged],
          tags: Events.page_tags(id),
          only_last_event: true
        }
      },
      fn
        _state, %Events.PageCreated{responsible_id: id}, _ -> id
        _state, %Events.PageResponsibleChanged{responsible_id: id}, _ -> id
      end
    )
  end

  def module_admin(id, issuer_roles) do
    Projection.new(
      %{
        initial_state: false,
        filter: %{
          types: [Events.ModuleAccessChanged],
          tags: Events.module_tags(id),
          only_last_event: true
        }
      },
      fn
        _state, %Events.ModuleAccessChanged{roles: roles}, _ ->
          !MapSet.disjoint?(to_string_set(roles), to_string_set(issuer_roles))
      end
    )
  end

  def page_admin(%{
        page_id: page_id,
        module_id: module_id,
        issuer: %{id: issuer_id, roles: issuer_roles}
      }) do
    Composite.new(
      %{
        responsible_id: page_responsible(page_id),
        module_admin?: module_admin(module_id, issuer_roles)
      },
      fn
        %{responsible_id: responsible_id, module_admin?: module_admin} ->
          responsible_id == issuer_id or module_admin
      end
    )
  end

  defp to_string_set(roles), do: MapSet.new(roles, &to_string/1)
end
