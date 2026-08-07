defmodule Ariadne.Flow.Examples.PageResponsible.CommandsTest do
  use Ariadne.Flow.Test.Gwt, async: true
  alias Ariadne.Flow.Examples.PageResponsible.Commands
  alias Ariadne.Flow.Examples.PageResponsible.Events

  @page_id "e1ca4b3f-d22b-4e25-9ff4-cfe83c79f2a0"
  @module_id "e68fde11-56a3-4b6a-801d-74441ec546c6"
  @responsible_id "cee97b43-b12d-408b-9e8f-048bb8662ccf"
  @issuer %{id: @responsible_id, roles: []}

  @event %{
    page_created: %Events.PageCreated{
      id: @page_id,
      module_id: @module_id,
      responsible_id: @responsible_id
    },
    page_deleted: %Events.PageDeleted{
      id: @page_id
    },
    responsible_changed: %Events.PageResponsibleChanged{
      id: @page_id,
      responsible_id: @responsible_id
    },
    module_access_changed: %Events.ModuleAccessChanged{
      id: @module_id,
      roles: ["key_user", "qm"]
    }
  }

  defp params(opts \\ []) do
    Enum.into(opts, %{
      id: @page_id,
      module_id: @module_id,
      new_responsible_id: @responsible_id,
      issuer: @issuer
    })
  end

  @new_responsible_id "884e7b0f-4179-4be5-a833-8436d4b65682"

  gwt "change_page_responsible" do
    err("finds no page",
      given: [%{@event.page_created | id: "30e1ee7e-b3f0-456b-935b-d0db6d60014a"}],
      when: Commands.change_page_responsible(params()),
      then: :not_found
    )

    err("finds no page if deleted",
      given: [@event.page_created, @event.page_deleted],
      when: Commands.change_page_responsible(params()),
      then: :not_found
    )

    ok("changes responsible",
      given: [@event.page_created],
      when: Commands.change_page_responsible(params(new_responsible_id: @new_responsible_id)),
      then: [%{@event.responsible_changed | responsible_id: @new_responsible_id}]
    )

    err("cannot change responsible if ids are identical",
      given: [
        %{@event.page_created | id: "761d7ebc-5006-4a43-9f37-31c0181cb3f6"},
        @event.page_created
      ],
      when: Commands.change_page_responsible(params()),
      then: :conflict
    )

    err("cannot change responsible if ids are identical after changing the responsible",
      given: [@event.page_created, @event.responsible_changed],
      when: Commands.change_page_responsible(params()),
      then: :conflict
    )

    err("cannot change responsible if not responsible and not module admin",
      given: [@event.page_created],
      when:
        Commands.change_page_responsible(
          params(issuer: %{id: "f1a8a94c-211a-47b1-b569-ee61001c2296", roles: []})
        ),
      then: :forbidden
    )

    ok("can change responsible if module admin",
      given: [
        %{@event.page_created | responsible_id: "761d7ebc-5006-4a43-9f37-31c0181cb3f6"},
        @event.module_access_changed
      ],
      when:
        Commands.change_page_responsible(
          params(issuer: %{id: "f1a8a94c-211a-47b1-b569-ee61001c2296", roles: [:key_user]})
        ),
      then: [@event.responsible_changed]
    )
  end
end
