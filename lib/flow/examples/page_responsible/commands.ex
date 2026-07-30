defmodule Ariadne.Flow.Examples.PageResponsible.Commands do
  alias Ariadne.Flow.Composite
  alias Ariadne.Flow.Examples.PageResponsible.Events
  alias Ariadne.Flow.Examples.PageResponsible.ReadModels

  def change_page_responsible(%{
        id: id,
        module_id: module_id,
        new_responsible_id: new_responsible_id,
        issuer: issuer
      }) do
    Composite.new(
      %{
        exists?: ReadModels.page_exists(id),
        responsible_id: ReadModels.page_responsible(id),
        page_admin?:
          ReadModels.page_admin(%{
            page_id: id,
            module_id: module_id,
            issuer: issuer
          })
      },
      fn
        %{exists?: false} ->
          {:error, :not_found}

        %{page_admin?: false} ->
          {:error, :forbidden}

        %{responsible_id: ^new_responsible_id} ->
          {:error, :conflict}

        _ ->
          {:ok, [%Events.PageResponsibleChanged{id: id, responsible_id: new_responsible_id}]}
      end
    )
  end
end
