defmodule Ariadne.Flow.Query.Optimizer do
  alias Ariadne.Flow.Query.Item

  def optimize(query) do
    {last_event_items, all_event_items} = Enum.split_with(query, & &1.only_last_event)

    optimize_all_event_items(all_event_items) ++ Enum.uniq(last_event_items)
  end

  defp optimize_all_event_items(query) do
    query
    |> expand_to_type_constraint_pairs()
    |> minimize_constraints_per_type()
    |> invert_to_constraint_first()
    |> build_items()
  end

  defp expand_to_type_constraint_pairs(query) do
    for %Item{types: types, tags: tags} <- query,
        type <- types,
        do: {type, normalize_constraint(tags)}
  end

  defp normalize_constraint(nil), do: :unrestricted
  defp normalize_constraint(tags), do: MapSet.new(tags)

  defp minimize_constraints_per_type(type_constraint_pairs) do
    type_constraint_pairs
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.map(fn {type, constraints} ->
      minimal_constraints =
        if :unrestricted in constraints do
          [:unrestricted]
        else
          remove_supersets(constraints)
        end

      {type, minimal_constraints}
    end)
  end

  defp invert_to_constraint_first(type_to_constraints) do
    for {type, constraints} <- type_to_constraints,
        constraint <- constraints,
        reduce: %{} do
      acc -> Map.update(acc, constraint, MapSet.new([type]), &MapSet.put(&1, type))
    end
  end

  defp build_items(constraint_to_types) do
    for {constraint, types_set} <- constraint_to_types do
      tags = if constraint == :unrestricted, do: nil, else: Enum.to_list(constraint)
      Item.new(%{types: Enum.to_list(types_set), tags: tags})
    end
  end

  defp remove_supersets(constraints) do
    Enum.reject(constraints, fn constraint ->
      Enum.any?(constraints, fn other ->
        constraint != other && MapSet.subset?(other, constraint)
      end)
    end)
  end
end
