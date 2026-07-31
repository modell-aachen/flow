defmodule Ariadne.Flow.Query.Optimizer do
  alias Ariadne.Flow.Query.Item

  # Every rewrite below rests on a broader item covering a narrower one's events, which
  # only holds while an item wants all of its matches: the last CourseDefined tagged
  # "course:42" is not the last CourseDefined, so merging the two would lose an event.
  # only_last_event items are therefore passed through, deduplicated and nothing more.
  def optimize(query) do
    {only_last_event, all_events} = Enum.split_with(query, & &1.only_last_event)

    optimize_all_events(all_events) ++ Enum.uniq(only_last_event)
  end

  defp optimize_all_events(query) do
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
