defmodule Ariadne.Flow.Event.Type do
  @moduledoc false
  alias Ariadne.Flow.Event

  @index {__MODULE__, :index}

  def of(module) when is_atom(module) do
    impl = Module.concat(Event, module)

    if declares_type?(impl),
      do: declared!(impl, module),
      else: from_module_name(module)
  end

  def module!(type) when is_binary(type) do
    case Map.fetch(cached_index(), type) do
      {:ok, module} -> module
      :error -> rediscover!(type)
    end
  end

  def index(typed_modules) when is_list(typed_modules) do
    Enum.reduce(typed_modules, %{}, fn {type, module}, index ->
      case index do
        %{^type => ^module} -> index
        %{^type => taken_by} -> raise ArgumentError, duplicate_hint(type, taken_by, module)
        index -> Map.put(index, type, module)
      end
    end)
  end

  defp declares_type?(impl) do
    match?({:module, _}, Code.ensure_compiled(impl)) and function_exported?(impl, :type, 0)
  end

  defp from_module_name(module) do
    module
    |> Atom.to_string()
    |> String.trim_leading("Elixir.")
  end

  defp declared!(impl, module) do
    case impl.type() do
      type when is_binary(type) -> type
      other -> raise ArgumentError, not_a_string_hint(module, other)
    end
  end

  defp cached_index do
    case :persistent_term.get(@index, nil) do
      nil -> build_index()
      index -> index
    end
  end

  defp rediscover!(type) do
    case Map.fetch(build_index(), type) do
      {:ok, module} -> module
      :error -> raise ArgumentError, unknown_hint(type)
    end
  end

  defp build_index do
    index = index(Enum.map(event_modules(), &{of(&1), &1}))
    :persistent_term.put(@index, index)

    index
  end

  defp event_modules, do: Enum.uniq(compiled_modules() ++ loaded_modules())

  defp compiled_modules do
    case Event.__protocol__(:impls) do
      {:consolidated, modules} -> modules
      :not_consolidated -> Protocol.extract_impls(Event, :code.get_path())
    end
  end

  defp loaded_modules do
    for {module, _file} <- :code.all_loaded(),
        function_exported?(module, :__impl__, 1),
        module.__impl__(:protocol) == Event,
        do: module.__impl__(:for)
  end

  defp unknown_hint(type) do
    """
    No event module declares the stored event type #{inspect(type)}.

    Every type in the store has to resolve back to a module implementing \
    Ariadne.Flow.Event — a type that resolves to none is history no \
    version of the code can read. A renamed event module keeps its history by pinning \
    the type it was stored under:

        @derive {Ariadne.Flow.Event, type: #{inspect(type)}}
    """
  end

  defp duplicate_hint(type, taken_by, module) do
    """
    #{inspect(taken_by)} and #{inspect(module)} both declare the stored event type \
    #{inspect(type)}.

    A stored type names one event module, or reading it back would be a guess between \
    them. Give each of them a type of its own.
    """
  end

  defp not_a_string_hint(module, type) do
    """
    #{inspect(module)} declares the stored event type #{inspect(type)}, which is not a \
    string.

    The type is the name the event is stored under:

        @derive {Ariadne.Flow.Event, type: "#{from_module_name(module)}"}
    """
  end
end
