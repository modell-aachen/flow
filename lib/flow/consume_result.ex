defmodule Ariadne.Flow.ConsumeResult do
  @enforce_keys [:status, :processed, :last_position, :more?]
  defstruct [
    :status,
    :processed,
    :last_position,
    :more?,
    results: [],
    failure: nil,
    duration_ms: 0
  ]

  @type t :: %__MODULE__{
          status: :ok | :error,
          processed: non_neg_integer(),
          results: [term()],
          last_position: non_neg_integer(),
          more?: boolean(),
          failure: term(),
          duration_ms: non_neg_integer()
        }
end
