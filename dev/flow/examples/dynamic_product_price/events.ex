defmodule Ariadne.Flow.Examples.DynamicProductPrice.Events do
  alias __MODULE__
  def product_tag(product_id), do: "product:#{product_id}"

  defmodule ProductDefined do
    @derive {Ariadne.Flow.Event, type: "product-defined"}
    defstruct [:product_id, :price]

    def tags(e), do: [Events.product_tag(e.product_id)]
  end

  defmodule ProductPriceChanged do
    @derive {Ariadne.Flow.Event, type: "product-price-changed"}
    defstruct [:product_id, :new_price]

    def tags(e), do: [Events.product_tag(e.product_id)]
  end

  defmodule ProductsOrdered do
    @derive {Ariadne.Flow.Event, type: "products-ordered"}
    defstruct [:items]

    def tags(e), do: Enum.map(e.items, &Events.product_tag(&1.product_id))
  end
end
