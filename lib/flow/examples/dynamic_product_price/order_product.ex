defmodule Ariadne.Flow.Examples.DynamicProductPrice.OrderProduct do
  alias Ariadne.Flow.Composite
  alias Ariadne.Flow.Examples.DynamicProductPrice.Events
  alias Ariadne.Flow.Projection

  def command(%{items: items, now: now}) do
    Composite.new(
      %{
        all_prices_valid?: all_prices_valid?(items, now)
      },
      fn
        %{all_prices_valid?: false} ->
          {:error, :invalid_price}

        _ ->
          {:ok, [to_ordered_event(items)]}
      end
    )
  end

  defp all_prices_valid?(items, now) do
    items
    |> Enum.into(%{}, fn item ->
      {
        item.product_id,
        valid_price?(item.product_id, item.displayed_price, now)
      }
    end)
    |> Composite.new(fn model ->
      model
      |> Map.values()
      |> Enum.all?()
    end)
  end

  defp valid_price?(product_id, price, now) do
    Composite.new(
      %{
        prices: valid_prices(product_id, now)
      },
      fn %{prices: prices} -> price == prices.current or price in prices.in_grace_period end
    )
  end

  defp valid_prices(product_id, now) do
    Projection.new(
      %{
        filter: %{
          types: [Events.ProductDefined, Events.ProductPriceChanged],
          tags: [Events.product_tag(product_id)]
        },
        initial_state: %{current: nil, in_grace_period: []}
      },
      fn
        state, %Events.ProductDefined{price: price}, %{created_at: created_at} ->
          update_valid_prices(state, price, created_at, now)

        state, %Events.ProductPriceChanged{new_price: price}, %{created_at: created_at} ->
          update_valid_prices(state, price, created_at, now)
      end
    )
  end

  defp update_valid_prices(%{in_grace_period: in_grace_period}, new_price, created_at, now) do
    in_grace_period =
      if DateTime.diff(now, created_at, :minute) <= 10 do
        [new_price | in_grace_period]
      else
        in_grace_period
      end

    %{current: new_price, in_grace_period: in_grace_period}
  end

  defp to_ordered_event(items) do
    %Events.ProductsOrdered{
      items:
        Enum.map(items, fn %{product_id: product_id, displayed_price: price} ->
          %{product_id: product_id, price: price}
        end)
    }
  end
end
