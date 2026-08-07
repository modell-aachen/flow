defmodule Ariadne.Flow.Examples.DynamicProductPrice.OrderProductTest do
  use Ariadne.Flow.Test.Gwt, async: true

  alias Ariadne.Flow.Examples.DynamicProductPrice.Events
  alias Ariadne.Flow.Examples.DynamicProductPrice.OrderProduct

  gwt "Ordering products" do
    ok("with a valid displayed price",
      given: [%Events.ProductDefined{product_id: "p1", price: 100}],
      when:
        OrderProduct.command(%{
          items: [%{product_id: "p1", displayed_price: 100}],
          now: ~U[2025-01-01 12:00:00Z]
        }),
      then: [%Events.ProductsOrdered{items: [%{product_id: "p1", price: 100}]}]
    )

    err("with a displayed price that was never valid",
      given: [%Events.ProductDefined{product_id: "p1", price: 100}],
      when:
        OrderProduct.command(%{
          items: [%{product_id: "p1", displayed_price: 90}],
          now: ~U[2025-01-01 12:00:00Z]
        }),
      then: :invalid_price
    )

    err("with a price that was changed more than 10 minutes ago",
      given: [
        %{
          event: %Events.ProductDefined{product_id: "p1", price: 100},
          metadata: %{created_at: ~U[2025-01-01 12:00:00Z]}
        },
        %{
          event: %Events.ProductPriceChanged{product_id: "p1", new_price: 150},
          metadata: %{created_at: ~U[2025-01-01 12:05:00Z]}
        }
      ],
      when:
        OrderProduct.command(%{
          items: [%{product_id: "p1", displayed_price: 100}],
          now: ~U[2025-01-01 12:16:00Z]
        }),
      then: :invalid_price
    )

    ok("with a price that was changed less than 10 minutes ago",
      given: [
        %{
          event: %Events.ProductDefined{product_id: "p1", price: 100},
          metadata: %{created_at: ~U[2025-01-01 12:00:00Z]}
        },
        %{
          event: %Events.ProductPriceChanged{product_id: "p1", new_price: 150},
          metadata: %{created_at: ~U[2025-01-01 12:05:00Z]}
        }
      ],
      when:
        OrderProduct.command(%{
          items: [%{product_id: "p1", displayed_price: 100}],
          now: ~U[2025-01-01 12:09:00Z]
        }),
      then: [%Events.ProductsOrdered{items: [%{product_id: "p1", price: 100}]}]
    )

    ok("with multiple items and valid prices",
      given: [
        %{
          event: %Events.ProductDefined{product_id: "p1", price: 100},
          metadata: %{created_at: ~U[2025-01-01 12:00:00Z]}
        },
        %{
          event: %Events.ProductPriceChanged{product_id: "p1", new_price: 150},
          metadata: %{created_at: ~U[2025-01-01 12:05:00Z]}
        },
        %{
          event: %Events.ProductDefined{product_id: "p2", price: 120},
          metadata: %{created_at: ~U[2025-01-01 12:05:00Z]}
        }
      ],
      when:
        OrderProduct.command(%{
          items: [
            %{product_id: "p1", displayed_price: 100},
            %{product_id: "p2", displayed_price: 120}
          ],
          now: ~U[2025-01-01 12:09:00Z]
        }),
      then: [
        %Events.ProductsOrdered{
          items: [%{product_id: "p1", price: 100}, %{product_id: "p2", price: 120}]
        }
      ]
    )
  end
end
