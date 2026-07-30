defmodule Ariadne.Flow.Store.Postgres.AdvisoryLockTest do
  use ExUnit.Case, async: true

  alias Ariadne.Flow.Store.Postgres.Query

  @store_domain "modac_flow_postgres_store"
  @checkpoint_domain "modac_flow_reactor_checkpoint_lock"

  @bigint_min -9_223_372_036_854_775_808
  @bigint_max 9_223_372_036_854_775_807
  @int32_max 2_147_483_647

  describe "advisory lock keys" do
    test "are distinct for inputs that only differ in one part" do
      inputs = [
        store: [@store_domain, "public", "default"],
        store_other_context: [@store_domain, "public", "other"],
        store_other_prefix: [@store_domain, "tenant", "default"],
        checkpoint: [@checkpoint_domain, "public", "default", "reactor"],
        checkpoint_other_name: [@checkpoint_domain, "public", "default", "other_reactor"],
        checkpoint_other_context: [@checkpoint_domain, "public", "other", "reactor"]
      ]

      keys = Map.new(inputs, fn {label, parts} -> {label, Query.advisory_lock_key(parts)} end)

      assert map_size(keys) == length(Enum.uniq(Map.values(keys)))
    end

    test "do not collide when part boundaries shift" do
      refute Query.advisory_lock_key([@store_domain, "tenant_a", "bc"]) ==
               Query.advisory_lock_key([@store_domain, "tenant_ab", "c"])

      refute Query.advisory_lock_key([@checkpoint_domain, "public", "default", "reactor"]) ==
               Query.advisory_lock_key([@checkpoint_domain, "public", "defaultreactor", ""])
    end

    test "use the whole signed bigint range Postgres accepts" do
      keys =
        Enum.map(1..1_000, &Query.advisory_lock_key([@store_domain, "tenant_#{&1}", "default"]))

      assert Enum.all?(keys, &(&1 >= @bigint_min and &1 <= @bigint_max))
      assert Enum.any?(keys, &(&1 < -@int32_max))
      assert Enum.any?(keys, &(&1 > @int32_max))
      assert length(keys) == length(Enum.uniq(keys))
    end

    test "are stable, so all nodes and releases derive the same key" do
      assert 4_058_807_053_573_865_758 ==
               Query.advisory_lock_key([@store_domain, "postgres_store_test_schema", "default"])

      assert 6_644_676_092_048_313_026 ==
               Query.advisory_lock_key([@checkpoint_domain, "public", "default", "reactor"])
    end
  end
end
