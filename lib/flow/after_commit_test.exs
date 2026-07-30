defmodule Ariadne.Flow.AfterCommitTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.AfterCommit

  describe "new/1" do
    test "wraps a one-arity callback" do
      callback = fn _after_commit -> :ok end

      assert %AfterCommit{callback: ^callback} = AfterCommit.new(callback)
    end
  end

  describe "run/1" do
    test "calls the callback with the struct it was built from" do
      after_commit = AfterCommit.new(fn struct -> send(self(), {:ran, struct}) end)

      AfterCommit.run(after_commit)

      assert_received {:ran, ^after_commit}
    end
  end
end
