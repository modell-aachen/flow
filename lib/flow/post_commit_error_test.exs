defmodule Ariadne.Flow.PostCommitErrorTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.PostCommitError

  describe "failure/1" do
    test "names the reactor that failed, where it failed and why" do
      message =
        [%{name: "sync-counts", position: 7, reason: :kaboom}]
        |> PostCommitError.failure()
        |> Exception.message()

      assert message =~ ~s|sync reactor "sync-counts" failed at position 7: :kaboom|
    end

    test "leaves out a position a failure never reached" do
      message =
        [%{name: "sync-counts", position: nil, reason: %RuntimeError{message: "kaboom"}}]
        |> PostCommitError.failure()
        |> Exception.message()

      assert message =~ ~s|sync reactor "sync-counts" failed: kaboom|
    end

    test "carries every failure of the pass" do
      message =
        [
          %{name: "first", position: 1, reason: :kaboom},
          %{name: "second", position: 2, reason: :kaboom_again}
        ]
        |> PostCommitError.failure()
        |> Exception.message()

      assert message =~ ~s|sync reactor "first" failed at position 1: :kaboom|
      assert message =~ ~s|sync reactor "second" failed at position 2: :kaboom_again|
    end
  end

  describe "timeout/2" do
    test "names every unconfirmed reactor with the position it was awaited at" do
      message =
        [%{name: "sync", position: 7}, %{name: "tagged-sync", position: 3}]
        |> PostCommitError.timeout(5_000)
        |> Exception.message()

      assert message =~
               ~s|the sync reactors "sync" (awaited at position 7), "tagged-sync" (awaited at position 3)|

      assert message =~ "did not catch up within 5000ms"
    end

    test "reads as one reactor when only one did not confirm" do
      message =
        [%{name: "sync", position: 7}]
        |> PostCommitError.timeout(5_000)
        |> Exception.message()

      assert message =~ ~s|the sync reactor "sync" (awaited at position 7)|
    end
  end

  # The one thing both reasons have to say: the dispatch is past the point where retrying
  # it is safe, whichever way it went wrong afterwards.
  test "every reason says the events are committed and must not be dispatched again" do
    errors = [
      PostCommitError.failure([%{name: "sync", position: 1, reason: :kaboom}]),
      PostCommitError.timeout([%{name: "sync", position: 1}], 5_000)
    ]

    for error <- errors do
      message = Exception.message(error)

      assert message =~ "the events are committed"
      assert message =~ "never re-dispatch"
    end
  end
end
