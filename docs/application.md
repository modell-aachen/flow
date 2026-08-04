# Application

An event reducer on its own only *describes* what to do. The `Ariadne.Flow.Application` module is what drives a reducer against a store — it fetches the events the reducer needs, runs them through the reducer, and optionally appends new events. It provides two operations:

- `query/2` reads events from the store and folds them through a reducer.
- `dispatch/3` does the same, and when the reducer's result is `{:ok, events}` it appends those events to the store.

The examples below assume a `store` has already been set up. The store is covered in its own section — for now you can think of it as the place where events live.

## Query

Given a store and an event reducer, `query/2` returns the reducer's reduced value:

```elixir
iex> Ariadne.Flow.Application.query(store, course_capacity(42))
30

iex> Ariadne.Flow.Application.query(store, has_room?(42))
true
```

`query/2` accepts any event reducer. A projection returns the state it folded; a composite returns whatever its `map_fn` produced.

Internally, `query/2` performs three steps:

1. It asks the reducer which events it needs by calling `query/1` on it.
2. It reads those events from the store.
3. It calls `reduce/2` on the reducer with the events and returns the result.

Events are delivered to the reducer in the order they were appended. That ordering is across the whole query, not per tag — if a reducer asks for events from two different courses, they arrive interleaved in the order the store saw them.

No events are written. `query/2` is a pure read.

## Dispatch

`query/2` never writes to the store. To *also* append new events, use `dispatch/3`.

By convention, a **command** is any event reducer that returns either `{:ok, events}` (to emit events) or `{:error, reason}` (to refuse). Nothing in `Ariadne.Flow` marks a reducer as "a command" — it is just a reducer whose result shape lets `dispatch/3` act on it. In practice commands are almost always composites, because a command needs to look at several projections before deciding what to do. Here is a command that subscribes a student to a course, refusing if the course is already full:

```elixir
def subscribe_student(course_id, student_id) do
  Composite.new(
    %{
      capacity: course_capacity(course_id),
      subscriptions: course_subscriptions(course_id)
    },
    fn
      %{capacity: capacity, subscriptions: subscriptions} when subscriptions >= capacity ->
        {:error, :course_full}

      _ ->
        {:ok, [%StudentSubscribedToCourse{course_id: course_id, student_id: student_id}]}
    end
  )
end
```

Dispatching the command against a store:

```elixir
iex> Ariadne.Flow.Application.dispatch(store, subscribe_student(42, 7))
{:ok,
 %{
   events: [
     %{
       event: %StudentSubscribedToCourse{course_id: 42, student_id: 7},
       metadata: %{created_at: ~U[2026-04-24 10:15:00Z]}
     }
   ]
 }}

iex> Ariadne.Flow.Application.dispatch(store, subscribe_student(42, 7))
{:error, :course_full}
```

`dispatch/3` returns one of three shapes:

- `{:ok, %{events: entries}}` — the command emitted events and they were appended. `entries` is a list with one map per appended event, each carrying:
  - `:event` — the event struct the command emitted.
  - `:metadata` — the metadata stored with the event, including the default `:created_at`.
  - `:type` and `:tags` — the stored form of the event, as `Ariadne.Flow.Store.Event.Encoder` wrote it.
- `{:error, reason}` — the command returned `{:error, reason}`. Nothing is written and the reason is passed back unchanged.
- `{:error, %Ariadne.Flow.AppendConditionError{}}` — a concurrent dispatch appended events matching this command's query between its read and its append. Nothing is written; see [concurrency](#concurrency).

The line between values and raises is the commit: both error values mean nothing was written, so the caller may safely dispatch again. Everything on the other side of the commit is raised instead — a reactor failing after the events were appended, or a synchronous reactor not catching up in time; see [reactors](#reactors) and [synchronous reactors](#synchronous-reactors).

`dispatch/3` is the only way events enter the store in an `Ariadne.Flow` program — every write goes through a command and is justified by the events that came before it.

The third argument is a keyword list of options, all optional — the calls above pass none, so they look like a two-argument call. The available options are covered in the next section.

### dispatch!/3

`dispatch!/3` behaves like `dispatch/3` but raises where `dispatch/3` returns an error value, so a caller that only cares about the success path cannot silently drop a failure. Each exception type carries its retry semantics:

- `Ariadne.Flow.CommandError` — the command refused, wrapping the refusal `reason`. Nothing was written; dispatching again is pointless until the state it decided from changes.
- `Ariadne.Flow.AppendConditionError` — the append condition failed. Nothing was written; retrying the dispatch is safe and decides from the new events.
- `Ariadne.Flow.ReactorError` — raised by `dispatch/3` and `dispatch!/3` alike: the events were committed and a reactor failed afterwards. Never re-dispatch the command — that would append the events a second time. See [reactors](#reactors).
- `Ariadne.Flow.ConsistencyTimeoutError` — raised by `dispatch/3` and `dispatch!/3` alike: the events were committed and a synchronous reactor did not catch up with them within the timeout. Nothing failed and the reactor's run is still scheduled; never re-dispatch. See [synchronous reactors](#synchronous-reactors).

## Metadata

`dispatch/3` accepts a `:metadata` option — a map that is stored alongside every event appended in that dispatch:

```elixir
iex> Ariadne.Flow.Application.dispatch(
...>   store,
...>   subscribe_student(42, 7),
...>   metadata: %{"user_id" => "alice", "trace_id" => "abc123"}
...> )
```

Metadata is for context that does not belong in the event payload itself — who triggered the command, which request produced it, correlation IDs for tracing. The business facts go in the event; the circumstances under which those facts were recorded go in metadata.

When events are later read back — whether by a subsequent `dispatch/3` or by `query/2` — the metadata arrives at the reducer's handler as its third argument:

```elixir
handler: fn state, %SomeEvent{}, metadata ->
  # metadata is what was passed to dispatch when the event was appended
end
```

Every dispatch also adds default metadata on top of what you pass in. Currently this is only `:created_at`, a timestamp of when the events were appended. Default metadata keys are atoms, while user-provided metadata keys are serialised as strings when read back.

## Reactors

A **command** decides what new events to append. A **reactor** is the other half: it observes events that were appended and *reacts* to them — projecting them into a read model, publishing them elsewhere, kicking off follow-up work. Where a command's consistency is checked at append time, a reactor is a catch-up consumer: it remembers, per reactor name, the last position it processed (its checkpoint), and on each run continues from there.

A reactor is configured on an application as a **module** exporting `reactor/0`, which returns an `Ariadne.Flow.Reactor`:

```elixir
defmodule CourseSize do
  def reactor do
    Ariadne.Flow.Reactor.new(
      %{name: "course_size", filter: %{types: [StudentSubscribedToCourse]}},
      fn event, _metadata -> update_read_model(event) end
    )
  end
end

application =
  Ariadne.Flow.Application.new(%{store: store, reactors: [CourseSize]})
```

The `name` is what the checkpoint is keyed on, so it must be stable across versions. The `reactor/0` convention is part of the public contract: the module name is all that identifies a reactor, which lets a reactor run be serialised and handed to an execution backend.

The `filter` is a query item like a projection's, with one exception: a reactor reacts to every event it matches, so it cannot ask for [`only_last_event`](event_reducer.html#reducing-the-last-event-only) — the events it skipped would be checkpointed past and never delivered. `Reactor.new/2` raises on such a filter.

Every successful `dispatch/3` drives the configured reactors over the events that dispatch produced, in one ordered pass. The append and that pass share one store transaction (see [transactions](store.html#transactions)), so they commit as one unit — a reactor that returns an error still leaves the events in the store, but a crash part-way through the pass takes the append back out with it. A reactor therefore chooses the fate of the dispatch's events by how it expresses a failure: returning `{:error, reason}` keeps them, raising rolls them back.

A returned error does not stop the pass: every reactor declared after a failing one is still run, in declaration order. Since the events commit either way, stopping early would buy nothing and cost the later reactors their run — and for a reactor whose run the engine defers, that run *is* the enqueue, so skipping it would commit events with no job to catch that reactor up. Whenever the events commit, every reactor got its one run.

When reactors return errors, the dispatch raises `Ariadne.Flow.ReactorError` — carrying the whole pass's failures as a list, one entry per failed reactor with its `name`, the `position` it failed at, and its `reason` — rather than returning an error value, because the events are already committed: an error value would be a partial success masquerading as a failure, and any generic retry-on-error wrapper around the dispatch would append the events a second time. Recovery is not the dispatcher's job either — each failed reactor's checkpoint is parked right before its poison event and commits with the dispatch, so the reactor picks up from there on its next run once the cause is fixed.

Each reactor is handed to the application's **engine**, which decides how it runs — inline in the dispatching process, or deferred onto a job system. See [the engine](#the-engine) below.

## Synchronous reactors

A reactor may declare that a dispatch must not return before it has caught up with the events that dispatch appended. Passing `sync: true` marks it synchronous; the default is asynchronous.

```elixir
Ariadne.Flow.Reactor.new(
  %{name: "course_size", filter: %{types: [StudentSubscribedToCourse]}, sync: true},
  fn event, _metadata -> update_read_model(event) end
)
```

`sync: true` says something about the dispatch, not about where the reactor runs. Both kinds of reactor are handed to the engine the same way and either may be deferred onto a job system; what synchronous adds is that once the events are committed, `dispatch/3` waits for the reactor to reach them before returning. Declare it for the reactor whose work the caller reads back immediately — a request that finishes by rendering the read model this reactor maintains.

What the dispatch waits on is the reactor's **checkpoint**: it returns once every synchronous reactor has checkpointed at or past the last of the dispatch's events *that reactor is going to process*. The target is per reactor, because a checkpoint records the last matching event a reactor consumed and [stays put](#reactors) on events its filter skips — a reactor is caught up once it reaches the highest appended position matching its filter, and one that matches none of the dispatch's events has nothing to catch up to and is not waited on at all.

Deliberately not the state of whatever the engine enqueued — a reactor's events are consumed one run at a time, so a concurrent dispatch's run may be the one that processes these events and leave this dispatch's own run with nothing to do, and a job that no-ops is not a reactor that failed to catch up. The checkpoint is the store's own record of how far the reactor got, so it holds whoever advanced it.

The wait is bounded by the `:await_timeout` option, a non-negative number of milliseconds defaulting to 5000:

```elixir
iex> Ariadne.Flow.Application.dispatch(application, subscribe_student(42, 7), await_timeout: 1_000)
```

There is no `:infinity` — a dispatch that may never return is not a guarantee a caller can act on — and anything that is not a non-negative integer raises `ArgumentError`. That check happens before the command runs, so a bad option is a plain argument error with nothing written, rather than a crash on the far side of the commit.

When the timeout runs out, `dispatch/3` raises `Ariadne.Flow.ConsistencyTimeoutError`, carrying every reactor that did not confirm together with the position it was awaited at. That raise does not mean the dispatch failed: the events are committed and the runs are still durably scheduled, so those reactors will still catch up. What went unmet is the caller's expectation of reading its own write back. **Never re-dispatch the command** — the events are already in the store and dispatching again would append them a second time. Returning an error value instead would invite exactly that, since any retry-on-error wrapper around the dispatch would double-write; and returning `{:ok, ...}` would quietly withdraw the guarantee the caller asked for by declaring the reactor synchronous.

The wait is for the reactor to reach this dispatch's events, but a reactor resumes from wherever its checkpoint stands, so it reaches them by working through everything in between. A synchronous reactor that has fallen ten thousand events behind cannot confirm a new dispatch until it has drained all ten thousand — which makes a single lagging synchronous reactor a way to turn *every* subsequent dispatch into a timeout, and the reason a reactor declared synchronous belongs on a queue that keeps up with it. `Ariadne.Flow.ConsistencyTimeoutError` naming the same reactor across unrelated dispatches is what that looks like from the outside.

Each wait is reported as a `[:ariadne, :flow, :dispatch, :await]` telemetry span, with the reactors and positions it awaited, whether it ended `:confirmed` or in a `:timeout`, and how many rounds of checkpoint reads it took — so how long callers actually block, and how close to the timeout they come, is measurable before it turns into raises. Nothing is emitted for a dispatch with nothing to await.

A synchronous reactor that *fails* is a different outcome, and `Ariadne.Flow.ReactorError` is raised for it rather than the timeout being waited out — a definitive failure says more than a wait that could only run out. That holds for a failure the dispatch can see: one the engine ran into while executing the reactor, or a run it could not hand over. A reactor that fails inside deferred work is invisible to the dispatch, which has nothing to do but wait: it times out, and the failure surfaces wherever the job system reports it.

### Nesting

A dispatch made inside a transaction the *caller* opened cannot wait for a deferred run at all. Its events, and any job row the engine wrote alongside them, stay invisible outside that transaction until it commits, so nothing could advance a checkpoint while the dispatch sits inside it waiting — the wait could only ever run out.

`dispatch/3` therefore asks the store whether a transaction is already open, before opening its own, and marks the runs it builds as nested. `Ariadne.Flow.ReactorRun.inline?/1` is true for exactly the runs that makes undeferrable — the synchronous ones of a nested dispatch — and the engine has to execute those inline, where the confirmation comes with the execution instead of being waited for. The dispatch then waits for nothing, because there is nothing left outstanding.

So a dispatch nested in an outer transaction still gets its synchronous reactors, run before it returns. What it gives up is the engine's job system for them.

## The engine

The `engine` is an `Ariadne.Flow.ReactorEngine` — the execution backend for reactors. It is configured on `new/1`, making the application struct the complete description of a context's flow setup: store, reactors, and engine.

```elixir
Ariadne.Flow.Application.new(%{store: store, reactors: [CourseSize], engine: MyEngine})
```

On dispatch, the application builds one `Ariadne.Flow.ReactorRun` per reactor — the storeless value that owns the boundary-crossing data: the reactor module, the position to resume after, the dispatch metadata, and whether the dispatch was nested in an outer transaction — and hands every run to the engine's `run/3`, along with the store. The engine is called once per reactor, in the order the reactors are declared, and decides what happens to each run: execute it now with `Ariadne.Flow.ReactorRun.execute/2` (returning the reactor's result), or defer it (returning `:ok`, so the deferred reactor never fails the dispatch). `run/3` is called synchronously inside the dispatch, and inside the dispatch's transaction while the append still holds its lock, so an engine may decide and hand the run over there, but anything slow belongs in the deferred work itself.

Any run may be deferred, synchronous ones included — provided deferring it means *durably* enqueuing it in the transaction `run/3` was called in. The job row is what makes the run certain to happen, and it is the checkpoint that job advances that the dispatch waits on. `Ariadne.Flow.ReactorRun.sync?/1` tells the engine that a dispatch is waiting for this run, which is worth knowing — a synchronous run belongs on a queue that gets to it promptly, and its first error is worth surfacing early — but it is not an instruction to run it inline.

`Ariadne.Flow.ReactorRun.inline?/1` is that instruction. It is true for a synchronous run of a dispatch [nested](#nesting) in an outer transaction, the one case where a deferred run could never be confirmed, and such a run must be executed with `Ariadne.Flow.ReactorRun.execute/2` before `run/3` returns. An engine that defers it anyway leaves the dispatch waiting on a checkpoint nobody can advance.

An engine that executes every run inline satisfies both obligations, which is why adopting deferred synchronous reactors is a change an engine makes on its own schedule: the wait is on a position, so an inline engine's checkpoints are already committed with the events by the time the wait starts, it confirms on the first look, and never sleeps.

Any `{:error, reason}` the engine returns is collected and surfaces as a raised `Ariadne.Flow.ReactorError` after the commit, exactly like a failed reactor — the events stay in the store, and the remaining runs are still handed to the engine. An engine that fails to *hand off* a deferred run (a job insert failing, say) must therefore raise instead of returning an error: only a raise rolls the transaction back, and committing events whose run was never enqueued would leave no one to catch the reactor up.

An engine that defers serialises the run with `Ariadne.Flow.ReactorRun.dump/1` and hands the resulting map to its job system (e.g. as job arguments); the worker rebuilds the run with `Ariadne.Flow.ReactorRun.load/1` and executes it against the store. The dump carries the dispatch metadata, so context the worker needs (correlation IDs, tenancy) crosses the boundary with the run rather than beside it. `Ariadne.Flow` itself knows nothing about queueing or retries — that is entirely the engine's concern.

The default engine is `Ariadne.Flow.ReactorEngine.Inline`, which runs every reactor inline. It requires no job system, so the library works out of the box.

## Concurrency

Dispatches can run in parallel. Each one takes a snapshot of the events its command cares about, decides what to do from that snapshot, and appends the resulting events. If another dispatch slips in and appends matching events in between, the later dispatch's snapshot is stale and its append is rejected.

For example, two processes try to subscribe the last free seat of a course at the same time:

1. Both read `course_capacity` and `course_subscriptions` for course 42 and see one seat left.
2. Both decide `{:ok, [%StudentSubscribedToCourse{...}]}`.
3. The first one appends its event and returns `{:ok, ...}`.
4. The second one's append fails — a new event has appeared in its query range since the snapshot was taken. `dispatch/3` returns `{:error, %Ariadne.Flow.AppendConditionError{}}`.

The caller can retry a conflicted dispatch: nothing was written, and on retry the command re-reads and now sees the other dispatch's event — the second subscription is rejected with `{:error, :course_full}` instead. This retry advice covers only the two error *values* (the conflict and the command's own refusal), which both happen before anything is committed. A raise out of a dispatch means the events were committed — re-dispatching would append them again.

Waiting for a [synchronous reactor](#synchronous-reactors) does not join that contention. The wait starts after the commit, with the dispatch's transaction closed and the append lock released, so a dispatch waiting on a reactor holds nothing that another dispatch on the same store needs. Where the engine keeps that true is in what it does with a run: handing it to a job system inside the dispatch's transaction is a row insert, while executing the reactor there does its work under the append lock, stalling every concurrent dispatch on that store for the duration.

More generally, each event reducer defines its own consistency boundary: its `query/1` tells the Application which events it depends on, and that same query is what concurrency checks against. Two reducers with non-overlapping queries never conflict — `subscribe_student(42, 7)` and `subscribe_student(99, 3)` proceed independently because their tags (`"course:42"` vs `"course:99"`) make their queries disjoint.
