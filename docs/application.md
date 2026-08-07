# Application

An event reducer on its own only *describes* what to do. The `Ariadne.Flow.Application` module is what drives a reducer against a store — it fetches the events the reducer needs, runs them through the reducer, and optionally appends new events. It provides three operations:

- `query/2` reads events from the store and folds them through a reducer.
- `dispatch/3` does the same, and when the reducer's result is `{:ok, events}` it appends those events to the store.
- `catch_up/2` involves no reducer at all: it drives the configured reactors from their own checkpoints, with no events of its own. See [catching up out of band](#catching-up-out-of-band).

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
- `{:error, %Ariadne.Flow.AppendConditionError{}}` — a concurrent dispatch appended events matching this command's query between its read and its append, and deciding again did not get past it either. Nothing is written; see [concurrency](#concurrency).

The line between values and raises is the commit: both error values mean nothing was written, so the caller may safely dispatch again. Everything on the other side of the commit is raised instead, as one `Ariadne.Flow.PostCommitError` — a synchronous reactor failing, or not catching up in time; see [synchronous reactors](#synchronous-reactors).

`dispatch/3` is the only way events enter the store in an `Ariadne.Flow` program — every write goes through a command and is justified by the events that came before it.

The third argument is a keyword list of options, all optional — the calls above pass none, so they look like a two-argument call. The available options are covered in the next section.

### dispatch!/3

`dispatch!/3` behaves like `dispatch/3` but raises where `dispatch/3` returns an error value, so a caller that only cares about the success path cannot silently drop a failure. Each exception type carries its retry semantics:

- `Ariadne.Flow.CommandError` — the command refused, wrapping the refusal `reason`. Nothing was written; dispatching again is pointless until the state it decided from changes.
- `Ariadne.Flow.AppendConditionError` — the append condition failed on every attempt the dispatch made. Nothing was written; dispatching again is safe and decides from the new events, but a conflict the [retries](#retrying-a-conflict) could not get past is usually saying something about the command's consistency boundary.
- `Ariadne.Flow.PostCommitError` — raised by `dispatch/3` and `dispatch!/3` alike: the events were committed and a synchronous reactor did not deliver what the caller declared it wanted. Its `reason` says which way — `:failure` for a reactor that failed, `:timeout` for one that did not catch up in time. **Never re-dispatch the command**, whichever it is: the events are already in the store and dispatching again would append them a second time. The one exception is a [nested](#nesting) dispatch, whose events are the caller's to roll back; the error carries `nested: true` and its message says so. See [synchronous reactors](#synchronous-reactors).

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

The `name` is what the checkpoint is keyed on, so it must be stable across versions. The `reactor/0` convention is part of the public contract: the module name is all that identifies a reactor, which lets a reactor run be serialised and handed to a job system.

The `filter` is a query item like a projection's, with one exception: a reactor reacts to every event it matches, so it cannot ask for [`only_last_event`](event_reducer.html#reducing-the-last-event-only) — the events it skipped would be checkpointed past and never delivered. `Reactor.new/2` raises on such a filter.

A reactor with no checkpoint yet has to start somewhere, and `start_after_position` is the declaration that says where:

- **`:head`** — the default: *from now*. The reactor starts at the first dispatch that concerns it and never sees an event appended before that one, whatever the store already holds.
- **an integer** — after that position. `0` is the store's origin, so the reactor works through the entire history matching its filter on its first run; any other position starts it after there.

History is opt-in because a reactor has effects. A read model can be rebuilt from the origin as often as you like; a reactor that sends mail cannot replay a year of it. `:head` is the safe default, and a reactor that wants history says so.

The declaration only ever decides where the *first* run begins. The moment a dispatch concerns a reactor, that declaration is turned into a real checkpoint — written into the store **in the same transaction as the events**, `:head` resolving to the position just before the first of them. From then on the checkpoint is the only thing a run resumes from, which is why lowering the declaration later does not replay anything, why a reactor cannot be dragged backwards by a stale run, and why a run carries no starting position of its own: it names a reactor, and the store already knows where that reactor stands.

```elixir
Ariadne.Flow.Reactor.new(
  %{name: "course_size", filter: %{types: [StudentSubscribedToCourse]}, start_after_position: 0},
  fn event, _metadata -> update_read_model(event) end
)
```

Writing the checkpoint with the events is what makes *from now* mean anything under concurrency. Appends serialise on the store's append lock and that lock is held until the dispatch commits, so the first dispatch to append is also the first to start the reactor, and a dispatch that lost the race finds the checkpoint already there and leaves it alone. It also means a reactor can never miss events because a run went astray: whatever happens to the run, the checkpoint sitting in front of those events is committed, and the next dispatch or [`catch_up/2`](#catching-up-out-of-band) delivers them.

A reactor declaring history gets it from whoever runs it first, which may be a dispatch — that dispatch works through the whole history after committing, and the caller waits for it if the reactor is [synchronous](#synchronous-reactors). It is self-healing and happens once, but the way to keep it off a request is to run `catch_up/2` at deploy or boot, before there is a dispatch to lose the race to.

Every successful `dispatch/3` then drives the reactors nobody else will over the events they have not seen, in declaration order, **after the transaction has committed**. Nothing a reactor does can undo the dispatch: its transaction is closed and its events are in the store before the first handler is called. The fate of the dispatch depends on the command and nothing else.

### When a reactor fails

An **asynchronous reactor's failure never surfaces in the dispatch**, however it is expressed. Returning `{:error, reason}`, raising, exiting — a `GenServer.call` to a dead process is the everyday one — and throwing are all contained; the dispatch returns `{:ok, ...}` as if nothing had happened. There is nothing useful the dispatcher could do with the failure: the events are committed, and turning that into an error value would only invite a retry wrapper to append them again.

Every failure, synchronous or not, is reported as a `[:ariadne, :flow, :reactor, :failure]` telemetry event and logged with its origin — the stacktrace included, when there was one. That log line is the only place a raise inside a reactor keeps its trace, since the exception a synchronous failure raises at the caller carries the *dispatch's* stack, not the reactor's.

Recovery does not need the dispatcher either. The failed reactor's checkpoint is parked right in front of the event it choked on, so the next dispatch or catch-up runs it again from there once the cause is fixed. That is also the shape of the guarantee reactors come with: **side effects are at-least-once**, even without a job system behind them. A reactor that fails half-way through a batch will see the events it already processed again, so a handler whose effect must not happen twice has to make itself idempotent.

A failure stops nothing else. Every reactor declared after a failing one is still run, in declaration order — the failed one's checkpoint stayed put, so skipping the others would cost them their run and buy nothing.

Only a **synchronous** reactor's failure reaches the caller, because a caller declared it wanted to read that reactor's work back. See below.

## Synchronous reactors

A reactor may declare that a dispatch must not return before it has caught up with the events that dispatch appended. Passing `sync: true` marks it synchronous; the default is asynchronous.

```elixir
Ariadne.Flow.Reactor.new(
  %{name: "course_size", filter: %{types: [StudentSubscribedToCourse]}, sync: true},
  fn event, _metadata -> update_read_model(event) end
)
```

`sync: true` says something about the dispatch, not about where the reactor runs. Both kinds of reactor are offered to the [engine](#the-engine) the same way and either may end up on a job system; what synchronous adds is that once the events are committed, `dispatch/3` waits for the reactor to reach them before returning. Declare it for the reactor whose work the caller reads back immediately — a request that finishes by rendering the read model this reactor maintains.

What the dispatch waits on is the reactor's **checkpoint**: it returns once every synchronous reactor has checkpointed at or past the last of the dispatch's events *that reactor is going to process*. The target is per reactor, because a checkpoint records the last matching event a reactor consumed and [stays put](#reactors) on events its filter skips — a reactor is caught up once it reaches the highest appended position matching its filter, and one that matches none of the dispatch's events has nothing to catch up to and is not waited on at all.

Deliberately not the state of whatever the engine scheduled — a reactor's events are consumed one run at a time, so a concurrent dispatch's run may be the one that processes these events and leave this dispatch's own run with nothing to do, and a job that no-ops is not a reactor that failed to catch up. The checkpoint is the store's own record of how far the reactor got, so it holds whoever advanced it.

The wait is bounded by the `:await_timeout` option, a non-negative number of milliseconds defaulting to 5000:

```elixir
iex> Ariadne.Flow.Application.dispatch(application, subscribe_student(42, 7), await_timeout: 1_000)
```

There is no `:infinity` — a dispatch that may never return is not a guarantee a caller can act on — and anything that is not a non-negative integer raises `ArgumentError`. That check happens before the command runs, so a bad option is a plain argument error with nothing written, rather than a crash on the far side of the commit.

When the timeout runs out, `dispatch/3` raises `Ariadne.Flow.PostCommitError` with `reason: :timeout`, carrying every reactor that did not confirm together with the position it was awaited at. That raise does not mean the dispatch failed: the events are committed and the reactors are still going to catch up. What went unmet is the caller's expectation of reading its own write back. **Never re-dispatch the command** — the events are already in the store and dispatching again would append them a second time. Returning an error value instead would invite exactly that, since any retry-on-error wrapper around the dispatch would double-write; and returning `{:ok, ...}` would quietly withdraw the guarantee the caller asked for by declaring the reactor synchronous.

The wait is for the reactor to reach this dispatch's events, but a reactor resumes from wherever its checkpoint stands, so it reaches them by working through everything in between. A synchronous reactor that has fallen ten thousand events behind cannot confirm a new dispatch until it has drained all ten thousand — which makes a single lagging synchronous reactor a way to turn *every* subsequent dispatch into a timeout, and the reason a reactor declared synchronous belongs on a queue that keeps up with it. A `:timeout` `Ariadne.Flow.PostCommitError` naming the same reactor across unrelated dispatches is what that looks like from the outside.

Each wait is reported as a `[:ariadne, :flow, :dispatch, :await]` telemetry span, with the reactors and positions it awaited, whether it ended `:confirmed` or in a `:timeout`, and how many rounds of checkpoint reads it took — so how long callers actually block, and how close to the timeout they come, is measurable before it turns into raises. Nothing is emitted for a dispatch with nothing to await.

A synchronous reactor that *fails* is a different outcome, and `Ariadne.Flow.PostCommitError` with `reason: :failure` is raised for it rather than the timeout being waited out — a definitive failure says more than a wait that could only run out. That holds for a failure the dispatch can see, which is any run Flow executed itself. A reactor that fails inside a job system's worker is invisible to the dispatch, which has nothing to do but wait: it times out, and the failure surfaces wherever the job system reports it.

### Nesting

A dispatch made inside a transaction the *caller* opened cannot wait for a scheduled run at all. Its events, and any job row the engine wrote alongside them, stay invisible outside that transaction until it commits, so nothing could advance a checkpoint while the dispatch sits inside it waiting — the wait could only ever run out.

`dispatch/3` therefore asks the store whether a transaction is already open before opening its own, and splits the runs accordingly:

- **Synchronous runs are never offered to the engine.** Flow executes them itself, in the caller's transaction, where the uncommitted events are visible. The confirmation comes with the execution rather than being waited for, so the dispatch waits for nothing. What it gives up is the job system for them, and the isolation that comes with a committed transaction: a synchronous reactor that fails here raises inside the caller's transaction, and what that does to the caller's work is the caller's to decide. The `Ariadne.Flow.PostCommitError` says so — it carries `nested: true` and drops the usual "never re-dispatch", because letting the raise propagate rolls the events back with everything else the caller was doing, and the command can then be dispatched again.
- **Asynchronous runs are offered to the engine as usual and never executed.** A job row inserted in the outer transaction runs after that transaction commits, which is exactly when the next dispatch or `catch_up/2` would have picked the same events up. Whether an engine is configured or not, the work lands in the same place — so nesting behaves the same under every engine.

## The engine

The `engine` is an `Ariadne.Flow.ReactorEngine` — a **scheduler**, not an execution backend. It is configured on `new/1`, making the application struct the complete description of a context's flow setup: store, reactors, and engine.

```elixir
Ariadne.Flow.Application.new(%{store: store, reactors: [CourseSize], engine: MyEngine})
```

It has one callback:

```elixir
@callback schedule(
            reactor_runs :: [%Ariadne.Flow.ReactorRun{}],
            store :: %Ariadne.Flow.Store{},
            opts :: keyword()
          ) :: [%Ariadne.Flow.ReactorRun{}]
```

`schedule/3` is called **once per dispatch, inside the dispatch's transaction, with the whole run set** — one `Ariadne.Flow.ReactorRun` per reactor, in declaration order. A run is the storeless value that crosses the boundary to a job system: the reactor module and the dispatch metadata, and nothing else. Where the reactor resumes is not part of it, because the checkpoint in the store already says.

The engine returns the runs it scheduled, and that return carries its single obligation: **a run may be reported as scheduled only if it will execute even when this process dies, given the transaction commits.** A row inserted into a job table is that. A message to another process is not. `Ariadne.Flow` executes every run the engine did not claim, so a scheduler that claims fewer runs than it was given — or none, or no engine at all — costs promptness and nothing else.

A claimed run is recognised by its reactor, so returning runs you rebuilt (`load/1` on the args you just dumped, say) claims them just as returning the runs you were handed does.

An Oban engine is the whole of it: `insert_all` the job rows in the transaction `schedule/3` was called in, return the runs. Nothing about failure isolation, nothing about running anything inline — those obligations do not exist any more, because the engine never holds them. Since `schedule/3` runs inside the dispatch's transaction while the append still holds its lock, anything slower than that insert belongs in the job, not here.

The worker on the other side rebuilds the run and executes it:

```elixir
args = Ariadne.Flow.ReactorRun.dump(run)   # in schedule/3
# ... later, in the worker:
args |> Ariadne.Flow.ReactorRun.load() |> Ariadne.Flow.ReactorRun.execute(store)
```

The dump carries the dispatch metadata, so context the worker needs (correlation IDs, tenancy) crosses the boundary with the run rather than beside it. `Ariadne.Flow` itself knows nothing about queueing or retries — that is entirely the engine's concern. `Ariadne.Flow.ReactorRun.sync?/1` says whether a dispatch is waiting on this run, which is worth knowing when choosing a queue, but it is not an instruction about where to run it.

**There is no default engine.** Without one, every run is Flow's own to execute after the commit, which is the whole library working out of the box — including synchronous reactors, whose checkpoint is advanced before the wait begins, so the first look confirms and nothing ever sleeps.

## Catching up out of band

A dispatch drives reactors over the events *it* appended. `catch_up/2` drives them over whatever their checkpoints have not reached yet, with no events and no command of its own:

```elixir
iex> Ariadne.Flow.Application.catch_up(application)
:ok
```

It answers what a dispatch structurally cannot:

- **A new reactor over history.** A reactor declaring [`start_after_position: 0`](#reactors) has the whole store to work through before it is current. Calling `catch_up/2` at deploy or boot is what gets it there, instead of leaving the work to the next dispatch that happens along.
- **A manual or scheduled retrigger.** A reactor parked in front of a poison event resumes once the cause is fixed — but only when something runs it. A cron calling `catch_up/2` is that something, and it does not need a write to hang the work off.
- **Events another node appended.** Reactors are driven by the dispatch that produced the events, so a node that only reads never runs them. `catch_up/2` reacts to writes that happened elsewhere.
- **Replay**, which is moving a checkpoint back and then catching up. Renaming the reactor does it today: the name is what its checkpoint is keyed on, and a handler that changed enough to need a replay is arguably a new reactor anyway. Truncating whatever effects the old one left behind is the caller's job.

Each configured reactor is built into a run and offered to the [engine](#the-engine) exactly as a dispatch's runs are — the same `schedule/3`, the same `:metadata` option riding along on each run, and the same rule that Flow executes whatever the engine did not claim. What differs is everything a dispatch's events imply and a catch-up has none of:

- **It returns a value.** `:ok`, or `{:error, %Ariadne.Flow.ReactorError{}}` carrying one entry per failed reactor with its `name`, the `position` it failed at and its `reason` — a raised handler included, caught the same way a dispatch catches it. A dispatch cannot return a reactor failure because its events are committed and a retry wrapper around it would append them twice; a catch-up writes nothing of its own, so retrying it is always safe — and an error value is what a cron caller can act on.
- **Nothing is awaited.** No events were appended and no caller is waiting to read a write back, so `sync: true` says nothing here. A synchronous reactor is treated like any other and `catch_up/2` returns without waiting on a checkpoint.
- **Each batch commits on its own.** Outside a dispatch's transaction, every batch `consume` processes commits by itself. A catch-up that crashes half-way through a large history keeps the progress it made and resumes from there.
- **A reactor that declared a position and has never run is started at it**, exactly as a dispatch would have. A **`:head` reactor with no checkpoint is skipped**: *from now* is defined by the dispatch that first runs it, and a catch-up has no events to define it against. Such a reactor is left out of the pass entirely rather than being started at the current head.

Nesting says nothing here either. A catch-up has no events of its own to keep invisible, so one made inside a transaction the caller opened runs its reactors like any other — its consumed batches and its checkpoint writes simply join that transaction and are undone with it, the way every other store write inside it is.

Running a catch-up while dispatches are happening needs no coordination from the caller. A reactor's events are consumed under a lock held per reactor, taken before its checkpoint is read and released when the new one is committed, so a post-dispatch run and a scheduled catch-up for the same reactor serialise against each other: whichever arrives second reads the first's committed checkpoint and continues from there, or finds nothing left and no-ops. Neither can move the checkpoint backwards, because a checkpoint is only ever created where it does not exist. Two catch-ups running at once are the same story.

## Concurrency

Dispatches can run in parallel. Each one takes a snapshot of the events its command cares about, decides what to do from that snapshot, and appends the resulting events. If another dispatch slips in and appends matching events in between, the later dispatch's snapshot is stale and its append is rejected.

For example, two processes try to subscribe the last free seat of a course at the same time:

1. Both read `course_capacity` and `course_subscriptions` for course 42 and see one seat left.
2. Both decide `{:ok, [%StudentSubscribedToCourse{...}]}`.
3. The first one appends its event and returns `{:ok, ...}`.
4. The second one's append is rejected — a new event has appeared in its query range since the snapshot was taken — so that dispatch decides again: it re-reads, now sees the other dispatch's event, and refuses the second subscription with `{:error, :course_full}`.

### Retrying a conflict

There is only one thing a caller can do with a rejected append: dispatch the same command again, now that the events it conflicted with are there to be read. `dispatch/3` does that itself. Each attempt is a fresh transaction with a fresh read, reduce and decision — a **re-decision, not a re-play** — so a conflicted command either succeeds against the state it now sees or returns its own refusal, and never appends what an earlier attempt decided on. Reactors are driven over the events of the attempt that committed, being the only events the dispatch appended.

How many attempts a dispatch gets is bounded by the `:attempts` option, a positive integer defaulting to 3:

```elixir
iex> Ariadne.Flow.Application.dispatch(application, subscribe_student(42, 7), attempts: 1)
```

`attempts: 1` opts out of retrying entirely. Anything that is not a positive integer raises `ArgumentError`, before the command runs and with nothing written. The bound is an option on the dispatch rather than on the application because how much contention is worth riding out is a property of the command doing the deciding.

The bound is small, and there is no delay between attempts, because appends on a store serialise on the append lock: a dispatch that lost a race usually wins the next attempt. What more attempts cannot fix is the reason for the contention. Every attempt reads before it appends, so N dispatches contending over the same events do O(N²) reads between them — the retries are there to absorb the occasional loser, not to make a hot consistency boundary work. Once they run out, `dispatch/3` returns `{:error, %Ariadne.Flow.AppendConditionError{}}`.

Retrying must not hide the contention it absorbs, so every dispatch is reported as a `[:ariadne, :flow, :dispatch]` telemetry span carrying the number of attempts it took as a measurement, and its outcome as metadata: `:ok`, `:conflict` for a dispatch that exhausted its attempts, `:error` for a command that refused. Attempt counts climbing above one are what a consistency boundary drawn too broadly looks like from the outside, and they say so before the conflicts start surviving the retries.

A dispatch [nested](#nesting) in a transaction the caller opened is not retried — it gets a single attempt, whatever `:attempts` says. Its re-read would happen inside the caller's transaction: under repeatable read or serializable it sees the same snapshot and fails exactly as deterministically, and under read committed the outer transaction would end up containing work decided on state that changed mid-flight. As with a synchronous reactor, nesting changes the rules, and retrying is left to whoever owns the outer transaction.

Retrying is safe because nothing was committed, which is also the limit of it. That covers the two error *values* — the conflict and the command's own refusal — and nothing else. A raise out of a dispatch means the events *were* committed, and re-dispatching would append them a second time. Now that the framework retries the error values itself, that line is all a caller has left to observe: there is no conflict to catch and retry any more, only raises that must not be retried.

Reactors do not join that contention either. Every run — Flow's own and the engine's alike — happens after the dispatch has committed, with the append lock released, so no reactor's work stalls a concurrent dispatch on the same store, and neither does a dispatch [waiting](#synchronous-reactors) on one. All the dispatch's transaction ever holds the lock for is the append itself, the checkpoint init that goes with it, and the engine's row insert.

More generally, each event reducer defines its own consistency boundary: its `query/1` tells the Application which events it depends on, and that same query is what concurrency checks against. Two reducers with non-overlapping queries never conflict — `subscribe_student(42, 7)` and `subscribe_student(99, 3)` proceed independently because their tags (`"course:42"` vs `"course:99"`) make their queries disjoint.
