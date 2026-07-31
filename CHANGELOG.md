# Changelog

## [0.3.0](https://github.com/modell-aachen/flow/compare/v0.2.0...v0.3.0) (2026-07-31)


### ⚠ BREAKING CHANGES

* add only_last_event to query items ([#11](https://github.com/modell-aachen/flow/issues/11))
* Store.Backend gained a count/1 callback, so a backend outside this repo has to implement it. Store.Postgres.total_events/1 is gone — call Store.count/1 instead.

### Features

* add count to the store backend contract ([7723cc8](https://github.com/modell-aachen/flow/commit/7723cc8312ba0a5d53a675e565520eaca16a7e50))
* add only_last_event to query items ([#11](https://github.com/modell-aachen/flow/issues/11)) ([07edbb5](https://github.com/modell-aachen/flow/commit/07edbb53b4024f2e64fb674dd4116873bdaf2ad6))
* define the store backend behaviour ([#7](https://github.com/modell-aachen/flow/issues/7)) ([525058a](https://github.com/modell-aachen/flow/commit/525058aa3e7bfabe81d5133c32e6d724740177f3))


### Refactors

* make the normalised query a struct ([#13](https://github.com/modell-aachen/flow/issues/13)) ([9877bde](https://github.com/modell-aachen/flow/commit/9877bde34a66bd083899f99a230d3bd876a94ea6))
* merge Postgres.Query into the backend ([f11647d](https://github.com/modell-aachen/flow/commit/f11647dfd59ad74f7c08df2e821b9649d8f483bb))
* normalise a query once per dispatch ([#12](https://github.com/modell-aachen/flow/issues/12)) ([8b3a9f3](https://github.com/modell-aachen/flow/commit/8b3a9f3aa860ba737165bf5a3253c19ea25e8340))
* normalise the query in Store.consume ([#14](https://github.com/modell-aachen/flow/issues/14)) ([bdb13fe](https://github.com/modell-aachen/flow/commit/bdb13feda1de863b2bc4ec5d152c7fde658721bc))


### Documentation

* drop moduledocs until 1.0 ([2dcaa41](https://github.com/modell-aachen/flow/commit/2dcaa41f4158d24c6db08c27ddbfd1617c0bf3fd))
* scope the dump portability claim per backend ([9f01782](https://github.com/modell-aachen/flow/commit/9f017824de527fdbd033651d42e499fc06108001))


### Dependencies

* bump postgrex from 0.22.2 to 0.22.3 in the hex group ([#10](https://github.com/modell-aachen/flow/issues/10)) ([c643445](https://github.com/modell-aachen/flow/commit/c64344570fba246c75265329649ff33693ebfb18))

## [0.2.0](https://github.com/modell-aachen/flow/compare/v0.1.0...v0.2.0) (2026-07-31)


### ⚠ BREAKING CHANGES

* Store.read/3 returns %{events: [...]} without :append_condition, and EventReducer.evaluate/2 returns the reduced result instead of {result, append_condition}. Callers that read and then append conditionally build the condition with AppendCondition.for_read/2.

### Refactors

* build append conditions only for writes ([#5](https://github.com/modell-aachen/flow/issues/5)) ([29f696a](https://github.com/modell-aachen/flow/commit/29f696a16683e2689fc4379352596d228776e3ea))
