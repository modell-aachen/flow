# Changelog

## [0.2.0](https://github.com/modell-aachen/flow/compare/v0.1.0...v0.2.0) (2026-07-31)


### ⚠ BREAKING CHANGES

* Store.read/3 returns %{events: [...]} without :append_condition, and EventReducer.evaluate/2 returns the reduced result instead of {result, append_condition}. Callers that read and then append conditionally build the condition with AppendCondition.for_read/2.

### Refactors

* build append conditions only for writes ([#5](https://github.com/modell-aachen/flow/issues/5)) ([29f696a](https://github.com/modell-aachen/flow/commit/29f696a16683e2689fc4379352596d228776e3ea))
