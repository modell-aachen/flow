# Introduction

Flow is an event-sourcing library for Elixir. Instead of storing state directly, a Flow-based service stores the sequence of events that produced it. Any value you need — the current capacity of a course, whether it is full, whether a subscription is allowed — is derived by reading the relevant events and folding them into a result.

Flow is built on the concept of **Dynamic Consistency Boundaries** (DCB). Rather than committing to static aggregates that fix the transactional scope up front, each read or write declares which events it depends on, and that declaration *becomes* its consistency boundary. Two operations touching disjoint sets of events never conflict; two touching the same events are ordered by optimistic concurrency. The boundary is shaped by the code that needs it, not by a pre-committed data model.

Flow is built from a small number of pieces, each covered on its own page.
