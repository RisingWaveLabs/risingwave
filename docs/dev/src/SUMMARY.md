# Summary

[Introduction](./intro.md)
[Contribution Guidelines](./contributing.md)

---

# Building and debugging RisingWave

- [Building and Running](./build-and-run/intro.md)
    - [Profiles](./build-and-run/profiles.md)
- [Testing](./tests/intro.md)
- [Debugging](./debugging.md)
- [Observability](./observability.md)
    - [Metrics](./metrics.md)

---

# Benchmarking and profiling

- [CPU Profiling](./benchmark-and-profile/cpu-profiling.md)
- [Memory (Heap) Profiling](./benchmark-and-profile/memory-profiling.md)
- [Microbench](./benchmark-and-profile/microbenchmarks.md)

---

# Specialized topics

- [Develop Connectors](./connector/intro.md)
    - [Source](./connector/source.md)
- [Continuous Integration](./ci.md)

---

# Design docs

- [Architecture Design](./design/architecture-design.md)
- [Streaming Engine](./design/streaming-overview.md)
    - [Checkpoint](./design/checkpoint.md)
    - [Aggregation](./design/aggregation.md)
    - [Row Pattern Recognition (MATCH_RECOGNIZE)](./design/match-recognize.md)
    - [MView on Top of MView](./design/mv-on-mv.md)
    - [Backfill](./design/backfill.md)
    - [Streaming Parallelism Configuration](./design/streaming-parallelism.md)
- [State Store](./design/state-store-overview.md)
    - [Shared Buffer](./design/shared-buffer.md)
    - [Relational Table](./design/relational-table.md)
    - [Multiple Object Storage Backends](./design/multi-object-store.md)
- [Meta Service](./design/meta-service.md)
- [Data Model and Encoding](./design/data-model-and-encoding.md)
- [Batch Local Execution Mode](./design/batch-local-execution-mode.md)
- [Consistent Hash](./design/consistent-hash.md)
- [Keys](./design/keys.md)
<!--

TODO:

- [RiseDev](./risedev.md)
- [Error Handling](./error-handling.md)
- [Develop Connector]()
    - [Connector e2e tests]()
    - [integration tests]()
- [Compile time]()
    - [Crate organization]()
    - [Optimize for compile time]()
- [Adding dependencies]
