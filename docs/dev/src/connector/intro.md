# Develop Connectors

This page describes the development workflow to develop connectors. For design docs, see

- [Source](./source.md)

RisingWave supports a lot of connectors (sources and sinks).
However, developing connectors is tricky because it involves external systems:

- Before developing and test, it's troublesome to set up the development environment
- During testing, we need to seed the external system with test data (perhaps with some scripts)
- The test relies on the configuration of the setup. e.g., the test needs to know the port of your Kafka in order to
- We need to do the setup for both CI and local development.

Our solution is: we resort to RiseDev, our all-in-one development tool, to help manage external systems and solve these
problems.

Before going to the specific methods described in the sections below, the principles we should follow are:

- *environment-independent*: It should easy to start cluster and run tests on your own machine, other developers'
  machines, and CI.
    * Don't use hard-coded configurations (e.g., `localhost:9092` for Kafka).
    * Don't write too many logic in `ci/scripts`. Let CI scripts be thin wrappers.
- *self-contained* tests: It should be straightforward to run one test case, without worrying about where is the script
  to prepare the test.
    * Don't put setup logic, running logic and verification logic of a test in different places.

Reference: for the full explanations of the difficulies and the design of our solution,
see [here](https://github.com/risingwavelabs/risingwave/issues/12451#issuecomment-2051861048).

The following sections first walk you through what is the development workflow for
existing connectors, and finally explain how to extend the development framework to support a new connector.

<!-- toc -->

## Set up the development environment

RiseDev supports starting external connector systems (e.g., Kafka, MySQL) just like how it starts the RisingWave
cluster, and other standard systems used as part of the RisingWave Cluster (e.g., MinIO, etcd, Grafana).

You write the profile in `risedev.yml` (Or `risedev-profiles.user.yml` ), e.g., the following config includes Kafka and
MySQL, which will be used to test sources.

```yml
  my-cool-profile:
    steps:
      # RisingWave cluster
      - use: minio
      - use: sqlite
      - use: meta-node
        meta-backend: sqlite
      - use: compute-node
      - use: frontend
      - use: compactor
      # Connectors
      - use: kafka
        address: message_queue
        port: 29092
      - use: mysql
        port: 3306
        address: mysql
        user: root
        password: 123456
```

Then

```sh
# will start the cluster along with Kafka and MySQL for you
risedev d my-cool-profile
```

For all config options of supported systems, check the comments in `template` section of `risedev.yml` .

### Escape hatch: `user-managed` mode

`user-managed` is a special config. When set to `true` , you will need to start the system by yourself. You may wonder
why bother to add it to the RiseDev profile if you start it by yourself. In this case, the config will still be loaded
by RiseDev, which will be useful in tests. See chapters below for more details.

The `user-managed` mode can be used as a workaround to start a system that is not yet supported by RiseDev, or is buggy.
It's also used to config the CI profile. (In CI, all services are pre-started by `ci/docker-compose.yml` )

Example of the config:

```yml
      - use: kafka
        user-managed: true
        address: message_queue
        port: 29092
```

## End-to-end tests

The e2e tests are written in `slt` files. There are 2 main points to note:

1. Use `system ok` to run `bash` commands to interact with external systems.
   Use this to prepare the test data, and verify the results. The whole lifecycle of
   a test case should be written in the same `slt` file.
2. Use `control substitution on` and then use environment variables to specify the config of the external systems, e.g.,
   the port of Kafka.

Refer to
the [sqllogictest-rs documentation](https://github.com/risinglightdb/sqllogictest-rs#extension-run-external-shell-commands)
for the details of `system` and `substitution` .

---

Take Kafka as an example about how to the tests are written:

When you use `risedev d` to start the external services, related environment variables for Kafka will be available when
you run `risedev slt`:

```sh
RISEDEV_KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:9092"
RISEDEV_KAFKA_WITH_OPTIONS_COMMON="connector='kafka',properties.bootstrap.server='127.0.0.1:9092'"
RPK_BROKERS="127.0.0.1:9092"
```

The `slt` test case looks like this:

```
control substitution on

# Note: you can also use envvars in `system` commands, but usually it's not necessary since the CLI tools can load envvars themselves.
system ok
rpk topic create my_source -p 4

# Prepared test topic above, and produce test data now
system ok
cat << EOF | rpk topic produce my_source -f "%p %v\n" -p 0
0 {"v1": 1, "v2": "a"}
1 {"v1": 2, "v2": "b"}
2 {"v1": 3, "v2": "c"}
EOF

# Create the source, connecting to the Kafka started by RiseDev
statement ok
create source s0 (v1 int, v2 varchar) with (
  ${RISEDEV_KAFKA_WITH_OPTIONS_COMMON},
  topic = 'my_source',
  scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;
```

See `src/risedevtool/src/risedev_env.rs` for variables supported for each service.

> Note again: You need to use `risedev d` to start the cluster, and then use `risedev slt` to run the tests. It doesn't
> work if you start the cluster by yourself without telling RiseDev, or you use raw `sqllogictest` binary directly.
>
> How it works: `risedev d` will write env vars to `.risingwave/config/risedev-env`,
> and `risedev slt` will load env vars from this file.

For example, run the Redis sink test locally without preparing Redis separately:

```sh
./risedev d local-redis-sink-test
./risedev slt './e2e_test/sink/redis_sink.slt'
./risedev k
```

### Tips for writing `system` commands

Refer to
the [sqllogictest-rs documentation](https://github.com/risinglightdb/sqllogictest-rs#extension-run-external-shell-commands)
for the syntax.

For simple cases, you can directly write a bash command, e.g.,

```
system ok
mysql -e "
    DROP DATABASE IF EXISTS testdb1; CREATE DATABASE testdb1;
    USE testdb1;
    CREATE TABLE tt1 (v1 int primary key, v2 timestamp);
    INSERT INTO tt1 VALUES (1, '2023-10-23 10:00:00');
"

system ok
cat << EOF | rpk topic produce my_source -f "%p %v\n" -p 0
0 {"v1": 1, "v2": "a"}
1 {"v1": 2, "v2": "b"}
2 {"v1": 3, "v2": "c"}
EOF
```

For more complex cases, you can write a test script, and invoke it in `slt`. Scripts can be written in any language you
like, but kindly write a `README.md` to help other developers get started more easily.

- For ad-hoc scripts (only used for one test), it's better to put next to the test file.

  e.g., [`e2e_test/source_inline/kafka/consumer_group.mjs`](https://github.com/risingwavelabs/risingwave/blob/c22c4265052c2a4f2876132a10a0b522ec7c03c9/e2e_test/source_inline/kafka/consumer_group.mjs),
  which is invoked
  by [`consumer_group.slt`](https://github.com/risingwavelabs/risingwave/blob/c22c4265052c2a4f2876132a10a0b522ec7c03c9/e2e_test/source_inline/kafka/consumer_group.slt)
  next to it.
- For general scripts that can be used under many situations, put it in `e2e_test/commands/`. This directory will be
  loaded in `PATH` by `risedev slt`, and thus function as kind of "built-in" commands.

  A common scenario is when a CLI tool does not accept envvars as arguments. In such cases, instead of manually
  specifying the arguments each time invoking it in `slt`, you can create a wrapper to handle this implicitly, making it
  more
  concise. [`e2e_test/commands/mysql`](https://github.com/risingwavelabs/risingwave/blob/c22c4265052c2a4f2876132a10a0b522ec7c03c9/e2e_test/commands/mysql)
  is a good demonstration.

---
Tips for debugging:

- Use `echo` to check whether the environment is correctly set.

    ```
    system ok
    echo $PGPORT
    ----
    placeholder
    ```

  Then running `risedev slt` will return error "result mismatch", and shows what's the output
  of the `echo` command, i.e., the value of `PGPORT`.

- Use `risedev show-risedev-env` to see the environment variables available for `risedev slt`, after you starting the
  cluster with `risedev d`.

## Adding a new connector to the development framework

Refer to [#16449](https://github.com/risingwavelabs/risingwave/pull/16449) ( `user-managed` only MySQL),
and [#16514](https://github.com/risingwavelabs/risingwave/pull/16514) (Docker based MySQL) as examples.

1. Add a new service in `template` section of `risedev.yml`.
   And add corresponding config in `src/risedevtool/src/service_config.rs` .
2. Implement the new service task, and add it to `src/risedevtool/src/bin/risedev-dev.rs`.
3. Add environment variables you want to use in the `slt` tests in `src/risedevtool/src/risedev_env.rs`.
4. Write tests according to the style explained in the previous section.

<!-- That's all?? -->
