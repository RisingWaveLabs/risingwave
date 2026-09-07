# Build Profiles

RisingWave uses Cargo profiles to manage build settings. To briefly introduce Cargo profiles, here is a snippet from the [Cargo References](https://doc.rust-lang.org/cargo/reference/profiles.html):

> Profiles provide a way to alter the compiler settings, influencing things like optimizations and debugging symbols.
>
> Cargo has 4 built-in profiles: `dev`, `release`, `test`, and `bench`. The profile is automatically chosen based on which command is being run if a profile is not specified on the command-line. In addition to the built-in profiles, custom user-defined profiles can also be specified.

All profiles talked in this document are defined in the `Cargo.toml` file of the root directory of the project. Please always refer to it for the most up-to-date information.

## Built-in Profiles

RisingWave tweaks some settings of the built-in profiles to better fit its needs, in the sections of `[profile.<built-in-profile>]` in `Cargo.toml`. For example,

- `dev`: for local development and testing

  - completely disables [LTO](https://doc.rust-lang.org/cargo/reference/profiles.html#lto) to speed up the build time
  - emits line tables only instead of full debug information, which keeps `file:line` and inlined
    frames in backtraces while cutting the size of `target/` by several times

- `release`: for local testing with near-production performance

  - completely disables LTO to speed up the build time
  - embeds full debug information to help with debugging in production

## Custom Profiles

RisingWave also defines some custom profiles that inherit from the built-in profiles, in the sections of `[profile.<custom-profile>]` in `Cargo.toml`. For example,

- `production`: for distribution and production deployment

  - inherits from `release`
  - enables aggressive code optimizations (like LTO) for maximum performance, at the cost of significantly increased build time

- `ci-dev`: for `pull-request` pipelines in CI

  - inherits from `dev`
  - tweaks some settings to reduce the build time and binary size
  - enables code optimizations for 3rd-party dependencies to improve CI performance

- `ci-release`: for `main` and `main-cron` pipelines in CI

  - inherits from `release`
  - tweaks some settings to reduce the build time and binary size
  - enables more runtime checks (like debug assertions and overflow checks) to catch potential bugs

- `ci-sim`: for `madsim` simulation tests in CI
  - similar to `ci-dev`
  - enables slight code optimizations for all crates to improve CI performance under single-threaded madsim execution

## Comparisons

To give a better idea of the differences between the profiles, here is a matrix comparing the profiles:

| Profile      | Debug Info     | `cfg(debug_assertions)` | Performance | Build Time |
| ------------ | -------------- | ----------------------- | ----------- | ---------- |
| `dev`        | Backtrace only | `true`                  | Bad         | Fastest    |
| `release`    | Full           | `false`                 | Good        | Slow       |
| `production` | Full           | `false`                 | Best        | Slowest    |
| `ci-dev`     | Backtrace only | `true`                  | Medium      | Fast       |
| `ci-release` | Backtrace only | `true`                  | Good        | Slow       |
| `ci-sim`     | Backtrace only | `true`                  | Medium      | Medium     |

Some miscellaneous notes:

- Compared to "Backtrace only", "Full" debug information additionally includes the capability to attach a debugger at runtime or on core dumps, to inspect variables and stack traces.
- There are also other subtle differences like incremental compilation settings, overflow checks, and more. They are not listed here for brevity.
- `cfg(debug_assertions)` can be roughly used to determine whether it's a production environment or not. Note that even though `ci-release` contains `release` in its name, the `debug_assertions` are still enabled.

## Choose a Profile

By default, RisingWave (and RiseDev) uses the `dev` profile for local development and testing. To use `release` profile instead, you can set the corresponding configuration entry by running `risedev configure`. Other profiles are for their specific use cases and are not meant to be used directly by developers, thus not available with RiseDev.
