FROM ubuntu:24.04 AS base

ENV LANG en_US.utf8

RUN apt-get update \
  && apt-get -y install ca-certificates build-essential libsasl2-dev openjdk-21-jdk software-properties-common \
    python3.12 python3.12-dev openssl pkg-config curl git libblas-dev liblapack-dev libomp-dev

FROM base AS rust-base

RUN apt-get update && apt-get -y install make cmake protobuf-compiler bash lld unzip rsync

# Install Node.js as dependency for building the dashboard.
# Bump version together with `dashboard/.node-version`.
ENV NVM_DIR /root/.nvm
ENV NODE_VERSION 20.11.1
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash \
  && . $NVM_DIR/nvm.sh \
  && nvm install $NODE_VERSION \
  && npm install --global pnpm@10.14.0
ENV PATH $NVM_DIR/versions/node/v$NODE_VERSION/bin:$PATH

SHELL ["/bin/bash", "-c"]

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /root/.cargo/bin:$PATH
ENV CARGO_INCREMENTAL=0

COPY rust-toolchain rust-toolchain

# We need to add the `rustfmt` dependency, otherwise `risingwave_pb` will not compile
RUN rustup self update \
  && rustup toolchain install --profile minimal \
  && rustup show \
  && rustup component add rustfmt \
  && rustup target add wasm32-wasip1

RUN cargo install flamegraph
RUN cargo install addr2line --features bin --bin addr2line

# TODO: cargo-chef doesn't work well now, because we update Cargo.lock very often.
# We may consider sccache instead.

# RUN cargo install --git https://github.com/xxchan/cargo-chef cargo-chef --locked --rev 11f9fed

# FROM rust-base AS rust-planner

# RUN mkdir -p /risingwave
# WORKDIR /risingwave
# COPY ./ /risingwave

# RUN cargo chef prepare --recipe-path recipe.json

# FROM rust-base AS rust-builder

# RUN mkdir -p /risingwave
# WORKDIR /risingwave

# COPY --from=rust-planner /risingwave/recipe.json recipe.json

# # Build dependencies - this can be cached if the dependencies don't change
# RUN cargo chef cook --profile production --recipe-path recipe.json

FROM rust-base AS rust-builder

# Build application
ARG GIT_SHA
ENV GIT_SHA=$GIT_SHA

ARG CARGO_PROFILE
ENV CARGO_PROFILE=$CARGO_PROFILE

COPY ./ /risingwave
WORKDIR /risingwave

ENV ENABLE_BUILD_DASHBOARD=1
ENV OPENSSL_STATIC=1

RUN cargo fetch && \
  cargo build -p risingwave_cmd_all --profile ${CARGO_PROFILE} --features "rw-static-link" --features udf --features datafusion && \
  mkdir -p /risingwave/bin && \
  mv /risingwave/target/${CARGO_PROFILE}/risingwave /risingwave/bin/ && \
  mv /risingwave/target/${CARGO_PROFILE}/risingwave.dwp /risingwave/bin/ && \
  cp ./target/${CARGO_PROFILE}/build/tikv-jemalloc-sys-*/out/build/bin/jeprof /risingwave/bin/ && \
  chmod +x /risingwave/bin/jeprof && \
  mkdir -p /risingwave/lib/adbc && \
  # Download ADBC Snowflake driver shared library for runtime (avoid invoking risedev/cargo-make here).
  DEST_DIR="/risingwave/lib/adbc" TMP_DIR="/tmp" bash ./src/risedevtool/scripts/download_adbc_snowflake_driver.sh && \
  cargo clean

FROM base AS java-planner

RUN mkdir -p /risingwave
WORKDIR /risingwave

COPY java /risingwave/java/

# Move java/**/pom.xml to poms/**/pom.xml
RUN find . -name pom.xml -exec bash -c 'mkdir -p poms/$(dirname {}); mv {} poms/{}' \;

# We use rust-maven-plugin to build java-binding. So it's FROM rust-base
FROM rust-base AS java-builder

RUN apt-get update && apt-get -y install maven

RUN mkdir -p /risingwave
WORKDIR /risingwave/java

# 1. copy only poms
COPY --from=java-planner /risingwave/poms /risingwave/java/

# 2. start downloading dependencies
RUN mvn dependency:go-offline --fail-never

# 3. add all source code and start compiling
# TODO: only add java related code so that changing rust code won't recompile java code
# Currently java-binding depends on the workspace Cargo.toml, which depends on the whole rust codebase
# Besides, rust-maven-plugin sets --target-dir, so the dependencies are built twice. How to dedup?
COPY ./ /risingwave

RUN mvn -B package -Dmaven.test.skip=true -Dno-build-rust && \
  mkdir -p /risingwave/bin/connector-node && \
  tar -zxvf /risingwave/java/connector-node/assembly/target/risingwave-connector-1.0.0.tar.gz -C /risingwave/bin/connector-node

FROM base AS risingwave

LABEL org.opencontainers.image.source https://github.com/risingwavelabs/risingwave

RUN apt-get update && apt-get -y install linux-tools-generic \
  && ln -s "$(find /usr/lib/linux-tools/*/perf | head -1)" /usr/local/bin/perf

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install gdb libpam-krb5 krb5-user telnet kafkacat less rustup \
  && rm -rf /var/lib/{apt,dpkg,cache,log}/

# Set default Rust toolchain but don't install it to keep the image size small
# The toolchain will be installed when it is first used
# Do not use `rustup default stable` because it will install immediately
RUN rustup show && sed -i '1s/^/default_toolchain = "stable"\n/' ~/.rustup/settings.toml

RUN mkdir -p /risingwave/bin/connector-node && mkdir -p /risingwave/lib

COPY --from=rust-builder /risingwave/bin/risingwave /risingwave/bin/risingwave
COPY --from=rust-builder /risingwave/bin/risingwave.dwp /risingwave/bin/risingwave.dwp
COPY --from=java-builder /risingwave/bin/connector-node /risingwave/bin/connector-node
COPY --from=rust-builder /risingwave/bin/jeprof /usr/local/bin/jeprof
COPY --from=rust-base /root/.cargo/bin/flamegraph /usr/local/bin/flamegraph
COPY --from=rust-base /root/.cargo/bin/addr2line /usr/local/bin/addr2line

COPY --from=rust-builder /risingwave/lib/adbc /risingwave/lib/adbc

# Set default playground mode to docker-playground profile
ENV PLAYGROUND_PROFILE docker-playground
# Set default connector libs path
ENV CONNECTOR_LIBS_PATH /risingwave/bin/connector-node/libs
ENV IN_CONTAINER=1

# Runtime dynamic library search path for ADBC (Linux containers use LD_LIBRARY_PATH).
ENV ADBC_DRIVER_PATH /risingwave/lib/adbc
ENV LD_LIBRARY_PATH /risingwave/lib/adbc:${LD_LIBRARY_PATH}

ENTRYPOINT [ "/risingwave/bin/risingwave" ]
CMD [ "single_node" ]
