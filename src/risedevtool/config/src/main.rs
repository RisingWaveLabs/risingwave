// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::{BufRead, BufReader, BufWriter, Write};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use console::style;
use dialoguer::MultiSelect;
use enum_iterator::{Sequence, all};
use fs_err::OpenOptions;
use itertools::Itertools;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
#[clap(infer_subcommands = true)]
pub struct RiseDevConfigOpts {
    #[clap(subcommand)]
    command: Option<Commands>,
    #[clap(short, long)]
    file: String,
}

#[derive(Subcommand)]
#[clap(infer_subcommands = true)]
enum Commands {
    /// Enable one component
    Enable {
        /// Component to enable
        #[clap(value_enum)]
        component: Components,
    },
    /// Disable one component
    Disable {
        /// Component to disable
        #[clap(value_enum)]
        component: Components,
    },
    /// Use default configuration
    Default,
}

#[expect(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug, Sequence, PartialEq, Eq, ValueEnum)]
pub enum Components {
    #[clap(name = "minio")]
    Minio,
    Lakekeeper,
    Hdfs,
    PrometheusAndGrafana,
    Pubsub,
    Tracing,
    RustComponents,
    UseSystem,
    BuildConnectorNode,
    Dashboard,
    Release,
    Sanitizer,
    DynamicLinking,
    HummockTrace,
    Coredump,
    NoBacktrace,
    Udf,
    NoDefaultFeatures,
    Moat,
    DataFusion,
    Adbc,
}

impl Components {
    pub fn title(&self) -> String {
        match self {
            Self::Minio => "[Component] Hummock: MinIO + MinIO-CLI",
            Self::Lakekeeper => "[Component] Apache Iceberg: Lakekeeper REST Catalog",
            Self::Hdfs => "[Component] Hummock: Hdfs Backend",
            Self::PrometheusAndGrafana => "[Component] Metrics: Prometheus + Grafana",
            Self::Pubsub => "[Component] Google Pubsub",
            Self::BuildConnectorNode => "[Build] Build RisingWave Connector (Java)",
            Self::RustComponents => "[Build] Rust components",
            Self::UseSystem => "[Build] Use system RisingWave",
            Self::Dashboard => "[Build] Dashboard",
            Self::Tracing => "[Component] Tracing: Grafana Tempo",
            Self::Release => "[Build] Enable release mode",
            Self::Sanitizer => "[Build] Enable sanitizer",
            Self::DynamicLinking => "[Build] Enable dynamic linking",
            Self::HummockTrace => "[Build] Hummock Trace",
            Self::Coredump => "[Runtime] Enable coredump",
            Self::NoBacktrace => "[Runtime] Disable backtrace",
            Self::Udf => "[Build] Enable UDF",
            Self::NoDefaultFeatures => "[Build] Disable default features",
            Self::Moat => "[Component] Enable Moat",
            Self::DataFusion => "[Build] Enable DataFusion",
            Self::Adbc => "[Component] ADBC Snowflake Driver",
        }
        .into()
    }

    pub fn description(&self) -> String {
        match self {
            Self::Minio => {
                "
Required by Hummock state store."
            }
            Self::Lakekeeper => {
                "
Required if you want to use Apache Iceberg REST Catalog.
Provides catalog and metadata management for Apache Iceberg tables."
            }
            Self::Hdfs => {
                "
Required by Hummock state store."
            }
            Self::PrometheusAndGrafana => {
                "
Required if you want to view metrics."
            }
            Self::Pubsub => {
                "
Required if you want to create source from Emulated Google Pub/sub.
                "
            }
            Self::RustComponents => {
                "
Required if you want to build compute-node and meta-node.
Otherwise you will need to enable `USE_SYSTEM_RISINGWAVE`, or
manually download a binary and copy it to RiseDev directory."
            }
            Self::UseSystem => {
                "
Use the RisingWave installed in the PATH, instead of building it
from source. This implies `ENABLE_BUILD_RUST` to be false.
                "
            }
            Self::Dashboard => {
                "
Required if you want to build dashboard from source.
This is generally not the option you want to use to develop the
dashboard. Instead, directly run `pnpm run dev` in the dashboard
directory to start the development server, set the API endpoint
to a running RisingWave cluster in the settings page.
"
            }
            Self::Tracing => {
                "
Required if you want to use tracing. This option will help
you download Grafana Tempo."
            }
            Self::Release => {
                "
Build RisingWave in release mode"
            }
            Self::Sanitizer => {
                "
With this option enabled, RiseDev will build Rust components
with thread sanitizer. The built binaries will be at
`target/<arch-triple>/(debug|release)` instead of simply at
`target/debug`. RiseDev will help link binaries when starting
a dev cluster.
"
            }
            Self::BuildConnectorNode => {
                "
Required if you want to build Connector Node from source locally.
                "
            }
            Self::DynamicLinking => {
                "
With this option enabled, RiseDev will use dynamic linking when
building Rust components. This can speed up the build process,
but you might need the expertise to install dependencies correctly.
                "
            }
            Self::HummockTrace => {
                "
With this option enabled, RiseDev will enable tracing for Hummock.
See storage/hummock_trace for details.
                "
            }
            Self::Coredump => {
                "
With this option enabled, RiseDev will unlimit the size of core
files before launching RisingWave. On Apple Silicon platforms,
the binaries will also be codesigned with `get-task-allow` enabled.
As a result, RisingWave will dump the core on panics.
                "
            }
            Self::NoBacktrace => {
                "
With this option enabled, RiseDev will not set `RUST_BACKTRACE` when launching nodes.
                "
            }
            Self::Udf => {
                "
Add --features udf to build command (by default disabled).
Required if you want to support UDF."
            }
            Self::NoDefaultFeatures => {
                "
Add --no-default-features to build command.
Currently, default features are: rw-static-link, all-connectors
"
            }
            Self::Moat => {
                "
Enable Moat as distributed hybrid cache service."
            }
            Self::DataFusion => {
                "
Enable DataFusion as the optional query engine for Iceberg tables."
            }
            Self::Adbc => {
                "
Enable ADBC (Arrow Database Connectivity) Snowflake driver support.
Required if you want to use ADBC Snowflake source.
This will download the ADBC Snowflake driver shared library (.so/.dylib)."
            }
        }
        .into()
    }

    pub fn from_env(env: impl AsRef<str>) -> Option<Self> {
        match env.as_ref() {
            "ENABLE_MINIO" => Some(Self::Minio),
            "ENABLE_LAKEKEEPER" => Some(Self::Lakekeeper),
            "ENABLE_HDFS" => Some(Self::Hdfs),
            "ENABLE_PROMETHEUS_GRAFANA" => Some(Self::PrometheusAndGrafana),
            "ENABLE_PUBSUB" => Some(Self::Pubsub),
            "ENABLE_BUILD_RUST" => Some(Self::RustComponents),
            "USE_SYSTEM_RISINGWAVE" => Some(Self::UseSystem),
            "ENABLE_BUILD_DASHBOARD" => Some(Self::Dashboard),
            "ENABLE_COMPUTE_TRACING" => Some(Self::Tracing),
            "ENABLE_RELEASE_PROFILE" => Some(Self::Release),
            "ENABLE_DYNAMIC_LINKING" => Some(Self::DynamicLinking),
            "ENABLE_SANITIZER" => Some(Self::Sanitizer),
            "ENABLE_BUILD_RW_CONNECTOR" => Some(Self::BuildConnectorNode),
            "ENABLE_HUMMOCK_TRACE" => Some(Self::HummockTrace),
            "ENABLE_COREDUMP" => Some(Self::Coredump),
            "DISABLE_BACKTRACE" => Some(Self::NoBacktrace),
            "ENABLE_UDF" => Some(Self::Udf),
            "DISABLE_DEFAULT_FEATURES" => Some(Self::NoDefaultFeatures),
            "ENABLE_MOAT" => Some(Self::Moat),
            "ENABLE_DATAFUSION" => Some(Self::DataFusion),
            "ENABLE_ADBC" => Some(Self::Adbc),
            _ => None,
        }
    }

    pub fn env(&self) -> String {
        match self {
            Self::Minio => "ENABLE_MINIO",
            Self::Lakekeeper => "ENABLE_LAKEKEEPER",
            Self::Hdfs => "ENABLE_HDFS",
            Self::PrometheusAndGrafana => "ENABLE_PROMETHEUS_GRAFANA",
            Self::Pubsub => "ENABLE_PUBSUB",
            Self::RustComponents => "ENABLE_BUILD_RUST",
            Self::UseSystem => "USE_SYSTEM_RISINGWAVE",
            Self::Dashboard => "ENABLE_BUILD_DASHBOARD",
            Self::Tracing => "ENABLE_COMPUTE_TRACING",
            Self::Release => "ENABLE_RELEASE_PROFILE",
            Self::Sanitizer => "ENABLE_SANITIZER",
            Self::BuildConnectorNode => "ENABLE_BUILD_RW_CONNECTOR",
            Self::DynamicLinking => "ENABLE_DYNAMIC_LINKING",
            Self::HummockTrace => "ENABLE_HUMMOCK_TRACE",
            Self::Coredump => "ENABLE_COREDUMP",
            Self::NoBacktrace => "DISABLE_BACKTRACE",
            Self::Udf => "ENABLE_UDF",
            Self::NoDefaultFeatures => "DISABLE_DEFAULT_FEATURES",
            Self::Moat => "ENABLE_MOAT",
            Self::DataFusion => "ENABLE_DATAFUSION",
            Self::Adbc => "ENABLE_ADBC",
        }
        .into()
    }

    pub fn default_enabled() -> &'static [Self] {
        &[Self::RustComponents]
    }
}

fn configure(chosen: &[Components]) -> Result<Option<Vec<Components>>> {
    println!("=== Configure RiseDev ===");

    let all_components = all::<Components>().collect_vec();

    const ITEMS_PER_PAGE: usize = 6;

    let items = all_components
        .iter()
        .map(|c| {
            let title = c.title();
            let desc = style(
                ("\n".to_owned() + c.description().trim())
                    .split('\n')
                    .join("\n      "),
            )
            .dim();

            (format!("{title}{desc}",), chosen.contains(c))
        })
        .collect_vec();

    let Some(chosen_indices) = MultiSelect::new()
        .with_prompt(
            format!(
                "RiseDev includes several components. You can select the ones you need, so as to reduce build time\n\n{}: navigate\n{}: confirm and save   {}: quit without saving\n\nPick items with {}",
                style("↑ / ↓ / ← / → ").reverse(),
                style("Enter").reverse(),
                style("Esc / q").reverse(),
                style("Space").reverse(),
            )
        )
        .items_checked(items)
        .max_length(ITEMS_PER_PAGE)
        .interact_opt()? else {
        return Ok(None);
    };

    let chosen = chosen_indices
        .into_iter()
        .map(|i| all_components[i])
        .collect_vec();

    Ok(Some(chosen))
}

fn main() -> Result<()> {
    let opts = RiseDevConfigOpts::parse();
    let file_path = opts.file;

    let chosen = {
        match OpenOptions::new().read(true).open(&file_path) {
            Ok(file) => {
                let reader = BufReader::new(file);
                let mut enabled = vec![];
                for line in reader.lines() {
                    let line = line?;
                    if line.trim().is_empty() || line.trim().starts_with('#') {
                        continue;
                    }
                    let Some((component, val)) = line.split_once('=') else {
                        println!("invalid config line {}, discarded", line);
                        continue;
                    };
                    if component == "RISEDEV_CONFIGURED" {
                        continue;
                    }
                    match Components::from_env(component) {
                        Some(component) => {
                            if val == "true" {
                                enabled.push(component);
                            }
                        }
                        None => {
                            println!("unknown configure {}, discarded", component);
                            continue;
                        }
                    }
                }
                enabled
            }
            _ => {
                println!(
                    "RiseDev component config not found, generating {}",
                    file_path
                );
                Components::default_enabled().to_vec()
            }
        }
    };

    let chosen = match &opts.command {
        Some(Commands::Default) => {
            println!("Using default config");
            Components::default_enabled().to_vec()
        }
        Some(Commands::Enable { component }) => {
            let mut chosen = chosen;
            chosen.push(*component);
            chosen
        }
        Some(Commands::Disable { component }) => {
            chosen.into_iter().filter(|x| x != component).collect()
        }
        None => match configure(&chosen)? {
            Some(chosen) => chosen,
            None => {
                println!("Quit without saving");
                println!("=========================");
                return Ok(());
            }
        },
    };

    println!("=== Enabled Components ===");
    for component in all::<Components>() {
        println!(
            "{}: {}",
            component.title(),
            if chosen.contains(&component) {
                style("enabled").green()
            } else {
                style("disabled").dim()
            }
        );
    }

    println!("Configuration saved at {}", file_path);
    println!("=========================");

    let mut file = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&file_path)
            .context(format!("failed to open component config at {}", file_path))?,
    );

    writeln!(file, "RISEDEV_CONFIGURED=true")?;
    writeln!(file)?;

    for component in all::<Components>() {
        writeln!(file, "# {}", component.title())?;
        writeln!(
            file,
            "# {}",
            component.description().trim().split('\n').join("\n# ")
        )?;
        if chosen.contains(&component) {
            writeln!(file, "{}=true", component.env())?;
        } else {
            writeln!(file, "# {}=true", component.env())?;
        }
        writeln!(file)?;
    }

    file.flush()?;

    println!(
        "RiseDev will {} the components you've enabled.",
        style("only download").bold()
    );
    println!(
        "If you want to use these components, please {} in {} to start that component.",
        style("modify the cluster config").yellow().bold(),
        style("risedev.yml").bold(),
    );
    println!("See CONTRIBUTING.md or RiseDev's readme for more information.");

    Ok(())
}
