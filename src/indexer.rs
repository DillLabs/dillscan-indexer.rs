use std::{thread, time::Duration};

use anyhow::Result;
use backoff::future::retry_notify;
use clap::Parser;
use tracing::{error, warn};

use crate::{
    args::Args,
    context::{Config as ContextConfig, Context},
    env::Environment,
    synchronizer::{config::ConfigBuilder as SynchronizerConfigBuilder, Synchronizer},
    utils::exp_backoff::get_exp_backoff_config,
};

pub fn print_banner(args: &Args, env: &Environment) {
    println!("____  _       _                         ");
    println!("| __ )| | ___ | |__  ___  ___ __ _ _ __  ");
    println!("|  _ \\| |/ _ \\| '_ \\/ __|/ __/ _` | '_ \\ ");
    println!("| |_) | | (_) | |_) \\__ \\ (_| (_| | | | |");
    println!("|____/|_|\\___/|_.__/|___/\\___\\__,_|_| |_|\n");
    println!("Blobscan indexer (EIP-4844 blob indexer) - blobscan.com");
    println!("=======================================================");

    if let Some(num_threads) = args.num_threads {
        println!("Number of threads: {}", num_threads);
    } else {
        println!("Number of threads: auto");
    }

    if let Some(slots_per_save) = args.slots_per_save {
        println!("Slot chunk size: {}", slots_per_save);
    } else {
        println!("Slot chunk size: auto");
    }

    println!("Blobscan API endpoint: {}", env.blobscan_api_endpoint);
    println!("CL endpoint: {}", env.beacon_node_endpoint);
    println!("EL endpoint: {}", env.execution_node_endpoint);

    if let Some(sentry_dsn) = env.sentry_dsn.clone() {
        println!("Sentry DSN: {}", sentry_dsn);
    }

    println!("\n");
}

pub async fn run(env: Environment) -> Result<()> {
    let args = Args::parse();

    let mut synchronizer_config_builder = SynchronizerConfigBuilder::new()?;

    if let Some(num_threads) = args.num_threads {
        synchronizer_config_builder.with_num_threads(num_threads);
    }

    if let Some(slots_checkpoint) = args.slots_per_save {
        synchronizer_config_builder.with_slots_checkpoint(slots_checkpoint);
    }

    print_banner(&args, &env);

    let context = match Context::try_new(ContextConfig::from(env)) {
        Ok(c) => c,
        Err(error) => {
            error!(target = "indexer", ?error, "Failed to create context");

            return Err(error);
        }
    };

    let beacon_client = context.beacon_client();
    let blobscan_client = context.blobscan_client();

    let mut current_slot = match args.from_slot {
        Some(from_slot) => from_slot,
        None => match blobscan_client.get_slot().await {
            Err(error) => {
                error!(target = "indexer", ?error, "Failed to fetch latest slot");

                return Err(error.into());
            }
            Ok(res) => match res {
                Some(latest_slot) => latest_slot + 1,
                None => 0,
            },
        },
    };

    let synchronizer = Synchronizer::new(context.clone(), synchronizer_config_builder.build());

    loop {
        let beacon_head_result = match retry_notify(
            get_exp_backoff_config(),
            || async move {
                beacon_client
                    .get_block(None)
                    .await
                    .map_err(|err| err.into())
            },
            |_, duration: Duration| {
                let duration = duration.as_secs();
                warn!(
                    target = "indexer",
                    "Failed to fetch beacon head block. Retrying in {duration} seconds…"
                );
            },
        )
        .await
        {
            Ok(res) => res,
            Err(error) => {
                error!(
                    target = "indexer",
                    ?error,
                    "Failed to fetch beacon head block"
                );

                return Err(error.into());
            }
        };

        if let Some(beacon_head_block) = beacon_head_result {
            let head_slot: u32 = beacon_head_block.slot.parse()?;

            synchronizer.run(current_slot, head_slot).await?;

            current_slot = head_slot;
        }

        thread::sleep(Duration::from_secs(10));
    }
}
