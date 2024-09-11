// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    counters::{MAX_TXNS_FROM_BLOCK_TO_EXECUTE, TXN_SHUFFLE_SECONDS},
    monitor,
    payload_manager::TPayloadManager,
    transaction_deduper::TransactionDeduper,
    transaction_filter::TransactionFilter,
    transaction_shuffler::TransactionShuffler,
};
use aptos_consensus_types::{block::Block, pipelined_block::OrderedBlockWindow};
use aptos_executor_types::ExecutorResult;
use aptos_logger::info;
use aptos_types::transaction::SignedTransaction;
use fail::fail_point;
use futures::{stream::FuturesOrdered, StreamExt};
use std::{cmp::Reverse, collections::HashSet, sync::Arc};

pub struct BlockPreparer {
    payload_manager: Arc<dyn TPayloadManager>,
    txn_filter: Arc<TransactionFilter>,
    txn_deduper: Arc<dyn TransactionDeduper>,
    txn_shuffler: Arc<dyn TransactionShuffler>,
}

impl BlockPreparer {
    pub fn new(
        payload_manager: Arc<dyn TPayloadManager>,
        txn_filter: Arc<TransactionFilter>,
        txn_deduper: Arc<dyn TransactionDeduper>,
        txn_shuffler: Arc<dyn TransactionShuffler>,
    ) -> Self {
        Self {
            payload_manager,
            txn_filter,
            txn_deduper,
            txn_shuffler,
        }
    }

    async fn get_transactions(
        &self,
        block: &Block,
        block_window: &OrderedBlockWindow,
    ) -> ExecutorResult<(Vec<(Vec<SignedTransaction>, u64)>, Option<u64>)> {
        let mut txns = vec![];
        let mut futures = FuturesOrdered::new();
        for block in block_window
            .pipelined_blocks()
            .iter()
            .map(|b| b.block())
            .chain(std::iter::once(block))
        {
            futures.push_back(async move { self.payload_manager.get_transactions(block).await });
        }
        let mut max_txns_from_block_to_execute = None;
        loop {
            match futures.next().await {
                // TODO: we are turning off the max txns from block to execute feature for now
                Some(Ok((block_txns, _max_txns))) => {
                    txns.extend(block_txns);
                    max_txns_from_block_to_execute = None;
                },
                Some(Err(e)) => {
                    return Err(e);
                },
                None => break,
            }
        }
        Ok((txns, max_txns_from_block_to_execute))
    }

    pub async fn prepare_block(
        &self,
        block: &Block,
        block_window: &OrderedBlockWindow,
    ) -> ExecutorResult<Vec<SignedTransaction>> {
        fail_point!("consensus::prepare_block", |_| {
            use aptos_executor_types::ExecutorError;
            use std::{thread, time::Duration};
            thread::sleep(Duration::from_millis(10));
            Err(ExecutorError::CouldNotGetData)
        });
        info!(
            "BlockPreparer: Preparing for block ({}, {}) {} and window {:?}",
            block.epoch(),
            block.round(),
            block.id(),
            block_window
                .blocks()
                .iter()
                .map(|b| b.id())
                .collect::<Vec<_>>()
        );

        let now = std::time::Instant::now();
        // TODO: we could do this incrementally, but for now just do it every time
        let mut committed_transactions = HashSet::new();

        // TODO: lots of repeated code here
        block_window
            .pipelined_blocks()
            .iter()
            .filter(|window_block| window_block.round() < block.round() - 1)
            .for_each(|b| {
                info!(
                    "BlockPreparer: Waiting for committed transactions at block {} for block {}",
                    b.round(),
                    block.round()
                );
                for txn_hash in b.wait_for_committed_transactions() {
                    committed_transactions.insert(*txn_hash);
                }
            });
        info!(
            "BlockPreparer: Waiting for part of committed transactions took {:?}",
            now.elapsed()
        );
        // TODO: this blocks the pipeline, so removed for now, to be revived with a streaming TransactionProvider based BlockSTM
        // block_window
        //     .pipelined_blocks()
        //     .iter()
        //     .filter(|window_block| window_block.round() == block.round() - 1)
        //     .for_each(|b| {
        //         // TODO: this wait means there is no pipeline with execution
        //         for txn_hash in b.wait_for_committed_transactions() {
        //             committed_transactions.insert(*txn_hash);
        //         }
        //     });
        // info!(
        //     "BlockPreparer: Waiting for all committed transactions took {:?}",
        //     now.elapsed()
        // );

        let (mut batched_txns, max_txns_from_block_to_execute) = monitor!("get_transactions", {
            self.get_transactions(block, block_window).await?
        });

        let txn_filter = self.txn_filter.clone();
        let txn_deduper = self.txn_deduper.clone();
        let txn_shuffler = self.txn_shuffler.clone();
        let block_id = block.id();
        let block_timestamp_usecs = block.timestamp_usecs();
        // Transaction filtering, deduplication and shuffling are CPU intensive tasks, so we run them in a blocking task.
        tokio::task::spawn_blocking(move || {
            // stable sort to ensure batches with same gas are in the same order
            batched_txns.sort_by_key(|(_, gas)| Reverse(*gas));
            info!("BlockPreparer: Batched transactions:");
            for (txns, gas_bucket_start) in &batched_txns {
                info!(
                    "  BlockPreparer: Batched transactions: gas: {}, txns: {}",
                    gas_bucket_start,
                    txns.len()
                );
            }

            let txns: Vec<_> = batched_txns
                .into_iter()
                .flat_map(|(txns, _)| txns.into_iter())
                .collect();
            let txns_len = txns.len();
            let filtered_txns = txns
                .into_iter()
                .filter(|txn| !committed_transactions.contains(&txn.committed_hash()))
                .collect::<Vec<_>>();
            info!(
                "BlockPreparer: Filtered {}/{} committed transactions",
                txns_len - filtered_txns.len(),
                txns_len
            );
            let filtered_txns = txn_filter.filter(block_id, block_timestamp_usecs, filtered_txns);
            let deduped_txns = txn_deduper.dedup(filtered_txns);
            let mut shuffled_txns = {
                let _timer = TXN_SHUFFLE_SECONDS.start_timer();

                txn_shuffler.shuffle(deduped_txns)
            };

            if let Some(max_txns_from_block_to_execute) = max_txns_from_block_to_execute {
                shuffled_txns.truncate(max_txns_from_block_to_execute as usize);
            }
            MAX_TXNS_FROM_BLOCK_TO_EXECUTE.observe(shuffled_txns.len() as f64);
            Ok(shuffled_txns)
        })
        .await
        .expect("Failed to spawn blocking task for transaction generation")
    }
}
