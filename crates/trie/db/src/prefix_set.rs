use alloy_primitives::{
    map::{HashMap, HashSet},
    BlockNumber, B256,
};
use core::{
    marker::PhantomData,
    ops::{Deref, RangeInclusive},
};
use rayon::prelude::*;
use reth_db_api::{
    cursor::DbCursorRO,
    models::{AccountBeforeTx, BlockNumberAddress},
    tables,
    transaction::DbTx,
    DatabaseError,
};
use reth_primitives_traits::StorageEntry;
use reth_trie::{
    prefix_set::{PrefixSetMut, TriePrefixSets},
    KeyHasher, Nibbles,
};

/// A wrapper around a database transaction that loads prefix sets within a given block range.
#[derive(Debug)]
pub struct PrefixSetLoader<'a, TX, KH>(&'a TX, PhantomData<KH>);

impl<'a, TX, KH> PrefixSetLoader<'a, TX, KH> {
    /// Create a new loader.
    pub const fn new(tx: &'a TX) -> Self {
        Self(tx, PhantomData)
    }
}

impl<TX, KH> Deref for PrefixSetLoader<'_, TX, KH> {
    type Target = TX;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<TX: DbTx, KH: KeyHasher> PrefixSetLoader<'_, TX, KH> {
    /// Load all account and storage changes for the given block range.
    pub fn load(self, range: RangeInclusive<BlockNumber>) -> Result<TriePrefixSets, DatabaseError> {
        let mut account_prefix_set = PrefixSetMut::default();
        let mut storage_prefix_sets = HashMap::<B256, PrefixSetMut>::default();
        let mut destroyed_accounts = HashSet::default();

        // 收集所有需要处理的地址和存储键
        let mut addresses = Vec::new();
        let mut storage_entries = Vec::new();

        // 收集账户变更数据
        let mut account_changeset_cursor = self.cursor_read::<tables::AccountChangeSets>()?;
        let mut account_hashed_state_cursor = self.cursor_read::<tables::HashedAccounts>()?;
        for account_entry in account_changeset_cursor.walk_range(range.clone())? {
            let (_, AccountBeforeTx { address, .. }) = account_entry?;
            addresses.push(address);
        }

        // 并行计算地址哈希
        let hashed_addresses: Vec<_> = addresses
            .par_iter()
            .map(|&address| (address, KH::hash_key(address)))
            .collect();

        // 处理地址哈希结果
        for (_, hashed_address) in &hashed_addresses {
            account_prefix_set.insert(Nibbles::unpack(*hashed_address));
            if account_hashed_state_cursor.seek_exact(*hashed_address)?.is_none() {
                destroyed_accounts.insert(*hashed_address);
            }
        }

        // 收集存储变更数据
        let mut storage_cursor = self.cursor_dup_read::<tables::StorageChangeSets>()?;
        let storage_range = BlockNumberAddress::range(range);
        for storage_entry in storage_cursor.walk_range(storage_range)? {
            let (BlockNumberAddress((_, address)), StorageEntry { key, .. }) = storage_entry?;
            storage_entries.push((address, key));
        }

        // 并行计算存储键哈希
        let storage_hashes: Vec<_> = storage_entries
            .par_iter()
            .map(|&(address, key)| {
                let hashed_address = KH::hash_key(address);
                let hashed_key = KH::hash_key(key);
                (hashed_address, hashed_key)
            })
            .collect();

        // 处理存储哈希结果
        for (hashed_address, hashed_key) in storage_hashes {
            account_prefix_set.insert(Nibbles::unpack(hashed_address));
            storage_prefix_sets
                .entry(hashed_address)
                .or_default()
                .insert(Nibbles::unpack(hashed_key));
        }

        Ok(TriePrefixSets {
            account_prefix_set: account_prefix_set.freeze(),
            storage_prefix_sets: storage_prefix_sets
                .into_iter()
                .map(|(k, v)| (k, v.freeze()))
                .collect(),
            destroyed_accounts,
        })
    }
}