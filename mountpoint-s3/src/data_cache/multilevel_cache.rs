use async_trait::async_trait;
use tracing::warn;

use crate::object::ObjectId;

use super::{BlockIndex, ChecksummedBytes, DataCache, DataCacheResult};

pub struct MultilevelDataCache<FirstCache: DataCache, SecondCache: DataCache> {
    first_cache: FirstCache,
    second_cache: SecondCache,
}

impl<FirstCache: DataCache, SecondCache: DataCache> MultilevelDataCache<FirstCache, SecondCache> {
    pub fn new(first_cache: FirstCache, second_cache: SecondCache) -> Self {
        assert_eq!(first_cache.block_size(), second_cache.block_size());
        Self {
            first_cache,
            second_cache,
        }
    }
}

#[async_trait]
impl<FirstCache: DataCache + Sync, SecondCache: DataCache + Sync> DataCache
    for MultilevelDataCache<FirstCache, SecondCache>
{
    async fn get_block(
        &self,
        cache_key: &ObjectId,
        block_idx: BlockIndex,
        block_offset: u64,
    ) -> DataCacheResult<Option<ChecksummedBytes>> {
        // todo: populate cache based on data from the other one? both express->local and local->express?
        if let Some(data) = self.first_cache.get_block(cache_key, block_idx, block_offset).await? {
            warn!(cache_key=?cache_key, block_idx=block_idx, "block served from the first layer cache");
            return DataCacheResult::Ok(Some(data));
        }
        if let Some(data) = self.second_cache.get_block(cache_key, block_idx, block_offset).await? {
            warn!(cache_key=?cache_key, block_idx=block_idx, "block served from the second layer cache");
            return DataCacheResult::Ok(Some(data));
        }
        DataCacheResult::Ok(None)
    }

    async fn put_block(
        &self,
        cache_key: ObjectId,
        block_idx: BlockIndex,
        block_offset: u64,
        bytes: ChecksummedBytes,
    ) -> DataCacheResult<()> {
        self.first_cache
            .put_block(cache_key.clone(), block_idx, block_offset, bytes.clone())
            .await?;
        self.second_cache
            .put_block(cache_key, block_idx, block_offset, bytes)
            .await
    }

    fn block_size(&self) -> u64 {
        self.first_cache.block_size()
    }
}
