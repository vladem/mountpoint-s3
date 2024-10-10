use crate::object::ObjectId;

use super::{BlockIndex, ChecksummedBytes, DataCache, DataCacheError, DataCacheResult};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::{Bytes, BytesMut};
use futures::{pin_mut, StreamExt};
use mountpoint_s3_client::error::{GetObjectError, ObjectClientError};
use mountpoint_s3_client::types::ETag;
use mountpoint_s3_client::types::{GetObjectRequest, PutObjectParams};
use mountpoint_s3_client::ObjectClient;
use mountpoint_s3_crt::checksums::crc32c::{self, Crc32c};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::str::FromStr;
use thiserror::Error;
use tracing::{warn, Instrument};

const CACHE_VERSION: &str = "V1";

/// A data cache on S3 Express One Zone that can be shared across Mountpoint instances.
pub struct ExpressDataCache<Client: ObjectClient> {
    client: Client,
    bucket_name: String,
    prefix: String,
    block_size: u64,
    source_description: String,
}

impl<S, C> From<ObjectClientError<S, C>> for DataCacheError
where
    S: std::error::Error + Send + Sync + 'static,
    C: std::error::Error + Send + Sync + 'static,
{
    fn from(e: ObjectClientError<S, C>) -> Self {
        DataCacheError::IoFailure(e.into())
    }
}

#[derive(Debug, Error)]
enum BlockAccessError {
    #[error("one or more of the fields in this block were incorrect {0}")]
    FieldMismatchError(String),
}

struct BlockHeader {
    version: String,
    block_idx: BlockIndex,
    block_offset: u64,
    cache_key: ObjectId,
    source_description: String,
    data_checksum: u32,
    header_checksum: u32,
}

impl BlockHeader {
    pub fn new(
        block_idx: BlockIndex,
        block_offset: u64,
        cache_key: &ObjectId,
        source_description: &str,
        data_checksum: Crc32c,
    ) -> Self {
        let data_checksum = data_checksum.value();
        let header_checksum =
            Self::compute_checksum(block_idx, block_offset, cache_key, data_checksum, source_description).value();
        Self {
            version: CACHE_VERSION.to_string(),
            block_idx,
            block_offset,
            cache_key: cache_key.clone(),
            source_description: source_description.to_string(),
            data_checksum,
            header_checksum,
        }
    }

    fn compute_checksum(
        block_idx: BlockIndex,
        block_offset: u64,
        cache_key: &ObjectId,
        data_checksum: u32,
        source_description: &str,
    ) -> Crc32c {
        let mut hasher = crc32c::Hasher::new();
        hasher.update(CACHE_VERSION.as_bytes());
        hasher.update(&block_idx.to_be_bytes());
        hasher.update(&block_offset.to_be_bytes());
        hasher.update(cache_key.etag().as_str().as_bytes());
        hasher.update(cache_key.key().as_bytes());
        hasher.update(source_description.as_bytes());
        hasher.update(&data_checksum.to_be_bytes());
        hasher.finalize()
    }

    /// Validate the integrity of the contained data and return the stored data checksum.
    ///
    /// Execute this method before acting on the data contained within.
    pub fn validate(
        &self,
        cache_key: &ObjectId,
        block_idx: BlockIndex,
        block_offset: u64,
        source_description: &str,
    ) -> Result<Crc32c, DataCacheError> {
        let cache_key_match = cache_key == &self.cache_key;
        let block_idx_match = block_idx == self.block_idx;
        let block_offset_match = block_offset == self.block_offset;
        let source_description_match = source_description == &self.source_description;

        let data_checksum = self.data_checksum;
        if cache_key_match && block_idx_match && block_offset_match && source_description_match {
            if Self::compute_checksum(block_idx, block_offset, cache_key, data_checksum, source_description).value()
                != self.header_checksum
            {
                Err(DataCacheError::InvalidBlockContent)
            } else {
                Ok(Crc32c::new(data_checksum))
            }
        } else {
            warn!(
                cache_key_match,
                block_idx_match,
                block_offset_match,
                source_description_match,
                "block data did not match expected values",
            );
            Err(DataCacheError::InvalidBlockContent)
        }
    }

    fn to_headers(&self) -> HashMap<String, String> {
        let encoded_key = STANDARD.encode(self.cache_key.key());
        let encoded_bucket = STANDARD.encode(&self.source_description);
        let headers = HashMap::from([
            ("x-amz-meta-version".to_string(), self.version.clone()),
            ("x-amz-meta-block-idx".to_string(), format!("{}", self.block_idx)),
            ("x-amz-meta-block-offset".to_string(), format!("{}", self.block_offset)),
            (
                "x-amz-meta-etag".to_string(),
                self.cache_key.etag().as_str().to_string(),
            ),
            ("x-amz-meta-s3-bucket-name".to_string(), encoded_bucket),
            ("x-amz-meta-s3-key".to_string(), encoded_key),
            (
                "x-amz-meta-data-checksum".to_string(),
                format!("{}", self.data_checksum),
            ),
            (
                "x-amz-meta-header-checksum".to_string(),
                format!("{}", self.header_checksum),
            ),
        ]);
        let mut headers_size = 0;
        for (k, v) in headers.iter() {
            headers_size += k.len() + v.len();
        }
        warn!("headers of size {}: {:?}", headers_size, headers);
        headers
    }

    fn from_headers(attributes: HashMap<String, String>) -> Result<Self, BlockAccessError> {
        let version = attributes
            .get("x-amz-meta-version")
            .ok_or(BlockAccessError::FieldMismatchError("x-amz-meta-version".to_string()))?;
        if version.as_str() != CACHE_VERSION {
            warn!("invalid version");
            return Err(BlockAccessError::FieldMismatchError("x-amz-meta-version".to_string()));
        }

        let s3_key = attributes
            .get("x-amz-meta-s3-key")
            .ok_or(BlockAccessError::FieldMismatchError("x-amz-meta-s3-key".to_string()))?
            .to_string();
        let s3_key = String::from_utf8(
            STANDARD
                .decode(s3_key)
                .map_err(|_| BlockAccessError::FieldMismatchError("x-amz-meta-s3-key".to_string()))?,
        )
        .map_err(|_| BlockAccessError::FieldMismatchError("x-amz-meta-s3-key".to_string()))?;
        let source_description = attributes
            .get("x-amz-meta-s3-bucket-name")
            .ok_or(BlockAccessError::FieldMismatchError(
                "x-amz-meta-s3-bucket-name".to_string(),
            ))?
            .to_string();
        let source_description = String::from_utf8(
            STANDARD
                .decode(source_description)
                .map_err(|_| BlockAccessError::FieldMismatchError("x-amz-meta-s3-bucket-name".to_string()))?,
        )
        .map_err(|_| BlockAccessError::FieldMismatchError("x-amz-meta-s3-bucket-name".to_string()))?;

        Ok(Self {
            version: version.to_string(),
            block_idx: attributes
                .get("x-amz-meta-block-idx")
                .ok_or(BlockAccessError::FieldMismatchError("x-amz-meta-block-idx".to_string()))?
                .parse::<u64>()
                .unwrap(),
            block_offset: attributes
                .get("x-amz-meta-block-offset")
                .ok_or(BlockAccessError::FieldMismatchError(
                    "x-amz-meta-block-offset".to_string(),
                ))?
                .parse::<u64>()
                .unwrap(),
            cache_key: ObjectId::new(
                s3_key,
                ETag::from_str(
                    attributes
                        .get("x-amz-meta-etag")
                        .ok_or(BlockAccessError::FieldMismatchError("x-amz-meta-etag".to_string()))?,
                )
                .map_err(|_| BlockAccessError::FieldMismatchError("x-amz-meta-etag".to_string()))?,
            ),
            source_description,
            data_checksum: attributes
                .get("x-amz-meta-data-checksum")
                .ok_or(BlockAccessError::FieldMismatchError(
                    "x-amz-meta-data-checksum".to_string(),
                ))?
                .parse::<u32>()
                .unwrap(),
            header_checksum: attributes
                .get("x-amz-meta-header-checksum")
                .ok_or(BlockAccessError::FieldMismatchError(
                    "x-amz-meta-header-checksum".to_string(),
                ))?
                .parse::<u32>()
                .unwrap(),
        })
    }
}

impl<Client> ExpressDataCache<Client>
where
    Client: ObjectClient + Send + Sync + 'static,
{
    /// Create a new instance.
    ///
    /// TODO: consider adding some validation of the bucket.
    pub fn new(bucket_name: &str, client: Client, source_description: &str, block_size: u64) -> Self {
        let prefix = hex::encode(
            Sha256::new()
                .chain_update(CACHE_VERSION.as_bytes())
                .chain_update(block_size.to_be_bytes())
                .chain_update(source_description.as_bytes())
                .finalize(),
        );
        Self {
            client,
            bucket_name: bucket_name.to_owned(),
            prefix,
            block_size,
            source_description: source_description.to_owned(),
        }
    }

    fn validate_block(
        &self,
        data: Bytes,
        attributes: HashMap<String, String>,
        cache_key: &ObjectId,
        block_idx: BlockIndex,
        block_offset: u64,
        source_description: &str,
    ) -> Result<ChecksummedBytes, DataCacheError> {
        let block_header = BlockHeader::from_headers(attributes).map_err(|err| {
            warn!("failed to parse headers: {:?}", err);
            DataCacheError::InvalidBlockContent
        })?;
        let checksum = block_header.validate(cache_key, block_idx, block_offset, source_description)?;
        Ok(ChecksummedBytes::new_from_inner_data(data, checksum))
    }
}

#[async_trait]
impl<Client> DataCache for ExpressDataCache<Client>
where
    Client: ObjectClient + Send + Sync + 'static,
{
    async fn get_block(
        &self,
        cache_key: &ObjectId,
        block_idx: BlockIndex,
        block_offset: u64,
    ) -> DataCacheResult<Option<ChecksummedBytes>> {
        if block_offset != block_idx * self.block_size {
            return Err(DataCacheError::InvalidBlockOffset);
        }

        let object_key = block_key(&self.prefix, cache_key, block_idx);
        let result = match self.client.get_object(&self.bucket_name, &object_key, None, None).await {
            Ok(result) => result,
            Err(ObjectClientError::ServiceError(GetObjectError::NoSuchKey)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        pin_mut!(result);

        // TODO: optimize for the common case of a single chunk.
        let mut buffer = BytesMut::default();
        while let Some(chunk) = result.next().await {
            let (offset, body) = chunk?;
            if offset != buffer.len() as u64 {
                return Err(DataCacheError::InvalidBlockOffset);
            }
            buffer.extend_from_slice(&body);

            // Ensure the flow-control window is large enough.
            result.as_mut().increment_read_window(self.block_size as usize);
        }
        let data = self.validate_block(
            buffer.freeze(),
            result.as_ref().get_attributes(),
            cache_key,
            block_idx,
            block_offset,
            &self.source_description,
        )?;
        DataCacheResult::Ok(Some(data))
    }

    async fn put_block(
        &self,
        cache_key: ObjectId,
        block_idx: BlockIndex,
        block_offset: u64,
        bytes: ChecksummedBytes,
    ) -> DataCacheResult<()> {
        if block_offset != block_idx * self.block_size {
            return Err(DataCacheError::InvalidBlockOffset);
        }

        let object_key = block_key(&self.prefix, &cache_key, block_idx);

        let mut params = PutObjectParams::new();
        let (data, checksum) = bytes.into_inner().map_err(|_| DataCacheError::InvalidBlockContent)?;
        let block_header = BlockHeader::new(
            block_idx,
            block_offset,
            &cache_key,
            &self.source_description,
            checksum.clone(),
        );
        params.additional_headers = block_header.to_headers();
        let _req = self
            .client
            .put_object_single(&self.bucket_name, &object_key, &params, data)
            .in_current_span()
            .await?;

        DataCacheResult::Ok(())
    }

    fn block_size(&self) -> u64 {
        self.block_size
    }
}

fn block_key(prefix: &str, cache_key: &ObjectId, block_idx: BlockIndex) -> String {
    let hashed_cache_key = hex::encode(
        Sha256::new()
            .chain_update(cache_key.key())
            .chain_update(cache_key.etag().as_str())
            .finalize(),
    );
    format!("{}/{}/{:010}", prefix, hashed_cache_key, block_idx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checksums::ChecksummedBytes;
    use crate::sync::Arc;

    use test_case::test_case;

    use mountpoint_s3_client::mock_client::{MockClient, MockClientConfig};
    use mountpoint_s3_client::types::ETag;

    #[test_case(1024, 512 * 1024; "block_size smaller than part_size")]
    #[test_case(8 * 1024 * 1024, 512 * 1024; "block_size larger than part_size")]
    #[tokio::test]
    async fn test_put_get(part_size: usize, block_size: u64) {
        let bucket = "test-bucket";
        let config = MockClientConfig {
            bucket: bucket.to_string(),
            part_size,
            enable_backpressure: true,
            initial_read_window_size: part_size,
            ..Default::default()
        };
        let client = Arc::new(MockClient::new(config));

        let cache = ExpressDataCache::new(bucket, client, "unique source description", block_size);

        let data_1 = ChecksummedBytes::new("Foo".into());
        let data_2 = ChecksummedBytes::new("Bar".into());
        let data_3 = ChecksummedBytes::new("a".repeat(block_size as usize).into());

        let cache_key_1 = ObjectId::new("a".into(), ETag::for_tests());
        let cache_key_2 = ObjectId::new(
            "longkey_".repeat(128), // 1024 bytes, max length for S3 keys
            ETag::for_tests(),
        );

        let block = cache
            .get_block(&cache_key_1, 0, 0)
            .await
            .expect("cache should be accessible");
        assert!(
            block.is_none(),
            "no entry should be available to return but got {:?}",
            block,
        );

        // PUT and GET, OK?
        cache
            .put_block(cache_key_1.clone(), 0, 0, data_1.clone())
            .await
            .expect("cache should be accessible");
        let entry = cache
            .get_block(&cache_key_1, 0, 0)
            .await
            .expect("cache should be accessible")
            .expect("cache entry should be returned");
        assert_eq!(
            data_1, entry,
            "cache entry returned should match original bytes after put"
        );

        // PUT AND GET block for a second key, OK?
        cache
            .put_block(cache_key_2.clone(), 0, 0, data_2.clone())
            .await
            .expect("cache should be accessible");
        let entry = cache
            .get_block(&cache_key_2, 0, 0)
            .await
            .expect("cache should be accessible")
            .expect("cache entry should be returned");
        assert_eq!(
            data_2, entry,
            "cache entry returned should match original bytes after put"
        );

        // PUT AND GET a second block in a cache entry, OK?
        cache
            .put_block(cache_key_1.clone(), 1, block_size, data_3.clone())
            .await
            .expect("cache should be accessible");
        let entry = cache
            .get_block(&cache_key_1, 1, block_size)
            .await
            .expect("cache should be accessible")
            .expect("cache entry should be returned");
        assert_eq!(
            data_3, entry,
            "cache entry returned should match original bytes after put"
        );

        // Entry 1's first block still intact
        let entry = cache
            .get_block(&cache_key_1, 0, 0)
            .await
            .expect("cache should be accessible")
            .expect("cache entry should be returned");
        assert_eq!(
            data_1, entry,
            "cache entry returned should match original bytes after put"
        );
    }
}
