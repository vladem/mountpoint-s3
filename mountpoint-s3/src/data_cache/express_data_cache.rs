use crate::checksums::IntegrityError;
use crate::object::ObjectId;

use super::{BlockIndex, ChecksummedBytes, DataCache, DataCacheError, DataCacheResult};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{pin_mut, StreamExt};
use mountpoint_s3_client::error::{GetObjectError, ObjectClientError};
use mountpoint_s3_client::types::{GetObjectRequest, PutObjectParams};
use mountpoint_s3_client::{ObjectClient, PutObjectRequest};
use mountpoint_s3_crt::checksums::crc32c::{self, Crc32c};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::{Cursor, Read, Write};
use thiserror::Error;
use tracing::{warn, Instrument};

const CACHE_VERSION: &str = "V1";

/// A data cache on S3 Express One Zone that can be shared across Mountpoint instances.
pub struct ExpressDataCache<Client: ObjectClient> {
    client: Client,
    bucket_name: String,
    prefix: String,
    block_size: u64,
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

/// Error during access to a [Block]
#[derive(Debug, Error)]
enum BlockAccessError {
    #[error("checksum over the block's fields did not match the field content")]
    ChecksumError,
    #[error("one or more of the fields in this block were incorrect")]
    FieldMismatchError,
}

/// Describes additional information about the data stored in the block.
///
/// It should be written alongside the block's data
/// and used to verify it contains the correct contents to avoid blocks being mixed up.
///
/// Given that `s3_key` is limited to 1KiB, `s3_bucket_name` to 63 characters, we assume
/// the serialized size of this struct to be less than 2KiB.
#[derive(Serialize, Deserialize, Debug)]
struct BlockHeader {
    block_idx: BlockIndex,
    block_offset: u64,
    etag: String,
    s3_bucket_name: String,
    s3_key: String,
    data_checksum: u32,
    header_checksum: u32,
}

impl BlockHeader {
    pub fn new(
        block_idx: BlockIndex,
        block_offset: u64,
        etag: String,
        s3_bucket_name: String,
        s3_key: String,
        data_checksum: Crc32c,
    ) -> Self {
        let data_checksum = data_checksum.value();
        let header_checksum =
            Self::compute_checksum(block_idx, block_offset, &etag, &s3_bucket_name, &s3_key, data_checksum).value();
        BlockHeader {
            block_idx,
            block_offset,
            etag,
            s3_bucket_name,
            s3_key,
            data_checksum,
            header_checksum,
        }
    }

    fn compute_checksum(
        block_idx: BlockIndex,
        block_offset: u64,
        etag: &str,
        s3_bucket_name: &str,
        s3_key: &str,
        data_checksum: u32,
    ) -> Crc32c {
        let mut hasher = crc32c::Hasher::new();
        // Note: .to_be_bytes() always returns big-endian encoded integers
        hasher.update(&block_idx.to_be_bytes());
        hasher.update(&block_offset.to_be_bytes());
        hasher.update(etag.as_bytes());
        hasher.update(s3_bucket_name.as_bytes());
        hasher.update(s3_key.as_bytes());
        hasher.update(&data_checksum.to_be_bytes());
        hasher.finalize()
    }

    /// Validate the integrity of the contained data and return the stored data checksum.
    ///
    /// Execute this method before acting on the data contained within.
    pub fn validate(
        &self,
        s3_bucket_name: &str,
        s3_key: &str,
        etag: &str,
        block_idx: BlockIndex,
        block_offset: u64,
    ) -> Result<Crc32c, BlockAccessError> {
        let s3_bucket_match = s3_bucket_name == self.s3_bucket_name;
        let s3_key_match = s3_key == self.s3_key;
        let etag_match = etag == self.etag;
        let block_idx_match = block_idx == self.block_idx;
        let block_offset_match = block_offset == self.block_offset;

        let data_checksum = self.data_checksum;
        if s3_bucket_match && s3_key_match && etag_match && block_idx_match && block_offset_match {
            if Self::compute_checksum(block_idx, block_offset, etag, s3_bucket_name, s3_key, data_checksum).value()
                != self.header_checksum
            {
                Err(BlockAccessError::ChecksumError)
            } else {
                Ok(Crc32c::new(data_checksum))
            }
        } else {
            warn!(
                s3_key_match,
                etag_match, block_idx_match, "block data did not match expected values",
            );
            Err(BlockAccessError::FieldMismatchError)
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Block {
    /// Information describing the content of `data`, to be used to verify correctness
    header: BlockHeader,
    /// Cached bytes
    data: Bytes,
}

impl Block {
    fn new(
        s3_bucket_name: String,
        cache_key: ObjectId,
        block_idx: BlockIndex,
        block_offset: u64,
        bytes: ChecksummedBytes,
    ) -> Result<Self, IntegrityError> {
        let s3_key = cache_key.key().to_owned();
        let etag = cache_key.etag().as_str().to_owned();
        let (data, data_checksum) = bytes.into_inner()?;
        let header = BlockHeader::new(block_idx, block_offset, etag, s3_bucket_name, s3_key, data_checksum);

        Ok(Block { data, header })
    }

    fn data(
        &self,
        s3_bucket_name: &str,
        cache_key: &ObjectId,
        block_idx: BlockIndex,
        block_offset: u64,
    ) -> Result<ChecksummedBytes, BlockAccessError> {
        let data_checksum = self.header.validate(
            s3_bucket_name,
            cache_key.key(),
            cache_key.etag().as_str(),
            block_idx,
            block_offset,
        )?;
        let bytes = ChecksummedBytes::new_from_inner_data(self.data.clone(), data_checksum);
        Ok(bytes)
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
        }
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

            // Ensure the flow-control window is large enough. We maintain window at least `block_size` away from the last received offset.
            // With default configuration when `block_size + header_size` is less than `part_size` we will complete this loop in one cycle.
            // On contrary, when `part_size` is set to a low value multiple cycles will be required, but we won't ever exhaust the window.
            let end_offset = offset + body.len() as u64;
            let desired_window_end = end_offset.saturating_add(self.block_size);
            let to_increment = desired_window_end.saturating_sub(result.as_ref().read_window_end_offset());
            if to_increment > 0 {
                result.as_mut().increment_read_window(to_increment as usize);
            }
        }

        let mut buffer = Cursor::new(buffer.freeze());
        let mut block_version = [0; CACHE_VERSION.len()];
        buffer.read_exact(&mut block_version)?; // TODO: allow more than 9 cache versions
        if block_version != CACHE_VERSION.as_bytes() {
            warn!(
                found_version = ?block_version, expected_version = ?CACHE_VERSION, bucket_name = self.bucket_name, cache_key = ?cache_key,
                "stale block format found during reading"
            );
            return Err(DataCacheError::InvalidBlockContent);
        }

        let block: Block = match bincode::deserialize_from(buffer) {
            Ok(block) => block,
            Err(e) => {
                warn!("block could not be deserialized: {:?}", e);
                return Err(DataCacheError::InvalidBlockContent);
            }
        };
        let bytes = block
            .data(&self.bucket_name, cache_key, block_idx, block_offset)
            .map_err(|err| match err {
                BlockAccessError::ChecksumError | BlockAccessError::FieldMismatchError => {
                    DataCacheError::InvalidBlockContent
                }
            })?;
        DataCacheResult::Ok(Some(bytes))
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

        let block =
            Block::new(self.bucket_name.clone(), cache_key, block_idx, block_offset, bytes).map_err(
                |err| match err {
                    IntegrityError::ChecksumMismatch(_c1, _c2) => DataCacheError::InvalidBlockContent,
                },
            )?;
        let mut buf = Vec::<u8>::new();
        let mut writer = Cursor::new(&mut buf);
        writer.write_all(CACHE_VERSION.as_bytes())?;
        // TODO: ideally we'd avoid this allocation / memcpy (by storing meta in attrs and removing serializaton)
        if let Err(err) = bincode::serialize_into(writer, &block) {
            return match *err {
                bincode::ErrorKind::Io(io_err) => return Err(DataCacheError::from(io_err)),
                _ => Err(DataCacheError::InvalidBlockContent),
            };
        };

        // TODO: ideally we should use a simple Put rather than MPU.
        let params = PutObjectParams::new();
        let mut req = self
            .client
            .put_object(&self.bucket_name, &object_key, &params)
            .in_current_span()
            .await?;
        req.write(&buf).await?;
        req.complete().await?;

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
    use std::str::FromStr;

    use super::*;
    use crate::checksums::ChecksummedBytes;
    use crate::sync::Arc;

    use test_case::test_case;

    use mountpoint_s3_client::mock_client::{MockClient, MockClientConfig, MockObject};
    use mountpoint_s3_client::types::ETag;

    const DEFAULT_PART_SIZE: usize = 8 * 1024 * 1024;
    const DEFAULT_BLOCK_SIZE: u64 = 512 * 1024;
    const DEFAULT_BUCKET_NAME: &str = "test-bucket";

    #[test_case(1024, 512 * 1024; "block_size smaller than part_size")]
    #[test_case(8 * 1024 * 1024, 512 * 1024; "block_size larger than part_size")]
    #[tokio::test]
    async fn test_put_get(part_size: usize, block_size: u64) {
        let config = MockClientConfig {
            bucket: DEFAULT_BUCKET_NAME.to_string(),
            part_size,
            enable_backpressure: true,
            initial_read_window_size: part_size,
            ..Default::default()
        };
        let client = Arc::new(MockClient::new(config));

        let cache = ExpressDataCache::new(DEFAULT_BUCKET_NAME, client, "unique source description", block_size);

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

    #[tokio::test]
    async fn test_read_bad_version() {
        let key = ObjectId::new("a".into(), ETag::for_tests());
        let config = MockClientConfig {
            bucket: DEFAULT_BUCKET_NAME.to_string(),
            part_size: DEFAULT_PART_SIZE,
            enable_backpressure: true,
            initial_read_window_size: DEFAULT_PART_SIZE,
            ..Default::default()
        };
        let client = Arc::new(MockClient::new(config));
        let cache = ExpressDataCache::new(
            DEFAULT_BUCKET_NAME,
            client.clone(),
            "unique source description",
            DEFAULT_BLOCK_SIZE,
        );

        let create_serialized_block = |block_idx: BlockIndex, cache_version: &str| {
            let block_key = block_key(&cache.prefix, &key, block_idx);
            let block = Block::new(
                DEFAULT_BUCKET_NAME.to_owned(),
                key.clone(),
                block_idx,
                block_idx * DEFAULT_BLOCK_SIZE,
                ChecksummedBytes::new("Foo".into()),
            )
            .expect("should create a block");
            let mut body = Vec::<u8>::new();
            let mut writer = Cursor::new(&mut body);
            writer
                .write_all(cache_version.as_bytes())
                .expect("writing to memory should always succeed");
            bincode::serialize_into(writer, &block).expect("serialization should succeed");
            (block_key, body)
        };

        let (bad_block_key, data) = create_serialized_block(0, "V9");
        client.add_object(&bad_block_key, MockObject::from_bytes(&data, ETag::for_tests()));
        let (good_block_key, data) = create_serialized_block(1, CACHE_VERSION);
        client.add_object(&good_block_key, MockObject::from_bytes(&data, ETag::for_tests()));

        let err = cache
            .get_block(&key, 0, 0)
            .await
            .expect_err("block with bad version should not be retrieved");
        assert!(matches!(err, DataCacheError::InvalidBlockContent));
        cache
            .get_block(&key, 1, DEFAULT_BLOCK_SIZE)
            .await
            .expect("block with good version should be retrieved");
    }

    #[test]
    fn test_block_format_version_requires_update() {
        // check that block's binary repr haven't changed
        let block = Block::new(
            DEFAULT_BUCKET_NAME.to_owned(),
            ObjectId::new("aaa".to_owned(), ETag::for_tests()),
            1,
            DEFAULT_BLOCK_SIZE,
            ChecksummedBytes::new("Foo".into()),
        )
        .expect("should create a block");
        let expected_bytes: Vec<u8> = vec![
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 116, 101, 115, 116, 95, 101, 116,
            97, 103, 11, 0, 0, 0, 0, 0, 0, 0, 116, 101, 115, 116, 45, 98, 117, 99, 107, 101, 116, 3, 0, 0, 0, 0, 0, 0,
            0, 97, 97, 97, 9, 85, 128, 46, 87, 115, 8, 24, 3, 0, 0, 0, 0, 0, 0, 0, 70, 111, 111,
        ];
        let serialized_bytes = bincode::serialize(&block).unwrap();
        assert_eq!(
            expected_bytes, serialized_bytes,
            "serialized disk format appears to have changed, version bump required"
        );
    }

    #[tokio::test]
    async fn test_block_key() {
        // check that the returned hash haven't changed for predefined parameters
        let key = ObjectId::new("a".repeat(266), ETag::for_tests());
        let config = MockClientConfig {
            bucket: DEFAULT_BUCKET_NAME.to_string(),
            ..Default::default()
        };
        let client = Arc::new(MockClient::new(config));
        let cache = ExpressDataCache::new(
            DEFAULT_BUCKET_NAME,
            client.clone(),
            "unique source description",
            DEFAULT_BLOCK_SIZE,
        );

        let expected_key = "bf7942170926947f4d1d83257455085d699183840a44ea2de4fea1227cc6970e/5931fd6bf1fe4eb26db321dda8c5a8917750d8e3a8a984fdbf028b3df59e89ae/0000000000";
        let actual_block_key = block_key(&cache.prefix, &key, 0);
        assert_eq!(expected_key, actual_block_key);
    }

    #[tokio::test]
    async fn test_checksummed_bytes_slice() {
        // TODO: document this test; in particular: why sliced range is lost during put-get?
        let data = ChecksummedBytes::new("0123456789".into());
        let slice = data.slice(1..5);

        let config = MockClientConfig {
            bucket: DEFAULT_BUCKET_NAME.to_string(),
            part_size: DEFAULT_PART_SIZE,
            enable_backpressure: true,
            initial_read_window_size: DEFAULT_PART_SIZE,
            ..Default::default()
        };
        let client = Arc::new(MockClient::new(config));
        let cache = ExpressDataCache::new(
            DEFAULT_BUCKET_NAME,
            client.clone(),
            "unique source description",
            DEFAULT_BLOCK_SIZE,
        );
        let cache_key = ObjectId::new("a".into(), ETag::for_tests());

        cache
            .put_block(cache_key.clone(), 0, 0, slice.clone())
            .await
            .expect("cache should be accessible");
        let entry = cache
            .get_block(&cache_key, 0, 0)
            .await
            .expect("cache should be accessible")
            .expect("cache entry should be returned");
        assert_eq!(
            slice.into_bytes().expect("original slice should be valid"),
            entry.into_bytes().expect("returned entry should be valid"),
            "cache entry returned should match original slice after put"
        );
    }

    #[test]
    fn data_block_extract_checks() {
        let data_1 = ChecksummedBytes::new("Foo".into());

        let cache_key_1 = ObjectId::new("a".into(), ETag::for_tests());
        let cache_key_2 = ObjectId::new("b".into(), ETag::for_tests());
        let cache_key_3 = ObjectId::new("a".into(), ETag::from_str("badetag").unwrap());

        let block = Block::new(
            DEFAULT_BUCKET_NAME.to_owned(),
            cache_key_1.clone(),
            0,
            0,
            data_1.clone(),
        )
        .expect("should have no checksum err");
        block
            .data(DEFAULT_BUCKET_NAME, &cache_key_1, 1, 0)
            .expect_err("should fail due to incorrect block index");
        block
            .data(DEFAULT_BUCKET_NAME, &cache_key_1, 0, 1024)
            .expect_err("should fail due to incorrect block offset");
        block
            .data(DEFAULT_BUCKET_NAME, &cache_key_2, 0, 0)
            .expect_err("should fail due to incorrect s3 key in cache key");
        block
            .data(DEFAULT_BUCKET_NAME, &cache_key_3, 0, 0)
            .expect_err("should fail due to incorrect etag in cache key");
        block
            .data("wrong_bucket", &cache_key_3, 0, 0)
            .expect_err("should fail due to incorrect bucket");
        let unpacked_bytes = block
            .data(DEFAULT_BUCKET_NAME, &cache_key_1, 0, 0)
            .expect("should be OK as all fields match");
        assert_eq!(data_1, unpacked_bytes, "data block should return original bytes");
    }

    #[test]
    fn validate_block_header() {
        let block_idx = 0;
        let block_offset = 0;
        let etag = ETag::for_tests();
        let s3_key = String::from("s3/key");
        let data_checksum = Crc32c::new(42);
        let mut header = BlockHeader::new(
            block_idx,
            block_offset,
            etag.as_str().to_owned(),
            DEFAULT_BUCKET_NAME.to_owned(),
            s3_key.clone(),
            data_checksum,
        );

        let checksum = header
            .validate(DEFAULT_BUCKET_NAME, &s3_key, etag.as_str(), block_idx, block_offset)
            .expect("should be OK with valid fields and checksum");
        assert_eq!(data_checksum, checksum);

        // Bad fields
        let err = header
            .validate(DEFAULT_BUCKET_NAME, "hello", etag.as_str(), block_idx, block_offset)
            .expect_err("should fail with invalid s3_key");
        assert!(matches!(err, BlockAccessError::FieldMismatchError));
        let err = header
            .validate(DEFAULT_BUCKET_NAME, &s3_key, "bad etag", block_idx, block_offset)
            .expect_err("should fail with invalid etag");
        assert!(matches!(err, BlockAccessError::FieldMismatchError));
        let err = header
            .validate(DEFAULT_BUCKET_NAME, &s3_key, etag.as_str(), 5, block_offset)
            .expect_err("should fail with invalid block idx");
        assert!(matches!(err, BlockAccessError::FieldMismatchError));
        let err = header
            .validate(DEFAULT_BUCKET_NAME, &s3_key, etag.as_str(), block_idx, 1024)
            .expect_err("should fail with invalid block offset");
        assert!(matches!(err, BlockAccessError::FieldMismatchError));
        let err = header
            .validate("wrong_bucket", &s3_key, etag.as_str(), block_idx, 1024)
            .expect_err("should fail with invalid bucket name");
        assert!(matches!(err, BlockAccessError::FieldMismatchError));

        // Bad checksum
        header.header_checksum = 23;
        let err = header
            .validate(DEFAULT_BUCKET_NAME, &s3_key, etag.as_str(), block_idx, block_offset)
            .expect_err("should fail with invalid checksum");
        assert!(matches!(err, BlockAccessError::ChecksumError));
    }
}
