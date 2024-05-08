use mountpoint_s3_client::{ListObjectsError, ObjectClientError};
use tracing::error;
use mountpoint_s3_client::S3CrtClient;
use mountpoint_s3_client::ErrorCode;

macro_rules! event_log_entry {
    ($event_type:expr, $($args:tt)*) => { error!(event_type = $event_type, $($args)*) }
}

pub trait ReportClientError {
    fn report_client_error(&self, bucket: &str, key: &str);
}

impl ReportClientError for ObjectClientError<ListObjectsError, <S3CrtClient as mountpoint_s3_client::ObjectClient>::ClientError> {
    fn report_client_error(&self, bucket: &str, key: &str) {
        match &self {
            ObjectClientError::ClientError(client_err) => {
                event_log_entry!(client_err.get_code(), message = ?client_err, s3_bucket = bucket, s3_object_key = key)
            },
            err @ ObjectClientError::ServiceError(_) => {
                event_log_entry!("error.cli.internal", message = ?err, s3_bucket = bucket, s3_object_key = key)
            }
        };
    }
}