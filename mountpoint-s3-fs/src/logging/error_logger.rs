use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, serde::rfc3339};

use crate::fs::Error;
use crate::fs::error_metadata::{MOUNTPOINT_ERROR_INTERNAL, MOUNTPOINT_ERROR_LOOKUP_NONEXISTENT};
use crate::fuse::ErrorLogger;

const VERSION: &str = "1";

/// Trait for abstracting over different stream types (TCP and Unix sockets)
trait HttpStream: std::io::Read + std::io::Write {}

impl HttpStream for std::net::TcpStream {}
impl HttpStream for std::os::unix::net::UnixStream {}

/// Configuration for the HTTP error logger
#[derive(Debug, Clone)]
pub struct HttpErrorLoggerConfig {
    /// Maximum number of events to store in memory
    pub max_events: usize,
    /// HTTP server bind address (e.g., "127.0.0.1:8080" or "/tmp/mountpoint-errors.sock" for UDS)
    pub bind_address: String,
    /// Whether to use Unix Domain Socket (true) or TCP (false)
    pub use_unix_socket: bool,
}

impl Default for HttpErrorLoggerConfig {
    fn default() -> Self {
        Self {
            max_events: 1000,
            bind_address: "/tmp/mountpoint-s3-errors.sock".to_string(),
            use_unix_socket: true,
        }
    }
}

/// [HttpErrorLogger] provides a callback for logging errors in a structured format to memory
/// and serves them via HTTP interface. Events are stored in a circular buffer with configurable
/// size, and can be accessed via HTTP GET requests.
///
/// The HTTP server supports both TCP and Unix Domain Socket endpoints. Events are served as
/// JSON arrays at the root endpoint ("/").
///
/// The output format is the same as FileErrorLogger - JSON objects with fields like
/// `error_code`, `s3_error_http_status`, `s3_error_code`, etc.
pub struct HttpErrorLogger {
    http_server_thread: Option<JoinHandle<()>>,
    events_buffer: Arc<Mutex<VecDeque<Event>>>,
    config: HttpErrorLoggerConfig,
}

impl ErrorLogger for HttpErrorLogger {
    fn error(&self, err: &crate::fs::Error, fuse_operation: &str, fuse_request_id: u64) {
        self.log_error(err, fuse_operation, fuse_request_id);
    }

    fn event(&self, operation: &str, event_code: &str) {
        self.log_event(operation, event_code);
    }
}

impl HttpErrorLogger {
    /// Creates a new HTTP error logger with the given configuration
    pub fn new(config: HttpErrorLoggerConfig) -> anyhow::Result<Self> {
        let events_buffer = Arc::new(Mutex::new(VecDeque::with_capacity(config.max_events)));
        let events_buffer_http = Arc::clone(&events_buffer);

        // Spawn HTTP server thread
        let bind_address = config.bind_address.clone();
        let use_unix_socket = config.use_unix_socket;
        let http_server_thread =
            thread::spawn(move || Self::run_http_server(events_buffer_http, bind_address, use_unix_socket));

        Ok(Self {
            http_server_thread: Some(http_server_thread),
            events_buffer,
            config,
        })
    }

    /// Creates a new HTTP error logger with default configuration
    pub fn with_defaults() -> anyhow::Result<Self> {
        Self::new(HttpErrorLoggerConfig::default())
    }

    /// Logs a failed fuse operation. The field `error_code` is set to `MOUNTPOINT_ERROR_INTERNAL` if it
    /// is missing in the input `fs::Error`.
    fn log_error(&self, error: &Error, fuse_operation: &str, fuse_request_id: u64) {
        let error_code = match &error.meta().error_code {
            Some(error_code) => error_code,
            None => MOUNTPOINT_ERROR_INTERNAL,
        };
        if error_code == MOUNTPOINT_ERROR_LOOKUP_NONEXISTENT {
            return;
        }
        let event = Event::new_error_event(error, fuse_operation, fuse_request_id, error_code);
        self.add_event_to_buffer(event);
    }

    /// Log an event with the given operation and code
    fn log_event(&self, operation: &str, event_code: &str) {
        let event = Event::new_simple_event(operation, event_code);
        self.add_event_to_buffer(event);
    }

    /// Helper method to add an event to the circular buffer
    fn add_event_to_buffer(&self, event: Event) {
        if let Ok(mut buffer) = self.events_buffer.lock() {
            // Add new event
            buffer.push_back(event);

            // Remove old events if we exceed the limit
            while buffer.len() > self.config.max_events {
                buffer.pop_front();
            }
        }
    }

    /// Run the HTTP server to serve events
    fn run_http_server(events_buffer: Arc<Mutex<VecDeque<Event>>>, bind_address: String, use_unix_socket: bool) {
        use std::net::TcpListener;
        use std::os::unix::net::UnixListener;

        if use_unix_socket {
            // Remove existing socket file if it exists
            let _ = std::fs::remove_file(&bind_address);

            match UnixListener::bind(&bind_address) {
                Ok(listener) => {
                    tracing::info!("HTTP error logger listening on Unix socket: {}", bind_address);
                    for stream in listener.incoming() {
                        match stream {
                            Ok(stream) => {
                                Self::handle_connection(stream, Arc::clone(&events_buffer));
                            }
                            Err(e) => {
                                tracing::error!("Failed to accept Unix socket connection: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to bind Unix socket {}: {}", bind_address, e);
                }
            }
        } else {
            match TcpListener::bind(&bind_address) {
                Ok(listener) => {
                    tracing::info!("HTTP error logger listening on TCP: {}", bind_address);
                    for stream in listener.incoming() {
                        match stream {
                            Ok(stream) => {
                                Self::handle_connection(stream, Arc::clone(&events_buffer));
                            }
                            Err(e) => {
                                tracing::error!("Failed to accept TCP connection: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to bind TCP socket {}: {}", bind_address, e);
                }
            }
        }
    }

    fn handle_connection<S: HttpStream>(mut stream: S, events_buffer: Arc<Mutex<VecDeque<Event>>>) {
        let mut buffer = [0; 1024];
        match stream.read(&mut buffer) {
            Ok(_) => {
                let response = Self::generate_http_response(events_buffer);
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
            Err(e) => {
                tracing::error!("Failed to read from stream: {}", e);
            }
        }
    }

    fn generate_http_response(events_buffer: Arc<Mutex<VecDeque<Event>>>) -> String {
        let events = match events_buffer.lock() {
            Ok(buffer) => buffer.iter().cloned().collect::<Vec<_>>(),
            Err(_) => Vec::new(),
        };

        let json_body = match serde_json::to_string_pretty(&events) {
            Ok(json) => json,
            Err(e) => {
                tracing::error!("Failed to serialize events to JSON: {}", e);
                "[]".to_string()
            }
        };

        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\n\r\n{}",
            json_body.len(),
            json_body
        )
    }

    /// Get current events (for testing or direct access)
    pub fn get_events(&self) -> Vec<Event> {
        match self.events_buffer.lock() {
            Ok(buffer) => buffer.iter().cloned().collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Get the number of events currently stored
    pub fn event_count(&self) -> usize {
        match self.events_buffer.lock() {
            Ok(buffer) => buffer.len(),
            Err(_) => 0,
        }
    }
}

impl Drop for HttpErrorLogger {
    fn drop(&mut self) {
        // Note: We don't wait for the HTTP server thread as it runs indefinitely
        // In a real implementation, you might want to add a shutdown mechanism

        // Clean up Unix socket file if we were using one
        if self.config.use_unix_socket {
            let _ = std::fs::remove_file(&self.config.bind_address);
        }

        tracing::debug!("HttpErrorLogger dropped");
    }
}

impl std::fmt::Debug for HttpErrorLogger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HttpErrorLogger(bind_address: {}, max_events: {})",
            self.config.bind_address, self.config.max_events
        )
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct Event {
    #[serde(with = "rfc3339")]
    pub timestamp: OffsetDateTime,
    pub operation: String,
    pub fuse_request_id: Option<u64>,
    pub error_code: String,
    pub errno: Option<i32>,
    pub internal_message: Option<String>,
    pub s3_object_key: Option<String>,
    pub s3_bucket_name: Option<String>,
    pub s3_error_http_status: Option<i32>,
    pub s3_error_code: Option<String>,
    pub s3_error_message: Option<String>,
    pub version: String,
}

impl Event {
    /// Creates a new simple event with just operation and event code
    ///
    /// Note: `event_code` is serialized as `error_code` because of the existing contract
    pub fn new_simple_event(operation: &str, event_code: &str) -> Self {
        Event {
            timestamp: OffsetDateTime::now_utc(),
            operation: operation.to_string(),
            error_code: event_code.to_string(),
            version: VERSION.to_string(),
            fuse_request_id: None,
            errno: None,
            internal_message: None,
            s3_object_key: None,
            s3_bucket_name: None,
            s3_error_http_status: None,
            s3_error_code: None,
            s3_error_message: None,
        }
    }

    /// Creates a new error event with comprehensive error details from a filesystem error
    pub fn new_error_event(error: &Error, fuse_operation: &str, fuse_request_id: u64, error_code: &str) -> Self {
        Event {
            timestamp: OffsetDateTime::now_utc(),
            operation: fuse_operation.to_string(),
            fuse_request_id: Some(fuse_request_id),
            error_code: error_code.to_string(),
            errno: Some(error.errno),
            internal_message: Some(format!("{error:#}")),
            s3_object_key: error.meta().s3_object_key.clone(),
            s3_bucket_name: error.meta().s3_bucket_name.clone(),
            s3_error_http_status: error.meta().client_error_meta.http_code,
            s3_error_code: error.meta().client_error_meta.error_code.clone(),
            s3_error_message: error.meta().client_error_meta.error_message.clone(),
            version: VERSION.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf, time::Duration};

    use crate::{
        fs::error_metadata::{ErrorMetadata, MOUNTPOINT_ERROR_CLIENT},
        prefetch::PrefetchReadError,
    };

    use super::*;
    use mountpoint_s3_client::{
        error::{GetObjectError, ObjectClientError},
        error_metadata::ClientErrorMetadata,
    };
    use tempfile::tempdir;

    #[test]
    fn test_http_error_logger() {
        let config = HttpErrorLoggerConfig {
            max_events: 5,
            bind_address: "/tmp/test-mountpoint-errors.sock".to_string(),
            use_unix_socket: true,
        };

        let logger = HttpErrorLogger::new(config).expect("must create HTTP error logger");

        // Log some events
        logger.log_event("test_operation", "test_code");
        logger.log_event("another_operation", "another_code");

        // Check that events were stored immediately (no async processing)
        let events = logger.get_events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].operation, "test_operation");
        assert_eq!(events[0].error_code, "test_code");
        assert_eq!(events[1].operation, "another_operation");
        assert_eq!(events[1].error_code, "another_code");
    }

    #[test]
    fn test_circular_buffer() {
        let config = HttpErrorLoggerConfig {
            max_events: 2, // Small buffer to test circular behavior
            bind_address: "/tmp/test-circular-errors.sock".to_string(),
            use_unix_socket: true,
        };

        let logger = HttpErrorLogger::new(config).expect("must create HTTP error logger");

        // Log more events than the buffer can hold
        logger.log_event("event1", "code1");
        logger.log_event("event2", "code2");
        logger.log_event("event3", "code3"); // This should push out event1

        // Check that circular buffer behavior works immediately
        let events = logger.get_events();
        assert_eq!(events.len(), 2);
        // Should have event2 and event3, event1 should be gone
        assert_eq!(events[0].operation, "event2");
        assert_eq!(events[1].operation, "event3");
    }
}
