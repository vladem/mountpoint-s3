use mountpoint_s3_fs::logging::error_logger::Event;
use serde_json;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::thread;
use std::time::Duration;

fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("HttpErrorLogger Event Reader");
    println!("Connecting to server at /tmp/mountpoint-s3-errors.sock");
    println!("Reading events every 5 seconds...");
    println!("Press Ctrl+C to exit...");
    println!();

    loop {
        match read_events_from_server() {
            Ok(events) => {
                println!("=== {} Events Retrieved ===", events.len());
                if events.is_empty() {
                    println!("No events found");
                } else {
                    for (i, event) in events.iter().enumerate() {
                        println!(
                            "Event {}: {} - {} ({})",
                            i + 1,
                            event.operation,
                            event.error_code,
                            event.timestamp.format(&time::format_description::well_known::Rfc3339)
                                .unwrap_or_else(|_| "Invalid timestamp".to_string())
                        );
                        if let Some(msg) = &event.internal_message {
                            println!("  Message: {}", msg);
                        }
                        if let Some(key) = &event.s3_object_key {
                            println!("  S3 Object: {}", key);
                        }
                    }
                }
                println!();
            }
            Err(e) => {
                println!("Failed to read events: {}", e);
                println!("Make sure the HttpErrorLogger server is running on /tmp/mountpoint-s3-errors.sock");
                println!();
            }
        }

        // Wait 5 seconds before next read
        thread::sleep(Duration::from_secs(5));
    }
}

fn read_events_from_server() -> anyhow::Result<Vec<Event>> {
    // Connect to Unix socket
    let mut stream = UnixStream::connect("/tmp/mountpoint-s3-errors.sock")?;

    // Send HTTP GET request
    let request = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";
    stream.write_all(request.as_bytes())?;

    // Read response
    let mut response = String::new();
    stream.read_to_string(&mut response)?;

    // Parse HTTP response to extract JSON body
    let body = extract_json_from_http_response(&response)?;

    // Parse JSON to events
    let events: Vec<Event> = serde_json::from_str(&body)?;
    Ok(events)
}

fn extract_json_from_http_response(response: &str) -> anyhow::Result<String> {
    // Find the double CRLF that separates headers from body
    if let Some(body_start) = response.find("\r\n\r\n") {
        Ok(response[body_start + 4..].to_string())
    } else {
        anyhow::bail!("Invalid HTTP response format");
    }
}
