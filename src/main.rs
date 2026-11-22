use std::env;
use std::io::{ErrorKind, Result};
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const BUFLEN: usize = 4096 * 4;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const RESPONSE: &[u8] = b"HTTP/1.1 101 GegeVPS\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: foo\r\n\r\n";

fn find_header(head: &[u8], header: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(head);
    let pattern = format!("{}: ", String::from_utf8_lossy(header));
    let index = text.find(&pattern)?;
    let start = text[index..].find(':')? + index;
    let value = text.get(start + 2..)?;
    let end = value.find("\r\n")?;
    Some(value[..end].to_owned())
}

async fn copy_bidirectional(client: &mut TcpStream, target: &mut TcpStream) -> Result<()> {
    let mut client_buffer = vec![0u8; BUFLEN];
    let mut target_buffer = vec![0u8; BUFLEN];

    loop {
        tokio::select! {
            result = client.read(&mut client_buffer) => {
                let count = result?;
                if count == 0 {
                    return Ok(());
                }
                target.write_all(&client_buffer[..count]).await?;
                target.flush().await?;
            }
            result = target.read(&mut target_buffer) => {
                let count = result?;
                if count == 0 {
                    return Ok(());
                }
                client.write_all(&target_buffer[..count]).await?;
                client.flush().await?;
            }
        }
    }
}

async fn handle_connection(mut client: TcpStream, peer: SocketAddr, default_port: u16) {
    let log = |message: &str| println!("Connection: {peer} - {message}");

    let mut initial = vec![0u8; BUFLEN];
    let received = match timeout(CONNECT_TIMEOUT, client.read(&mut initial)).await {
        Ok(Ok(count)) => count,
        Ok(Err(error)) => {
            log(&format!("error: {error}"));
            return;
        }
        Err(_) => {
            log("error: initial request timeout");
            return;
        }
    };

    if received == 0 {
        return;
    }
    initial.truncate(received);

    let mut host_port = find_header(&initial, b"X-Real-Host").unwrap_or_default();
    if host_port.is_empty() {
        host_port = format!("127.0.0.1:{default_port}");
    }

    if find_header(&initial, b"X-Split").is_some() {
        let mut ignored = [0u8; BUFLEN];
        let _ = timeout(CONNECT_TIMEOUT, client.read(&mut ignored)).await;
    }

    let (host, port) = match host_port.rsplit_once(':') {
        Some((host, port)) => match port.parse::<u16>() {
            Ok(port) => (host.to_owned(), port),
            Err(_) => {
                let _ = client.write_all(b"HTTP/1.1 400 WrongPort!\r\n\r\n").await;
                log(&format!("CONNECT {host_port} - error: invalid port"));
                return;
            }
        },
        None => (host_port.clone(), default_port),
    };

    let mut target = match timeout(CONNECT_TIMEOUT, TcpStream::connect((host.as_str(), port))).await
    {
        Ok(Ok(target)) => target,
        Ok(Err(error)) => {
            let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway!\r\n\r\n").await;
            log(&format!("CONNECT {host_port} - error: {error}"));
            return;
        }
        Err(_) => {
            let _ = client
                .write_all(b"HTTP/1.1 504 Gateway Timeout!\r\n\r\n")
                .await;
            log(&format!("CONNECT {host_port} - error: connection timeout"));
            return;
        }
    };

    if let Err(error) = client.write_all(RESPONSE).await {
        log(&format!("CONNECT {host_port} - error: {error}"));
        return;
    }

    log(&format!("CONNECT {host_port}"));

    match timeout(IDLE_TIMEOUT, copy_bidirectional(&mut client, &mut target)).await {
        Ok(Ok(())) => log("closed"),
        Ok(Err(error)) if error.kind() != ErrorKind::UnexpectedEof => {
            log(&format!("error: {error}"))
        }
        Ok(Err(_)) => log("closed"),
        Err(_) => log("timeout"),
    }
}

fn usage() {
    println!("Usage: websocket-bridge [port]");
    println!("       websocket-bridge --bind <addr> --port <port>");
    println!("       websocket-bridge --bind 0.0.0.0 --port 8880");
}

fn parse_port(value: &str) -> Result<u16> {
    value
        .parse()
        .map_err(|error| std::io::Error::new(ErrorKind::InvalidInput, error))
}

fn parse_args() -> Result<(String, u16)> {
    let mut bind = "0.0.0.0".to_owned();
    let mut port = 8880;
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                usage();
                std::process::exit(0);
            }
            "-b" | "--bind" => {
                bind = args.next().ok_or_else(|| {
                    std::io::Error::new(ErrorKind::InvalidInput, "--bind requires an address")
                })?
            }
            "-p" | "--port" => {
                port = parse_port(&args.next().ok_or_else(|| {
                    std::io::Error::new(ErrorKind::InvalidInput, "--port requires a value")
                })?)?
            }
            value if !value.starts_with('-') => port = parse_port(value)?,
            value => {
                return Err(std::io::Error::new(
                    ErrorKind::InvalidInput,
                    format!("unknown argument: {value}"),
                ))
            }
        }
    }

    Ok((bind, port))
}

#[tokio::main]
async fn main() -> Result<()> {
    let (bind, port) = parse_args()?;
    let listener = TcpListener::bind((bind.as_str(), port)).await?;

    println!("\n:-------WebSocketProxy-------:\n");
    println!("Listening addr: {bind}");
    println!("Listening port: {port}\n");
    println!(":---------------------------:\n");

    loop {
        let (client, peer) = listener.accept().await?;
        tokio::spawn(handle_connection(client, peer, port));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_exact_header() {
        let request = b"GET / HTTP/1.1\r\nX-Real-Host: 127.0.0.1:143\r\nX-Split: 1\r\n\r\n";
        assert_eq!(
            find_header(request, b"X-Real-Host").as_deref(),
            Some("127.0.0.1:143")
        );
        assert_eq!(find_header(request, b"X-Split").as_deref(), Some("1"));
        assert_eq!(find_header(request, b"X-Missing"), None);
    }

    #[test]
    fn ignores_invalid_utf8_without_panic() {
        assert_eq!(find_header(&[0xff, b'X', b'-', b'R'], b"X-Real-Host"), None);
    }

    #[test]
    fn rejects_invalid_port() {
        assert!(parse_port("not-a-port").is_err());
        assert_eq!(parse_port("8880").unwrap(), 8880);
    }

    #[tokio::test]
    async fn proxies_bidirectional_tcp_traffic() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};
        use tokio::time::{sleep, Duration};

        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_port = target.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut upstream, _) = target.accept().await.unwrap();
            let mut buffer = [0u8; 16];
            let count = upstream.read(&mut buffer).await.unwrap();
            upstream.write_all(&buffer[..count]).await.unwrap();
            upstream.flush().await.unwrap();
        });

        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_port = proxy.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (client, _) = proxy.accept().await.unwrap();
            handle_connection(client, "127.0.0.1:1".parse().unwrap(), 1).await;
        });

        let mut client = TcpStream::connect(("127.0.0.1", proxy_port)).await.unwrap();
        let request = format!("GET / HTTP/1.1\r\nX-Real-Host: 127.0.0.1:{target_port}\r\n\r\n");
        client.write_all(request.as_bytes()).await.unwrap();

        let mut response = Vec::new();
        while !response.ends_with(b"\r\n\r\n") {
            let mut buffer = [0u8; 256];
            let count = client.read(&mut buffer).await.unwrap();
            assert_ne!(count, 0, "proxy closed before sending upgrade response");
            response.extend_from_slice(&buffer[..count]);
        }
        assert!(response.starts_with(b"HTTP/1.1 101 GegeVPS"));

        client.write_all(b"hello").await.unwrap();
        sleep(Duration::from_millis(50)).await;
        let mut echoed = vec![0u8; 5];
        let count = client.read(&mut echoed).await.unwrap();
        assert_eq!(&echoed[..count], b"hello");
    }
}
