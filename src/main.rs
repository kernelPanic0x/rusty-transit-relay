use clap::Parser;
use env_logger::Builder;
use handshake_parser::Token;
use log::{debug, info, warn};
use multimap::MultiMap;
use regex::Regex;
use std::fmt::{self, Debug, Display};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};

use crate::handshake_parser::{parse_handshake, DecodeSideError, DecodeTokenError, HandshakeType};

mod handshake_parser;

const BUFFER_SIZE: usize = 1024 * 1024;
const PEER_TIMEOUT: Duration = Duration::from_secs(5);
const GC_INTERVAL: Duration = Duration::from_secs(10);

// Main struct representing a connection
#[derive(Debug)]
struct Connection {
    stream: TcpStream,
    socket: SocketAddr,
    handshake: HandshakeType,
    timestamp: SystemTime,
}

#[derive(Debug, Error)]
enum HandleConnectionError {
    #[error("Buffer is full")]
    BufferFull,
    #[error("Invalid utf-8 string")]
    InvalidString(#[from] core::str::Utf8Error),
    #[error("IO error: {0}")]
    IoError(#[from] tokio::io::Error),
    #[error("Connection timed out")]
    ConnectionTimeout(#[from] tokio::time::error::Elapsed),
    #[error("Missing lagacy token variable")]
    NoLagacyToken,
    #[error("Missing token variable")]
    NoToken,
    #[error("Missing side variable")]
    NoSide,
    #[error("No valid handshake")]
    NoValidHandshake,
    #[error("Peer is impatient")]
    PeerImpatient,
    #[error("Decode side error")]
    DecodeSideError(#[from] DecodeSideError),
    #[error("Decode token error")]
    DecodeTokenError(#[from] DecodeTokenError),
}

impl Connection {
    fn new(stream: TcpStream, socket: SocketAddr, handshake: HandshakeType) -> Self {
        let timestamp = SystemTime::now();
        Connection {
            stream,
            socket,
            handshake,
            timestamp,
        }
    }

    async fn handle_handshake(
        mut stream: TcpStream,
        socket: SocketAddr,
    ) -> Result<Connection, HandleConnectionError> {
        let mut buf = [0u8; 128];
        let mut read_buf = ReadBuf::new(&mut buf);

        let mut line = timeout(PEER_TIMEOUT, async {
            loop {
                if read_buf.remaining() == 0 {
                    return Err(HandleConnectionError::BufferFull);
                }

                stream.read_buf(&mut read_buf).await?;

                if let Some(pos) = read_buf.filled().iter().position(|&b| b == b'\n') {
                    return Ok(std::str::from_utf8(&read_buf.filled()[..pos])?);
                }
            }
        })
        .await??;

        let handshake =
            parse_handshake(&mut line).map_err(|_| HandleConnectionError::NoValidHandshake)?;

        Ok(Self::new(stream, socket, handshake))
    }
}

#[derive(Debug, Default)]
struct Pending {
    peers: Mutex<MultiMap<Token, Connection>>,
}

impl Pending {
    async fn add(&self, conn: Connection) {
        let mut peers = self.peers.lock().await;
        let token = conn.handshake.get_token();
        peers.insert(token.clone(), conn);
    }

    async fn find_match_and_remove(&self, peer: &Connection) -> Option<Connection> {
        let mut peers = self.peers.lock().await;
        let token = peer.handshake.get_token();

        if let Some(peer_list) = peers.remove(token) {
            debug!(
                "Peers matched and removed from queue: {:?} ({} remaining in queue)",
                peer_list,
                peers.len()
            );

            for other_peer in peer_list {
                let side = peer.handshake.get_side();
                let other_side = other_peer.handshake.get_side();

                match (side, other_side) {
                    // One peer uses old protocol, connect anyways
                    (None, _) | (_, None) => return Some(other_peer),
                    // Only return if ID's are not the same
                    (Some(s1), Some(s2)) if s1 != s2 => return Some(other_peer),
                    _ => {}
                }
            }
        }

        None
    }

    async fn collect_garbage(&self) {
        let mut peers = self.peers.lock().await;

        let dead = peers
            .iter()
            .filter(|(_, conn)| {
                conn.timestamp
                    .elapsed()
                    .unwrap_or(Duration::from_secs(0))
                    .ge(&PEER_TIMEOUT)
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();

        if !dead.is_empty() {
            debug!("{} dead peers found", dead.len());
        }

        for peer_key in dead {
            if let Some(c) = peers.remove(&peer_key) {
                debug!(
                    "Peer removed from queue: {:?} ({} remaining in queue)",
                    c,
                    peers.len()
                );
            }
        }
    }
}

#[derive(Debug, Default)]
struct TransitRelay {
    pending: Pending,
}

impl TransitRelay {
    async fn handle_connection(&self, conn: Connection) -> Result<(), HandleConnectionError> {
        info!("Peer connected: {}", &conn.socket);

        if let Some(other_peer) = self.pending.find_match_and_remove(&conn).await {
            // Partner found - notify them and start relay
            info!(
                "Peers matched: {} {} <=> {} {}",
                conn.socket, conn.handshake, other_peer.handshake, other_peer.socket
            );

            Self::tunnel(conn.stream, other_peer.stream).await?;
            info!("Tunnel closed")
        } else {
            // No partner yet - add to pending and wait
            debug!("Peer {} added to list", conn.socket);
            self.pending.add(conn).await;
        }

        Ok(())
    }

    async fn tunnel(mut s1: TcpStream, mut s2: TcpStream) -> Result<(), HandleConnectionError> {
        // Check for premature data
        if [&mut s1, &mut s2]
            .iter()
            .map(|s| s.try_read(&mut [0u8; 1]))
            .any(|r| r.unwrap_or_default() > 0)
        {
            for s in [&mut s1, &mut s2] {
                let _ = s.write_all(b"impatient\n").await;
            }
            return Err(HandleConnectionError::PeerImpatient);
        }

        // Send ready flag
        for s in [&mut s1, &mut s2] {
            s.write_all(b"ok\n").await?;
        }

        debug!("Ready flags sent, starting relay");

        tokio::io::copy_bidirectional_with_sizes(&mut s1, &mut s2, BUFFER_SIZE, BUFFER_SIZE)
            .await?;

        Ok(())
    }
}

async fn create_connection(
    relay: Arc<TransitRelay>,
    stream: TcpStream,
    socket: SocketAddr,
) -> Result<(), HandleConnectionError> {
    let conn = Connection::handle_handshake(stream, socket).await?;
    relay.handle_connection(conn).await
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[clap(long, value_parser = parse_socket_addrs, default_values = ["[::]:4001"])]
    listen: Vec<SocketAddr>,
}

fn parse_socket_addrs(s: &str) -> Result<SocketAddr, std::net::AddrParseError> {
    s.parse::<SocketAddr>()
}

// main.rs
#[tokio::main]
async fn main() -> ! {
    let args = Args::parse();
    Builder::from_default_env().init();

    let relay = Arc::new(TransitRelay::default());

    let mut handles = Vec::new();

    for addr in args.listen {
        let relay_clone = relay.clone();
        handles.push(tokio::spawn(listen_on(addr, relay_clone)));
    }

    // Garbage collection job
    handles.push(tokio::spawn(async move {
        loop {
            relay.pending.collect_garbage().await;
            sleep(GC_INTERVAL).await;
        }
    }));

    let _ = futures::future::select_all(handles).await;
    unreachable!("critical process exited")
}

async fn listen_on(addr: SocketAddr, relay: Arc<TransitRelay>) {
    let listener = TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("Failed to bind to {addr}: {e}"));

    info!("Listening on {addr}");

    loop {
        let (stream, socket) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to accept connection: {e}");
                continue;
            }
        };

        // Make the connection as realtime as possible
        stream.set_nodelay(true).expect("Set socket no delay");

        let relay_clone = relay.clone();
        tokio::spawn(async move {
            if let Err(e) = create_connection(relay_clone, stream, socket).await {
                info!("Connection error ({socket}): {e}");
            }
        });
    }
}
