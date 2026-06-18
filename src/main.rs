use clap::Parser;
use handshake_parser::Token;
use log::{debug, error, info};
use multimap::MultiMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};

use crate::handshake_parser::{DecodeSideError, DecodeTokenError, HandshakeType, parse_handshake};

mod handshake_parser;

const PEER_TIMEOUT: Duration = Duration::from_secs(5);
const GC_INTERVAL: Duration = Duration::from_secs(10);

// Main struct representing a connection
#[derive(Debug)]
struct Peer {
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
    #[error("No valid handshake")]
    NoValidHandshake,
    #[error("Peer is impatient")]
    PeerImpatient,
    #[error("Decode side error")]
    DecodeSideError(#[from] DecodeSideError),
    #[error("Decode token error")]
    DecodeTokenError(#[from] DecodeTokenError),
}

impl Peer {
    fn new(stream: TcpStream, socket: SocketAddr, handshake: HandshakeType) -> Self {
        let timestamp = SystemTime::now();
        Peer {
            stream,
            socket,
            handshake,
            timestamp,
        }
    }

    async fn handle_handshake(
        mut stream: TcpStream,
        socket: SocketAddr,
    ) -> Result<Peer, HandleConnectionError> {
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
    peers: Mutex<MultiMap<Token, Peer>>,
}

impl Pending {
    async fn add(&self, peer: Peer) {
        let mut peers = self.peers.lock().await;
        let token = peer.handshake.get_token();
        peers.insert(token.clone(), peer);
    }

    async fn match_peers(&self, peer_a: &Peer) -> Option<Peer> {
        let mut peers = self.peers.lock().await;
        let token_a = peer_a.handshake.get_token();

        if let Some(peers) = peers.remove(token_a) {
            debug!(
                "Peers matched and removed from queue: {:?} ({} remaining in queue)",
                peers,
                peers.len()
            );

            for peer_b in peers {
                let side_a = peer_a.handshake.get_side();
                let side_b = peer_b.handshake.get_side();

                match (side_a, side_b) {
                    // One peer uses old protocol, connect anyways
                    (None, _) | (_, None) => return Some(peer_b),
                    // Only return if ID's are not the same
                    (Some(s1), Some(s2)) if s1 != s2 => return Some(peer_b),
                    _ => {}
                }
            }
        }

        None
    }

    async fn collect_garbage(&self) {
        let mut peers = self.peers.lock().await;

        let tokens = peers
            .iter()
            .filter(|(_, peer)| {
                peer.timestamp
                    .elapsed()
                    // In case we are in the future, just use 0
                    .unwrap_or(Duration::from_secs(0))
                    .ge(&PEER_TIMEOUT)
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();

        if !tokens.is_empty() {
            debug!("{} dead peers found", tokens.len());
        }

        for token in tokens {
            if let Some(c) = peers.remove(&token) {
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
    async fn handle_connection(
        &self,
        stream: TcpStream,
        socket: SocketAddr,
    ) -> Result<(), HandleConnectionError> {
        let peer = Peer::handle_handshake(stream, socket).await?;
        self.match_peers(peer).await
    }

    async fn match_peers(&self, peer: Peer) -> Result<(), HandleConnectionError> {
        info!("Peer connected: {}", &peer.socket);

        if let Some(other_peer) = self.pending.match_peers(&peer).await {
            // Partner found - notify them and start relay
            info!(
                "Peers matched: {} {} <=> {} {}",
                peer.socket, peer.handshake, other_peer.handshake, other_peer.socket
            );

            Self::tunnel(peer.stream, other_peer.stream).await?;
            info!("Tunnel closed")
        } else {
            // No partner yet - add to pending and wait
            debug!("Peer {} added to list", peer.socket);
            self.pending.add(peer).await;
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

        tokio::io::copy_bidirectional(&mut s1, &mut s2).await?;

        Ok(())
    }
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
    env_logger::init();

    let relay: &'static TransitRelay = Box::leak(Box::new(TransitRelay::default()));

    let mut handles = Vec::new();

    for addr in args.listen {
        handles.push(tokio::spawn(listen_on(addr, relay)));
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

async fn listen_on(addr: SocketAddr, relay: &'static TransitRelay) {
    let listener = TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("Failed to bind to {addr}: {e}"));

    info!("Listening on {addr}");

    loop {
        let (stream, socket) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to accept connection: {e}");
                continue;
            }
        };

        // Make the connection as realtime as possible
        stream.set_nodelay(true).expect("Set socket no delay");

        tokio::spawn(async move {
            if let Err(e) = relay.handle_connection(stream, socket).await {
                error!("Connection error ({socket}): {e}");
            }
        });
    }
}
