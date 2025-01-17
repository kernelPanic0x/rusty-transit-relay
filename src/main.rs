use anyhow::{anyhow, bail};
use bytes::{BufMut, BytesMut};
use clap::Parser;
use env_logger::Builder;
use log::{debug, info};
use multimap::MultiMap;
use regex::Regex;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, Interest};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;

const BUFFER_SIZE: usize = 1024 * 1024;

type Token = [u8; 32];
type Side = [u8; 8];

// Represents the type of handshake received
#[derive(Debug, PartialEq)]
enum HandshakeType {
    Legacy { token: Token },
    Modern { token: Token, side: Side },
}

impl HandshakeType {
    fn get_token(&self) -> &Token {
        match self {
            HandshakeType::Legacy { token } => token,
            HandshakeType::Modern { token, .. } => token,
        }
    }

    fn get_side(&self) -> Option<&Side> {
        match self {
            HandshakeType::Legacy { .. } => None,
            HandshakeType::Modern { token: _, side } => Some(side),
        }
    }
}

impl fmt::Display for HandshakeType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            HandshakeType::Legacy { token } => {
                write!(f, "Legacy(token={})", hex::encode(token))
            }
            HandshakeType::Modern { token, side } => {
                write!(
                    f,
                    "Modern(token={}, side={})",
                    hex::encode(token),
                    hex::encode(side)
                )
            }
        }
    }
}

// Main struct representing a connection
#[derive(Debug)]
struct Connection {
    stream: Arc<Mutex<TcpStream>>,
    socket: SocketAddr,
    handshake: HandshakeType,
}

impl Connection {
    fn new(stream: TcpStream, socket: SocketAddr, handshake: HandshakeType) -> Self {
        let stream = Arc::new(Mutex::new(stream));
        Connection {
            stream,
            socket,
            handshake,
        }
    }

    async fn handle_handshake(
        mut stream: TcpStream,
        socket: SocketAddr,
    ) -> anyhow::Result<Connection> {
        let regex_lagacy = Regex::new(r"^please relay (\w{64})$").unwrap();
        let regex_current = Regex::new(r"^please relay (\w{64}) for side (\w{16})$").unwrap();

        let mut buf = BytesMut::with_capacity(128).limit(128);

        let line = loop {
            if buf.remaining_mut() == 0 {
                bail!("Buffer is full without finding EOL");
            }

            stream.read_buf(&mut buf).await?;

            if let Some(pos) = buf.get_ref().iter().position(|b| b == &b'\n') {
                let limited = buf
                    .get_ref()
                    .get(..pos)
                    .ok_or(anyhow!("Buffer out of bounds"))?;
                break std::str::from_utf8(limited)?;
            }
        };

        if let Some(cap) = regex_lagacy.captures(line) {
            let token = cap.get(1).ok_or(anyhow!("No lagacy token"))?.as_str();

            let token = hex::decode(token)?
                .try_into()
                .map_err(|_| anyhow!("Unexpected byte in token"))?;

            let handshake = HandshakeType::Legacy { token };
            Ok(Self::new(stream, socket, handshake))
        } else if let Some(cap) = regex_current.captures(line) {
            let token = cap.get(1).ok_or(anyhow!("No token"))?.as_str();

            let token = hex::decode(token)?
                .try_into()
                .map_err(|_| anyhow!("Unexpected byte in token"))?;

            let side = cap.get(2).ok_or(anyhow!("No side"))?.as_str();

            let side = hex::decode(side)?
                .try_into()
                .map_err(|_| anyhow!("Unexpected byte in side"))?;

            let handshake = HandshakeType::Modern { token, side };
            Ok(Self::new(stream, socket, handshake))
        } else {
            bail!("No valid handshake");
        }
    }
}

#[derive(Debug)]
struct Pending {
    peers: RwLock<MultiMap<Token, Arc<Connection>>>,
}

impl Pending {
    fn new() -> Self {
        Pending {
            peers: RwLock::new(MultiMap::new()),
        }
    }

    async fn add(&self, conn: Arc<Connection>) {
        let mut peers = self.peers.write().await;
        let token = conn.handshake.get_token();
        peers.insert(*token, conn);
    }

    async fn find_match_and_remove(&self, other_peer: Arc<Connection>) -> Option<Arc<Connection>> {
        let mut peers = self.peers.write().await;
        let token = other_peer.handshake.get_token();
        if let Some(peer_list) = peers.remove(token) {
            debug!("Peers removed: {:?} ({} remaining)", peer_list, peers.len());
            for peer in peer_list {
                let side = peer.handshake.get_side();
                let other_side = other_peer.handshake.get_side();
                match (side, other_side) {
                    (None, _) | (_, None) => return Some(peer),
                    (Some(s1), Some(s2)) if s1 != s2 => return Some(peer),
                    _ => {}
                }
            }
        }
        None
    }

    async fn remove(&self, peer: Arc<Connection>) {
        let mut peers = self.peers.write().await;
        let token = peer.handshake.get_token();
        let removed = peers.remove(token);
        debug!("Peers removed: {:?} ({} remaining)", removed, peers.len());
    }
}

#[derive(Debug)]
struct TransitRelay {
    pending: Pending,
}

impl TransitRelay {
    fn new() -> Self {
        TransitRelay {
            pending: Pending::new(),
        }
    }

    async fn handle_connection(&self, conn: Arc<Connection>) -> anyhow::Result<()> {
        let addr = format!("{}:{}", conn.socket.ip(), conn.socket.port());
        debug!("Peer connected: {}", &addr);

        if let Some(partner) = self.pending.find_match_and_remove(conn.clone()).await {
            // Partner found - notify them and start relay
            info!(
                "Peers matched: {} {} <=> {} {}",
                addr,
                conn.handshake,
                partner.handshake,
                format!("{}:{}", partner.socket.ip(), partner.socket.port())
            );

            Self::tunnel(conn.stream.clone(), partner.stream.clone()).await?;
            info!("Tunnel closed")
        } else {
            // No partner yet - add to pending and wait
            self.pending.add(conn.clone()).await;
            debug!("Peer {} added to list", addr);
            sleep(Duration::from_secs(10)).await;
        }

        Ok(())
    }

    async fn tunnel(s1: Arc<Mutex<TcpStream>>, s2: Arc<Mutex<TcpStream>>) -> anyhow::Result<()> {
        let mut s1 = s1.lock().await;
        let mut s2 = s2.lock().await;

        // Wait for sockets to be readable
        for s in [&mut s1, &mut s2] {
            s.ready(Interest::READABLE).await?;
        }

        // Check for premature data
        if s1.try_read(&mut [0u8; 1])?.max(s2.try_read(&mut [0u8; 1])?) > 0 {
            for s in [&mut s1, &mut s2] {
                let _ = s.write_all(b"impatient\n").await;
            }
            bail!("Peer impatient");
        }

        for s in [&mut s1, &mut s2] {
            s.write_all(b"ok\n").await?;
        }

        debug!("Ready flags sent, starting relay");

        tokio::io::copy_bidirectional_with_sizes(&mut *s1, &mut *s2, BUFFER_SIZE, BUFFER_SIZE)
            .await?;

        Ok(())
    }
}

async fn create_connection(
    relay: Arc<TransitRelay>,
    stream: TcpStream,
    socket: SocketAddr,
) -> anyhow::Result<()> {
    let conn = Connection::handle_handshake(stream, socket).await?;
    let conn = Arc::new(conn);

    let addr = format!("{}:{}", conn.socket.ip(), conn.socket.port());

    if let Err(e) = relay.handle_connection(conn.clone()).await {
        info!("Connection error: {} ({})", e, &addr);
    }

    relay.pending.remove(conn).await;

    Ok(())
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[clap(long, value_parser = parse_socket_addrs, default_values = ["0.0.0.0:4001", "[::]:4001"])]
    listen: Vec<SocketAddr>,
}

fn parse_socket_addrs(s: &str) -> Result<SocketAddr, std::net::AddrParseError> {
    s.parse::<SocketAddr>()
}

// main.rs
#[tokio::main]
async fn main() {
    let args = Args::parse();
    Builder::from_default_env().init();

    let relay = Arc::new(TransitRelay::new());

    let mut handles = Vec::new();
    for addr in args.listen {
        let relay_clone = relay.clone();
        handles.push(tokio::spawn(listen_on(addr, relay_clone)));
    }

    futures::future::join_all(handles).await;
}

async fn listen_on(addr: SocketAddr, relay: Arc<TransitRelay>) {
    let addr = format!("{}:{}", addr.ip(), addr.port());

    let listener = TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("Failed to bind to {}: {}", addr, e));

    info!("Listening on {}", addr);

    loop {
        let (stream, socket) = listener.accept().await.expect("Failed to listen");
        stream.set_nodelay(true).unwrap();

        let relay_clone = relay.clone();
        tokio::spawn(async move {
            let addr = format!("{}:{}", socket.ip(), socket.port());
            if let Err(e) = create_connection(relay_clone, stream, socket).await {
                info!("Connection error ({}): {}", &addr, e);
            }
        });
    }
}
