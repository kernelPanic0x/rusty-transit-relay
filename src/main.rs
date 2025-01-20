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
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::sleep;

const BUFFER_SIZE: usize = 1024 * 1024;
const PEER_TIMEOUT: Duration = Duration::from_secs(5);
const GC_INTERVAL: Duration = Duration::from_secs(10);

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
    stream: TcpStream,
    socket: SocketAddr,
    handshake: HandshakeType,
    timestamp: SystemTime,
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
    peers: Mutex<MultiMap<Token, Connection>>,
}

impl Pending {
    fn new() -> Self {
        Pending {
            peers: Mutex::new(MultiMap::new()),
        }
    }

    async fn add(&self, conn: Connection) {
        let mut peers = self.peers.lock().await;
        let token = conn.handshake.get_token();
        peers.insert(*token, conn);
    }

    async fn find_match_and_remove(&self, peer: &Connection) -> Option<Connection> {
        let mut peers = self.peers.lock().await;
        let token = peer.handshake.get_token();

        if let Some(peer_list) = peers.remove(token) {
            debug!("Peers removed: {:?} ({} remaining)", peer_list, peers.len());

            for other_peer in peer_list {
                let side = peer.handshake.get_side();
                let other_side = other_peer.handshake.get_side();

                match (side, other_side) {
                    (None, _) | (_, None) => return Some(other_peer),
                    (Some(s1), Some(s2)) if s1 != s2 => return Some(other_peer),
                    _ => {}
                }
            }
        }

        None
    }

    async fn collect_garbage(&self) {
        let mut peers = self.peers.lock().await;

        let now = SystemTime::now();

        let dead = peers
            .iter()
            .filter(|(_, conn)| {
                now.duration_since(conn.timestamp).expect("Timestamp early") > PEER_TIMEOUT
            })
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();

        let n = dead.len();
        if n > 0 {
            debug!("{} dead peers found", n);
        }

        for peer_key in dead {
            if let Some(c) = peers.remove(&peer_key) {
                debug!("Peers removed: {:?} ({} remaining)", c, peers.len());
            }
        }
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

    async fn handle_connection(&self, conn: Connection) -> anyhow::Result<()> {
        let addr = format!("{}:{}", conn.socket.ip(), conn.socket.port());
        debug!("Peer connected: {}", &addr);

        if let Some(other_peer) = self.pending.find_match_and_remove(&conn).await {
            // Partner found - notify them and start relay
            info!(
                "Peers matched: {} {} <=> {} {}",
                addr,
                &conn.handshake,
                &other_peer.handshake,
                format!("{}:{}", other_peer.socket.ip(), other_peer.socket.port())
            );

            Self::tunnel(conn.stream, other_peer.stream).await?;
            info!("Tunnel closed")
        } else {
            // No partner yet - add to pending and wait
            self.pending.add(conn).await;
            debug!("Peer {} added to list", addr);
        }

        Ok(())
    }

    async fn tunnel(mut s1: TcpStream, mut s2: TcpStream) -> anyhow::Result<()> {
        // Check for premature data
        if [&mut s1, &mut s2]
            .iter()
            .map(|s| s.try_read(&mut [0u8; 1]))
            .any(|r| r.unwrap_or_default() > 0)
        {
            for s in [&mut s1, &mut s2] {
                let _ = s.write_all(b"impatient\n").await;
            }
            bail!("Peer is impatient");
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
) -> anyhow::Result<()> {
    let conn = Connection::handle_handshake(stream, socket).await?;

    let addr = format!("{}:{}", conn.socket.ip(), conn.socket.port());

    if let Err(e) = relay.handle_connection(conn).await {
        info!("Connection error: {} ({})", e, &addr);
    }

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

    // Garbage collection job
    handles.push(tokio::spawn(async move {
        loop {
            relay.pending.collect_garbage().await;
            sleep(GC_INTERVAL).await;
        }
    }));

    let _ = futures::future::select_all(handles).await;
    unreachable!("Critical process exited")
}

fn format_ip_port(addr: &SocketAddr) -> String {
    if addr.is_ipv6() {
        format!("[{}]:{}", addr.ip(), addr.port())
    } else {
        format!("{}:{}", addr.ip(), addr.port())
    }
}

async fn listen_on(addr: SocketAddr, relay: Arc<TransitRelay>) {
    let addr = format_ip_port(&addr);
    let listener = TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("Failed to bind to {}: {}", addr, e));

    info!("Listening on {}", addr);

    loop {
        let (stream, socket) = listener.accept().await.expect("Failed to listen");
        stream.set_nodelay(true).expect("Set socket no delay");

        let relay_clone = relay.clone();
        tokio::spawn(async move {
            let addr = format_ip_port(&socket);
            if let Err(e) = create_connection(relay_clone, stream, socket).await {
                info!("Connection error ({}): {}", &addr, e);
            }
        });
    }
}
