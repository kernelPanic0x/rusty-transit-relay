use anyhow::{anyhow, bail};
use bytes::{BufMut, BytesMut};
use clap::Parser;
use env_logger::Builder;
use futures::future::Either;
use futures::FutureExt;
use log::{debug, info};
use multimap::MultiMap;
use regex::Regex;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;

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

// Main struct representing a connection
#[derive(Debug)]
struct Connection {
    stream: Arc<Mutex<TcpStream>>,
    handshake: HandshakeType,
}

impl Connection {
    fn new(stream: TcpStream, handshake: HandshakeType) -> Self {
        let stream = Arc::new(Mutex::new(stream));
        Connection { stream, handshake }
    }

    async fn handle_handshake(mut stream: TcpStream) -> anyhow::Result<Connection> {
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

        if let Some(cap) = regex_lagacy.captures(&line) {
            let token = cap.get(1).ok_or(anyhow!("No lagacy token"))?.as_str();

            let token = hex::decode(token)?
                .try_into()
                .map_err(|_| anyhow!("Unexpected byte in token"))?;

            let handshake = HandshakeType::Legacy { token };
            Ok(Self::new(stream, handshake))
        } else if let Some(cap) = regex_current.captures(&line) {
            let token = cap.get(1).ok_or(anyhow!("No token"))?.as_str();

            let token = hex::decode(token)?
                .try_into()
                .map_err(|_| anyhow!("Unexpected byte in token"))?;

            let side = cap.get(2).ok_or(anyhow!("No side"))?.as_str();

            let side = hex::decode(side)?
                .try_into()
                .map_err(|_| anyhow!("Unexpected byte in side"))?;

            let handshake = HandshakeType::Modern { token, side };
            Ok(Self::new(stream, handshake))
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
        peers.remove(token);
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
        debug!("Peer connected: {:?}", conn.handshake);

        if let Some(partner) = self.pending.find_match_and_remove(conn.clone()).await {
            // Partner found - notify them and start relay
            info!(
                "Peers matched: {:?} <-> {:?}",
                conn.handshake, partner.handshake
            );

            Self::tunnel(conn.stream.clone(), partner.stream.clone()).await?;
        } else {
            // No partner yet - add to pending and wait
            self.pending.add(conn.clone()).await;
            debug!("Peer added to list");
            sleep(Duration::from_secs(10)).await;
        }

        Ok(())
    }

    async fn tunnel(s1: Arc<Mutex<TcpStream>>, s2: Arc<Mutex<TcpStream>>) -> anyhow::Result<()> {
        let mut s1 = s1.lock().await;
        let mut s2 = s2.lock().await;

        // Check for premature data
        let mut check_buf = [0u8; 64];
        if let Some(n) = s1.try_read(&mut check_buf).ok() {
            if n > 0 {
                log::debug!("S1 sent {} bytes before ok flag: {:?}", n, &check_buf[..n]);
                s1.write_all(b"impatient\n").await?;
                s2.write_all(b"impatient\n").await?;
                bail!("Peer impatient");
            }
        }
        if let Some(n) = s2.try_read(&mut check_buf).ok() {
            if n > 0 {
                log::debug!("S2 sent {} bytes before ok flag: {:?}", n, &check_buf[..n]);
                s1.write_all(b"impatient\n").await?;
                s2.write_all(b"impatient\n").await?;
                bail!("Peer impatient");
            }
        }

        s1.write_all(b"ok\n").await?;
        s2.write_all(b"ok\n").await?;
        debug!("Ready flags sent, starting relay");

        let (s1_read, s1_write) = s1.split();
        let (s2_read, s2_write) = s2.split();

        let fut1 = pin!(Self::transfer(s1_read, s2_write).fuse());
        let fur2 = pin!(Self::transfer(s2_read, s1_write).fuse());

        match futures::future::select(fut1, fur2).await {
            Either::Left((Err(e), _)) | Either::Right((Err(e), _)) => Err(e),
            _ => Ok(()),
        }
    }

    async fn transfer(mut read: ReadHalf<'_>, mut write: WriteHalf<'_>) -> anyhow::Result<()> {
        let mut buf = [0u8; 8192];
        loop {
            match read.read(&mut buf).await {
                Ok(0) => {
                    break;
                }
                Ok(n) => {
                    write.write_all(&buf[..n]).await?;
                }
                Err(e) => {
                    Err(e)?;
                }
            }
        }
        Ok(())
    }
}

async fn create_connection(relay: Arc<TransitRelay>, stream: TcpStream) -> anyhow::Result<()> {
    let conn = Connection::handle_handshake(stream).await?;
    let conn = Arc::new(conn);

    if let Err(e) = relay.handle_connection(conn.clone()).await {
        info!("Connection error: {}", e);
    }

    relay.pending.remove(conn).await;

    Ok(())
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[clap(long, value_parser = parse_socket_addrs)]
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
        handles.push(tokio::spawn(
            async move { listen_on(addr, relay_clone).await },
        ));
    }

    futures::future::join_all(handles).await;
}

async fn listen_on(addr: SocketAddr, relay: Arc<TransitRelay>) {
    let addr = format!("{}:{}", addr.ip(), addr.port());

    let listener = TcpListener::bind(&addr)
        .await
        .expect(&format!("Failed to bind to {}", addr));

    debug!("Listening on {}", addr);

    loop {
        let (stream, _stocket) = listener.accept().await.expect("Failed to listen");

        let relay_clone = relay.clone();
        tokio::spawn(async move {
            if let Err(e) = create_connection(relay_clone, stream).await {
                info!("Connection error: {}", e);
            }
        });
    }
}
