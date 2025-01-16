use anyhow::{anyhow, bail};
use bytes::{BufMut, BytesMut};
use env_logger::Builder;
use futures::future::Either;
use futures::FutureExt;
use log::{debug, error, info};
use regex::Regex;
use std::pin::pin;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, OnceCell, RwLock};

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
    handshake: OnceCell<HandshakeType>,
}

impl Connection {
    fn new(stream: TcpStream) -> Self {
        let stream = Arc::new(Mutex::new(stream));
        Connection {
            stream,
            handshake: OnceCell::new(),
        }
    }

    async fn handle_handshake(&mut self) -> anyhow::Result<HandshakeType> {
        let regex_lagacy = Regex::new(r"^please relay (\w{64})$").unwrap();
        let regex_current = Regex::new(r"^please relay (\w{64}) for side (\w{16})$").unwrap();

        let mut stream = self.stream.lock().await;

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

            Ok(HandshakeType::Legacy { token })
        } else if let Some(cap) = regex_current.captures(&line) {
            let token = cap.get(1).ok_or(anyhow!("No token"))?.as_str();

            let token = hex::decode(token)?
                .try_into()
                .map_err(|_| anyhow!("Unexpected byte in token"))?;

            let side = cap.get(2).ok_or(anyhow!("No side"))?.as_str();

            let side = hex::decode(side)?
                .try_into()
                .map_err(|_| anyhow!("Unexpected byte in side"))?;

            Ok(HandshakeType::Modern { token, side })
        } else {
            bail!("No valid handshake");
        }
    }
}

#[derive(Debug)]
struct TransitRelay {
    pending: RwLock<Vec<Arc<Connection>>>,
}

impl TransitRelay {
    fn new() -> Self {
        TransitRelay {
            pending: RwLock::new(Vec::new()),
        }
    }

    async fn handle_connection(&self, stream: TcpStream) -> anyhow::Result<()> {
        let mut conn = Connection::new(stream);

        let handshake = conn.handle_handshake().await?;
        conn.handshake.set(handshake).expect("First init");

        let handshake = conn.handshake.get().expect("Set at connect");
        let token = handshake.get_token();
        let side = handshake.get_side();

        debug!("Peer connected: {:?}", conn.handshake);

        if let Some(partner) = self.find_match(token, side).await {
            // Partner found - notify them and start relay
            self.remove_pending(partner.clone()).await?;

            info!(
                "Peers matched: {:?} <-> {:?}",
                conn.handshake, partner.handshake
            );

            Self::tunnel(conn.stream, partner.stream.clone()).await?;

            // Start relay with partner...
        } else {
            // No partner yet - add to pending and wait
            let mut pending = self.pending.write().await;
            pending.push(Arc::new(conn));
            debug!("Pending len: {}", pending.len())
        }

        Ok(())
    }

    async fn remove_pending(&self, conn: Arc<Connection>) -> anyhow::Result<()> {
        let mut pending = self.pending.write().await;
        let i = pending
            .iter()
            .position(|c| c.handshake == conn.handshake)
            .ok_or(anyhow!("Element not found"))?;
        pending.remove(i);
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
                return Ok(());
            }
        }
        if let Some(n) = s2.try_read(&mut check_buf).ok() {
            if n > 0 {
                log::debug!("S2 sent {} bytes before ok flag: {:?}", n, &check_buf[..n]);
                s1.write_all(b"impatient\n").await?;
                s2.write_all(b"impatient\n").await?;
                return Ok(());
            }
        }

        s1.write_all(b"ok\n").await?;
        s2.write_all(b"ok\n").await?;
        log::debug!("Ready flags sent, starting relay");

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

    async fn find_match(
        &self,
        other_token: &Token,
        other_side: Option<&Side>,
    ) -> Option<Arc<Connection>> {
        let pending = self.pending.read().await;

        pending
            .iter()
            .filter(|c| {
                let token = c.handshake.get().expect("Set at connect").get_token();
                token == other_token
            })
            .find(|c| {
                let side = c.handshake.get().expect("Set at connect").get_side();

                match (other_side, side) {
                    (None, _) | (_, None) => true,
                    (Some(s1), Some(s2)) => *s1 != *s2,
                }
            })
            .map(|conn| conn.clone())
    }
}

// main.rs
#[tokio::main]
async fn main() {
    Builder::from_default_env().init();
    let listener = TcpListener::bind("127.0.0.1:4001")
        .await
        .expect("Failed to bind");
    let relay = Arc::new(TransitRelay::new());

    loop {
        let (stream, _stocket) = listener.accept().await.expect("Failed to listen");

        let relay = relay.clone();

        if let Err(e) = relay.handle_connection(stream).await {
            error!("Relaying failed: {}", e);
        }
    }
}
