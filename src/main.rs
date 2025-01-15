use anyhow::{anyhow, bail};
use bytes::{BufMut, BytesMut};
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

type Token = [u8; 32];
type Side = [u8; 8];

// Represents the type of handshake received
#[derive(Debug)]
enum HandshakeType {
    Legacy { token: Token },
    Modern { token: Token, side: Side },
}

// Main struct representing a connection
#[derive(Debug)]
struct Connection {
    stream: TcpStream,
    handshake: Option<HandshakeType>,
    total_sent: usize,
}

impl Connection {
    fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            handshake: None,
            total_sent: 0,
        }
    }

    async fn handle_handshake(&mut self) -> anyhow::Result<()> {
        let regex_lagacy = Regex::new(r"^please relay (\w{64})$").unwrap();
        let regex_current = Regex::new(r"^please relay (\w{64}) for side (\w{16})$").unwrap();

        let mut buf = BytesMut::with_capacity(1024).limit(1024);

        let line = loop {
            if buf.remaining_mut() == 0 {
                bail!("Buffer is full without finding EOL");
            }

            self.stream.read_buf(&mut buf).await?;

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

            self.handshake = Some(HandshakeType::Legacy { token })
        } else if let Some(cap) = regex_current.captures(&line) {
            let token = cap.get(1).ok_or(anyhow!("No token"))?.as_str();

            let token = hex::decode(token)?
                .try_into()
                .map_err(|_| anyhow!("Unexpected byte in token"))?;

            let side = cap.get(2).ok_or(anyhow!("No side"))?.as_str();

            let side = hex::decode(side)?
                .try_into()
                .map_err(|_| anyhow!("Unexpected byte in side"))?;

            self.handshake = Some(HandshakeType::Modern { token, side })
        } else {
            bail!("No valid handshake");
        }

        Ok(())
    }
}

#[derive(Debug)]
struct TransitRelay {
    pending: RwLock<HashMap<HandshakeType, Vec<Connection>>>,
}

impl TransitRelay {
    fn new() -> Self {
        TransitRelay {
            pending: RwLock::new(HashMap::new()),
        }
    }

    async fn handle_connection(&self, stream: TcpStream) -> anyhow::Result<()> {
        // Get handshake type
        let mut conn = Connection::new(stream);
        conn.handle_handshake().await?;

        dbg!(&conn.handshake);

        todo!();

        // let mut pending = self.pending.write().await;

        // // Try to find matching connection
        // if let Some(partner) = Self::find_matching_connection(&mut pending, &conn.handshake_type) {
        //     // Found a match - start relaying
        //     drop(pending); // Release the lock before starting relay
        //     conn.relay_to_partner(partner).await?;
        // } else {
        //     // No match - store as pending
        //     pending
        //         .entry(conn.handshake_type.token())
        //         .or_insert_with(Vec::new)
        //         .push(conn);
        // }

        Ok(())
    }
}

// main.rs
#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:4001").await.unwrap();
    let relay = Arc::new(TransitRelay::new());

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let relay = relay.clone();

        relay.handle_connection(stream).await.unwrap();
        // tokio::spawn(async move {});
    }
}
