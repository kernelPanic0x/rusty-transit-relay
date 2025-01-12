// lib.rs
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

// Core state storing pending and active connections
struct TransitRelay {
    pending_requests: HashMap<Vec<u8>, Vec<(Option<Vec<u8>>, Arc<Mutex<Connection>>)>>,
    active_connections: HashSet<Arc<Mutex<Connection>>>,
}

struct Connection {
    stream: TcpStream,
    token: Option<Vec<u8>>,
    side: Option<Vec<u8>>,
    partner: Option<Arc<Mutex<Connection>>>,
    total_sent: u64,
}

impl TransitRelay {
    fn new() -> Self {
        TransitRelay {
            pending_requests: HashMap::new(),
            active_connections: HashSet::new(),
        }
    }

    async fn handle_connection(&mut self, mut stream: TcpStream) {
        // 1. Read handshake line
        // 2. Parse "please relay TOKEN" or "please relay TOKEN for side SIDE"
        // 3. Store connection in pending_requests or match with partner
        // 4. Start relaying data if matched
    }

    async fn relay_data(conn1: Arc<Mutex<Connection>>, conn2: Arc<Mutex<Connection>>) {
        // Bi-directional forwarding between connections
    }
}

// main.rs
#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:4001").await.unwrap();
    let relay = Arc::new(Mutex::new(TransitRelay::new()));

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let relay = relay.clone();

        tokio::spawn(async move {
            relay.lock().await.handle_connection(socket).await;
        });
    }
}
