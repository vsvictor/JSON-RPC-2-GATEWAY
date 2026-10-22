//! Bounded native-SRT UDP relay. Only the shared ingress reads the public socket.

use std::{
    collections::HashMap,
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use json_rpc_2_transport::{CancellationToken, TransportRuntime, log_transport_error};
use tokio::{
    net::UdpSocket,
    sync::{Mutex as AsyncMutex, mpsc},
    task::JoinSet,
    time::Instant,
};

const QUEUE_PACKETS: usize = 64;
const MAX_PACKET_BYTES: usize = 65_507;
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

struct Peer {
    sender: mpsc::Sender<Bytes>,
    last_inbound: Instant,
    generation: Arc<()>,
    relay_local: SocketAddr,
}

#[derive(Default)]
struct State {
    peers: HashMap<SocketAddr, Peer>,
    /// Relay loopback socket -> external peer; same bound as `peers`.
    external: HashMap<SocketAddr, SocketAddr>,
    tasks: JoinSet<()>,
    closed: bool,
}

/// Each admitted external endpoint owns one ephemeral loopback UDP socket and
/// one task. Packets are forwarded intact; libsrt still owns native handshakes,
/// encryption, keepalives, retransmission and application session lifecycle.
///
/// Call `close` after the private SRT listener has drained, while public ingress
/// is still forwarding packets. Cancellation/drop stops relaying immediately;
/// only explicit `close` joins tasks.
pub struct SrtUdpRelay {
    public: Arc<UdpSocket>,
    target: SocketAddr,
    runtime: Arc<TransportRuntime>,
    stop: CancellationToken,
    state: Arc<Mutex<State>>,
    admission: AsyncMutex<()>,
}

impl SrtUdpRelay {
    pub fn new(
        public: Arc<UdpSocket>,
        private_listener: SocketAddr,
        runtime: Arc<TransportRuntime>,
        shutdown: CancellationToken,
    ) -> io::Result<Self> {
        if !private_listener.ip().is_loopback() || private_listener.port() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SRT relay requires a bound loopback native listener",
            ));
        }
        if runtime.limits.max_connections == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SRT relay max_connections must be positive",
            ));
        }
        public.local_addr()?;
        Ok(Self {
            public,
            target: private_listener,
            runtime,
            stop: shutdown.child_token(),
            state: Arc::new(Mutex::new(State::default())),
            admission: AsyncMutex::new(()),
        })
    }

    /// Resolve the native listener's accepted peer (a relay loopback socket)
    /// to the external UDP peer it carries. Mappings exist only while the relay
    /// peer is admitted; eviction/close removes them.
    pub fn external_peer(&self, relay_peer: SocketAddr) -> Option<SocketAddr> {
        self.state
            .lock()
            .unwrap()
            .external
            .get(&relay_peer)
            .copied()
    }

    pub fn owns_peer(&self, source: SocketAddr) -> bool {
        !self.stop.is_cancelled() && self.state.lock().unwrap().peers.contains_key(&source)
    }

    /// Recognize a native caller's initial UDT/SRT induction, not merely the
    /// control bit (also set by RTP and some QUIC packets), or a JSRT marker.
    /// libsrt 1.5 callers initiate with a 64-byte version-4 induction; version-5
    /// negotiation and encrypted extensions follow through the admitted relay.
    pub fn is_handshake(packet: &[u8]) -> bool {
        if packet.len() != 64 || packet[..8] != [0x80, 0, 0, 0, 0, 0, 0, 0] {
            return false;
        }
        let word = |offset| u32::from_be_bytes(packet[offset..offset + 4].try_into().unwrap());
        word(12) == 0 // Initial destination socket ID.
            && word(16) == 4 // Native induction wire version.
            && word(20) == 2 // UDT datagram socket.
            && word(24) <= 0x7fff_ffff
            && (576..=65_535).contains(&word(28)) // Advertised MTU.
            && word(32) >= 2 // Flow window.
            && word(36) == 1 // Induction, not a conclusion/response.
            && (1..=0x7fff_ffff).contains(&word(40)) // Caller socket ID.
            && word(44) == 0 // No induction cookie yet.
    }

    /// Queue one intact UDP packet without blocking the sole public reader on a
    /// slow peer. Full queues explicitly return WouldBlock; native SRT may retry
    /// lost transport packets, but this relay never replays application RPCs.
    pub async fn forward(&self, packet: Bytes, source: SocketAddr) -> io::Result<()> {
        let result = self.forward_inner(packet, source).await;
        if let Err(error) = &result {
            log_transport_error("SRT relay", source, "forward", error);
        }
        result
    }

    async fn forward_inner(&self, packet: Bytes, source: SocketAddr) -> io::Result<()> {
        if packet.is_empty() || packet.len() > MAX_PACKET_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid SRT UDP packet size",
            ));
        }
        {
            let mut state = self.state.lock().unwrap();
            Self::check_open(&state, &self.stop)?;
            if let Some(peer) = state.peers.get_mut(&source) {
                return Self::enqueue(peer, packet);
            }
        }
        if !Self::is_handshake(&packet) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "new SRT relay peer must send a native induction handshake",
            ));
        }
        // Serialize admission across awaits; concurrent callers cannot exceed
        // max_connections with in-progress socket allocations.
        let _admission = self.admission.lock().await;
        {
            let mut state = self.state.lock().unwrap();
            Self::check_open(&state, &self.stop)?;
            if let Some(peer) = state.peers.get_mut(&source) {
                return Self::enqueue(peer, packet);
            }
            while let Some(result) = state.tasks.try_join_next() {
                if let Err(error) = result {
                    log_transport_error("SRT relay", source, "join", &error);
                }
            }
            if state.tasks.len() >= self.runtime.limits.max_connections {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "SRT relay peer limit reached",
                ));
            }
        }
        let loopback = match self.target {
            SocketAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
            SocketAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
        };
        let socket = UdpSocket::bind(SocketAddr::new(loopback, 0)).await?;
        socket.connect(self.target).await?;
        let relay_local = socket.local_addr()?;
        let (sender, receiver) = mpsc::channel(QUEUE_PACKETS);
        let generation = Arc::new(());
        let mut peer = Peer {
            sender,
            last_inbound: Instant::now(),
            generation: generation.clone(),
            relay_local,
        };
        Self::enqueue(&mut peer, packet)?;
        let mut state = self.state.lock().unwrap();
        Self::check_open(&state, &self.stop)?;
        // Insert before the task can send the induction to the private listener.
        state.external.insert(relay_local, source);
        state.peers.insert(source, peer);
        let weak_state = Arc::downgrade(&self.state);
        let stop = self.stop.clone();
        let public = self.public.clone();
        state.tasks.spawn(async move {
            let result =
                Self::serve_peer(socket, receiver, public, source, stop, &weak_state).await;
            if let Err(error) = result {
                log_transport_error("SRT relay", source, "peer", &error);
            }
            if let Some(state) = weak_state.upgrade() {
                let mut state = state.lock().unwrap();
                if state
                    .peers
                    .get(&source)
                    .is_some_and(|peer| Arc::ptr_eq(&peer.generation, &generation))
                {
                    Self::remove_peer(&mut state, source);
                }
            }
        });
        Ok(())
    }

    fn remove_peer(state: &mut State, source: SocketAddr) {
        if let Some(peer) = state.peers.remove(&source) {
            state.external.remove(&peer.relay_local);
        }
    }

    fn check_open(state: &State, stop: &CancellationToken) -> io::Result<()> {
        if state.closed || stop.is_cancelled() {
            Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "SRT relay closed",
            ))
        } else {
            Ok(())
        }
    }

    fn enqueue(peer: &mut Peer, packet: Bytes) -> io::Result<()> {
        peer.sender.try_send(packet).map_err(|error| {
            let kind = match error {
                mpsc::error::TrySendError::Full(_) => io::ErrorKind::WouldBlock,
                mpsc::error::TrySendError::Closed(_) => io::ErrorKind::NotConnected,
            };
            io::Error::new(kind, "SRT relay peer queue unavailable")
        })?;
        peer.last_inbound = Instant::now();
        Ok(())
    }

    async fn serve_peer(
        socket: UdpSocket,
        mut receiver: mpsc::Receiver<Bytes>,
        public: Arc<UdpSocket>,
        source: SocketAddr,
        stop: CancellationToken,
        state: &std::sync::Weak<Mutex<State>>,
    ) -> io::Result<()> {
        let mut buffer = vec![0; 65_535];
        let mut last_outbound = Instant::now();
        let idle = tokio::time::sleep(IDLE_TIMEOUT);
        tokio::pin!(idle);
        loop {
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                _ = &mut idle => {
                    let Some(state) = state.upgrade() else { return Ok(()) };
                    let mut state = state.lock().unwrap();
                    let Some(peer) = state.peers.get(&source) else { return Ok(()) };
                    let deadline = peer.last_inbound.max(last_outbound) + IDLE_TIMEOUT;
                    if Instant::now() >= deadline {
                        // Remove atomically with the expiry check, so a freshly
                        // queued keepalive cannot race eviction.
                        Self::remove_peer(&mut state, source);
                        return Ok(());
                    }
                    idle.as_mut().reset(deadline);
                }
                packet = receiver.recv() => {
                    let Some(packet) = packet else { return Ok(()) };
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => return Ok(()),
                        result = socket.send(&packet) => {
                            if result? != packet.len() {
                                return Err(io::Error::new(io::ErrorKind::WriteZero, "short private SRT datagram send"));
                            }
                        }
                    }
                }
                result = socket.recv(&mut buffer) => {
                    let length = result?;
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => return Ok(()),
                        result = public.send_to(&buffer[..length], source) => {
                            if result? != length {
                                return Err(io::Error::new(io::ErrorKind::WriteZero, "short public SRT datagram send"));
                            }
                        }
                    }
                    last_outbound = Instant::now();
                }
            }
        }
    }

    /// Cancel, close all peer sockets, and join every task within close_timeout.
    /// Timeout aborts and joins the remaining tasks and returns an explicit error.
    pub async fn close(&self) -> io::Result<()> {
        let _admission = self.admission.lock().await;
        self.stop.cancel();
        let mut tasks = {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            state.peers.clear();
            state.external.clear();
            std::mem::take(&mut state.tasks)
        };
        let result = tokio::time::timeout(self.runtime.close_timeout(), async {
            let mut failure = None;
            while let Some(result) = tasks.join_next().await {
                if let Err(error) = result {
                    log_transport_error("SRT relay", self.target, "join", &error);
                    failure = Some(io::Error::other(error));
                }
            }
            failure.map_or(Ok(()), Err)
        })
        .await;
        match result {
            Ok(result) => result,
            Err(_) => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                let error = io::Error::new(io::ErrorKind::TimedOut, "SRT relay close timed out");
                log_transport_error("SRT relay", self.target, "close", &error);
                Err(error)
            }
        }
    }
}

impl Drop for SrtUdpRelay {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
