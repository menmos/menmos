use std::io;
use std::time::Instant;
use std::{net::SocketAddr, sync::Arc};

use mpsc::{UnboundedReceiver, UnboundedSender};
use resolver::ResolveError;
use snafu::{ResultExt, Snafu};

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinHandle;

use crate::{
    packet::PacketError, packet_buffer::BufferError, query_type::QueryType, record::DnsRecord,
    resolver, BytePacketBuffer, Config, DnsPacket, ResultCode,
};

#[derive(Debug, Snafu)]
pub enum ServerError {
    InvalidBuffer { source: BufferError },
    InvalidPacket { source: PacketError },

    SocketRecvError { source: io::Error },
    SocketSendError { source: io::Error },

    ResolutionError { source: ResolveError },

    JoinError,
    PoisonedMutex,
}

type Result<T> = std::result::Result<T, ServerError>;

type ResponseSender = UnboundedSender<(SocketAddr, Vec<u8>)>;
type ResponseReceiver = UnboundedReceiver<(SocketAddr, Vec<u8>)>;

struct ServerProcess {
    pub join_handle: JoinHandle<()>,
    pub tx_stop: mpsc::Sender<()>,
}

pub struct Server {
    txt_challenge: Arc<Mutex<String>>,
    handle: ServerProcess,
}

impl Server {
    pub fn start(cfg: Config) -> Server {
        let (tx_stop, rx) = mpsc::channel(1);

        tracing::info!("starting DNS layer");

        let txt_challenge = Arc::from(Mutex::from(String::default()));
        let join_handle: JoinHandle<()> = {
            let challenge_cloned = txt_challenge.clone();
            tokio::task::spawn(async move {
                Server::run(cfg, rx, challenge_cloned).await;
            })
        };

        tracing::info!("DNS layer started");

        Server {
            handle: ServerProcess {
                join_handle,
                tx_stop,
            },
            txt_challenge,
        }
    }

    pub async fn set_dns_challenge(&self, challenge: &str) -> Result<()> {
        let mut guard = self.txt_challenge.lock().await;
        *guard = challenge.to_string();
        tracing::info!("set acme challenge: {}", &*guard);
        Ok(())
    }

    async fn handle_query(
        cfg: &Config,
        req_buffer: &mut BytePacketBuffer,
        challenge: Arc<Mutex<String>>,
    ) -> Result<Vec<u8>> {
        // Next, `DnsPacket::from_buffer` is used to parse the raw bytes into
        // a `DnsPacket`.
        let mut request = DnsPacket::from_buffer(req_buffer).context(InvalidPacket)?;

        // Create and initialize the response packet
        let mut packet = DnsPacket::new();
        packet.header.id = request.header.id;
        packet.header.recursion_desired = false;
        packet.header.recursion_available = false;
        packet.header.response = true;

        // In the normal case, exactly one question is present
        if let Some(question) = request.questions.pop() {
            // Handle the special case of a TXT query on our handled domain.

            if question.qtype == QueryType::TXT && question.name.ends_with(&cfg.root_domain) {
                let guard = challenge.lock().await;
                let chall = &*guard.clone();
                if !chall.is_empty() {
                    tracing::info!("query is an ACME challenge");
                    // Resolve the challenge without going to the resolver.
                    packet.questions.push(question);
                    let challenge_bytes = chall.as_bytes().to_vec();
                    packet.answers.push(DnsRecord::TXT {
                        domain_bytes: vec![192, 12],
                        ttl: 500,
                        data_len: challenge_bytes.len() as u16,
                        text: vec![challenge_bytes],
                    });
                    packet.header.authoritative_answer = true;
                } else {
                    tracing::warn!("got ACME challenge but no challenge is set");
                }
            } else {
                match resolver::lookup(&question.name, question.qtype, cfg).await {
                    Ok(Some(result)) => {
                        packet.questions.push(question);
                        packet.header.rescode = result.header.rescode;

                        for rec in result.answers {
                            tracing::debug!("answer: {:?}", rec);
                            packet.answers.push(rec);
                        }
                        for rec in result.authorities {
                            tracing::debug!("authority: {:?}", rec);
                            packet.authorities.push(rec);
                        }
                        for rec in result.resources {
                            tracing::debug!("resource: {:?}", rec);
                            packet.resources.push(rec);
                        }
                    }
                    Ok(None) => {
                        tracing::debug!("ignoring packet");
                    }
                    Err(e) => {
                        tracing::error!("servfail: {}", e);
                        packet.header.rescode = ResultCode::ServFail;
                    }
                }
            }
        }
        // Being mindful of how unreliable input data from arbitrary senders can be, we
        // need make sure that a question is actually present. If not, we return `FORMERR`
        // to indicate that the sender made something wrong.
        else {
            tracing::warn!("FORMERR");
            packet.header.rescode = ResultCode::FormErr;
        }

        // The only thing remaining is to encode our response and send it off!
        let mut res_buffer = BytePacketBuffer::new();
        packet.write(&mut res_buffer).context(InvalidPacket)?;

        let len = res_buffer.pos();
        let data = res_buffer.get_range(0, len).context(InvalidBuffer)?;

        tracing::trace!(
            "sending raw packet of length {} as response: {:?}",
            len,
            data
        );

        // TODO: Instead, take the response buffer as argument as well.
        Ok(data.to_vec())
    }

    async fn run(cfg: Config, mut stop_rx: mpsc::Receiver<()>, challenge: Arc<Mutex<String>>) {
        // Bind to the UDP socket.
        let socket = match UdpSocket::bind(cfg.listen).await {
            Ok(s) => Arc::from(s),
            Err(e) => {
                tracing::error!("cannot bind to socket: {}", e);
                return;
            }
        };

        let (req_tx, mut req_rx) = mpsc::unbounded_channel();
        let (resp_tx, mut resp_rx): (ResponseSender, ResponseReceiver) = mpsc::unbounded_channel();

        let (recv_stop_tx, mut recv_stop_rx) = mpsc::channel(1);
        let (send_stop_tx, mut send_stop_rx) = mpsc::channel(1);

        // Socket read routine.
        let socket_copy = socket.clone();
        let recv_task_handle = tokio::task::spawn(async move {
            loop {
                let mut req_buffer = BytePacketBuffer::new();

                let recv_future = socket_copy.recv_from(&mut req_buffer.buf);
                let abort_future = recv_stop_rx.recv();

                let should_abort = tokio::select! {
                    _ = abort_future => {
                        true
                    }
                    packet_result = recv_future => {
                        match packet_result {
                            Ok((_, addr)) => {
                                if let Err(e) = req_tx.send((addr, req_buffer)) {
                                    tracing::warn!("failed to send request: {}", e);
                                }
                            }
                            Err(e) => {
                                tracing::warn!("packet recv error: {}", e);
                            }
                        };
                        false
                    }
                };

                if should_abort {
                    tracing::info!("quitting receive task");
                    break;
                }
            }
        });

        // Socket write routine.
        let socket_copy = socket.clone();
        let send_task_handle = tokio::task::spawn(async move {
            loop {
                let recv_future = resp_rx.recv();
                let abort_future = send_stop_rx.recv();

                let should_abort = tokio::select! {
                    _ = abort_future => {
                        true
                    }
                    opt_response = recv_future => {
                        match opt_response {
                            Some((socket_addr, resp_data)) => {
                                if let Err(e) = socket_copy.send_to(resp_data.as_ref(), &socket_addr).await {
                                    tracing::warn!("error sending on socket: {}", e);
                                }
                                false
                            }
                            None => {
                                true
                            }

                        }
                    }
                };

                if should_abort {
                    tracing::info!("quitting send task");
                    break;
                }
            }
        });

        // DNS server loop (main task).
        let concurrent_query_sem = Arc::from(Semaphore::new(cfg.nb_of_concurrent_requests));
        loop {
            let abort_future = stop_rx.recv();
            let req_future = req_rx.recv();

            let should_abort = tokio::select! {
                _ = abort_future => {
                    true
                }
                opt_request = req_future => {
                    match opt_request {
                        Some((socket_addr, mut req_buffer)) => {
                            let wait_start = Instant::now();
                            let concurrent_query_permit = concurrent_query_sem.clone().acquire_owned().await;
                            let cloned_cfg = cfg.clone();
                            let cloned_challenge = challenge.clone();
                            let cloned_tx = resp_tx.clone();

                            let wait_duration = Instant::now().duration_since(wait_start);
                            tracing::debug!("started processing packet from {} (waited {}ms)", socket_addr.ip(), wait_duration.as_millis());
                            let _permit_handle = concurrent_query_permit; // 0% useful, except to keep the permit alive until the end of the tokio task.
                            match Server::handle_query(&cloned_cfg, &mut req_buffer, cloned_challenge).await {
                                Ok(data) => {
                                    if let Err(e) = cloned_tx.send((socket_addr, data)) {
                                        tracing::error!("failed to send reply to writer thread: {}", e);
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("uncaught error: {}", e);
                                }
                            }

                            false
                        }
                        None => {
                            true
                        }
                    }
                }
            };

            if should_abort {
                tracing::info!("quitting main task");
                break;
            }
        }

        if let Err(e) = send_stop_tx.send(()).await {
            tracing::error!("failed to stop writer task: {}", e);
        }

        if let Err(e) = recv_stop_tx.send(()).await {
            tracing::error!("failed to stop reader task: {}", e);
        }

        if let Err(e) = tokio::try_join!(recv_task_handle, send_task_handle) {
            tracing::error!("failed to join tasks: {}", e);
        }
    }

    pub async fn stop(self) -> Result<()> {
        tracing::info!("requesting to quit");
        self.handle.tx_stop.send(()).await.unwrap();
        self.handle
            .join_handle
            .await
            .map_err(|_e| ServerError::JoinError)?;
        tracing::info!("exited");
        Ok(())
    }
}
