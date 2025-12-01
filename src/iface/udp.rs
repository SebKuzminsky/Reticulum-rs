use std::os::fd::AsRawFd;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

use crate::buffer::{InputBuffer, OutputBuffer};
use crate::error::RnsError;
use crate::iface::RxMessage;
use crate::packet::Packet;
use crate::serde::Serialize;

use super::{Interface, InterfaceContext};

// TODO: Configure via features
const PACKET_TRACE: bool = true;

pub struct UdpInterface {
    bind_addr: String,
    forward_addr: Option<String>,
}

impl UdpInterface {
    pub fn new<T: Into<String>>(bind_addr: T, forward_addr: Option<T>) -> Self {
        Self {
            bind_addr: bind_addr.into(),
            forward_addr: forward_addr.map(Into::into),
        }
    }

    pub async fn spawn(context: InterfaceContext<Self>) {
        let bind_addr = { context.inner.lock().unwrap().bind_addr.clone() };
        let forward_addr = { context.inner.lock().unwrap().forward_addr.clone() };
        let iface_address = context.channel.address;

        let (rx_channel, tx_channel) = context.channel.split();
        let tx_channel = Arc::new(tokio::sync::Mutex::new(tx_channel));

        loop {
            if context.cancel.is_cancelled() {
                break;
            }

            let socket = match nix::sys::socket::socket(
                nix::sys::socket::AddressFamily::Inet,
                nix::sys::socket::SockType::Datagram,
                nix::sys::socket::SockFlag::empty(),
                nix::sys::socket::SockProtocol::Udp,
            ) {
                Ok(socket) => socket,
                Err(e) => {
                    log::info!("udp_interface: couldn't create udp socket: {e:?}");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            nix::sys::socket::setsockopt(&socket, nix::sys::socket::sockopt::ReuseAddr, &true)
                .unwrap();

            let bind_sockaddr = nix::sys::socket::SockaddrIn::new(239, 0, 0, 69, 4242);
            nix::sys::socket::bind(socket.as_raw_fd(), &bind_sockaddr).unwrap();

            let socket: std::net::UdpSocket = socket.into();
            socket.set_nonblocking(true).unwrap();

            let socket = tokio::net::UdpSocket::from_std(socket).unwrap();

            let cancel = context.cancel.clone();
            let stop = CancellationToken::new();

            if let Some(forward_addr) = &forward_addr {
                // FIXME: this parse should happen much earlier
                let r: Result<std::net::SocketAddr, _> = forward_addr.parse();
                if let Ok(forward_addr) = r {
                    if let std::net::SocketAddr::V4(forward_addr) = forward_addr {
                        if forward_addr.ip().is_multicast() {
                            match socket.join_multicast_v4(
                                *forward_addr.ip(),
                                std::net::Ipv4Addr::UNSPECIFIED,
                            ) {
                                Ok(()) => {
                                    log::info!("joined multicast channel {:?}", forward_addr.ip());
                                }
                                Err(e) => {
                                    log::info!(
                                        "failed to join multicast channel {forward_addr:?}: {e:?}"
                                    );
                                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                    continue;
                                }
                            }
                        }
                    }
                }
            }

            let read_socket = Arc::new(socket);
            let write_socket = read_socket.clone();

            log::info!("udp_interface bound to <{}>", bind_addr);

            const BUFFER_SIZE: usize = core::mem::size_of::<Packet>() * 3;

            // Start receive task
            let rx_task = {
                let cancel = cancel.clone();
                let stop = stop.clone();
                let socket = read_socket;
                let rx_channel = rx_channel.clone();

                tokio::spawn(async move {
                    loop {
                        let mut rx_buffer = [0u8; BUFFER_SIZE];

                        tokio::select! {
                            _ = cancel.cancelled() => {
                                    break;
                            }
                            _ = stop.cancelled() => {
                                    break;
                            }
                            result = socket.recv_from(&mut rx_buffer) => {
                                match result {
                                    Ok((0, _)) => {
                                        log::warn!("udp_interface: connection closed");
                                        stop.cancel();
                                        break;
                                    }
                                    Ok((n, _in_addr)) => {
                                        if let Ok(packet) = Packet::deserialize(&mut InputBuffer::new(&rx_buffer[..n])) {
                                            if PACKET_TRACE {
                                                log::trace!("udp_interface: rx << ({}) {}", iface_address, packet);
                                            }
                                            let _ = rx_channel.send(RxMessage { address: iface_address, packet }).await;
                                        } else {
                                            log::warn!("udp_interface: couldn't decode packet");
                                        }
                                    }
                                    Err(e) => {
                                        log::warn!("udp_interface: connection error {}", e);
                                        break;
                                    }
                                }
                            },
                        };
                    }
                })
            };

            if let Some(forward_addr) = forward_addr.clone() {
                // Start transmit task
                let tx_task = {
                    let cancel = cancel.clone();
                    let tx_channel = tx_channel.clone();
                    let socket = write_socket;

                    tokio::spawn(async move {
                        loop {
                            if stop.is_cancelled() {
                                break;
                            }

                            let mut tx_buffer = [0u8; BUFFER_SIZE];

                            let mut tx_channel = tx_channel.lock().await;

                            tokio::select! {
                                _ = cancel.cancelled() => {
                                        break;
                                }
                                _ = stop.cancelled() => {
                                        break;
                                }
                                Some(message) = tx_channel.recv() => {
                                    let packet = message.packet;
                                    if PACKET_TRACE {
                                        log::trace!("udp_interface: tx >> ({}) {}", iface_address, packet);
                                    }
                                    let mut output = OutputBuffer::new(&mut tx_buffer);
                                    if let Ok(_) = packet.serialize(&mut output) {
                                        let _ = socket.send_to(output.as_slice(), &forward_addr).await;
                                    }
                                }
                            };
                        }
                    })
                };
                tx_task.await.unwrap();
            }

            rx_task.await.unwrap();

            log::info!("udp_interface <{}>: closed", bind_addr);
        }
    }
}

impl Interface for UdpInterface {
    fn mtu() -> usize {
        2048
    }
}
