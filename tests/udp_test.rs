use rand_core::OsRng;
use reticulum::{
    identity::PrivateIdentity,
    iface::udp::UdpInterface,
    packet::Packet,
    transport::{Transport, TransportConfig},
};
use tokio_util::sync::CancellationToken;

async fn build_transport(name: &str, bind_addr: &str, forward_addr: Option<&str>) -> Transport {
    let transport = Transport::new(TransportConfig::new(
        name,
        &PrivateIdentity::new_from_rand(OsRng),
        true,
    ));

    transport.iface_manager().lock().await.spawn(
        UdpInterface::new(bind_addr, forward_addr),
        UdpInterface::spawn,
    );

    log::info!("test: transport {} created", name);

    transport
}

#[tokio::test]
async fn udp_unicast() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let transport_a = build_transport("a", "127.0.0.1:8081", Some("127.0.0.1:8082")).await;
    let transport_b = build_transport("b", "127.0.0.1:8082", Some("127.0.0.1:8081")).await;

    let stop = CancellationToken::new();

    let producer_task = {
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut tx_counter = 0;
            let mut payload_size = 0;
            let mut packet = Packet::default();
            packet.data.resize(payload_size);
            loop {
                tokio::select! {
                    _ = stop.cancelled() => {
                        break;
                    },
                    _ = transport_a.send_packet(packet) => {
                        tx_counter += 1;

                        // Throttle the transmitted packets to something tolerable.
                        tokio::time::sleep(std::time::Duration::from_micros(10)).await;

                        payload_size += 1;
                        if payload_size >= 3072 {
                            payload_size = 0;
                        }
                        packet.data.resize(payload_size);
                    },
                };
                if tx_counter == 2000 {
                    break;
                }
            }
            return tx_counter;
        })
    };

    let consumer_task = {
        let stop = stop.clone();
        let mut messages = transport_b.iface_rx();
        tokio::spawn(async move {
            let mut rx_counter = 0;
            loop {
                tokio::select! {
                    _ = stop.cancelled() => {
                        break;
                    },
                    Ok(_) = messages.recv() => {
                        rx_counter += 1;
                    },
                };
            }
            return rx_counter;
        })
    };

    // FIXME: this terminating condition is not reliable

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    stop.cancel();

    let tx_counter = producer_task.await.unwrap();
    let rx_counter = consumer_task.await.unwrap();

    log::info!("TX: {}, RX: {}", tx_counter, rx_counter);

    assert_eq!(tx_counter, rx_counter);
}
