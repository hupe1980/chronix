use std::{collections::HashSet, net::SocketAddr, process::Stdio, time::Duration};

use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::test]
async fn startup_reports_reachable_ephemeral_listeners() {
    chronixd::tls::ensure_crypto_provider();
    let dir = tempfile::tempdir().unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_chronixd"))
        .arg("--data-dir")
        .arg(dir.path())
        .args([
            "--bind",
            "127.0.0.1:0",
            "--grpc-bind",
            "127.0.0.1:0",
            "--flight-bind",
            "127.0.0.1:0",
            "--log-format",
            "json",
        ])
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    tokio::time::timeout(Duration::from_secs(30), async {
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let fields = loop {
            let line = lines.next_line().await.unwrap().expect("startup record");
            if let Ok(record) = serde_json::from_str::<serde_json::Value>(&line)
                && record["fields"]["message"] == "chronixd started"
            {
                break record["fields"].clone();
            }
        };
        let mut ports = HashSet::new();
        for protocol in ["http", "grpc", "flight_sql"] {
            let addr: SocketAddr = fields[protocol].as_str().unwrap().parse().unwrap();
            assert!(addr.ip().is_loopback());
            assert_ne!(addr.port(), 0);
            assert!(ports.insert(addr.port()));
            if protocol == "http" {
                let ready = reqwest::get(format!("http://{addr}/ready")).await.unwrap();
                assert!(ready.status().is_success());
            } else {
                tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                    .unwrap()
                    .connect()
                    .await
                    .unwrap();
            }
        }
    })
    .await
    .expect("server startup timed out");
    child.kill().await.unwrap();
}
