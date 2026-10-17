//! エクスプローラが実際に HTTP として答えることを確かめる。
//!
//! 索引そのものの正しさは `oag-store` の単体試験が見ている。ここで見るの
//! は**その上に載る層**である。要求を読み、経路を振り分け、索引に問い合わせ、
//! HTML を返すところまでが繋がっているか。
//!
//! 採掘はしない。ジェネシスだけの鎖で足りる。ジェネシスにもコインベースが
//! 1 件あり、それが索引にもアドレス履歴にも現れるからである。RandomX を
//! 積まずに走る。

use oag_node::service::{NodeHandle, NodeService};
use oag_primitives::{Address, Network};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// mainnet のジェネシスを使う。
///
/// regtest のジェネシスは**報酬を発行しない** (出力が 0 個) ので、
/// アドレスの頁に出すものが無い。mainnet のジェネシスは焼却先への出力を
/// 1 件持っており、索引にもアドレス履歴にも現れる。
///
/// 通信は一切始めないので、ここで mainnet を開いても外には何も出ない。
const NETWORK: Network = Network::Mainnet;

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> TempDir {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "oag-explorer-{tag}-{}-{n}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// 1 要求出して、状態行と本文を返す。
async fn get(addr: std::net::SocketAddr, target: &str) -> (String, String) {
    let mut stream = TcpStream::connect(addr).await.expect("it connects");
    let request = format!("GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("it can send");

    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut raw))
        .await
        .expect("a response arrives")
        .expect("readable");

    let text = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = text
        .split_once("\r\n\r\n")
        .expect("headers and body are separated");
    let status = head.lines().next().unwrap_or_default().to_string();
    (status, body.to_string())
}

/// 索引付きでエクスプローラを起こす。
fn start(
    tag: &str,
) -> (
    TempDir,
    NodeService,
    tokio::runtime::Runtime,
    std::net::SocketAddr,
) {
    let dir = TempDir::new(tag);
    let service = NodeService::start(NETWORK, &dir.0).expect("a node can be started");
    let handle = service.handle();
    let runtime = runtime();
    let addr = runtime.block_on(async {
        handle.build_index().await.expect("the index can be built");
        oag_node::explorer::start_explorer(handle.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .expect("it can listen")
    });
    (dir, service, runtime, addr)
}

fn genesis_txid(handle: &NodeHandle, runtime: &tokio::runtime::Runtime) -> String {
    runtime.block_on(async {
        let hash = handle.hash_at_height(0).await.unwrap().unwrap();
        let block = handle.block(hash).await.unwrap().unwrap();
        block.transactions[0].txid().to_string()
    })
}

#[test]
fn the_overview_renders() {
    let (_dir, service, runtime, addr) = start("overview");
    let (status, body) = runtime.block_on(get(addr, "/"));
    assert!(status.contains("200"), "{status}");
    assert!(body.contains("<!doctype html>"));
    assert!(body.contains("Orange"), "the name appears");
    assert!(body.contains("recent blocks"), "the list appears");
    // 索引を作ってから起こしたので「あり」のはずである。
    assert!(body.contains("yes"), "the index status appears");
    drop(service);
}

#[test]
fn a_block_page_shows_its_header_and_transactions() {
    let (_dir, service, runtime, addr) = start("block");
    let (status, body) = runtime.block_on(get(addr, "/block/0"));
    assert!(status.contains("200"), "{status}");
    assert!(body.contains("block 0"));
    assert!(body.contains("merkle root"));
    assert!(body.contains("mined"), "the coinbase marker is shown");
    drop(service);
}

#[test]
fn a_block_can_be_found_by_hash_as_well_as_height() {
    let (_dir, service, runtime, addr) = start("byhash");
    let handle = service.handle();
    let hash = runtime
        .block_on(handle.hash_at_height(0))
        .unwrap()
        .unwrap()
        .to_string();
    let (status, body) = runtime.block_on(get(addr, &format!("/block/{hash}")));
    assert!(status.contains("200"), "{status}");
    assert!(body.contains("block 0"));
    drop(service);
}

#[test]
fn a_transaction_page_is_served_from_the_index() {
    // 索引が無ければ txid から確定した取引は引けない。ここが通るという
    // ことは、索引・位置の解決・ブロックの読み出しが繋がっている。
    let (_dir, service, runtime, addr) = start("tx");
    let txid = genesis_txid(&service.handle(), &runtime);
    let (status, body) = runtime.block_on(get(addr, &format!("/tx/{txid}")));
    assert!(status.contains("200"), "{status}");
    assert!(body.contains("confirmed"));
    assert!(body.contains(&txid));
    assert!(
        body.contains("newly issued by mining"),
        "the coinbase input column"
    );
    drop(service);
}

#[test]
fn an_address_page_shows_the_history_from_the_index() {
    // ジェネシスの報酬は使用不能の支払い条件へ送られる (SPEC §14.4)。
    // アドレスとしては表示でき、履歴にも 1 件出るはずである。
    let (_dir, service, runtime, addr) = start("address");
    let handle = service.handle();
    let text = runtime.block_on(async {
        let hash = handle.hash_at_height(0).await.unwrap().unwrap();
        let block = handle.block(hash).await.unwrap().unwrap();
        block.transactions[0].outputs[0]
            .lock
            .to_address(NETWORK)
            .unwrap()
            .to_string()
    });

    let (status, body) = runtime.block_on(get(addr, &format!("/address/{text}")));
    assert!(status.contains("200"), "{status}");
    assert!(body.contains("history"));
    assert!(body.contains("+"), "shown as a receipt");
    assert!(
        !body.contains("no index is held, so"),
        "the index is present"
    );
    drop(service);
}

#[test]
fn searching_routes_by_the_shape_of_the_input() {
    let (_dir, service, runtime, addr) = start("search");
    let txid = genesis_txid(&service.handle(), &runtime);

    // 数字は高さ。
    let (status, body) = runtime.block_on(get(addr, "/search?q=0"));
    assert!(status.contains("200"), "{status}");
    assert!(body.contains("block 0"));

    // 64 文字の 16 進で、ブロックではないものは取引として引く。
    let (status, body) = runtime.block_on(get(addr, &format!("/search?q={txid}")));
    assert!(status.contains("200"), "{status}");
    assert!(body.contains("confirmed"));
    drop(service);
}

#[test]
fn unknown_things_are_refused_rather_than_shown_empty() {
    let (_dir, service, runtime, addr) = start("notfound");

    let (status, _) = runtime.block_on(get(addr, "/block/99999"));
    assert!(status.contains("404"), "{status}");

    let zero = "0".repeat(64);
    let (status, _) = runtime.block_on(get(addr, &format!("/tx/{zero}")));
    assert!(status.contains("404"), "{status}");

    let (status, _) = runtime.block_on(get(addr, "/address/oag1qqqqqq"));
    assert!(status.contains("404"), "{status}");

    let (status, _) = runtime.block_on(get(addr, "/no-such-page"));
    assert!(status.contains("404"), "{status}");
    drop(service);
}

#[test]
fn what_the_visitor_types_is_escaped_before_it_goes_into_the_page() {
    // 経路もクエリも利用者が決める。そのまま埋めると、局所の頁とはいえ
    // 任意の script を仕込める入口になる。
    let (_dir, service, runtime, addr) = start("escape");
    let (_, body) = runtime.block_on(get(addr, "/address/%3Cscript%3Ealert(1)%3C/script%3E"));
    assert!(
        !body.contains("<script>alert(1)"),
        "raw script must not appear in the body"
    );
    assert!(body.contains("&lt;script&gt;"), "it appears escaped");

    let (_, body) = runtime.block_on(get(addr, "/search?q=%22%3E%3Csvg+onload%3Dalert(1)%3E"));
    assert!(
        !body.contains("<svg onload"),
        "it must not be possible to break out of the attribute"
    );
    drop(service);
}

#[test]
fn writing_verbs_are_refused() {
    // 読むだけの口である。書く動詞を受けないことを確かめる。
    let (_dir, service, runtime, addr) = start("readonly");
    let status = runtime.block_on(async {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        let mut raw = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut raw))
            .await
            .unwrap()
            .unwrap();
        String::from_utf8_lossy(&raw).into_owned()
    });
    assert!(status.contains("400"), "{status}");
    drop(service);
}

#[test]
fn without_an_index_the_pages_say_so_instead_of_showing_nothing() {
    // 索引が無いのに空の履歴を出すと、利用者はそれを「取引が無い」と
    // 受け取る。持っていないことを言わなければならない。
    let dir = TempDir::new("noindex");
    let service = NodeService::start(NETWORK, &dir.0).expect("a node can be started");
    let handle = service.handle();
    let runtime = runtime();
    let addr = runtime.block_on(async {
        oag_node::explorer::start_explorer(handle.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .expect("it can listen")
    });

    let (status, body) = runtime.block_on(get(addr, "/"));
    assert!(status.contains("200"), "{status}");
    assert!(body.contains("no"), "shown when there is no index");

    let text = runtime.block_on(async {
        let hash = handle.hash_at_height(0).await.unwrap().unwrap();
        let block = handle.block(hash).await.unwrap().unwrap();
        block.transactions[0].outputs[0]
            .lock
            .to_address(NETWORK)
            .unwrap()
            .to_string()
    });
    let (_, body) = runtime.block_on(get(addr, &format!("/address/{text}")));
    assert!(
        body.contains("no index is held, so history cannot be shown"),
        "the refusal is shown"
    );

    // ブロックは索引なしでも見られる。
    let (status, body) = runtime.block_on(get(addr, "/block/0"));
    assert!(status.contains("200"), "{status}");
    assert!(body.contains("block 0"));
    drop(service);
}

#[test]
fn an_address_of_the_wrong_network_is_refused() {
    // 別のネットワークのアドレスを貼っても、別物として断られなければ
    // ならない。
    let (_dir, service, runtime, addr) = start("network");
    let text = Address::from_pubkey(
        Network::Regtest,
        &oag_primitives::SecretKey::generate().public_key(),
    )
    .to_string();
    let (status, _) = runtime.block_on(get(addr, &format!("/address/{text}")));
    assert!(status.contains("404"), "{status}");
    drop(service);
}

/// 繋がっているピアが、住所を伏せた形で一覧に出ること。
#[test]
fn connected_peers_are_listed_with_their_addresses_cut_down() {
    let (_dir, service, runtime, explorer) = start("peers");
    let handle = service.handle();
    let other_dir = TempDir::new("peers-other");
    let other = NodeService::start(NETWORK, &other_dir.0).expect("a node can be started");
    let other_handle = other.handle();

    runtime.block_on(async {
        let (_, body) = get(explorer, "/peers").await;
        assert!(body.contains("no peers are connected"), "{body}");

        let listener = oag_net::transport::Listener::bind(
            oag_net::magic::magic_for(NETWORK),
            "127.0.0.1:0".parse().unwrap(),
        )
        .await
        .unwrap();
        let other_addr = listener.local_addr().unwrap();
        tokio::spawn(oag_node::accept_loop(other_handle, listener));
        tokio::spawn(oag_node::connect::maintain(
            handle.clone(),
            oag_node::connect::Outbound::new(),
            vec![other_addr],
        ));

        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while handle.peers().await.unwrap().is_empty() {
            assert!(std::time::Instant::now() < deadline, "the peers never met");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let (status, body) = get(explorer, "/peers").await;
        assert!(status.contains("200"), "{status}");
        assert!(body.contains("127.0.*.*"), "{body}");
        assert!(
            !body.contains("127.0.0.1") && !body.contains(&other_addr.port().to_string()),
            "the full address leaked: {body}"
        );
        assert!(body.contains("outbound"), "{body}");
        let agent = concat!("/oag-node:", env!("CARGO_PKG_VERSION"), "/");
        assert!(body.contains(agent), "{body}");
        assert!(
            body.contains("1 / 1"),
            "the upgraded count is wrong: {body}"
        );
    });
}

/// 1 要求出して、頭 (状態行とヘッダ) と本文を返す。
async fn get_with_head(addr: std::net::SocketAddr, target: &str) -> (String, String) {
    let mut stream = TcpStream::connect(addr).await.expect("it connects");
    let request = format!("GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("it can send");
    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut raw))
        .await
        .expect("a response arrives")
        .expect("readable");
    let text = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = text
        .split_once("\r\n\r\n")
        .expect("headers and body are separated");
    (head.to_string(), body.to_string())
}

/// `/api/stats` が、名前を決めた JSON を返すこと。
///
/// **名前は外の集計サイトが読む。** ここで全部の名前を確かめておけば、
/// うっかり変えたときに試験が落ちる (docs/COMMANDS.md)。
#[test]
fn the_stats_api_answers_in_json_with_the_documented_names() {
    let (_dir, service, runtime, addr) = start("api");
    let (head, body) = runtime.block_on(get_with_head(addr, "/api/stats"));
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert!(head.contains("Content-Type: application/json"), "{head}");
    assert!(head.contains("Access-Control-Allow-Origin: *"), "{head}");

    let json: serde_json::Value = serde_json::from_str(&body).expect("the body is JSON");
    let keys: Vec<&str> = json
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    for name in [
        "network",
        "height",
        "difficulty",
        "hashrate",
        "hashrate_24h",
        "average_block_time",
        "block_reward",
        "supply",
        "last_block",
        "softfork",
    ] {
        assert!(keys.contains(&name), "{name} is missing from {body}");
    }
    for name in ["state", "signalled", "elapsed", "threshold", "period"] {
        assert!(
            json["softfork"].get(name).is_some(),
            "softfork.{name} is missing from {body}"
        );
    }

    // ジェネシスだけの鎖。ジェネシスは数えないので、測れないものは null。
    assert_eq!(json["network"], "mainnet");
    assert_eq!(json["height"], 0);
    assert!(json["difficulty"].as_u64().unwrap() > 0);
    assert!(json["hashrate"].is_null());
    assert!(json["last_block"].is_null());
    assert_eq!(json["block_reward"], "10");
    assert_eq!(json["supply"], "0");
    assert_eq!(json["softfork"]["period"], 1440);
    assert_eq!(json["softfork"]["threshold"], 960);
    drop(service);
}

/// 頁の「chain」の欄も同じ数字から作られ、今まで通り出ること。
#[test]
fn the_stats_page_still_renders() {
    let (_dir, service, runtime, addr) = start("statspage");
    let (status, body) = runtime.block_on(get(addr, "/stats"));
    assert!(status.contains("200"), "{status}");
    assert!(body.contains("hashrate (24 h)"));
    assert!(body.contains("soft fork (commitments)"));
    drop(service);
}

/// 知らない API は、HTML の頁ではなく JSON の誤りで断る。
#[test]
fn an_unknown_api_is_refused_in_json() {
    let (_dir, service, runtime, addr) = start("api404");
    let (head, body) = runtime.block_on(get_with_head(addr, "/api/nothing"));
    assert!(head.starts_with("HTTP/1.1 404"), "{head}");
    assert!(head.contains("application/json"), "{head}");
    let json: serde_json::Value = serde_json::from_str(&body).expect("the body is JSON");
    assert_eq!(json["error"], "no such endpoint");
    drop(service);
}
