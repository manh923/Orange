//! Orange (OAG) のノード。
//!
//! - [`addrbook`] — ピアの住所帳
//! - [`connect`] — 外向きの接続を保つ
//! - [`log`] — 記録の印付け
//! - [`genesis`] — ネットワークごとのジェネシスブロック
//! - [`node`] — チェーン・mempool・採掘を束ねた本体
//! - [`service`] — 本体を専用スレッドに載せ、非同期側から使えるようにする
//! - [`peer`] — 1 本の接続の面倒を見る
//! - [`portmap`] — ルーターに待ち受けのポートを開けてもらう (UPnP / NAT-PMP)
//! - [`rpc`] — JSON-RPC の手続き
//! - [`seeds`] — 最初の繋ぎ先 (DNS シード)
//! - [`stats`] — 鎖の統計 (エクスプローラの `/stats`)
//!
//! 実行ファイルは `main.rs` にあり、ここを呼ぶだけの薄い層である。
//! 束ねる部分を library に置いているのは、**2 台のノードを実際に繋いだ
//! 試験**を書けるようにするためである。

#![forbid(unsafe_code)]
#![warn(missing_docs, clippy::all)]

pub mod addrbook;
pub mod connect;
pub mod explorer;
pub mod genesis;
pub mod light;
pub mod log;
pub mod node;
pub mod peer;
pub mod pool;
pub mod pool_page;
pub mod portmap;
pub mod rpc;
pub mod seeds;
pub mod service;
pub mod stats;
pub mod stratum;
pub mod wallet;

use oag_net::magic::magic_for;
use oag_net::transport::{Connection, Listener};
use oag_rpc::auth::{write_cookie, AuthError, Credential, COOKIE_USER};
use service::NodeHandle;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// 受け入れる接続の上限 (SPEC §10.5)。
///
/// Bitcoin Core の 125 本から、外向きの 8 本を引いた数。これが無いと、
/// 繋いだまま黙る相手にファイル記述子を使い切らせられる。
pub const MAX_INBOUND: usize = 117;

/// 同じ IP から受け入れる接続の上限 (SPEC §10.5)。
///
/// 同じ家 (NAT の内側) で何台か動かしている人がいても困らない程度にする。
/// **ループバックは数えない**。同じ機械で動かしている道具を締め出さない。
pub const MAX_INBOUND_PER_IP: usize = 4;

/// 受け入れに失敗したときに、次を待つまでの間。
pub(crate) const ACCEPT_RETRY: Duration = Duration::from_secs(1);

/// 受け入れた接続の数。
#[derive(Clone, Default)]
pub struct InboundSlots {
    inner: Arc<Mutex<InboundCounts>>,
}

#[derive(Default)]
struct InboundCounts {
    total: usize,
    per_ip: HashMap<IpAddr, usize>,
}

/// 受け入れた接続 1 本ぶんの枠。落とすと返る。
pub struct InboundSlot {
    slots: InboundSlots,
    ip: Option<IpAddr>,
}

impl InboundSlots {
    /// 枠を 1 つ取る。上限に達していれば `None`。
    ///
    /// `ip` が分からない接続は、全体の上限だけで数える。
    pub fn take(&self, ip: Option<IpAddr>) -> Option<InboundSlot> {
        let mut counts = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if counts.total >= MAX_INBOUND {
            return None;
        }
        if let Some(ip) = ip.filter(|ip| !ip.is_loopback()) {
            let used = counts.per_ip.get(&ip).copied().unwrap_or(0);
            if used >= MAX_INBOUND_PER_IP {
                return None;
            }
            counts.per_ip.insert(ip, used + 1);
        }
        counts.total += 1;
        Some(InboundSlot {
            slots: self.clone(),
            ip,
        })
    }

    /// いま受け入れている接続の数。
    pub fn in_use(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).total
    }
}

impl Drop for InboundSlot {
    fn drop(&mut self) {
        let mut counts = self.slots.inner.lock().unwrap_or_else(|e| e.into_inner());
        counts.total = counts.total.saturating_sub(1);
        if let Some(ip) = self.ip.filter(|ip| !ip.is_loopback()) {
            if let Some(used) = counts.per_ip.get_mut(&ip) {
                *used = used.saturating_sub(1);
                if *used == 0 {
                    counts.per_ip.remove(&ip);
                }
            }
        }
    }
}

/// 待ち受けた接続を順に受け入れる。**戻らない。**
///
/// # 受け入れに失敗しても止まらない
///
/// 0.4 までは `accept()` が失敗すると、ここから戻っていた。ファイル記述子を
/// 使い切ると失敗するので、繋いだまま黙る接続を大量に開かれるだけで、
/// **再起動するまで二度と受け入れなくなった。** 今は記録して少し待ち、
/// 続ける。
pub async fn accept_loop(handle: NodeHandle, listener: Listener) {
    let slots = InboundSlots::default();
    // 断っている最中か。記録を 1 度ずつ出すために覚える。
    let mut refusing = false;
    let mut full = false;
    loop {
        match listener.accept().await {
            Ok(conn) => {
                // **同期の最中は繋がれても応えない。** 追いつくまで渡せる
                // ものが無く、相手の外向きの枠を 1 本無駄にさせるだけである。
                // 名乗り合う前に閉じるので、相手は住所帳でこちらを減点しない。
                if handle.is_syncing().await.unwrap_or(false) {
                    if !refusing {
                        crate::log_peer!("syncing, so not accepting connections until caught up");
                        refusing = true;
                    }
                    drop(conn);
                    continue;
                }
                if refusing {
                    crate::log_peer!("caught up, so accepting connections again");
                    refusing = false;
                }
                let ip = conn.peer_addr().ok().map(|a| a.ip());
                // 出入り禁止にしている相手は、名乗り合う前に閉じる。
                if let Some(ip) = ip {
                    if handle.is_banned(ip).await.unwrap_or(false) {
                        drop(conn);
                        continue;
                    }
                }
                let Some(slot) = slots.take(ip) else {
                    if !full {
                        crate::log_warn!(
                            "refusing connections: {} in use (limit {MAX_INBOUND}, \
                             {MAX_INBOUND_PER_IP} per address)",
                            slots.in_use()
                        );
                        full = true;
                    }
                    drop(conn);
                    continue;
                };
                full = false;
                let handle = handle.clone();
                tokio::spawn(async move {
                    // 接続が終わるまで枠を持つ。
                    let _slot = slot;
                    if let Err(e) = peer::run(handle, conn).await {
                        crate::log_warn!("dropped the exchange with the peer: {e}");
                    }
                });
            }
            Err(e) => {
                crate::log_warn!("cannot accept a connection: {e} (trying again)");
                tokio::time::sleep(ACCEPT_RETRY).await;
            }
        }
    }
}

/// 相手に繋ぎにいく。
pub async fn dial(handle: NodeHandle, addr: SocketAddr) {
    let network = handle.network();
    match Connection::connect(magic_for(network), addr).await {
        Ok(conn) => {
            if let Err(e) = peer::run(handle, conn).await {
                crate::log_warn!("dropped the exchange with {addr}: {e}");
            }
        }
        Err(e) => crate::log_warn!("cannot connect to {addr}: {e}"),
    }
}

/// 相手に繋ぎに行き、切れたら繋ぎ直す。**戻らない。**
///
/// `--no-discovery` で名指しした相手用である。住所帳を使わないので、
/// 切れた相手を補充する仕組みが他に無い。**返事の無い相手は `ping` で
/// 切る** ([`peer`]) ので、繋ぎ直さないと二度と繋がらない。
pub async fn keep_dialling(handle: NodeHandle, addr: SocketAddr) {
    /// 切れてから繋ぎ直すまでの間。
    const REDIAL: std::time::Duration = std::time::Duration::from_secs(5);
    loop {
        dial(handle.clone(), addr).await;
        tokio::time::sleep(REDIAL).await;
    }
}

/// RPC の待ち受けを起こす。
///
/// 使い捨ての合言葉を作り、データディレクトリの `.cookie` に書き出す。
/// 呼び出し側 (ウォレットの CLI) はそれを読んで認証する。
///
/// 実際に開いた住所を返す。
pub async fn start_rpc(
    handle: NodeHandle,
    addr: SocketAddr,
    data_dir: &Path,
) -> Result<SocketAddr, String> {
    // 合言葉は起動のたびに作り直す。漏れても次の起動で無効になる。
    let secret: String = oag_primitives::SecretKey::generate()
        .to_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let userpass = format!("{COOKIE_USER}:{secret}");
    let path = write_cookie(data_dir, &userpass).map_err(|e: AuthError| e.to_string())?;
    let credential = Credential::from_userpass(&userpass);

    let rpc_handle: oag_rpc::http::Handler = Arc::new(move |body: String| {
        let handle = handle.clone();
        Box::pin(async move { rpc::handle(handle, body).await })
    });

    let server =
        oag_rpc::http::Server::bind(addr, credential.header_value().to_string(), rpc_handle)
            .await
            .map_err(|e| format!("cannot listen for RPC on {addr}: {e}"))?;
    let bound = server.local_addr().map_err(|e| e.to_string())?;
    println!("listening for RPC on {bound} (cookie: {})", path.display());
    tokio::spawn(server.serve());
    Ok(bound)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(last: u8) -> Option<IpAddr> {
        Some(IpAddr::from([203, 0, 113, last]))
    }

    #[test]
    fn one_address_gets_at_most_four_slots() {
        let slots = InboundSlots::default();
        let held: Vec<InboundSlot> = (0..MAX_INBOUND_PER_IP)
            .map(|_| slots.take(ip(1)).expect("within the limit"))
            .collect();
        assert!(slots.take(ip(1)).is_none(), "a fifth from the same address");
        // 別の住所はまだ入れる。
        assert!(slots.take(ip(2)).is_some());
        drop(held);
        assert!(
            slots.take(ip(1)).is_some(),
            "freed when the connections end"
        );
    }

    #[test]
    fn the_total_is_bounded_and_loopback_is_not_counted_per_address() {
        let slots = InboundSlots::default();
        let local = Some(IpAddr::from([127, 0, 0, 1]));
        let mut held = Vec::new();
        for _ in 0..MAX_INBOUND {
            held.push(
                slots
                    .take(local)
                    .expect("loopback is not limited per address"),
            );
        }
        assert_eq!(slots.in_use(), MAX_INBOUND);
        assert!(
            slots.take(ip(3)).is_none(),
            "the total limit applies to everyone"
        );
        held.pop();
        assert!(slots.take(ip(3)).is_some());
    }
}
