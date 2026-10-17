//! ブロックエクスプローラ。
//!
//! ブラウザで鎖の中身を見るための、読むだけの HTTP である。既定では
//! `127.0.0.1:8080` で待ち受ける。
//!
//! # なぜ RPC の HTTP を使い回さないのか
//!
//! [`oag_rpc::http`] は「`POST /` に JSON を 1 個、合言葉つき」だけを
//! 受け付ける。**その狭さがあの層の安全性そのもの**であり (SPEC §14.1)、
//! `GET` と任意のパスを通すために広げると、資金を動かせる入口の作りを
//! 弱めることになる。
//!
//! こちらは読むだけで、状態を変える経路を一切持たない。別の口として
//! 分けておけば、片方を緩めてももう片方は緩まない。
//!
//! # 何ができないか
//!
//! - **書き込みは無い。** 送金も採掘操作もここからはできない。
//! - **索引が要る。** 取引とアドレスの照会は `--index` で作った索引に
//!   頼る。無ければその旨を表示して、ブロックの閲覧だけを提供する。
//! - 待ち受けは既定でループバックのみである。外に出すなら住所を明示する。
//!
//! # 表示の向き
//!
//! アドレスの履歴は**古い順**に出す。索引の鍵が高さ順に並んでいるため、
//! その順に読むのが最も安く、続きから読むだけで頁送りになるからである。
//! 新しい順に並べ替えるには全件を数える必要があり、それは「途中で
//! 打ち切った」ことを利用者に隠したまま行うには危うい。

use crate::service::{ConnectedPeer, NodeHandle, TxRecord};
use crate::stats::{self, Tally};
use oag_consensus::lock::Lock;
use oag_consensus::{Block, Transaction};
use oag_primitives::{Address, Amount, Hash, Network};
use std::fmt::Write as _;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

/// 要求の頭の上限。
const MAX_REQUEST: usize = 8 * 1024;
/// 要求が来ないまま接続を保つ上限。
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// 一覧に出す最近のブロックの数。
const RECENT_BLOCKS: usize = 25;
/// 履歴 1 頁の件数。
const PAGE: usize = 50;
/// アドレス頁に出す UTXO の上限。
const MAX_UTXOS: usize = 500;

/// 接続をまたいで持つもの。
struct Shared {
    handle: NodeHandle,
    /// 鎖の統計。数えたところまでを覚えておき、次は足すだけにする。
    tally: Mutex<Tally>,
}

/// 待ち受けを始める。実際に結びついた住所を返す。
pub async fn start_explorer(handle: NodeHandle, addr: SocketAddr) -> Result<SocketAddr, String> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| format!("cannot listen for the explorer on {addr}: {e}"))?;
    let bound = listener
        .local_addr()
        .map_err(|e| format!("cannot determine the address: {e}"))?;

    let shared = Arc::new(Shared {
        handle,
        tally: Mutex::new(Tally::new()),
    });

    // 統計を先に数えておく。最初に頁を開いた人を待たせない。
    let warm = shared.clone();
    tokio::spawn(async move {
        let mut tally = warm.tally.lock().await;
        if let Err(e) = stats::catch_up(&mut tally, &warm.handle).await {
            crate::log_warn!("cannot count the chain statistics: {e}");
        }
    });

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                // **止めない。** ファイル記述子が尽きたときなどに 1 度失敗した
                // だけで、ノードを再起動するまで頁も API も返らなくなっていた。
                // P2P の待ち受けと同じく、少し待ってからやり直す。
                Err(e) => {
                    crate::log_warn!("cannot accept an explorer connection: {e} (trying again)");
                    tokio::time::sleep(crate::ACCEPT_RETRY).await;
                    continue;
                }
            };
            let shared = shared.clone();
            tokio::spawn(async move {
                let _ = serve_connection(stream, shared).await;
            });
        }
    });
    Ok(bound)
}

/// 応答 1 個。
struct Response {
    status: &'static str,
    /// `Content-Type`。
    kind: Kind,
    body: String,
}

/// 応答の中身の種類。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Html,
    /// `/api/…`。外の集計サイトがブラウザから読めるように、どこからの
    /// 読み出しも許す (`Access-Control-Allow-Origin: *`)。読むだけの
    /// 公開の数字なので、許して困るものは無い。
    Json,
}

async fn serve_connection(mut stream: TcpStream, shared: Arc<Shared>) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    let mut buffer = Vec::with_capacity(1024);

    // 1 要求 1 接続とする。使い回しに応じないぶん、扱いが単純になる。
    let target = match read_target(&mut stream, &mut buffer).await {
        Some(target) => target,
        None => {
            let body = page("400", "<h1>cannot read the request</h1>");
            return write_response(
                &mut stream,
                &Response {
                    status: "400 Bad Request",
                    kind: Kind::Html,
                    body,
                },
            )
            .await;
        }
    };

    let response = route(&shared, &target).await;
    write_response(&mut stream, &response).await
}

/// 要求行から要求先を取り出す。`GET` 以外は `None`。
pub(crate) async fn read_target(stream: &mut TcpStream, buffer: &mut Vec<u8>) -> Option<String> {
    let mut chunk = [0u8; 1024];
    loop {
        if buffer.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buffer.len() > MAX_REQUEST {
            return None;
        }
        let read = tokio::time::timeout(IDLE_TIMEOUT, stream.read(&mut chunk))
            .await
            .ok()?
            .ok()?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }

    let text = String::from_utf8_lossy(buffer);
    let line = text.lines().next()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?;
    let target = parts.next()?;
    // 読むだけの口なので、読む動詞しか受けない。
    if method != "GET" && method != "HEAD" {
        return None;
    }
    Some(target.to_string())
}

async fn write_response(stream: &mut TcpStream, response: &Response) -> std::io::Result<()> {
    let (kind, origin) = match response.kind {
        Kind::Html => ("text/html; charset=utf-8", ""),
        Kind::Json => ("application/json", "Access-Control-Allow-Origin: *\r\n"),
    };
    let head = format!(
        "HTTP/1.1 {}\r\n\
         Content-Type: {kind}\r\n\
         {origin}\
         Content-Length: {}\r\n\
         X-Content-Type-Options: nosniff\r\n\
         Referrer-Policy: no-referrer\r\n\
         X-Robots-Tag: noindex, nofollow\r\n\
         Connection: close\r\n\r\n",
        response.status,
        response.body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(response.body.as_bytes()).await?;
    stream.flush().await
}

// ━━━━━━━━ 経路 ━━━━━━━━

async fn route(shared: &Shared, target: &str) -> Response {
    let handle = &shared.handle;
    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path, query),
        None => (target, ""),
    };
    let path = percent_decode(path);
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();

    match segments.as_slice() {
        [""] => overview(shared).await,
        ["stats"] => stats_page(shared).await,
        ["richlist"] => richlist_page(shared).await,
        ["search"] => search(handle, query).await,
        ["block", rest @ ..] => block_page(handle, &rest.join("/"), query).await,
        ["tx", rest @ ..] => tx_page(handle, &rest.join("/")).await,
        ["address", rest @ ..] => address_page(handle, &rest.join("/"), query).await,
        ["mempool"] => mempool_page(handle).await,
        ["peers"] => peers_page(handle).await,
        ["api", "stats"] => api_stats(shared).await,
        ["api", ..] => json_error("404 Not Found", "no such endpoint"),
        _ => not_found("no such page"),
    }
}

async fn overview(shared: &Shared) -> Response {
    let handle = &shared.handle;
    let status = match handle.status().await {
        Ok(status) => status,
        Err(e) => return error_page(&e),
    };
    let indexed = handle.index_from().await.unwrap_or(None);
    let mempool = handle.mempool_txids().await.unwrap_or_default();

    let mut body = String::new();
    body.push_str(&search_box(""));

    body.push_str("<div class=\"grid\">");
    stat(&mut body, "network", &esc(&status.network.to_string()));
    stat(&mut body, "height", &status.height.to_string());
    stat(&mut body, "next difficulty", &group(status.next_difficulty));
    // **数え終わっていなければ待たない。** 起動直後の数え上げの最中に
    // 開かれても、トップはすぐに返す。
    let hashrate = match shared.tally.try_lock() {
        Ok(mut tally) => {
            let _ = stats::catch_up(&mut tally, handle).await;
            tally.hashrate(stats::SHORT_WINDOW)
        }
        Err(_) => None,
    };
    stat(
        &mut body,
        "hashrate (1 h)",
        &hashrate.map_or_else(|| "—".to_string(), stats::format_hashrate),
    );
    stat(&mut body, "UTXO", &group(status.utxo_count));
    stat(&mut body, "mempool", &mempool.len().to_string());
    stat(
        &mut body,
        "index",
        match indexed {
            Some(_) => "<span class=\"ok\">yes</span>",
            None => "<span class=\"warn\">no</span>",
        },
    );
    body.push_str("</div>");

    if indexed.is_none() {
        body.push_str(
            "<p class=\"note\">no index is held, so transaction IDs and addresses cannot be \
             looked up. Start the node with <code>--index</code> to enable them.</p>",
        );
    }

    body.push_str("<h2>tip</h2>");
    body.push_str(&format!(
        "<p class=\"mono break\">{}</p>",
        esc(&status.tip.to_string())
    ));

    // 最近のブロック。
    body.push_str("<h2>recent blocks</h2><div class=\"wrap\"><table class=\"list\">");
    body.push_str("<tr class=\"head\"><th>height</th><th>time (UTC)</th><th>txs</th><th>size</th><th>difficulty</th><th>hash</th></tr>");
    // **本体は引かない。** 高さごとに 2 往復して 25 個のブロックを丸ごと
    // 復号すると、満杯の鎖では 5 MB を読んで 16,000 件あまりの取引を組み
    // 立てることになる。表に出すのは 1 行 5 項目だけである。
    for summary in handle
        .recent_blocks(RECENT_BLOCKS)
        .await
        .unwrap_or_default()
    {
        let hash = summary.hash.to_string();
        let _ = write!(
            body,
            "<tr><td data-label=\"height\"><a href=\"/block/{h}\">{h}</a></td>\
             <td data-label=\"time (UTC)\" class=\"mono\">{t}</td>\
             <td data-label=\"txs\">{n}</td><td data-label=\"size\" class=\"wide\">{s} B</td>\
             <td data-label=\"difficulty\" class=\"wide\">{d}</td>\
             <td data-label=\"hash\" class=\"mono trunc wide\"><a href=\"/block/{hash}\">{short}</a></td></tr>",
            h = summary.height,
            t = utc(summary.header.timestamp),
            n = summary.transactions,
            s = group(summary.size as u64),
            d = group(summary.header.difficulty),
            hash = esc(&hash),
            short = esc(&shorten(&hash)),
        );
    }
    body.push_str("</table></div>");

    if !mempool.is_empty() {
        body.push_str(&format!(
            "<h2>mempool</h2><p><a href=\"/mempool\">{} unconfirmed transactions</a></p>",
            mempool.len()
        ));
    }

    ok(page("Orange explorer", &body))
}

async fn stats_page(shared: &Shared) -> Response {
    let handle = &shared.handle;
    let status = match handle.status().await {
        Ok(status) => status,
        Err(e) => return error_page(&e),
    };

    let mut tally = shared.tally.lock().await;
    if let Err(e) = stats::catch_up(&mut tally, handle).await {
        return error_page(&e);
    }
    let holdings = tally.holdings();
    let days = tally.days();

    let mut body = String::new();
    body.push_str(&search_box(""));
    body.push_str("<h1>statistics</h1>");

    let summary = stats::summary(&tally, &status);
    let rate = |rate: Option<f64>| rate.map_or_else(|| "—".to_string(), stats::format_hashrate);
    let running = match tally.first_time() {
        Some(first) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(first);
            format!("{:.1} days", (now - first).max(0) as f64 / 86_400.0)
        }
        None => "—".to_string(),
    };

    body.push_str("<h2>chain</h2><div class=\"grid\">");
    stat(&mut body, "running", &running);
    stat(&mut body, "blocks", &group(summary.height));
    stat(
        &mut body,
        "average block time",
        &summary
            .average_block_time
            .map_or_else(|| "—".to_string(), |s| format!("{s:.1} s")),
    );
    stat(&mut body, "hashrate (1 h)", &rate(summary.hashrate));
    stat(&mut body, "hashrate (24 h)", &rate(summary.hashrate_24h));
    stat(&mut body, "next difficulty", &group(summary.difficulty));
    // コミットメント出力などのソフトフォーク (SPEC §10.9)。数えている間は、
    // 今の区切りで印の付いたブロックの数も出す。
    let softfork = &summary.softfork;
    let label = match (
        softfork.signalled,
        softfork.elapsed,
        softfork.threshold,
        softfork.period,
    ) {
        (Some(count), Some(elapsed), Some(threshold), Some(period)) => format!(
            "{} — {count} of {elapsed} signalled (needs {threshold} of {period})",
            softfork.state
        ),
        _ => softfork.state.to_string(),
    };
    stat(&mut body, "soft fork (commitments)", &esc(&label));
    body.push_str("</div>");

    body.push_str("<h2>people</h2><div class=\"grid\">");
    stat(
        &mut body,
        "addresses holding coins",
        &group(holdings.holders as u64),
    );
    stat(
        &mut body,
        "addresses that have mined",
        &group(tally.miners() as u64),
    );
    stat(
        &mut body,
        "mined in the last 7 days",
        &group(tally.recent_miners() as u64),
    );
    stat(
        &mut body,
        "received in the last 7 days",
        &group(tally.recent_receivers() as u64),
    );
    stat(&mut body, "transactions", &group(tally.transactions()));
    stat(
        &mut body,
        "coins unspent",
        &format!("{} OAG", esc(&whole_oag(holdings.supply))),
    );
    body.push_str("</div>");

    // 採掘の偏り。
    body.push_str(&miner_shares(&tally, handle.network()));

    // 保有の分布。
    body.push_str("<h2>who holds the coins</h2><div class=\"grid\">");
    stat(&mut body, "largest address", &percent(holdings.top1));
    stat(&mut body, "largest 10", &percent(holdings.top10));
    stat(&mut body, "largest 100", &percent(holdings.top100));
    body.push_str("</div>");
    body.push_str(
        "<div class=\"wrap\"><table class=\"list\"><tr class=\"head\"><th>balance</th>\
         <th class=\"num\">addresses</th><th class=\"num\">coins</th><th class=\"num\">share</th></tr>",
    );
    for band in tally.bands() {
        let range = match band.to {
            Some(to) if band.from == 0 => format!("under {} OAG", group(to)),
            Some(to) => format!("{} – {} OAG", group(band.from), group(to)),
            None => format!("{} OAG and over", group(band.from)),
        };
        let share = if holdings.supply == 0 {
            0.0
        } else {
            band.amount as f64 / holdings.supply as f64
        };
        let _ = write!(
            body,
            "<tr><td data-label=\"balance\">{range}</td>\
             <td data-label=\"addresses\" class=\"num\">{holders}</td>\
             <td data-label=\"coins\" class=\"num\">{coins} OAG</td>\
             <td data-label=\"share\" class=\"num\">{share}</td></tr>",
            range = esc(&range),
            holders = group(band.holders as u64),
            coins = esc(&whole_oag(band.amount)),
            share = percent(share),
        );
    }
    body.push_str("</table></div>");
    body.push_str("<p><a href=\"/richlist\">the 100 largest addresses &rarr;</a></p>");

    // 日ごとの推移。
    body.push_str("<h2>day by day (UTC)</h2>");
    if days.is_empty() {
        body.push_str("<p class=\"note\">nothing has been mined yet.</p>");
    } else {
        let labels: Vec<String> = days
            .iter()
            .map(|d| {
                let date = utc(d.day * 86_400)[..10].to_string();
                if d.partial {
                    format!("{date} (so far)")
                } else {
                    date
                }
            })
            .collect();
        let series = |f: &dyn Fn(&stats::Day) -> Option<f64>| -> Vec<Option<f64>> {
            days.iter().map(f).collect()
        };
        body.push_str(&line_chart(
            "hashrate",
            &labels,
            &[("hashrate", "s1", series(&|d| d.hashrate()))],
            false,
            &|v| stats::format_hashrate(v),
        ));
        body.push_str(&line_chart(
            "addresses that mined",
            &labels,
            &[("addresses", "s1", series(&|d| Some(d.miners as f64)))],
            true,
            &|v| group(v.round() as u64),
        ));
        body.push_str(&line_chart(
            "addresses holding coins, at the end of the day",
            &labels,
            &[(
                "addresses",
                "s1",
                series(&|d| Some(d.holdings.holders as f64)),
            )],
            true,
            &|v| group(v.round() as u64),
        ));
        body.push_str(&line_chart(
            "share of all coins held by the largest addresses",
            &labels,
            &[
                (
                    "largest address",
                    "s1",
                    series(&|d| Some(d.holdings.top1 * 100.0)),
                ),
                (
                    "largest 10",
                    "s2",
                    series(&|d| Some(d.holdings.top10 * 100.0)),
                ),
            ],
            false,
            &|v| format!("{v:.0}%"),
        ));
        body.push_str(&line_chart(
            "transactions",
            &labels,
            &[(
                "transactions",
                "s1",
                series(&|d| Some(d.transactions as f64)),
            )],
            true,
            &|v| group(v.round() as u64),
        ));

        // 同じ数字を表でも出す。色が見分けにくい人、数字を写したい人のため。
        body.push_str(
            "<details><summary>the same numbers as a table</summary>\
             <div class=\"wrap\"><table class=\"list\"><tr class=\"head\"><th>day</th>\
             <th class=\"num\">blocks</th><th class=\"num\">hashrate</th>\
             <th class=\"num\">mined</th><th class=\"num\">new</th>\
             <th class=\"num\">holding</th><th class=\"num\">largest</th>\
             <th class=\"num\">largest 10</th><th class=\"num\">txs</th></tr>",
        );
        for (day, label) in days.iter().zip(&labels).rev() {
            let _ = write!(
                body,
                "<tr><td data-label=\"day\" class=\"mono\">{label}</td>\
                 <td data-label=\"blocks\" class=\"num\">{blocks}</td>\
                 <td data-label=\"hashrate\" class=\"num\">{rate}</td>\
                 <td data-label=\"mined\" class=\"num\">{miners}</td>\
                 <td data-label=\"new\" class=\"num\">{new}</td>\
                 <td data-label=\"holding\" class=\"num\">{holders}</td>\
                 <td data-label=\"largest\" class=\"num\">{top1}</td>\
                 <td data-label=\"largest 10\" class=\"num\">{top10}</td>\
                 <td data-label=\"txs\" class=\"num\">{txs}</td></tr>",
                label = esc(label),
                blocks = group(day.blocks),
                rate = day
                    .hashrate()
                    .map_or_else(|| "—".to_string(), stats::format_hashrate),
                miners = group(day.miners as u64),
                new = group(day.new_miners as u64),
                holders = group(day.holdings.holders as u64),
                top1 = percent(day.holdings.top1),
                top10 = percent(day.holdings.top10),
                txs = group(day.transactions),
            );
        }
        body.push_str("</table></div></details>");
    }

    body.push_str(
        "<p class=\"note\">The hashrate is an estimate: the work the difficulty asks for, \
         divided by the time the blocks took. An address is counted once however many \
         blocks or coins it has; one person can hold many addresses, and several people \
         can share one. Transactions do not include mining rewards. The genesis block is \
         left out of every count.</p>",
    );

    ok(page("statistics", &body))
}

/// `GET /api/stats`。`/stats` の「chain」の欄と同じ数字を JSON で返す。
///
/// 外の集計サイト (採掘の収益計算など) が読むためのもの。中身と名前は
/// [`stats::Summary`] と `docs/COMMANDS.md` にある。
async fn api_stats(shared: &Shared) -> Response {
    let handle = &shared.handle;
    let status = match handle.status().await {
        Ok(status) => status,
        Err(e) => return json_error("503 Service Unavailable", &e),
    };
    let mut tally = shared.tally.lock().await;
    if let Err(e) = stats::catch_up(&mut tally, handle).await {
        return json_error("503 Service Unavailable", &e);
    }
    match serde_json::to_string(&stats::summary(&tally, &status)) {
        Ok(body) => json_ok(body),
        Err(e) => json_error("500 Internal Server Error", &e.to_string()),
    }
}

/// 直近のブロックを誰が掘ったか。
///
/// **5 割を超えた 1 人がいれば、はっきり書く。** 過半の力を持てば、最近の
/// ブロックを掘り直して自分の支払いを取り消せる。本人に悪意がなくても、
/// 受け取る側は待つ承認の数を増やすべきで、それを知る場所はここしかない。
fn miner_shares(tally: &stats::Tally, network: Network) -> String {
    /// 表に並べる数。残りはまとめて 1 行にする。
    const ROWS: usize = 10;

    let (all, counted) = tally.block_shares(stats::SHARE_WINDOW);
    let mut out = String::new();
    let _ = write!(
        out,
        "<h2>who mined the last {} blocks</h2>",
        group(counted as u64)
    );
    if counted == 0 {
        out.push_str("<p class=\"note\">nothing has been mined yet.</p>");
        return out;
    }
    let share = |n: usize| n as f64 / counted as f64;

    if let Some((_, top)) = all.first() {
        if *top * 2 > counted {
            let _ = write!(
                out,
                "<p class=\"warn\">One address mined {} of the last {} blocks. \
                 Whoever has more than half of the hashrate can undo recent blocks, \
                 their own payments included. For large amounts, wait for more \
                 confirmations than usual.</p>",
                percent(share(*top)),
                group(counted as u64),
            );
        }
    }

    out.push_str(
        "<div class=\"wrap\"><table class=\"list\"><tr class=\"head\"><th>#</th><th>address</th>\
         <th class=\"num\">blocks</th><th class=\"num\">share</th></tr>",
    );
    for (rank, (lock, blocks)) in all.iter().take(ROWS).enumerate() {
        let addr = lock
            .to_address(network)
            .map(|a| a.to_string())
            .unwrap_or_else(|_| format!("version {}", lock.version()));
        let _ = write!(
            out,
            "<tr><td data-label=\"#\">{rank}</td>\
             <td data-label=\"address\" class=\"mono trunc\"><a href=\"/address/{a}\">{short}</a></td>\
             <td data-label=\"blocks\" class=\"num\">{blocks}</td>\
             <td data-label=\"share\" class=\"num\">{share}</td></tr>",
            rank = rank + 1,
            a = esc(&addr),
            short = esc(&shorten(&addr)),
            blocks = group(*blocks as u64),
            share = percent(share(*blocks)),
        );
    }
    if all.len() > ROWS {
        let rest: usize = all[ROWS..].iter().map(|(_, n)| n).sum();
        let _ = write!(
            out,
            "<tr><td data-label=\"#\"></td>\
             <td data-label=\"address\">{} more addresses</td>\
             <td data-label=\"blocks\" class=\"num\">{}</td>\
             <td data-label=\"share\" class=\"num\">{}</td></tr>",
            group((all.len() - ROWS) as u64),
            group(rest as u64),
            percent(share(rest)),
        );
    }
    out.push_str("</table></div>");
    out
}

async fn richlist_page(shared: &Shared) -> Response {
    let handle = &shared.handle;
    let network = handle.network();
    let mut tally = shared.tally.lock().await;
    if let Err(e) = stats::catch_up(&mut tally, handle).await {
        return error_page(&e);
    }
    let holdings = tally.holdings();

    let mut body = String::new();
    body.push_str(&search_box(""));
    body.push_str("<h1>the 100 largest addresses</h1>");
    let _ = write!(
        body,
        "<p class=\"note\">{} addresses hold {} OAG. One person can hold many addresses.</p>",
        group(holdings.holders as u64),
        esc(&whole_oag(holdings.supply)),
    );
    body.push_str(
        "<div class=\"wrap\"><table class=\"list\"><tr class=\"head\"><th>#</th><th>address</th>\
         <th class=\"num\">balance</th><th class=\"num\">share</th><th class=\"num\">total so far</th></tr>",
    );
    let mut running = 0u128;
    for (rank, (lock, amount)) in tally.richest(100).into_iter().enumerate() {
        running += amount;
        let addr = lock
            .to_address(network)
            .map(|a| a.to_string())
            .unwrap_or_else(|_| format!("version {}", lock.version()));
        let share = |n: u128| {
            if holdings.supply == 0 {
                0.0
            } else {
                n as f64 / holdings.supply as f64
            }
        };
        let _ = write!(
            body,
            "<tr><td data-label=\"#\">{rank}</td>\
             <td data-label=\"address\" class=\"mono trunc\"><a href=\"/address/{a}\">{short}</a></td>\
             <td data-label=\"balance\" class=\"num\">{balance} OAG</td>\
             <td data-label=\"share\" class=\"num\">{share}</td>\
             <td data-label=\"total so far\" class=\"num\">{cumulative}</td></tr>",
            rank = rank + 1,
            a = esc(&addr),
            short = esc(&shorten(&addr)),
            balance = esc(&whole_oag(amount)),
            share = percent(share(amount)),
            cumulative = percent(share(running)),
        );
    }
    body.push_str("</table></div>");
    ok(page("richest addresses", &body))
}

/// 日ごとの折れ線。
///
/// **JavaScript を使わない。** 頁は読むだけのものであり、スクリプトを
/// 持たないことで差し込みの余地を狭めている。線は SVG を横に引き伸ばして
/// 描き (線の太さは `non-scaling-stroke` で保つ)、目盛りと日付は SVG の
/// 外の HTML に置く。こうすれば画面の幅によらず文字の大きさが変わらない。
/// 値は各日の列の `<title>` に持たせ、指を置けば出る。
fn line_chart(
    title: &str,
    labels: &[String],
    series: &[(&str, &str, Vec<Option<f64>>)],
    whole: bool,
    format: &dyn Fn(f64) -> String,
) -> String {
    let n = labels.len().max(1);
    let max = series
        .iter()
        .flat_map(|(_, _, values)| values.iter().flatten())
        .fold(0.0f64, |m, v| m.max(*v));
    let (top, step) = nice_scale(max, whole);
    let y = |v: f64| {
        if top > 0.0 {
            100.0 - v / top * 100.0
        } else {
            100.0
        }
    };

    let mut out = String::new();
    // 見出しに最新の値を添える。数字を全部の点に書かないかわりに、
    // いちばん知りたい 1 つだけを出す。
    let latest: Vec<String> = series
        .iter()
        .filter_map(|(name, _, values)| {
            let v = values.last().copied().flatten()?;
            Some(if series.len() > 1 {
                format!("{name} {}", format(v))
            } else {
                format(v)
            })
        })
        .collect();
    let _ = write!(
        out,
        "<figure class=\"chart\"><figcaption><b>{}</b> <span class=\"note\">latest: {}</span></figcaption>",
        esc(title),
        esc(&latest.join(", ")),
    );
    if series.len() > 1 {
        out.push_str("<div class=\"legend\">");
        for (name, class, _) in series {
            let _ = write!(
                out,
                "<span><i class=\"key {class}\"></i>{}</span>",
                esc(name)
            );
        }
        out.push_str("</div>");
    }
    out.push_str("<div class=\"plot\">");
    let mut tick = 0.0;
    while tick <= top + step / 2.0 && step > 0.0 {
        let _ = write!(
            out,
            "<span class=\"y\" style=\"top:{:.2}%\">{}</span>",
            y(tick),
            esc(&format(tick))
        );
        tick += step;
    }
    let _ = write!(
        out,
        "<svg viewBox=\"0 0 {n} 100\" preserveAspectRatio=\"none\" role=\"img\" aria-label=\"{}\">",
        esc(title)
    );
    let mut tick = 0.0;
    while tick <= top + step / 2.0 && step > 0.0 {
        let _ = write!(
            out,
            "<line class=\"gridline\" x1=\"0\" x2=\"{n}\" y1=\"{v:.2}\" y2=\"{v:.2}\" vector-effect=\"non-scaling-stroke\"/>",
            v = y(tick)
        );
        tick += step;
    }
    for (_, class, values) in series {
        let mut points = String::new();
        for (i, v) in values.iter().enumerate() {
            let Some(v) = v else { continue };
            let _ = write!(points, "{:.3},{:.2} ", i as f64 + 0.5, y(*v));
        }
        // 1 日しか無ければ点にならないので、幅いっぱいに引く。
        if values.len() == 1 {
            if let Some(v) = values[0] {
                points = format!("0,{0:.2} {n},{0:.2}", y(v));
            }
        }
        let _ = write!(
            out,
            "<polyline class=\"line {class}\" points=\"{}\" vector-effect=\"non-scaling-stroke\"/>",
            points.trim_end()
        );
    }
    for (i, label) in labels.iter().enumerate() {
        let values: Vec<String> = series
            .iter()
            .map(|(name, _, values)| {
                let v = values
                    .get(i)
                    .copied()
                    .flatten()
                    .map_or_else(|| "—".to_string(), format);
                if series.len() > 1 {
                    format!("{name}: {v}")
                } else {
                    v
                }
            })
            .collect();
        let _ = write!(
            out,
            "<rect class=\"hit\" x=\"{i}\" y=\"0\" width=\"1\" height=\"100\"><title>{}: {}</title></rect>",
            esc(label),
            esc(&values.join(", "))
        );
    }
    out.push_str("</svg></div>");
    let first = labels.first().map(String::as_str).unwrap_or("");
    let last = labels.last().map(String::as_str).unwrap_or("");
    let _ = write!(
        out,
        "<div class=\"xaxis\"><span>{}</span><span>{}</span></div></figure>",
        esc(first),
        if labels.len() > 1 {
            esc(last)
        } else {
            String::new()
        },
    );
    out
}

/// 目盛りの上端と刻み。刻みは 1・2・5 の 10 のべき倍にし、4 目盛り前後にする。
///
/// `whole` なら刻みを 1 より細かくしない。人数や件数に 0.5 の目盛りを
/// 振ると、丸めた表示が「1、1、2、2」と重なる。
fn nice_scale(max: f64, whole: bool) -> (f64, f64) {
    if max <= 0.0 || !max.is_finite() {
        return (1.0, 1.0);
    }
    let rough = max / 4.0;
    let power = 10f64.powf(rough.log10().floor());
    let mut step = [1.0, 2.0, 5.0, 10.0]
        .iter()
        .map(|m| m * power)
        .find(|s| *s >= rough)
        .unwrap_or(10.0 * power);
    if whole {
        step = step.max(1.0);
    }
    ((max / step).ceil() * step, step)
}

/// 0〜1 の割合を百分率にする。
fn percent(share: f64) -> String {
    format!("{:.1}%", share * 100.0)
}

/// 最小単位を、小数点以下を 2 桁に丸めた OAG にする。表に 16 桁並べると
/// 読めない。正確な値はアドレスの頁にある。
fn whole_oag(atomic: u128) -> String {
    let unit = oag_primitives::amount::ATOMIC_PER_OAG;
    let cents = (atomic + unit / 200) / (unit / 100);
    let whole = (cents / 100) as u64;
    format!("{}.{:02}", group(whole), cents % 100)
}

async fn search(handle: &NodeHandle, query: &str) -> Response {
    let q = query_value(query, "q").unwrap_or_default();
    let q = q.trim().to_string();
    if q.is_empty() {
        return not_found("nothing was entered");
    }

    // 数字だけなら高さ。
    if q.chars().all(|c| c.is_ascii_digit()) {
        return block_page(handle, &q, "").await;
    }
    // 64 文字の 16 進はブロックか取引。ブロックを先に見る。
    if q.len() == 64 && q.chars().all(|c| c.is_ascii_hexdigit()) {
        if let Ok(hash) = q.parse::<Hash>() {
            if matches!(handle.entry(hash).await, Ok(Some(_))) {
                return block_page(handle, &q, "").await;
            }
        }
        return tx_page(handle, &q).await;
    }
    address_page(handle, &q, "").await
}

async fn block_page(handle: &NodeHandle, key: &str, query: &str) -> Response {
    let hash = if key.chars().all(|c| c.is_ascii_digit()) {
        let Ok(height) = key.parse::<u64>() else {
            return not_found(&format!("{key} cannot be read as a height"));
        };
        match handle.hash_at_height(height).await {
            Ok(Some(hash)) => hash,
            Ok(None) => return not_found(&format!("there is no block at height {height}")),
            Err(e) => return error_page(&e),
        }
    } else {
        match key.parse::<Hash>() {
            Ok(hash) => hash,
            Err(_) => return not_found(&format!("{key} cannot be read as a block hash")),
        }
    };

    let block = match handle.block(hash).await {
        Ok(Some(block)) => block,
        Ok(None) => return not_found("the body of that block is not held"),
        Err(e) => return error_page(&e),
    };

    let mut body = String::new();
    body.push_str(&search_box(""));
    let _ = write!(body, "<h1>block {}</h1>", block.header.height);
    body.push_str(&format!(
        "<p class=\"mono break\">{}</p>",
        esc(&hash.to_string())
    ));

    body.push_str("<div class=\"wrap\"><table class=\"kv\">");
    row(&mut body, "height", &block.header.height.to_string());
    row(&mut body, "time (UTC)", &utc(block.header.timestamp));
    row(
        &mut body,
        "unix seconds",
        &block.header.timestamp.to_string(),
    );
    row(&mut body, "difficulty", &group(block.header.difficulty));
    row(&mut body, "nonce", &group(block.header.nonce));
    row(&mut body, "version", &block.header.version.to_string());
    row(
        &mut body,
        "size",
        &format!("{} B", group(block.size() as u64)),
    );
    row(
        &mut body,
        "transactions",
        &block.transactions.len().to_string(),
    );
    row_raw(
        &mut body,
        "merkle root",
        &format!(
            "<span class=\"mono break\">{}</span>",
            esc(&block.header.merkle_root.to_string())
        ),
    );
    let prev = block.header.prev_hash;
    if block.header.height > 0 {
        row_raw(
            &mut body,
            "parent",
            &format!(
                "<a class=\"mono break\" href=\"/block/{p}\">{p}</a>",
                p = esc(&prev.to_string())
            ),
        );
    }
    body.push_str("</table></div>");

    // 満杯のブロックは約 670 取引を持ちうる。全部を 1 枚に吐くと
    // 130 KB の表になるので、アドレス履歴と同じ幅で区切る。
    let from: usize = query_value(query, "from")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
        .min(block.transactions.len());
    let upto = (from + PAGE).min(block.transactions.len());

    body.push_str("<h2>transactions</h2>");
    if block.transactions.len() > PAGE && upto > from {
        let _ = write!(
            body,
            "<p class=\"note\">{} to {} of {}</p>",
            group(block.transactions.len() as u64),
            from + 1,
            upto
        );
    }
    body.push_str(&tx_table(&block, handle.network(), from, upto));

    body.push_str(&tx_pager(&hash, from, upto, block.transactions.len()));

    let mut nav = String::new();
    if block.header.height > 0 {
        let _ = write!(
            nav,
            "<a href=\"/block/{}\">&larr; previous</a> ",
            block.header.height - 1
        );
    }
    let _ = write!(
        nav,
        "<a href=\"/block/{}\">next &rarr;</a>",
        block.header.height + 1
    );
    let _ = write!(body, "<p class=\"nav\">{nav}</p>");

    ok(page(&format!("block {}", block.header.height), &body))
}

async fn tx_page(handle: &NodeHandle, key: &str) -> Response {
    let Ok(txid) = key.parse::<Hash>() else {
        return not_found(&format!("{key} cannot be read as a transaction ID"));
    };
    let network = handle.network();

    let indexed = handle.index_from().await.unwrap_or(None).is_some();
    let record = if indexed {
        handle.tx_record(txid).await.unwrap_or(None)
    } else {
        None
    };

    let mut body = String::new();
    body.push_str(&search_box(""));
    body.push_str("<h1>transaction</h1>");
    body.push_str(&format!(
        "<p class=\"mono break\">{}</p>",
        esc(&txid.to_string())
    ));

    if let Some(record) = record {
        body.push_str(&record_detail(&record, network));
        return ok(page("transaction", &body));
    }

    // 索引に無ければ mempool を見る。
    match handle.mempool_tx(txid).await {
        Ok(Some(tx)) => {
            body.push_str("<p class=\"warn\">unconfirmed (in the mempool)</p>");
            body.push_str(&tx_detail(&tx, &[], network));
            ok(page("transaction", &body))
        }
        Ok(None) if !indexed => not_found(
            "without an index, a confirmed transaction cannot be looked up by ID; start the node with --index",
        ),
        Ok(None) => not_found("there is no such transaction"),
        Err(e) => error_page(&e),
    }
}

async fn address_page(handle: &NodeHandle, key: &str, query: &str) -> Response {
    let network = handle.network();
    let address = match Address::decode_on(network, key) {
        Ok(address) => address,
        Err(e) => return not_found(&format!("{key} cannot be read as an address: {e}")),
    };
    let lock = Lock::from_address(&address);
    let from: u64 = query_value(query, "from")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let mut body = String::new();
    body.push_str(&search_box(key));
    body.push_str("<h1>address</h1>");
    body.push_str(&format!("<p class=\"mono break\">{}</p>", esc(key)));

    // 残高は索引に頼らない。UTXO セットを丸ごと見て拾う。
    let utxos = handle
        .scan_utxos(vec![lock.clone()], MAX_UTXOS)
        .await
        .unwrap_or_default();
    let balance = Amount::sum(utxos.iter().map(|u| u.entry.output.amount));

    body.push_str("<div class=\"grid\">");
    stat(
        &mut body,
        "balance",
        &match balance {
            Some(amount) => format!("{} OAG", esc(&amount.to_string())),
            None => "cannot be computed".to_string(),
        },
    );
    stat(&mut body, "UTXO", &utxos.len().to_string());
    body.push_str("</div>");
    if utxos.len() >= MAX_UTXOS {
        body.push_str(
            "<p class=\"warn\">the UTXO scan hit its limit, so this balance is lower than the real one.</p>",
        );
    }

    // 履歴は索引に頼る。
    match handle.index_from().await {
        Ok(Some(_)) => {
            let history = handle
                .address_history(lock.clone(), from, PAGE + 1)
                .await
                .unwrap_or_default();
            let more = history.len() > PAGE;
            let shown = &history[..history.len().min(PAGE)];

            body.push_str("<h2>history <span class=\"note\">(oldest first)</span></h2>");
            if shown.is_empty() {
                body.push_str("<p class=\"note\">no transactions in this range.</p>");
            } else {
                body.push_str(&history_table(shown, &lock, network));
            }
            if more {
                let next = shown.last().map(|r| r.location.height + 1).unwrap_or(from);
                let _ = write!(
                    body,
                    "<p class=\"nav\"><a href=\"/address/{}?from={}\">more &rarr;</a></p>",
                    esc(key),
                    next
                );
            }
            if from > 0 {
                let _ = write!(
                    body,
                    "<p class=\"nav\"><a href=\"/address/{}\">&larr; back to start</a></p>",
                    esc(key)
                );
            }
        }
        _ => body.push_str(
            "<h2>history</h2><p class=\"warn\">no index is held, so history cannot be shown. \
             Start the node with <code>--index</code>.</p>",
        ),
    }

    // 保有中の UTXO。
    if !utxos.is_empty() {
        body.push_str("<h2>unspent outputs</h2><div class=\"wrap\"><table class=\"list\">");
        body.push_str("<tr class=\"head\"><th>transaction</th><th>index</th><th>amount</th><th>height</th><th>kind</th></tr>");
        for utxo in &utxos {
            let _ = write!(
                body,
                "<tr><td data-label=\"transaction\" class=\"mono trunc\"><a href=\"/tx/{full}\">{short}</a></td>\
                 <td data-label=\"index\" class=\"wide\">{i}</td><td data-label=\"amount\" class=\"num\">{a} OAG</td>\
                 <td data-label=\"height\">{h}</td><td data-label=\"kind\" class=\"wide\">{c}</td></tr>",
                full = esc(&utxo.outpoint.txid.to_string()),
                short = esc(&shorten(&utxo.outpoint.txid.to_string())),
                i = utxo.outpoint.index,
                a = esc(&utxo.entry.output.amount.to_string()),
                h = utxo.entry.height,
                c = if utxo.entry.is_coinbase {
                    "mined"
                } else {
                    "ordinary"
                },
            );
        }
        body.push_str("</table></div>");
    }

    ok(page("address", &body))
}

async fn mempool_page(handle: &NodeHandle) -> Response {
    let txids = match handle.mempool_txids().await {
        Ok(txids) => txids,
        Err(e) => return error_page(&e),
    };
    let mut body = String::new();
    body.push_str(&search_box(""));
    let _ = write!(body, "<h1>mempool ({} entries)</h1>", txids.len());
    if txids.is_empty() {
        body.push_str("<p class=\"note\">there are no unconfirmed transactions.</p>");
    } else {
        body.push_str("<div class=\"wrap\"><table class=\"list\"><tr class=\"head\"><th>transaction ID</th></tr>");
        for txid in &txids {
            let _ = write!(
                body,
                "<tr><td class=\"mono trunc\"><a href=\"/tx/{full}\">{full}</a></td></tr>",
                full = esc(&txid.to_string())
            );
        }
        body.push_str("</table></div>");
    }
    ok(page("mempool", &body))
}

/// このノードに繋がっているピアの一覧。
///
/// # 住所を伏せる
///
/// 公開の頁である。繋いでくれている人の住所をそのまま出すと、家の回線を
/// 晒すことになる。上位 2 区切りだけを残し ([`masked_ip`])、ポートも
/// 出さない。どの辺りの回線か (プロバイダや地域) が分かる程度に留める。
///
/// # 見えるのはこのノードの周りだけ
///
/// ネットワーク全体の一覧ではない。エクスプローラーを載せたノードが
/// いま握手を済ませている相手だけである。頁にもそう書く。
async fn peers_page(handle: &NodeHandle) -> Response {
    let peers = match handle.peers().await {
        Ok(peers) => peers,
        Err(e) => return error_page(&e),
    };
    let known = handle.status().await.map(|s| s.known_addresses).ok();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64);

    let outbound = peers.iter().filter(|p| p.outbound).count();
    let upgraded = peers
        .iter()
        .filter(|p| node_version(&p.user_agent).is_some_and(|v| v >= SOFT_FORK_VERSION))
        .count();

    let mut body = String::new();
    body.push_str(&search_box(""));
    body.push_str("<h1>peers</h1>");
    body.push_str(
        "<p class=\"note\">The nodes connected to the node behind this explorer right now, \
         not the whole network. Addresses are cut down to their first two parts.</p>",
    );

    body.push_str("<div class=\"grid\">");
    stat(&mut body, "connected", &peers.len().to_string());
    stat(&mut body, "outbound", &outbound.to_string());
    stat(&mut body, "inbound", &(peers.len() - outbound).to_string());
    stat(
        &mut body,
        "addresses known",
        &known.map_or_else(|| "—".to_string(), |n| group(n as u64)),
    );
    stat(
        &mut body,
        "on 0.4.0 or later",
        &format!("{upgraded} / {}", peers.len()),
    );
    body.push_str("</div>");

    if peers.is_empty() {
        body.push_str("<p class=\"note\">no peers are connected.</p>");
        return ok(page("peers", &body));
    }

    // 版ごとの数。多い順、同数なら名前順。
    let mut versions: Vec<(&str, usize)> = Vec::new();
    for peer in &peers {
        match versions
            .iter_mut()
            .find(|(agent, _)| *agent == peer.user_agent)
        {
            Some((_, count)) => *count += 1,
            None => versions.push((&peer.user_agent, 1)),
        }
    }
    versions.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    body.push_str("<h2>versions</h2><div class=\"wrap\"><table class=\"list\">");
    body.push_str("<tr class=\"head\"><th>version</th><th class=\"num\">peers</th></tr>");
    for (agent, count) in &versions {
        let _ = write!(
            body,
            "<tr><td class=\"mono\">{}</td><td class=\"num\" data-label=\"peers\">{count}</td></tr>",
            esc(agent)
        );
    }
    body.push_str("</table></div>");

    body.push_str("<h2>connected now</h2><div class=\"wrap\"><table class=\"list\">");
    body.push_str(
        "<tr class=\"head\"><th>address</th><th>direction</th><th>version</th>\
         <th class=\"num\">height when connected</th><th>serves</th>\
         <th class=\"num\">connected for</th></tr>",
    );
    for peer in &peers {
        let _ = write!(
            body,
            "<tr><td class=\"mono\">{address}</td>\
             <td data-label=\"direction\">{direction}</td>\
             <td class=\"mono\" data-label=\"version\">{agent}</td>\
             <td class=\"num\" data-label=\"height when connected\">{height}</td>\
             <td data-label=\"serves\">{serves}</td>\
             <td class=\"num\" data-label=\"connected for\">{since}</td></tr>",
            address = esc(&masked_ip(peer.addr.ip())),
            direction = if peer.outbound { "outbound" } else { "inbound" },
            agent = esc(&peer.user_agent),
            height = group(peer.start_height),
            serves = serves(peer),
            since = crate::pool_page::ago(now.saturating_sub(peer.since).max(0) as u64),
        );
    }
    body.push_str("</table></div>");
    ok(page("peers", &body))
}

/// 高さ 40,000 からの相対 locktime を強制する最初の版。
const SOFT_FORK_VERSION: (u32, u32, u32) = (0, 4, 0);

/// 名乗り (`/oag-node:0.4.1/`) から版数を読む。
///
/// **名乗りは相手が好きに決める。** 読めなければ `None` とし、数えない。
/// 嘘をつかれても、この頁の数字が狂うだけで害は無い。
fn node_version(user_agent: &str) -> Option<(u32, u32, u32)> {
    let version = user_agent.strip_prefix("/oag-node:")?.split('/').next()?;
    let mut parts = version.splitn(3, '.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    // `0.4.1-dev` のような後ろ書きは読み飛ばす。
    let patch: String = parts
        .next()?
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    Some((major, minor, patch.parse().ok()?))
}

/// 相手が配れると名乗ったもの。
fn serves(peer: &ConnectedPeer) -> &'static str {
    let services = oag_net::message::effective_services(peer.protocol_version, peer.services);
    if services & oag_net::message::SERVICE_FULL_NODE != 0 {
        "all blocks"
    } else if services & oag_net::message::SERVICE_LIMITED != 0 {
        "recent blocks"
    } else {
        "none"
    }
}

/// 住所を上位 2 区切りだけ残して伏せる。
///
/// IPv4 は `203.168.*.*`、IPv6 は `2001:db8:*` の形にする。IPv4 を
/// 写した IPv6 (`::ffff:a.b.c.d`) は IPv4 として扱う。
pub(crate) fn masked_ip(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, _, _] = v4.octets();
            format!("{a}.{b}.*.*")
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return masked_ip(IpAddr::V4(v4));
            }
            let [a, b, ..] = v6.segments();
            format!("{a:x}:{b:x}:*")
        }
    }
}

// ━━━━━━━━ 部品 ━━━━━━━━

/// ブロック内の取引表の頁送り。区切る必要が無ければ空文字列を返す。
///
/// # なぜ高さではなくハッシュで繋ぐのか
///
/// 高さで繋ぐと、**サイドチェーンのブロックを開いているときに、続きが
/// アクティブチェーンの同じ高さの別のブロックへ飛ぶ。** ハッシュなら
/// 開いている当のブロックを指す。
fn tx_pager(hash: &Hash, from: usize, upto: usize, total: usize) -> String {
    let here = esc(&hash.to_string());
    let mut pager = String::new();
    if from > 0 {
        let _ = write!(
            pager,
            "<a href=\"/block/{here}?from={f}\">&larr; previous {p}</a> ",
            f = from.saturating_sub(PAGE),
            p = PAGE
        );
    }
    if upto < total {
        let _ = write!(
            pager,
            "<a href=\"/block/{here}?from={f}\">next {p} &rarr;</a>",
            f = upto,
            p = PAGE
        );
    }
    if pager.is_empty() {
        return String::new();
    }
    format!("<p class=\"nav\">{pager}</p>")
}

/// ブロック内の取引を `from` 番目から `upto` 番目の手前まで並べる。
///
/// 位置の番号は**ブロック内の通し番号**であり、頁の中の番号ではない。
/// 0 は必ずコインベースである。
fn tx_table(block: &Block, network: Network, from: usize, upto: usize) -> String {
    let mut out = String::from("<div class=\"wrap\"><table class=\"list\">");
    out.push_str("<tr class=\"head\"><th>#</th><th>transaction ID</th><th>in</th><th>out</th><th>total</th></tr>");
    for (position, tx) in block.transactions[from..upto].iter().enumerate() {
        let position = from + position;
        let total = Amount::sum(tx.outputs.iter().map(|o| o.amount));
        let _ = write!(
            out,
            "<tr><td data-label=\"#\">{p}{cb}</td>\
             <td data-label=\"transaction ID\" class=\"mono trunc\"><a href=\"/tx/{full}\">{short}</a></td>\
             <td data-label=\"in\">{i}</td><td data-label=\"out\">{o}</td>\
             <td data-label=\"total\" class=\"num\">{t} OAG</td></tr>",
            p = position,
            cb = if tx.is_coinbase() {
                " <span class=\"tag\">mined</span>"
            } else {
                ""
            },
            full = esc(&tx.txid().to_string()),
            short = esc(&shorten(&tx.txid().to_string())),
            i = tx.inputs.len(),
            o = tx.outputs.len(),
            t = total
                .map(|a| esc(&a.to_string()))
                .unwrap_or_else(|| "?".to_string()),
        );
    }
    let _ = network;
    out.push_str("</table></div>");
    out
}

fn history_table(records: &[TxRecord], lock: &Lock, network: Network) -> String {
    let mut out = String::from("<div class=\"wrap\"><table class=\"list\">");
    out.push_str("<tr class=\"head\"><th>height</th><th>transaction ID</th><th>change</th></tr>");
    for record in records {
        // このアドレスから見た増減を出す。受け取った出力の合計から、
        // 使った入力の合計を引く。
        let received: u128 = record
            .tx
            .outputs
            .iter()
            .filter(|o| o.lock == *lock)
            .map(|o| o.amount.to_atomic())
            .sum();
        let sent: u128 = record
            .spent
            .iter()
            .flatten()
            .filter(|o| o.lock == *lock)
            .map(|o| o.amount.to_atomic())
            .sum();
        let delta = received as i128 - sent as i128;
        let class = if delta >= 0 { "plus" } else { "minus" };
        let _ = write!(
            out,
            "<tr><td data-label=\"height\"><a href=\"/block/{h}\">{h}</a></td>\
             <td data-label=\"transaction ID\" class=\"mono trunc\"><a href=\"/tx/{full}\">{short}</a></td>\
             <td data-label=\"change\" class=\"num {class}\">{sign}{amount} OAG</td></tr>",
            h = record.location.height,
            full = esc(&record.txid_text()),
            short = esc(&shorten(&record.txid_text())),
            class = class,
            sign = if delta >= 0 { "+" } else { "−" },
            amount = esc(&atomic_to_oag(delta.unsigned_abs())),
        );
    }
    let _ = network;
    out.push_str("</table></div>");
    out
}

fn record_detail(record: &TxRecord, network: Network) -> String {
    let mut out = String::new();
    out.push_str("<div class=\"wrap\"><table class=\"kv\">");
    row_raw(&mut out, "status", "<span class=\"ok\">confirmed</span>");
    row_raw(
        &mut out,
        "block",
        &format!("<a href=\"/block/{h}\">{h}</a>", h = record.location.height),
    );
    row_raw(
        &mut out,
        "block hash",
        &format!(
            "<a class=\"mono break\" href=\"/block/{b}\">{b}</a>",
            b = esc(&record.location.block.to_string())
        ),
    );
    row(
        &mut out,
        "index within the block",
        &record.location.position.to_string(),
    );
    out.push_str("</table></div>");
    out.push_str(&tx_detail(&record.tx, &record.spent, network));
    out
}

fn tx_detail(
    tx: &Transaction,
    spent: &[Option<oag_consensus::tx::TxOutput>],
    network: Network,
) -> String {
    let mut out = String::new();

    out.push_str("<div class=\"wrap\"><table class=\"kv\">");
    row(&mut out, "version", &tx.version.to_string());
    row(&mut out, "size", &format!("{} B", group(tx.size() as u64)));
    row(&mut out, "locktime", &tx.locktime.to_string());
    row(
        &mut out,
        "kind",
        if tx.is_coinbase() {
            "mined (coinbase)"
        } else {
            "ordinary"
        },
    );

    let out_total: u128 = tx.outputs.iter().map(|o| o.amount.to_atomic()).sum();
    // 入力側が全部引けているときだけ手数料を出す。1 つでも欠けていれば
    // 出さない。引けなかった分を 0 として足すと、手数料を多く見せる。
    let in_known =
        !tx.is_coinbase() && spent.len() == tx.inputs.len() && spent.iter().all(|s| s.is_some());
    if in_known {
        let in_total: u128 = spent.iter().flatten().map(|o| o.amount.to_atomic()).sum();
        row(
            &mut out,
            "fee",
            &format!("{} OAG", atomic_to_oag(in_total.saturating_sub(out_total))),
        );
    }
    row(
        &mut out,
        "total out",
        &format!("{} OAG", atomic_to_oag(out_total)),
    );
    out.push_str("</table></div>");

    // 入力。
    out.push_str("<h2>inputs</h2><div class=\"wrap\"><table class=\"list\">");
    if tx.is_coinbase() {
        out.push_str(
            "<tr><td class=\"note\">newly issued by mining. no outputs were spent.</td></tr>",
        );
    } else {
        out.push_str(
            "<tr class=\"head\"><th>source transaction</th><th>index</th><th>address</th><th>amount</th></tr>",
        );
        for (n, input) in tx.inputs.iter().enumerate() {
            let prev = spent.get(n).and_then(|s| s.as_ref());
            let (addr, amount) = match prev {
                Some(prev) => (
                    prev.lock
                        .to_address(network)
                        .ok()
                        .map(|a| a.to_string())
                        .unwrap_or_else(|| format!("version {}", prev.lock.version())),
                    format!("{} OAG", prev.amount),
                ),
                // 索引が無い、あるいは未確定の取引では引けない。
                None => ("unknown".to_string(), "unknown".to_string()),
            };
            let _ = write!(
                out,
                "<tr><td data-label=\"source transaction\" class=\"mono trunc\"><a href=\"/tx/{full}\">{short}</a></td>\
                 <td data-label=\"index\">{i}</td>\
                 <td data-label=\"address\" class=\"mono trunc\"><a href=\"/address/{a}\">{a_short}</a></td>\
                 <td data-label=\"amount\" class=\"num\">{amount}</td></tr>",
                full = esc(&input.prev_out.txid.to_string()),
                short = esc(&shorten(&input.prev_out.txid.to_string())),
                i = input.prev_out.index,
                a = esc(&addr),
                a_short = esc(&shorten(&addr)),
                amount = esc(&amount),
            );
        }
    }
    out.push_str("</table></div>");

    // 出力。
    out.push_str("<h2>outputs</h2><div class=\"wrap\"><table class=\"list\">");
    out.push_str("<tr class=\"head\"><th>#</th><th>address</th><th>amount</th></tr>");
    for (n, output) in tx.outputs.iter().enumerate() {
        // コミットメント出力は誰の支払いでもない。アドレスにせず、刻まれた
        // 中身をそのまま見せる (SPEC §7.7)。
        if output.lock.is_commitment() {
            let hex: String = output
                .lock
                .payload()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            let _ = write!(
                out,
                "<tr><td data-label=\"#\">{n}</td>\
                 <td data-label=\"address\" class=\"mono\">commitment: {hex}</td>\
                 <td data-label=\"amount\" class=\"num\">{amount} OAG</td></tr>",
                hex = esc(&hex),
                amount = esc(&output.amount.to_string()),
            );
            continue;
        }
        let addr = output
            .lock
            .to_address(network)
            .ok()
            .map(|a| a.to_string())
            .unwrap_or_else(|| format!("version {} (unknown)", output.lock.version()));
        let _ = write!(
            out,
            "<tr><td data-label=\"#\">{n}</td>\
             <td data-label=\"address\" class=\"mono trunc\"><a href=\"/address/{a}\">{a_short}</a></td>\
             <td data-label=\"amount\" class=\"num\">{amount} OAG</td></tr>",
            n = n,
            a = esc(&addr),
            a_short = esc(&shorten(&addr)),
            amount = esc(&output.amount.to_string()),
        );
    }
    out.push_str("</table></div>");
    out
}

fn search_box(value: &str) -> String {
    format!(
        "<form class=\"search\" action=\"/search\" method=\"get\">\
         <input name=\"q\" value=\"{}\" placeholder=\"height, block, transaction ID or address\" \
         autocapitalize=\"off\" autocomplete=\"off\" spellcheck=\"false\">\
         <button type=\"submit\">search</button></form>",
        esc(value)
    )
}

fn stat(out: &mut String, label: &str, value: &str) {
    let _ = write!(
        out,
        "<div class=\"stat\"><div class=\"label\">{}</div><div class=\"value\">{}</div></div>",
        esc(label),
        value
    );
}

fn row(out: &mut String, label: &str, value: &str) {
    let _ = write!(
        out,
        "<tr><th>{}</th><td>{}</td></tr>",
        esc(label),
        esc(value)
    );
}

fn row_raw(out: &mut String, label: &str, value: &str) {
    let _ = write!(out, "<tr><th>{}</th><td>{}</td></tr>", esc(label), value);
}

fn ok(body: String) -> Response {
    Response {
        status: "200 OK",
        kind: Kind::Html,
        body,
    }
}

fn not_found(message: &str) -> Response {
    Response {
        status: "404 Not Found",
        kind: Kind::Html,
        body: page(
            "not found",
            &format!(
                "{}<h1>not found</h1><p>{}</p>",
                search_box(""),
                esc(message)
            ),
        ),
    }
}

fn error_page(message: &str) -> Response {
    Response {
        status: "500 Internal Server Error",
        kind: Kind::Html,
        body: page(
            "error",
            &format!("{}<h1>error</h1><p>{}</p>", search_box(""), esc(message)),
        ),
    }
}

/// JSON の応答。
fn json_ok(body: String) -> Response {
    Response {
        status: "200 OK",
        kind: Kind::Json,
        body,
    }
}

/// JSON の誤り。`{"error": "…"}` の形で返す。
fn json_error(status: &'static str, message: &str) -> Response {
    Response {
        status,
        kind: Kind::Json,
        body: serde_json::json!({ "error": message }).to_string(),
    }
}

fn page(title: &str, body: &str) -> String {
    format!(
        "<!doctype html><html lang=\"ja\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <meta name=\"robots\" content=\"noindex, nofollow\">\
         <title>{title} · Orange</title><style>{css}</style></head>\
         <body><header><a class=\"brand\" href=\"/\">Orange <span>OAG</span></a>\
         <nav><a href=\"/\">overview</a> <a href=\"/stats\">stats</a> <a href=\"/mempool\">mempool</a> <a href=\"/peers\">peers</a></nav></header>\
         <main>{body}</main>\
         <footer>A local, read-only explorer. It cannot send coins or change settings.</footer>\
         </body></html>",
        title = esc(title),
        css = CSS,
        body = body,
    )
}

pub(crate) const CSS: &str = "\
:root{--bg:#fff;--fg:#1a1a1a;--dim:#666;--line:#e3e3e3;--accent:#e8720c;--card:#faf9f7;--s1:#d9650a;--s2:#2f6fd6;}\
@media (prefers-color-scheme:dark){:root{--bg:#16150f;--fg:#ececec;--dim:#9a9a9a;--line:#333;--accent:#ff9f45;--card:#1f1e18;--s1:#d6711c;--s2:#5a84dc;}}\
*{box-sizing:border-box}\
body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.6 system-ui,-apple-system,'Hiragino Sans','Noto Sans JP',sans-serif;}\
header{display:flex;flex-wrap:wrap;gap:12px;align-items:baseline;justify-content:space-between;\
padding:14px 16px;border-bottom:1px solid var(--line);}\
.brand{font-weight:700;font-size:18px;color:var(--accent);text-decoration:none}\
.brand span{color:var(--dim);font-weight:400;font-size:13px}\
nav a{color:var(--dim);text-decoration:none;margin-left:12px}\
nav a:hover{color:var(--accent)}\
main{max-width:960px;margin:0 auto;padding:16px;}\
footer{max-width:960px;margin:0 auto;padding:24px 16px;color:var(--dim);font-size:12px}\
h1{font-size:20px;margin:16px 0 4px}h2{font-size:16px;margin:28px 0 8px}\
a{color:var(--accent)}\
.search{display:flex;gap:8px;margin:8px 0 20px}\
.search input{flex:1;min-width:0;padding:10px 12px;border:1px solid var(--line);border-radius:8px;\
background:var(--card);color:var(--fg);font-size:16px}\
.search button{padding:10px 16px;border:0;border-radius:8px;background:var(--accent);color:#fff;font-size:15px;cursor:pointer}\
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin:12px 0}\
.stat{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:10px 12px}\
.stat .label{color:var(--dim);font-size:12px}\
.stat .value{font-size:18px;font-variant-numeric:tabular-nums;margin-top:2px}\
.wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;border:1px solid var(--line);border-radius:10px}\
table{border-collapse:collapse;width:100%;min-width:480px}\
th,td{padding:8px 12px;text-align:left;border-bottom:1px solid var(--line);white-space:nowrap}\
tr:last-child th,tr:last-child td{border-bottom:0}\
th{color:var(--dim);font-weight:500;font-size:13px}\
.num{text-align:right;font-variant-numeric:tabular-nums}\
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:13px}\
.break{word-break:break-all;white-space:normal}\
.trunc{max-width:22ch;overflow:hidden;text-overflow:ellipsis}\
table.kv{min-width:0}.kv th{width:1%}.kv td{white-space:normal;overflow-wrap:anywhere}\
@media (max-width:600px){\
table.list{min-width:0}.list,.list tbody,.list tr,.list td{display:block}.list tr.head{display:none}\
.list tr{padding:8px 12px;border-bottom:1px solid var(--line)}.list tr:last-child{border-bottom:0}\
.list td{display:flow-root;padding:2px 0;border:0;white-space:normal;text-align:right;overflow-wrap:anywhere}\
.list td.trunc{max-width:none;overflow:visible}.list td:not([data-label]){text-align:left}.list td.wide{display:none}\
.list td[data-label]::before{content:attr(data-label);float:left;margin-right:12px;color:var(--dim);\
font-family:system-ui,-apple-system,sans-serif;font-size:13px;line-height:24px}}\
.note{color:var(--dim);font-size:13px}\
.warn{color:#d97706}.ok{color:#16a34a}\
.plus{color:#16a34a}.minus{color:#dc2626}\
.tag{background:var(--card);border:1px solid var(--line);border-radius:4px;padding:0 4px;font-size:11px;color:var(--dim)}\
.nav{margin:16px 0}\
code{background:var(--card);border:1px solid var(--line);border-radius:4px;padding:1px 5px;font-size:13px}\
.chart{margin:16px 0 28px}.chart figcaption{margin-bottom:8px}\
.legend{display:flex;flex-wrap:wrap;gap:4px 16px;font-size:12px;color:var(--dim);margin-bottom:6px}\
.key{display:inline-block;width:14px;height:2px;border-radius:1px;vertical-align:middle;margin-right:6px}\
.key.s1{background:var(--s1)}.key.s2{background:var(--s2)}\
.plot{position:relative;height:160px;margin:8px 0 0 64px}\
.plot svg{display:block;width:100%;height:100%;overflow:visible}\
.plot .y{position:absolute;left:-64px;width:58px;text-align:right;transform:translateY(-50%);\
font-size:11px;color:var(--dim);font-variant-numeric:tabular-nums;white-space:nowrap}\
.gridline{stroke:var(--line);stroke-width:1}\
.line{fill:none;stroke-width:2;stroke-linejoin:round;stroke-linecap:round}\
.line.s1{stroke:var(--s1)}.line.s2{stroke:var(--s2)}\
.hit{fill:transparent}.hit:hover{fill:var(--line);fill-opacity:.6}\
.xaxis{display:flex;justify-content:space-between;margin-left:64px;font-size:11px;color:var(--dim)}\
details summary{cursor:pointer;color:var(--dim);font-size:13px;margin:8px 0}\
";

// ━━━━━━━━ 変換 ━━━━━━━━

/// HTML に埋め込むための逃がし。
///
/// **経路もクエリも利用者が決める文字列である。** そのまま埋めると、
/// 局所の頁とはいえ任意の script を仕込める。すべての埋め込みはここを通す。
pub(crate) fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// 長い識別子を頭と尻だけにする。
pub(crate) fn shorten(s: &str) -> String {
    if s.chars().count() <= 20 {
        return s.to_string();
    }
    let chars: Vec<char> = s.chars().collect();
    let head: String = chars[..10].iter().collect();
    let tail: String = chars[chars.len() - 6..].iter().collect();
    format!("{head}…{tail}")
}

/// 3 桁ごとに区切る。
pub(crate) fn group(n: u64) -> String {
    let text = n.to_string();
    let mut out = String::with_capacity(text.len() + text.len() / 3);
    for (i, c) in text.chars().enumerate() {
        if i > 0 && (text.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// 最小単位を OAG の表記にする。
pub(crate) fn atomic_to_oag(atomic: u128) -> String {
    match Amount::from_atomic(atomic) {
        Ok(amount) => amount.to_string(),
        Err(_) => atomic.to_string(),
    }
}

/// Unix 秒を UTC の表記にする。
///
/// 暦の変換は Howard Hinnant の `civil_from_days` による。外部の暦を
/// 引き込むほどの用途ではない。
pub(crate) fn utc(timestamp: i64) -> String {
    let days = timestamp.div_euclid(86_400);
    let secs = timestamp.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}",
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    )
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// パーセント符号化を戻す。
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// クエリ文字列から 1 つ取り出す。
fn query_value(query: &str, name: &str) -> Option<String> {
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=')?;
        if key == name {
            return Some(percent_decode(value));
        }
    }
    None
}

impl TxRecord {
    fn txid_text(&self) -> String {
        self.location.txid.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn everything_that_goes_into_a_page_is_escaped() {
        assert_eq!(esc("<script>"), "&lt;script&gt;");
        assert_eq!(esc("a&b"), "a&amp;b");
        assert_eq!(esc("\"><svg"), "&quot;&gt;&lt;svg");
        assert_eq!(esc("it's"), "it&#39;s");
        // 日本語はそのまま通る。
        assert_eq!(esc("height"), "height");
    }

    #[test]
    fn percent_encoding_is_undone() {
        assert_eq!(percent_decode("/address/%3Cscript%3E"), "/address/<script>");
        assert_eq!(percent_decode("a+b"), "a b");
        // 半端な % はそのまま残す。落とすと別の文字列になる。
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz"), "%zz");
    }

    #[test]
    fn a_query_value_is_picked_out_by_name() {
        assert_eq!(query_value("q=abc&from=5", "q").as_deref(), Some("abc"));
        assert_eq!(query_value("q=abc&from=5", "from").as_deref(), Some("5"));
        assert_eq!(query_value("q=abc", "missing"), None);
        assert_eq!(query_value("", "q"), None);
    }

    #[test]
    fn chart_scales_use_round_steps() {
        assert_eq!(nice_scale(100.0, false), (100.0, 50.0));
        assert_eq!(nice_scale(4_200.0, false), (6_000.0, 2_000.0));
        assert_eq!(nice_scale(7.0, true), (8.0, 2.0));
        assert_eq!(nice_scale(0.0, false), (1.0, 1.0));
        assert_eq!(nice_scale(f64::NAN, false), (1.0, 1.0));
        // 人数に半端な目盛りは振らない。
        assert_eq!(nice_scale(2.0, false), (2.0, 0.5));
        assert_eq!(nice_scale(2.0, true), (2.0, 1.0));
    }

    #[test]
    fn amounts_in_tables_are_rounded_to_cents() {
        let unit = oag_primitives::amount::ATOMIC_PER_OAG;
        assert_eq!(whole_oag(95_179 * unit + unit / 2), "95,179.50");
        assert_eq!(whole_oag(unit / 1000), "0.00");
        assert_eq!(whole_oag(unit - 1), "1.00");
        assert_eq!(percent(0.6949), "69.5%");
    }

    #[test]
    fn a_chart_has_a_line_per_series_and_a_hover_target_per_day() {
        let labels = vec!["a<b".to_string(), "c".to_string(), "d".to_string()];
        let html = line_chart(
            "t<i>tle",
            &labels,
            &[
                ("one", "s1", vec![Some(1.0), None, Some(3.0)]),
                ("two", "s2", vec![Some(2.0), Some(2.0), Some(2.0)]),
            ],
            false,
            &|v| format!("{v:.0}"),
        );
        assert_eq!(html.matches("<polyline").count(), 2);
        assert_eq!(html.matches("class=\"hit\"").count(), 3);
        // 2 系列なので凡例がある。欠けた日は線から外し、表示は「—」。
        assert!(html.contains("class=\"legend\""));
        assert!(html.contains("one: —"));
        // 利用者の決めた文字列ではないが、見出しも日付も逃がす。
        assert!(!html.contains("t<i>tle") && !html.contains("a<b"));
        let single = line_chart(
            "x",
            &labels[..1],
            &[("x", "s1", vec![Some(5.0)])],
            true,
            &|v| format!("{v}"),
        );
        assert!(!single.contains("class=\"legend\""));
    }

    #[test]
    fn timestamps_are_shown_as_utc() {
        assert_eq!(utc(0), "1970-01-01 00:00:00");
        assert_eq!(utc(1_700_000_000), "2023-11-14 22:13:20");
        // 閏年の 2 月 29 日を跨ぐところ。
        assert_eq!(utc(1_709_164_800), "2024-02-29 00:00:00");
        // 1970 より前も崩れない。
        assert_eq!(utc(-1), "1969-12-31 23:59:59");
    }

    #[test]
    fn long_identifiers_are_shortened_in_the_middle() {
        let hash = "0".repeat(64);
        let short = shorten(&hash);
        assert!(short.contains('…'));
        assert!(short.len() < hash.len());
        // 短いものはそのまま。
        assert_eq!(shorten("abc"), "abc");
    }

    /// 取引を `count` 件持つブロック。0 番目はコインベースにする。
    fn block_of(count: usize) -> Block {
        use oag_consensus::tx::{OutPoint, TxInput, TxOutput, CURRENT_TX_VERSION};
        use oag_consensus::BlockHeader;
        use oag_primitives::{hash, SecretKey};

        let lock = Lock::pay_to_pubkey(&SecretKey::generate().public_key());
        let transactions = (0..count)
            .map(|i| Transaction {
                version: CURRENT_TX_VERSION,
                inputs: vec![if i == 0 {
                    TxInput::new(OutPoint::null())
                } else {
                    TxInput::new(OutPoint::new(hash::txid(&[i as u8]), 0))
                }],
                outputs: vec![TxOutput::new(
                    Amount::from_atomic(i as u128 + 1).unwrap(),
                    lock.clone(),
                )],
                locktime: 0,
            })
            .collect();
        Block {
            header: BlockHeader {
                version: 0,
                prev_hash: Hash::ZERO,
                merkle_root: Hash::ZERO,
                timestamp: 1_800_000_000,
                difficulty: 1,
                height: 7,
                nonce: 0,
            },
            transactions,
        }
    }

    /// `miners` の順に掘られたブロックを数えた集計。同じ番号は同じ鍵。
    fn tally_mined_by(miners: &[u8]) -> Tally {
        use oag_primitives::SecretKey;
        let mut tally = Tally::new();
        for (i, miner) in miners.iter().enumerate() {
            let mut block = block_of(1);
            let secret = SecretKey::from_bytes([*miner; 32]).unwrap();
            block.transactions[0].outputs[0].lock = Lock::pay_to_pubkey(&secret.public_key());
            block.header.height = i as u64 + 1;
            block.header.timestamp += 60 * i as i64;
            tally.add(&block, Hash::ZERO);
        }
        tally
    }

    #[test]
    fn a_miner_with_more_than_half_of_the_blocks_is_called_out() {
        let html = miner_shares(&tally_mined_by(&[1, 1, 1, 2]), Network::Mainnet);
        assert!(html.contains("the last 4 blocks"), "{html}");
        assert!(html.contains("class=\"warn\""), "{html}");
        assert!(html.contains("75.0%"), "{html}");
    }

    #[test]
    fn exactly_half_is_not_a_majority() {
        let html = miner_shares(&tally_mined_by(&[1, 1, 2, 3]), Network::Mainnet);
        assert!(!html.contains("class=\"warn\""), "{html}");
        assert!(html.contains("50.0%"), "{html}");
    }

    #[test]
    fn miners_beyond_the_first_ten_are_folded_into_one_row() {
        let miners: Vec<u8> = (1..=12).collect();
        let html = miner_shares(&tally_mined_by(&miners), Network::Mainnet);
        assert!(html.contains("2 more addresses"), "{html}");
        assert_eq!(html.matches("/address/").count(), 10, "{html}");
    }

    #[test]
    fn a_page_of_transactions_keeps_the_numbering_of_the_whole_block() {
        // **頁の中の番号ではなく、ブロック内の通し番号を出す。** ここが
        // ずれると、表の `#` が索引の位置と食い違い、`/tx/` の照合結果と
        // 説明がつかなくなる。
        let block = block_of(5);
        let table = tx_table(&block, Network::Mainnet, 2, 5);

        assert!(
            table.contains("<td data-label=\"#\">2</td>"),
            "it does not start from the second"
        );
        assert!(
            table.contains("<td data-label=\"#\">4</td>"),
            "the last, fourth one is missing"
        );
        assert!(
            !table.contains("<td data-label=\"#\">0</td>"),
            "the zeroth, outside the page, is shown"
        );
        assert!(
            !table.contains("<td data-label=\"#\">5</td>"),
            "a fifth that does not exist is shown"
        );

        // 採掘の印は 0 番目だけのもの。2 件目以降の頁には出ない。
        assert!(
            !table.contains("mined"),
            "shown as mined although it is not a coinbase"
        );

        // 先頭の頁には出る。
        let first = tx_table(&block, Network::Mainnet, 0, 2);
        assert!(first.contains("mined"));
    }

    #[test]
    fn the_pager_points_at_the_block_being_read_not_at_a_height() {
        // **高さで繋ぐと、サイドチェーンのブロックを開いているとき、
        // 続きがアクティブチェーンの別のブロックへ飛ぶ。**
        let hash = block_of(1).header.hash();
        let here = hash.to_string();

        // 真ん中の頁。前にも次にも行ける。
        let mid = tx_pager(&hash, PAGE, PAGE * 2, PAGE * 3);
        assert!(mid.contains(&format!("/block/{here}?from=0")), "{mid}");
        assert!(
            mid.contains(&format!("/block/{here}?from={}", PAGE * 2)),
            "{mid}"
        );

        // 先頭の頁に「前」は無い。
        let first = tx_pager(&hash, 0, PAGE, PAGE * 2);
        assert!(!first.contains("previous "));
        assert!(first.contains("next "));

        // 末尾の頁に「次」は無い。
        let last = tx_pager(&hash, PAGE, PAGE * 2, PAGE * 2);
        assert!(last.contains("previous "));
        assert!(!last.contains("next "));

        // 区切る必要が無ければ何も出さない。
        assert_eq!(tx_pager(&hash, 0, 3, 3), "");
    }

    #[test]
    fn an_empty_range_renders_a_table_with_no_rows() {
        // 高さの末尾を越えて `from` を指定されても落ちない。
        let block = block_of(3);
        let table = tx_table(&block, Network::Mainnet, 3, 3);
        assert!(table.contains("<th>#</th>"));
        assert!(!table.contains("/tx/"));
    }

    #[test]
    fn peer_addresses_keep_only_their_first_two_parts() {
        assert_eq!(masked_ip("203.168.96.48".parse().unwrap()), "203.168.*.*");
        assert_eq!(
            masked_ip("::ffff:68.233.113.1".parse().unwrap()),
            "68.233.*.*"
        );
        assert_eq!(
            masked_ip("2001:db8:85a3::8a2e:370:7334".parse().unwrap()),
            "2001:db8:*"
        );
    }

    #[test]
    fn a_node_version_is_read_from_the_user_agent() {
        assert_eq!(node_version("/oag-node:0.4.1/"), Some((0, 4, 1)));
        assert_eq!(node_version("/oag-node:0.10.0-dev/"), Some((0, 10, 0)));
        assert_eq!(node_version("/oag-node:0.3/"), None);
        assert_eq!(node_version("/Satoshi:27.0.0/"), None);
        assert_eq!(node_version("<script>"), None);
        assert!(node_version("/oag-node:0.3.0/").unwrap() < SOFT_FORK_VERSION);
        assert!(node_version("/oag-node:0.4.0/").unwrap() >= SOFT_FORK_VERSION);
    }

    #[test]
    fn numbers_are_grouped_in_threes() {
        assert_eq!(group(0), "0");
        assert_eq!(group(999), "999");
        assert_eq!(group(1_000), "1,000");
        assert_eq!(group(24_850), "24,850");
        assert_eq!(group(1_234_567), "1,234,567");
    }
}
