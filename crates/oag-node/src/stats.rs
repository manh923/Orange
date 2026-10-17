//! 鎖の統計。エクスプローラの `/stats` と `/richlist`、`/api/stats` が出す
//! 数字を数える。
//!
//! # 何を数えるか
//!
//! - ブロック 1 からの日数と、平均のブロック間隔
//! - ハッシュレート (難易度と時刻から逆算した推定)
//! - 採掘したことのあるアドレスの数、直近 7 日に採掘した数
//! - 直近 7 日にコインを受け取ったアドレスの数
//! - 直近 100 ブロックを誰が掘ったか (採掘の偏り)
//! - 取引の数 (採掘の報酬を除く)
//! - アドレスごとの残高 (保有の分布、上位の一覧)
//! - 以上の日ごとの推移 (UTC の日付で区切る)
//!
//! # ハッシュレートの出し方
//!
//! ターゲットは `(2^256 − 1) / 難易度` なので、1 ブロックを見つけるのに
//! 要るハッシュ数の期待値は**難易度そのもの**である。直近 n ブロックの
//! 難易度の和を、その n ブロックにかかった時間で割れば、その間の
//! 平均のハッシュレートになる。1 ブロックだけで割ると運に振り回される
//! ので、幅を持たせる。日ごとの値は、その日のブロックの難易度の和を
//! その日の長さ (鎖が動いていた分) で割る。
//!
//! # 残高を自分で追う
//!
//! 日ごとの保有の推移は、その日の終わりの残高から出す。ノードの UTXO
//! セットは今の姿しか持たないので、ここで**出力の出入りを自分で追う**。
//! 持つのは使われていない出力の支払い先と金額だけで、大きさは UTXO
//! セットと同じ程度である。
//!
//! # 数え直しを避ける
//!
//! 一度数えたところまでを覚えておき、次は新しいブロックの分だけ足す。
//! 覚えた先端が鎖から外れていたら (再編成)、最初から数え直す。
//! 滅多に起きず、起きても数秒で済む。

use crate::node::NodeStatus;
use crate::service::NodeHandle;
use oag_consensus::codec::Encode;
use oag_consensus::lock::Lock;
use oag_consensus::params::block_subsidy;
use oag_consensus::tx::OutPoint;
use oag_consensus::Block;
use oag_primitives::{Amount, Hash};
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};

/// 「直近」とみなす期間 (秒)。
pub const RECENT_SECONDS: i64 = 7 * 24 * 60 * 60;
/// ハッシュレートの短い窓 (約 1 時間)。
pub const SHORT_WINDOW: usize = 60;
/// ハッシュレートの長い窓 (約 1 日)。
pub const LONG_WINDOW: usize = 1440;
/// 採掘の偏りを見る窓 (約 1 時間 40 分)。
///
/// 短すぎると運に振り回され、長すぎると大きな採掘者が来た・去ったことが
/// 数字に出るまでに時間がかかる。100 なら 1 人が 5 割を超えたかどうかを
/// 見るには十分で、半日も遅れない。
pub const SHARE_WINDOW: usize = 100;
/// 1 日の秒数。
const DAY: i64 = 86_400;

/// 1 ブロック分の覚え書き。直近の分だけ持つ。
#[derive(Debug, Clone)]
struct Recent {
    time: i64,
    difficulty: u64,
    miners: Vec<Lock>,
    receivers: Vec<Lock>,
}

/// 1 日分の数字。
#[derive(Debug, Clone, PartialEq)]
pub struct Day {
    /// 1970-01-01 からの日数 (UTC)。
    pub day: i64,
    /// その日のブロックの数。
    pub blocks: u64,
    /// その日のブロックの難易度の和。
    pub work: u128,
    /// その日のうち、鎖が動いていた秒数。
    pub seconds: i64,
    /// その日に採掘した支払い先の数。
    pub miners: usize,
    /// その日に初めて採掘した支払い先の数。
    pub new_miners: usize,
    /// その日の取引の数 (コインベースを除く)。
    pub transactions: u64,
    /// その日の終わりの保有の姿。
    pub holdings: Holdings,
    /// まだ終わっていない日か。
    pub partial: bool,
}

impl Day {
    /// その日の平均のハッシュレート (H/s)。
    pub fn hashrate(&self) -> Option<f64> {
        (self.seconds > 0).then(|| self.work as f64 / self.seconds as f64)
    }
}

/// ある時点の保有の姿。
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Holdings {
    /// 残高を持つ支払い先の数。
    pub holders: usize,
    /// 残高の合計 (最小単位)。
    pub supply: u128,
    /// 最も多く持つ 1 件の割合 (0〜1)。
    pub top1: f64,
    /// 上位 10 件の割合。
    pub top10: f64,
    /// 上位 100 件の割合。
    pub top100: f64,
}

/// 残高帯ごとの人数と金額。
#[derive(Debug, Clone, PartialEq)]
pub struct Band {
    /// 下限 (OAG、この値を含む)。
    pub from: u64,
    /// 上限 (OAG、この値を含まない)。`None` は上限なし。
    pub to: Option<u64>,
    /// 支払い先の数。
    pub holders: usize,
    /// 金額の合計 (最小単位)。
    pub amount: u128,
}

/// 残高帯の区切り (OAG)。
pub const BAND_EDGES: [u64; 5] = [1, 10, 100, 1_000, 10_000];

/// 開いている (まだ終わっていない) 日。
#[derive(Debug, Clone, Default)]
struct OpenDay {
    day: i64,
    blocks: u64,
    work: u128,
    miners: HashSet<Lock>,
    new_miners: usize,
    transactions: u64,
}

/// 数えた結果。
#[derive(Debug, Default)]
pub struct Tally {
    /// 数え終えた先端。
    tip: Option<(u64, Hash)>,
    /// ブロック 1 の時刻。
    first_time: Option<i64>,
    /// 先端の時刻。
    tip_time: i64,
    /// 採掘したことのある支払い先。
    miners: HashSet<Lock>,
    /// 取引の数 (コインベースを除く)。
    transactions: u64,
    /// 直近のブロック。古い順。
    recent: VecDeque<Recent>,
    /// 使われていない出力の支払い先と金額。
    unspent: HashMap<OutPoint, (Lock, u128)>,
    /// 支払い先ごとの残高。0 になったものは消す。
    balances: HashMap<Lock, u128>,
    /// 終わった日。
    days: Vec<Day>,
    /// 今の日。
    open: Option<OpenDay>,
}

impl Tally {
    /// 空の集計。
    pub fn new() -> Tally {
        Tally::default()
    }

    /// 次のブロックを足す。**高さの順に渡すこと。**
    ///
    /// ジェネシスは数えない。ジェネシスの時刻は鎖が動き出した時刻ではなく、
    /// 報酬は誰のものでもない (焼却されていて、UTXO セットにも入らない)。
    pub fn add(&mut self, block: &Block, hash: Hash) {
        let height = block.header.height;
        self.tip = Some((height, hash));
        if height == 0 {
            return;
        }
        let time = block.header.timestamp;
        if self.first_time.is_none() {
            self.first_time = Some(time);
        }
        self.tip_time = time;

        // 日が替わっていれば、前の日を閉じる。**残高はこのブロックを足す
        // 前の姿である。** それが前の日の終わりの姿になる。時刻は前後し
        // うるので、前の日付に見えるブロックは今の日に入れる。
        let today = time.div_euclid(DAY);
        match &self.open {
            Some(open) if today > open.day => {
                let closed = self.close_open(false);
                self.days.push(closed);
                self.open = Some(OpenDay {
                    day: today,
                    ..OpenDay::default()
                });
            }
            Some(_) => {}
            None => {
                self.open = Some(OpenDay {
                    day: today,
                    ..OpenDay::default()
                })
            }
        }

        let mut miners = Vec::new();
        let mut receivers = Vec::new();
        let mut first_time_miners = 0;
        let mut transactions = 0;
        for tx in &block.transactions {
            let coinbase = tx.is_coinbase();
            if coinbase {
                // コミットメント出力は誰の報酬でもない (SPEC §7.7)。数えない。
                for output in tx.outputs.iter().filter(|o| !o.lock.is_commitment()) {
                    miners.push(output.lock.clone());
                    if self.miners.insert(output.lock.clone()) {
                        first_time_miners += 1;
                    }
                }
            } else {
                transactions += 1;
                for input in &tx.inputs {
                    if let Some((lock, amount)) = self.unspent.remove(&input.prev_out) {
                        self.debit(&lock, amount);
                    }
                }
            }
            let txid = tx.txid();
            for (index, output) in tx.outputs.iter().enumerate() {
                // コミットメント出力は誰の持ち分でもなく、使われることもない。
                if output.lock.is_commitment() {
                    continue;
                }
                let amount = output.amount.to_atomic();
                self.unspent.insert(
                    OutPoint::new(txid, index as u32),
                    (output.lock.clone(), amount),
                );
                if amount > 0 {
                    *self.balances.entry(output.lock.clone()).or_insert(0) += amount;
                }
            }
            receivers.extend(
                tx.outputs
                    .iter()
                    .filter(|o| !o.lock.is_commitment())
                    .map(|o| o.lock.clone()),
            );
        }
        self.transactions += transactions;

        if let Some(open) = self.open.as_mut() {
            open.blocks += 1;
            open.work += u128::from(block.header.difficulty);
            open.miners.extend(miners.iter().cloned());
            open.new_miners += first_time_miners;
            open.transactions += transactions;
        }

        self.recent.push_back(Recent {
            time,
            difficulty: block.header.difficulty,
            miners,
            receivers,
        });

        // 古いものを捨てる。ハッシュレートの長い窓と 7 日の両方が要らなく
        // なったものだけを捨てる。
        while self.recent.len() > LONG_WINDOW + 1 {
            let front = &self.recent[0];
            if front.time >= time - RECENT_SECONDS {
                break;
            }
            self.recent.pop_front();
        }
    }

    fn debit(&mut self, lock: &Lock, amount: u128) {
        if let Some(balance) = self.balances.get_mut(lock) {
            *balance = balance.saturating_sub(amount);
            if *balance == 0 {
                self.balances.remove(lock);
            }
        }
    }

    /// 開いている日を、今の残高で締めた姿にする。
    fn close_open(&self, partial: bool) -> Day {
        let open = self.open.clone().unwrap_or_default();
        let start = (open.day * DAY).max(self.first_time.unwrap_or(i64::MIN));
        let end = if partial {
            self.tip_time
        } else {
            (open.day + 1) * DAY
        };
        Day {
            day: open.day,
            blocks: open.blocks,
            work: open.work,
            seconds: (end - start).max(0),
            miners: open.miners.len(),
            new_miners: open.new_miners,
            transactions: open.transactions,
            holdings: self.holdings(),
            partial,
        }
    }

    /// 日ごとの数字。古い順。最後の 1 件はまだ終わっていない今日である。
    pub fn days(&self) -> Vec<Day> {
        let mut days = self.days.clone();
        if self.open.is_some() {
            days.push(self.close_open(true));
        }
        days
    }

    /// 今の保有の姿。
    pub fn holdings(&self) -> Holdings {
        let mut amounts: Vec<u128> = self.balances.values().copied().collect();
        amounts.sort_unstable_by(|a, b| b.cmp(a));
        let supply: u128 = amounts.iter().sum();
        let share = |n: usize| {
            if supply == 0 {
                0.0
            } else {
                amounts.iter().take(n).sum::<u128>() as f64 / supply as f64
            }
        };
        Holdings {
            holders: amounts.len(),
            supply,
            top1: share(1),
            top10: share(10),
            top100: share(100),
        }
    }

    /// 残高の多い順に `n` 件。同額は支払い条件の符号化の順に並べる
    /// (頁を開くたびに順番が入れ替わらないように)。
    pub fn richest(&self, n: usize) -> Vec<(Lock, u128)> {
        let mut all: Vec<(Lock, u128)> =
            self.balances.iter().map(|(l, a)| (l.clone(), *a)).collect();
        all.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.encode().cmp(&b.0.encode())));
        all.truncate(n);
        all
    }

    /// 残高帯ごとの人数と金額。
    pub fn bands(&self) -> Vec<Band> {
        let unit = oag_primitives::amount::ATOMIC_PER_OAG;
        let mut bands: Vec<Band> = Vec::new();
        let mut from = 0u64;
        for edge in BAND_EDGES {
            bands.push(Band {
                from,
                to: Some(edge),
                holders: 0,
                amount: 0,
            });
            from = edge;
        }
        bands.push(Band {
            from,
            to: None,
            holders: 0,
            amount: 0,
        });
        for amount in self.balances.values() {
            let band = bands
                .iter_mut()
                .find(|b| b.to.is_none_or(|to| *amount < u128::from(to) * unit))
                .expect("the last band has no upper bound");
            band.holders += 1;
            band.amount += amount;
        }
        bands
    }

    /// 使われていない出力の数。
    pub fn unspent_outputs(&self) -> usize {
        self.unspent.len()
    }

    /// 数え終えた先端。
    pub fn tip(&self) -> Option<(u64, Hash)> {
        self.tip
    }

    /// ブロック 1 の時刻。
    pub fn first_time(&self) -> Option<i64> {
        self.first_time
    }

    /// ブロック 1 から先端までの平均の間隔 (秒)。
    pub fn average_interval(&self) -> Option<f64> {
        let (height, _) = self.tip?;
        let first = self.first_time?;
        if height < 2 {
            return None;
        }
        Some((self.tip_time - first) as f64 / (height - 1) as f64)
    }

    /// 直近 `blocks` 個のブロックから推定したハッシュレート (H/s)。
    ///
    /// ブロックが足りない、あるいは時刻が進んでいなければ `None`。
    pub fn hashrate(&self, blocks: usize) -> Option<f64> {
        if blocks == 0 || self.recent.len() < blocks + 1 {
            return None;
        }
        let end = self.recent.len() - 1;
        let start = end - blocks;
        let span = self.recent[end].time - self.recent[start].time;
        if span <= 0 {
            return None;
        }
        let work: u128 = self
            .recent
            .range(start + 1..=end)
            .map(|r| u128::from(r.difficulty))
            .sum();
        Some(work as f64 / span as f64)
    }

    /// 採掘したことのある支払い先の数。
    pub fn miners(&self) -> usize {
        self.miners.len()
    }

    /// 直近 7 日に採掘した支払い先の数。
    pub fn recent_miners(&self) -> usize {
        self.distinct_recent(|r| &r.miners)
    }

    /// 直近 7 日にコインを受け取った支払い先の数。
    pub fn recent_receivers(&self) -> usize {
        self.distinct_recent(|r| &r.receivers)
    }

    /// 取引の数 (コインベースを除く)。
    pub fn transactions(&self) -> u64 {
        self.transactions
    }

    /// 直近 `blocks` 個のブロックを、報酬の支払い先ごとに数える。
    ///
    /// 返すのは (支払い先, ブロック数) の多い順と、実際に数えたブロック数
    /// (鎖が短ければ `blocks` より少ない)。1 つのブロックの報酬を複数の
    /// 支払い先に分けていれば、それぞれに 1 つと数える。同数は支払い条件の
    /// 符号化の順に並べる。
    pub fn block_shares(&self, blocks: usize) -> (Vec<(Lock, usize)>, usize) {
        let counted = blocks.min(self.recent.len());
        let mut counts: HashMap<&Lock, usize> = HashMap::new();
        for r in self.recent.range(self.recent.len() - counted..) {
            let mut seen = HashSet::new();
            for lock in &r.miners {
                if seen.insert(lock) {
                    *counts.entry(lock).or_insert(0) += 1;
                }
            }
        }
        let mut all: Vec<(Lock, usize)> = counts.into_iter().map(|(l, n)| (l.clone(), n)).collect();
        all.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.encode().cmp(&b.0.encode())));
        (all, counted)
    }

    fn distinct_recent(&self, pick: impl Fn(&Recent) -> &Vec<Lock>) -> usize {
        let since = self.tip_time - RECENT_SECONDS;
        let mut seen = HashSet::new();
        for r in self.recent.iter().filter(|r| r.time >= since) {
            seen.extend(pick(r).iter());
        }
        seen.len()
    }
}

/// 鎖の先端まで数え進める。
///
/// 覚えた先端が鎖から外れていれば、最初から数え直す。
pub async fn catch_up(tally: &mut Tally, handle: &NodeHandle) -> Result<(), String> {
    if let Some((height, hash)) = tally.tip() {
        if handle.hash_at_height(height).await? != Some(hash) {
            *tally = Tally::new();
        }
    }
    let target = handle.status().await?.height;
    let from = match tally.tip() {
        Some((height, _)) => height + 1,
        None => 0,
    };
    for height in from..=target {
        // 数えている間に再編成が起きると、途中で引けなくなる。次の呼び出しで
        // 先端の食い違いに気づいて数え直すので、ここでは止めるだけでよい。
        let Some(hash) = handle.hash_at_height(height).await? else {
            break;
        };
        let Some(block) = handle.block(hash).await? else {
            return Err(format!("the body of block {height} is not held"));
        };
        tally.add(&block, hash);
    }
    Ok(())
}

/// 鎖の今の姿。エクスプローラの `/api/stats` がそのまま JSON にして返し、
/// `/stats` の「chain」の欄も同じ値を出す。**計算はここだけに置く。**
/// 頁と JSON が別々に数えると、そのうち食い違う。
///
/// **名前と単位は一度出したら変えない** (`docs/COMMANDS.md`)。外の集計
/// サイトが読みに来るので、変えるとそちらが壊れる。足すのは構わない。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Summary {
    /// `mainnet` / `testnet` / `regtest`。
    pub network: String,
    /// 先端の高さ。
    pub height: u64,
    /// 次のブロックの難易度 (= 1 ブロックに要るハッシュ数の期待値)。
    pub difficulty: u64,
    /// 直近 [`SHORT_WINDOW`] ブロックから出した実測のハッシュレート (H/s)。
    pub hashrate: Option<f64>,
    /// 直近 [`LONG_WINDOW`] ブロックから出した実測のハッシュレート (H/s)。
    pub hashrate_24h: Option<f64>,
    /// ブロック 1 から先端までの平均の間隔 (秒)。
    pub average_block_time: Option<f64>,
    /// 次のブロックの報酬 (OAG、十進の文字列)。
    pub block_reward: String,
    /// 使われていない出力にある OAG の合計 (十進の文字列)。ジェネシスの
    /// 報酬 (焼却) とコミットメント出力 (金額 0) は入らない。
    pub supply: String,
    /// 数え終えた先端のブロック。ジェネシスしか無ければ `None`。
    pub last_block: Option<LastBlock>,
    /// コミットメント出力などのソフトフォーク (SPEC §10.9)。
    pub softfork: SoftForkSummary,
}

/// [`Summary::last_block`]。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LastBlock {
    /// 高さ。
    pub height: u64,
    /// ブロックハッシュ (16 進)。
    pub hash: String,
    /// Unix 秒 (ブロックのヘッダの時刻)。
    pub time: i64,
}

/// [`Summary::softfork`]。名前は RPC の `getinfo` の `softfork` に合わせる。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SoftForkSummary {
    /// `defined` / `started` / `locked_in` / `active` / `failed`。
    pub state: &'static str,
    /// いまの区切りで印の付いたブロックの数。数えていなければ `None`。
    pub signalled: Option<u64>,
    /// いまの区切りに積まれたブロックの数。
    pub elapsed: Option<u64>,
    /// 閾値。最初から有効なネットワークでは `None`。
    pub threshold: Option<u64>,
    /// 区切りの長さ。
    pub period: Option<u64>,
}

/// ノードの状態と、数え終えた集計から [`Summary`] を作る。
pub fn summary(tally: &Tally, status: &NodeStatus) -> Summary {
    let softfork = &status.softfork;
    let (signalled, elapsed) = match softfork.signals {
        Some((count, elapsed)) if softfork.state.should_signal() => (Some(count), Some(elapsed)),
        _ => (None, None),
    };
    let last_block = match tally.tip {
        Some((height, hash)) if height > 0 => Some(LastBlock {
            height,
            hash: hash.to_string(),
            time: tally.tip_time,
        }),
        _ => None,
    };
    let supply = tally.holdings().supply;
    Summary {
        network: status.network.to_string(),
        height: status.height,
        difficulty: status.next_difficulty,
        hashrate: tally.hashrate(SHORT_WINDOW),
        hashrate_24h: tally.hashrate(LONG_WINDOW),
        average_block_time: tally.average_interval(),
        block_reward: block_subsidy(status.height.saturating_add(1)).to_string(),
        supply: Amount::from_atomic(supply).map_or_else(|_| supply.to_string(), |a| a.to_string()),
        last_block,
        softfork: SoftForkSummary {
            state: softfork.state.name(),
            signalled,
            elapsed,
            threshold: softfork.deployment.map(|d| d.threshold),
            period: softfork.deployment.map(|d| d.period),
        },
    }
}

/// ハッシュレートを読みやすい単位にする。
pub fn format_hashrate(rate: f64) -> String {
    const UNITS: [&str; 5] = ["H/s", "kH/s", "MH/s", "GH/s", "TH/s"];
    let mut value = rate;
    let mut unit = 0;
    while value >= 1000.0 && unit < UNITS.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oag_consensus::block::BlockHeader;
    use oag_consensus::tx::{OutPoint, TxInput, CURRENT_TX_VERSION};
    use oag_consensus::{Transaction, TxOutput};
    use oag_primitives::{Amount, SecretKey};

    fn lock(seed: u8) -> Lock {
        let secret = SecretKey::from_bytes([seed.max(1); 32]).unwrap();
        Lock::pay_to_pubkey(&secret.public_key())
    }

    fn tx(input: OutPoint, to: u8) -> Transaction {
        Transaction {
            version: CURRENT_TX_VERSION,
            inputs: vec![TxInput::new(input)],
            outputs: vec![TxOutput::new(Amount::ONE_OAG, lock(to))],
            locktime: 0,
        }
    }

    fn block(height: u64, time: i64, difficulty: u64, miner: u8, payments: &[u8]) -> Block {
        let mut transactions = vec![tx(OutPoint::null(), miner)];
        for (n, to) in payments.iter().enumerate() {
            transactions.push(tx(OutPoint::new(Hash::ZERO, n as u32), *to));
        }
        Block {
            header: BlockHeader {
                version: 1,
                prev_hash: Hash::ZERO,
                merkle_root: Hash::ZERO,
                timestamp: time,
                difficulty,
                height,
                nonce: 0,
            },
            transactions,
        }
    }

    #[test]
    fn the_genesis_block_is_not_counted() {
        let mut tally = Tally::new();
        tally.add(&block(0, 1_000, 1, 9, &[]), Hash::ZERO);
        assert_eq!(tally.miners(), 0);
        assert_eq!(tally.first_time(), None);
        assert_eq!(tally.tip(), Some((0, Hash::ZERO)));
    }

    #[test]
    fn miners_payments_and_intervals_are_counted() {
        let mut tally = Tally::new();
        tally.add(&block(0, 0, 1, 9, &[]), Hash::ZERO);
        // 60 秒ごとに 3 ブロック。採掘者は 2 人、送金は 2 件。
        tally.add(&block(1, 10_000, 100, 1, &[]), Hash::ZERO);
        tally.add(&block(2, 10_060, 100, 2, &[3]), Hash::ZERO);
        tally.add(&block(3, 10_120, 100, 1, &[4]), Hash::ZERO);

        assert_eq!(tally.first_time(), Some(10_000));
        assert_eq!(tally.average_interval(), Some(60.0));
        assert_eq!(tally.miners(), 2);
        assert_eq!(tally.recent_miners(), 2);
        // 採掘者 2 人と、受け取った 2 人。
        assert_eq!(tally.recent_receivers(), 4);
        assert_eq!(tally.transactions(), 2);
    }

    #[test]
    fn a_commitment_is_neither_a_miner_nor_a_holder() {
        let mut tally = Tally::new();
        tally.add(&block(0, 0, 1, 9, &[]), Hash::ZERO);
        let mut b = block(1, 10_000, 100, 1, &[2]);
        let note = TxOutput::new(Amount::ZERO, Lock::commitment(vec![7; 32]).unwrap());
        b.transactions[0].outputs.push(note.clone());
        b.transactions[1].outputs.push(note);
        tally.add(&b, Hash::ZERO);
        assert_eq!(tally.miners(), 1, "only the real payee mined");
        assert_eq!(tally.recent_receivers(), 2);
        assert_eq!(tally.unspent_outputs(), 2);
    }

    #[test]
    fn the_hashrate_is_work_over_time() {
        let mut tally = Tally::new();
        for h in 1..=11u64 {
            tally.add(&block(h, 1_000 + 60 * h as i64, 6_000, 1, &[]), Hash::ZERO);
        }
        // 10 ブロックで 6,000 × 10 のハッシュを 600 秒で。
        assert_eq!(tally.hashrate(10), Some(100.0));
        // ブロックが足りなければ出さない。
        assert_eq!(tally.hashrate(11), None);
        assert_eq!(tally.hashrate(0), None);
    }

    #[test]
    fn a_window_whose_clock_did_not_advance_gives_no_hashrate() {
        let mut tally = Tally::new();
        tally.add(&block(1, 1_000, 10, 1, &[]), Hash::ZERO);
        tally.add(&block(2, 1_000, 10, 1, &[]), Hash::ZERO);
        assert_eq!(tally.hashrate(1), None);
    }

    #[test]
    fn only_the_last_seven_days_count_as_recent() {
        let mut tally = Tally::new();
        tally.add(&block(1, 0, 1, 1, &[]), Hash::ZERO);
        tally.add(&block(2, RECENT_SECONDS + 1, 1, 2, &[]), Hash::ZERO);
        assert_eq!(tally.miners(), 2);
        assert_eq!(tally.recent_miners(), 1);
    }

    #[test]
    fn old_blocks_are_dropped_once_neither_window_needs_them() {
        let mut tally = Tally::new();
        let step = RECENT_SECONDS; // 1 ブロックごとに 7 日進める
        for h in 1..=(LONG_WINDOW as u64 + 10) {
            tally.add(&block(h, h as i64 * step, 1, 1, &[]), Hash::ZERO);
        }
        assert_eq!(tally.recent.len(), LONG_WINDOW + 1);
        assert!(tally.hashrate(LONG_WINDOW).is_some());
    }

    #[test]
    fn block_shares_count_only_the_last_blocks_largest_first() {
        let mut tally = Tally::new();
        tally.add(&block(0, 0, 1, 9, &[]), Hash::ZERO);
        // 1 が 3 つ、2 が 1 つ、その後に 3 が 2 つ。
        for (h, miner) in [(1, 1), (2, 1), (3, 2), (4, 1), (5, 3), (6, 3)] {
            tally.add(&block(h, 1_000 + 60 * h as i64, 1, miner, &[]), Hash::ZERO);
        }

        let (all, counted) = tally.block_shares(100);
        assert_eq!(counted, 6);
        assert_eq!(all, vec![(lock(1), 3), (lock(3), 2), (lock(2), 1)]);

        // 窓を狭めれば、古いブロックは外れる。
        let (last, counted) = tally.block_shares(3);
        assert_eq!(counted, 3);
        assert_eq!(last, vec![(lock(3), 2), (lock(1), 1)]);
    }

    #[test]
    fn a_reward_split_into_two_outputs_to_one_address_is_one_block() {
        let mut tally = Tally::new();
        let mut b = block(1, 1_000, 1, 1, &[]);
        let output = b.transactions[0].outputs[0].clone();
        b.transactions[0].outputs.push(output);
        tally.add(&b, Hash::ZERO);
        assert_eq!(tally.block_shares(100), (vec![(lock(1), 1)], 1));
    }

    /// 送金の取引。`spend` の出力を使い、`to` へ `oag` を送り、残りを
    /// `change` へ返す。
    fn pay(spend: OutPoint, to: u8, oag: u128, change: u8, back: u128) -> Transaction {
        let unit = oag_primitives::amount::ATOMIC_PER_OAG;
        Transaction {
            version: CURRENT_TX_VERSION,
            inputs: vec![TxInput::new(spend)],
            outputs: vec![
                TxOutput::new(Amount::from_atomic(oag * unit).unwrap(), lock(to)),
                TxOutput::new(Amount::from_atomic(back * unit).unwrap(), lock(change)),
            ],
            locktime: 0,
        }
    }

    #[test]
    fn balances_follow_the_coins() {
        let unit = oag_primitives::amount::ATOMIC_PER_OAG;
        let mut tally = Tally::new();
        let first = block(1, 1_000, 1, 1, &[]);
        let reward = OutPoint::new(first.transactions[0].txid(), 0);
        tally.add(&first, Hash::ZERO);
        assert_eq!(tally.holdings().holders, 1);

        // 2 ブロック目で、1 の報酬 (1 OAG) を 2 へ 0 OAG、1 へおつり 1 OAG と使う。
        let mut second = block(2, 1_060, 1, 3, &[]);
        second.transactions.push(pay(reward, 2, 0, 1, 1));
        tally.add(&second, Hash::ZERO);

        let holdings = tally.holdings();
        // 1 (おつり 1 OAG) と 3 (報酬 1 OAG)。2 は 0 OAG なので数えない。
        assert_eq!(holdings.holders, 2);
        assert_eq!(holdings.supply, 2 * unit);
        assert_eq!(holdings.top1, 0.5);
        assert_eq!(tally.unspent_outputs(), 3);
        assert_eq!(tally.transactions(), 1);
    }

    #[test]
    fn the_richest_come_first_and_ties_keep_their_order() {
        let mut tally = Tally::new();
        tally.add(&block(1, 1_000, 1, 1, &[]), Hash::ZERO);
        tally.add(&block(2, 1_060, 1, 2, &[]), Hash::ZERO);
        tally.add(&block(3, 1_120, 1, 1, &[]), Hash::ZERO);
        let top = tally.richest(10);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].0, lock(1));
        assert!(top[0].1 > top[1].1);
        assert_eq!(tally.richest(1).len(), 1);
        // 同額の並びは何度引いても同じ。
        let mut even = Tally::new();
        even.add(&block(1, 1_000, 1, 5, &[]), Hash::ZERO);
        even.add(&block(2, 1_060, 1, 6, &[]), Hash::ZERO);
        assert_eq!(even.richest(2), even.richest(2));
    }

    #[test]
    fn balances_fall_into_bands() {
        let mut tally = Tally::new();
        tally.add(&block(1, 1_000, 1, 1, &[]), Hash::ZERO);
        let bands = tally.bands();
        assert_eq!(bands.len(), BAND_EDGES.len() + 1);
        // 1 OAG は「1 以上 10 未満」に入る。
        let band = bands.iter().find(|b| b.holders == 1).unwrap();
        assert_eq!((band.from, band.to), (1, Some(10)));
        assert_eq!(bands.last().unwrap().to, None);
    }

    #[test]
    fn days_are_closed_with_the_balances_at_their_end() {
        let mut tally = Tally::new();
        // 1 日目の昼に始まり、2 ブロック。2 日目に 1 ブロック。
        let noon = 10 * DAY + DAY / 2;
        tally.add(&block(1, noon, 100, 1, &[]), Hash::ZERO);
        tally.add(&block(2, noon + 60, 100, 2, &[]), Hash::ZERO);
        tally.add(&block(3, 11 * DAY + 30, 300, 1, &[]), Hash::ZERO);

        let days = tally.days();
        assert_eq!(days.len(), 2);
        let first = &days[0];
        assert_eq!(
            (first.day, first.blocks, first.miners, first.new_miners),
            (10, 2, 2, 2)
        );
        // 鎖が動き出した昼から日付が替わるまで。
        assert_eq!(first.seconds, DAY / 2);
        assert_eq!(first.hashrate(), Some(200.0 / (DAY / 2) as f64));
        // 1 日目の終わりには 2 人が 1 OAG ずつ持っていた。
        assert_eq!(first.holdings.holders, 2);
        assert!(!first.partial);

        let second = &days[1];
        assert_eq!((second.day, second.blocks, second.new_miners), (11, 1, 0));
        assert!(second.partial);
        assert_eq!(second.seconds, 30);
    }

    #[test]
    fn a_block_that_looks_like_yesterday_stays_in_today() {
        let mut tally = Tally::new();
        tally.add(&block(1, 10 * DAY + 10, 1, 1, &[]), Hash::ZERO);
        // 時刻が少し前後して、前の日付に見える。
        tally.add(&block(2, 10 * DAY - 5, 1, 1, &[]), Hash::ZERO);
        let days = tally.days();
        assert_eq!(days.len(), 1);
        assert_eq!(days[0].blocks, 2);
    }

    #[test]
    fn hashrates_are_shown_in_a_readable_unit() {
        assert_eq!(format_hashrate(950.0), "950 H/s");
        assert_eq!(format_hashrate(3_106.0), "3.11 kH/s");
        assert_eq!(format_hashrate(2_500_000.0), "2.50 MH/s");
    }
}
