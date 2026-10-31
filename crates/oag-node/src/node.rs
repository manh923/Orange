//! ノード本体。
//!
//! 記憶域を開き、チェーンを復元し、採掘する。
//!
//! # RandomX の検証器はエポックごとに作り直す
//!
//! RandomX のシードは 2048 ブロックごとに切り替わる (`docs/SPEC.md` §11.3)。
//! 検証器の初期化には 256 MB の確保が伴うため、切り替わるまでは使い回す。
//!
//! # 中断について
//!
//! ブロックを 1 個受け入れるたびに記憶域への書き込みが確定する。途中で
//! 強制終了しても、そのブロックが入っているか入っていないかのどちらかに
//! なり、中途半端な状態は残らない。信号を捕まえる仕掛けを置いていないのは
//! そのためである。

use crate::addrbook::AddressBook;
use oag_chain::chain::{AcceptOutcome, Chain, ChainError, HeaderOutcome, Reorg, Retarget};
use oag_consensus::lock::Lock;
use oag_consensus::params;
use oag_consensus::validate::PowVerifier;
use oag_consensus::{Block, BlockHeader, Transaction};
use oag_mempool::{Mempool, Reject};
use oag_miner::{build_template, BlockTemplate, TemplateError, TemplateRequest};
use oag_pow::randomx::{RandomXPowError, RandomXVerifier, SharedCache};
use oag_pow::seed_height;
use oag_primitives::{Amount, Hash, Network};
use oag_store::{Store, StoreError};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

/// ノードの失敗。
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    /// 記憶域の操作に失敗した。
    #[error(transparent)]
    Store(#[from] StoreError),
    /// チェーンの操作に失敗した。
    #[error(transparent)]
    Chain(#[from] ChainError),
    /// テンプレートの組み立てに失敗した。
    #[error(transparent)]
    Template(#[from] TemplateError),
    /// RandomX の操作に失敗した。
    #[error(transparent)]
    RandomX(#[from] RandomXPowError),
    /// 採掘に失敗した。
    #[error(transparent)]
    Mine(#[from] oag_miner::MineError),
    /// 作業スレッドを起こせない、あるいは落ちた。
    #[error("the node's worker thread: {0}")]
    Thread(String),
    /// 掘ったブロックが受理されなかった。
    #[error("a block we mined ourselves was not accepted: {0:?}")]
    SelfMinedRejected(AcceptOutcome),
    /// RandomX のシードになるブロックを知らない。
    ///
    /// **ここで代わりのハッシュを使ってはならない。** 鍵が違えば RandomX は
    /// 違うハッシュを返し、正しいブロックの PoW が落ちる。
    #[error(
        "the block at height {seed_height} is unknown, so the RandomX seed cannot be determined"
    )]
    UnknownSeedBlock {
        /// シードになるはずの高さ。
        seed_height: u64,
    },
    /// タイムスタンプを決められない。
    #[error("the chain tip is too far in the future (median time past {mtp}, now {now})")]
    ClockTooFarBehind {
        /// 先端の Median Time Past。
        mtp: i64,
        /// ノードの現在時刻。
        now: i64,
    },
}

/// ノードの現在の様子。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeStatus {
    /// ネットワーク。
    pub network: Network,
    /// 先端の高さ。**本体を繋いだ高さ**である。
    ///
    /// 軽量モードでは本体を繋がないので 0 のまま動かない。どこまで
    /// 追えているかは [`header_height`](NodeStatus::header_height) が答える。
    pub height: u64,
    /// 検証済みヘッダの先端の高さ。
    pub header_height: u64,
    /// 先端のブロックハッシュ。
    pub tip: Hash,
    /// 先端までの累積作業量。
    pub cumulative_work: u128,
    /// 次のブロックの難易度。
    pub next_difficulty: u64,
    /// UTXO の件数。
    pub utxo_count: u64,
    /// 知っているブロックの数 (サイドチェーンを含む)。
    pub indexed_blocks: u64,
    /// mempool の件数。
    pub mempool_len: usize,
    /// 住所帳に覚えているピアの数。
    pub known_addresses: usize,
    /// ブロック本体を持っている一番低い高さ。剪定していなければ 0。
    pub blocks_from: u64,
    /// 軽量モードの様子。通常のノードでは `None`。
    ///
    /// **`Node` は埋めない。** 走査の状態を持っているのは
    /// [`crate::service`] 側なので、あちらが後から入れる。
    pub light: Option<LightStatus>,
    /// コミットメント出力などのソフトフォークの様子 (SPEC §10.9)。
    pub softfork: SoftForkStatus,
}

/// ソフトフォークの有効化の様子 (SPEC §10.9)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SoftForkStatus {
    /// 次のブロックでの状態。
    pub state: oag_consensus::deployment::DeploymentState,
    /// 印で有効にする場合の決まり。最初から有効なネットワークでは `None`。
    pub deployment: Option<oag_consensus::deployment::Deployment>,
    /// 先端を含む区切りで、印の付いたブロックの数と、積まれたブロックの数。
    pub signals: Option<(u64, u64)>,
}

/// 軽量モードの様子。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LightStatus {
    /// どの高さまで走査したか。まだなら `None`。
    pub scanned_to: Option<u64>,
    /// 見張っている支払い条件の数。
    pub watched: usize,
    /// 持っている硬貨の数。
    pub coins: usize,
    /// 手元にある合計。**成熟していないコインベースを含む。**
    pub total: Amount,
    /// いま使える合計。
    pub spendable: Amount,
}

/// 掘れて、先端になったブロック。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MinedBlock {
    /// 掘ったブロックのハッシュ。
    pub hash: Hash,
    /// 新しい高さ。
    pub height: u64,
}

/// ノード。
pub struct Node {
    chain: Chain<Store>,
    mempool: Mempool,
    network: Network,
    /// 現在のシードエポックの検証器 (light、256 MB)。
    ///
    /// **検証には常にこちらを使う。** 採掘を fast モードで行っていても、
    /// 掘れたブロックは light の検証器に通す。両者が同じハッシュを返す
    /// ことは `oag-pow` の試験で確かめてあるが、確かめてあることと
    /// 実際に通すことは別である。
    ///
    /// キャッシュ ([`SharedCache`]) も一緒に持つ。ヘッダの PoW を並べて
    /// 確かめるときに、各スレッドがその上に自分の VM を建てる
    /// ([`Node::pow_verdicts`])。
    verifier: Option<(u64, SharedCache, RandomXVerifier)>,
    /// ヘッダの PoW を並べて確かめるスレッドの数。1 なら並べない。
    pow_threads: usize,
    /// ピアの住所帳。
    addresses: AddressBook,
}

/// assumevalid をどう決めるか。
///
/// **ネットワークの既定に従う**のが普通である。`Off` と `Block` は利用者が
/// 明示したときだけ使う。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AssumeValidSetting {
    /// ネットワークの既定 ([`crate::genesis::assume_valid_for`]) に従う。
    #[default]
    Network,
    /// 無効にする。創世記から全署名を検証し直す。
    Off,
    /// このブロックとその祖先の署名検証を飛ばす。
    Block(Hash),
}

impl AssumeValidSetting {
    /// このネットワークで実際に使うハッシュ。
    pub fn resolve(self, network: Network) -> Option<Hash> {
        match self {
            AssumeValidSetting::Network => crate::genesis::assume_valid_for(network),
            AssumeValidSetting::Off => None,
            AssumeValidSetting::Block(hash) => Some(hash),
        }
    }
}

/// ノードを開くときの設定。
///
/// **`Copy` ではない。** `watch` が伸びるためである。
#[derive(Debug, Clone, Default)]
pub struct NodeOptions {
    /// assumevalid。
    pub assume_valid: AssumeValidSetting,
    /// 巻き戻し情報を残す深さ。`None` なら捨てない (既定)。
    pub undo_keep: Option<u64>,
    /// ブロック本体を残す深さ。`None` なら捨てない (既定)。
    ///
    /// **`undo_keep` と同じ値を渡すこと。** 片方だけ深く持っても、戻れる
    /// 深さは浅いほうで決まる。`--prune` は両方を一度に決める。
    pub block_keep: Option<u64>,
    /// 開いた直後に取引索引を捨てる。
    ///
    /// **剪定より先に行う。** 索引は本体を読んで答えるので、剪定した記憶域
    /// に残っていると、捨てた範囲の照会が「その取引は存在しない」に化ける。
    /// `block_keep` を付けて索引の残った記憶域を開くと断られるため、
    /// `--prune --drop-index` を 1 回で通すにはここで順序を決める必要が
    /// ある。
    pub drop_index: bool,
    /// 軽量モードで動かす。
    ///
    /// UTXO セットも索引も mempool も持たず、本体は走査して捨てる
    /// (`crate::light`)。**検証を預けるわけではない。** ヘッダの PoW も
    /// 本体のマークルルートも自分で確かめる。預けるのは「見せられなかった
    /// ものがあるか」だけである (SPEC §19)。
    pub light: bool,
    /// 軽量モードで見張る支払い条件。
    ///
    /// **走査を始める前に渡しきること。** 後から足したものは、通り過ぎた
    /// ブロックには適用されない。
    pub watch: Vec<Lock>,
}

impl Node {
    /// 記憶域を開き、必要ならジェネシスで初期化する。
    pub fn open(network: Network, data_dir: &Path) -> Result<Node, NodeError> {
        Node::open_with(network, data_dir, NodeOptions::default())
    }

    /// 設定を渡して開く。
    pub fn open_with(
        network: Network,
        data_dir: &Path,
        options: NodeOptions,
    ) -> Result<Node, NodeError> {
        let genesis = crate::genesis::genesis_for(network);
        std::fs::create_dir_all(data_dir)
            .map_err(|e| StoreError::Io(format!("cannot create {}: {e}", data_dir.display())))?;
        let store = Store::open(data_dir.join("chain.redb"))?;
        // 剪定の可否を見る前に捨てる。順序が逆だと `--prune --drop-index`
        // が 1 回で通らない。
        if options.drop_index {
            store.drop_index()?;
        }
        let store = store
            .with_undo_keep(options.undo_keep)
            .with_block_keep(options.block_keep)?;
        // 既に同期済みの記憶域に後から `--prune` を付けた場合、1 ブロック
        // ずつ落とすのを待っていたら縮み始めるのは数日先になる。開いた
        // ところで一度追いつかせる。
        let pruned = store.prune_stale_blocks()?;
        if pruned > 0 {
            crate::log_warn!("pruned {pruned} old block bodies");
        }
        let retarget = if network.retargets() {
            Retarget::Enabled
        } else {
            Retarget::Disabled
        };
        let mut chain = Chain::open(store, genesis, network.genesis_difficulty(), retarget)?;
        chain.set_assume_valid(options.assume_valid.resolve(network));
        chain.set_relative_locktime_height(network.relative_locktime_height());
        chain.set_commitment_activation(oag_consensus::deployment::commitment_activation(network));

        // 0.1.1 までは、初期同期でシードのエポックをまたぐと、正しいブロックに
        // 無効の印が付くことがあった (`Chain::reconsider_invalid`)。印は
        // 永続化されるので、上げただけでは外れない。**開くたびに外して見直す。**
        // 本当に無効なものは、繋ぎ直そうとした時点でまた印が付く。
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let reconsidered = chain.reconsider_invalid(now)?;
        if reconsidered > 0 {
            crate::log_warn!(
                "reconsidered {reconsidered} blocks that were marked invalid \
                 (height now {})",
                chain.tip()?.height()
            );
        }

        let addresses = AddressBook::open(network, &data_dir.join("peers.json"));
        Ok(Node {
            chain,
            mempool: Mempool::new(),
            network,
            verifier: None,
            pow_threads: default_pow_threads(),
            addresses,
        })
    }

    /// ピアの住所帳。
    pub fn addresses(&self) -> &AddressBook {
        &self.addresses
    }

    /// ピアの住所帳 (書き換え可)。
    pub fn addresses_mut(&mut self) -> &mut AddressBook {
        &mut self.addresses
    }

    /// チェーン。
    pub fn chain(&self) -> &Chain<Store> {
        &self.chain
    }

    /// mempool。
    pub fn mempool(&self) -> &Mempool {
        &self.mempool
    }

    /// mempool (書き換え可能)。
    pub fn mempool_mut(&mut self) -> &mut Mempool {
        &mut self.mempool
    }

    /// トランザクションを mempool に入れる。
    ///
    /// 検証の文脈 (次の高さ、Median Time Past、過去の時刻を引く口) は
    /// 現在の先端から作る。外側の `Err` はチェーンが読めなかったこと、
    /// 内側の `Err` は mempool が断った理由である。
    pub fn submit_transaction(
        &mut self,
        tx: Transaction,
    ) -> Result<Result<Hash, Reject>, NodeError> {
        let tip = self.chain.tip()?;
        let next_height = tip.height() + 1;
        let median_time_past = self.chain.median_time_past_for_child_of(&tip.hash)?;
        let view = self.chain.utxo_view()?;
        let times = self.chain.times_on(tip.hash);
        let rules = self.chain.rules_for_child_of(&tip.hash)?;
        Ok(self
            .mempool
            .accept(tx, &view, next_height, median_time_past, &times, rules))
    }

    /// 現在の様子。
    pub fn status(&self) -> Result<NodeStatus, NodeError> {
        let tip = self.chain.tip()?;
        Ok(NodeStatus {
            network: self.network,
            height: tip.height(),
            header_height: self.chain.best_header()?.height(),
            tip: tip.hash,
            cumulative_work: tip.cumulative_work,
            next_difficulty: self.chain.expected_difficulty_for_child_of(&tip.hash)?,
            utxo_count: self.chain.store().utxo_count()?,
            indexed_blocks: self.chain.indexed_blocks()?,
            mempool_len: self.mempool.len(),
            known_addresses: self.addresses.len(),
            blocks_from: self.chain.store().blocks_from()?,
            light: None,
            softfork: SoftForkStatus {
                state: self.chain.deployment_state_for_child_of(&tip.hash)?,
                deployment: match self.chain.commitment_activation() {
                    oag_consensus::deployment::Activation::Signalled(d) => Some(d),
                    _ => None,
                },
                signals: self.chain.signals_so_far(&tip.hash)?,
            },
        })
    }

    /// この高さのシードエポックに合う検証器を用意する。
    ///
    /// `branch` は検証しようとしているブロックの**親**のハッシュである。
    /// シードはその枝の祖先から引く。`seed_height(h) < h` なので、シードは
    /// 必ず親か親の祖先にあたる。
    ///
    /// エポックが変わっていなければ何もしない。初期化には 256 MB の確保が
    /// 伴うため、毎ブロック作り直すわけにはいかない。
    ///
    /// **エポックが同じなら枝は見直さない。** 見直すには毎回祖先を辿る
    /// ことになり、初期同期の全体で効いてくる。シードのブロックは
    /// [`SEED_LAG`](oag_consensus::params::SEED_LAG) だけ後ろにあるので、
    /// それより浅いリオーグでシードは動かない (`docs/SPEC.md` §11.3)。
    fn ensure_verifier(&mut self, height: u64, branch: &Hash) -> Result<(), NodeError> {
        let wanted = seed_height(height);
        if self
            .verifier
            .as_ref()
            .is_some_and(|(current, _, _)| *current == wanted)
        {
            return Ok(());
        }
        let seed = self.seed_for(wanted, branch)?;
        let (shared, verifier) = SharedCache::with_verifier(&seed, wanted)?;
        if !verifier.uses_jit() {
            // JIT を使えない環境である。検証は動くが 10 倍ほど遅い。
            // 黙っていると「なぜか同期が進まない」に見えるので伝える。
            crate::log_warn!(
                "RandomX cannot use the JIT, so validation runs in the interpreter ({:?}).\n                 It works, but block validation is roughly ten times slower.\n                 Check whether something forbids executable memory\n                 (SELinux deny_execmem and the like).",
                verifier.flags()
            );
        }
        crate::log_verify!(
            "switched the RandomX seed to block {} at height {wanted}",
            crate::log::short(&seed)
        );
        self.verifier = Some((wanted, shared, verifier));
        Ok(())
    }

    /// その高さの検証器を用意し、それを借りたまま `f` を走らせる。
    ///
    /// 検証器を一旦 `self` から取り出す。こうしないと、検証器を借りたまま
    /// チェーンを変更できない。`f` が失敗しても必ず戻す。
    fn with_verifier<T>(
        &mut self,
        height: u64,
        branch: &Hash,
        f: impl FnOnce(&mut Self, &RandomXVerifier) -> Result<T, NodeError>,
    ) -> Result<T, NodeError> {
        self.ensure_verifier(height, branch)?;
        let (epoch, shared, verifier) = self.verifier.take().expect("prepared just beforehand");
        let result = f(self, &verifier);
        self.verifier = Some((epoch, shared, verifier));
        result
    }

    /// 他所から来たブロックを受け取る。
    ///
    /// ヘッダを既に知っているなら検証器を用意しない。PoW は見直されない。
    pub fn accept_block(&mut self, block: Block, now: i64) -> Result<AcceptOutcome, NodeError> {
        let outcome = if self.chain.contains(&block.header.hash())? {
            self.chain
                .accept_block(block.clone(), &HeaderAlreadyChecked, now)?
        } else {
            let height = block.header.height;
            let branch = block.header.prev_hash;
            self.with_verifier(height, &branch, |node, verifier| {
                Ok(node.chain.accept_block(block.clone(), verifier, now)?)
            })?
        };
        self.sync_mempool(&outcome, &block)?;
        Ok(outcome)
    }

    /// ブロックを受け取った結果に合わせて mempool を直す。
    ///
    /// mempool は**アクティブチェーンの先を写したもの**である。先端が動いた
    /// ときだけ直す。
    fn sync_mempool(&mut self, outcome: &AcceptOutcome, block: &Block) -> Result<(), NodeError> {
        match outcome {
            // 知っていたもの。
            AcceptOutcome::Duplicate => Ok(()),
            // **負けた枝に入っただけのものは外さない。** そのブロックは
            // アクティブチェーンに無く、入っているトランザクションは依然
            // 未確認である。ここで外すと、まだ有効な支払いを中継しなくなり、
            // 自分が掘るときにも詰めなくなる。
            AcceptOutcome::SideChain => Ok(()),
            AcceptOutcome::ExtendedTip => {
                self.mempool.on_block_connected(block);
                Ok(())
            }
            AcceptOutcome::Reorganized(reorg) => self.rebuild_mempool(reorg),
        }
    }

    /// リオーグの後、mempool を組み直す。
    ///
    /// 取り消された枝のトランザクションを集めて戻し、残っているものも含めて
    /// リオーグ後の UTXO で検証し直す ([`Mempool::rebuild_after_reorg`])。
    ///
    /// 受け取ったブロック 1 個だけを見て済ませるわけにはいかない。リオーグ
    /// では複数のブロックが一度に繋がり、同時に複数が取り消されるためで
    /// ある。取り消された枝にあった支払いを戻さなければ、それは誰の mempool
    /// にも無いまま消える。新しい枝で確認済みになったものを外さなければ、
    /// 次に自分で掘るブロックがその分だけ無効になる。
    fn rebuild_mempool(&mut self, reorg: &Reorg) -> Result<(), NodeError> {
        let orphaned = orphaned_transactions(reorg, |hash| self.chain.store().block(hash))?;

        let tip = self.chain.tip()?;
        let next_height = tip.height() + 1;
        let median_time_past = self.chain.median_time_past_for_child_of(&tip.hash)?;
        let view = self.chain.utxo_view()?;
        let times = self.chain.times_on(tip.hash);
        let rules = self.chain.rules_for_child_of(&tip.hash)?;
        self.mempool.rebuild_after_reorg(
            orphaned,
            &view,
            next_height,
            median_time_past,
            &times,
            rules,
        );
        Ok(())
    }

    /// 他所から来たヘッダを受け取る。
    ///
    /// 既に知っているヘッダなら検証器を用意しない。PoW は見直されない。
    pub fn accept_header(
        &mut self,
        header: &BlockHeader,
        now: i64,
    ) -> Result<HeaderOutcome, NodeError> {
        self.accept_header_with_verdict(header, None, now)
    }

    /// 他所から来たヘッダを、PoW の判定を添えて受け取る。
    ///
    /// `verdict` は [`Node::pow_verdicts`] が出したもの。`None` なら
    /// [`Node::accept_header`] と同じく、ここで RandomX を回す。
    ///
    /// **判定が使われるのは PoW の 1 項目だけである。** 高さ・親・時刻・
    /// 難易度は、判定があってもなくてもここで確かめる。
    pub fn accept_header_with_verdict(
        &mut self,
        header: &BlockHeader,
        verdict: Option<bool>,
        now: i64,
    ) -> Result<HeaderOutcome, NodeError> {
        if self.chain.contains(&header.hash())? {
            return Ok(self
                .chain
                .accept_header(header, &HeaderAlreadyChecked, now)?);
        }
        if let Some(meets) = verdict {
            let judged = Judged {
                hash: header.hash(),
                meets,
            };
            return Ok(self.chain.accept_header(header, &judged, now)?);
        }
        let height = header.height;
        let branch = header.prev_hash;
        self.with_verifier(height, &branch, |node, verifier| {
            Ok(node.chain.accept_header(header, verifier, now)?)
        })
    }

    /// ヘッダの PoW を、複数のスレッドで並べて確かめる。
    ///
    /// 戻り値は `headers` と同じ並びで、判定できたものは `Some`、できな
    /// かったものは `None` である。`None` のヘッダは
    /// [`Node::accept_header_with_verdict`] が今までどおり 1 件ずつ確かめる。
    /// **ここで判定を諦めても、検証が抜けることはない。**
    ///
    /// 初期同期の時間は、ほとんどがヘッダの PoW である (light で 1 件
    /// 30 ms 前後、署名の検証は全部で 1 秒もかからない)。assumevalid が
    /// 省くのは署名だけなので、ここを並べないと速くならない。
    ///
    /// # 何を確かめるか
    ///
    /// 先頭から [`POW_WAVE`] 件までの、**互いに繋がっている部分**だけを見る
    /// (次の親が前のハッシュで、高さが 1 つずつ増える)。その中で、まだ
    /// 知らないもの、かつシードのブロックが先頭の親以下にあるものを選ぶ。
    /// シードは高さの 64 以上前にあるので、64 件以内ならいつも先頭の親から
    /// 祖先を辿って引ける。
    ///
    /// 判定はここで出した時点のシードによる。受け取るときにチェーンが
    /// 高さと親を確かめるので、判定を使えるのは、先頭の親から 1 つずつ
    /// 繋がっていると確かめられたヘッダだけである。すなわち、ここで仮定
    /// した枝と同じ枝にある。
    ///
    /// # 悪いピア
    ///
    /// ここでは高さ・時刻・難易度より先に RandomX を回す。SPEC §10.5 は
    /// PoW を最後に見ることを求めているので、**呼ぶ側は、直前のヘッダが
    /// PoW まで通ってから呼ぶこと** (`Service::accept_headers`)。そうすれば
    /// 偽のヘッダで無駄にさせられる計算は、本物の PoW 1 件につき最大
    /// [`POW_WAVE`] 件に限られ、最初の無効なヘッダで出入り禁止になる。
    pub fn pow_verdicts(&mut self, headers: &[BlockHeader]) -> Vec<Option<bool>> {
        let mut verdicts = vec![None; headers.len()];
        let threads = self.pow_threads;
        let Some(first) = headers.first() else {
            return verdicts;
        };
        if threads < 2 || headers.len() < 2 {
            return verdicts;
        }
        let branch = first.prev_hash;
        let base = first.height;
        // 親を知らないヘッダは、チェーンが PoW を見る前に断る。ここで回すと、
        // 繋がらないヘッダを送るだけで RandomX を回させられる。
        if !self.chain.contains(&branch).unwrap_or(false) {
            return verdicts;
        }

        let mut hashes = vec![first.hash()];
        for pair in headers.windows(2).take(POW_WAVE - 1) {
            let (prev, next) = (&pair[0], &pair[1]);
            let prev_hash = *hashes.last().expect("starts with one");
            if next.prev_hash != prev_hash || prev.height.checked_add(1) != Some(next.height) {
                break;
            }
            hashes.push(next.hash());
        }

        // シードの高さごとに分ける。高さが増える順なので、境目は高々 1 つ。
        let mut groups: Vec<(u64, Vec<usize>)> = Vec::new();
        for (i, hash) in hashes.iter().enumerate() {
            match self.chain.contains(hash) {
                Ok(false) => {}
                // 知っているものは見直さない。読めなかったものは 1 件ずつの
                // 経路に任せる (そこで誤りになる)。
                Ok(true) | Err(_) => continue,
            }
            let wanted = seed_height(headers[i].height);
            if wanted >= base {
                continue;
            }
            match groups.last_mut() {
                Some((seed, members)) if *seed == wanted => members.push(i),
                _ => groups.push((wanted, vec![i])),
            }
        }

        for (_, members) in groups {
            // 1 件ずつ受け取るときと同じ検証器を用意する。エポックが同じなら
            // 作り直さない。用意できなければ判定しない (1 件ずつの経路で
            // 同じ誤りが出る)。
            if self
                .ensure_verifier(headers[members[0]].height, &branch)
                .is_err()
            {
                continue;
            }
            let Some((_, shared, _)) = self.verifier.as_ref() else {
                continue;
            };
            for (i, verdict) in verify_in_parallel(shared, headers, &members, threads) {
                verdicts[i] = verdict;
            }
        }
        verdicts
    }

    /// ヘッダの PoW を並べて確かめるスレッドの数を変える。1 で並べない。
    pub fn set_pow_threads(&mut self, threads: usize) {
        self.pow_threads = threads.clamp(1, MAX_POW_THREADS);
    }

    /// 次のブロックを掘る土台を組む。
    ///
    /// `now` はノードの現在時刻 (Unix 秒)。組んだ土台は
    /// [`MiningPool`](oag_miner::MiningPool) に配る。**組むのはここ、
    /// 探すのは作業スレッドである。** チェーンと mempool に触るのは
    /// ノードのスレッドだけに保つ。
    ///
    /// 時計が Median Time Past より大きく遅れていると組めない。掘れても
    /// 他のノードが撥ねるブロックにしかならないためである。
    pub fn mining_template(
        &self,
        payout: &Lock,
        now: i64,
        extra_nonce: u64,
    ) -> Result<BlockTemplate, NodeError> {
        let tip = self.chain.tip()?;
        let prev_hash = tip.hash;
        let height = tip.height() + 1;
        let difficulty = self.chain.expected_difficulty_for_child_of(&prev_hash)?;
        let mtp = self.chain.median_time_past_for_child_of(&prev_hash)?;

        // タイムスタンプは Median Time Past より後でなければならない。
        let timestamp = now.max(mtp + 1);
        if timestamp > now + params::MAX_FUTURE_TIME_DRIFT_SECS {
            return Err(NodeError::ClockTooFarBehind { mtp, now });
        }

        let request = TemplateRequest {
            prev_hash,
            height,
            difficulty,
            // ソフトフォークの印 (SPEC §10.9)。数えている間だけ付く。
            version: self.chain.block_version_for_child_of(&prev_hash)?,
            timestamp,
            payout: payout.clone(),
            extra_nonce: extra_nonce.to_le_bytes().to_vec(),
        };
        Ok(build_template(&request, &self.mempool)?)
    }

    /// 自分で掘ったブロックを自分のチェーンに入れる。
    ///
    /// **他所から来たものと同じ道を通す。** 掘った側だからといって検証を
    /// 省かない。省けば、自分だけが正しいと思っているブロックを撒くことに
    /// なる。
    ///
    /// 先端にならなかったときは [`NodeError::SelfMinedRejected`] を返す。
    pub fn accept_mined(&mut self, block: Block, now: i64) -> Result<MinedBlock, NodeError> {
        let hash = block.header.hash();
        let height = block.header.height;
        match self.accept_block(block, now)? {
            AcceptOutcome::ExtendedTip | AcceptOutcome::Reorganized(_) => {
                Ok(MinedBlock { hash, height })
            }
            other => Err(NodeError::SelfMinedRejected(other)),
        }
    }

    /// ヘッダの PoW ハッシュを計算する。
    ///
    /// 外の採掘器 (Stratum) が出してきた答えを確かめるのに使う。難易度を
    /// 満たすかどうかはここでは見ない。**ハッシュそのものを返す**のは、
    /// 採掘器が申告した値と突き合わせ、ずれていれば「何が違うのか」を
    /// 言えるようにするためである。
    pub fn pow_hash(&mut self, header: &BlockHeader) -> Result<Hash, NodeError> {
        self.with_verifier(header.height, &header.prev_hash, |_, verifier| {
            Ok(verifier.hash_header(header)?)
        })
    }

    /// 次のブロックを掘るときの RandomX シード (高さと値)。
    ///
    /// 採掘器はこれで建てる。**エポックが変われば建て直しである。**
    /// 呼び出し側は返ってきた高さを覚えておき、変わったかどうかを見る。
    pub fn mining_seed(&self) -> Result<(u64, Hash), NodeError> {
        let tip = self.chain.tip()?;
        let wanted = seed_height(tip.height() + 1);
        Ok((wanted, self.seed_for(wanted, &tip.hash)?))
    }

    /// `branch` の枝で、そのシード高さにあたるブロックハッシュ。
    ///
    /// **アクティブチェーンの高さの索引では引けない。** headers-first の
    /// 同期では、ヘッダが高さ数千まで届いていても本体が 1 つも繋がって
    /// いない時期がある。その間アクティブチェーンの高さは 0 のままなので、
    /// 索引は知っているはずのシードにも `None` を返す。
    ///
    /// 以前はそこで先端のハッシュを代わりに使っていた。鍵が違えば RandomX は
    /// 違うハッシュを返すので、**正しいヘッダの PoW が全部落ちる**。最初の
    /// エポック境界 (高さ 2112) で同期が止まり、二度と先へ進めなくなる。
    fn seed_for(&self, seed_height: u64, branch: &Hash) -> Result<Hash, NodeError> {
        // 最初のエポックのシードはジェネシスのハッシュであり、枝によらない。
        // 高さ 2112 未満はすべてここを通る。
        if seed_height == 0 {
            return self
                .chain
                .hash_at_height(0)?
                .ok_or(NodeError::UnknownSeedBlock { seed_height });
        }
        // 親を知らなければシードも決められないが、**断る理由はシードでは
        // なく親である**。そのまま名乗る。
        if !self.chain.contains(branch)? {
            return Err(ChainError::UnknownParent(*branch).into());
        }
        self.chain
            .ancestor_hash_at(branch, seed_height)?
            .ok_or(NodeError::UnknownSeedBlock { seed_height })
    }

    /// ブロックをファイルに書き出す。
    pub fn export_blocks(&self, dir: &Path) -> Result<usize, NodeError> {
        Ok(self.chain.store().export_blocks(dir)?)
    }
}

/// 既に知っているヘッダのブロックを渡すときの PoW 検証器。
///
/// チェーンは、知っているヘッダの PoW を見直さない。受け取った時点で
/// 検証済みだからである。それでも検証器を用意すると、RandomX の
/// キャッシュ (256 MB) をそのヘッダのエポックに合わせて作り直すことになる。
///
/// 初期同期では、シードの境目より下の本体を取り寄せている間に、境目より
/// 上の新しいヘッダがピアから流れてくる。以前は両者が届くたびに二つの
/// エポックを行き来し、1 回あたり数秒かかる作り直しを延々と繰り返して
/// 同期が大きく遅れた。
///
/// **呼ばれたら通さない。** 呼ばれないはずだが、万一呼ばれても検証を
/// 素通りさせることはない。
struct HeaderAlreadyChecked;

impl PowVerifier for HeaderAlreadyChecked {
    fn verify(&self, _header: &BlockHeader) -> bool {
        false
    }
}

/// [`Node::pow_verdicts`] が 1 回に確かめるヘッダの数の上限。
///
/// シードは高さの [`SEED_LAG`](oag_consensus::params::SEED_LAG) (64) 以上前に
/// あるので、これを超えなければ、どのヘッダのシードも先頭の親から引ける。
pub const POW_WAVE: usize = 64;

/// ヘッダの PoW を並べて確かめるスレッドの数の上限。
///
/// 1 本につき VM の 2 MB と JIT の符号だけが増える (キャッシュは共有)。
/// 64 件を 8 本で分けると 1 本 8 件であり、それより増やしても VM を建てる
/// 手間が目立ってくる。
const MAX_POW_THREADS: usize = 8;

/// 既定のスレッドの数。CPU の数 (上限 [`MAX_POW_THREADS`])。
fn default_pow_threads() -> usize {
    std::thread::available_parallelism()
        .map_or(1, |n| n.get())
        .clamp(1, MAX_POW_THREADS)
}

/// `members` のヘッダの PoW を、`threads` 本のスレッドで確かめる。
///
/// 各スレッドはキャッシュの上に自分の VM を建て、番号を 1 つずつ取って
/// いく。VM を建てられなかったスレッドは何も取らずに抜けるので、その分は
/// ほかのスレッドが確かめる。全部が抜けたら判定は `None` のまま残る。
fn verify_in_parallel(
    shared: &SharedCache,
    headers: &[BlockHeader],
    members: &[usize],
    threads: usize,
) -> Vec<(usize, Option<bool>)> {
    let next = AtomicUsize::new(0);
    let workers = threads.min(members.len());
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .filter_map(|_| {
                std::thread::Builder::new()
                    .name("oag-pow-verify".to_string())
                    .spawn_scoped(scope, || {
                        let mut out = Vec::new();
                        let Ok(verifier) = shared.verifier() else {
                            return out;
                        };
                        loop {
                            let k = next.fetch_add(1, Ordering::Relaxed);
                            let Some(&i) = members.get(k) else {
                                break;
                            };
                            // 誤り (エポックの食い違いなど) は判定しない。
                            out.push((i, verifier.check(&headers[i]).ok()));
                        }
                        out
                    })
                    .ok()
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap_or_default())
            .collect()
    })
}

/// [`Node::pow_verdicts`] が先に出した判定を、チェーンに渡すための検証器。
///
/// 判定を出したヘッダ以外が来たら通さない。来ないはずだが、来ても素通り
/// させない。
struct Judged {
    hash: Hash,
    meets: bool,
}

impl PowVerifier for Judged {
    fn verify(&self, header: &BlockHeader) -> bool {
        self.meets && header.hash() == self.hash
    }
}

/// 取り消された枝に入っていたトランザクションを、**古い順に**集める。
///
/// `Reorg::disconnected` は先端に近い順に並んでいる。そのまま戻すと子が親
/// より先になり、親をまだ知らない時点で子を検証することになる。逆から見る。
///
/// コインベースは戻さない。取り消された枝のコインベースはもう存在しない
/// 報酬であり、mempool は受け付けない (`docs/SPEC.md` §10.6)。
///
/// 本体を持っていないブロックは飛ばす。取り消したばかりの枝なので通常は
/// 揃っているが、揃っていないことを失敗にはしない。戻せなかった支払いは
/// 送った側が送り直せるが、ここで止まるとノードが先へ進めない。
fn orphaned_transactions(
    reorg: &Reorg,
    mut block_of: impl FnMut(&Hash) -> Result<Option<Block>, StoreError>,
) -> Result<Vec<oag_consensus::Transaction>, NodeError> {
    let mut orphaned = Vec::new();
    for hash in reorg.disconnected.iter().rev() {
        if let Some(block) = block_of(hash)? {
            orphaned.extend(Mempool::transactions_to_resubmit(&block));
        }
    }
    Ok(orphaned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use oag_consensus::tx::{OutPoint, TxInput, CURRENT_TX_VERSION};
    use oag_consensus::{Transaction, TxOutput};
    use oag_primitives::hash;
    use std::collections::HashMap;

    /// 指定した親を使うトランザクション。`tag` を変えれば別の txid になる。
    fn spending(tag: &[u8], parent: Hash) -> Transaction {
        let mut input = TxInput::new(OutPoint::new(parent, 0));
        input.signature = tag.to_vec();
        Transaction {
            version: CURRENT_TX_VERSION,
            inputs: vec![input],
            outputs: vec![TxOutput::new("1".parse().unwrap(), Lock::unspendable())],
            locktime: 0,
        }
    }

    fn coinbase(tag: &[u8]) -> Transaction {
        let mut input = TxInput::new(OutPoint::null());
        input.signature = tag.to_vec();
        Transaction {
            version: CURRENT_TX_VERSION,
            inputs: vec![input],
            outputs: vec![TxOutput::new("50".parse().unwrap(), Lock::unspendable())],
            locktime: 0,
        }
    }

    fn block_of(transactions: Vec<Transaction>) -> Block {
        Block {
            header: BlockHeader {
                version: 1,
                prev_hash: Hash::ZERO,
                merkle_root: Hash::ZERO,
                timestamp: 0,
                difficulty: 1,
                height: 0,
                nonce: 0,
            },
            transactions,
        }
    }

    #[test]
    fn the_undone_branch_is_collected_oldest_first() {
        // `disconnected` は先端に近い順である。そのまま戻すと、子が親より
        // 先に来る。
        let parent = spending(b"parent", hash::txid(b"funding"));
        let child = spending(b"child", parent.txid());
        let older = block_of(vec![coinbase(b"cb1"), parent.clone()]);
        let newer = block_of(vec![coinbase(b"cb2"), child.clone()]);

        let blocks: HashMap<Hash, Block> =
            HashMap::from([(hash::txid(b"older"), older), (hash::txid(b"newer"), newer)]);
        let reorg = Reorg {
            // 先端に近い順。
            disconnected: vec![hash::txid(b"newer"), hash::txid(b"older")],
            connected: Vec::new(),
        };

        let orphaned = orphaned_transactions(&reorg, |h| Ok(blocks.get(h).cloned())).unwrap();

        assert_eq!(
            orphaned,
            vec![parent, child],
            "not ordered oldest first (a child comes before its parent)"
        );
    }

    #[test]
    fn a_coinbase_from_the_undone_branch_is_not_put_back() {
        // 取り消した枝のコインベースはもう存在しない報酬である。
        let payment = spending(b"payment", hash::txid(b"funding"));
        let block = block_of(vec![coinbase(b"cb"), payment.clone()]);
        let blocks: HashMap<Hash, Block> = HashMap::from([(hash::txid(b"only"), block)]);
        let reorg = Reorg {
            disconnected: vec![hash::txid(b"only")],
            connected: Vec::new(),
        };

        let orphaned = orphaned_transactions(&reorg, |h| Ok(blocks.get(h).cloned())).unwrap();

        assert_eq!(orphaned, vec![payment]);
    }

    /// 使い捨ての記憶域でノードを開く。
    fn open_regtest(tag: &str) -> (std::path::PathBuf, Node) {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("oag-node-{tag}-{}-{nanos}", std::process::id()));
        let node = Node::open(Network::Regtest, &dir).unwrap();
        (dir, node)
    }

    #[test]
    fn a_known_header_does_not_build_a_verifier() {
        // 知っているヘッダの PoW は見直さない。検証器を用意すれば、
        // RandomX のキャッシュを作り直すだけ無駄になる。
        let (dir, mut node) = open_regtest("known-header");
        let genesis = crate::genesis::genesis_for(Network::Regtest);

        let outcome = node.accept_header(&genesis.header, 0).unwrap();

        assert_eq!(outcome, HeaderOutcome::Known);
        assert!(
            node.verifier.is_none(),
            "built a verifier for a known header"
        );
        drop(node);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_body_of_a_known_header_does_not_build_a_verifier() {
        // ヘッダ先行の同期で届く本体は、ヘッダが検証済みである。
        let (dir, mut node) = open_regtest("known-body");
        let genesis = crate::genesis::genesis_for(Network::Regtest);

        let outcome = node.accept_block(genesis, 0).unwrap();

        assert!(matches!(outcome, AcceptOutcome::Duplicate));
        assert!(
            node.verifier.is_none(),
            "built a verifier for a known block"
        );
        drop(node);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_missing_body_does_not_stop_the_reorg() {
        let reorg = Reorg {
            disconnected: vec![hash::txid(b"gone")],
            connected: Vec::new(),
        };

        let orphaned = orphaned_transactions(&reorg, |_| Ok(None)).unwrap();

        assert!(orphaned.is_empty());
    }

    /// 1 分おきの時刻で `count` 個掘り、ヘッダを返す。難易度は 1 のまま。
    fn mine(node: &mut Node, count: u64) -> (Vec<BlockHeader>, i64) {
        let mut now = crate::genesis::genesis_for(Network::Regtest)
            .header
            .timestamp;
        let mut headers = Vec::new();
        for i in 0..count {
            now += 60;
            let template = node.mining_template(&Lock::unspendable(), now, i).unwrap();
            assert_eq!(template.header.difficulty, 1, "regtest left difficulty 1");
            let block = template.into_block();
            headers.push(block.header);
            node.accept_mined(block, now).unwrap();
        }
        (headers, now)
    }

    #[test]
    fn headers_judged_in_parallel_are_accepted_like_one_by_one() {
        let (dir_a, mut a) = open_regtest("pow-wave-a");
        let (headers, now) = mine(&mut a, 100);
        let (dir_b, mut b) = open_regtest("pow-wave-b");
        b.set_pow_threads(4);

        for wave in headers.chunks(POW_WAVE) {
            let verdicts = b.pow_verdicts(wave);
            assert_eq!(
                verdicts,
                vec![Some(true); wave.len()],
                "not judged in parallel"
            );
            for (header, verdict) in wave.iter().zip(verdicts) {
                let outcome = b.accept_header_with_verdict(header, verdict, now).unwrap();
                assert_eq!(outcome, HeaderOutcome::New);
            }
        }
        assert_eq!(
            b.chain.best_header().unwrap().hash,
            a.chain.best_header().unwrap().hash
        );
        // 知っているものは判定し直さない。
        assert_eq!(b.pow_verdicts(&headers[..10]), vec![None; 10]);

        drop((a, b));
        let _ = std::fs::remove_dir_all(dir_a);
        let _ = std::fs::remove_dir_all(dir_b);
    }

    #[test]
    fn a_parallel_verdict_is_what_the_chain_uses() {
        let (dir_a, mut a) = open_regtest("pow-verdict-a");
        let (headers, now) = mine(&mut a, 3);
        let (dir_b, mut b) = open_regtest("pow-verdict-b");
        b.set_pow_threads(4);

        // 難易度を書き換えたヘッダは PoW を満たさない。判定は 1 件ずつの
        // 検証器と同じになる。
        let mut forged = headers[1];
        forged.difficulty = u64::MAX;
        assert_eq!(
            b.pow_verdicts(&[headers[0], forged]),
            vec![Some(true), Some(false)]
        );

        // 「満たさない」の判定を渡せば、正しいヘッダでも撥ねる。
        // すなわち判定は実際に使われている。
        assert!(b
            .accept_header_with_verdict(&headers[0], Some(false), now)
            .is_err());
        assert_eq!(
            b.accept_header_with_verdict(&headers[0], Some(true), now)
                .unwrap(),
            HeaderOutcome::New
        );

        drop((a, b));
        let _ = std::fs::remove_dir_all(dir_a);
        let _ = std::fs::remove_dir_all(dir_b);
    }

    #[test]
    fn headers_that_cannot_be_judged_are_left_to_the_one_by_one_path() {
        let (dir_a, mut a) = open_regtest("pow-skip-a");
        let (headers, _) = mine(&mut a, 4);
        let (dir_b, mut b) = open_regtest("pow-skip-b");

        // 1 本なら並べない。検証器も用意しない。
        b.set_pow_threads(1);
        assert_eq!(b.pow_verdicts(&headers), vec![None; 4]);
        assert!(
            b.verifier.is_none(),
            "built a verifier without judging anything"
        );

        b.set_pow_threads(4);
        // 親を知らない波は判定しない。
        assert_eq!(b.pow_verdicts(&headers[1..]), vec![None; 3]);
        assert!(
            b.verifier.is_none(),
            "built a verifier for an unknown parent"
        );
        // 繋がっていないところから先は判定しない。
        let gap = [headers[0], headers[2], headers[3]];
        assert_eq!(b.pow_verdicts(&gap), vec![Some(true), None, None]);

        drop((a, b));
        let _ = std::fs::remove_dir_all(dir_a);
        let _ = std::fs::remove_dir_all(dir_b);
    }

    #[test]
    fn a_wave_across_the_seed_boundary_uses_the_right_seed_on_each_side() {
        // 高さ 2112 からシードが高さ 2048 のブロックに変わる。その境目を
        // またぐ波で、両側がそれぞれ正しいシードで判定されること。
        //
        // regtest の難易度 1 ではどのハッシュも通るので、シードを取り違えても
        // 見えない。そこで波のヘッダを難易度 2 に書き換えて繋ぎ直し、1 件
        // ずつの検証器の判定と突き合わせる。64 件がたまたま揃う見込みは無い。
        let (dir_a, mut a) = open_regtest("pow-epoch-a");
        let base = 2_090u64;
        let (headers, now) = mine(&mut a, base - 1 + POW_WAVE as u64);
        let (dir_b, mut b) = open_regtest("pow-epoch-b");
        b.set_pow_threads(4);
        for wave in headers[..base as usize - 1].chunks(POW_WAVE) {
            let verdicts = b.pow_verdicts(wave);
            for (header, verdict) in wave.iter().zip(verdicts) {
                b.accept_header_with_verdict(header, verdict, now).unwrap();
            }
        }

        let mut forged: Vec<BlockHeader> = headers[base as usize - 1..][..POW_WAVE].to_vec();
        let mut prev = headers[base as usize - 2].hash();
        for header in &mut forged {
            header.prev_hash = prev;
            header.difficulty = 2;
            prev = header.hash();
        }
        assert_eq!(forged[0].height, base);
        assert!(forged.iter().any(|h| seed_height(h.height) == 0));
        assert!(forged.iter().any(|h| seed_height(h.height) == 2_048));

        let first = RandomXVerifier::new(&a.seed_for(0, &headers[0].hash()).unwrap(), 0).unwrap();
        let second =
            RandomXVerifier::new(&a.seed_for(2_048, &headers[2_100].hash()).unwrap(), 2_048)
                .unwrap();
        let expected: Vec<Option<bool>> = forged
            .iter()
            .map(|h| {
                let v = if seed_height(h.height) == 0 {
                    &first
                } else {
                    &second
                };
                Some(v.check(h).unwrap())
            })
            .collect();
        assert!(expected.contains(&Some(true)) && expected.contains(&Some(false)));

        assert_eq!(b.pow_verdicts(&forged), expected);

        drop((a, b));
        let _ = std::fs::remove_dir_all(dir_a);
        let _ = std::fs::remove_dir_all(dir_b);
    }
}
