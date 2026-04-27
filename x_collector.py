"""
x_collector.py
==============
X（旧Twitter）投稿収集・銘柄分析モジュール
twscrape ベース（X公式APIキー不要・無料）

DB設計
------
x_users         : 追跡ユーザー
x_tweets        : 収集済みツイート（tweet_id PRIMARY KEY で重複防止）
x_stock_mentions: 銘柄メンション（tweet_id × stock_code UNIQUE）
x_noise_words   : ノイズワード辞書
x_fetch_log     : 取得履歴ログ
"""

from __future__ import annotations

import asyncio
import base64
import concurrent.futures
import os
import re
import secrets
import sqlite3
from typing import Optional


# ── twscrape XClIdGen パッチ ────────────────────────────────────────────────
# twscrape 0.17.0 の XClIdGen.create() は X の JS 解析に依存しているが、
# X の JS 構造変更により IndexError で失敗する。
# x-client-transaction-id の生成をランダム値で代替してバイパスする。
def _patch_twscrape_xclid() -> None:
    try:
        from twscrape import queue_client as _qc

        class _DummyXClIdGen:
            def calc(self, method: str, path: str) -> str:
                # X が要求する形式に近い base64 ランダム文字列
                return base64.b64encode(secrets.token_bytes(48)).decode()

        async def _dummy_get(cls_or_username, fresh=False):  # type: ignore
            return _DummyXClIdGen()

        _qc.XClIdGenStore.get = classmethod(  # type: ignore[attr-defined]
            lambda cls, username, fresh=False: _dummy_get(username, fresh)
        )
    except Exception:
        pass  # パッチ失敗しても続行


_patch_twscrape_xclid()

# ── パス ─────────────────────────────────────────────────────────────────────
_DIR          = os.path.dirname(__file__)
_DB_PATH      = os.path.join(_DIR, "x_tweets.db")
_TW_ACCT_DB   = os.path.join(_DIR, "twscrape_accounts.db")  # twscrape 内部用

# ── デフォルトノイズワード ────────────────────────────────────────────────────
_DEFAULT_NOISE_WORDS: list[str] = [
    "おはよう", "こんにちは", "こんばんは", "お疲れ", "ありがとう",
    "おやすみ", "天気", "ランチ", "晩ごはん", "朝ごはん",
    "誕生日", "週末", "休日", "旅行", "映画",
    "サッカー", "野球", "バスケ", "フォロー", "リプ",
    "RT @", "いいね", "拡散", "よろしく",
]

# ── センチメントキーワード ─────────────────────────────────────────────────────
_BULLISH: list[str] = [
    "買い", "強い", "上昇", "ブレイク", "陽線", "爆上げ", "注目", "仕込み",
    "上げ", "高値", "底打ち", "反発", "好決算", "増益", "急騰", "好調",
    "チャンス", "積み増し", "強気", "買い場", "ロング", "buy", "bull",
    "上方修正", "増配", "自社株買い",
]
_BEARISH: list[str] = [
    "売り", "弱い", "下落", "割れ", "陰線", "急落", "危険", "損切り",
    "下げ", "安値", "天井", "崩れ", "悪決算", "減益", "暴落", "不調",
    "ショート", "売り場", "ベア", "警戒", "sell", "bear",
    "下方修正", "減配",
]

_MIN_TEXT_LEN = 30   # 短文フィルタ（文字数）


# ═══════════════════════════════════════════════════════════════════════════════
# DB
# ═══════════════════════════════════════════════════════════════════════════════

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """テーブル作成 & デフォルトデータ挿入"""
    with _db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS x_users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            username      TEXT    UNIQUE NOT NULL,
            display_name  TEXT,
            user_id       TEXT,
            last_tweet_id TEXT,
            is_active     INTEGER DEFAULT 1,
            added_at      TEXT    DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS x_tweets (
            tweet_id      TEXT PRIMARY KEY,
            username      TEXT NOT NULL,
            text          TEXT NOT NULL,
            created_at    TEXT NOT NULL,
            like_count    INTEGER DEFAULT 0,
            retweet_count INTEGER DEFAULT 0,
            reply_count   INTEGER DEFAULT 0,
            quote_count   INTEGER DEFAULT 0,
            fetched_at    TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_tw_user    ON x_tweets(username);
        CREATE INDEX IF NOT EXISTS idx_tw_created ON x_tweets(created_at);

        CREATE TABLE IF NOT EXISTS x_stock_mentions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            tweet_id     TEXT NOT NULL,
            stock_code   TEXT NOT NULL,
            score        REAL DEFAULT 0,
            mentioned_at TEXT NOT NULL,
            UNIQUE(tweet_id, stock_code),
            FOREIGN KEY(tweet_id) REFERENCES x_tweets(tweet_id)
        );
        CREATE INDEX IF NOT EXISTS idx_mn_code ON x_stock_mentions(stock_code);
        CREATE INDEX IF NOT EXISTS idx_mn_at   ON x_stock_mentions(mentioned_at);

        CREATE TABLE IF NOT EXISTS x_noise_words (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS x_fetch_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            username   TEXT NOT NULL,
            fetched_at TEXT DEFAULT (datetime('now')),
            new_count  INTEGER DEFAULT 0,
            status     TEXT
        );
        """)
        for w in _DEFAULT_NOISE_WORDS:
            conn.execute(
                "INSERT OR IGNORE INTO x_noise_words(word) VALUES(?)", (w,)
            )


# ═══════════════════════════════════════════════════════════════════════════════
# twscrape ヘルパー（非同期→同期ラッパー）
# ═══════════════════════════════════════════════════════════════════════════════

def _run(coro, timeout: int = 120):
    """asyncio コルーチンを別スレッドで実行（Streamlit のイベントループと干渉しない）"""
    def _worker():
        return asyncio.run(coro)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(_worker)
        return future.result(timeout=timeout)


# ── アカウントログイン ─────────────────────────────────────────────────────────

async def _alogin_password(username: str, password: str, email: str) -> str:
    """パスワードでログイン"""
    from twscrape import API  # type: ignore
    from twscrape.db import execute  # type: ignore
    api = API(_TW_ACCT_DB)
    # 既存エントリを削除してから再登録
    await execute(_TW_ACCT_DB, "DELETE FROM accounts WHERE username = :u", {"u": username})
    await api.pool.add_account(
        username=username,
        password=password,
        email=email,
        email_password="",
    )
    await api.pool.login_all()
    accounts = await api.pool.get_all()
    logged = [a for a in accounts if a.active]
    if not logged:
        errs = [a.error_msg for a in accounts if a.error_msg]
        detail = f"（{errs[0]}）" if errs else ""
        raise RuntimeError(
            f"ログインに失敗しました{detail}\n"
            "・ユーザー名／パスワード／メールアドレスを確認してください\n"
            "・Googleアカウント連携の場合は「Cookieログイン」をお使いください"
        )
    return f"ログイン成功: @{logged[0].username}"


async def _alogin_cookie(username: str, cookies: str) -> str:
    """ブラウザCookieでログイン（Googleアカウント連携に対応）

    cookies: "auth_token=xxx; ct0=yyy" 形式の文字列
    """
    from twscrape import API  # type: ignore
    from twscrape.db import execute  # type: ignore

    username = username.lstrip("@").strip()

    # Cookie文字列の簡易バリデーション
    cookie_dict: dict[str, str] = {}
    for part in cookies.split(";"):
        part = part.strip()
        if "=" in part:
            k, v = part.split("=", 1)
            cookie_dict[k.strip()] = v.strip()

    if "auth_token" not in cookie_dict:
        raise ValueError("auth_token が含まれていません。入力内容を確認してください。")
    if "ct0" not in cookie_dict:
        raise ValueError("ct0 が含まれていません。入力内容を確認してください。")

    api = API(_TW_ACCT_DB)

    # 既存エントリを削除（以前のログイン失敗が残っているとadd_accountがスキップされるため）
    await execute(_TW_ACCT_DB, "DELETE FROM accounts WHERE username = :u", {"u": username})

    # Cookie付きでアカウント登録
    # ct0 が含まれていると twscrape が自動で active=True にセットする
    await api.pool.add_account(
        username=username,
        password="cookie_auth",
        email="cookie@localhost",
        email_password="",
        cookies=cookies,
    )

    # active=True になっているか確認（login_all は不要・呼ぶと逆効果）
    accounts = await api.pool.get_all()
    logged = [a for a in accounts if a.active and a.username == username]
    if not logged:
        # DB から直接確認して詳細エラーを返す
        all_accts = await api.pool.get_all()
        this_acct = [a for a in all_accts if a.username == username]
        detail = ""
        if this_acct:
            a = this_acct[0]
            detail = f"\nactive={a.active}, error_msg={a.error_msg}, cookies={list(a.cookies.keys()) if a.cookies else '[]'}"
        raise RuntimeError(
            "Cookieの設定に失敗しました。\n"
            "・auth_token / ct0 の値をもう一度コピーし直してください\n"
            "・ブラウザでx.comを開き直してから再取得してください"
            + detail
        )
    return f"Cookieログイン成功: @{logged[0].username}"


def login_account(username: str, password: str, email: str) -> str:
    return _run(_alogin_password(username, password, email))


def login_account_cookie(username: str, cookies: str) -> str:
    return _run(_alogin_cookie(username, cookies))


async def _aget_login_status() -> list[dict]:
    from twscrape import API  # type: ignore
    api = API(_TW_ACCT_DB)
    accounts = await api.pool.get_all()
    return [
        {
            "username": a.username,
            "active":   a.active,
            "last_used": str(a.last_used)[:19] if a.last_used else "—",
        }
        for a in accounts
    ]


def get_login_status() -> list[dict]:
    return _run(_aget_login_status())


def logout_account(username: str) -> None:
    async def _do():
        from twscrape import API  # type: ignore
        api = API(_TW_ACCT_DB)
        await api.pool.delete_accounts(username)
    _run(_do())


# ── ユーザー追加（API呼び出しなし・即時保存）─────────────────────────────────

def add_user(username: str) -> dict:
    """ユーザーをDBに追加（user_idはツイート取得時に自動解決）"""
    username = username.lstrip("@").strip()
    if not username:
        raise ValueError("ユーザー名を入力してください")
    with _db() as conn:
        conn.execute("""
            INSERT INTO x_users(username, is_active)
            VALUES(?,1)
            ON CONFLICT(username) DO UPDATE SET is_active=1
        """, (username,))
    return {"username": username, "display_name": username, "user_id": None}


async def _aresolve_user_id(api, username: str) -> tuple[str, str]:
    """user_id と display_name を X API で解決してDBに保存"""
    user = await api.user_by_login(username)
    if not user:
        raise ValueError(f"ユーザー @{username} が見つかりません（アカウント非公開または存在しない可能性があります）")
    user_id      = str(user.id)
    display_name = user.displayname or username
    with _db() as conn:
        conn.execute("""
            UPDATE x_users SET user_id=?, display_name=? WHERE username=?
        """, (user_id, display_name, username))
    return user_id, display_name


def remove_user(username: str) -> None:
    with _db() as conn:
        conn.execute(
            "UPDATE x_users SET is_active=0 WHERE username=?", (username,)
        )


def get_users() -> list[dict]:
    with _db() as conn:
        rows = conn.execute(
            "SELECT * FROM x_users WHERE is_active=1 ORDER BY added_at"
        ).fetchall()
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════════════════════
# フィルタリング & スコアリング
# ═══════════════════════════════════════════════════════════════════════════════

def _get_noise_words() -> list[str]:
    with _db() as conn:
        rows = conn.execute("SELECT word FROM x_noise_words").fetchall()
    return [r["word"] for r in rows]


def _is_noisy(text: str, noise_words: list[str]) -> bool:
    tl = text.lower()
    return any(w.lower() in tl for w in noise_words)


def _extract_stock_codes(text: str) -> list[str]:
    """テキストから銘柄コードを抽出"""
    codes: set[str] = set()
    # 日本株: 4桁数字（1000〜9999）
    for m in re.finditer(r'(?<!\d)(\d{4})(?!\d)', text):
        c = m.group(1)
        if 1000 <= int(c) <= 9999:
            codes.add(c)
    # US株: $TICKER（1〜5大文字）
    for m in re.finditer(r'\$([A-Z]{1,5})\b', text):
        codes.add(m.group(1))
    return list(codes)


def _calc_score(like: int, rt: int, reply: int, text: str) -> float:
    score  = 1.0
    score += min(like  / 10.0, 3.0) * 0.30
    score += min(rt    /  5.0, 3.0) * 0.50
    score += min(reply /  5.0, 2.0) * 0.20
    tl    = text.lower()
    score += sum(1 for w in _BULLISH if w in tl) * 0.5
    score -= sum(1 for w in _BEARISH if w in tl) * 0.3
    return round(max(0.0, score), 2)


# ═══════════════════════════════════════════════════════════════════════════════
# ツイート取得メイン
# ═══════════════════════════════════════════════════════════════════════════════

async def _acollect_tweets(api, uid: int, limit: int = 100) -> list:
    """非同期ジェネレータからツイートをリストに収集"""
    tweets = []
    async for tw in api.user_tweets(uid, limit=limit):
        tweets.append(tw)
        if len(tweets) >= limit:
            break
    return tweets


async def _adiagnose() -> dict:
    """twscrape の状態を診断する"""
    import urllib.request
    from twscrape import API  # type: ignore
    api = API(_TW_ACCT_DB)
    result: dict = {}

    # ── ① 基本ネットワーク疎通（twscrape不使用）─────────────────────────────
    try:
        req = urllib.request.Request(
            "https://x.com",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result["network_x_com"] = f"OK (HTTP {resp.status})"
    except Exception as e:
        result["network_x_com"] = f"エラー: {type(e).__name__}: {e}"

    # ── ② Cookieを使った直接API疎通テスト ────────────────────────────────────
    accounts_pre = await api.pool.get_all()
    result["accounts"] = [
        {
            "username": a.username,
            "active": a.active,
            "error_msg": a.error_msg,
            "locks": {k: str(v) for k, v in (a.locks or {}).items()},
            "has_cookies": bool(a.cookies),
            "cookie_keys": list(a.cookies.keys()) if a.cookies else [],
        }
        for a in accounts_pre
    ]

    if not accounts_pre:
        result["error"] = "ログイン済みアカウントがありません"
        return result

    active = [a for a in accounts_pre if a.active]
    if not active:
        result["error"] = f"アクティブなアカウントがありません。error_msg: {accounts_pre[0].error_msg}"
        return result

    # ── ③ Cookieで直接GraphQL疎通テスト ──────────────────────────────────────
    acct = active[0]
    if acct.cookies:
        cookie_str = "; ".join(f"{k}={v}" for k, v in acct.cookies.items())
        ct0 = acct.cookies.get("ct0", "")
        try:
            import json as _json
            url = (
                "https://api.twitter.com/graphql/SAMkL5y_N9pmahSw8yy6gA/UserByScreenName"
                "?variables=%7B%22screen_name%22%3A%22twitter%22%7D"
                "&features=%7B%22hidden_profile_subscriptions_enabled%22%3Atrue%7D"
            )
            req2 = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "*/*",
                "Accept-Language": "ja,en;q=0.9",
                "Cookie": cookie_str,
                "x-csrf-token": ct0,
                "authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
                "x-twitter-active-user": "yes",
                "x-twitter-client-language": "ja",
            })
            with urllib.request.urlopen(req2, timeout=15) as resp2:
                body = resp2.read(500).decode("utf-8", errors="replace")
                result["direct_api_test"] = f"OK (HTTP {resp2.status}) - {body[:200]}"
        except urllib.error.HTTPError as e:
            body = e.read(300).decode("utf-8", errors="replace") if e.fp else ""
            result["direct_api_test"] = f"HTTP {e.code}: {body[:200]}"
        except Exception as e:
            result["direct_api_test"] = f"エラー: {type(e).__name__}: {e}"
    else:
        result["direct_api_test"] = "Cookieなし - スキップ"

    # ── ④ twscrape 経由テスト ────────────────────────────────────────────────
    try:
        test_user = await asyncio.wait_for(
            api.user_by_login("twitter"), timeout=20
        )
        result["twscrape_test"] = "OK" if test_user else "None返却"
    except asyncio.TimeoutError:
        result["twscrape_test"] = "タイムアウト（20秒）"
    except Exception as e:
        result["twscrape_test"] = f"エラー: {type(e).__name__}: {e}"

    # テスト後のアカウント状態
    accounts_after = await api.pool.get_all()
    result["active_after_test"] = [a.active for a in accounts_after]
    result["error_after_test"] = [a.error_msg for a in accounts_after if a.error_msg]

    return result


def diagnose() -> dict:
    return _run(_adiagnose(), timeout=60)


async def _afetch_user(user: dict, noise: list[str], api) -> dict:
    """1ユーザー分のツイートを差分取得してDBへ保存"""
    uname     = user["username"]
    uid_str   = user.get("user_id")
    since_id  = user.get("last_tweet_id")
    new_count = 0
    latest_id: Optional[str] = since_id

    # user_id 未解決の場合はここで解決（タイムアウト30秒）
    if not uid_str:
        try:
            uid_str, _ = await asyncio.wait_for(
                _aresolve_user_id(api, uname), timeout=30
            )
        except asyncio.TimeoutError:
            # アカウントが非アクティブになっていないか確認
            accts_now = await api.pool.get_all()
            inactive = [a for a in accts_now if not a.active]
            if inactive:
                errs = [a.error_msg for a in inactive if a.error_msg]
                detail = f"（{errs[0]}）" if errs else ""
                return {"error": (
                    f"@{uname} ユーザーID取得タイムアウト。"
                    f"アカウントが非アクティブになりました{detail}。"
                    "Cookieが期限切れの可能性があります。再ログインしてください。"
                )}
            return {"error": f"@{uname} のユーザーID取得がタイムアウトしました（30秒）。X側のレート制限かもしれません。"}
        except Exception as e:
            accts_now = await api.pool.get_all()
            inactive = [a for a in accts_now if not a.active]
            if inactive:
                errs = [a.error_msg for a in inactive if a.error_msg]
                detail = f"（{errs[0]}）" if errs else ""
                return {"error": (
                    f"@{uname}: APIエラー{detail}。"
                    "Cookieが無効または期限切れです。ブラウザから再取得してログインし直してください。"
                )}
            return {"error": f"@{uname}: {e}"}

    uid = int(uid_str)

    # ツイート収集（タイムアウト60秒）
    try:
        raw_tweets = await asyncio.wait_for(
            _acollect_tweets(api, uid, limit=100), timeout=60
        )
    except asyncio.TimeoutError:
        return {"error": f"@{uname} のツイート取得がタイムアウトしました（60秒）"}
    except Exception as e:
        return {"error": f"@{uname} 取得エラー: {e}"}

    for tw in raw_tweets:
        tid = str(tw.id)

        # 差分チェック
        if since_id and tid <= since_id:
            continue

        # リツイート除外
        if tw.retweetedTweet is not None:
            continue

        text    = tw.rawContent or ""
        created = tw.date.isoformat() if tw.date else ""

        # フィルタリング
        if len(text) < _MIN_TEXT_LEN:
            continue
        if _is_noisy(text, noise):
            continue

        likes  = tw.likeCount    or 0
        rts    = tw.retweetCount or 0
        reps   = tw.replyCount   or 0
        quotes = tw.quoteCount   or 0

        with _db() as conn:
            cur = conn.execute("""
                INSERT OR IGNORE INTO x_tweets(
                    tweet_id, username, text, created_at,
                    like_count, retweet_count, reply_count, quote_count
                ) VALUES(?,?,?,?,?,?,?,?)
            """, (tid, uname, text, created, likes, rts, reps, quotes))
            if cur.rowcount > 0:
                new_count += 1

        # 銘柄メンション & スコア
        codes = _extract_stock_codes(text)
        score = _calc_score(likes, rts, reps, text)
        for code in codes:
            with _db() as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO x_stock_mentions(
                        tweet_id, stock_code, score, mentioned_at
                    ) VALUES(?,?,?,?)
                """, (tid, code, score, created))

        if latest_id is None or tid > latest_id:
            latest_id = tid

    # last_tweet_id 更新
    if latest_id and latest_id != since_id:
        with _db() as conn:
            conn.execute(
                "UPDATE x_users SET last_tweet_id=? WHERE username=?",
                (latest_id, uname),
            )

    with _db() as conn:
        conn.execute(
            "INSERT INTO x_fetch_log(username, new_count, status) VALUES(?,?,'ok')",
            (uname, new_count),
        )

    return {"new_count": new_count}


async def _afetch_all() -> dict[str, dict]:
    from twscrape import API  # type: ignore
    from twscrape.db import execute  # type: ignore
    init_db()
    api   = API(_TW_ACCT_DB)
    users = get_users()
    noise = _get_noise_words()

    if not users:
        return {}

    # ── ログイン済みアカウントの確認 ──────────────────────────────────────────
    accounts = await api.pool.get_all()
    active_accounts = [a for a in accounts if a.active]

    if not accounts:
        return {"_error": {"error": "Xアカウントがログインされていません。先にCookieでログインしてください。"}}

    if not active_accounts:
        # 全アカウントが非アクティブ → Cookie再ログインが必要
        err_msgs = [a.error_msg for a in accounts if a.error_msg]
        detail = f"（{err_msgs[0]}）" if err_msgs else ""
        return {"_error": {
            "error": (
                f"アクティブなアカウントがありません{detail}。\n"
                "CookieのログインでCookieが期限切れか無効の可能性があります。\n"
                "ブラウザから auth_token と ct0 を再取得してCookieログインを再実行してください。"
            )
        }}

    results: dict[str, dict] = {}
    for user in users:
        results[user["username"]] = await _afetch_user(user, noise, api)
    return results


def fetch_all_users() -> dict[str, dict]:
    """全アクティブユーザーのツイートを差分取得（同期インターフェース）"""
    return _run(_afetch_all())


# ═══════════════════════════════════════════════════════════════════════════════
# 分析クエリ
# ═══════════════════════════════════════════════════════════════════════════════

def get_stock_summary(days: int = 30, min_mentions: int = 1) -> list[dict]:
    with _db() as conn:
        rows = conn.execute("""
            SELECT
                m.stock_code,
                COUNT(*)              AS mention_count,
                ROUND(SUM(m.score),2) AS total_score,
                ROUND(AVG(m.score),2) AS avg_score,
                MAX(m.mentioned_at)   AS latest_at,
                COUNT(DISTINCT t.username) AS user_count
            FROM x_stock_mentions m
            JOIN x_tweets t ON t.tweet_id = m.tweet_id
            WHERE m.mentioned_at >= datetime('now', ? || ' days')
            GROUP BY m.stock_code
            HAVING mention_count >= ?
            ORDER BY total_score DESC, mention_count DESC
        """, (f"-{days}", min_mentions)).fetchall()
    return [dict(r) for r in rows]


def get_tweets_for_stock(stock_code: str, days: int = 30, limit: int = 50) -> list[dict]:
    with _db() as conn:
        rows = conn.execute("""
            SELECT
                t.tweet_id, t.username, t.text, t.created_at,
                t.like_count, t.retweet_count, t.reply_count,
                m.score
            FROM x_stock_mentions m
            JOIN x_tweets t ON t.tweet_id = m.tweet_id
            WHERE m.stock_code = ?
              AND m.mentioned_at >= datetime('now', ? || ' days')
            ORDER BY m.score DESC, t.created_at DESC
            LIMIT ?
        """, (stock_code, f"-{days}", limit)).fetchall()
    return [dict(r) for r in rows]


def get_fetch_log(limit: int = 30) -> list[dict]:
    with _db() as conn:
        rows = conn.execute(
            "SELECT * FROM x_fetch_log ORDER BY fetched_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_db_stats() -> dict:
    with _db() as conn:
        tweets   = conn.execute("SELECT COUNT(*) FROM x_tweets").fetchone()[0]
        users    = conn.execute("SELECT COUNT(*) FROM x_users WHERE is_active=1").fetchone()[0]
        mentions = conn.execute("SELECT COUNT(*) FROM x_stock_mentions").fetchone()[0]
        stocks   = conn.execute("SELECT COUNT(DISTINCT stock_code) FROM x_stock_mentions").fetchone()[0]
    return {"tweets": tweets, "active_users": users,
            "mentions": mentions, "unique_stocks": stocks}


# ── ノイズワード管理 ──────────────────────────────────────────────────────────

def add_noise_word(word: str) -> None:
    with _db() as conn:
        conn.execute("INSERT OR IGNORE INTO x_noise_words(word) VALUES(?)", (word.strip(),))


def remove_noise_word(word: str) -> None:
    with _db() as conn:
        conn.execute("DELETE FROM x_noise_words WHERE word=?", (word,))


def get_noise_words() -> list[str]:
    with _db() as conn:
        rows = conn.execute("SELECT word FROM x_noise_words ORDER BY word").fetchall()
    return [r["word"] for r in rows]
