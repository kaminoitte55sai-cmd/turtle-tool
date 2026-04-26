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
import os
import re
import sqlite3
from typing import Optional

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

def _run(coro):
    """asyncio コルーチンを同期的に実行するユーティリティ"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Jupyter / Streamlit の既存ループ内では nest_asyncio が必要
            import nest_asyncio  # type: ignore
            nest_asyncio.apply()
            return loop.run_until_complete(coro)
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


def _tw_api():
    """twscrape API インスタンスを返す"""
    from twscrape import API  # type: ignore
    return API(_TW_ACCT_DB)


# ── アカウントログイン ─────────────────────────────────────────────────────────

async def _alogin_password(username: str, password: str, email: str) -> str:
    """パスワードでログイン"""
    from twscrape import API  # type: ignore
    api = API(_TW_ACCT_DB)
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
    api = API(_TW_ACCT_DB)

    # Cookie文字列をパース
    cookie_dict: dict[str, str] = {}
    for part in cookies.split(";"):
        part = part.strip()
        if "=" in part:
            k, v = part.split("=", 1)
            cookie_dict[k.strip()] = v.strip()

    if "auth_token" not in cookie_dict:
        raise ValueError("Cookie に auth_token が含まれていません。\nauth_token=xxx; ct0=yyy の形式で入力してください。")
    if "ct0" not in cookie_dict:
        raise ValueError("Cookie に ct0 が含まれていません。\nauth_token=xxx; ct0=yyy の形式で入力してください。")

    # Cookie付きでアカウント登録（パスワード不要）
    # username はダミーでも動くが、実際のIDを推奨
    await api.pool.add_account(
        username=username,
        password="cookie_auth",       # Cookie認証時はパスワード不使用
        email="cookie@example.com",   # Cookie認証時はメール不使用
        email_password="",
        cookies=cookies,
    )

    # login_all は Cookie があれば実際のログインをスキップする
    await api.pool.login_all()

    accounts = await api.pool.get_all()
    logged = [a for a in accounts if a.active]
    if not logged:
        errs = [a.error_msg for a in accounts if a.error_msg]
        detail = f"（{errs[0]}）" if errs else ""
        raise RuntimeError(
            f"Cookieログインに失敗しました{detail}\n"
            "・auth_token と ct0 の値が正しいか確認してください\n"
            "・Cookieの有効期限が切れていないか確認してください（再度ブラウザからコピーしてください）"
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


# ── ユーザー情報取得 ──────────────────────────────────────────────────────────

async def _aadd_user(username: str) -> dict:
    from twscrape import API  # type: ignore
    api = API(_TW_ACCT_DB)
    username = username.lstrip("@").strip()
    user = await api.user_by_login(username)
    if not user:
        raise ValueError(f"ユーザー @{username} が見つかりません")
    display_name = user.displayname or username
    user_id      = str(user.id)
    with _db() as conn:
        conn.execute("""
            INSERT INTO x_users(username, display_name, user_id, is_active)
            VALUES(?,?,?,1)
            ON CONFLICT(username) DO UPDATE SET
                display_name = excluded.display_name,
                user_id      = excluded.user_id,
                is_active    = 1
        """, (username, display_name, user_id))
    return {"username": username, "display_name": display_name, "user_id": user_id}


def add_user(username: str) -> dict:
    return _run(_aadd_user(username))


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

async def _afetch_user(user: dict, noise: list[str]) -> dict:
    """1ユーザー分のツイートを差分取得してDBへ保存"""
    from twscrape import API  # type: ignore
    api       = API(_TW_ACCT_DB)
    uname     = user["username"]
    uid_str   = user.get("user_id")
    since_id  = user.get("last_tweet_id")
    new_count = 0
    latest_id: Optional[str] = since_id

    if not uid_str:
        return {"error": "user_id 未取得。ユーザーを再追加してください。"}

    uid = int(uid_str)
    try:
        async for tw in api.user_tweets(uid, limit=100):
            tid = str(tw.id)

            # 差分チェック（since_id より古いものはスキップ）
            if since_id and tid <= since_id:
                continue

            # リツイート除外（twscrape の user_tweets は exclude RT 対応済みだが念のため）
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

    except Exception as e:
        return {"error": str(e)}

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
    init_db()
    users = get_users()
    noise = _get_noise_words()
    results: dict[str, dict] = {}
    for user in users:
        results[user["username"]] = await _afetch_user(user, noise)
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
