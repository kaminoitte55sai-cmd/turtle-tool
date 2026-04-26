"""
x_collector.py
==============
X（旧Twitter）投稿収集・銘柄分析モジュール

DB設計
------
x_users         : 追跡ユーザー
x_tweets        : 収集済みツイート
x_stock_mentions: 銘柄メンション（tweet × code）
x_noise_words   : ノイズワード
x_fetch_log     : 取得ログ
"""

from __future__ import annotations

import os
import re
import sqlite3
from typing import Optional

import requests

# ── パス ─────────────────────────────────────────────────────────────────────
_DB_PATH = os.path.join(os.path.dirname(__file__), "x_tweets.db")

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

# ── 定数 ──────────────────────────────────────────────────────────────────────
_MIN_TEXT_LEN = 30          # フィルタ: 最小文字数
_API_BASE     = "https://api.twitter.com/2"


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
        -- 追跡ユーザー
        CREATE TABLE IF NOT EXISTS x_users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            username      TEXT    UNIQUE NOT NULL,   -- @なし
            display_name  TEXT,
            user_id       TEXT,                      -- X API の数値ID
            last_tweet_id TEXT,                      -- 差分取得用
            is_active     INTEGER DEFAULT 1,
            added_at      TEXT    DEFAULT (datetime('now'))
        );

        -- ツイート本体
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

        -- 銘柄メンション（tweet × stock_code は UNIQUE）
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

        -- ノイズワード
        CREATE TABLE IF NOT EXISTS x_noise_words (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT UNIQUE NOT NULL
        );

        -- 取得ログ
        CREATE TABLE IF NOT EXISTS x_fetch_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            username   TEXT NOT NULL,
            fetched_at TEXT DEFAULT (datetime('now')),
            new_count  INTEGER DEFAULT 0,
            status     TEXT
        );
        """)
        # デフォルトノイズワード
        for w in _DEFAULT_NOISE_WORDS:
            conn.execute(
                "INSERT OR IGNORE INTO x_noise_words(word) VALUES(?)", (w,)
            )


# ═══════════════════════════════════════════════════════════════════════════════
# ユーザー管理
# ═══════════════════════════════════════════════════════════════════════════════

def add_user(username: str, bearer_token: str) -> dict:
    """ユーザーを追跡リストに追加（API でユーザーID取得）"""
    username = username.lstrip("@").strip()
    headers  = {"Authorization": f"Bearer {bearer_token}"}
    r = requests.get(
        f"{_API_BASE}/users/by/username/{username}",
        headers=headers,
        params={"user.fields": "name"},
        timeout=10,
    )
    if r.status_code == 404:
        raise ValueError(f"ユーザー @{username} が見つかりません")
    if r.status_code == 401:
        raise ValueError("Bearer Token が無効です")
    if r.status_code != 200:
        raise ValueError(f"API エラー {r.status_code}: {r.text[:200]}")

    data         = r.json().get("data", {})
    user_id      = data.get("id")
    display_name = data.get("name", username)

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
# フィルタリング
# ═══════════════════════════════════════════════════════════════════════════════

def _get_noise_words() -> list[str]:
    with _db() as conn:
        rows = conn.execute("SELECT word FROM x_noise_words").fetchall()
    return [r["word"] for r in rows]


def _is_noisy(text: str, noise_words: list[str]) -> bool:
    tl = text.lower()
    return any(w.lower() in tl for w in noise_words)


def _extract_stock_codes(text: str) -> list[str]:
    """テキストから銘柄コードを抽出する

    対応形式:
      - 日本株 4桁数字   : 「7203」「9984」など
      - US株 $ティッカー : 「$AAPL」「$NVDA」
    """
    codes: set[str] = set()

    # 日本株: 1000〜9999 の4桁数字（前後が数字でない）
    for m in re.finditer(r'(?<!\d)(\d{4})(?!\d)', text):
        c = m.group(1)
        if 1000 <= int(c) <= 9999:
            codes.add(c)

    # US株: $TICKER（1〜5大文字）
    for m in re.finditer(r'\$([A-Z]{1,5})\b', text):
        codes.add(m.group(1))

    return list(codes)


# ═══════════════════════════════════════════════════════════════════════════════
# スコアリング
# ═══════════════════════════════════════════════════════════════════════════════

def _calc_score(metrics: dict, text: str) -> float:
    """ツイートのスコアを計算

    基本スコア 1.0
    + エンゲージメント加算（いいね / RT / リプライ）
    + センチメント加算（強気・弱気ワード）
    """
    likes   = metrics.get("like_count",    0)
    rts     = metrics.get("retweet_count", 0)
    replies = metrics.get("reply_count",   0)

    score  = 1.0
    score += min(likes   / 10.0, 3.0) * 0.30   # いいねボーナス（最大+0.9）
    score += min(rts     /  5.0, 3.0) * 0.50   # RTボーナス（最大+1.5）
    score += min(replies /  5.0, 2.0) * 0.20   # リプライボーナス（最大+0.4）

    tl    = text.lower()
    bull  = sum(1 for w in _BULLISH if w in tl)
    bear  = sum(1 for w in _BEARISH if w in tl)
    score += bull * 0.5
    score -= bear * 0.3

    return round(max(0.0, score), 2)


# ═══════════════════════════════════════════════════════════════════════════════
# 取得メイン
# ═══════════════════════════════════════════════════════════════════════════════

def _api_fetch_tweets(
    user_id:      str,
    bearer_token: str,
    since_id:     Optional[str] = None,
    max_results:  int = 100,
) -> list[dict]:
    """X API v2 でツイートを取得（リツイート・リプライ除外）"""
    headers = {"Authorization": f"Bearer {bearer_token}"}
    params: dict = {
        "max_results":   min(max_results, 100),
        "exclude":       "retweets,replies",
        "tweet.fields":  "created_at,public_metrics,text",
    }
    if since_id:
        params["since_id"] = since_id

    r = requests.get(
        f"{_API_BASE}/users/{user_id}/tweets",
        headers=headers,
        params=params,
        timeout=15,
    )
    if r.status_code == 429:
        raise RuntimeError("APIレート制限に達しました。15分後に再試行してください。")
    if r.status_code == 401:
        raise RuntimeError("Bearer Token が無効です。設定を確認してください。")
    if r.status_code != 200:
        raise RuntimeError(f"API エラー {r.status_code}: {r.text[:300]}")

    return r.json().get("data") or []


def fetch_all_users(bearer_token: str) -> dict[str, dict]:
    """全アクティブユーザーのツイートを差分取得してDBへ保存

    Returns
    -------
    dict: {username: {"new_count": int} or {"error": str}}
    """
    init_db()
    users   = get_users()
    noise   = _get_noise_words()
    results: dict[str, dict] = {}

    for user in users:
        uname = user["username"]
        uid   = user.get("user_id")
        if not uid:
            results[uname] = {"error": "user_id 未取得。ユーザーを再追加してください。"}
            continue

        try:
            raw_tweets = _api_fetch_tweets(
                uid, bearer_token, since_id=user.get("last_tweet_id")
            )
        except Exception as e:
            results[uname] = {"error": str(e)}
            with _db() as conn:
                conn.execute(
                    "INSERT INTO x_fetch_log(username, new_count, status) VALUES(?,0,?)",
                    (uname, f"error: {e}"),
                )
            continue

        new_count = 0
        latest_id = user.get("last_tweet_id")

        for tw in raw_tweets:
            tweet_id = tw["id"]
            text     = tw.get("text", "")
            created  = tw.get("created_at", "")
            metrics  = tw.get("public_metrics", {})

            # ─ フィルタリング ─
            if len(text) < _MIN_TEXT_LEN:
                continue
            if _is_noisy(text, noise):
                continue

            # ─ ツイート保存 ─
            with _db() as conn:
                cur = conn.execute("""
                    INSERT OR IGNORE INTO x_tweets(
                        tweet_id, username, text, created_at,
                        like_count, retweet_count, reply_count, quote_count
                    ) VALUES(?,?,?,?,?,?,?,?)
                """, (
                    tweet_id, uname, text, created,
                    metrics.get("like_count",    0),
                    metrics.get("retweet_count", 0),
                    metrics.get("reply_count",   0),
                    metrics.get("quote_count",   0),
                ))
                if cur.rowcount > 0:
                    new_count += 1

            # ─ 銘柄メンション & スコア ─
            codes = _extract_stock_codes(text)
            score = _calc_score(metrics, text)
            for code in codes:
                with _db() as conn:
                    conn.execute("""
                        INSERT OR IGNORE INTO x_stock_mentions(
                            tweet_id, stock_code, score, mentioned_at
                        ) VALUES(?,?,?,?)
                    """, (tweet_id, code, score, created))

            # 最新ツイートID を追跡
            if latest_id is None or tweet_id > latest_id:
                latest_id = tweet_id

        # last_tweet_id を更新（差分取得の起点）
        if latest_id and latest_id != user.get("last_tweet_id"):
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

        results[uname] = {"new_count": new_count}

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# 分析クエリ
# ═══════════════════════════════════════════════════════════════════════════════

def get_stock_summary(days: int = 30, min_mentions: int = 1) -> list[dict]:
    """銘柄別サマリー（スコア降順）"""
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


def get_tweets_for_stock(
    stock_code: str,
    days:       int = 30,
    limit:      int = 50,
) -> list[dict]:
    """指定銘柄に言及したツイート一覧（スコア降順）"""
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


def get_recent_tweets(days: int = 7, limit: int = 100) -> list[dict]:
    with _db() as conn:
        rows = conn.execute("""
            SELECT * FROM x_tweets
            WHERE created_at >= datetime('now', ? || ' days')
            ORDER BY created_at DESC
            LIMIT ?
        """, (f"-{days}", limit)).fetchall()
    return [dict(r) for r in rows]


# ── ノイズワード管理 ──────────────────────────────────────────────────────────

def add_noise_word(word: str) -> None:
    with _db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO x_noise_words(word) VALUES(?)", (word.strip(),)
        )


def remove_noise_word(word: str) -> None:
    with _db() as conn:
        conn.execute("DELETE FROM x_noise_words WHERE word=?", (word,))


def get_noise_words() -> list[str]:
    with _db() as conn:
        rows = conn.execute(
            "SELECT word FROM x_noise_words ORDER BY word"
        ).fetchall()
    return [r["word"] for r in rows]


def get_db_stats() -> dict:
    """DB統計情報"""
    with _db() as conn:
        tweets   = conn.execute("SELECT COUNT(*) FROM x_tweets").fetchone()[0]
        users    = conn.execute("SELECT COUNT(*) FROM x_users WHERE is_active=1").fetchone()[0]
        mentions = conn.execute("SELECT COUNT(*) FROM x_stock_mentions").fetchone()[0]
        stocks   = conn.execute("SELECT COUNT(DISTINCT stock_code) FROM x_stock_mentions").fetchone()[0]
    return {
        "tweets":        tweets,
        "active_users":  users,
        "mentions":      mentions,
        "unique_stocks": stocks,
    }
