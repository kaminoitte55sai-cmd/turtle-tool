"""上場来高値データの永続化レイヤー（DB処理のみ）。

責務はここに閉じる:
  - SQLite スキーマの作成 / マイグレーション
  - 取得済み記事の記録（再取得を防ぐ）
  - 銘柄行の追加（code + article_date で重複排除）
  - 期間フィルタ・検索・集計クエリ

取得処理（modules/kabutan_high.py）と表示処理（pages/上場来高値.py）からは
完全に独立しており、このモジュールは requests もスクレイピングも一切知らない。

■ Streamlit Community Cloud での永続化について
Cloud はファイルシステムが揮発性で、再起動・再デプロイのたびに SQLite が消える。
そこで既存 app.py と同じ GitHub API 方式でスナップショット（JSON）を
リポジトリに書き戻し、起動時に復元する。GITHUB_TOKEN 未設定（ローカル実行）の
場合はスナップショット処理を丸ごとスキップし、純粋なローカル SQLite として動く。
"""

from __future__ import annotations

import base64
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

# リポジトリルート（modules/ の 1 つ上）
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "high_history.db")

# GitHub 上のスナップショット保存先（リポジトリ内の相対パス）
SNAPSHOT_REPO_PATH = "high_history_snapshot.json"

JST = timezone(timedelta(hours=9))

# 期間指定ラベル -> 遡る日数（None = 全期間）
PERIOD_DAYS: dict[str, int | None] = {
    "直近1か月": 30,
    "3か月": 91,
    "6か月": 183,
    "1年": 365,
    "全期間": None,
}

# sqlite3 のコネクションはスレッドを跨げないため、書き込みは直列化しておく
_write_lock = threading.Lock()


# ---------------------------------------------------------------------------
# 接続 / スキーマ
# ---------------------------------------------------------------------------


@contextmanager
def get_conn():
    """SQLite コネクションを返すコンテキストマネージャ。

    Streamlit は 1 リクエストごとに別スレッドで動くため、
    コネクションは使い捨てにして共有しない（check_same_thread 問題の回避）。
    """
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    """テーブルを作成する（存在すれば何もしない）。何度呼んでも安全。"""
    with get_conn() as conn:
        cur = conn.cursor()

        # 本体テーブル。仕様どおり code + article_date を一意にして重複を防ぐ。
        # sector / market は同じ正規表現から無料で取れる付加情報で、
        # 業種別の分析や将来の外部 API 連携で効くため保持している。
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS high_history (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                article_date TEXT NOT NULL,   -- 記事日付 'YYYY-MM-DD'
                code         TEXT NOT NULL,   -- 銘柄コード '7203' / '160A' など
                name         TEXT NOT NULL,   -- 銘柄名
                title        TEXT NOT NULL,   -- 記事タイトル
                sector       TEXT,            -- 業種（例: 電気機器）
                market       TEXT,            -- 市場（例: 東証Ｐ）
                UNIQUE (code, article_date)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_hh_date ON high_history (article_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_hh_code ON high_history (code)")

        # 取得済み記事の台帳。「既に取得済みの記事は取得しない」ための判定に使う。
        # 銘柄が 0 件でも記録して、失敗記事の無限リトライを避ける。
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fetched_articles (
                article_id   TEXT PRIMARY KEY,  -- 例 'n202607250151'
                url          TEXT,
                article_date TEXT,
                title        TEXT,
                n_codes      INTEGER,
                status       TEXT,              -- 'ok' | 'skipped' | 'error'
                fetched_at   TEXT
            )
            """
        )

        # 更新日時などの小さなキー・バリュー
        cur.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")


# ---------------------------------------------------------------------------
# meta（更新日時など）
# ---------------------------------------------------------------------------


def set_meta(key: str, value: str) -> None:
    with _write_lock, get_conn() as conn:
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def get_meta(key: str, default: str = "") -> str:
    with get_conn() as conn:
        row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def touch_updated_at() -> str:
    """更新日時を「今」に設定して、その文字列を返す。"""
    now = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    set_meta("updated_at", now)
    return now


# ---------------------------------------------------------------------------
# 取得済み記事の管理
# ---------------------------------------------------------------------------


def known_article_ids() -> set[str]:
    """取得済み（成功・スキップ問わず）の記事 ID 集合を返す。

    スクレイパにこれを渡すことで、同じ記事を二度取りに行かない。
    """
    with get_conn() as conn:
        rows = conn.execute("SELECT article_id FROM fetched_articles").fetchall()
    return {r["article_id"] for r in rows}


def record_article(
    article_id: str,
    url: str,
    article_date: str,
    title: str,
    n_codes: int,
    status: str = "ok",
) -> None:
    """記事の取得結果を台帳に記録する（同じ ID は上書き）。"""
    with _write_lock, get_conn() as conn:
        conn.execute(
            """
            INSERT INTO fetched_articles
                (article_id, url, article_date, title, n_codes, status, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(article_id) DO UPDATE SET
                url = excluded.url,
                article_date = excluded.article_date,
                title = excluded.title,
                n_codes = excluded.n_codes,
                status = excluded.status,
                fetched_at = excluded.fetched_at
            """,
            (
                article_id,
                url,
                article_date,
                title,
                n_codes,
                status,
                datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )


# ---------------------------------------------------------------------------
# 銘柄行の追加
# ---------------------------------------------------------------------------


def insert_rows(rows: list[dict]) -> int:
    """銘柄行をまとめて追加し、"実際に増えた件数" を返す。

    rows の各要素は article_date / code / name / title (/ sector / market) を持つ dict。
    UNIQUE(code, article_date) により重複は INSERT OR IGNORE で捨てられる。
    """
    if not rows:
        return 0
    payload = [
        (
            r["article_date"],
            r["code"],
            r["name"],
            r["title"],
            r.get("sector"),
            r.get("market"),
        )
        for r in rows
    ]
    with _write_lock, get_conn() as conn:
        before = conn.total_changes
        conn.executemany(
            """
            INSERT OR IGNORE INTO high_history
                (article_date, code, name, title, sector, market)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        return conn.total_changes - before


# ---------------------------------------------------------------------------
# 参照系クエリ
# ---------------------------------------------------------------------------


def _period_clause(period: str) -> tuple[str, list]:
    """期間ラベルから WHERE 句の断片とパラメータを組み立てる。"""
    days = PERIOD_DAYS.get(period)
    if days is None:
        return "", []
    cutoff = (datetime.now(JST).date() - timedelta(days=days)).isoformat()
    return "article_date >= ?", [cutoff]


def load_history(
    period: str = "全期間",
    code_query: str = "",
    name_query: str = "",
) -> pd.DataFrame:
    """条件に合致する履歴を DataFrame で返す（表示・CSV 出力の共通入口）。"""
    where, params = [], []

    clause, p = _period_clause(period)
    if clause:
        where.append(clause)
        params += p
    if code_query.strip():
        where.append("code LIKE ?")
        params.append(f"%{code_query.strip()}%")
    if name_query.strip():
        where.append("name LIKE ?")
        params.append(f"%{name_query.strip()}%")

    sql = "SELECT article_date, code, name, sector, market, title FROM high_history"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY article_date DESC, code ASC"

    with get_conn() as conn:
        return pd.read_sql_query(sql, conn, params=params)


def latest_article_date() -> str | None:
    """最新週（= 最も新しい記事日付）を返す。データが無ければ None。"""
    with get_conn() as conn:
        row = conn.execute("SELECT MAX(article_date) AS d FROM high_history").fetchone()
    return row["d"] if row and row["d"] else None


def load_latest_week() -> pd.DataFrame:
    """最新週の銘柄一覧（コード・銘柄・取得日）を返す。"""
    d = latest_article_date()
    if not d:
        return pd.DataFrame(columns=["article_date", "code", "name", "sector", "market", "title"])
    with get_conn() as conn:
        return pd.read_sql_query(
            "SELECT article_date, code, name, sector, market, title FROM high_history "
            "WHERE article_date = ? ORDER BY code",
            conn,
            params=[d],
        )


def ranking(period: str = "全期間", limit: int = 200) -> pd.DataFrame:
    """出現回数ランキング。銘柄名は最新の記事日付のものを採用する。"""
    clause, params = _period_clause(period)
    where = f"WHERE {clause}" if clause else ""

    sql = f"""
        SELECT
            h.code AS code,
            (SELECT x.name FROM high_history x
              WHERE x.code = h.code ORDER BY x.article_date DESC LIMIT 1) AS name,
            COUNT(*) AS count
        FROM high_history h
        {where}
        GROUP BY h.code
        ORDER BY count DESC, h.code ASC
        LIMIT ?
    """
    with get_conn() as conn:
        return pd.read_sql_query(sql, conn, params=params + [limit])


def stock_summary(period: str = "全期間") -> pd.DataFrame:
    """銘柄ごとの 初登場日 / 直近登場日 / 登場回数 を集計して返す。"""
    clause, params = _period_clause(period)
    where = f"WHERE {clause}" if clause else ""

    sql = f"""
        SELECT
            h.code AS code,
            (SELECT x.name FROM high_history x
              WHERE x.code = h.code ORDER BY x.article_date DESC LIMIT 1) AS name,
            MIN(h.article_date) AS first_seen,
            MAX(h.article_date) AS last_seen,
            COUNT(*)             AS count
        FROM high_history h
        {where}
        GROUP BY h.code
        ORDER BY count DESC, last_seen DESC
    """
    with get_conn() as conn:
        return pd.read_sql_query(sql, conn, params=params)


def stats() -> dict:
    """画面ヘッダ用のサマリ値をまとめて返す。"""
    with get_conn() as conn:
        n_articles = conn.execute(
            "SELECT COUNT(*) c FROM fetched_articles WHERE status = 'ok'"
        ).fetchone()["c"]
        n_rows = conn.execute("SELECT COUNT(*) c FROM high_history").fetchone()["c"]
        n_codes = conn.execute("SELECT COUNT(DISTINCT code) c FROM high_history").fetchone()["c"]
        rng = conn.execute(
            "SELECT MIN(article_date) a, MAX(article_date) b FROM high_history"
        ).fetchone()
    return {
        "articles": n_articles,
        "rows": n_rows,
        "codes": n_codes,
        "date_from": rng["a"],
        "date_to": rng["b"],
        "updated_at": get_meta("updated_at", "-"),
    }


# ---------------------------------------------------------------------------
# GitHub スナップショット（Streamlit Cloud での永続化）
# ---------------------------------------------------------------------------


def _github_conf() -> tuple[str, str]:
    """(token, repo) を返す。token が空ならスナップショット機能は無効。

    st.secrets は Streamlit 実行時のみ利用可能なので、失敗しても落とさない。
    """
    token, repo = "", "kaminoitte55sai-cmd/turtle-tool"
    try:
        import streamlit as st

        token = st.secrets.get("GITHUB_TOKEN", "")
        repo = st.secrets.get("GITHUB_REPO", repo)
    except Exception:
        pass
    # 環境変数でも上書きできるようにしておく（ローカル検証用）
    token = os.environ.get("GITHUB_TOKEN", token)
    repo = os.environ.get("GITHUB_REPO", repo)
    return token, repo


def push_snapshot() -> bool:
    """DB の内容を JSON にして GitHub へ書き戻す。失敗しても例外は投げない。"""
    token, repo = _github_conf()
    if not token:
        return False  # ローカル実行時は何もしない

    try:
        with get_conn() as conn:
            hist = pd.read_sql_query(
                "SELECT article_date, code, name, title, sector, market FROM high_history", conn
            ).to_dict("records")
            arts = pd.read_sql_query(
                "SELECT article_id, url, article_date, title, n_codes, status, fetched_at "
                "FROM fetched_articles",
                conn,
            ).to_dict("records")

        blob = json.dumps(
            {"high_history": hist, "fetched_articles": arts, "updated_at": get_meta("updated_at")},
            ensure_ascii=False,
        ).encode("utf-8")

        api = f"https://api.github.com/repos/{repo}/contents/{SNAPSHOT_REPO_PATH}"
        hdrs = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
        # 既存ファイルの SHA を取得（更新時に必須）
        r = requests.get(api, headers=hdrs, timeout=15)
        sha = r.json().get("sha", "") if r.status_code == 200 else ""
        payload = {
            "message": "Update 上場来高値 snapshot",
            "content": base64.b64encode(blob).decode(),
        }
        if sha:
            payload["sha"] = sha
        r2 = requests.put(api, headers=hdrs, json=payload, timeout=30)
        return r2.status_code in (200, 201)
    except Exception:
        return False


def restore_snapshot_if_empty() -> int:
    """DB が空なら GitHub のスナップショットから復元し、復元件数を返す。

    Cloud の再起動でローカル SQLite が消えた場合の自動リカバリ。
    """
    try:
        with get_conn() as conn:
            n = conn.execute("SELECT COUNT(*) c FROM high_history").fetchone()["c"]
        if n > 0:
            return 0

        token, repo = _github_conf()
        if not token:
            return 0

        api = f"https://api.github.com/repos/{repo}/contents/{SNAPSHOT_REPO_PATH}"
        hdrs = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
        r = requests.get(api, headers=hdrs, timeout=15)
        if r.status_code != 200:
            return 0

        data = json.loads(base64.b64decode(r.json()["content"]).decode("utf-8"))
        restored = insert_rows(data.get("high_history", []))

        # 記事台帳も戻す（戻さないと同じ記事を再取得してしまう）
        for a in data.get("fetched_articles", []):
            record_article(
                a["article_id"],
                a.get("url", ""),
                a.get("article_date", ""),
                a.get("title", ""),
                a.get("n_codes", 0),
                a.get("status", "ok"),
            )
        if data.get("updated_at"):
            set_meta("updated_at", data["updated_at"])
        return restored
    except Exception:
        return 0
