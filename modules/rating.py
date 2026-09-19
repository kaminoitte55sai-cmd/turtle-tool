"""アナリストレーティング（目標株価）のデータ処理。

トレーダーズ・ウェブの「注目レーティング」ページ
（https://www.traders.co.jp/premium/rating）から
証券会社・調査機関が発表した投資判断と目標株価を取得し、
現在株価からの乖離率を付けて返す。

責務はここに閉じる（Streamlit は import しない）。表示は modules/rating_ui.py。

■ ページの構造
1ページに直近3週間ぶん・数百件が載っている。1行 = 1件のレーティング発表で、
同じ銘柄に複数のシンクタンクが並ぶことも多い。列は以下の5つ。

    日付 / 銘柄名(コード/市場) / シンクタンク / レーティング / ターゲット

ターゲットの表記は2通りしかない（2026-09 時点で確認）。
    "1200→1050円"  … 目標株価を 1200 円から 1050 円へ変更
    "1200円継続"    … 1200 円のまま据え置き

レーティングの表記はシンクタンクごとに流儀が違う。
    "Buy継続" / "Neutral→Buy" / "買い継続" / "中立→買い" / "2継続" / "2→1" / "新規Neutral"
数字系（SMBC日興・大和など）は 1 が最上位で、数字が小さいほど強気。
統一した強弱の判定はせず、引き上げ / 引き下げ / 据え置き / 新規 の4分類だけを行う。

■ 株価の取得について
yfinance は環境によって動かないことがあるため（Windows でユーザー名に日本語が
含まれる場合の curl_cffi 問題など）、ここでは Yahoo Finance の chart API を
urllib で直接叩く。依存を増やさず、数百銘柄でも数十秒で終わる。
"""

from __future__ import annotations

import concurrent.futures as _cf
import json
import os
import re
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import pandas as pd

BASE_DIR      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SNAPSHOT_PATH = os.path.join(BASE_DIR, "rating_snapshot.json")

SOURCE_URL = "https://www.traders.co.jp/premium/rating"

_UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    )
}

JST = timezone(timedelta(hours=9))

# 市場区分の表記をそろえる
MARKET_MAP = {"東P": "プライム", "東S": "スタンダード", "東G": "グロース", "東": "東証"}

_ROW_RE = re.compile(
    r'<tr class="rating_row[^"]*">\s*'
    r'<td[^>]*>([^<]*)</td>\s*'
    r'<td class="stock_name[^"]*">\s*<a href="/stocks/(\d+)/">\s*([^<]+?)\s*</a><br>\s*'
    r'\(([^/]+)/([^)]+)\)\s*</td>\s*'
    r'<td[^>]*>([^<]*)</td>\s*'
    r'<td[^>]*>([^<]*)</td>\s*'
    r'<td[^>]*>([^<]*)</td>'
)


# ---------------------------------------------------------------------------
# ページ取得 & パース
# ---------------------------------------------------------------------------

def fetch_html(timeout: int = 30) -> str:
    """レーティングページの HTML を返す。"""
    req = urllib.request.Request(SOURCE_URL, headers=_UA)
    with urllib.request.urlopen(req, timeout=timeout) as f:
        raw = f.read()
    m = re.search(rb'charset=["\']?([\w-]+)', raw[:2000], re.I)
    return raw.decode(m.group(1).decode() if m else "utf-8", "replace")


def parse_target(s: str) -> tuple[float | None, float | None, bool]:
    """ターゲット欄を (目標株価, 変更前, 据え置きか) に分解する。

    "1200→1050円" → (1050.0, 1200.0, False)
    "1200円継続"   → (1200.0, None,   True)
    それ以外       → (None,   None,   False)
    """
    s = (s or "").strip().replace(",", "")
    m = re.match(r"^([\d.]+)\s*→\s*([\d.]+)\s*円", s)
    if m:
        return float(m.group(2)), float(m.group(1)), False
    m = re.match(r"^([\d.]+)\s*円\s*継続", s)
    if m:
        return float(m.group(1)), None, True
    m = re.match(r"^([\d.]+)\s*円", s)
    if m:
        return float(m.group(1)), None, False
    return None, None, False


# シンクタンク固有の略語。同じ略語が別の意味で使われるため個別に読み替える。
# ・みずほ の "UP" はアンダーパフォーム。
#   （例: 2026-09-09 ニコン「中立→UP」で目標株価を 2110→1600 円へ引き下げている）
# ・マッコーリー の "OP" はアウトパフォーム。
BROKER_ALIAS = {
    "みずほ":       {"up": "underperform"},
    "マッコーリー": {"op": "outperform"},
}


def classify_rating(s: str, broker: str | None = None) -> str:
    """レーティング欄を 引き上げ / 引き下げ / 据え置き / 新規 / 不明 に分類する。

    数字系（1 が最上位）は「2→1」で引き上げ。
    英語・日本語系は語の順位表から判定する。
    broker を渡すと BROKER_ALIAS の読み替えを適用する。
    """
    s = (s or "").strip()
    if not s:
        return "不明"
    if s.startswith("新規"):
        return "新規"
    if "継続" in s:
        return "据え置き"

    if "→" not in s:
        return "不明"
    before, after = [x.strip() for x in s.split("→", 1)]

    # 数字系: 小さいほど強気
    if re.fullmatch(r"\d", before) and re.fullmatch(r"\d", after):
        return "引き上げ" if int(after) < int(before) else (
            "引き下げ" if int(after) > int(before) else "据え置き")

    # 語句系: 強気ほど大きい値
    ORDER = {
        "sell": 0, "underperform": 1, "under": 1, "underweight": 1, "reduce": 1,
        "neutral": 2, "hold": 2, "equal": 2, "equalweight": 2, "marketperform": 2,
        "outperform": 3, "overweight": 3, "over": 3, "op": 3, "accumulate": 3, "add": 3,
        "buy": 4, "strongbuy": 5,
        "売り": 0, "弱気": 1, "中立": 2, "やや強気": 3, "買い": 4, "強気": 4,
    }
    alias = BROKER_ALIAS.get((broker or "").strip(), {})

    def rank(x: str):
        k = re.sub(r"[^\wぁ-んァ-ヶ一-龥]", "", x).lower()
        k = alias.get(k, k)
        return ORDER.get(k)
    rb, ra = rank(before), rank(after)
    if rb is None or ra is None:
        return "不明"
    return "引き上げ" if ra > rb else ("引き下げ" if ra < rb else "据え置き")


def parse_ratings(html: str, year: int | None = None) -> pd.DataFrame:
    """HTML をパースして1行=1レーティング発表の DataFrame を返す。"""
    rows = _ROW_RE.findall(html)
    if not rows:
        raise ValueError(
            "レーティング表を読み取れませんでした。"
            "ページの構造が変わった可能性があります。"
        )

    today = datetime.now(JST).date()
    year  = year or today.year

    recs = []
    for date_s, _sid, name, code, market, broker, rating, target in rows:
        date_s = date_s.strip()
        m = re.match(r"^(\d{1,2})/(\d{1,2})$", date_s)
        if m:
            mm, dd = int(m.group(1)), int(m.group(2))
            y = year
            # 年末年始をまたぐ場合の補正（未来日付になったら前年とみなす）
            try:
                d = datetime(y, mm, dd).date()
                if (d - today).days > 30:
                    d = datetime(y - 1, mm, dd).date()
            except ValueError:
                d = None
        else:
            d = None

        tgt, prev, cont = parse_target(target)
        recs.append({
            "日付":         d,
            "コード":       code.strip(),
            "銘柄名":       name.strip(),
            "市場":         MARKET_MAP.get(market.strip(), market.strip()),
            "シンクタンク": broker.strip(),
            "レーティング": rating.strip(),
            "判定":         classify_rating(rating, broker),
            "目標株価":     tgt,
            "変更前":       prev,
            "据え置き":     cont,
            # 4桁コードのみ普通株。5桁は種類株・優先出資証券で、
            # 目標株価は普通株に対するものなので株価と比較できない
            # （例: ソフトバンク 9434 は 248円 だが 94345/94346 は数千円で建値が違う）。
            # 同じレーティングが普通株の行としても必ず載っているため除外して問題ない。
            "普通株":       bool(re.fullmatch(r"\d{4}", code.strip())),
        })

    df = pd.DataFrame(recs)
    df["日付"] = pd.to_datetime(df["日付"], errors="coerce")
    return df.sort_values("日付", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# 株価取得（Yahoo Finance chart API を直叩き）
# ---------------------------------------------------------------------------

def _fetch_one_price(code: str) -> tuple[str, float | None]:
    for attempt in range(3):
        host = "query1" if attempt % 2 == 0 else "query2"
        url = (f"https://{host}.finance.yahoo.com/v8/finance/chart/"
               f"{code}.T?range=5d&interval=1d")
        try:
            req = urllib.request.Request(url, headers=_UA)
            with urllib.request.urlopen(req, timeout=20) as f:
                d = json.loads(f.read())
            res = d["chart"]["result"][0]
            closes = [c for c in res["indicators"]["quote"][0]["close"] if c is not None]
            if closes:
                return code, float(closes[-1])
            meta = res.get("meta") or {}
            p = meta.get("regularMarketPrice")
            return code, (float(p) if p else None)
        except Exception:
            time.sleep(0.8)
    return code, None


def fetch_prices(codes, max_workers: int = 6) -> dict[str, float]:
    """4桁コードのリストから {コード: 直近終値} を返す。

    5桁コード（種類株）は Yahoo 上では建値の違う別証券に解決されてしまうため、
    問い合わせ自体を行わない。
    """
    codes = sorted({str(c).strip() for c in codes
                    if re.fullmatch(r"\d{4}", str(c).strip())})
    out: dict[str, float] = {}
    if not codes:
        return out
    with _cf.ThreadPoolExecutor(max_workers) as ex:
        for code, px in ex.map(_fetch_one_price, codes):
            if px is not None:
                out[code] = px
    return out


# ---------------------------------------------------------------------------
# 乖離率テーブルの組み立て
# ---------------------------------------------------------------------------

def add_upside(df: pd.DataFrame, prices: dict[str, float]) -> pd.DataFrame:
    """現在株価と乖離率（目標株価 ÷ 現在株価 − 1）を付ける。"""
    df = df.copy()
    df["現在株価"] = df["コード"].map(prices)
    ok = df["現在株価"].notna() & df["目標株価"].notna() & (df["現在株価"] > 0)
    if "普通株" in df.columns:
        ok &= df["普通株"].fillna(True).astype(bool)
    df["乖離率(%)"] = None
    df.loc[ok, "乖離率(%)"] = (
        df.loc[ok, "目標株価"] / df.loc[ok, "現在株価"] - 1
    ) * 100
    df["乖離率(%)"] = pd.to_numeric(df["乖離率(%)"], errors="coerce").round(2)
    # 目標株価の上げ幅（据え置きは対象外）
    df["目標変化率(%)"] = pd.to_numeric(
        (df["目標株価"] / df["変更前"] - 1) * 100, errors="coerce"
    ).round(2)
    return df


def aggregate_by_stock(df: pd.DataFrame, how: str = "max") -> pd.DataFrame:
    """銘柄ごとに集計する。同一シンクタンクは最新の1件だけを使う。

    how: "max"（最も強気な目標）/ "median"（中央値）/ "mean"（平均）
    """
    if df.empty:
        return df
    d = df.dropna(subset=["目標株価"]).copy()
    if "普通株" in d.columns:
        d = d[d["普通株"].fillna(True).astype(bool)]
    if d.empty:
        return d
    # 同じ銘柄×シンクタンクは最新の発表のみ残す
    d = (d.sort_values("日付")
           .drop_duplicates(subset=["コード", "シンクタンク"], keep="last"))

    # 「シンクタンク 更新日」の文字列を作り、新しい順に並べて1セルにまとめる
    def _label(r) -> str:
        b = str(r["シンクタンク"])
        dt = r["日付"]
        return f"{b} {dt:%m/%d}" if pd.notna(dt) else b

    d["_bd"] = d.apply(_label, axis=1)
    d = d.sort_values("日付", ascending=False)

    agg_fn = {"max": "max", "median": "median", "mean": "mean"}.get(how, "max")
    g = d.groupby("コード", as_index=False).agg(
        銘柄名   = ("銘柄名", "first"),
        市場     = ("市場", "first"),
        目標株価 = ("目標株価", agg_fn),
        最高目標 = ("目標株価", "max"),
        最低目標 = ("目標株価", "min"),
        社数     = ("シンクタンク", "nunique"),
        直近日付 = ("日付", "max"),
        現在株価 = ("現在株価", "first"),
        引き上げ = ("判定", lambda s: int((s == "引き上げ").sum())),
        引き下げ = ("判定", lambda s: int((s == "引き下げ").sum())),
        # 新しい順に「シンクタンク 更新日」を並べる
        各社の更新日 = ("_bd", lambda s: "、".join(s)),
    )
    ok = g["現在株価"].notna() & (g["現在株価"] > 0)
    g["乖離率(%)"] = None
    g.loc[ok, "乖離率(%)"] = (g.loc[ok, "目標株価"] / g.loc[ok, "現在株価"] - 1) * 100
    g["乖離率(%)"] = pd.to_numeric(g["乖離率(%)"], errors="coerce").round(2)
    return g.sort_values("乖離率(%)", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# スナップショット（取得できない環境向けのフォールバック）
# ---------------------------------------------------------------------------

def save_snapshot(df: pd.DataFrame, path: str = SNAPSHOT_PATH) -> None:
    d = df.copy()
    d["日付"] = d["日付"].dt.strftime("%Y-%m-%d")
    payload = {
        "updated_at": datetime.now(JST).isoformat(timespec="seconds"),
        "source":     SOURCE_URL,
        "rows":       d.where(pd.notna(d), None).to_dict(orient="records"),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)


def load_snapshot(path: str = SNAPSHOT_PATH) -> tuple[pd.DataFrame, str | None]:
    """スナップショットを (DataFrame, 更新日時) で返す。無ければ空。"""
    if not os.path.exists(path):
        return pd.DataFrame(), None
    try:
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
        df = pd.DataFrame(payload.get("rows") or [])
        if not df.empty:
            df["日付"] = pd.to_datetime(df["日付"], errors="coerce")
        return df, payload.get("updated_at")
    except Exception:
        return pd.DataFrame(), None


def fetch_ratings() -> pd.DataFrame:
    """取得 → パースまでをまとめて行う。"""
    return parse_ratings(fetch_html())
