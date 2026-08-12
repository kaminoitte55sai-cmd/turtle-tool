"""高配当株ポートフォリオ候補リストのデータ処理。

某哲也氏のブログ（https://boutetsuya.livedoor.blog/）の銘柄記事から抽出した
「分類」「判定」と、記事執筆時点の指標を収めた CSV を読み、
現在の株価指標を添えて返す。

責務はここに閉じる（Streamlit は import しない）。表示は modules/kohaito_ui.py。

■ 指標の出どころに注意
・blog_* 列 … 記事執筆時点の値。PER は会社予想ベースであることが多い。
・現在の per / pbr / yield … yfinance の実績ベース。
両者は基準が違うので単純比較はできない。列名で区別できるようにしてある。

■ 株価の更新について
yfinance の .info は 1 銘柄ずつしか取れず 341 銘柄で数分かかるため、
CSV に取得済みの値を持たせている。「更新」は終値のみ一括取得し、
価格変化率から PER・PBR・利回りを機械的に補正する
（EPS・BPS・配当が据え置きという前提の近似）。
"""

from __future__ import annotations

import os

# yfinance より先に import して SSL 証明書のパスを整える
# （Windows でユーザー名に日本語が含まれると curl_cffi が証明書を読めない）
import utils  # noqa: F401

import pandas as pd
import yfinance as yf

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "boutetsuya_stocks.csv")

SOURCE_URL = "https://boutetsuya.livedoor.blog/"

# 表示順を安定させるための並び（ブログ内での位置づけが重い順）
CATEGORY_ORDER = ["主力", "準主力", "監視銘柄", "分散枠"]
JUDGEMENT_ORDER = ["割安", "やや割安", "妥当", "やや割高", "割高"]


REQUIRED_COLS = {"code", "name"}


def read_csv(source) -> pd.DataFrame:
    """CSV を読み込んで整える。パスでもアップロードされたファイルでもよい。"""
    df = pd.read_csv(source, dtype={"code": str})
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(
            f"必要な列が足りません: {', '.join(sorted(missing))}"
            f"（読み込んだ列: {', '.join(df.columns)}）"
        )
    df["code"] = df["code"].astype(str).str.strip()
    if "article_date" in df.columns:
        df["article_date"] = pd.to_datetime(df["article_date"], errors="coerce")
    # 以降の処理で存在を前提にしている列を補う
    for c in ("category", "judgement", "comment", "price", "per", "pbr", "yield"):
        if c not in df.columns:
            df[c] = None
    return df


def load() -> pd.DataFrame:
    """同梱の銘柄リストを読み込む。無ければ空の DataFrame を返す。"""
    if not os.path.exists(CSV_PATH):
        return pd.DataFrame()
    return read_csv(CSV_PATH)


def refresh_prices(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """最新終値を一括取得し、価格変化に応じて PER・PBR・利回りを補正する。

    戻り値は (更新後の DataFrame, 更新できた銘柄数)。
    """
    out = df.copy()
    if out.empty:
        return out, 0

    tickers = [f"{c}.T" for c in out["code"]]
    try:
        data = yf.download(
            tickers, period="5d", progress=False, auto_adjust=True, threads=True
        )
    except Exception:
        return out, 0
    if data is None or data.empty:
        return out, 0

    close = data["Close"] if isinstance(data.columns, pd.MultiIndex) else data[["Close"]]
    latest = close.ffill().iloc[-1]

    n = 0
    for i, r in out.iterrows():
        t = f"{r['code']}.T"
        if t not in latest.index or pd.isna(latest[t]):
            continue
        new_price = float(latest[t])
        old_price = r.get("price")
        out.at[i, "price"] = new_price
        n += 1
        # 価格が変わった分だけ指標をスライドさせる（EPS/BPS/配当は据え置き前提）
        if old_price and not pd.isna(old_price) and old_price > 0:
            ratio = new_price / float(old_price)
            for col, direction in (("per", 1), ("pbr", 1), ("yield", -1)):
                v = r.get(col)
                if v is not None and not pd.isna(v):
                    out.at[i, col] = float(v) * (ratio if direction == 1 else 1 / ratio)

            # PERが動いたので「1年平均との差」も引き直す。
            # PER水準（パーセンタイル）は過去系列がないと再計算できないため、
            # データ作成時点の値のまま据え置く。
            avg1 = r.get("per_avg_1y")
            new_per = out.at[i, "per"]
            if (
                avg1 is not None and not pd.isna(avg1) and avg1 > 0
                and new_per is not None and not pd.isna(new_per)
            ):
                out.at[i, "per_vs_1y"] = (float(new_per) / float(avg1) - 1) * 100
    return out, n


def summarize(df: pd.DataFrame) -> dict:
    """画面ヘッダ用の集計値。"""
    if df.empty:
        return {}
    have_yield = df["yield"].notna() if "yield" in df else pd.Series(dtype=bool)
    return {
        "n": len(df),
        "n_price": int(df["price"].notna().sum()) if "price" in df else 0,
        "mean_yield": df.loc[have_yield, "yield"].mean() if have_yield.any() else None,
        "n_high_yield": int((df["yield"] >= 4).sum()) if "yield" in df else 0,
        "categories": df["category"].nunique() if "category" in df else 0,
        "date_from": df["article_date"].min() if "article_date" in df else None,
        "date_to": df["article_date"].max() if "article_date" in df else None,
    }


def sort_key(series: pd.Series, order: list[str]) -> pd.Series:
    """決められた順序で並べるための序数を返す。順序外は最後に回す。"""
    rank = {v: i for i, v in enumerate(order)}
    return series.map(lambda x: rank.get(x, len(order)))
