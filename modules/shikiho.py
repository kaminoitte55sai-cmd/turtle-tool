"""四季報オンライン「厳選注目株」の配信後パフォーマンス分析（データ処理のみ）。

責務はここに閉じる:
  - 銘柄リスト（CSV）の読み込み
  - 配信日 -> 現在 の株価騰落率の計算
  - ベンチマーク（日経平均）との比較・集計

Streamlit は import しない。表示は modules/shikiho_ui.py が担当する。

■ データの出所について
四季報オンラインの「厳選注目株一覧」はベーシック・プレミアムプラン限定で、
未ログインではAPIも一覧ページも銘柄を返さない（記事の公開リード文に
コードが載るのは実測で 20 件中 2 件のみ）。したがって自動取得はできない。
このモジュールが読む CSV は、購読者本人が閲覧した一覧を書き出したもの。

■ 株価の前提
記事は 06:00 配信（寄り付き前）なので、配信日の終値を取得価格とみなす。
配信日が非営業日なら直後の営業日の終値を使う。現在値は取得できる最新終値。
"""

from __future__ import annotations

import os

# utils を先に import して SSL 証明書のパスを整える。
# Windows でユーザー名に日本語が含まれると curl_cffi が証明書を読めず、
# yfinance が SSLError になるため、yfinance より前に読み込む必要がある。
import utils  # noqa: F401

import pandas as pd
import yfinance as yf

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "shikiho_selection.csv")

# ベンチマーク（日経平均）。同期間の騰落率と比較して超過リターンを出す。
BENCHMARK = "^N225"
BENCHMARK_NAME = "日経平均"


def load_selection() -> pd.DataFrame:
    """銘柄リストを読み込む。ファイルが無ければ空の DataFrame を返す。"""
    cols = ["code", "name", "published", "title"]
    if not os.path.exists(CSV_PATH):
        return pd.DataFrame(columns=cols)
    df = pd.read_csv(CSV_PATH, dtype={"code": str})
    df["published"] = pd.to_datetime(df["published"])
    return df.sort_values("published", ascending=False, ignore_index=True)


def _download_closes(tickers: list[str], start: str, chunk: int = 40) -> pd.DataFrame:
    """終値をまとめて取得する。銘柄数が多いので分割して負荷を抑える。"""
    frames = []
    for i in range(0, len(tickers), chunk):
        part = tickers[i : i + chunk]
        try:
            data = yf.download(part, start=start, progress=False, auto_adjust=True)
        except Exception:
            continue
        if data is None or data.empty:
            continue
        if isinstance(data.columns, pd.MultiIndex):
            close = data["Close"]
        else:  # 1銘柄だけのとき
            close = data[["Close"]].copy()
            close.columns = part
        frames.append(close)
    if not frames:
        return pd.DataFrame()
    px = pd.concat(frames, axis=1)
    px.index = pd.to_datetime(px.index).tz_localize(None)
    return px.sort_index()


def analyze(df: pd.DataFrame, progress_cb=None) -> tuple[pd.DataFrame, dict]:
    """騰落率を計算した DataFrame と、集計値の dict を返す。

    progress_cb(current, total, message) を渡すと進捗を通知する（UI 非依存）。
    """
    if df.empty:
        return df.assign(entry=None, now=None, ret=None), {}

    tickers = [f"{c}.T" for c in df["code"]]
    start = (df["published"].min() - pd.Timedelta(days=10)).strftime("%Y-%m-%d")

    if progress_cb:
        progress_cb(0, 2, f"{len(tickers)} 銘柄の株価を取得中…")
    px = _download_closes(tickers, start)

    if progress_cb:
        progress_cb(1, 2, "日経平均を取得中…")
    bench = _download_closes([BENCHMARK], start)

    rows = []
    for _, r in df.iterrows():
        t = f"{r['code']}.T"
        rec = {**r.to_dict(), "entry_date": None, "entry": None, "now": None, "ret": None}

        if t in px.columns:
            s = px[t].dropna()
            after = s[s.index >= r["published"].normalize()] if not s.empty else s
            if not after.empty:
                entry = float(after.iloc[0])
                now = float(s.iloc[-1])
                rec.update(
                    entry_date=after.index[0].date(),
                    entry=entry,
                    now=now,
                    ret=(now / entry - 1) * 100 if entry else None,
                )

        # 同期間の日経平均騰落率（超過リターンの算出に使う）
        rec["bench_ret"] = None
        if not bench.empty:
            b = bench.iloc[:, 0].dropna()
            ba = b[b.index >= r["published"].normalize()]
            if not ba.empty and float(ba.iloc[0]):
                rec["bench_ret"] = (float(b.iloc[-1]) / float(ba.iloc[0]) - 1) * 100

        if rec["ret"] is not None and rec["bench_ret"] is not None:
            rec["excess"] = rec["ret"] - rec["bench_ret"]
        else:
            rec["excess"] = None
        rows.append(rec)

    res = pd.DataFrame(rows)
    if progress_cb:
        progress_cb(2, 2, "完了")
    return res, summarize(res)


def summarize(res: pd.DataFrame) -> dict:
    """集計値をまとめて返す。計算できた銘柄だけを対象にする。"""
    ok = res.dropna(subset=["ret"])
    if ok.empty:
        return {}
    ex = ok.dropna(subset=["excess"])
    return {
        "n": len(ok),
        "n_total": len(res),
        "mean": ok["ret"].mean(),
        "median": ok["ret"].median(),
        "win_rate": (ok["ret"] > 0).mean() * 100,
        "max": ok["ret"].max(),
        "max_name": ok.loc[ok["ret"].idxmax(), "name"],
        "min": ok["ret"].min(),
        "min_name": ok.loc[ok["ret"].idxmin(), "name"],
        "bench_mean": ex["bench_ret"].mean() if not ex.empty else None,
        "excess_mean": ex["excess"].mean() if not ex.empty else None,
        "excess_win": (ex["excess"] > 0).mean() * 100 if not ex.empty else None,
        "date_from": res["published"].min(),
        "date_to": res["published"].max(),
    }


def by_month(res: pd.DataFrame) -> pd.DataFrame:
    """配信月ごとの平均騰落率と勝率。相場環境の影響を見るために使う。"""
    ok = res.dropna(subset=["ret"]).copy()
    if ok.empty:
        return pd.DataFrame(columns=["月", "件数", "平均騰落率", "勝率"])
    ok["月"] = ok["published"].dt.to_period("M").astype(str)
    g = ok.groupby("月").agg(
        件数=("ret", "size"),
        平均騰落率=("ret", "mean"),
        勝率=("ret", lambda s: (s > 0).mean() * 100),
    )
    return g.reset_index().sort_values("月", ascending=False, ignore_index=True)
