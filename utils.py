"""
utils.py
========
タートルズ資金管理ツールの計算・データ取得ロジック

このモジュールは main.py および将来の Streamlit アプリから
インポートして使うことを想定している。
CLI に依存するコードは一切含まない。
"""

from __future__ import annotations

import certifi
import os
import shutil
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Windows 環境でユーザー名に日本語が含まれる場合、yfinance が内部で使う
# curl_cffi が SSL 証明書ファイルのパスを読めず SSLError になる。
# 対策: ASCII パスにある ProgramData へ証明書をコピーし、
#       curl_cffi のロード（= yfinance の import）より前に環境変数をセットする。
# ---------------------------------------------------------------------------
_CERT_DST = r"C:\ProgramData\yfinance_ssl\cacert.pem"

if not os.path.exists(_CERT_DST):
    os.makedirs(os.path.dirname(_CERT_DST), exist_ok=True)
    shutil.copy2(certifi.where(), _CERT_DST)

# この2行は yfinance import より必ず前に置くこと
os.environ.setdefault("CURL_CA_BUNDLE", _CERT_DST)
os.environ.setdefault("SSL_CERT_FILE",  _CERT_DST)

import pandas as pd
import yfinance as yf


# ===========================================================================
# データクラス（画面表示に必要な情報をまとめて持ち運ぶ）
# ===========================================================================

@dataclass
class MarketData:
    """
    yfinance から取得した市場データ。
    計算関数に渡す前の「生の入力」として使う。
    """
    ticker: str       # 銘柄コード（例: "7203.T"）
    close: float      # 最新終値
    atr: float        # 20日ATR
    currency: str     # 通貨コード（"JPY" or "USD"）


@dataclass
class PositionResult:
    """
    ポジションサイズ計算の結果をすべて格納するデータクラス。
    main.py や Streamlit 側はこのオブジェクトを受け取って表示するだけでよい。
    """
    # --- 入力情報 ---
    ticker: str
    close: float
    atr: float
    capital: float
    risk_pct: float

    # --- 計算結果（株価はすべて銘柄の現地通貨建て） ---
    risk_amount: float = 0.0       # リスク金額（現地通貨）
    unit_shares: int   = 0         # 1ユニットの株数
    stop_loss: float   = 0.0       # 損切り価格（終値 - 2ATR）
    unit_loss: float   = 0.0       # 1ユニット当たりの最大損失額（現地通貨）
    max_investment: float = 0.0    # 最大4ユニット時の合計投資額（現地通貨）
    is_japan: bool     = False     # 日本株フラグ

    # --- 円建て換算値（米株のみ有効、日本株は capital_jpy と同値） ---
    capital_jpy: float    = 0.0    # ユーザーが入力した元の円建て資金
    exchange_rate: float  = 1.0    # 取得した USD/JPY レート（日本株は 1.0）
    risk_amount_jpy: float = 0.0   # リスク金額の円換算
    unit_loss_jpy: float   = 0.0   # 1ユニット損失額の円換算
    max_investment_jpy: float = 0.0  # 4ユニット合計投資額の円換算

    # ピラミッディング用：各ユニットのエントリー情報リスト
    # 例: [{"unit": 1, "entry_price": 2650, "stop_loss": 2453, ...}, ...]
    pyramid_entries: list[dict] = field(default_factory=list)


# ===========================================================================
# ユーティリティ関数：銘柄の種類判定
# ===========================================================================

def is_japan_stock(ticker: str) -> bool:
    """
    日本株かどうかを判定する。
    yfinance の日本株ティッカーは末尾が '.T'（例: 7203.T）。

    Parameters
    ----------
    ticker : 銘柄コード文字列

    Returns
    -------
    bool : 日本株なら True
    """
    return ticker.upper().endswith(".T")


def get_currency(ticker: str) -> str:
    """
    銘柄コードから通貨を返す。
    現在は日本株（JPY）とそれ以外（USD）のみ対応。

    Parameters
    ----------
    ticker : 銘柄コード文字列

    Returns
    -------
    str : "JPY" or "USD"
    """
    return "JPY" if is_japan_stock(ticker) else "USD"


# ===========================================================================
# データ取得・ATR計算
# ===========================================================================

def fetch_ohlcv(ticker: str, period: str = "6mo") -> pd.DataFrame:
    """
    yfinance から日足 OHLCV データを取得して返す。

    取得後に MultiIndex（複数銘柄取得時に発生）をフラット化する。

    Parameters
    ----------
    ticker : 銘柄コード
    period : yfinance の期間指定文字列（デフォルト "6mo" = 6ヶ月）

    Returns
    -------
    pd.DataFrame : 日足 OHLCV（列: Open, High, Low, Close, Volume）

    Raises
    ------
    ValueError : データが空、または取得に失敗した場合
    """
    df = yf.download(ticker, period=period, auto_adjust=True, progress=False)

    if df is None or df.empty:
        raise ValueError(
            f"銘柄 '{ticker}' のデータを取得できませんでした。\n"
            "ティッカーコードを確認してください（例: 7203.T / AAPL）。"
        )

    # yfinance が複数銘柄を返す場合に MultiIndex になることがある → フラット化
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    return df


def calculate_atr(df: pd.DataFrame, period: int = 20) -> float:
    """
    ATR（Average True Range）を計算する。

    True Range（TR）は以下3つの最大値：
        1. 当日高値 - 当日安値
        2. |当日高値 - 前日終値|
        3. |当日安値 - 前日終値|

    ATR = 直近 period 本の TR の単純平均

    Parameters
    ----------
    df     : OHLCV の DataFrame（yfinance から取得済みのもの）
    period : ATR の計算期間（デフォルト 20 日）

    Returns
    -------
    float : ATR 値（小数点以下2桁に丸め）

    Raises
    ------
    ValueError : データ行数が period より少ない場合
    """
    if len(df) < period + 1:
        raise ValueError(
            f"ATR 計算に必要なデータが不足しています。\n"
            f"  必要: {period + 1} 行以上 / 取得済み: {len(df)} 行"
        )

    high       = df["High"]
    low        = df["Low"]
    prev_close = df["Close"].shift(1)  # 前日終値（1行ずらし）

    # 3つの候補を横並びに並べ、行ごとの最大値 = True Range
    tr = pd.concat(
        [
            high - low,                   # 当日のレンジ
            (high - prev_close).abs(),    # ギャップアップ対応
            (low  - prev_close).abs(),    # ギャップダウン対応
        ],
        axis=1,
    ).max(axis=1)

    # 直近 period 本の平均（Wilder 平均より単純だが、初心者に分かりやすい）
    atr_value = float(tr.iloc[-period:].mean())
    return round(atr_value, 2)


def fetch_market_data(ticker: str, atr_period: int = 20) -> MarketData:
    """
    銘柄コードを受け取り、終値と ATR を取得して MarketData を返す。
    データ取得・ATR 計算の両方を一括で行うラッパー関数。

    Parameters
    ----------
    ticker     : 銘柄コード（例: "7203.T", "AAPL"）
    atr_period : ATR 計算期間（デフォルト 20 日）

    Returns
    -------
    MarketData

    Raises
    ------
    ValueError : データ取得・計算に失敗した場合（メッセージ付き）
    """
    df    = fetch_ohlcv(ticker, period="6mo")
    close = float(df["Close"].iloc[-1])  # 最新終値
    atr   = calculate_atr(df, period=atr_period)

    return MarketData(
        ticker=ticker,
        close=close,
        atr=atr,
        currency=get_currency(ticker),
    )


# ===========================================================================
# 為替レート取得
# ===========================================================================

def fetch_exchange_rate(pair: str = "USDJPY=X") -> float:
    """
    yfinance で為替レートを取得する。

    Parameters
    ----------
    pair : yfinance の為替ティッカー（デフォルト "USDJPY=X" = 1USD 何円か）

    Returns
    -------
    float : 為替レート（例: 150.25）

    Raises
    ------
    ValueError : データ取得失敗時
    """
    df = yf.download(pair, period="5d", auto_adjust=True, progress=False)

    if df is None or df.empty:
        raise ValueError(f"為替データ '{pair}' を取得できませんでした。")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    return round(float(df["Close"].iloc[-1]), 4)


# ===========================================================================
# ポジションサイズ計算
# ===========================================================================

def round_to_lot(shares: float, japan: bool) -> int:
    """
    株数を売買単位に合わせて切り捨て丸めする。
    - 日本株 : 100株単位（最低 100 株）
    - 米国株 : 1株単位（最低 1 株）

    Parameters
    ----------
    shares : 計算で求めた生の株数（float）
    japan  : 日本株なら True

    Returns
    -------
    int : 単位丸め後の株数
    """
    if japan:
        # 100 で割って切り捨て → 100 倍、最低 100 株を保証
        return max(100, int(shares // 100) * 100)
    # 米国株は 1 株単位、最低 1 株
    return max(1, int(shares))


def calculate_pyramid_entries(
    entry_price: float,
    atr: float,
    unit_shares: int,
    num_units: int = 4,
    step_atr: float = 0.5,
) -> list[dict]:
    """
    ピラミッディング（段階的なポジション追加）の計画を計算する。

    タートルズのルール：
        ブレイクアウト後、0.5ATR 上昇するたびに 1 ユニット追加（最大 4 ユニット）
        各エントリーの損切りは「そのエントリー価格 - 2ATR」

    Parameters
    ----------
    entry_price : 最初のエントリー価格（通常は現在の終値）
    atr         : ATR 値
    unit_shares : 1ユニットの株数
    num_units   : 最大ユニット数（デフォルト 4）
    step_atr    : 追加エントリー間隔（ATR の倍数、デフォルト 0.5）

    Returns
    -------
    list[dict] : 各ユニットの情報
        - unit        : ユニット番号（1〜4）
        - entry_price : エントリー価格
        - stop_loss   : 損切り価格
        - shares      : 株数
        - investment  : 投資額（エントリー価格 × 株数）
    """
    entries = []
    for i in range(num_units):
        price = entry_price + (i * step_atr * atr)   # 0.5ATR ずつ上にずれる
        stop  = price - 2 * atr                        # 損切りは常に -2ATR

        entries.append({
            "unit":        i + 1,
            "entry_price": round(price, 2),
            "stop_loss":   round(stop, 2),
            "shares":      unit_shares,
            "investment":  round(price * unit_shares, 0),
        })

    return entries


def compute_position(
    market: MarketData,
    capital_jpy: float,
    risk_pct: float = 0.01,
    exchange_rate: float | None = None,
) -> PositionResult:
    """
    タートルズのルールに従い、ポジションサイズを計算する。
    投資資金は常に円建てで受け取り、米株の場合は為替レートで換算してから計算する。

    計算式（タートルズ原則）:
        [日本株]
            リスク金額（円） = 資金（円） × リスク割合
            1ユニット株数    = リスク金額（円） ÷ ATR（円）
        [米国株]
            リスク金額（USD）= 資金（円） ÷ レート × リスク割合
            1ユニット株数    = リスク金額（USD） ÷ ATR（USD）
        損切り価格 = 終値 - 2 × ATR  （現地通貨）
        損失額     = 株数 × 2 × ATR  （現地通貨）

    Parameters
    ----------
    market        : MarketData（終値・ATR を含む）
    capital_jpy   : 投資資金（円建て、統一）
    risk_pct      : 1ユニット当たりのリスク割合（デフォルト 1% = 0.01）
    exchange_rate : USD/JPY レート。None の場合は自動取得（米株のみ使用）

    Returns
    -------
    PositionResult : 全計算結果
    """
    japan = is_japan_stock(market.ticker)

    # --- 為替レートの決定 ---
    if japan:
        rate = 1.0  # 日本株は換算不要
    else:
        rate = exchange_rate if exchange_rate is not None else fetch_exchange_rate()

    # --- 現地通貨建ての資金・リスク金額を算出 ---
    # 日本株: そのまま円  /  米国株: 円 ÷ レート = ドル
    capital_local = capital_jpy if japan else capital_jpy / rate
    risk_amount   = capital_local * risk_pct          # 現地通貨建てリスク金額

    # --- コアの計算（現地通貨建て） ---
    raw_shares  = risk_amount / market.atr                  # 生の株数（float）
    unit_shares = round_to_lot(raw_shares, japan=japan)     # 単位丸め
    stop_loss   = round(market.close - 2 * market.atr, 2)  # 損切り価格
    unit_loss   = round(unit_shares * 2 * market.atr, 2)   # 1ユニット損失額
    max_invest  = round(market.close * unit_shares * 4, 2)  # 4ユニット合計投資額

    # --- 円換算（米株のみ実質的に変わる） ---
    risk_amount_jpy    = round(risk_amount  * rate, 0)
    unit_loss_jpy      = round(unit_loss    * rate, 0)
    max_investment_jpy = round(max_invest   * rate, 0)

    # ピラミッディング計画
    pyramid = calculate_pyramid_entries(
        entry_price=market.close,
        atr=market.atr,
        unit_shares=unit_shares,
    )

    return PositionResult(
        ticker=market.ticker,
        close=market.close,
        atr=market.atr,
        capital=capital_local,      # 現地通貨建て資金
        risk_pct=risk_pct,
        risk_amount=risk_amount,
        unit_shares=unit_shares,
        stop_loss=stop_loss,
        unit_loss=unit_loss,
        max_investment=max_invest,
        is_japan=japan,
        capital_jpy=capital_jpy,
        exchange_rate=rate,
        risk_amount_jpy=risk_amount_jpy,
        unit_loss_jpy=unit_loss_jpy,
        max_investment_jpy=max_investment_jpy,
        pyramid_entries=pyramid,
    )
