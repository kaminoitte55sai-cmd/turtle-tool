"""
turtle_position.py
==================
タートルズの資金管理ルールに基づいたポジションサイズ計算ツール

使い方:
    python turtle_position.py
    python turtle_position.py --ticker 7203.T --capital 1000000 --risk 0.01

将来的に Streamlit 化しやすいよう、ロジックを関数に分離している。
"""

import argparse
import sys
from dataclasses import dataclass, field
from typing import Optional

import yfinance as yf
import pandas as pd


# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class MarketData:
    """取得した市場データを保持する"""
    ticker: str
    close: float          # 最新終値
    atr: float            # 20日ATR
    currency: str = "JPY"


@dataclass
class PositionResult:
    """計算結果を保持する"""
    ticker: str
    close: float
    atr: float
    capital: float
    risk_pct: float

    # 計算結果
    risk_amount: float = 0.0        # リスク金額
    unit_shares: int = 0            # 1ユニットの株数
    stop_loss: float = 0.0          # 損切り価格（エントリー - 2ATR）
    unit_loss: float = 0.0          # 1ユニットの最大損失額
    is_japan: bool = False          # 日本株フラグ（100株単位丸め用）

    # ピラミッディング（0.5ATR ごとの追加エントリー価格）
    pyramid_entries: list = field(default_factory=list)

    # 最大4ユニット分の合計投資額
    max_investment: float = 0.0


# ---------------------------------------------------------------------------
# 市場データ取得
# ---------------------------------------------------------------------------

def is_japan_stock(ticker: str) -> bool:
    """日本株かどうかを判定する（末尾が .T）"""
    return ticker.upper().endswith(".T")


def fetch_market_data(ticker: str, atr_period: int = 20) -> MarketData:
    """
    yfinance を使って終値と ATR を取得する。

    Parameters
    ----------
    ticker     : 銘柄コード（例: "7203.T", "AAPL"）
    atr_period : ATR の計算期間（デフォルト 20 日）

    Returns
    -------
    MarketData

    Raises
    ------
    ValueError : データ取得失敗・データ不足の場合
    """
    # ATR 計算に必要な最低限のデータを取得（期間 + バッファ）
    required_rows = atr_period + 5
    raw = yf.download(ticker, period="60d", auto_adjust=True, progress=False)

    if raw is None or raw.empty:
        raise ValueError(f"銘柄 '{ticker}' のデータを取得できませんでした。ティッカーを確認してください。")

    # MultiIndex の場合はフラット化
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    if len(raw) < required_rows:
        raise ValueError(
            f"データが不足しています（取得行数: {len(raw)}, 必要行数: {required_rows}）。"
        )

    close = float(raw["Close"].iloc[-1])
    atr = calculate_atr(raw, period=atr_period)
    currency = "JPY" if is_japan_stock(ticker) else "USD"

    return MarketData(ticker=ticker, close=close, atr=atr, currency=currency)


def calculate_atr(df: pd.DataFrame, period: int = 20) -> float:
    """
    True Range の平均（ATR）を計算する。

    True Range = max(高値-安値, |高値-前日終値|, |安値-前日終値|)

    Parameters
    ----------
    df     : OHLCV の DataFrame（yfinance から取得）
    period : 平均化する日数

    Returns
    -------
    float : ATR 値
    """
    high = df["High"]
    low  = df["Low"]
    prev_close = df["Close"].shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)

    # 直近 period 日の単純平均（Wilder 平均も選択肢だが、シンプルさ優先）
    atr = float(tr.iloc[-period:].mean())
    return round(atr, 2)


# ---------------------------------------------------------------------------
# ポジションサイズ計算
# ---------------------------------------------------------------------------

def round_to_lot(shares: float, is_japan: bool) -> int:
    """
    株数を売買単位に丸める。
    - 日本株: 100株単位（切り捨て）
    - 米国株: 1株単位（切り捨て）
    """
    if is_japan:
        return max(100, int(shares // 100) * 100)
    return max(1, int(shares))


def calculate_pyramid_entries(
    entry_price: float,
    atr: float,
    unit_shares: int,
    num_units: int = 4,
    step_atr: float = 0.5,
) -> list[dict]:
    """
    ピラミッディングのエントリー価格を計算する。

    タートルズのルールでは 0.5ATR ごとにポジションを追加する。

    Parameters
    ----------
    entry_price  : 最初のエントリー価格（終値）
    atr          : ATR
    unit_shares  : 1ユニットの株数
    num_units    : 最大ユニット数（デフォルト 4）
    step_atr     : 追加間隔（デフォルト 0.5ATR）

    Returns
    -------
    list[dict] : 各ユニットのエントリー情報
    """
    entries = []
    for i in range(num_units):
        price = entry_price + (i * step_atr * atr)
        stop  = price - 2 * atr
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
    capital: float,
    risk_pct: float = 0.01,
) -> PositionResult:
    """
    タートルズのルールに従ってポジションサイズを計算する。

    計算式:
        リスク金額  = 資金 × リスク割合
        1ユニット株数 = リスク金額 ÷ ATR
        損切り価格   = 終値 - 2 × ATR

    Parameters
    ----------
    market    : MarketData（終値・ATR）
    capital   : 投資資金
    risk_pct  : 1ユニットあたりのリスク割合（デフォルト 1%）

    Returns
    -------
    PositionResult
    """
    japan = is_japan_stock(market.ticker)

    risk_amount  = capital * risk_pct                             # リスク金額
    raw_shares   = risk_amount / market.atr                       # 生の株数
    unit_shares  = round_to_lot(raw_shares, is_japan=japan)       # 単位丸め後
    stop_loss    = round(market.close - 2 * market.atr, 2)        # 損切り価格
    unit_loss    = round(unit_shares * 2 * market.atr, 0)         # 1ユニット損失額
    max_invest   = round(market.close * unit_shares * 4, 0)       # 4ユニット合計投資額

    pyramid = calculate_pyramid_entries(
        entry_price=market.close,
        atr=market.atr,
        unit_shares=unit_shares,
        num_units=4,
    )

    return PositionResult(
        ticker=market.ticker,
        close=market.close,
        atr=market.atr,
        capital=capital,
        risk_pct=risk_pct,
        risk_amount=risk_amount,
        unit_shares=unit_shares,
        stop_loss=stop_loss,
        unit_loss=unit_loss,
        is_japan=japan,
        pyramid_entries=pyramid,
        max_investment=max_invest,
    )


# ---------------------------------------------------------------------------
# 表示
# ---------------------------------------------------------------------------

def fmt_price(value: float, currency: str) -> str:
    """通貨に応じた価格フォーマット"""
    if currency == "JPY":
        return f"¥{value:,.0f}"
    return f"${value:,.2f}"


def print_result(result: PositionResult, currency: str = "JPY") -> None:
    """計算結果をターミナルに表示する"""
    fp = lambda v: fmt_price(v, currency)
    sep = "=" * 52

    print()
    print(sep)
    print(f"  タートルズ ポジションサイズ計算結果")
    print(sep)
    print(f"  銘柄コード      : {result.ticker}")
    print(f"  投資資金        : {fp(result.capital)}")
    print(f"  リスク割合      : {result.risk_pct * 100:.1f}%")
    print(sep)
    print(f"  現在の終値      : {fp(result.close)}")
    print(f"  ATR（20日）     : {fp(result.atr)}")
    print()
    print(f"  【ポジション計算】")
    print(f"  リスク金額      : {fp(result.risk_amount)}")
    print(f"  1ユニット株数   : {result.unit_shares:,} 株")
    print(f"  損切り価格      : {fp(result.stop_loss)}  （終値 - 2ATR）")
    print(f"  1ユニット損失額 : {fp(result.unit_loss)}")
    print(f"  最大4ユニット   : {fp(result.max_investment)}  （合計投資額）")
    print()
    print(f"  【ピラミッディング（0.5ATR ごと追加）】")
    print(f"  {'Unit':<6} {'エントリー価格':>14} {'損切り価格':>14} {'株数':>8} {'投資額':>14}")
    print(f"  {'-'*6} {'-'*14} {'-'*14} {'-'*8} {'-'*14}")
    for e in result.pyramid_entries:
        print(
            f"  {e['unit']:<6} "
            f"{fp(e['entry_price']):>14} "
            f"{fp(e['stop_loss']):>14} "
            f"{e['shares']:>8,} 株 "
            f"{fp(e['investment']):>14}"
        )
    print(sep)
    print()


# ---------------------------------------------------------------------------
# CLI エントリポイント
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="タートルズ ポジションサイズ計算ツール",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
例:
  python turtle_position.py
  python turtle_position.py --ticker 7203.T --capital 1000000
  python turtle_position.py --ticker AAPL --capital 10000 --risk 0.02
        """,
    )
    parser.add_argument("--ticker",  type=str,   default=None,  help="銘柄コード（例: 7203.T）")
    parser.add_argument("--capital", type=float, default=None,  help="投資資金（円 or ドル）")
    parser.add_argument("--risk",    type=float, default=0.01,  help="リスク割合（デフォルト: 0.01 = 1%%）")
    return parser


def prompt_inputs(args: argparse.Namespace) -> tuple[str, float, float]:
    """未入力の引数をインタラクティブに取得する"""
    ticker = args.ticker
    if not ticker:
        ticker = input("銘柄コードを入力してください（例: 7203.T / AAPL）: ").strip()
        if not ticker:
            print("エラー: 銘柄コードが入力されていません。")
            sys.exit(1)

    capital = args.capital
    if capital is None:
        raw = input("投資資金を入力してください（例: 1000000）: ").strip()
        try:
            capital = float(raw.replace(",", ""))
        except ValueError:
            print("エラー: 投資資金は数値で入力してください。")
            sys.exit(1)

    risk = args.risk
    # デフォルト 1% をそのまま使用（CLI 引数で上書き可能）

    return ticker, capital, risk


def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()

    ticker, capital, risk = prompt_inputs(args)

    print(f"\n{ticker} のデータを取得中...")

    try:
        market = fetch_market_data(ticker)
    except ValueError as e:
        print(f"\nエラー: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n予期しないエラーが発生しました: {e}")
        sys.exit(1)

    result   = compute_position(market, capital=capital, risk_pct=risk)
    currency = market.currency
    print_result(result, currency=currency)


if __name__ == "__main__":
    main()
