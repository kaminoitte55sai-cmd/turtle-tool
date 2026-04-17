"""
app.py
======
タートルズ資金管理ツール + ブレイクアウトスクリーナー（実戦強化版）
Streamlit UI（タブ構成）

実行方法:
    streamlit run app.py
"""
from __future__ import annotations

import json
import math
import os
import re
import requests as _req
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")          # Streamlit 環境では非インタラクティブ backend を指定
import matplotlib.pyplot as plt
import yfinance as yf
import streamlit as st
import jpholiday

from utils import fetch_exchange_rate, fetch_market_data, is_japan_stock

# ===========================================================================
# ページ設定
# ===========================================================================
st.set_page_config(
    page_title="タートルズ 資金管理ツール",
    page_icon="🐢",
    layout="wide",
)

# ===========================================================================
# 銘柄プリセット定数
# ===========================================================================

TOPIX100_TICKERS = [
    # 情報・通信
    "9432.T", "9433.T", "9434.T", "9984.T", "9613.T", "4704.T",
    # 電機・精密
    "6758.T", "6861.T", "8035.T", "6723.T", "6762.T", "6981.T",
    "6971.T", "6857.T", "6146.T", "6702.T", "6501.T", "6954.T",
    "6902.T", "6594.T", "6645.T", "6367.T", "6976.T", "7733.T", "6503.T",
    # 自動車・輸送機器
    "7203.T", "7267.T", "7270.T", "7201.T", "7309.T",
    # 銀行
    "8306.T", "8316.T", "8411.T", "8309.T",
    # 保険・証券・その他金融
    "8766.T", "8630.T", "8750.T", "8725.T", "8795.T",
    "8591.T", "8604.T", "8601.T", "8697.T",
    # 製薬・ヘルスケア
    "4502.T", "4519.T", "4568.T", "4523.T", "4503.T",
    "4507.T", "4543.T", "4578.T", "6869.T", "4151.T",
    # 商社
    "8058.T", "8031.T", "8001.T", "8002.T", "8053.T", "8015.T",
    # 化学・素材
    "4063.T", "3407.T", "4188.T", "4183.T", "4901.T",
    "4452.T", "4911.T", "5201.T", "5713.T",
    # 重工業・機械
    "7011.T", "7012.T", "7013.T", "6301.T", "6326.T", "6273.T", "6506.T",
    # 鉄鋼・非鉄
    "5401.T", "5411.T", "5802.T",
    # 不動産
    "8802.T", "8801.T", "8830.T",
    # 小売・消費財
    "9983.T", "3382.T", "8267.T", "9843.T",
    # 食品・飲料・たばこ
    "2802.T", "2502.T", "2503.T", "2914.T",
    # 海運
    "9101.T", "9104.T", "9107.T",
    # 陸運・物流
    "9020.T", "9022.T", "9021.T", "9064.T", "9062.T",
    # エネルギー
    "5020.T", "5019.T",
    # その他
    "7741.T", "6098.T", "4661.T", "5108.T", "7751.T",
    "2413.T", "3092.T", "6178.T", "6460.T",
]

_NIKKEI225_EXTRA = [
    "1332.T", "1333.T", "1605.T",
    "1721.T", "1801.T", "1802.T", "1803.T", "1812.T", "1925.T", "1928.T",
    "2002.T", "2269.T", "2282.T", "2432.T", "2501.T", "2531.T", "2768.T",
    "3105.T", "3401.T", "3402.T", "3405.T", "3436.T", "3861.T",
    "4021.T", "4042.T", "4061.T", "4208.T", "4307.T", "4324.T",
    "4385.T", "4612.T", "4631.T", "4689.T",
    "5101.T", "5214.T", "5332.T", "5333.T", "5334.T",
    "5406.T", "5631.T", "5711.T", "5714.T", "5801.T", "5803.T",
    "6103.T", "6113.T", "6201.T", "6268.T", "6302.T", "6305.T",
    "6361.T", "6383.T", "6471.T", "6479.T", "6508.T", "6586.T", "6674.T",
    "6701.T", "6724.T", "6752.T", "6753.T", "6770.T", "6841.T", "6952.T", "6963.T",
    "7003.T", "7181.T", "7182.T", "7211.T", "7240.T", "7259.T",
    "7261.T", "7272.T", "7276.T", "7731.T", "7735.T", "7752.T",
    "7832.T", "7911.T", "7912.T", "7951.T",
    "8233.T", "8252.T", "8253.T", "8308.T", "8331.T", "8354.T",
    "9001.T", "9005.T", "9007.T", "9008.T", "9009.T",
    "9042.T", "9045.T", "9048.T", "9301.T",
    "9501.T", "9502.T", "9503.T", "9531.T", "9532.T",
    "9602.T", "9735.T", "9766.T",
]

NIKKEI225_TICKERS = list(dict.fromkeys(TOPIX100_TICKERS + _NIKKEI225_EXTRA))

# ===========================================================================
# その他定数
# ===========================================================================
INITIAL_ROWS   = 20
MARGIN_BALANCE = 5_000_000

READONLY_COLS = [
    "銘柄名", "前日終値", "ATR", "ユニットサイズ", "1日のリスク",
    "ロスカットライン", "購入価格(円)", "購入価格(USD)",
    "評価損益率(%)", "評価損益額(円)",
]

NUMERIC_COLS = [
    "前日終値", "ATR", "ユニットサイズ", "保有株数", "1日のリスク",
    "建玉時株価", "ロスカットライン", "購入価格(円)", "購入価格(USD)",
    "評価損益率(%)", "評価損益額(円)",
]

NAN          = float("nan")
SAVE_FILE    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "turtle_save.json")  # 旧互換用
MASTER_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "master_save.pkl")

# ── 複数ポジションリスト設定 ─────────────────────────────────────────────────
NUM_POS_LISTS = 5
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def _list_save_file(list_id: int) -> str:
    """リストIDに対応する保存ファイルパスを返す。"""
    return os.path.join(_BASE_DIR, f"turtle_save_{list_id}.json")
FILTER_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "filter_save.json")
FUNDA_FILE        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fundamental_list.csv")
MEMO_FILE         = os.path.join(os.path.dirname(os.path.abspath(__file__)), "memo_save.json")
try:
    EDINETDB_API_KEY = st.secrets["EDINETDB_API_KEY"]
except Exception:
    EDINETDB_API_KEY = "edb_473632966cd7b89fda850fde6baa05f6"  # ローカル開発用フォールバック

EDINETDB_MCP_URL  = "https://edinetdb.jp/mcp"

# J-Quants API（JPX公式、理論株価計算の主データソース）
try:
    JQUANTS_REFRESH_TOKEN = st.secrets["JQUANTS_REFRESH_TOKEN"]
except Exception:
    JQUANTS_REFRESH_TOKEN = "oi4woGg05PUMAi2Lx8WJm2udTm6_H2by1peFcYQaQOU"  # ローカル開発用

JQUANTS_AUTH_URL  = "https://api.jquants.com/v1/token/auth_refresh"
JQUANTS_FINS_URL  = "https://api.jquants.com/v1/fins/statements"

# クラウド環境検出（Streamlit Cloud = Linux）
import platform as _platform
_IS_WINDOWS = _platform.system() == "Windows"

# フィルター設定の保存対象キー（session_state のキーと対応）
_FILTER_KEYS = [
    "sc_market_options",
    "sc_industry_options",
    "sc_topix_only",
    "sc_topix_size",
]

PRESET_OPTIONS = [
    "手動入力",
    f"TOPIX100 プリセット（{len(TOPIX100_TICKERS)}銘柄）",
    f"日経225 プリセット（約{len(NIKKEI225_TICKERS)}銘柄）",
]

# 戦略比較: 戦略キー → 表示名
STRATEGIES: dict[str, str] = {
    "momentum":   "📈 モメンタム（ブレイク強度 + 出来高）",
    "squeeze":    "🗜️ スクイーズ（レンジ圧縮）",
    "volatility": "🌊 ボラティリティ（ATR比率）",
    "trend":      "📊 トレンド（MA傾き）",
    "event":      "⭐ 複合スコア（全要素均等）",
}

# ===========================================================================
# 保存 / 読み込み
# ===========================================================================

def save_state(list_id: int | None = None) -> None:
    """現在のポジションリストをファイルに保存する。list_id 省略時はアクティブリストへ保存。"""
    if list_id is None:
        list_id = st.session_state.get("active_list", 1)
    records = st.session_state.df.to_dict("records")
    clean = [
        {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()}
        for row in records
    ]
    _names = st.session_state.get("list_names", {})
    # 理論株価キャッシュ
    _tp = st.session_state.get(f"tp_cache_{list_id}", {})
    data = {
        "name":               _names.get(list_id, f"リスト{list_id}"),
        "n_rows":             st.session_state.n_rows,
        "capital":            st.session_state.capital,
        "losscut_mult":       st.session_state.losscut_mult,
        "risk_pct":           st.session_state.risk_pct,
        "prev_tickers":       st.session_state.prev_tickers,
        "fx_rates":           st.session_state.fx_rates,
        "df_records":         clean,
        "theoretical_prices": _tp.get("prices", {}),
        "theoretical_at":     _tp.get("calculated_at", ""),
    }
    with open(_list_save_file(list_id), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def load_state(list_id: int | None = None) -> dict | None:
    """指定リストのデータを読み込む。list_id 省略時はアクティブリストから読む。"""
    if list_id is None:
        list_id = st.session_state.get("active_list", 1)
    path = _list_save_file(list_id)
    # リスト1 で新ファイルがなければ旧 turtle_save.json にフォールバック
    if not os.path.exists(path) and list_id == 1 and os.path.exists(SAVE_FILE):
        path = SAVE_FILE
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _load_list_into_state(list_id: int) -> None:
    """指定リストのデータを session_state に読み込む（リスト切替・初期化で使用）。"""
    saved = load_state(list_id)
    if saved:
        n = saved.get("n_rows", INITIAL_ROWS)
        st.session_state.n_rows       = n
        st.session_state.capital      = saved.get("capital", 1_000_000)
        st.session_state.losscut_mult = saved.get("losscut_mult", 2.0)
        st.session_state.risk_pct     = saved.get("risk_pct", 0.01)
        st.session_state.prev_tickers = saved.get("prev_tickers", [""] * n)
        st.session_state.fx_rates     = saved.get("fx_rates", [1.0] * n)
        st.session_state.df           = records_to_df(saved["df_records"])
        # 保存されたリスト名を反映
        _saved_name = saved.get("name")
        if _saved_name:
            _names = st.session_state.setdefault("list_names",
                         {i: f"リスト{i}" for i in range(1, NUM_POS_LISTS + 1)})
            _names[list_id] = _saved_name
        # 保存された理論株価キャッシュを復元
        _saved_tp = saved.get("theoretical_prices", {})
        _saved_tp_at = saved.get("theoretical_at", "")
        if _saved_tp:
            st.session_state[f"tp_cache_{list_id}"] = {
                "prices":        _saved_tp,
                "calculated_at": _saved_tp_at,
            }
    else:
        st.session_state.n_rows       = INITIAL_ROWS
        st.session_state.capital      = 1_000_000
        st.session_state.losscut_mult = 2.0
        st.session_state.risk_pct     = 0.01
        st.session_state.prev_tickers = [""] * INITIAL_ROWS
        st.session_state.fx_rates     = [1.0] * INITIAL_ROWS
        st.session_state.df           = make_empty_df(INITIAL_ROWS)


def records_to_df(records: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = NAN
    for col in ["銘柄コード", "銘柄名"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
    if "売買" in df.columns:
        df["売買"] = df["売買"].fillna("買い").astype(str)
    if "購入価格" in df.columns and "購入価格(円)" not in df.columns:
        df.rename(columns={"購入価格": "購入価格(円)"}, inplace=True)
    return df


# ===========================================================================
# ポジション計算タブ: DataFrame 初期化
# ===========================================================================

def empty_row() -> dict:
    return {
        "銘柄コード":       "",
        "銘柄名":           "",
        "前日終値":         NAN,
        "ATR":              NAN,
        "ユニットサイズ":   NAN,
        "保有株数":         NAN,
        "1日のリスク":      NAN,
        "建玉時株価":       NAN,
        "売買":             "買い",
        "ロスカットライン": NAN,
        "購入価格(円)":     NAN,
        "購入価格(USD)":    NAN,
        "評価損益率(%)":    NAN,
        "評価損益額(円)":   NAN,
    }


def make_empty_df(n: int) -> pd.DataFrame:
    return pd.DataFrame([empty_row() for _ in range(n)])


def init_state() -> None:
    # アクティブリスト・リスト名を初期化（初回のみ）
    if "active_list" not in st.session_state:
        st.session_state.active_list = 1
    if "list_names" not in st.session_state:
        # 保存済みファイルからリスト名を復元
        _names: dict[int, str] = {}
        for _lid in range(1, NUM_POS_LISTS + 1):
            _d = load_state(_lid)
            _names[_lid] = _d.get("name", f"リスト{_lid}") if _d else f"リスト{_lid}"
        st.session_state.list_names = _names

    # ポジションデータをアクティブリストから読み込む（初回のみ）
    if "df" not in st.session_state:
        _load_list_into_state(st.session_state.active_list)

    if "screener_results"     not in st.session_state:
        st.session_state.screener_results     = None
    if "screener_input"       not in st.session_state:
        st.session_state.screener_input       = "7203, 9984, 6758, 8306, 9433, 6367, 8035, 4063, 6501, 6857"
    if "screener_prev_preset" not in st.session_state:
        st.session_state.screener_prev_preset = PRESET_OPTIONS[0]

    # バックテストタブ用セッション変数
    if "bt_results"      not in st.session_state:
        st.session_state.bt_results      = None
    if "bt_raw_trades"   not in st.session_state:
        st.session_state.bt_raw_trades   = []        # 戦略比較用の生トレードリスト
    if "bt_ticker_input" not in st.session_state:
        st.session_state.bt_ticker_input = "7203, 9984, 6758, 8306, 9433"
    if "bt_prev_preset"  not in st.session_state:
        st.session_state.bt_prev_preset  = PRESET_OPTIONS[0]
    if "bt_csv_file_id"  not in st.session_state:
        st.session_state.bt_csv_file_id  = None
    if "tf_results"      not in st.session_state:
        st.session_state.tf_results      = None
    if "opt_results"     not in st.session_state:
        st.session_state.opt_results     = None

    # スクリーナータブ用セッション変数
    _SC_DEFAULT = "7203, 9984, 6758, 8306, 9433"
    if "sc_ticker_input"  not in st.session_state:
        st.session_state.sc_ticker_input  = _SC_DEFAULT
    if "sc_ticker_area"   not in st.session_state:
        st.session_state.sc_ticker_area   = _SC_DEFAULT
    if "sc_prev_preset"   not in st.session_state:
        st.session_state.sc_prev_preset   = PRESET_OPTIONS[0]
    if "sc_csv_file_id"   not in st.session_state:
        st.session_state.sc_csv_file_id   = None

    # フィルター設定をファイルから復元（保存済みの場合のみ上書き）
    if os.path.exists(FILTER_FILE):
        try:
            with open(FILTER_FILE, encoding="utf-8") as _ff:
                _saved_filters = json.load(_ff)
            for _fk in _FILTER_KEYS:
                if _fk in _saved_filters and _fk not in st.session_state:
                    st.session_state[_fk] = _saved_filters[_fk]
        except Exception:
            pass

    # 銘柄マスタ / ファンダ分析リスト
    if "master" not in st.session_state:
        # ページリロード後もマスタを保持するためファイルから復元
        import pickle as _pkl
        if os.path.exists(MASTER_FILE):
            try:
                with open(MASTER_FILE, "rb") as _f:
                    _saved_master = _pkl.load(_f)
                st.session_state.master      = _saved_master.get("master")
                st.session_state.last_update = _saved_master.get("last_update")
            except Exception:
                st.session_state.master      = None
                st.session_state.last_update = None
        else:
            st.session_state.master      = None
            st.session_state.last_update = None
    if "last_update" not in st.session_state:
        st.session_state.last_update = None
    if "funda_list" not in st.session_state:
        st.session_state.funda_list = []   # スクリーニング → ファンダタブへの橋渡しコードリスト
    if "fund_df" not in st.session_state:
        st.session_state.fund_df = load_funda_data()   # ファンダ一覧 DataFrame（CSV 永続化）
    if "funda_memos" not in st.session_state:
        st.session_state.funda_memos = load_memo_data()  # 銘柄メモ dict {code: text}


# ===========================================================================
# ポジション計算タブ: ティッカー処理
# ===========================================================================

def normalize_ticker(code: str) -> str:
    code = code.strip().upper()
    if not code:
        return code
    if code.endswith(".T"):
        return code
    if code.isdigit():
        return code + ".T"
    return code


@st.cache_data(ttl=86400, show_spinner=False)
def get_ticker_name(ticker: str) -> str:
    """銘柄コードから銘柄名を取得する。日本株はEdinetDB優先→YahooFinance→yfinance の順で試みる。"""
    if is_japan_stock(ticker):
        _code = ticker.upper().replace(".T", "")

        # ① EdinetDB（最も信頼性高・クラウドでも動作）
        try:
            _r = _call_edinetdb("search_companies", query=_code)
            for _c in _r.get("companies", []):
                if str(_c.get("secCode", "")).startswith(_code):
                    _n = _c.get("name") or _c.get("filerName")
                    if _n:
                        return _n
            # 先頭一致がなければ最初の候補を使用
            if _r.get("companies"):
                _n = _r["companies"][0].get("name") or _r["companies"][0].get("filerName")
                if _n:
                    return _n
        except Exception:
            pass

        # ② Yahoo Finance Search API（ローカル環境向け）
        try:
            r = _req.get(
                "https://query1.finance.yahoo.com/v1/finance/search",
                params={"q": ticker, "lang": "ja", "region": "JP", "newsCount": 0},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=5,
            )
            for q in r.json().get("quotes", []):
                if q.get("symbol") == ticker:
                    name = q.get("longname") or q.get("shortname")
                    if name:
                        return name
        except Exception:
            pass

    # ③ yfinance info（米国株・最終フォールバック）
    try:
        info = yf.Ticker(ticker).info
        return info.get("longName") or info.get("shortName") or ticker
    except Exception:
        return ticker


# ===========================================================================
# ポジション計算タブ: 計算ヘルパー
# ===========================================================================

def _ok(val) -> bool:
    try:
        return val is not None and not pd.isna(val)
    except Exception:
        return False


def to_man_en(value: float) -> str:
    man = value / 10_000
    return f"{int(man)}万円" if man == int(man) else f"{man:.1f}万円"


def _floor_jp(value: float, japan: bool) -> float:
    return float(math.floor(value)) if japan else value


def calc_unit_size(capital_jpy, risk_pct, atr, japan, fx_rate) -> float | None:
    if not _ok(atr) or float(atr) == 0:
        return None
    cap = capital_jpy if japan else capital_jpy / fx_rate
    return round(cap * risk_pct / float(atr), 1)


def calc_daily_risk_ratio(unit_size, shares_held) -> float | None:
    if not _ok(unit_size) or not _ok(shares_held):
        return None
    u, h = float(unit_size), float(shares_held)
    return None if u == 0 else round(h / u, 2)


def calc_losscut(entry_price, atr, losscut_mult: float, is_buy: bool, japan: bool = False) -> float | None:
    if not _ok(entry_price) or not _ok(atr):
        return None
    delta  = losscut_mult * float(atr)
    base   = float(entry_price)
    result = base - delta if is_buy else base + delta
    return _floor_jp(round(result, 2), japan)


def calc_purchase(shares_held, entry_price, japan: bool, fx_rate: float) -> tuple[float | None, float | None]:
    if not _ok(shares_held) or not _ok(entry_price):
        return None, None
    shares = float(shares_held)
    entry  = float(entry_price)
    if japan:
        return round(shares * entry, 0), None
    else:
        usd = round(shares * entry, 2)
        return round(usd * fx_rate, 0), usd


def recalc_row(i: int, capital: float, risk_pct: float, losscut_mult: float) -> bool:
    df     = st.session_state.df
    ticker = df.at[i, "銘柄コード"]
    atr    = df.at[i, "ATR"]
    if not ticker or not _ok(atr):
        return False

    japan, fx, changed = is_japan_stock(ticker), st.session_state.fx_rates[i], False

    new_unit = calc_unit_size(capital, risk_pct, float(atr), japan, fx)
    if new_unit != df.at[i, "ユニットサイズ"]:
        df.at[i, "ユニットサイズ"] = new_unit if new_unit is not None else NAN
        changed = True
    else:
        new_unit = df.at[i, "ユニットサイズ"]

    new_ratio, cur_ratio = calc_daily_risk_ratio(new_unit, df.at[i, "保有株数"]), df.at[i, "1日のリスク"]
    if not (not _ok(new_ratio) and not _ok(cur_ratio)) and new_ratio != cur_ratio:
        df.at[i, "1日のリスク"] = new_ratio if new_ratio is not None else NAN
        changed = True

    is_buy = df.at[i, "売買"] == "買い"
    new_lc = calc_losscut(df.at[i, "建玉時株価"], float(atr), losscut_mult, is_buy, japan)
    cur_lc = df.at[i, "ロスカットライン"]
    if not (not _ok(new_lc) and not _ok(cur_lc)) and new_lc != cur_lc:
        df.at[i, "ロスカットライン"] = new_lc if new_lc is not None else NAN
        changed = True

    new_jpy, new_usd = calc_purchase(df.at[i, "保有株数"], df.at[i, "建玉時株価"], japan, fx)
    cur_jpy, cur_usd = df.at[i, "購入価格(円)"], df.at[i, "購入価格(USD)"]
    if not (not _ok(new_jpy) and not _ok(cur_jpy)) and new_jpy != cur_jpy:
        df.at[i, "購入価格(円)"] = new_jpy if new_jpy is not None else NAN
        changed = True
    if not (not _ok(new_usd) and not _ok(cur_usd)) and new_usd != cur_usd:
        df.at[i, "購入価格(USD)"] = new_usd if new_usd is not None else NAN
        changed = True

    # ── 評価損益率 / 評価損益額 ─────────────────────────────────────────────
    cur_price  = df.at[i, "前日終値"]
    entry_price = df.at[i, "建玉時株価"]
    shares      = df.at[i, "保有株数"]
    if _ok(cur_price) and _ok(entry_price) and float(entry_price) != 0:
        sign    = 1.0 if df.at[i, "売買"] == "買い" else -1.0
        pnl_pct = sign * (float(cur_price) - float(entry_price)) / float(entry_price) * 100
        new_pnl_pct = round(pnl_pct, 2)
        if new_pnl_pct != df.at[i, "評価損益率(%)"]:
            df.at[i, "評価損益率(%)"] = new_pnl_pct
            changed = True
        if _ok(shares):
            pnl_amount = sign * (float(cur_price) - float(entry_price)) * float(shares) * fx
            new_pnl_amt = round(pnl_amount, 0)
            if new_pnl_amt != df.at[i, "評価損益額(円)"]:
                df.at[i, "評価損益額(円)"] = new_pnl_amt
                changed = True
    else:
        if _ok(df.at[i, "評価損益率(%)"]):
            df.at[i, "評価損益率(%)"] = NAN
            changed = True
        if _ok(df.at[i, "評価損益額(円)"]):
            df.at[i, "評価損益額(円)"] = NAN
            changed = True

    return changed


# ===========================================================================
# ポジション計算タブ: 行操作
# ===========================================================================

def clear_row(i: int) -> None:
    for k, v in empty_row().items():
        st.session_state.df.at[i, k] = v
    st.session_state.prev_tickers[i] = ""
    st.session_state.fx_rates[i]     = 1.0


def delete_row(i: int) -> None:
    df = st.session_state.df.drop(index=i).reset_index(drop=True)
    st.session_state.df = df
    st.session_state.prev_tickers.pop(i)
    st.session_state.fx_rates.pop(i)
    st.session_state.n_rows -= 1


def move_row(i: int, direction: int) -> None:
    j = i + direction
    n = st.session_state.n_rows
    if j < 0 or j >= n:
        return
    idx = list(range(n))
    idx[i], idx[j] = idx[j], idx[i]
    st.session_state.df = st.session_state.df.iloc[idx].reset_index(drop=True)
    pt = st.session_state.prev_tickers
    pt[i], pt[j] = pt[j], pt[i]
    fx = st.session_state.fx_rates
    fx[i], fx[j] = fx[j], fx[i]


def add_row() -> None:
    new_row = pd.DataFrame([empty_row()])
    st.session_state.df = pd.concat([st.session_state.df, new_row], ignore_index=True)
    st.session_state.prev_tickers.append("")
    st.session_state.fx_rates.append(1.0)
    st.session_state.n_rows += 1


# ===========================================================================
# 銘柄マスタ: JPX公式データ取得
# ===========================================================================

@st.cache_data(ttl=86400)
def load_jpx_data() -> pd.DataFrame:
    """JPX公式XLSから銘柄マスタを取得して返す（24hキャッシュ）。

    JPXは2024年頃にCSV提供を廃止しXLS形式に変更。
    URL: .../tvdivq0000001vg2-att/data_j.xls
    カラム: 日付, コード, 銘柄名, 市場・商品区分, 33業種区分 ...
    """
    import io as _io
    url = (
        "https://www.jpx.co.jp/markets/statistics-equities/misc/"
        "tvdivq0000001vg2-att/data_j.xls"
    )
    resp = _req.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    resp.raise_for_status()

    df = pd.read_excel(_io.BytesIO(resp.content), engine="xlrd", dtype=str)

    df = df.rename(columns={
        "コード":         "code",
        "銘柄名":         "name",
        "市場・商品区分": "market",
        "33業種区分":     "industry",
    })

    # コード正規化: "1301" or "1301.0" → "1301.T"
    df["code"] = (
        df["code"].astype(str).str.strip()
        .str.split(".").str[0].str.zfill(4) + ".T"
    )

    cols = [c for c in ["code", "name", "market", "industry"] if c in df.columns]
    return df[cols].reset_index(drop=True)


@st.cache_data(ttl=86400)
def load_topix_detail() -> pd.DataFrame:
    """JPXからTOPIX構成銘柄・指数区分を取得して返す（24hキャッシュ）。

    URL: /automation/markets/indices/topix/files/topixweight_j.csv
    カラム: 日付, 銘柄名, コード, 業種, TOPIXに占める個別銘柄のウエイト, ニューインデックス区分
    ニューインデックス区分: TOPIX Core30 / TOPIX Large70 / TOPIX Mid400 / TOPIX Small 1 / TOPIX Small 2
    """
    url = (
        "https://www.jpx.co.jp/automation/markets/indices/topix/"
        "files/topixweight_j.csv"
    )
    resp = _req.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    resp.raise_for_status()

    import io as _io
    df = pd.read_csv(_io.BytesIO(resp.content), encoding="shift-jis", dtype=str)

    df = df.rename(columns={
        "コード":             "code",
        "ニューインデックス区分": "topix_size",
    })

    # コード正規化: "1301" → "1301.T"
    df["code"] = (
        df["code"].astype(str).str.strip()
        .str.split(".").str[0].str.zfill(4) + ".T"
    )

    # 指数区分を短縮名に変換
    SIZE_MAP = {
        "TOPIX Core30":  "Core30",
        "TOPIX Large70": "Large70",
        "TOPIX Mid400":  "Mid400",
        "TOPIX Small 1": "Small1",
        "TOPIX Small 2": "Small2",
    }
    df["topix_size"] = df["topix_size"].map(SIZE_MAP).fillna("Other")
    df["topix_flag"] = True

    return df[["code", "topix_flag", "topix_size"]].drop_duplicates("code").reset_index(drop=True)


@st.cache_data(ttl=86400)
def get_market_cap(code: str):
    """yfinance から時価総額を取得（24hキャッシュ）"""
    try:
        return yf.Ticker(code).info.get("marketCap")
    except Exception:
        return None


# ===========================================================================
# スクリーナー: 銘柄名取得（キャッシュ付き）
# ===========================================================================

@st.cache_data(ttl=3600)
def _get_stock_name(ticker: str) -> str:
    """
    yfinance から銘柄名を取得（1時間キャッシュ）。
    日本株（.T）は shortName（日本語）を優先する。
    """
    try:
        info = yf.Ticker(ticker).info
        if ticker.upper().endswith(".T"):
            name = info.get("shortName") or info.get("longName") or ticker
        else:
            name = info.get("longName") or info.get("shortName") or ticker
        return name or ticker
    except Exception:
        return ticker


# ===========================================================================
# スクリーナー: 1銘柄スクリーニング（コアロジック）
# ===========================================================================

def screen_ticker(
    ticker:        str,
    donchian_days: int,
    vol_mult_thr:  float,
    delay_days:    int,
    dd_threshold:  float,
) -> tuple[dict | None, str | None]:
    """
    時間フィルター＋待機DDフィルター付きブレイクアウトスクリーナー。

    ロジック:
        1. ドンチャン高値上抜け（shift(1)使用）を検出
        2. ブレイク日から delay_days 後の価格がブレイク価格を上回るか確認
        3. 待機期間中の最大下落が dd_threshold 以内か確認
        4. 出来高倍率が vol_mult_thr 以上か確認

    Returns:
        (metrics_dict, None) : 条件通過
        (None, None)         : 条件不通過（エラーなし）
        (None, error_msg)    : データ取得失敗
    """
    SEARCH_WINDOW = delay_days + 8

    try:
        df = yf.download(ticker, period="6mo", progress=False, auto_adjust=True)

        if df.empty:
            return None, "データなし"

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        min_rows = max(donchian_days + SEARCH_WINDOW + 5, 40)
        if len(df) < min_rows:
            return None, f"データ不足（{len(df)}日）"

        close_arr    = df["Close"].to_numpy(dtype=float)
        low_arr      = df["Low"].to_numpy(dtype=float)
        vol_arr      = df["Volume"].to_numpy(dtype=float)
        donchian_arr = df["High"].rolling(donchian_days).max().shift(1).to_numpy(dtype=float)
        avg_vol_arr  = df["Volume"].rolling(donchian_days).mean().shift(1).to_numpy(dtype=float)

        n       = len(df)
        today_i = n - 1

        for days_since_break in range(delay_days, SEARCH_WINDOW):
            breakout_i = today_i - days_since_break

            if breakout_i < donchian_days + 1:
                break

            dc = donchian_arr[breakout_i]
            if np.isnan(dc) or dc <= 0:
                continue

            bo_close = float(close_arr[breakout_i])
            if bo_close <= dc:
                continue

            avg_v = float(avg_vol_arr[breakout_i])
            if avg_v <= 0 or np.isnan(avg_v):
                continue
            vol_ratio = float(vol_arr[breakout_i]) / avg_v
            if vol_ratio < vol_mult_thr:
                continue

            entry_i = breakout_i + delay_days
            if entry_i > today_i:
                continue

            entry_close = float(close_arr[entry_i])
            if entry_close <= bo_close:
                continue

            waiting_dd = 0.0
            if delay_days > 0:
                window_low = low_arr[breakout_i : entry_i + 1]
                min_price  = float(np.nanmin(window_low))
                waiting_dd = (min_price - bo_close) / bo_close * 100
                if waiting_dd <= dd_threshold:
                    continue

            _today_price   = float(close_arr[today_i])
            _breakout_date = df.index[breakout_i].date()
            _today_date    = pd.Timestamp.now().date()
            # 祝日リストを生成（ブレイク日〜今日の範囲）
            import datetime as _dt
            _holidays = [
                _d for _d in (
                    _breakout_date + _dt.timedelta(days=_i)
                    for _i in range((_today_date - _breakout_date).days + 1)
                )
                if jpholiday.is_holiday(_d)
            ]
            _elapsed_days = int(np.busday_count(
                _breakout_date, _today_date,
                holidays=[str(_h) for _h in _holidays],
            ))
            _price_change  = (_today_price / bo_close - 1) * 100

            return {
                "ティッカー":      ticker,
                "現在価格":        round(_today_price, 2),
                "経過日数":        _elapsed_days,
                "ブレイク比(%)":   round(_price_change, 2),
                "出来高倍率":      round(vol_ratio, 2),
                "waiting_dd(%)":   round(waiting_dd, 2),
                "delay日数":       delay_days,
                "ブレイク日":      str(_breakout_date),
                "ブレイク価格":    round(bo_close, 2),
                "エントリー価格":  round(entry_close, 2),
            }, None

        return None, None

    except Exception as e:
        return None, str(e)


# ===========================================================================
# スクリーナー: CSV 銘柄コード解析
# ===========================================================================

def _parse_screener_csv(uploaded_file) -> tuple[list[str], str | None]:
    """
    スクリーナー用 CSV から銘柄コードリストを抽出する（3段階フォールバック）。
    """
    import re as _re

    def _looks_like_code(val: str) -> bool:
        v = val.strip().split(".")[0]
        return v.isdigit() and 3 <= len(v) <= 6

    try:
        csv_df = pd.read_csv(uploaded_file, dtype=str)
        orig_cols = list(csv_df.columns)
        norm_cols = [c.strip().lower() for c in orig_cols]

        col_candidates = [
            "ticker", "code", "symbol",
            "銘柄コード", "証券コード", "コード", "ｺｰﾄﾞ",
            "銘柄cd", "銘柄ｃｄ", "銘柄", "ティッカー",
        ]
        target_col = None
        for cand in col_candidates:
            cand_norm = cand.strip().lower()
            if cand_norm in norm_cols:
                target_col = orig_cols[norm_cols.index(cand_norm)]
                break

        if target_col is None:
            kw_list = ["コード", "code", "ticker", "symbol", "銘柄"]
            for kw in kw_list:
                kw_n = kw.lower()
                for orig, norm in zip(orig_cols, norm_cols):
                    if kw_n in norm:
                        target_col = orig
                        break
                if target_col:
                    break

        if target_col is None:
            best_col, best_score = None, 0
            for col in orig_cols:
                vals  = csv_df[col].dropna().astype(str)
                score = sum(1 for v in vals if _looks_like_code(v))
                if score > best_score:
                    best_score = score
                    best_col   = col
            target_col = best_col or orig_cols[0]

        raw = csv_df[target_col].dropna().astype(str).str.strip()

        tickers: list[str] = []
        for val in raw:
            val = val.strip()
            if not val or val.lower() in ("nan", "none", ""):
                continue
            base = val.split(".")[0].strip()
            if base.isdigit():
                val = base + ".T"
            elif not val.replace(".", "").replace("-", "").replace("_", "").replace(" ", "").isalnum():
                continue
            tickers.append(val.upper())

        tickers = list(dict.fromkeys(tickers))
        if not tickers:
            return [], "CSVに有効な銘柄コードが見つかりませんでした。"
        return tickers, None

    except Exception as e:
        return [], f"CSV読み込みエラー: {e}"


# ===========================================================================
# 画面描画: ポジションサイズ計算タブ
# ===========================================================================

def render_position_tab() -> None:
    # ════════════════════════════════════════════════════════════════════════
    # ▼ ポジションリスト選択（5つ）
    # ════════════════════════════════════════════════════════════════════════
    _active   = st.session_state.get("active_list", 1)
    _names    = st.session_state.get("list_names", {i: f"リスト{i}" for i in range(1, NUM_POS_LISTS + 1)})

    # タブ風ボタン横並び
    _list_cols = st.columns(NUM_POS_LISTS)
    for _ci, _col in enumerate(_list_cols):
        _lid  = _ci + 1
        _lbl  = _names.get(_lid, f"リスト{_lid}")
        _type = "primary" if _lid == _active else "secondary"
        if _col.button(f"{'▶ ' if _lid == _active else ''}{_lbl}",
                       key=f"pl_switch_{_lid}",
                       type=_type,
                       use_container_width=True):
            if _lid != _active:
                save_state(_active)                     # 現在リストを自動保存
                st.session_state.active_list = _lid
                _load_list_into_state(_lid)             # 新リストを読み込み
                st.rerun()

    # リスト名編集 + 自動保存ステータス
    _edit_col, _save_col, _info_col = st.columns([0.35, 0.15, 0.5])
    with _edit_col:
        _new_name = st.text_input(
            "リスト名",
            value=_names.get(_active, f"リスト{_active}"),
            key=f"pl_name_{_active}",
            placeholder="リスト名を入力...",
            label_visibility="collapsed",
        )
        if _new_name and _new_name != _names.get(_active):
            st.session_state.list_names[_active] = _new_name
            save_state(_active)   # リスト名変更で自動保存
    with _save_col:
        if st.button("💾 保存", key="pl_save_btn", use_container_width=True):
            save_state(_active)
            st.toast(f"「{_names.get(_active)}」を保存しました ✅", icon="💾")
    with _info_col:
        # 他リストの銘柄数をコンパクト表示
        _summary_parts = []
        for _lid2 in range(1, NUM_POS_LISTS + 1):
            if _lid2 == _active:
                continue
            _d2 = load_state(_lid2)
            if _d2 and _d2.get("df_records"):
                _cnt = sum(1 for r in _d2["df_records"] if r.get("銘柄コード"))
                if _cnt > 0:
                    _n2 = _d2.get("name", f"リスト{_lid2}")
                    _summary_parts.append(f"{_n2}: {_cnt}銘柄")
        if _summary_parts:
            st.caption("他リスト: " + "　".join(_summary_parts))

    st.divider()

    # ════════════════════════════════════════════════════════════════════════
    # ▼ ポジション設定 & サマリー
    # ════════════════════════════════════════════════════════════════════════
    n_rows = st.session_state.n_rows

    total_purchase = float(st.session_state.df["購入価格(円)"].dropna().sum())
    balance        = st.session_state.capital - total_purchase

    # ── 評価損益サマリーを計算 ────────────────────────────────────────────
    _pnl_series = st.session_state.df["評価損益額(円)"].dropna() \
        if "評価損益額(円)" in st.session_state.df.columns else pd.Series([], dtype=float)
    _pnl_total  = float(_pnl_series.sum()) if len(_pnl_series) > 0 else 0.0
    _pnl_gain   = float(_pnl_series[_pnl_series > 0].sum()) if len(_pnl_series) > 0 else 0.0
    _pnl_loss   = float(_pnl_series[_pnl_series < 0].sum()) if len(_pnl_series) > 0 else 0.0
    _pos_count  = int((st.session_state.df["銘柄コード"] != "").sum())

    def _pnl_card(col, label: str, amount: float, show_rate: bool = False) -> None:
        """評価損益を色付きカードで表示。含み益=緑、含み損=赤。"""
        _c = "#16a34a" if amount >= 0 else "#dc2626"   # green-700 / red-600
        _sign = "+" if amount >= 0 else ""
        _rate_html = ""
        if show_rate and st.session_state.capital:
            _r = amount / st.session_state.capital * 100
            _rate_html = f'<div style="font-size:0.78em;color:{_c};margin-top:1px;">{_r:+.2f}%</div>'
        col.markdown(
            f"""
            <div style="padding:6px 0 2px;">
              <div style="font-size:0.82em;color:#555;font-weight:500;">{label}</div>
              <div style="font-size:1.25em;font-weight:700;color:{_c};line-height:1.3;">
                {_sign}¥{amount:,.0f}
              </div>
              {_rate_html}
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ── サマリーメトリクス行 ──────────────────────────────────────────────
    _m1, _m2, _m3, _m4, _m5 = st.columns(5)
    _m1.metric("💴 残高", f"¥{balance:,.0f}")
    _pnl_card(_m2, "📈 評価損益合計", _pnl_total, show_rate=True)
    _pnl_card(_m3, "🟢 含み益",       _pnl_gain)
    _pnl_card(_m4, "🔴 含み損",       _pnl_loss)
    _m5.metric("📋 保有銘柄数", f"{_pos_count} 銘柄")

    c1, c2, c3, c4 = st.columns(4)

    capital = c1.number_input(
        "💴 投資資金（円）",
        min_value=100_000, max_value=1_000_000_000,
        value=st.session_state.capital,
        step=100_000, format="%d",
    )
    c1.caption(f"≈ {to_man_en(capital)}")

    losscut_mult = c2.number_input(
        "✂️ ロスカット幅（× ATR）",
        min_value=0.5, max_value=5.0,
        value=st.session_state.losscut_mult,
        step=0.5, format="%.1f",
    )
    risk_pct_display = c3.slider(
        "📊 リスク割合（%）",
        min_value=0.5, max_value=3.0,
        value=st.session_state.risk_pct * 100,
        step=0.1, format="%.1f%%",
    )
    risk_pct   = risk_pct_display / 100
    total_risk = float(st.session_state.df["1日のリスク"].dropna().sum())
    c4.metric("📈 現在の総リスク", f"{total_risk:.2f}")

    params_changed = (
        capital      != st.session_state.capital or
        losscut_mult != st.session_state.losscut_mult or
        risk_pct     != st.session_state.risk_pct
    )
    st.session_state.capital      = capital
    st.session_state.losscut_mult = losscut_mult
    st.session_state.risk_pct     = risk_pct

    needs_rerun = False
    if params_changed:
        for i in range(n_rows):
            if recalc_row(i, capital, risk_pct, losscut_mult):
                needs_rerun = True
        save_state()   # パラメータ変更で自動保存

    btn1, btn2, _ = st.columns([0.12, 0.18, 0.70])
    with btn1:
        if st.button("➕ 行追加", use_container_width=True):
            add_row(); save_state(); st.rerun()
    with btn2:
        _tp_sk  = f"show_theoretical_{_active}"   # リスト別キー
        _tp_on  = st.session_state.get(_tp_sk, False)
        if st.button(
            "💡 理論株価 ▶" if _tp_on else "💡 理論株価",
            key=f"btn_theoretical_{_active}",
            type="primary" if _tp_on else "secondary",
            use_container_width=True,
        ):
            st.session_state[_tp_sk] = not _tp_on
            st.rerun()

    st.divider()
    col_tbl, col_up, col_dn, col_clr, col_del = st.columns([0.81, 0.0475, 0.0475, 0.0475, 0.0475])

    with col_tbl:
        edited = st.data_editor(
            st.session_state.df,
            column_config={
                "銘柄コード":     st.column_config.TextColumn("銘柄コード",     width="small"),
                "銘柄名":         st.column_config.TextColumn("銘柄名",         width="medium"),
                "前日終値":       st.column_config.NumberColumn("前日終値",      format="%.2f",   width="small"),
                "ATR":            st.column_config.NumberColumn("ATR(20日)",     format="%.2f",   width="small"),
                "ユニットサイズ": st.column_config.NumberColumn("ユニットサイズ",format="%.1f",   width="small"),
                "保有株数":       st.column_config.NumberColumn("保有株数",      format="%.0f",   width="small"),
                "1日のリスク":    st.column_config.NumberColumn("1日のリスク",   format="%.2f",   width="small"),
                "建玉時株価":     st.column_config.NumberColumn("建玉時株価",    format="%.2f",   width="small"),
                "売買":           st.column_config.SelectboxColumn("売買", options=["買い","売り"], width="small"),
                "ロスカットライン": st.column_config.NumberColumn("ロスカットライン", format="%.2f", width="small"),
                "購入価格(円)":   st.column_config.NumberColumn("購入価格(円)",  format="¥%.0f", width="small"),
                "購入価格(USD)":  st.column_config.NumberColumn("購入価格(USD)", format="$%.2f", width="small"),
                "評価損益率(%)":  st.column_config.NumberColumn("損益率(%)",     format="%.2f%%", width="small"),
                "評価損益額(円)": st.column_config.NumberColumn("評価損益(円)",  format="¥%.0f", width="small"),
            },
            disabled=READONLY_COLS,
            use_container_width=True,
            hide_index=True,
            height=35 * n_rows + 40,
        )

    spacer = '<div style="height:40px;"></div>'
    with col_up:
        st.markdown(spacer, unsafe_allow_html=True)
        for i in range(n_rows):
            if st.button("↑", key=f"up_{i}", use_container_width=True, disabled=(i == 0)):
                move_row(i, -1); save_state(); st.rerun()
    with col_dn:
        st.markdown(spacer, unsafe_allow_html=True)
        for i in range(n_rows):
            if st.button("↓", key=f"dn_{i}", use_container_width=True, disabled=(i == n_rows - 1)):
                move_row(i, 1); save_state(); st.rerun()
    with col_clr:
        st.markdown(spacer, unsafe_allow_html=True)
        for i in range(n_rows):
            if st.button("Clear", key=f"clr_{i}", use_container_width=True):
                clear_row(i); save_state(); st.rerun()
    with col_del:
        st.markdown(spacer, unsafe_allow_html=True)
        for i in range(n_rows):
            if st.button("🗑️", key=f"del_{i}", use_container_width=True):
                delete_row(i); save_state(); st.rerun()

    for i in range(n_rows):
        row = edited.iloc[i]
        st.session_state.df.at[i, "銘柄コード"] = row["銘柄コード"] or ""
        st.session_state.df.at[i, "保有株数"]   = row["保有株数"]
        st.session_state.df.at[i, "建玉時株価"] = row["建玉時株価"]
        st.session_state.df.at[i, "売買"]       = row["売買"] or "買い"

        new_ticker  = normalize_ticker((row["銘柄コード"] or "").strip())
        prev_ticker = st.session_state.prev_tickers[i]

        if new_ticker != prev_ticker:
            if new_ticker:
                with st.spinner(f"🔄 {new_ticker} のデータを取得中..."):
                    try:
                        market    = fetch_market_data(new_ticker)
                        japan     = is_japan_stock(new_ticker)
                        fx        = 1.0 if japan else fetch_exchange_rate()
                        st.session_state.fx_rates[i] = fx

                        name      = get_ticker_name(new_ticker)
                        unit_size = calc_unit_size(capital, risk_pct, market.atr, japan, fx)
                        entry     = _floor_jp(round(market.close, 2), japan)
                        losscut   = calc_losscut(entry, market.atr, losscut_mult, True, japan)

                        for k, v in {
                            "銘柄コード":       new_ticker, "銘柄名": name,
                            "前日終値":         entry, "ATR": market.atr,
                            "ユニットサイズ":   unit_size if unit_size is not None else NAN,
                            "建玉時株価":       entry, "1日のリスク": NAN,
                            "ロスカットライン": losscut if losscut is not None else NAN,
                            "購入価格(円)": NAN, "購入価格(USD)": NAN,
                        }.items():
                            st.session_state.df.at[i, k] = v
                    except Exception as e:
                        st.error(f"行{i + 1} [{new_ticker}] データ取得失敗 → {e}")
            else:
                for k, v in empty_row().items():
                    st.session_state.df.at[i, k] = v
                st.session_state.fx_rates[i] = 1.0

            st.session_state.prev_tickers[i] = new_ticker
            save_state()   # ティッカー変更で自動保存
            needs_rerun = True
        elif new_ticker:
            if recalc_row(i, capital, risk_pct, losscut_mult):
                needs_rerun = True

    if needs_rerun:
        st.rerun()

    # ════════════════════════════════════════════════════════════════════════
    # ▼ 理論株価パネル（リスト別・ボタンで開閉・結果をキャッシュ保存）
    # ════════════════════════════════════════════════════════════════════════
    _tp_sk = f"show_theoretical_{_active}"
    if st.session_state.get(_tp_sk, False):
        st.divider()
        st.markdown("### 💡 理論株価")
        st.caption(
            "EdinetDB の財務データから独自ロジックで試算。"
            "理論株価 ＝ 資産価値 ＋ 事業価値 − 市場リスク。"
            "結果はリスト別に保存され、次回起動時も表示されます。"
            "APIは1日100回制限のため、必要時のみ再計算してください。"
        )

        # キャッシュ読み出し
        _tp_ck         = f"tp_cache_{_active}"
        _tp_cache      = st.session_state.get(_tp_ck, {})
        _cached_prices = _tp_cache.get("prices", {})
        _calculated_at = _tp_cache.get("calculated_at", "")

        # ── 銘柄コードが入力されている行を収集 ──────────────────────────
        _tp_rows = []
        for _i in range(n_rows):
            _code_raw = st.session_state.df.at[_i, "銘柄コード"]
            _price_v  = st.session_state.df.at[_i, "前日終値"]
            if _code_raw and str(_code_raw).strip():
                _sc   = str(_code_raw).upper().replace(".T", "").strip()
                _name = st.session_state.df.at[_i, "銘柄名"] or _sc
                _price = float(_price_v) if _ok(_price_v) else None
                _tp_rows.append((_sc, _name, _price))

        if not _tp_rows:
            st.info("銘柄コードが入力された行がありません。")
        else:
            # ── 計算ボタンエリア ──────────────────────────────────────────
            _bc1, _bc2, _bi = st.columns([0.18, 0.18, 0.64])
            _has_cache = bool(_cached_prices)
            with _bc1:
                _do_calc = st.button(
                    "▶ 計算する", key=f"tp_calc_{_active}",
                    use_container_width=True,
                    disabled=_has_cache,
                )
            with _bc2:
                _do_recalc = st.button(
                    "🔄 再計算", key=f"tp_recalc_{_active}",
                    use_container_width=True,
                    disabled=not _has_cache,
                )
            with _bi:
                if _calculated_at:
                    st.caption(
                        f"📅 最終計算: **{_calculated_at}**　"
                        f"（{len(_cached_prices)} 銘柄 / APIコール節約のため手動更新制）"
                    )
                else:
                    st.caption("まだ計算されていません。「▶ 計算する」を押してください。")

            # ── 実際の計算（クリック時のみ EdinetDB を叩く）─────────────
            if _do_calc or _do_recalc:
                _new_prices = {}
                _prog = st.progress(0, text="理論株価を計算中...")
                for _idx, (_sc, _name, _price) in enumerate(_tp_rows):
                    _prog.progress(
                        (_idx + 1) / len(_tp_rows),
                        text=f"計算中… {_name}（{_sc}）",
                    )
                    if _price is None:
                        _new_prices[_sc] = {"error": "現在値なし", "_name": _name, "_price": None}
                    else:
                        try:
                            _r2 = _calc_theoretical_price(_sc, _price)
                        except Exception as _ex:
                            _r2 = {"error": f"計算エラー: {_ex}"}
                        _r2["_name"]  = _name
                        _r2["_price"] = _price
                        _new_prices[_sc] = _r2
                _prog.empty()
                _now_str = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
                st.session_state[_tp_ck] = {"prices": _new_prices, "calculated_at": _now_str}
                save_state(_active)   # ← JSONに永続保存
                st.rerun()

            # ── キャッシュからカード表示 ──────────────────────────────────
            if _cached_prices:
                for _sc, _name, _price in _tp_rows:
                    _res = _cached_prices.get(_sc)
                    if _res is None:
                        st.warning(f"{_name}（{_sc}）: キャッシュなし → 再計算してください")
                        continue

                    # エラー系
                    if "error" in _res:
                        with st.container(border=True):
                            st.markdown(
                                f"**{_name}**  "
                                f"<span style='color:#555;font-size:0.85em'>({_sc})</span>",
                                unsafe_allow_html=True,
                            )
                            st.warning(f"⚠️ {_res['error']}")
                        continue

                    # 正常系（値が取れなければ安全なデフォルトにフォールバック）
                    _div    = _res.get("divergence_pct")
                    _theory = _res.get("theoretical_price") or 0
                    _cur    = _res.get("_price") or _price or 0
                    try:
                        _div_f     = float(_div) if _div is not None else None
                        _div_color = "#16a34a" if (_div_f is not None and _div_f >= 0) else "#dc2626"
                        _div_label = "割安↑" if (_div_f is not None and _div_f >= 0) else "割高↓"
                        _div_txt   = f"{_div_f:+.1f}%（{_div_label}）" if _div_f is not None else "―"
                    except Exception:
                        _div_color, _div_txt = "#888", "―"

                    with st.container(border=True):
                        # ── ヘッダー行：銘柄名 / 理論株価 / 乖離率 ────────
                        _hc1, _hc2, _hc3 = st.columns([0.35, 0.28, 0.37])
                        _hc1.markdown(
                            f"**{_name}**  "
                            f"<span style='color:#555;font-size:0.85em'>({_sc})</span>",
                            unsafe_allow_html=True,
                        )
                        _hc2.markdown(
                            f"<div style='font-size:1.3em;font-weight:700;'>"
                            f"¥{_theory:,.0f}</div>",
                            unsafe_allow_html=True,
                        )
                        _hc3.markdown(
                            f"<span style='color:{_div_color};font-size:1.05em;"
                            f"font-weight:600;'>{_div_txt}</span>　"
                            f"<span style='color:#666;font-size:0.82em'>"
                            f"現在値 ¥{_cur:,.0f}</span>",
                            unsafe_allow_html=True,
                        )

                        # ── ① 資産価値の算出過程 ─────────────────────────
                        st.markdown(
                            "<div style='font-size:0.78em;color:#555;"
                            "font-weight:600;margin-top:6px;'>① 資産価値</div>",
                            unsafe_allow_html=True,
                        )
                        _av1, _av2, _av3 = st.columns(3)
                        _av1.metric(
                            "BPS（1株純資産）",
                            f"¥{_res.get('bps',0):,.0f}",
                            help=f"取得元: {_res.get('bps_src','')}",
                        )
                        _av2.metric(
                            f"割引評価率（自己資本比率 {_res.get('equity_ratio_pct',0):.1f}%）",
                            f"×{_res.get('discount_rate',0)*100:.0f}%",
                        )
                        _av3.metric(
                            "資産価値",
                            f"¥{_res.get('asset_value',0):,.0f}",
                        )

                        # ── ② 事業価値の算出過程 ─────────────────────────
                        st.markdown(
                            "<div style='font-size:0.78em;color:#555;"
                            "font-weight:600;margin-top:4px;'>② 事業価値</div>",
                            unsafe_allow_html=True,
                        )
                        _bv1, _bv2, _bv3, _bv4 = st.columns(4)
                        _bv1.metric(
                            f"ROA（上限30%→{_res.get('roa_cap_pct',0):.1f}%）×10",
                            f"= {_res.get('roa_factor',0):.2f}",
                            help=f"ROA算出: {_res.get('roa_src','')}",
                        )
                        _bv2.metric(
                            "財務レバレッジ補正",
                            f"×{_res.get('lev_corr',1):.3f}",
                            help=(
                                f"1÷min(1.0, max(0.66, {_res.get('equity_ratio_pct',0)/100:.2f}+0.33))"
                                f" = 1÷{_res.get('lev_denom',1):.3f}"
                            ),
                        )
                        _bv3.metric(
                            "理論PER",
                            f"{_res.get('riron_per',0):.1f}倍",
                            help=f"15 × {_res.get('roa_factor',0):.2f} × {_res.get('lev_corr',1):.3f}",
                        )
                        _bv4.metric(
                            "PER計算用EPS",
                            f"¥{_res.get('per_eps',0):.2f}",
                            help=_res.get("per_eps_src", ""),
                        )
                        st.caption(
                            f"　事業価値 = 理論PER {_res.get('riron_per',0):.1f}倍"
                            f" × EPS ¥{_res.get('per_eps',0):.2f}"
                            f" = **¥{_res.get('business_value',0):,.0f}**　　"
                            f"({_res.get('per_eps_src','')})"
                        )

                        # ── ③ 市場リスク判定 ──────────────────────────────
                        st.markdown(
                            "<div style='font-size:0.78em;color:#555;"
                            "font-weight:600;margin-top:4px;'>③ 市場リスク（リーマンショックルール）</div>",
                            unsafe_allow_html=True,
                        )
                        _mr1, _mr2, _mr3 = st.columns(3)
                        _pbr_v = _res.get('pbr', 0)
                        _rr    = _res.get('risk_rate', 0)
                        _mr1.metric("現在PBR", f"{_pbr_v:.2f}倍")
                        _mr2.metric(
                            "リスク減額率",
                            f"−{_rr*100:.0f}%" if _rr > 0 else "0%（適用なし）",
                        )
                        _mr3.metric(
                            "市場リスク減額",
                            f"¥{_res.get('market_risk',0):,.0f}",
                        )

                        # ── ④ 理論株価 ───────────────────────────────────
                        _base = _res.get('asset_value',0) + _res.get('business_value',0)
                        st.markdown(
                            f"<div style='margin-top:6px;padding:6px 10px;"
                            f"background:#f0f9ff;border-radius:6px;font-size:0.85em;'>"
                            f"④ 理論株価 = 資産価値 <b>¥{_res.get('asset_value',0):,.0f}</b>"
                            f" ＋ 事業価値 <b>¥{_res.get('business_value',0):,.0f}</b>"
                            f" − 市場リスク <b>¥{_res.get('market_risk',0):,.0f}</b>"
                            f" = <b style='font-size:1.15em;color:#0066cc;'>¥{_theory:,.0f}</b>"
                            f"</div>",
                            unsafe_allow_html=True,
                        )
            elif not (_do_calc or _do_recalc):
                st.info("「▶ 計算する」を押すと EdinetDB から財務データを取得して理論株価を算出します。")


def render_screener_tab() -> None:
    st.title("🔍 スクリーニング")

    # ════════════════════════════════════════════════════════════════════════
    # ① 銘柄マスタ取得（手動）
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("### 🗂️ 銘柄マスタ取得")
    st.caption(
        "「銘柄マスタ更新」ボタンを押すとJPX公式データを取得します。"
        "CSVは**自動更新されません**。"
    )

    upd_col, info_col = st.columns([0.25, 0.75])
    with upd_col:
        if st.button("🔄 銘柄マスタ更新", use_container_width=True, key="jpx_update_btn"):
            with st.spinner("JPX公式データを取得中..."):
                try:
                    master = load_jpx_data()
                    topix  = load_topix_detail()

                    master = master.merge(topix, on="code", how="left")
                    master["topix_flag"] = master["topix_flag"].fillna(False)
                    master["topix_size"] = master["topix_size"].fillna("None")

                    st.session_state.master      = master
                    st.session_state.last_update = pd.Timestamp.now()
                    # ページリロード後も保持できるようファイルに永続化
                    import pickle as _pkl
                    with open(MASTER_FILE, "wb") as _f:
                        _pkl.dump({
                            "master":      master,
                            "last_update": st.session_state.last_update,
                        }, _f)
                    st.success(f"✅ 更新完了（{len(master):,} 銘柄）")
                except Exception as e:
                    st.error(f"❌ 更新失敗: {e}")

    with info_col:
        if st.session_state.last_update is not None and st.session_state.master is not None:
            ts  = st.session_state.last_update.strftime("%Y-%m-%d %H:%M:%S")
            cnt = len(st.session_state.master)
            st.info(f"最終更新日時: **{ts}** ／ 銘柄数: **{cnt:,}**")
        else:
            st.warning("銘柄マスタは未更新です。")

    st.divider()

    # ════════════════════════════════════════════════════════════════════════
    # ② フィルター設定
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("### ⚙️ フィルター設定")

    fc1, fc2 = st.columns(2)
    with fc1:
        market_options = st.multiselect(
            "📌 市場",
            options=["プライム", "スタンダード", "グロース"],
            default=["プライム", "スタンダード", "グロース"],
            key="sc_market_options",
        )

        # 33業種区分フィルター（マスタから動的に取得）
        _all_industries = sorted(
            st.session_state.master["industry"].dropna().unique().tolist()
            if st.session_state.master is not None and "industry" in st.session_state.master.columns
            else []
        )
        industry_options = st.multiselect(
            "🏭 33業種区分",
            options=_all_industries,
            default=_all_industries,
            key="sc_industry_options",
            placeholder="全業種（指定なし）",
        )

    with fc2:
        topix_only = st.checkbox("TOPIX採用のみ", value=False, key="sc_topix_only")

        # TOPIX指数プリセットボタン
        _TOPIX_PRESETS = {
            "TOPIX100":  ["Core30", "Large70"],
            "TOPIX500":  ["Core30", "Large70", "Mid400"],
            "TOPIX1000": ["Core30", "Large70", "Mid400", "Small1"],
        }
        _btn_cols = st.columns(3)
        for _col, (_label, _sizes) in zip(_btn_cols, _TOPIX_PRESETS.items()):
            with _col:
                if st.button(_label, key=f"sc_topix_preset_{_label}", use_container_width=True):
                    st.session_state["sc_topix_size"] = _sizes

        topix_size_options = st.multiselect(
            "📊 TOPIX区分",
            options=["Core30", "Large70", "Mid400", "Small1", "Small2", "None"],
            default=["Core30", "Large70", "Mid400", "Small1", "Small2", "None"],
            key="sc_topix_size",
        )

    # フィルター適用 + 銘柄コードリスト生成
    _filtered_tickers: list[str] = []
    _filtered_count   = 0
    if st.session_state.master is not None:
        _df = st.session_state.master.copy()

        if market_options:
            _df = _df[_df["market"].str.contains("|".join(market_options), na=False)]

        # 33業種区分フィルター（全選択 or 未選択なら絞り込まない）
        if industry_options and len(industry_options) < len(_all_industries):
            if "industry" in _df.columns:
                _df = _df[_df["industry"].isin(industry_options)]

        if topix_only:
            _df = _df[_df["topix_flag"] == True]

        if topix_size_options and "topix_size" in _df.columns:
            _df = _df[_df["topix_size"].isin(topix_size_options)]

        _filtered_tickers = _df["code"].dropna().tolist()
        _filtered_count   = len(_filtered_tickers)
        st.caption(f"フィルター後銘柄数: **{_filtered_count:,}** 件")
    else:
        st.caption("銘柄マスタ未取得 — マスタ更新後にフィルターが有効になります")

    # フィルター設定の保存ボタン
    _fsave_col, _fload_info_col = st.columns([0.2, 0.8])
    with _fsave_col:
        if st.button("💾 フィルター設定を保存", key="sc_filter_save_btn", use_container_width=True):
            try:
                _filter_data = {
                    _fk: st.session_state.get(_fk)
                    for _fk in _FILTER_KEYS
                }
                with open(FILTER_FILE, "w", encoding="utf-8") as _ff:
                    json.dump(_filter_data, _ff, ensure_ascii=False)
                st.toast("✅ フィルター設定を保存しました", icon="💾")
            except Exception as _e:
                st.error(f"保存失敗: {_e}")
    with _fload_info_col:
        if os.path.exists(FILTER_FILE):
            try:
                _mtime = pd.Timestamp(os.path.getmtime(FILTER_FILE), unit="s", tz="Asia/Tokyo")
                st.caption(f"最終保存: {_mtime.strftime('%Y-%m-%d %H:%M:%S')}")
            except Exception:
                pass

    st.divider()

    # ════════════════════════════════════════════════════════════════════════
    # ③ スクリーニング実行
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("### 🔍 ブレイクアウトスクリーニング")
    st.markdown("""
    ドンチャンブレイクアウトを検知し、**時間フィルター**と**待機DDフィルター**で
    ダマシを除外した高精度スクリーニングを実行します。
    - ⏱️ **時間フィルター**: ブレイク後N日待機し、価格がまだブレイク水準以上なら採用
    - 🛡️ **待機DDフィルター**: 待機期間中の最大下落が許容範囲内の銘柄のみ採用
    - 🔥 **出来高条件**: ブレイク日の出来高がN日平均の指定倍率以上
    """)
    st.divider()

    # ────────────────────────────────────────────────────────────────────────
    # スクリーニングパラメータ設定
    # ────────────────────────────────────────────────────────────────────────
    st.markdown("### ⚙️ スクリーニングパラメータ")
    p1, p2, p3, p4 = st.columns(4)

    with p1:
        sc_donchian = st.number_input(
            "📅 ドンチャン期間（日）",
            min_value=5, max_value=60, value=20, step=1,
            help="N日高値上抜けを判定する期間",
            key="sc_donchian",
        )

    with p2:
        sc_delay = st.selectbox(
            "⏱️ 遅延日数（delay_days）",
            options=[0, 3, 5, 10, 20],
            index=2,
            format_func=lambda x: f"{x}日" if x > 0 else "即時（0日）",
            key="sc_delay",
            help="ブレイク後この日数が経過してから条件チェック",
        )

    with p3:
        sc_dd = st.selectbox(
            "🛡️ 待機DD閾値（%）",
            options=[-1.5, -2.0, -2.5, -3.0],
            index=3,
            format_func=lambda x: f"{x:.1f}%",
            key="sc_dd",
            help="待機期間中の最大下落率の下限（例: -3.0% → 3%超の下落はNG）",
        )

    with p4:
        sc_vol = st.slider(
            "🔥 出来高倍率 閾値",
            min_value=1.0, max_value=5.0, value=1.0, step=0.1,
            format="%.1f×",
            key="sc_vol",
            help="ブレイク日出来高 ÷ N日平均出来高 がこの値以上の銘柄のみ通過",
        )

    st.caption(
        f"📌 判定条件: ドンチャン{sc_donchian}日 ｜ "
        f"遅延{sc_delay}日後に価格確認 ｜ "
        f"待機DD > {sc_dd:.1f}% ｜ "
        f"出来高 ≥ {sc_vol:.1f}×"
    )

    # ────────────────────────────────────────────────────────────────────────
    # 銘柄リスト（マスタフィルター結果 / プリセット / CSV / 手動入力）
    # ────────────────────────────────────────────────────────────────────────
    st.markdown("### 📋 銘柄リスト")

    # 銘柄マスタのフィルター結果を反映ボタン
    if _filtered_count > 0:
        if st.button(
            f"📥 フィルター結果を反映（{_filtered_count:,} 銘柄）",
            key="sc_import_master",
            help="上記フィルター後の銘柄をテキストエリアに一括反映します",
        ):
            _codes = ", ".join(_filtered_tickers)
            st.session_state.sc_ticker_input = _codes
            st.session_state.sc_ticker_area  = _codes
            st.success(f"✅ {_filtered_count:,} 銘柄を反映しました")

    sc_preset_col, sc_csv_col = st.columns([0.45, 0.55])

    with sc_preset_col:
        sc_preset = st.selectbox(
            "入力方法 / プリセット",
            options=PRESET_OPTIONS,
            key="sc_preset_select",
        )

    if sc_preset != st.session_state.sc_prev_preset:
        if sc_preset == PRESET_OPTIONS[1]:
            _ps = ", ".join(TOPIX100_TICKERS)
            st.session_state.sc_ticker_input = _ps
            st.session_state.sc_ticker_area  = _ps
        elif sc_preset == PRESET_OPTIONS[2]:
            _ps = ", ".join(NIKKEI225_TICKERS)
            st.session_state.sc_ticker_input = _ps
            st.session_state.sc_ticker_area  = _ps
        st.session_state.sc_prev_preset = sc_preset

    with sc_csv_col:
        sc_uploaded = st.file_uploader(
            "📂 銘柄CSVをアップロード",
            type=["csv"],
            key="sc_csv_uploader",
            help=(
                "ticker 列（例: 7203.T）または code 列（例: 7203）を含む CSV。\n"
                "アップロードするとテキストエリアの内容を自動置換します。"
            ),
        )

    if sc_uploaded is not None:
        sc_file_id = getattr(sc_uploaded, "file_id", sc_uploaded.name)
        if st.session_state.sc_csv_file_id != sc_file_id:
            st.session_state.sc_csv_file_id = sc_file_id
            csv_tickers, csv_err = _parse_screener_csv(sc_uploaded)
            if csv_err:
                st.error(f"❌ {csv_err}")
            else:
                ticker_str = ", ".join(csv_tickers)
                st.session_state.sc_ticker_input = ticker_str
                st.session_state.sc_ticker_area  = ticker_str
                st.success(
                    f"✅ CSV 読み込み成功 — **{len(csv_tickers)} 銘柄**をテキストエリアに反映しました。"
                )

    sc_ticker_input = st.text_area(
        "銘柄コード（カンマ区切り）",
        height=80,
        key="sc_ticker_area",
        label_visibility="collapsed",
        help="4桁数字も可（7203 → 7203.T に自動変換）。マスタ反映後は手動編集も可能。",
    )
    if sc_preset == PRESET_OPTIONS[0] and sc_uploaded is None:
        st.session_state.sc_ticker_input = sc_ticker_input

    raw_list = [t.strip() for t in sc_ticker_input.split(",") if t.strip()]
    st.caption(f"銘柄数: **{len(raw_list)}** 件")

    # ────────────────────────────────────────────────────────────────────────
    # ③ 実行 / クリア ボタン
    # ────────────────────────────────────────────────────────────────────────
    run_col, clr_col, _ = st.columns([0.22, 0.10, 0.68])
    with run_col:
        run_btn = st.button(
            "🚀 スクリーニング実行", use_container_width=True,
            type="primary", key="sc_run_btn",
        )
    with clr_col:
        if st.button("クリア", use_container_width=True, key="sc_clr_btn"):
            st.session_state.screener_results = None
            st.rerun()

    # ────────────────────────────────────────────────────────────────────────
    # ④ スクリーニング実行
    # ────────────────────────────────────────────────────────────────────────
    if run_btn:
        if not raw_list:
            st.warning("銘柄コードを入力してください。")
        else:
            tickers = [normalize_ticker(t) for t in raw_list if t]
            passed:  list[dict] = []
            errors:  list[str]  = []

            prog_bar  = st.progress(0, text=f"スクリーニング開始... (0 / {len(tickers)})")
            status_ph = st.empty()

            for idx, ticker in enumerate(tickers):
                prog_bar.progress(
                    idx / len(tickers),
                    text=f"処理中... {idx + 1} / {len(tickers)} | 通過: {len(passed)} 銘柄",
                )
                status_ph.caption(f"🔍 {ticker} を分析中")

                result, err = screen_ticker(
                    ticker        = ticker,
                    donchian_days = int(sc_donchian),
                    vol_mult_thr  = float(sc_vol),
                    delay_days    = int(sc_delay),
                    dd_threshold  = float(sc_dd),
                )

                if result:
                    passed.append(result)
                elif err:
                    errors.append(f"{ticker}: {err}")

            prog_bar.progress(
                1.0,
                text=f"✅ 完了  {len(passed)} / {len(tickers)} 銘柄が条件を通過",
            )
            status_ph.empty()

            if passed:
                _master_name_map = {}
                if st.session_state.master is not None and "name" in st.session_state.master.columns:
                    _master_name_map = dict(zip(
                        st.session_state.master["code"].astype(str),
                        st.session_state.master["name"]
                    ))
                for row in passed:
                    row["銘柄名"] = _master_name_map.get(str(row["ティッカー"])) or _get_stock_name(row["ティッカー"])

            st.session_state.screener_results = (
                pd.DataFrame(passed) if passed else pd.DataFrame()
            )

            if errors:
                with st.expander(f"⚠️ スキップされた銘柄 ({len(errors)} 件)"):
                    for e in errors:
                        st.caption(e)

    # ────────────────────────────────────────────────────────────────────────
    # ⑤ 結果表示
    # ────────────────────────────────────────────────────────────────────────
    if st.session_state.screener_results is None:
        return

    results_df = st.session_state.screener_results
    st.divider()

    if results_df.empty:
        st.info(
            "🔍 条件を満たす銘柄は見つかりませんでした。"
            "遅延日数を減らすか、DD閾値・出来高倍率を緩めてみてください。"
        )
        return

    st.success(f"✅ **{len(results_df)} 銘柄**が条件を通過しました")

    # ── 一括ファンダ追加ボタン ─────────────────────────────────────────────
    _all_codes   = results_df["ティッカー"].dropna().tolist() if "ティッカー" in results_df.columns else []
    _not_added   = [c for c in _all_codes if c not in st.session_state.funda_list]
    if _not_added:
        if st.button(f"📊 全 {len(_not_added)} 銘柄を一括でファンダ一覧に追加", key="sc_bulk_funda_add"):
            with st.spinner(f"{len(_not_added)} 銘柄のデータを取得中..."):
                for _bc in _not_added:
                    _bf = get_fundamentals(_bc)
                    st.session_state.fund_df = pd.concat(
                        [st.session_state.fund_df, pd.DataFrame([_bf])],
                        ignore_index=True,
                    ).drop_duplicates(subset="code", keep="last").reset_index(drop=True)
                    if _bc not in st.session_state.funda_list:
                        st.session_state.funda_list.append(_bc)
            save_funda_data(st.session_state.fund_df)
            st.success(f"✅ {len(_not_added)} 銘柄を追加しました")
            st.rerun()
    else:
        st.info("スクリーニング結果の全銘柄が既にファンダ一覧に追加済みです。")

    col_order = [
        "ティッカー", "銘柄名", "現在価格",
        "経過日数", "ブレイク比(%)",
        "出来高倍率", "waiting_dd(%)", "delay日数",
        "ブレイク日", "ブレイク価格", "エントリー価格",
    ]
    disp_df = results_df[[c for c in col_order if c in results_df.columns]].copy()

    # ── ソートコントロール ─────────────────────────────────────────────────────
    _sort_cols, _sort_dir_col = st.columns([2, 1])
    with _sort_cols:
        _sort_key = st.radio(
            "並び替え",
            options=["経過日数", "ブレイク比(%)"],
            horizontal=True,
            key="sc_sort_key",
        )
    with _sort_dir_col:
        _sort_asc = st.radio(
            "順序",
            options=["昇順 ▲", "降順 ▼"],
            horizontal=True,
            key="sc_sort_dir",
        ) == "昇順 ▲"
    if _sort_key in disp_df.columns:
        disp_df = disp_df.sort_values(_sort_key, ascending=_sort_asc).reset_index(drop=True)

    def _dd_color(val):
        try:
            v = float(val)
            return "color: #f44336; font-weight:bold" if v < -1.5 else "color: #4caf50"
        except Exception:
            return ""

    def _pct_color(val):
        try:
            v = float(val)
            return "color: #4caf50; font-weight:bold" if v >= 0 else "color: #f44336; font-weight:bold"
        except Exception:
            return ""

    fmt_map = {
        "現在価格":      "{:,.2f}",
        "経過日数":      "{:.0f}日",
        "ブレイク比(%)": "{:+.2f}%",
        "出来高倍率":    "{:.2f}×",
        "waiting_dd(%)": "{:+.2f}%",
        "ブレイク価格":  "{:,.2f}",
        "エントリー価格":"{:,.2f}",
    }
    fmt_map = {k: v for k, v in fmt_map.items() if k in disp_df.columns}

    # ── スクリーニング結果テーブル（ファンダ追加ボタン付き）─────────────────────────
    _SC_LABELS = ["ティッカー", "銘柄名", "現在価格", "経過日数", "ブレイク比(%)",
                  "出来高倍率", "waiting_dd(%)", "delay日数", "ブレイク日", "ブレイク価格", "エントリー価格",
                  "かぶたん", "理論株価", "チャート", "G", "＋"]
    _SC_WIDTHS = [0.7, 1.2, 0.8, 0.6, 0.8, 0.7, 0.8, 0.5, 0.8, 0.8, 0.9, 0.7, 0.7, 0.6, 0.5, 0.5]
    _sc_hdr = st.columns(_SC_WIDTHS)
    for _hc, _lbl in zip(_sc_hdr, _SC_LABELS):
        _hc.markdown(f"<small><b>{_lbl}</b></small>", unsafe_allow_html=True)

    for _, _row in disp_df.iterrows():
        _rc   = st.columns(_SC_WIDTHS)
        _code = str(_row.get("ティッカー", ""))
        _in_list = _code in st.session_state.funda_list

        _rc[0].write(_row.get("ティッカー", "—"))
        _rc[1].write(_row.get("銘柄名", "—"))

        _v = _row.get("現在価格")
        _rc[2].write(f"{_v:,.2f}" if pd.notna(_v) else "—")

        _v = _row.get("経過日数")
        _rc[3].write(f"{int(_v)}日" if pd.notna(_v) else "—")

        _v = _row.get("ブレイク比(%)")
        if pd.notna(_v):
            _clr = "#4caf50" if _v >= 0 else "#f44336"
            _rc[4].markdown(
                f'<span style="color:{_clr};font-weight:bold">{_v:+.2f}%</span>',
                unsafe_allow_html=True,
            )
        else:
            _rc[4].write("—")

        _v = _row.get("出来高倍率")
        _rc[5].write(f"{_v:.2f}×" if pd.notna(_v) else "—")

        _v = _row.get("waiting_dd(%)")
        if pd.notna(_v):
            _clr = "#f44336" if _v < -1.5 else "#4caf50"
            _rc[6].markdown(
                f'<span style="color:{_clr};font-weight:bold">{_v:+.2f}%</span>',
                unsafe_allow_html=True,
            )
        else:
            _rc[6].write("—")

        _v = _row.get("delay日数")
        _rc[7].write(f"{int(_v)}" if pd.notna(_v) else "—")

        _rc[8].write(str(_row.get("ブレイク日", "—")))

        _v = _row.get("ブレイク価格")
        _rc[9].write(f"{_v:,.2f}" if pd.notna(_v) else "—")

        _v = _row.get("エントリー価格")
        _rc[10].write(f"{_v:,.2f}" if pd.notna(_v) else "—")

        # 銘柄コード（4桁）を取得
        _sc_clean = _code.replace(".T", "").replace(".t", "")

        # かぶたん
        with _rc[11]:
            st.link_button("かぶたん", f"https://kabutan.jp/stock/news?code={_sc_clean}")

        # 理論株価
        with _rc[12]:
            st.link_button("理論株価", f"https://kabubiz.com/riron/stock.php?c={_sc_clean}")

        # TradingView チャート
        with _rc[13]:
            st.link_button("📈", f"https://www.tradingview.com/chart/?symbol=TSE:{_sc_clean}")

        # Google Finance
        with _rc[14]:
            st.link_button("G", f"https://www.google.com/finance/quote/{_sc_clean}:TYO?hl=ja")

        # ファンダ追加
        with _rc[15]:
            if _in_list:
                st.success("✅", icon=None)
            else:
                if st.button("＋", key=f"funda_{_code}"):
                    with st.spinner(f"{_code} のデータを取得中..."):
                        _fdata = get_fundamentals(_code)
                    st.session_state.fund_df = pd.concat(
                        [st.session_state.fund_df, pd.DataFrame([_fdata])],
                        ignore_index=True,
                    ).drop_duplicates(subset="code", keep="last").reset_index(drop=True)
                    save_funda_data(st.session_state.fund_df)
                    if _code not in st.session_state.funda_list:
                        st.session_state.funda_list.append(_code)
                    st.rerun()

    st.download_button(
        label="📥 CSV ダウンロード",
        data=disp_df.to_csv(index=False, encoding="utf-8-sig"),
        file_name="screening_result.csv",
        mime="text/csv",
        key="sc_csv_download",
    )

    # ────────────────────────────────────────────────────────────────────────
    # ⑥ ポジションサイズ計算
    # ────────────────────────────────────────────────────────────────────────
    st.divider()
    st.markdown("### 💰 ポジションサイズ計算")
    st.caption("スクリーニング通過銘柄のポジションサイズを一括計算します。")

    ps1, ps2, ps3 = st.columns(3)
    with ps1:
        ps_capital = st.number_input(
            "💴 総資金（円）",
            min_value=100_000, max_value=1_000_000_000,
            value=1_000_000, step=100_000, format="%d",
            key="sc_ps_capital",
        )
    with ps2:
        ps_risk_pct = st.slider(
            "📊 リスク率（%）",
            min_value=0.5, max_value=3.0, value=1.0, step=0.1,
            format="%.1f%%",
            key="sc_ps_risk",
        )
    with ps3:
        ps_loss_pct = st.number_input(
            "✂️ 損切り幅（%）",
            min_value=0.5, max_value=20.0, value=5.0, step=0.5, format="%.1f",
            key="sc_ps_loss",
            help="エントリー価格からの損切り率（例: 5.0 → 5%下落で損切り）",
        )

    if "エントリー価格" in disp_df.columns:
        pos_rows = []
        for _, row in disp_df.iterrows():
            ticker      = row["ティッカー"]
            name        = row.get("銘柄名", ticker)
            entry_price = float(row["エントリー価格"])
            losscut     = entry_price * (1 - ps_loss_pct / 100)
            risk_per_sh = entry_price - losscut
            if risk_per_sh > 0:
                budget   = ps_capital * (ps_risk_pct / 100)
                shares   = int(budget / risk_per_sh)
                purchase = shares * entry_price
            else:
                shares   = 0
                purchase = 0.0

            pos_rows.append({
                "ティッカー":     ticker,
                "銘柄名":         name,
                "エントリー価格": round(entry_price, 2),
                "損切り価格":     round(losscut, 2),
                "推奨株数":       shares,
                "購入総額（円）": round(purchase, 0),
            })

        if pos_rows:
            pos_df = pd.DataFrame(pos_rows)
            st.dataframe(
                pos_df.style.format({
                    "エントリー価格": "{:,.2f}",
                    "損切り価格":     "{:,.2f}",
                    "購入総額（円）": "¥{:,.0f}",
                }),
                use_container_width=True,
                hide_index=True,
            )

    st.divider()
    st.markdown("#### ➕ ポジション計算タブへ追加")
    if st.button(
        "📋 通過銘柄を全てポジション計算タブへ追加",
        type="secondary", key="sc_add_to_pos",
    ):
        added = 0
        for _, r in results_df.iterrows():
            ticker = r["ティッカー"]
            target = next(
                (i for i in range(st.session_state.n_rows)
                 if not st.session_state.df.at[i, "銘柄コード"]),
                None,
            )
            if target is None:
                add_row()
                target = st.session_state.n_rows - 1
            st.session_state.df.at[target, "銘柄コード"]   = ticker
            st.session_state.prev_tickers[target] = "__FORCE_FETCH__"
            added += 1
        st.toast(f"{added} 銘柄を追加しました。ポジション計算タブをご確認ください。", icon="✅")
        st.rerun()


# ===========================================================================
# ファンダ分析: データ取得 / 保存 / 読み込み
# ===========================================================================

@st.cache_data(ttl=3600, show_spinner=False)
def get_fundamentals(code: str) -> dict:
    """yfinance から現在価格・前日比を取得する（1hキャッシュ）"""
    try:
        info = yf.Ticker(code).info
        current_price = info.get("currentPrice") or info.get("regularMarketPrice")
        prev_close    = info.get("regularMarketPreviousClose")
        if current_price and prev_close and prev_close != 0:
            day_change_pct = (current_price - prev_close) / prev_close * 100
        else:
            day_change_pct = None
        return {
            "code":           code,
            "current_price":  current_price,
            "day_change_pct": day_change_pct,
        }
    except Exception as e:
        return {"code": code, "error": str(e)}


# ===========================================================================
# EdinetDB MCP ヘルパー
# ===========================================================================

def _call_edinetdb(tool_name: str, **kwargs) -> dict:
    """EdinetDB MCP サーバーにツール呼び出しを送信し、結果を dict で返す。"""
    try:
        resp = _req.post(
            EDINETDB_MCP_URL,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {EDINETDB_API_KEY}",
            },
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": tool_name, "arguments": kwargs},
            },
            timeout=20,
        )
        text = resp.json()["result"]["content"][0]["text"]
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {"error": text}
    except Exception as e:
        return {"error": str(e)}


@st.cache_data(ttl=86400, show_spinner=False)   # 1日キャッシュ
def edinet_get_edinet_code(sec_code: str) -> str | None:
    """証券コード(4桁) → EdinetCode を検索して返す。見つからなければ None。"""
    result = _call_edinetdb("search_companies", query=sec_code)
    companies = result.get("companies", [])
    if not companies:
        return None
    # secCode は "72030" のように末尾0付きで返るので前方一致で照合
    for c in companies:
        if str(c.get("secCode", "")).startswith(sec_code):
            return c["edinetCode"]
    return companies[0]["edinetCode"]


@st.cache_data(ttl=3600, show_spinner=False)    # 1時間キャッシュ
def edinet_get_company(sec_code: str) -> dict:
    """証券コード(4桁) → EdinetDB get_company の結果を返す。"""
    edinet_code = edinet_get_edinet_code(sec_code)
    if not edinet_code:
        return {"error": f"EDINETコードが見つかりません: {sec_code}"}
    return _call_edinetdb("get_company", edinet_code=edinet_code)


@st.cache_data(ttl=3600, show_spinner=False)
def edinet_get_shareholders(sec_code: str) -> dict:
    """証券コード(4桁) → EdinetDB get_shareholders の結果を返す。"""
    edinet_code = edinet_get_edinet_code(sec_code)
    if not edinet_code:
        return {"error": f"EDINETコードが見つかりません: {sec_code}"}
    return _call_edinetdb("get_shareholders", edinet_code=edinet_code)


# ===========================================================================
# J-Quants API ヘルパー（理論株価計算の主データソース）
# ===========================================================================

@st.cache_data(ttl=82800, show_spinner=False)   # 23時間キャッシュ（IDトークンは24時間有効）
def _jquants_get_id_token() -> str:
    """リフレッシュトークンから J-Quants IDトークンを取得する。"""
    if not JQUANTS_REFRESH_TOKEN:
        return ""
    try:
        resp = _req.post(
            JQUANTS_AUTH_URL,
            params={"refreshtoken": JQUANTS_REFRESH_TOKEN},
            timeout=10,
        )
        return resp.json().get("idToken", "")
    except Exception:
        return ""


@st.cache_data(ttl=3600, show_spinner=False)    # 1時間キャッシュ
def jquants_get_statements(code: str) -> dict:
    """
    証券コード(4桁) → J-Quants fins/statements の最新データを返す。

    返却フィールド（すべて円・株数ベースに正規化済み）:
      equity          : 純資産（円）
      totalAssets     : 総資産（円）
      forecastOrdinaryProfit : 予想経常利益（円）
      forecastNetIncome      : 予想純利益（円）
      sharesOutstanding      : 発行済株式数（自己株式除く）
      equityRatio            : 自己資本比率（小数）
      bps             : 純資産÷発行済株式数（円）
      roa_forecast    : 予想純利益÷総資産（小数）
    """
    id_token = _jquants_get_id_token()
    if not id_token:
        return {"error": "IDトークン取得失敗（リフレッシュトークンを確認してください）"}
    try:
        resp = _req.get(
            JQUANTS_FINS_URL,
            params={"code": code},
            headers={"Authorization": f"Bearer {id_token}"},
            timeout=15,
        )
        data = resp.json()
    except Exception as e:
        return {"error": str(e)}

    statements = data.get("statements", [])
    if not statements:
        return {"error": f"データなし (code={code})"}

    # 最新エントリ（リストの先頭）
    s = statements[0]

    def _f(key):
        """文字列→float、空文字・None は None を返す。"""
        v = s.get(key)
        if v in (None, "", "-", "—", "N/A"):
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    # ── 発行済株式数（自己株式除く）─────────────────────────────────────
    issued   = _f("NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock")
    treasury = _f("NumberOfTreasuryStockAtTheEndOfFiscalYear") or 0.0
    shares   = (issued - treasury) if issued else None

    # ── 財務数値（J-Quants は百万円単位で返す）────────────────────────────
    _M = 1_000_000   # 百万円 → 円
    equity_m    = _f("Equity")                   # 百万円
    ta_m        = _f("TotalAssets")              # 百万円
    fo_oi_m     = _f("ForecastOrdinaryProfit")   # 百万円
    fo_ni_m     = _f("ForecastProfit")           # 百万円

    equity   = equity_m  * _M if equity_m  is not None else None
    ta       = ta_m      * _M if ta_m      is not None else None
    fo_oi    = fo_oi_m   * _M if fo_oi_m   is not None else None
    fo_ni    = fo_ni_m   * _M if fo_ni_m   is not None else None

    # ── 自己資本比率（パーセント or 小数どちらでも対応）─────────────────
    eq_ratio_raw = _f("EquityToAssetRatio")
    if eq_ratio_raw is not None:
        eq_ratio = eq_ratio_raw / 100 if eq_ratio_raw > 1 else eq_ratio_raw
    elif equity and ta and ta > 0:
        eq_ratio = equity / ta
    else:
        eq_ratio = None

    # ── BPS = 純資産 ÷ 発行済株式数 ─────────────────────────────────────
    bps = None
    if equity and shares and shares > 0:
        bps = equity / shares

    # ── ROA = 予想純利益 ÷ 総資産 ────────────────────────────────────────
    roa_forecast = None
    if fo_ni and ta and ta > 0:
        roa_forecast = fo_ni / ta

    return {
        "equity":                 equity,
        "totalAssets":            ta,
        "forecastOrdinaryProfit": fo_oi,
        "forecastNetIncome":      fo_ni,
        "sharesOutstanding":      shares,
        "equityRatio":            eq_ratio,
        "bps":                    bps,
        "roa_forecast":           roa_forecast,
        # デバッグ用
        "_equity_m":   equity_m,
        "_ta_m":       ta_m,
        "_fo_oi_m":    fo_oi_m,
        "_fo_ni_m":    fo_ni_m,
        "_shares_raw": issued,
        "_treasury":   treasury,
        "_typeOfDoc":  s.get("TypeOfDocument", ""),
        "_period":     s.get("CurrentFiscalYearEndDate", ""),
    }


@st.cache_data(ttl=86400, show_spinner=False)   # 1日キャッシュ
def edinet_get_financials(sec_code: str) -> dict:
    """証券コード(4桁) → EdinetDB get_financials 最新年度データを返す（単位:円）。

    get_financials のレスポンス形式が複数あるため、柔軟に対応する:
      - JSON配列 [{"fiscalYear":2025, ...}, ...]  → 先頭要素を返す
      - {"financials": [...], ...}                → .financials の先頭を返す
      - {"fiscalYear":2025, ...}  (フラット)      → そのまま返す
    """
    edinet_code = edinet_get_edinet_code(sec_code)
    if not edinet_code:
        return {}
    try:
        result = _call_edinetdb("get_financials", edinet_code=edinet_code, years=1)
    except Exception:
        return {}

    # ── JSON配列として返ってきた場合 ─────────────────────────────────────
    if isinstance(result, list):
        return result[0] if result else {}

    # ── dict でなければ諦める ─────────────────────────────────────────────
    if not isinstance(result, dict):
        return {}

    # ── エラー dict ──────────────────────────────────────────────────────
    if "error" in result:
        return {}

    # ── {"financials": [...]} 形式 ────────────────────────────────────────
    for _key in ("financials", "data", "items", "results"):
        _items = result.get(_key)
        if isinstance(_items, list) and _items:
            return _items[0]

    # ── フラットな dict（bps や fiscalYear などを直接持つ）────────────────
    if any(k in result for k in ("fiscalYear", "bps", "totalAssets", "netAssets")):
        return result

    return {}


@st.cache_data(ttl=3600, show_spinner=False)    # 1時間キャッシュ
def edinet_get_latest_earnings(sec_code: str) -> dict:
    """証券コード(4桁) → 予想経常利益を含む直近の決算短信データを返す（単位:百万円）。"""
    edinet_code = edinet_get_edinet_code(sec_code)
    if not edinet_code:
        return {}
    try:
        result = _call_edinetdb("get_earnings", edinet_code=edinet_code, limit=4)
    except Exception:
        return {}

    # レスポンス形式を柔軟に処理
    if isinstance(result, list):
        earnings = result
    elif isinstance(result, dict):
        earnings = result.get("earnings") or result.get("data") or []
        if not isinstance(earnings, list):
            earnings = []
    else:
        earnings = []

    # forecastOrdinaryIncome を含む最新エントリを優先
    for e in earnings:
        if isinstance(e, dict) and e.get("forecastOrdinaryIncome") is not None:
            return e
    return earnings[0] if earnings else {}


# ---------------------------------------------------------------------------
# 理論株価計算
# ---------------------------------------------------------------------------
def _calc_theoretical_price(sec_code: str, current_price: float) -> dict:
    """
    EdinetDB の財務データから理論株価を算出する。

    理論株価 ＝ (資産価値 ＋ 事業価値) － 市場リスク

    ▸ 資産価値  = BPS × 割引評価率（自己資本比率に応じた段階）
    ▸ 事業価値  = 理論PER × PER計算用EPS
       理論PER         = 15 × (ROA上限30%)×10 × 財務レバレッジ補正
       PER計算用EPS    = 予想経常利益 × 0.7 ÷ 発行済株式数
       財務レバレッジ補正 = 1 ÷ min(1.0, max(0.66, 自己資本比率+0.33))
    ▸ 市場リスク = (資産価値+事業価値) × リーマンショックルール補正率
                   PBR ≥ 0.5 なら 0%（減額なし）

    データソース優先順位（J-Quants優先 → EdinetDB fallback）:
      BPS         : JQ.純資産÷発行済株式数 → EDB.netAssets/shares → EDB.bps → 現在値÷PBR
      自己資本比率 : JQ.equityRatio → EDB.equityRatioOfficial → EDB.equityRatio
      ROA         : JQ.roa_forecast → EDB.forecastNetIncome/totalAssets → ROE×自己資本比率
      予想経常利益 : JQ.forecastOrdinaryProfit → EDB.forecastOrdinaryIncome
      発行済株式数 : JQ.sharesOutstanding → EDB.sharesOutstanding/sharesIssued
      PBR         : 現在株価÷BPS（自前算出）→ EDB.pbr
    """
    # ── データ取得 ─────────────────────────────────────────────────────────
    try:
        edb = edinet_get_company(sec_code)
    except Exception as _e:
        return {"error": f"get_company 失敗: {_e}"}
    if not isinstance(edb, dict) or "error" in edb:
        return {"error": edb.get("error", "get_company 失敗") if isinstance(edb, dict) else "get_company 失敗"}

    try:
        jq = jquants_get_statements(sec_code)          # J-Quants（主ソース）
    except Exception:
        jq = {}
    try:
        fin = edinet_get_financials(sec_code)           # EdinetDB 財務諸表（fallback）
    except Exception:
        fin = {}
    try:
        earn = edinet_get_latest_earnings(sec_code)     # EdinetDB 決算短信（fallback）
    except Exception:
        earn = {}

    if not isinstance(jq,   dict): jq   = {}
    if not isinstance(fin,  dict): fin  = {}
    if not isinstance(earn, dict): earn = {}
    # J-Quants がエラーの場合は空に
    if "error" in jq and len(jq) == 1: jq = {}

    # ════════════════════════════════════════════════════════════════════════
    # BPS = 純資産 ÷ 発行済株式数
    # ════════════════════════════════════════════════════════════════════════
    bps     = None
    bps_src = ""

    # ① J-Quants: 純資産(円) ÷ 発行済株式数
    if jq.get("bps") and jq["bps"] > 0:
        bps     = jq["bps"]
        _eq_m   = jq.get("_equity_m")
        _sh     = jq.get("sharesOutstanding")
        bps_src = (
            f"純資産({_eq_m:,.0f}百万円)÷発行済株式数({_sh:,.0f}株) [J-Quants]"
            if _eq_m and _sh else "J-Quants"
        )
    else:
        # ② EdinetDB fin.netAssets ÷ earn.sharesOutstanding
        _na_fin  = fin.get("netAssets")                           # 円
        _na_earn = earn.get("netAssets") or earn.get("ownersEquity")
        _net_assets = _na_fin or (_na_earn * 1_000_000 if _na_earn else None)
        _shares_bps = earn.get("sharesOutstanding") or fin.get("sharesIssued")
        if _net_assets and _net_assets > 0 and _shares_bps and _shares_bps > 0:
            bps     = _net_assets / _shares_bps
            bps_src = f"純資産({_net_assets/1e8:.1f}億円)÷株式数({_shares_bps:,}株) [EdinetDB]"
        elif fin.get("bps") and fin["bps"] > 0:
            bps     = fin["bps"]
            bps_src = "EdinetDB get_financials.bps"
        else:
            pbr_v = fin.get("pbr") or edb.get("priceToBook") or edb.get("pbr")
            if pbr_v and pbr_v > 0 and current_price > 0:
                bps     = current_price / pbr_v
                bps_src = "現在株価÷PBR（推定）"
            else:
                eps_v = edb.get("eps")
                roe_v = fin.get("roeOfficial") or edb.get("roe")
                if eps_v and roe_v and roe_v > 0:
                    bps     = eps_v / roe_v
                    bps_src = "EPS÷ROE（推定）"

    # ════════════════════════════════════════════════════════════════════════
    # 自己資本比率（小数）
    # ════════════════════════════════════════════════════════════════════════
    equity_ratio = None
    eq_src       = ""

    if jq.get("equityRatio") is not None:
        equity_ratio = jq["equityRatio"]
        eq_src = "J-Quants"
    elif fin.get("equityRatioOfficial") is not None:
        equity_ratio = fin["equityRatioOfficial"]
        eq_src = "EdinetDB get_financials"
    else:
        eq_raw = earn.get("equityRatio")
        if eq_raw is not None:
            equity_ratio = eq_raw / 100 if eq_raw > 1 else eq_raw
            eq_src = "EdinetDB get_earnings"
        elif edb.get("equityRatio") is not None:
            equity_ratio = edb["equityRatio"]
            eq_src = "EdinetDB get_company"

    # ════════════════════════════════════════════════════════════════════════
    # ROA = 予想純利益 ÷ 総資産
    # ════════════════════════════════════════════════════════════════════════
    roa     = None
    roa_src = ""

    # ① J-Quants（予想純利益÷総資産、同一単位なので比率そのまま）
    if jq.get("roa_forecast") is not None:
        roa     = jq["roa_forecast"]
        _fni_m  = jq.get("_fo_ni_m")
        _ta_m   = jq.get("_ta_m")
        roa_src = (
            f"予想純利益({_fni_m:,.0f}百万円)÷総資産({_ta_m:,.0f}百万円) [J-Quants]"
            if _fni_m and _ta_m else "J-Quants"
        )
    else:
        # ② EdinetDB: forecastNetIncome(百万円) ÷ totalAssets(円 or 百万円)
        _fni_edb = earn.get("forecastNetIncome")
        _ta_fin  = fin.get("totalAssets")
        _ta_earn = earn.get("totalAssets")
        if _fni_edb and _ta_fin and _ta_fin > 0:
            roa     = (_fni_edb * 1_000_000) / _ta_fin
            roa_src = f"予想純利益({_fni_edb:,}百万円)÷総資産({_ta_fin/1e8:.1f}億円) [EdinetDB]"
        elif _fni_edb and _ta_earn and _ta_earn > 0:
            roa     = _fni_edb / _ta_earn
            roa_src = f"予想純利益({_fni_edb:,}百万円)÷総資産({_ta_earn:,}百万円) [EdinetDB]"
        elif fin.get("netIncome") and _ta_fin and _ta_fin > 0:
            _ni_act = fin["netIncome"]
            roa     = _ni_act / _ta_fin
            roa_src = f"実績純利益({_ni_act/1e8:.1f}億円)÷総資産({_ta_fin/1e8:.1f}億円)【予想なし】"
        else:
            roe_v = fin.get("roeOfficial") or edb.get("roe")
            if roe_v and equity_ratio and equity_ratio > 0:
                roa     = roe_v * equity_ratio
                roa_src = "ROE×自己資本比率（推定）"

    # ════════════════════════════════════════════════════════════════════════
    # PER計算用EPS = 予想経常利益 × 0.7 ÷ 発行済株式数
    # ════════════════════════════════════════════════════════════════════════
    per_eps     = None
    per_eps_src = ""

    # ① J-Quants: forecastOrdinaryProfit(円) ÷ sharesOutstanding
    _jq_foi    = jq.get("forecastOrdinaryProfit")   # 円
    _jq_shares = jq.get("sharesOutstanding")
    if _jq_foi and _jq_shares and _jq_shares > 0:
        per_eps     = _jq_foi * 0.7 / _jq_shares
        _jq_foi_m   = jq.get("_fo_oi_m")
        per_eps_src = (
            f"予想経常利益({_jq_foi_m:,.0f}百万円)×0.7÷株式数({_jq_shares:,.0f}株) [J-Quants]"
            if _jq_foi_m else "J-Quants"
        )
    else:
        # ② EdinetDB: forecastOrdinaryIncome(百万円) ÷ shares
        _edb_foi    = earn.get("forecastOrdinaryIncome")   # 百万円
        _edb_shares = earn.get("sharesOutstanding") or fin.get("sharesIssued")
        if _edb_foi is not None and _edb_shares and _edb_shares > 0:
            per_eps     = _edb_foi * 1_000_000 * 0.7 / _edb_shares
            per_eps_src = (
                f"予想経常利益({_edb_foi:,}百万円)×0.7"
                f"÷株式数({_edb_shares:,}株) [EdinetDB]"
            )
        elif earn.get("forecastNetIncome") and _edb_shares and _edb_shares > 0:
            _fni2 = earn["forecastNetIncome"]
            per_eps     = _fni2 * 1_000_000 / _edb_shares
            per_eps_src = f"予想純利益({_fni2:,}百万円)÷株式数（経常利益代用）"
        elif earn.get("forecastEps"):
            per_eps     = earn["forecastEps"]
            per_eps_src = "予想EPS（直接取得）"
        elif edb.get("eps"):
            per_eps     = edb["eps"]
            per_eps_src = "実績EPS（フォールバック）"

    # ════════════════════════════════════════════════════════════════════════
    # PBR = 現在株価 ÷ BPS（自前算出優先）
    # ════════════════════════════════════════════════════════════════════════
    pbr = None
    if bps and bps > 0 and current_price > 0:
        pbr = current_price / bps
    if not pbr:
        pbr = fin.get("pbr") or edb.get("priceToBook") or edb.get("pbr")

    # ── バリデーション ─────────────────────────────────────────────────────
    missing = []
    if bps is None or bps <= 0:            missing.append("BPS")
    if equity_ratio is None or equity_ratio <= 0: missing.append("自己資本比率")
    if roa is None:                        missing.append("ROA")
    if per_eps is None:                    missing.append("PER計算用EPS")
    if pbr is None or pbr <= 0:            missing.append("PBR")
    if missing:
        return {"error": f"データ不足: {', '.join(missing)}"}

    # ════════════════════════════════════════════════════════════════════════
    # 1. 資産価値 = BPS × 割引評価率
    # ════════════════════════════════════════════════════════════════════════
    eq_pct = equity_ratio * 100
    if eq_pct >= 80:
        discount = 0.80
    elif eq_pct >= 67:
        discount = 0.75
    elif eq_pct >= 50:
        discount = 0.70
    elif eq_pct >= 33:
        discount = 0.65
    elif eq_pct >= 10:
        discount = 0.60
    else:
        discount = 0.50
    asset_value = bps * discount

    # ════════════════════════════════════════════════════════════════════════
    # 2. 事業価値 = 理論PER × PER計算用EPS
    #
    #   理論PER = 15 × (ROA上限30%)×10 × 財務レバレッジ補正
    #   財務レバレッジ補正 = 1 ÷ min(1.0, max(0.66, 自己資本比率+0.33))
    #     目安: 自己資本比率67%以上(lev≤1.5) → 1.0倍
    #           自己資本比率33%〜67%(lev 1.5〜3) → 1.0〜1.5倍
    #           自己資本比率33%未満(lev>3)       → 1.515倍
    # ════════════════════════════════════════════════════════════════════════
    roa_cap    = min(roa, 0.30)                               # ROA上限 30%
    roa_factor = roa_cap * 10                                 # ×10正規化

    lev_denom  = min(1.0, max(0.66, equity_ratio + 0.33))    # 補正分母
    lev_corr   = 1.0 / lev_denom                             # 財務レバレッジ補正

    riron_per      = 15.0 * roa_factor * lev_corr            # 理論PER
    business_value = riron_per * per_eps                     # 事業価値

    # ════════════════════════════════════════════════════════════════════════
    # 3. 市場リスク（リーマンショックルール）
    #    PBR ≥ 0.5 なら適用なし
    # ════════════════════════════════════════════════════════════════════════
    if pbr >= 0.50:
        risk_rate = 0.00
    elif pbr >= 0.41:
        risk_rate = 0.20
    elif pbr >= 0.34:
        risk_rate = 0.33
    elif pbr >= 0.25:
        risk_rate = 0.50
    elif pbr >= 0.17:
        risk_rate = 0.66
    else:
        risk_rate = 0.80

    base_value  = asset_value + business_value
    market_risk = base_value * risk_rate

    # ════════════════════════════════════════════════════════════════════════
    # 4. 理論株価
    # ════════════════════════════════════════════════════════════════════════
    theoretical = base_value - market_risk
    divergence  = (theoretical - current_price) / current_price * 100 if current_price else None

    return {
        # 最終結果
        "theoretical_price": round(theoretical, 0),
        "divergence_pct":    round(divergence, 1) if divergence is not None else None,
        # 内訳
        "asset_value":       round(asset_value, 0),
        "business_value":    round(business_value, 0),
        "market_risk":       round(market_risk, 0),
        "risk_rate":         risk_rate,
        # 資産価値の根拠
        "bps":               round(bps, 0),
        "bps_src":           bps_src,
        "discount_rate":     discount,
        "equity_ratio_pct":  round(eq_pct, 1),
        "eq_src":            eq_src,
        # 事業価値の根拠
        "riron_per":         round(riron_per, 1),
        "roa_pct":           round(roa * 100, 1),
        "roa_cap_pct":       round(roa_cap * 100, 1),
        "roa_factor":        round(roa_factor, 2),
        "roa_src":           roa_src,
        "lev_denom":         round(lev_denom, 3),
        "lev_corr":          round(lev_corr, 3),
        "per_eps":           round(per_eps, 2),
        "per_eps_src":       per_eps_src,
        "forecast_oi_m":     jq.get("_fo_oi_m"),   # 百万円（Noneの場合あり）
        "shares":            jq.get("sharesOutstanding"),
        # 市場リスクの根拠
        "pbr":               pbr,
    }


def _fmt_oku(value) -> str:
    """円単位の数値を億円表示にフォーマット。"""
    try:
        v = float(value)
        return f"{v / 1e8:,.0f} 億円"
    except Exception:
        return "—"


def _health_badge(score) -> str:
    """健全性スコアを色付きバッジ HTML で返す。"""
    try:
        s = int(score)
    except Exception:
        return "—"
    if s >= 75:
        color, label = "#4caf50", "優良"
    elif s >= 50:
        color, label = "#ff9800", "良好"
    elif s >= 25:
        color, label = "#f44336", "注意"
    else:
        color, label = "#9e9e9e", "要検討"
    bar = "█" * (s // 10) + "░" * (10 - s // 10)
    return (
        f'<span style="color:{color};font-weight:bold">{s}/100</span> '
        f'<span style="color:{color};font-size:0.75em">{bar} {label}</span>'
    )


# ===========================================================================
# AI銘柄分析（Claude連携：プロンプト生成 → 外部貼り付け → 結果表示）
# ===========================================================================

_PROMPTS_DIR        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prompts")
_ANALYSIS_RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "analysis_results")



def load_prompt_templates() -> dict[str, str]:
    """prompts/ ディレクトリの .md ファイルを {ファイル名: 内容} で返す（_current.md 除外）。"""
    templates: dict[str, str] = {}
    if not os.path.isdir(_PROMPTS_DIR):
        return templates
    for fname in sorted(os.listdir(_PROMPTS_DIR)):
        if fname.endswith(".md") and fname not in ("README.md", "_current.md"):
            path = os.path.join(_PROMPTS_DIR, fname)
            try:
                with open(path, encoding="utf-8") as f:
                    templates[fname[:-3]] = f.read()   # key = ファイル名（拡張子なし）
            except Exception:
                pass
    return templates


def save_current_prompt(prompt_text: str) -> str:
    """プロンプトを prompts/_current.md に保存して保存先パスを返す。"""
    os.makedirs(_PROMPTS_DIR, exist_ok=True)
    path = os.path.join(_PROMPTS_DIR, "_current.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(prompt_text)
    return path


def list_analysis_results() -> list[str]:
    """analysis_results/ 内の .md ファイルを新しい順で返す。"""
    if not os.path.isdir(_ANALYSIS_RESULTS_DIR):
        return []
    files = [
        f for f in os.listdir(_ANALYSIS_RESULTS_DIR)
        if f.endswith(".md")
    ]
    files.sort(key=lambda f: os.path.getmtime(
        os.path.join(_ANALYSIS_RESULTS_DIR, f)
    ), reverse=True)
    return files


def load_analysis_result(filename: str) -> str:
    """analysis_results/{filename} の内容を返す。"""
    path = os.path.join(_ANALYSIS_RESULTS_DIR, filename)
    try:
        with open(path, encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


def save_funda_data(df: pd.DataFrame) -> None:
    """ファンダデータを CSV に保存する"""
    df.to_csv(FUNDA_FILE, index=False, encoding="utf-8-sig")


def load_funda_data() -> pd.DataFrame:
    """CSV からファンダデータを読み込む。ファイルがなければ空 DataFrame を返す"""
    try:
        return pd.read_csv(FUNDA_FILE, encoding="utf-8-sig", dtype=str)
    except Exception:
        return pd.DataFrame()


def save_memo_data(memos: dict) -> None:
    """メモデータを JSON に保存する"""
    with open(MEMO_FILE, "w", encoding="utf-8") as _mf:
        json.dump(memos, _mf, ensure_ascii=False, indent=2)


def load_memo_data() -> dict:
    """JSON からメモデータを読み込む"""
    try:
        with open(MEMO_FILE, encoding="utf-8") as _mf:
            return json.load(_mf)
    except Exception:
        return {}


# ===========================================================================
# ファンダ分析タブ
# ===========================================================================

def render_funda_tab() -> None:
    st.title("👀 監視銘柄")
    st.caption("銘柄を追加すると現在価格・前日比を自動取得します。データは CSV に自動保存されます。")

    # ════════════════════════════════════════════════════════════════════════
    # ① スクリーニング結果から追加
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("### 🔍 スクリーニング結果から追加")

    _sc_res = st.session_state.get("screener_results")
    _sc_codes: list[str] = []
    if _sc_res is not None and not _sc_res.empty and "ティッカー" in _sc_res.columns:
        _sc_codes = _sc_res["ティッカー"].dropna().unique().tolist()

    if _sc_codes:
        selected_codes = st.multiselect(
            "追加する銘柄を選択",
            options=_sc_codes,
            default=[c for c in st.session_state.funda_list if c in _sc_codes],
            key="funda_sc_select",
        )
        if st.button("➕ 追加実行", key="funda_sc_add_btn", type="primary"):
            if selected_codes:
                with st.spinner(f"{len(selected_codes)} 銘柄のファンダデータを取得中..."):
                    new_rows = []
                    for _c in selected_codes:
                        new_rows.append(get_fundamentals(_c))
                df_new = pd.DataFrame(new_rows)
                st.session_state.fund_df = pd.concat(
                    [st.session_state.fund_df, df_new], ignore_index=True
                ).drop_duplicates(subset="code", keep="last").reset_index(drop=True)
                save_funda_data(st.session_state.fund_df)
                # funda_list も同期
                for _c in selected_codes:
                    if _c not in st.session_state.funda_list:
                        st.session_state.funda_list.append(_c)
                st.success(f"✅ {len(selected_codes)} 銘柄を追加しました")
                st.rerun()
            else:
                st.warning("銘柄を選択してください。")
    else:
        st.info("スクリーニングタブでスクリーニングを実行すると、ここに結果が表示されます。")

    st.divider()

    # ════════════════════════════════════════════════════════════════════════
    # ② 手動追加
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("### ✍️ 手動追加")

    with st.form("funda_manual_form", clear_on_submit=True):
        _m_col1, _m_col2 = st.columns([0.6, 0.4])
        with _m_col1:
            manual_code = st.text_input(
                "銘柄コード（例: 7203.T）",
                key="funda_manual_input",
                placeholder="7203.T",
            )
        with _m_col2:
            st.write("")
            st.write("")
            _manual_submitted = st.form_submit_button("➕ 追加", use_container_width=True)
        if _manual_submitted:
            _code = normalize_ticker(manual_code.strip())
            if _code:
                with st.spinner(f"{_code} のデータを取得中..."):
                    _data = get_fundamentals(_code)
                _df_new = pd.DataFrame([_data])
                st.session_state.fund_df = pd.concat(
                    [st.session_state.fund_df, _df_new], ignore_index=True
                ).drop_duplicates(subset="code", keep="last").reset_index(drop=True)
                save_funda_data(st.session_state.fund_df)
                if _code not in st.session_state.funda_list:
                    st.session_state.funda_list.append(_code)
                st.success(f"✅ {_code} を追加しました")
                st.rerun()
            else:
                st.warning("銘柄コードを入力してください。")

    st.divider()

    # ════════════════════════════════════════════════════════════════════════
    # ③ ファンダ一覧表示
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("### 📋 銘柄リスト")

    fund_df = st.session_state.fund_df

    if fund_df.empty:
        st.info("まだ銘柄が登録されていません。上のセクションから追加してください。")
        return

    # 表示用に列名を日本語化
    _COL_RENAME = {
        "code":           "コード",
        "current_price":  "現在価格",
        "day_change_pct": "前日比(%)",
        "error":          "エラー",
    }
    disp_df = fund_df.rename(columns={k: v for k, v in _COL_RENAME.items() if k in fund_df.columns}).copy()

    # 数値列を numeric に変換
    for _col in ["現在価格", "前日比(%)"]:
        if _col in disp_df.columns:
            disp_df[_col] = pd.to_numeric(disp_df[_col], errors="coerce")

    # マスタから銘柄名マップを作成（日本語名）
    _funda_name_map = {}
    if st.session_state.master is not None and "name" in st.session_state.master.columns:
        _funda_name_map = dict(zip(
            st.session_state.master["code"].astype(str),
            st.session_state.master["name"]
        ))

    # ── テーブルヘッダー ─────────────────────────────────────────────────
    _FUNDA_LABELS = ["コード", "銘柄名", "現在価格", "前日比(%)",
                     "かぶたん", "理論株価", "チャート", "G", "X", "Yahoo", "メモ", "削除"]
    _FUNDA_WIDTHS = [0.8, 1.7, 1.0, 0.9, 0.8, 0.8, 0.6, 0.5, 0.5, 0.6, 0.6, 0.6]
    _hdr_cs = st.columns(_FUNDA_WIDTHS)
    for _hc, _lbl in zip(_hdr_cs, _FUNDA_LABELS):
        _hc.markdown(f"<small><b>{_lbl}</b></small>", unsafe_allow_html=True)

    for _, _row in disp_df.iterrows():
        _rc = st.columns(_FUNDA_WIDTHS)

        # コード・銘柄名
        _code_raw     = str(_row.get("コード", ""))
        _ticker_clean = _code_raw.replace(".T", "").replace(".t", "")
        _jp_name      = _funda_name_map.get(_ticker_clean) or _funda_name_map.get(_code_raw) or "—"
        _rc[0].write(_code_raw)
        _rc[1].write(_jp_name)

        # 現在価格
        _v = _row.get("現在価格")
        _rc[2].write(f"{float(_v):,.2f}" if pd.notna(_v) else "—")

        # 前日比(%)（正負カラー）
        _v = _row.get("前日比(%)")
        if pd.notna(_v):
            _fv = float(_v)
            _clr = "#4caf50" if _fv >= 0 else "#f44336"
            _rc[3].markdown(
                f'<span style="color:{_clr};font-weight:bold">{_fv:+.2f}%</span>',
                unsafe_allow_html=True,
            )
        else:
            _rc[3].write("—")

        # かぶたん
        with _rc[4]:
            st.link_button("かぶたん", f"https://kabutan.jp/stock/news?code={_ticker_clean}")

        # 理論株価
        with _rc[5]:
            st.link_button("理論株価", f"https://kabubiz.com/riron/stock.php?c={_ticker_clean}")

        # TradingView チャート
        with _rc[6]:
            st.link_button("📈", f"https://www.tradingview.com/chart/?symbol=TSE:{_ticker_clean}")

        # Google Finance
        with _rc[7]:
            st.link_button("G", f"https://www.google.com/finance/quote/{_ticker_clean}:TYO?hl=ja")

        # X（旧Twitter）検索
        with _rc[8]:
            import urllib.parse as _up
            _x_name = _up.quote(_jp_name) if _jp_name != "—" else _up.quote(_ticker_clean)
            st.link_button("𝕏", f"https://x.com/search?q={_x_name}&src=typed_query&f=top")

        # Yahoo ファイナンス
        with _rc[9]:
            st.link_button("Yahoo", f"https://finance.yahoo.co.jp/quote/{_ticker_clean}")

        # メモ開閉ボタン
        with _rc[10]:
            _memo_key = f"memo_open_{_code_raw}"
            if st.button("📝", key=f"memo_btn_{_code_raw}"):
                st.session_state[_memo_key] = not st.session_state.get(_memo_key, False)

        # 削除ボタン
        with _rc[11]:
            if st.button("🗑️", key=f"del_funda_{_code_raw}"):
                st.session_state.fund_df = st.session_state.fund_df[
                    st.session_state.fund_df["code"] != _code_raw
                ].reset_index(drop=True)
                save_funda_data(st.session_state.fund_df)
                if _code_raw in st.session_state.funda_list:
                    st.session_state.funda_list.remove(_code_raw)
                if _code_raw in st.session_state.funda_memos:
                    del st.session_state.funda_memos[_code_raw]
                    save_memo_data(st.session_state.funda_memos)
                st.rerun()

        # ── メモ展開エリア ──────────────────────────────────────────────────
        if st.session_state.get(f"memo_open_{_code_raw}", False):
            _current_memo = st.session_state.funda_memos.get(_code_raw, "")
            _new_memo = st.text_area(
                f"📝 {_jp_name}（{_code_raw}）のメモ",
                value=_current_memo,
                key=f"memo_text_{_code_raw}",
                height=80,
                label_visibility="visible",
            )
            if st.button("💾 保存", key=f"memo_save_{_code_raw}"):
                st.session_state.funda_memos[_code_raw] = _new_memo
                save_memo_data(st.session_state.funda_memos)
                st.success("✅ メモを保存しました")

    # ダウンロード + 削除操作
    dl_col, ref_col, clr_col = st.columns([0.25, 0.25, 0.5])
    with dl_col:
        st.download_button(
            label="📥 CSV ダウンロード",
            data=fund_df.to_csv(index=False, encoding="utf-8-sig"),
            file_name="fundamental_list.csv",
            mime="text/csv",
            key="funda_dl_btn",
        )
    with ref_col:
        if st.button("🔄 データを再取得", key="funda_refresh_btn"):
            get_fundamentals.clear()
            codes = fund_df["code"].dropna().tolist() if "code" in fund_df.columns else []
            with st.spinner(f"{len(codes)} 銘柄を再取得中..."):
                new_rows = [get_fundamentals(c) for c in codes]
            st.session_state.fund_df = pd.DataFrame(new_rows)
            save_funda_data(st.session_state.fund_df)
            st.success("✅ 再取得完了")
            st.rerun()

    # ════════════════════════════════════════════════════════════════════════
    # ④ AI銘柄分析（Claude連携）
    # ════════════════════════════════════════════════════════════════════════
    st.divider()
    st.markdown("### 🤖 AI銘柄分析")
    st.info(
        "**使い方：** ① テンプレートと銘柄を選んでプロンプトを生成 →"
        " ② 「📋 Claudeへ貼り付け用に保存」を押してコピー →"
        " ③ Claude のチャット画面に貼り付けて分析依頼 →"
        " ④ 下の「📂 分析結果を表示」で結果を確認",
        icon="ℹ️",
    )

    # ── テンプレート選択 ─────────────────────────────────────────────────
    _templates = load_prompt_templates()
    _template_labels = {
        "stock_analysis":  "📊 総合分析レポート",
        "peer_compare":    "🔀 同業他社比較",
        "shareholder_scan":"👥 株主構成スキャン",
        "screening_deep":  "🔍 財務スクリーニング深掘り",
        "watchlist_review":"📋 監視銘柄一括レビュー",
        "earnings_check":  "📈 直近決算速報チェック",
        "risk_analysis":   "⚠️ 事業リスク・競合分析",
        "valuation":       "💰 バリュエーション評価",
        "growth_check":    "🚀 成長性チェック",
    }

    _ai_c1, _ai_c2 = st.columns([0.5, 0.5])
    with _ai_c1:
        _tmpl_options = list(_templates.keys())
        _tmpl_display = [_template_labels.get(k, k) for k in _tmpl_options]
        _sel_idx = st.selectbox(
            "分析テンプレート",
            options=range(len(_tmpl_options)),
            format_func=lambda i: _tmpl_display[i],
            key="ai_template_select",
        )
        _selected_tmpl_key = _tmpl_options[_sel_idx] if _tmpl_options else None

    with _ai_c2:
        # 監視銘柄リストから選択 or 直接入力
        _watch_codes = fund_df["code"].dropna().tolist() if "code" in fund_df.columns else []
        _watch_clean = [c.replace(".T","").replace(".t","") for c in _watch_codes]
        _name_opts   = [
            f"{c}  {_funda_name_map.get(c, '')}" for c in _watch_clean
        ]
        if _name_opts:
            _sel_stock_idx = st.selectbox(
                "対象銘柄（監視リストから選択）",
                options=range(len(_name_opts)),
                format_func=lambda i: _name_opts[i],
                key="ai_stock_select",
            )
            _target_code = _watch_clean[_sel_stock_idx]
        else:
            _target_code = ""

        _manual_code = st.text_input(
            "または直接入力（証券コード4桁）",
            placeholder="例: 7203",
            key="ai_manual_code",
        )
        if _manual_code.strip():
            _target_code = _manual_code.strip()

    # ── プロンプトプレビュー（編集可能）──────────────────────────────────
    # テンプレートまたは銘柄コードが変わったら session_state のテキストを上書きする
    # （st.text_area は key が同じだと value 引数より session_state が優先されるため）
    if _selected_tmpl_key and _templates:
        _raw_tmpl = _templates[_selected_tmpl_key]
        _filled   = _raw_tmpl.replace("{{TARGET_CODE}}", _target_code or "（銘柄コード未設定）")

        _prev_tmpl = st.session_state.get("_ai_prev_tmpl", "")
        _prev_code = st.session_state.get("_ai_prev_code", "")
        if _selected_tmpl_key != _prev_tmpl or _target_code != _prev_code:
            # 変化を検知 → テキストエリアの内容を強制更新
            st.session_state["ai_prompt_editor"]  = _filled
            st.session_state["_ai_prev_tmpl"]     = _selected_tmpl_key
            st.session_state["_ai_prev_code"]     = _target_code

        _edited_prompt = st.text_area(
            "📋 プロンプト（編集可能）",
            height=240,
            key="ai_prompt_editor",
        )
    else:
        st.warning("prompts/ ディレクトリにテンプレートが見つかりません。")
        _edited_prompt = ""

    # ── 保存ボタン（Claudeへ貼り付け用）─────────────────────────────────
    _save_col, _hint_col = st.columns([0.25, 0.75])
    with _save_col:
        if st.button(
            "📋 Claudeへ貼り付け用に保存",
            type="primary",
            disabled=not (_edited_prompt and _target_code),
            key="ai_save_prompt_btn",
        ):
            _saved_path = save_current_prompt(_edited_prompt)
            st.session_state["ai_saved_prompt"] = _edited_prompt
            st.success(f"保存しました: `{_saved_path}`")
    with _hint_col:
        st.caption(
            "保存後、プロンプトを上のテキストエリアからコピーして Claude のチャットに貼り付けてください。"
            "　Claude が EdinetDB から情報を収集し、`analysis_results/` フォルダに結果を保存します。"
        )

    # ── 保存済みプロンプトのコピー用表示 ─────────────────────────────────
    if st.session_state.get("ai_saved_prompt"):
        with st.expander("📄 保存済みプロンプト（コピー用）", expanded=True):
            st.code(st.session_state["ai_saved_prompt"], language="markdown")

    # ════════════════════════════════════════════════════════════════════════
    # ⑤ 分析結果の表示（analysis_results/ フォルダから）
    # ════════════════════════════════════════════════════════════════════════
    st.divider()
    st.markdown("### 📂 分析結果を表示")

    _res_files = list_analysis_results()
    _res_c1, _res_c2 = st.columns([0.7, 0.3])
    with _res_c1:
        if _res_files:
            _sel_result = st.selectbox(
                "結果ファイルを選択",
                options=_res_files,
                key="ai_result_select",
            )
        else:
            st.info("まだ分析結果がありません。Claudeで分析後、`analysis_results/` フォルダにMarkdownを保存してください。")
            _sel_result = None
    with _res_c2:
        if st.button("🔄 ファイル一覧を更新", key="ai_refresh_results"):
            st.rerun()

    if _sel_result:
        _result_text = load_analysis_result(_sel_result)
        if _result_text:
            _rdl_c1, _rdl_c2 = st.columns([0.8, 0.2])
            with _rdl_c1:
                st.markdown(f"**{_sel_result}**")
            with _rdl_c2:
                st.download_button(
                    "📥 MDダウンロード",
                    data=_result_text,
                    file_name=_sel_result,
                    mime="text/markdown",
                    key="ai_dl_btn",
                )
            with st.container(border=True):
                st.markdown(_result_text)
        else:
            st.warning("ファイルの読み込みに失敗しました。")


# ===========================================================================
# バックテスト: 1銘柄シミュレーション
# ===========================================================================

def backtest_ticker(
    ticker:            str,
    start_date:        str,
    end_date:          str,
    donchian_days:     int   = 20,
    ema_fast:          int   = 5,
    ema_slow:          int   = 20,
    use_5day_lookback: bool  = False,
    delay_days:        int   = 0,
    vol_mult_thr:      float = 1.0,
    dd_threshold:      float = -100.0,
) -> tuple[list[dict], str | None]:
    """
    ドンチャンブレイクアウト + EMAクロス EXIT 戦略のバックテスト（1銘柄）。

    エントリー条件:
        - Close > donchian_series (shift(1)使用 — 当日を含まない前N日の高値最大)
        - delay_days 後に価格がブレイクレベル以上であること
        - 待機期間中の最大下落が dd_threshold (%) 以上であること（負の値）
        - ブレイク日の出来高倍率が vol_mult_thr 以上であること
        - use_5day_lookback=True の場合、delay_days 後±5日以内のブレイクも有効

    決済条件:
        - EMA(fast) < EMA(slow) → デッドクロス翌日の始値で決済
        - データ終端まで未決済  → 最終日の終値で決済

    Args:
        ticker:            ティッカーシンボル
        start_date:        取得開始日（YYYY-MM-DD）
        end_date:          取得終了日（YYYY-MM-DD）
        donchian_days:     ドンチャンチャネル期間（日）
        ema_fast:          短期 EMA の期間
        ema_slow:          長期 EMA の期間
        use_5day_lookback: True のとき delay_days 後±5日以内のブレイクも有効
        delay_days:        ブレイク後に待機する日数（0=即時）
        vol_mult_thr:      ブレイク日の出来高倍率閾値（例: 1.5 = 平均の1.5倍以上）
        dd_threshold:      待機期間中の最大下落率の下限（例: -3.0 = 3%超の下落はNG）

    Returns:
        (trades_list, error_message)
    """
    EXTRA    = 5 if use_5day_lookback else 1   # delay_days 後の追加探索幅
    LOOKBACK = EXTRA  # 後方互換（スコア計算で参照）

    try:
        df = yf.download(ticker, start=start_date, end=end_date,
                         progress=False, auto_adjust=True)

        if df.empty:
            return [], "データなし"

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # donchian + lookback + EMA 収束バッファ
        min_rows = max(donchian_days + LOOKBACK + 2, ema_slow + 10)
        if len(df) < min_rows:
            return [], f"データ不足（{len(df)}日）"

        # ── テクニカル指標を事前計算 ─────────────────────────────────────
        # ドンチャン: shift(1) で当日を含まない前N日の高値最大
        donchian_s = df["High"].rolling(donchian_days).max().shift(1)

        # 出来高移動平均（スコア用）
        avg_vol_s  = df["Volume"].rolling(donchian_days).mean().shift(1)

        # EMA クロス（決済判定）— pandas ewm を使用
        ema_fast_s = df["Close"].ewm(span=ema_fast,  adjust=False).mean()
        ema_slow_s = df["Close"].ewm(span=ema_slow,  adjust=False).mean()

        # スコア構成要素用
        ma25_s = df["Close"].rolling(25).mean()
        ma75_s = df["Close"].rolling(75).mean()

        # numpy 配列に変換（ループ内の高速アクセス用）
        close_arr    = df["Close"].to_numpy(dtype=float)
        open_arr     = df["Open"].to_numpy(dtype=float)
        high_arr     = df["High"].to_numpy(dtype=float)
        low_arr      = df["Low"].to_numpy(dtype=float)
        vol_arr      = df["Volume"].to_numpy(dtype=float)
        donchian_arr = donchian_s.to_numpy(dtype=float)
        avg_vol_arr  = avg_vol_s.to_numpy(dtype=float)
        ema_fast_arr = ema_fast_s.to_numpy(dtype=float)
        ema_slow_arr = ema_slow_s.to_numpy(dtype=float)
        ma25_arr     = ma25_s.to_numpy(dtype=float)
        ma75_arr     = ma75_s.to_numpy(dtype=float)
        dates        = df.index
        n            = len(df)

        trades:     list[dict] = []
        in_trade    = False
        entry_i     = -1
        last_exit_i = -1   # 前回決済日（重複防止）
        entry_price = 0.0

        # ループ開始: donchian / delay / EMA のデータが十分に揃う最初の日
        start_i = max(donchian_days + delay_days + EXTRA, ema_slow + 5)

        for i in range(start_i, n):

            # ──────────────────────────────────────────────────────────────
            # ① ポジション保有中: EMAクロスによる決済判定
            # ──────────────────────────────────────────────────────────────
            if in_trade:
                ef = ema_fast_arr[i]
                es = ema_slow_arr[i]

                exited     = False
                exit_price = 0.0
                exit_bar   = i
                exit_dt    = str(dates[i].date())
                reason     = ""

                # EMA(fast) < EMA(slow) → デッドクロス: 翌日始値で決済
                if not np.isnan(ef) and not np.isnan(es) and ef < es:
                    if i + 1 < n:
                        exit_price = float(open_arr[i + 1])
                        exit_bar   = i + 1
                        exit_dt    = str(dates[i + 1].date())
                    else:
                        exit_price = float(close_arr[i])   # 最終日は終値
                        exit_bar   = i
                        exit_dt    = str(dates[i].date())
                    reason = "ema_cross"
                    exited = True
                # データ最終日に保有中 → 終値で決済（翌日データなし）
                elif i == n - 1:
                    exit_price = float(close_arr[i])
                    exit_bar   = i
                    exit_dt    = str(dates[i].date())
                    reason     = "end_of_data"
                    exited     = True

                if exited:
                    holding_days = exit_bar - entry_i
                    ret          = (exit_price - entry_price) / entry_price * 100
                    # 最後に追加したトレード record を完成させる
                    trades[-1].update({
                        "exit_date":    exit_dt,
                        "exit_price":   round(float(exit_price), 4),
                        "return(%)":    round(ret, 2),
                        "holding_days": holding_days,
                        "exit_reason":  reason,
                    })
                    last_exit_i = exit_bar
                    in_trade    = False

            # ──────────────────────────────────────────────────────────────
            # ② ノーポジション: エントリーシグナル検索（フィルター付き）
            # ──────────────────────────────────────────────────────────────
            else:
                # delay_days 日前〜(delay_days + EXTRA - 1)日前の範囲でブレイクを探す
                # lag = "現在バーiからブレイク発生バーjまでの距離"
                # スクリーナーと同様: ブレイクは delay_days 以上前、
                # かつ delay_days+EXTRA 未満前でなければならない
                breakout_found = False
                breakout_lag   = 0

                for lag in range(delay_days, delay_days + EXTRA):
                    j = i - lag          # ブレイク発生バー
                    if j <= last_exit_i or j < start_i:
                        break
                    dc = donchian_arr[j]
                    if np.isnan(dc):
                        continue
                    if close_arr[j] <= dc:
                        continue

                    # ── 出来高フィルター（ブレイク発生日） ──────────────
                    avg_v = avg_vol_arr[j]
                    if avg_v > 0 and not np.isnan(avg_v):
                        if float(vol_arr[j]) / avg_v < vol_mult_thr:
                            continue   # 出来高不足

                    # ── 価格継続フィルター（現在バーでもブレイク水準以上） ─
                    if close_arr[i] <= float(close_arr[j]):
                        continue

                    # ── 待機DDフィルター（delay_days > 0 のときのみ） ────
                    if delay_days > 0:
                        window_low = low_arr[j: i + 1]
                        min_price  = float(np.nanmin(window_low))
                        waiting_dd = (min_price - float(close_arr[j])) / float(close_arr[j]) * 100
                        if waiting_dd <= dd_threshold:
                            continue   # 待機中に深押し → 除外

                    breakout_found = True
                    breakout_lag   = lag
                    break

                if not breakout_found:
                    continue

                # エントリー時点の EMA が有効か確認
                if np.isnan(ema_fast_arr[i]) or np.isnan(ema_slow_arr[i]):
                    continue

                # ── エントリー（翌日始値） ────────────────────────────────
                # 翌日データが存在しない場合はスキップ
                if i + 1 >= n:
                    continue
                in_trade    = True
                entry_i     = i + 1                        # 実際の約定バー（翌日）
                entry_price = float(open_arr[i + 1])       # 翌日始値

                # ── エントリー時点のスコア構成要素 ───────────────────────
                avg_v   = avg_vol_arr[i]
                vol_r   = float(vol_arr[i]) / avg_v if avg_v > 0 and not np.isnan(avg_v) else 0.0

                ma25_v  = ma25_arr[i]
                ma75_v  = ma75_arr[i]
                trend_s = (
                    (ma25_v - ma75_v) / ma75_v * 100
                    if ma75_v > 0 and not np.isnan(ma75_v) else 0.0
                )

                # ブレイク強度（ブレイク発生日の数値を使用）
                j_bp  = i - breakout_lag
                dc_bp = donchian_arr[j_bp]
                bp    = (close_arr[j_bp] - dc_bp) / dc_bp * 100 if dc_bp > 0 else 0.0

                # ATR比率の近似（スコア用: High-Low / 終値）
                atr_r = (float(high_arr[i]) - float(low_arr[i])) / entry_price if entry_price > 0 else 0.0

                # 10日レンジ圧縮
                if i >= 11:
                    hi10      = float(np.nanmax(high_arr[max(0, i - 11): i - 1]))
                    lo10      = float(np.nanmin(low_arr[max(0, i - 11): i - 1]))
                    range_pct = (hi10 - lo10) / entry_price * 100 if entry_price > 0 else 999.0
                else:
                    range_pct = 999.0
                compression = 100.0 / max(range_pct, 0.5)

                trades.append({
                    "ティッカー":   ticker,
                    "entry_date":   str(dates[i + 1].date()),   # 翌日の日付
                    "entry_price":  round(entry_price, 4),
                    "exit_date":    None,    # 決済後に更新
                    "exit_price":   None,
                    "return(%)":    None,
                    "holding_days": None,
                    "exit_reason":  None,
                    # スコア構成要素（後で正規化）
                    "_bp":   bp,
                    "_vr":   vol_r,
                    "_ts":   trend_s,
                    "_ar":   atr_r,
                    "_comp": compression,
                })

        return trades, None

    except Exception as e:
        return [], str(e)


# ===========================================================================
# バックテスト: スコア正規化 & DataFrame 整形
# ===========================================================================

def normalize_bt_scores(trades: list[dict]) -> pd.DataFrame:
    """
    全トレードのスコア構成要素を MinMax 正規化（0〜100）し、
    総合スコアを付与した DataFrame を返す。
    未決済のトレード（exit_date が None）は除外する。
    """
    if not trades:
        return pd.DataFrame()

    df = pd.DataFrame(trades)

    # 未決済トレードを除外
    df = df[df["return(%)"].notna()].copy()
    if df.empty:
        return df

    # ── MinMax 正規化 ────────────────────────────────────────────────────
    score_cols  = ["_bp", "_vr", "_ts", "_ar", "_comp"]
    norm_series = []
    for col in score_cols:
        if col not in df.columns:
            continue
        mn, mx = df[col].min(), df[col].max()
        if mx > mn:
            normed = (df[col] - mn) / (mx - mn) * 100
        else:
            normed = pd.Series([50.0] * len(df), index=df.index)
        norm_series.append(normed)

    df["スコア"] = (
        pd.concat(norm_series, axis=1).mean(axis=1).round(1)
        if norm_series else 0.0
    )

    # 内部用列を削除
    df = df.drop(columns=[c for c in df.columns if c.startswith("_")])

    # 表示列の順序を整える
    col_order = [
        "ティッカー", "entry_date", "exit_date",
        "entry_price", "exit_price", "return(%)",
        "holding_days", "exit_reason", "スコア",
    ]
    df = df[[c for c in col_order if c in df.columns]]

    return df.reset_index(drop=True)


# ===========================================================================
# 戦略比較: スコア計算ロジック
# ===========================================================================

def calc_raw_score(row: dict, strategy: str) -> float:
    """
    トレード record の生スコア構成要素から、戦略固有のスコア（生値）を返す。
    全て「高いほど強いシグナル」を意味する値に統一してある。

    構成要素（backtest_ticker で計算済み）:
        _bp   : ドンチャン突破強度 (%)         ← 大きいほど強い上抜け
        _vr   : 出来高倍率（平均比）            ← 大きいほど出来高急増
        _ts   : トレンド強度 (25MA-75MA)/75MA  ← 正値が上昇トレンド
        _ar   : ATR/終値比率                   ← 大きいほど高ボラ
        _comp : レンジ圧縮 = 100/range_pct    ← 大きいほど直前が横ばい
    """
    bp   = float(row.get("_bp",   0.0) or 0.0)
    vr   = float(row.get("_vr",   0.0) or 0.0)
    ts   = float(row.get("_ts",   0.0) or 0.0)
    ar   = float(row.get("_ar",   0.0) or 0.0)
    comp = float(row.get("_comp", 0.0) or 0.0)

    if strategy == "momentum":
        # ブレイク強度(60%) + 出来高急増(40%)
        return bp * 0.6 + vr * 0.4

    elif strategy == "squeeze":
        # レンジ圧縮のみ（価格がエネルギー蓄積してからブレイクした銘柄を優先）
        return comp

    elif strategy == "volatility":
        # ATR比率（ボラティリティが高い銘柄を優先: 値幅が取れる）
        return ar * 100.0

    elif strategy == "trend":
        # トレンド強度（上昇トレンドの強い銘柄のみ: 負は 0 に丸める）
        return max(ts, 0.0)

    elif strategy == "event":
        # 全構成要素の均等加重複合スコア
        return (bp + vr + max(ts, 0.0) + ar * 100.0 + comp) / 5.0

    return 0.0


def _minmax_norm(series: pd.Series) -> pd.Series:
    """MinMax 正規化 (0〜100)。全値同一の場合は 50 を返す"""
    mn, mx = series.min(), series.max()
    if mx > mn:
        return (series - mn) / (mx - mn) * 100.0
    return pd.Series([50.0] * len(series), index=series.index)


# ===========================================================================
# 戦略比較: メイン集計
# ===========================================================================

def compare_strategies(
    raw_trades:    list[dict],
    selected:      list[str],
    top_pct:       float,   # 上位 X% のみ対象 (100 = フィルターなし)
    min_vol_ratio: float,   # 最低出来高倍率 (0 = フィルターなし)
    min_price:     float,   # 最低価格 (0 = フィルターなし)
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """
    各戦略のスコアでフィルタリングしてパフォーマンス指標を比較する。

    Returns:
        summary_df   : 戦略比較テーブル
        strat_trades : {strategy_key: filtered_trades_df}  可視化用
    """
    if not raw_trades:
        return pd.DataFrame(), {}

    # 基底 DataFrame（決済済みトレードのみ）
    base = pd.DataFrame(raw_trades)
    base = base[base["return(%)"].notna()].copy()
    if base.empty:
        return pd.DataFrame(), {}

    # ── 共通フィルター（価格・出来高） ───────────────────────────────────
    if min_price > 0 and "entry_price" in base.columns:
        base = base[pd.to_numeric(base["entry_price"], errors="coerce") >= min_price]
    if min_vol_ratio > 0 and "_vr" in base.columns:
        base = base[pd.to_numeric(base["_vr"], errors="coerce") >= min_vol_ratio]
    if base.empty:
        return pd.DataFrame(), {}

    summary_rows: list[dict]               = []
    strat_trades: dict[str, pd.DataFrame]  = {}

    for strat in selected:
        df = base.copy()

        # ── 戦略スコアを計算 ────────────────────────────────────────────
        df["_raw_score"] = df.apply(lambda r: calc_raw_score(r.to_dict(), strat), axis=1)

        # ── スコア上位○%フィルター ──────────────────────────────────────
        if top_pct < 100.0 and len(df) > 1:
            threshold = df["_raw_score"].quantile(1.0 - top_pct / 100.0)
            df = df[df["_raw_score"] >= threshold].copy()

        if df.empty:
            continue

        # ── 正規化スコア (0〜100) ────────────────────────────────────────
        df["スコア"] = _minmax_norm(df["_raw_score"]).round(1)

        # ── パフォーマンス指標 ───────────────────────────────────────────
        rets = df["return(%)"].astype(float)
        n    = len(rets)

        total_ret  = float(rets.sum())
        avg_ret    = float(rets.mean())
        win_rate   = float((rets > 0).sum() / n * 100)

        # 最大ドローダウン: 累積リターン曲線のピーク比下落
        cum      = (1 + rets / 100).cumprod()
        roll_max = cum.cummax()
        max_dd   = float(((cum - roll_max) / roll_max * 100).min())

        # シャープレシオ: 平均リターン / 標準偏差（簡易版）
        std    = float(rets.std())
        sharpe = float(avg_ret / std) if std > 0 else 0.0

        # スコアとリターンの相関係数
        sc_df = df[["スコア", "return(%)"]].dropna()
        if len(sc_df) >= 3:
            corr = float(sc_df["スコア"].corr(sc_df["return(%)"]))
        else:
            corr = float("nan")

        summary_rows.append({
            "戦略":            STRATEGIES.get(strat, strat),
            "トレード数":      n,
            "総リターン(%)":   round(total_ret, 2),
            "平均リターン(%)": round(avg_ret, 2),
            "勝率(%)":         round(win_rate, 1),
            "最大DD(%)":       round(max_dd, 2),
            "シャープ比":      round(sharpe, 3),
            "スコア相関":      round(corr, 3) if not np.isnan(corr) else None,
        })

        # 可視化用: エントリー日順にソート
        if "entry_date" in df.columns:
            df = df.sort_values("entry_date").reset_index(drop=True)
        strat_trades[strat] = df

    summary_df = pd.DataFrame(summary_rows) if summary_rows else pd.DataFrame()
    return summary_df, strat_trades


# ===========================================================================
# 戦略比較: UI 描画
# ===========================================================================

def render_strategy_comparison(raw_trades: list[dict]) -> None:
    """戦略比較セクション全体を描画する"""
    if not raw_trades:
        st.info("バックテストを実行すると戦略比較が利用できます。")
        return

    st.divider()
    st.markdown("## ⚔️ 複数スコア戦略の比較")
    st.caption(
        "同じバックテストデータに対してスコア計算ロジックを切り替え、"
        "各戦略の優劣（総リターン・勝率・ドローダウン・シャープ比・スコア相関）を比較します。"
    )

    # ── 設定パネル ────────────────────────────────────────────────────────
    with st.expander("🎛️ 比較設定", expanded=True):
        sel_col, _ = st.columns([0.65, 0.35])
        with sel_col:
            selected = st.multiselect(
                "比較する戦略を選択",
                options=list(STRATEGIES.keys()),
                default=list(STRATEGIES.keys()),
                format_func=lambda k: STRATEGIES[k],
                key="strat_compare_select",
            )
        fa, fb, fc = st.columns(3)
        with fa:
            top_pct = st.slider(
                "📊 スコア上位 ○% のみ対象",
                min_value=10, max_value=100, value=100, step=10,
                help="100% = フィルターなし。50% = スコア上位50%のトレードのみ集計",
                key="strat_top_pct",
            )
        with fb:
            min_vol_ratio = st.number_input(
                "🔥 最低出来高倍率",
                min_value=0.0, max_value=10.0, value=0.0, step=0.5, format="%.1f",
                help="ブレイク日の出来高 ÷ N日平均の下限。0 = フィルターなし",
                key="strat_min_vol",
            )
        with fc:
            min_price = st.number_input(
                "💴 最低価格",
                min_value=0.0, value=0.0, step=100.0, format="%.0f",
                help="エントリー価格の下限（円 or USD）。0 = フィルターなし",
                key="strat_min_price",
            )

    if not selected:
        st.info("比較する戦略を1つ以上選択してください。")
        return

    if not st.button("⚔️ 戦略比較を実行", type="primary", key="strat_run_btn"):
        return

    with st.spinner("各戦略のパフォーマンスを集計中..."):
        summary_df, strat_trades = compare_strategies(
            raw_trades    = raw_trades,
            selected      = selected,
            top_pct       = float(top_pct),
            min_vol_ratio = float(min_vol_ratio),
            min_price     = float(min_price),
        )

    if summary_df.empty:
        st.warning("条件を満たすトレードがありませんでした。フィルター設定を緩めてください。")
        return

    # ── ① 戦略比較テーブル ──────────────────────────────────────────────
    st.markdown("### 📋 戦略比較テーブル")

    def _cc(val):
        try:
            v = float(val)
            return "color: #4caf50; font-weight:bold" if v > 0 else "color: #f44336; font-weight:bold"
        except Exception:
            return ""

    pos_cols = [c for c in ["総リターン(%)", "平均リターン(%)"] if c in summary_df.columns]
    fmt_cmp  = {k: v for k, v in {
        "総リターン(%)":   "{:+.2f}%",
        "平均リターン(%)": "{:+.2f}%",
        "勝率(%)":         "{:.1f}%",
        "最大DD(%)":       "{:+.2f}%",
        "シャープ比":      "{:.3f}",
        "スコア相関":      "{:.3f}",
    }.items() if k in summary_df.columns}

    styled_cmp = summary_df.style.format(fmt_cmp)
    if pos_cols:
        styled_cmp = styled_cmp.map(_cc, subset=pos_cols)
    st.dataframe(styled_cmp, use_container_width=True, hide_index=True)

    # ── ② 累積リターン曲線（戦略別）─────────────────────────────────────
    st.markdown("### 📈 累積リターン曲線（戦略別）")
    try:
        fig, ax = plt.subplots(figsize=(10, 4))
        for strat_key, df_s in strat_trades.items():
            if df_s.empty or "return(%)" not in df_s.columns:
                continue
            rets = df_s["return(%)"].astype(float).values
            cum  = np.cumsum(rets)   # 単純累積（視認性重視）
            ax.plot(range(len(cum)), cum,
                    label=STRATEGIES.get(strat_key, strat_key), linewidth=1.8)
        ax.axhline(0, color="gray", linewidth=0.8, linestyle="--")
        ax.set_xlabel("トレード番号（エントリー日順）", fontsize=10)
        ax.set_ylabel("累積リターン (%)", fontsize=10)
        ax.set_title("戦略別 累積リターン曲線", fontsize=12)
        ax.legend(fontsize=9, loc="upper left")
        fig.tight_layout()
        st.pyplot(fig)
        plt.close(fig)
    except Exception as e:
        st.warning(f"累積リターングラフの描画中にエラー: {e}")

    # ── ③ スコア vs リターン 散布図（戦略別サブプロット）──────────────
    st.markdown("### 🔍 スコア vs リターン 散布図（戦略別）")
    n_s = len(strat_trades)
    if n_s == 0:
        return
    try:
        cols_n = min(n_s, 3)
        rows_n = math.ceil(n_s / cols_n)
        fig2, axes = plt.subplots(rows_n, cols_n,
                                  figsize=(6 * cols_n, 4 * rows_n),
                                  squeeze=False)
        for idx, (strat_key, df_s) in enumerate(strat_trades.items()):
            ri, ci = divmod(idx, cols_n)
            ax_s   = axes[ri][ci]
            sc_df  = df_s[["スコア", "return(%)"]].dropna()
            if sc_df.empty:
                ax_s.set_visible(False)
                continue
            ax_s.scatter(sc_df["スコア"], sc_df["return(%)"],
                         alpha=0.5, edgecolors="none", s=30)
            ax_s.axhline(0, color="gray", linewidth=0.7, linestyle="--")
            if len(sc_df) >= 2:
                z  = np.polyfit(sc_df["スコア"], sc_df["return(%)"], 1)
                xs = np.linspace(sc_df["スコア"].min(), sc_df["スコア"].max(), 100)
                ax_s.plot(xs, np.poly1d(z)(xs), linewidth=1.5)
            corr_v = sc_df["スコア"].corr(sc_df["return(%)"])
            ax_s.set_title(
                f"{STRATEGIES.get(strat_key, strat_key)}\n(r = {corr_v:+.3f})",
                fontsize=9)
            ax_s.set_xlabel("スコア", fontsize=8)
            ax_s.set_ylabel("リターン (%)", fontsize=8)
        # 余ったサブプロットを非表示
        for idx in range(n_s, rows_n * cols_n):
            ri, ci = divmod(idx, cols_n)
            axes[ri][ci].set_visible(False)
        fig2.tight_layout()
        st.pyplot(fig2)
        plt.close(fig2)
    except Exception as e:
        st.warning(f"散布図の描画中にエラー: {e}")

    # ── ④ スコア別リターン分析（戦略別タブ表示）─────────────────────────
    st.markdown("### 🏆 スコア別リターン分析（戦略別）")
    st.caption(
        "各戦略のスコアを5分位に分けて、スコア帯ごとの平均リターン・勝率・最大/最小を集計します。"
        "「高スコア帯ほどリターンが良い」戦略が有効です。"
    )

    def _build_score_band_table(df_s: pd.DataFrame) -> pd.DataFrame | None:
        """戦略 DataFrame からスコア5分位テーブルを作成して返す。失敗時は None"""
        sc = df_s[["スコア", "return(%)"]].dropna()
        if len(sc) < 5:
            return None
        try:
            sc = sc.copy()
            sc["スコア帯"] = pd.qcut(
                sc["スコア"], q=5,
                labels=["最低(0-20)", "低(20-40)", "中(40-60)", "高(60-80)", "最高(80-100)"],
                duplicates="drop",
            )
            tbl = (
                sc.groupby("スコア帯", observed=True)["return(%)"]
                .agg(
                    トレード数   = "count",
                    平均リターン = "mean",
                    勝率         = lambda x: (x > 0).sum() / len(x) * 100,
                    最大         = "max",
                    最小         = "min",
                )
                .round(2)
                .reset_index()
            )
            tbl.columns = ["スコア帯", "トレード数", "平均リターン(%)", "勝率(%)", "最大(%)", "最小(%)"]
            return tbl
        except Exception:
            return None

    def _style_score_band(tbl: pd.DataFrame) -> pd.Styler:
        def _ca(val):
            try:
                v = float(val)
                return "color: #4caf50; font-weight:bold" if v > 0 else "color: #f44336; font-weight:bold"
            except Exception:
                return ""
        return (
            tbl.style
            .format({
                "平均リターン(%)": "{:+.2f}%",
                "勝率(%)":        "{:.1f}%",
                "最大(%)":        "{:+.2f}%",
                "最小(%)":        "{:+.2f}%",
            })
            .map(_ca, subset=["平均リターン(%)"])
        )

    # 各戦略を st.expander で折りたたみ表示
    for strat_key, df_s in strat_trades.items():
        strat_label = STRATEGIES.get(strat_key, strat_key)
        with st.expander(f"{strat_label}", expanded=False):
            tbl = _build_score_band_table(df_s)
            if tbl is None:
                st.info("スコア分析には5件以上のトレードが必要です。")
                continue

            st.dataframe(_style_score_band(tbl), use_container_width=True, hide_index=True)

            # スコア帯別 平均リターン バーチャート
            try:
                fig3, ax3 = plt.subplots(figsize=(7, 2.8))
                vals   = tbl["平均リターン(%)"].values
                labels = tbl["スコア帯"].astype(str).values
                colors = ["#4caf50" if v >= 0 else "#f44336" for v in vals]
                ax3.bar(labels, vals, color=colors)
                ax3.axhline(0, color="gray", linewidth=0.8, linestyle="--")
                ax3.set_ylabel("平均リターン (%)", fontsize=9)
                ax3.set_title(f"{strat_label} — スコア帯別 平均リターン", fontsize=10)
                ax3.tick_params(axis="x", labelsize=8)
                fig3.tight_layout()
                st.pyplot(fig3)
                plt.close(fig3)
            except Exception as e:
                st.warning(f"グラフ描画エラー: {e}")


# ===========================================================================
# バックテスト高度分析: 銘柄情報キャッシュ付き取得
# ===========================================================================

@st.cache_data(show_spinner=False, ttl=3600)
def _fetch_ticker_info(ticker: str) -> dict:
    """
    yfinance から銘柄名とセクターを取得してキャッシュする。
    API 呼び出し失敗時は空の dict を返す（呼び出し元で "Unknown" を補完）。
    TTL=3600 秒（1時間）でキャッシュを自動更新。
    """
    try:
        info = yf.Ticker(ticker).info
        return {
            "name":   info.get("longName") or info.get("shortName") or ticker,
            "sector": info.get("sector")   or "Unknown",
        }
    except Exception:
        return {"name": ticker, "sector": "Unknown"}


# ===========================================================================
# バックテスト高度分析: 銘柄別 / 相関 / セクター 集計
# ===========================================================================

def _render_advanced_analysis(result_df: pd.DataFrame) -> None:
    """
    バックテスト結果 DataFrame を受け取り、高度分析を描画する。

    ① 銘柄別パフォーマンス（トップ10 / ワースト10）
    ② スコアとリターンの相関分析（係数 + 散布図）
    ③ セクター別パフォーマンス
    """
    if result_df.empty or "return(%)" not in result_df.columns:
        return

    df = result_df[result_df["return(%)"].notna()].copy()
    if df.empty:
        return

    st.divider()
    st.markdown("## 🔬 高度分析")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ① 銘柄別パフォーマンス分析
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    st.markdown("### 📊 銘柄別パフォーマンス")

    ticker_col = "ティッカー"
    if ticker_col not in df.columns:
        st.info("銘柄コード列が見つかりません。")
    else:
        # 銘柄ごとに平均リターンとトレード数を集計
        ticker_agg = (
            df.groupby(ticker_col)["return(%)"]
            .agg(平均リターン="mean", トレード数="count")
            .round(2)
            .reset_index()
        )

        # 銘柄名を付与（キャッシュ経由）
        with st.spinner("銘柄情報を取得中..."):
            ticker_agg["銘柄名"] = ticker_agg[ticker_col].apply(
                lambda t: _fetch_ticker_info(t).get("name", t)
            )

        # 表示列を整理
        ticker_agg = ticker_agg[[ticker_col, "銘柄名", "平均リターン", "トレード数"]]
        ticker_agg_sorted = ticker_agg.sort_values("平均リターン", ascending=False)

        def _style_ticker_table(tbl: pd.DataFrame) -> pd.Styler:
            def _cr(val):
                try:
                    v = float(val)
                    return "color: #4caf50; font-weight:bold" if v > 0 else "color: #f44336; font-weight:bold"
                except Exception:
                    return ""
            return (
                tbl.style
                .format({"平均リターン": "{:+.2f}%"})
                .map(_cr, subset=["平均リターン"])
            )

        top_col, bot_col = st.columns(2)
        with top_col:
            st.markdown("#### 🏆 トップ10銘柄（平均リターン高い順）")
            top10 = ticker_agg_sorted.head(10).reset_index(drop=True)
            st.dataframe(_style_ticker_table(top10), use_container_width=True, hide_index=True)

        with bot_col:
            st.markdown("#### 💔 ワースト10銘柄（平均リターン低い順）")
            bot10 = ticker_agg_sorted.tail(10).sort_values("平均リターン").reset_index(drop=True)
            st.dataframe(_style_ticker_table(bot10), use_container_width=True, hide_index=True)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ② スコアとリターンの相関分析
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    st.divider()
    st.markdown("### 📈 スコアとリターンの相関分析")

    if "スコア" not in df.columns:
        st.info("スコア列が見つかりません。バックテストを再実行してください。")
    else:
        corr_df = df[["スコア", "return(%)"]].dropna()

        if len(corr_df) < 3:
            st.info("相関分析には3件以上のトレードが必要です。")
        else:
            corr = corr_df["スコア"].corr(corr_df["return(%)"])

            # 相関係数の強度を日本語で表現
            abs_c = abs(corr)
            if abs_c >= 0.7:
                strength = "強い相関"
            elif abs_c >= 0.4:
                strength = "中程度の相関"
            elif abs_c >= 0.2:
                strength = "弱い相関"
            else:
                strength = "ほぼ無相関"
            direction = "正" if corr > 0 else "負"

            c_metric, c_desc = st.columns([0.3, 0.7])
            with c_metric:
                st.metric(
                    label="スコア × リターン 相関係数",
                    value=f"{corr:+.4f}",
                    help="1.0 に近いほど「高スコア = 高リターン」の傾向が強い",
                )
            with c_desc:
                st.markdown(
                    f"**判定**: {direction}の{strength}（|r| = {abs_c:.3f}）\n\n"
                    f"スコアが高いエントリーほどリターンが{'良い' if corr > 0 else '悪い'}傾向が"
                    f"{'あります ✅' if abs_c >= 0.2 else 'ほぼ見られません ⚠️'}"
                )

            # 散布図（matplotlib）
            try:
                fig, ax = plt.subplots(figsize=(7, 4))
                ax.scatter(
                    corr_df["スコア"], corr_df["return(%)"],
                    alpha=0.55, edgecolors="none", s=40,
                )
                ax.axhline(0, color="gray", linewidth=0.8, linestyle="--")
                ax.set_xlabel("スコア（エントリー時）", fontsize=11)
                ax.set_ylabel("リターン (%)", fontsize=11)
                ax.set_title(f"スコア vs リターン  (r = {corr:+.4f})", fontsize=12)

                # 近似直線
                if len(corr_df) >= 2:
                    z = np.polyfit(corr_df["スコア"], corr_df["return(%)"], 1)
                    p = np.poly1d(z)
                    xs = np.linspace(corr_df["スコア"].min(), corr_df["スコア"].max(), 100)
                    ax.plot(xs, p(xs), linewidth=1.5, linestyle="-")

                fig.tight_layout()
                st.pyplot(fig)
                plt.close(fig)
            except Exception as e:
                st.warning(f"散布図の描画中にエラー: {e}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ③ セクター別パフォーマンス分析
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    st.divider()
    st.markdown("### 🏭 セクター別パフォーマンス")

    if ticker_col not in df.columns:
        st.info("銘柄コード列が見つかりません。")
        return

    with st.spinner("セクター情報を取得中（初回のみ時間がかかります）..."):
        unique_tickers = df[ticker_col].dropna().unique().tolist()
        sector_map: dict[str, str] = {
            t: _fetch_ticker_info(t).get("sector", "Unknown")
            for t in unique_tickers
        }

    # セクター列を付与してから集計
    df_sec = df.copy()
    df_sec["セクター"] = df_sec[ticker_col].map(sector_map).fillna("Unknown")

    sector_agg = (
        df_sec.groupby("セクター")["return(%)"]
        .agg(
            トレード数   = "count",
            平均リターン  = "mean",
            勝率         = lambda x: (x > 0).sum() / len(x) * 100,
            最大リターン  = "max",
            最小リターン  = "min",
        )
        .round(2)
        .sort_values("平均リターン", ascending=False)
        .reset_index()
    )
    sector_agg.columns = [
        "セクター", "トレード数", "平均リターン(%)", "勝率(%)", "最大(%)", "最小(%)"
    ]

    def _color_sec_avg(val):
        try:
            v = float(val)
            return "color: #4caf50; font-weight:bold" if v > 0 else "color: #f44336; font-weight:bold"
        except Exception:
            return ""

    styled_sec = (
        sector_agg.style
        .format({
            "平均リターン(%)": "{:+.2f}%",
            "勝率(%)":        "{:.1f}%",
            "最大(%)":        "{:+.2f}%",
            "最小(%)":        "{:+.2f}%",
        })
        .map(_color_sec_avg, subset=["平均リターン(%)"])
    )
    st.dataframe(styled_sec, use_container_width=True, hide_index=True)

    # セクター別平均リターン バーチャート
    try:
        fig2, ax2 = plt.subplots(figsize=(8, max(3, len(sector_agg) * 0.45)))
        values    = sector_agg["平均リターン(%)"].values
        labels    = sector_agg["セクター"].values
        colors    = ["#4caf50" if v >= 0 else "#f44336" for v in values]
        ax2.barh(labels[::-1], values[::-1], color=colors[::-1])
        ax2.axvline(0, color="gray", linewidth=0.8, linestyle="--")
        ax2.set_xlabel("平均リターン (%)", fontsize=11)
        ax2.set_title("セクター別 平均リターン", fontsize=12)
        fig2.tight_layout()
        st.pyplot(fig2)
        plt.close(fig2)
    except Exception as e:
        st.warning(f"セクターグラフの描画中にエラー: {e}")


# ===========================================================================
# バックテスト: CSV アップロードパーサー
# ===========================================================================

def _parse_csv_tickers(uploaded_file) -> tuple[list[str], str | None]:
    """
    アップロードされた CSV から銘柄コードのリストを抽出して返す。

    対応フォーマット:
        パターン①: カラム名 "ticker"  例: 7203.T
        パターン②: カラム名 "code"    例: 7203  → 自動で .T を付与

    Returns:
        (tickers_list, error_message)
        成功時は error_message=None、失敗時は tickers_list=[]
    """
    try:
        df = pd.read_csv(uploaded_file, dtype=str)

        # ── カラム名を小文字に正規化して判定 ────────────────────────────
        df.columns = df.columns.str.strip().str.lower()

        if "ticker" in df.columns:
            raw = df["ticker"].dropna().astype(str).str.strip()
        elif "code" in df.columns:
            raw = df["code"].dropna().astype(str).str.strip()
        else:
            # どちらのカラム名も見つからない場合: 最初の列を使用
            raw = df.iloc[:, 0].dropna().astype(str).str.strip()

        tickers: list[str] = []
        for val in raw:
            val = val.strip()
            if not val:
                continue

            # ── ドット前の「基本コード」を取り出す ──────────────────────
            # 例: "7203"    → base="7203"
            #     "7203.T"  → base="7203"
            #     "7203.JP" → base="7203"  ← .JP などの誤サフィックスを除去
            #     "7203.0"  → base="7203"  ← pandas の float 変換対策
            #     "AAPL"    → base="AAPL"
            base = val.split(".")[0].strip()

            if base.isdigit():
                # 数字コード → 日本株として .T を付与（既存サフィックスは上書き）
                val = base + ".T"
            # 英字ティッカー（米国株など）はそのまま保持

            tickers.append(val.upper())

        # 重複削除（順序を保持）
        tickers = list(dict.fromkeys(tickers))

        if not tickers:
            return [], "CSVに有効な銘柄コードが見つかりませんでした。"

        return tickers, None

    except Exception as e:
        return [], f"CSV 読み込みエラー: {e}"


# ===========================================================================
# 画面描画: 決算カレンダータブ
# ===========================================================================

_REPORTS_DIR    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
_EARNINGS_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "earnings_results.json")
_TASK_NAME      = "TurtleTool_EarningsCalendar"   # Windows タスクスケジューラ タスク名


def _build_earnings_prompt(
    sec_code: str,
    company_name: str,
    period_type: str,
    market: str,
    announcement_date: str,
) -> str:
    """
    EdinetDB から財務データを取得し、AI決算分析用プロンプトを生成して返す。
    Claude / ChatGPT / Gemini どれにでも貼り付け可能な形式。
    """
    lines: list[str] = []

    # ── 1. 基本情報セクション ────────────────────────────────────────────────
    lines += [
        f"# 決算速報分析レポート依頼",
        f"",
        f"## 対象企業",
        f"- **企業名**: {company_name}（証券コード: {sec_code}）",
        f"- **決算種別**: {period_type}",
        f"- **市場**: {market}",
        f"- **発表日**: {announcement_date}",
        f"",
    ]

    # ── 2. EdinetDB から基本財務データ取得 ──────────────────────────────────
    _edb = edinet_get_company(sec_code)
    _edinet_code = edinet_get_edinet_code(sec_code)
    _earnings_list: list[dict] = []
    if _edinet_code:
        _er = _call_edinetdb("get_earnings", edinet_code=_edinet_code, limit=8)
        _earnings_list = _er.get("earnings", []) if isinstance(_er, dict) else []

    if "error" not in _edb:
        # 年次財務サマリー
        _fy    = _edb.get("latestFiscalYear", "―")
        _rev   = _edb.get("revenue")
        _oi    = _edb.get("operatingIncome")
        _ni    = _edb.get("netIncome")
        _eps   = _edb.get("eps")
        _per   = _edb.get("per")
        _pbr   = _edb.get("priceToBook") or _edb.get("pbr")
        _roe   = _edb.get("roe")
        _opm   = _edb.get("operatingMargin")
        _eq    = _edb.get("equityRatio")
        _dy    = _edb.get("dividendYield")
        _hs    = _edb.get("healthScore")
        _ev_eb = _edb.get("evEbitda")

        def _m(v, fmt=".1f", unit="百万円"):
            if v is None:
                return "―"
            if unit == "百万円":
                return f"{v/1_000_000:,.0f}百万円"
            return f"{v:{fmt}}"

        lines += [
            f"## 直近年次財務データ（FY{_fy}）",
            f"| 指標 | 数値 |",
            f"|------|------|",
            f"| 売上高 | {_m(_rev)} |",
            f"| 営業利益 | {_m(_oi)} |",
            f"| 純利益 | {_m(_ni)} |",
            f"| EPS | {_eps:.2f}円 |" if _eps else "| EPS | ― |",
            f"| PER | {_per:.1f}倍 |" if _per else "| PER | ― |",
            f"| PBR | {_pbr:.2f}倍 |" if _pbr else "| PBR | ― |",
            f"| ROE | {_roe*100:.1f}% |" if _roe else "| ROE | ― |",
            f"| 営業利益率 | {_opm*100:.1f}% |" if _opm else "| 営業利益率 | ― |",
            f"| 自己資本比率 | {_eq*100:.1f}% |" if _eq else "| 自己資本比率 | ― |",
            f"| 配当利回り | {_dy*100:.2f}% |" if _dy else "| 配当利回り | ― |",
            f"| EV/EBITDA | {_ev_eb:.1f}倍 |" if _ev_eb else "| EV/EBITDA | ― |",
            f"| 財務健全性スコア | {_hs}/100 |" if _hs else "| 財務健全性スコア | ― |",
            f"",
        ]

        # 最新決算短信
        _le = _edb.get("latestEarnings")
        if _le:
            _qt     = _le.get("quarter", "")
            _qdate  = _le.get("disclosureDate", "")
            _qrev   = _le.get("revenue")
            _qoi    = _le.get("operatingIncome")
            _qni    = _le.get("netIncome")
            _qeps   = _le.get("eps")
            _qrchg  = _le.get("revenueYoy")
            _qoichg = _le.get("operatingIncomeYoy")
            _qnichg = _le.get("netIncomeYoy")
            _frev   = _le.get("forecastRevenue")
            _foi    = _le.get("forecastOperatingIncome")
            _fni    = _le.get("forecastNetIncome")
            _feps   = _le.get("forecastEps")

            def _pct(v):
                return f"{v*100:+.1f}%" if v is not None else "―"
            def _mn(v):
                return f"{v:,}百万円" if v is not None else "―"

            lines += [
                f"## 直近決算短信（Q{_qt}、開示日: {_qdate}）",
                f"| 項目 | 当期実績 | 前年比 |",
                f"|------|----------|--------|",
                f"| 売上高 | {_mn(_qrev)} | {_pct(_qrchg)} |",
                f"| 営業利益 | {_mn(_qoi)} | {_pct(_qoichg)} |",
                f"| 純利益 | {_mn(_qni)} | {_pct(_qnichg)} |",
                f"| EPS | {_qeps:.2f}円 |  |" if _qeps else "| EPS | ― |  |",
                f"",
                f"### 通期業績予想",
                f"| 項目 | 予想 |",
                f"|------|------|",
                f"| 売上高（予想） | {_mn(_frev)} |",
                f"| 営業利益（予想） | {_mn(_foi)} |",
                f"| 純利益（予想） | {_mn(_fni)} |",
                f"| EPS（予想） | {_feps:.2f}円 |" if _feps else "| EPS（予想） | ― |",
                f"",
            ]

    # ── 3. 直近8四半期 決算推移 ──────────────────────────────────────────────
    if _earnings_list:
        lines += [
            f"## 直近決算推移（直近{len(_earnings_list)}件）",
            f"| 開示日 | 種別 | 売上高(百万) | 営業利益(百万) | 純利益(百万) | EPS |",
            f"|--------|------|-------------|--------------|------------|-----|",
        ]
        for _e in _earnings_list:
            _eq2  = f"Q{_e.get('quarter','?')}"
            _ed   = _e.get("disclosureDate", "")
            _er2  = _e.get("revenue")
            _eoi2 = _e.get("operatingIncome")
            _eni  = _e.get("netIncome")
            _eep  = _e.get("eps")
            _c_rev = f"{_er2:,}" if _er2 is not None else "―"
            _c_oi  = f"{_eoi2:,}" if _eoi2 is not None else "―"
            _c_ni  = f"{_eni:,}" if _eni is not None else "―"
            _c_ep  = f"{_eep:.2f}" if _eep is not None else "―"
            lines.append(f"| {_ed} | {_eq2} | {_c_rev} | {_c_oi} | {_c_ni} | {_c_ep} |")
        lines.append("")

    # ── 4. 分析依頼 ──────────────────────────────────────────────────────────
    lines += [
        f"---",
        f"",
        f"## 分析依頼",
        f"上記のデータをもとに、以下の観点からプロの株式アナリスト視点で分析してください。",
        f"出力は **日本語・Markdown形式** でお願いします。",
        f"",
        f"### 1. 直近決算サマリー",
        f"- 売上高・営業利益・純利益・EPS（前年同期比付き）",
        f"- 会社予想との乖離（上振れ/下振れ/概ね一致）",
        f"",
        f"### 2. 業績トレンド",
        f"- 売上・営業利益の推移と方向性",
        f"  （加速↑↑ / 改善↑ / 横ばい→ / 悪化↓ / 急落↓↓）",
        f"- 利益率の変化（改善or悪化）",
        f"",
        f"### 3. バリュエーション評価",
        f"- PER・PBR・EV/EBITDAの割安/割高判定",
        f"- 同業他社と比較した場合の水準感",
        f"",
        f"### 4. 注目ポイント",
        f"- ✅ 良かった点（2〜3点）",
        f"- ⚠️ 懸念点（2〜3点）",
        f"",
        f"### 5. 投資判断",
        f"次回決算までの間：**買い継続 / 様子見 / 要注意** のいずれかを根拠付きで。",
        f"目標株価の目安（PER基準）も示してください。",
    ]

    return "\n".join(lines)


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_earnings_calendar(date_str: str) -> list[dict]:
    """EdinetDB から指定日の決算発表一覧を取得してリストで返す。"""
    result = _call_edinetdb("get_earnings_calendar", date_from=date_str, date_to=date_str)
    # レスポンス形式: {"calendar": [...], "count": N, ...}
    return result.get("calendar", [])


def _save_earnings_results(date_str: str, rows: list[dict]) -> None:
    """取得した決算一覧を earnings_results.json に追記保存する。"""
    try:
        existing: dict = {}
        if os.path.exists(_EARNINGS_FILE):
            with open(_EARNINGS_FILE, encoding="utf-8") as f:
                existing = json.load(f)
        existing[date_str] = rows
        with open(_EARNINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _load_saved_earnings(date_str: str) -> list[dict]:
    """earnings_results.json から指定日のデータを読み込む。"""
    try:
        with open(_EARNINGS_FILE, encoding="utf-8") as f:
            return json.load(f).get(date_str, [])
    except Exception:
        return []


def _list_saved_dates() -> list[str]:
    """earnings_results.json に保存済みの日付一覧を新しい順で返す。"""
    try:
        with open(_EARNINGS_FILE, encoding="utf-8") as f:
            keys = list(json.load(f).keys())
        return sorted(keys, reverse=True)
    except Exception:
        return []


# ── Windows タスクスケジューラ ヘルパー ─────────────────────────────────────

def _task_status() -> str:
    """タスクの登録状態を返す: 'running' / 'ready' / 'none' / 'error'"""
    import subprocess
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             f"(Get-ScheduledTask -TaskName '{_TASK_NAME}' -ErrorAction SilentlyContinue).State"],
            capture_output=True, text=True, timeout=8,
        )
        state = r.stdout.strip().lower()
        if not state:
            return "none"
        return state   # 'ready', 'running', 'disabled', etc.
    except Exception:
        return "error"


def _task_register(hour: int, minute: int) -> tuple[bool, str]:
    """指定時刻に毎日実行するタスクを登録する。"""
    import subprocess
    ps1 = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "daily_earnings_report.ps1").replace("\\", "\\\\")
    cmd = (
        f"$a = New-ScheduledTaskAction -Execute 'powershell.exe' "
        f"-Argument '-ExecutionPolicy Bypass -WindowStyle Hidden -File \"{ps1}\"'; "
        f"$t = New-ScheduledTaskTrigger -Daily -At '{hour:02d}:{minute:02d}'; "
        f"$s = New-ScheduledTaskSettingsSet -StartWhenAvailable; "
        f"Register-ScheduledTask -TaskName '{_TASK_NAME}' "
        f"-Action $a -Trigger $t -Settings $s -Force | Out-Null; "
        f"Write-Output 'OK'"
    )
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-Command", cmd],
            capture_output=True, text=True, timeout=15,
        )
        if "OK" in r.stdout:
            return True, "タスクを登録しました"
        return False, r.stderr.strip() or r.stdout.strip()
    except Exception as e:
        return False, str(e)


def _task_delete() -> tuple[bool, str]:
    """タスクを削除する。"""
    import subprocess
    cmd = f"Unregister-ScheduledTask -TaskName '{_TASK_NAME}' -Confirm:$false; Write-Output 'OK'"
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-Command", cmd],
            capture_output=True, text=True, timeout=10,
        )
        if "OK" in r.stdout or r.returncode == 0:
            return True, "タスクを削除しました"
        return False, r.stderr.strip()
    except Exception as e:
        return False, str(e)


def _task_run_now() -> tuple[bool, str]:
    """タスクを今すぐ実行する。"""
    import subprocess
    cmd = f"Start-ScheduledTask -TaskName '{_TASK_NAME}'; Write-Output 'OK'"
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-Command", cmd],
            capture_output=True, text=True, timeout=10,
        )
        return True, "実行を開始しました"
    except Exception as e:
        return False, str(e)


# ── タブ本体 ─────────────────────────────────────────────────────────────────

def render_earnings_report_tab() -> None:
    import datetime as _dt
    import subprocess

    st.title("📰 決算カレンダー")

    # ════════════════════════════════════════════════════════════════════════
    # ① 日付選択 + 決算一覧取得
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("### 📅 決算発表日を選択して取得")

    _today = _dt.date.today()

    # ショートカットボタン
    _sc1, _sc2, _sc3, _sc4, _sc5 = st.columns(5)
    for _col, _label, _delta in [
        (_sc1, "今日",       0),
        (_sc2, "昨日",       1),
        (_sc3, "2日前",      2),
        (_sc4, "先週金曜日", (_today.weekday() + 3) % 7 + (7 if _today.weekday() < 4 else 0)),
        (_sc5, "先週月曜日", _today.weekday() + 7),
    ]:
        if _col.button(_label, key=f"er_sc_{_label}", use_container_width=True):
            st.session_state["er_sel_date"] = _today - _dt.timedelta(days=_delta)

    # 日付ピッカー + 取得ボタン
    _dp_col, _btn_col, _ref_col = st.columns([0.5, 0.2, 0.3])
    with _dp_col:
        er_date = st.date_input(
            "対象日付",
            value=st.session_state.get("er_sel_date", _today),
            max_value=_today,
            key="er_sel_date",
            label_visibility="collapsed",
        )
    with _btn_col:
        er_fetch = st.button("📥 取得", type="primary",
                             use_container_width=True, key="er_fetch_btn")
    with _ref_col:
        _saved_dates = _list_saved_dates()
        st.caption(f"保存済み: {len(_saved_dates)} 日分")

    # 取得 or キャッシュ読み込み
    er_date_str = str(er_date)
    if er_fetch:
        with st.spinner(f"{er_date_str} の決算情報を取得中..."):
            _fetch_earnings_calendar.clear()          # キャッシュクリアして再取得
            rows = _fetch_earnings_calendar(er_date_str)
        if rows:
            _save_earnings_results(er_date_str, rows)
            st.session_state["er_rows"]      = rows
            st.session_state["er_rows_date"] = er_date_str
            st.success(f"{er_date_str}：{len(rows)} 社の決算情報を取得しました")
        elif "error" in str(rows):
            st.error(f"取得エラー: {rows}")
        else:
            # APIレート制限の可能性 → 保存済みデータを確認
            saved = _load_saved_earnings(er_date_str)
            if saved:
                st.session_state["er_rows"]      = saved
                st.session_state["er_rows_date"] = er_date_str
                st.info(f"APIレート制限のため保存済みデータを表示します（{len(saved)} 社）")
            else:
                st.warning(f"{er_date_str} の決算発表はありません（またはAPIレート制限）")
                st.session_state["er_rows"] = []
    elif "er_rows_date" not in st.session_state:
        # 初回表示: 保存済みデータがあれば自動ロード
        saved = _load_saved_earnings(er_date_str)
        if saved:
            st.session_state["er_rows"]      = saved
            st.session_state["er_rows_date"] = er_date_str

    # ════════════════════════════════════════════════════════════════════════
    # ② 決算一覧の表示
    # ════════════════════════════════════════════════════════════════════════
    er_rows: list[dict] = st.session_state.get("er_rows", [])
    er_rows_date: str   = st.session_state.get("er_rows_date", "")

    if er_rows:
        st.divider()
        _th1, _th2 = st.columns([0.7, 0.3])
        with _th1:
            st.markdown(f"#### {er_rows_date} の決算発表　**{len(er_rows)} 社**")
        with _th2:
            # 全銘柄を監視銘柄に一括追加
            if st.button(f"📌 全{len(er_rows)}社を監視銘柄に追加",
                         key="er_add_all", use_container_width=True):
                _fund_df = load_funda_data()
                _added = 0
                for _r in er_rows:
                    _sc = str(_r.get("secCode", "")).rstrip("0")
                    if _sc and len(_sc) == 4:
                        _tk = f"{_sc}.T"
                        _existing = _fund_df["code"].tolist() if "code" in _fund_df.columns else []
                        if _tk not in _existing:
                            _fund_df = pd.concat(
                                [_fund_df, pd.DataFrame([{"code": _tk}])],
                                ignore_index=True,
                            )
                            _added += 1
                save_funda_data(_fund_df)
                st.success(f"{_added} 社を監視銘柄に追加しました")
                st.rerun()

        # テキスト検索
        _search = st.text_input("🔍 絞り込み（企業名 / コード）",
                                placeholder="例: 7203 またはトヨタ",
                                key="er_search", label_visibility="collapsed")

        # ヘッダー行
        _hw = [0.65, 1.7, 1.1, 0.9, 0.85, 0.45, 0.45]
        _hcols = st.columns(_hw)
        for _hc, _hl in zip(_hcols, ["コード", "企業名", "決算種別", "市場", "発表日", "監視", "AI"]):
            _hc.markdown(f"**{_hl}**")
        st.divider()

        # データ行
        _fund_df_now = load_funda_data()
        _existing_codes = set(_fund_df_now["code"].tolist()) if "code" in _fund_df_now.columns else set()

        for _r in er_rows:
            _raw_sc   = str(_r.get("secCode", ""))
            _sec_code = _raw_sc.rstrip("0") if _raw_sc.endswith("0") else _raw_sc
            _name     = _r.get("companyName") or _r.get("name") or _r.get("filerName") or "―"
            _qtype    = _r.get("periodType") or _r.get("reportType") or _r.get("quarter") or "―"
            _market   = _r.get("marketSegment") or _r.get("market") or _r.get("exchange") or "―"
            _dtime    = _r.get("announcementDate") or _r.get("disclosureTime") or "―"
            _ticker   = f"{_sec_code}.T"

            # 絞り込み
            if _search and _search not in _sec_code and _search not in _name:
                continue

            _rc = st.columns(_hw)
            _rc[0].markdown(f"`{_sec_code}`")
            _rc[1].write(_name)
            _rc[2].write(str(_qtype))
            _rc[3].write(str(_market))
            _rc[4].write(str(_dtime))

            _already = _ticker in _existing_codes
            if _already:
                _rc[5].markdown("✅")
            else:
                if _rc[5].button("＋", key=f"er_add_{_sec_code}",
                                 help="監視銘柄に追加"):
                    _fund_df_now = pd.concat(
                        [_fund_df_now, pd.DataFrame([{"code": _ticker}])],
                        ignore_index=True,
                    )
                    save_funda_data(_fund_df_now)
                    _existing_codes.add(_ticker)
                    st.rerun()

            # AI分析ボタン
            if _rc[6].button("🤖", key=f"er_ai_{_sec_code}", help="AI決算分析"):
                st.session_state["er_ai_target"] = {
                    "secCode":     _sec_code,
                    "companyName": _name,
                    "periodType":  _qtype,
                    "market":      _market,
                    "date":        er_rows_date,
                }
                st.session_state.pop("er_ai_prompt", None)   # 旧プロンプトクリア
                st.session_state.pop("er_ai_data", None)

        # ── AI決算分析パネル ──────────────────────────────────────────────
        _ai_tgt = st.session_state.get("er_ai_target")
        if _ai_tgt:
            st.divider()
            _ai_sc   = _ai_tgt["secCode"]
            _ai_name = _ai_tgt["companyName"]
            _ai_qt   = _ai_tgt["periodType"]
            _ai_mkt  = _ai_tgt["market"]
            _ai_date = _ai_tgt["date"]

            st.markdown(f"### 🤖 AI決算分析　`{_ai_sc}` {_ai_name}")
            st.caption(f"決算種別: {_ai_qt}　／　市場: {_ai_mkt}　／　発表日: {_ai_date}")

            # ── データ取得 & プロンプト生成 ──────────────────────────────
            _ai_gen_col, _ai_clr_col = st.columns([0.3, 0.7])
            with _ai_gen_col:
                if st.button("📥 財務データ取得してプロンプト生成",
                             key="er_ai_gen_btn", type="primary"):
                    with st.spinner(f"{_ai_name} の財務データを取得中..."):
                        _ai_prompt_text = _build_earnings_prompt(
                            _ai_sc, _ai_name, _ai_qt, _ai_mkt, _ai_date
                        )
                    st.session_state["er_ai_prompt"] = _ai_prompt_text
            with _ai_clr_col:
                if st.button("✖️ 閉じる", key="er_ai_close"):
                    st.session_state.pop("er_ai_target", None)
                    st.session_state.pop("er_ai_prompt", None)
                    st.rerun()

            # ── プロンプト表示 & 編集 ─────────────────────────────────────
            if "er_ai_prompt" in st.session_state:
                _prompt_val = st.session_state["er_ai_prompt"]

                st.markdown("#### 📋 生成されたプロンプト（編集可能）")
                _edited = st.text_area(
                    "プロンプト",
                    value=_prompt_val,
                    height=320,
                    key="er_ai_prompt_editor",
                    label_visibility="collapsed",
                )

                # ── AI サービス選択ボタン ────────────────────────────────
                st.markdown("**▼ 以下のAIにプロンプトをコピー＆ペーストして分析依頼してください**")
                _ai_b1, _ai_b2, _ai_b3 = st.columns(3)
                _ai_b1.link_button(
                    "🟣 Claude で分析",
                    url="https://claude.ai/new",
                    use_container_width=True,
                )
                _ai_b2.link_button(
                    "🟢 ChatGPT で分析",
                    url="https://chatgpt.com/",
                    use_container_width=True,
                )
                _ai_b3.link_button(
                    "🔵 Gemini で分析",
                    url="https://gemini.google.com/",
                    use_container_width=True,
                )

                # ── プロンプト コードブロック（コピー用） ───────────────
                with st.expander("📄 プロンプト（コピー用コードブロック）", expanded=False):
                    st.code(_edited, language="markdown")

                # ── 分析結果の保存 ────────────────────────────────────────
                st.markdown("#### 💾 分析結果を保存")
                st.caption("AIから返ってきたMarkdown結果をここに貼り付けて保存します。")
                _result_input = st.text_area(
                    "AIの分析結果（Markdown）",
                    height=200,
                    placeholder="AIチャットから分析結果をここに貼り付けてください...",
                    key="er_ai_result_input",
                    label_visibility="collapsed",
                )
                import datetime as _dt2
                _today_str2 = _dt2.date.today().strftime("%Y%m%d")
                _default_fname = f"{_ai_sc}_earnings_{_today_str2}.md"
                _save_fname = st.text_input(
                    "保存ファイル名",
                    value=_default_fname,
                    key="er_ai_save_fname",
                )
                if st.button("💾 analysis_results/ に保存", key="er_ai_save_btn",
                             disabled=not _result_input.strip()):
                    os.makedirs(_ANALYSIS_RESULTS_DIR, exist_ok=True)
                    _save_path = os.path.join(_ANALYSIS_RESULTS_DIR, _save_fname)
                    with open(_save_path, "w", encoding="utf-8") as _sf:
                        _sf.write(_result_input)
                    st.success(f"保存しました: `{_save_path}`")

        # CSV ダウンロード
        st.divider()
        _csv_df = pd.DataFrame([{
            "secCode":    str(_r.get("secCode", "")).rstrip("0"),
            "企業名":     _r.get("companyName") or _r.get("name") or "",
            "決算種別":   _r.get("periodType") or _r.get("reportType") or "",
            "市場":       _r.get("marketSegment") or _r.get("market") or "",
            "発表日":     _r.get("announcementDate") or "",
        } for _r in er_rows])
        st.download_button(
            "📥 CSVダウンロード",
            data=_csv_df.to_csv(index=False, encoding="utf-8-sig"),
            file_name=f"earnings_{er_rows_date}.csv",
            mime="text/csv",
            key="er_csv_dl",
        )

    elif er_fetch:
        pass   # すでに上でメッセージ表示済み

    # ════════════════════════════════════════════════════════════════════════
    # ③ 保存データ横断検索（銘柄コード / 企業名）
    # ════════════════════════════════════════════════════════════════════════
    st.divider()
    st.markdown("### 🔎 保存データから銘柄を検索")
    st.caption("earnings_results.json に保存済みの全日付から、銘柄コードまたは企業名で検索します。")

    _xsearch = st.text_input(
        "銘柄コードまたは企業名",
        placeholder="例: 7203 または トヨタ",
        key="er_xsearch",
        label_visibility="collapsed",
    )

    if _xsearch:
        try:
            with open(_EARNINGS_FILE, encoding="utf-8") as _xf:
                _all_data: dict = json.load(_xf)
        except Exception:
            _all_data = {}

        _xresults: list[tuple[str, dict]] = []  # (date_str, record)
        for _xdate, _xrows in sorted(_all_data.items(), reverse=True):
            for _xr in _xrows:
                _xsc   = str(_xr.get("secCode", "")).rstrip("0")
                _xname = _xr.get("companyName") or _xr.get("name") or ""
                if _xsearch in _xsc or _xsearch.lower() in _xname.lower():
                    _xresults.append((_xdate, _xr))

        if _xresults:
            st.markdown(f"**{len(_xresults)} 件** 見つかりました")
            _xhw = [0.7, 1.8, 1.2, 1.0, 0.8]
            _xhcols = st.columns(_xhw)
            for _xhc, _xhl in zip(_xhcols, ["発表日", "企業名", "決算種別", "市場", "コード"]):
                _xhc.markdown(f"**{_xhl}**")
            st.divider()
            for _xdate, _xr in _xresults:
                _xsc2  = str(_xr.get("secCode", "")).rstrip("0")
                _xname2 = _xr.get("companyName") or _xr.get("name") or "―"
                _xqt   = _xr.get("periodType") or _xr.get("reportType") or "―"
                _xmkt  = _xr.get("marketSegment") or _xr.get("market") or "―"
                _xrc   = st.columns(_xhw)
                _xrc[0].write(_xdate)
                _xrc[1].write(_xname2)
                _xrc[2].write(str(_xqt))
                _xrc[3].write(str(_xmkt))
                _xrc[4].markdown(f"`{_xsc2}`")
        else:
            st.info(f"「{_xsearch}」に一致するデータが保存データ内に見つかりませんでした。")

    # ════════════════════════════════════════════════════════════════════════
    # ④ 自動取得スケジュール（Windows タスクスケジューラ）
    # ════════════════════════════════════════════════════════════════════════
    st.divider()
    st.markdown("### ⏰ 自動取得スケジュール")

    if not _IS_WINDOWS:
        st.info(
            "⚠️ **この機能はWindowsローカル版専用です。**\n\n"
            "Streamlit Cloud（スマホ・クラウド）では自動スケジュール登録はできません。\n"
            "決算カレンダーは「📥 取得」ボタンから手動で取得してください。",
            icon="🖥️",
        )
    else:
        _status = _task_status()
        _status_map = {
            "ready":    ("🟢 登録済み（待機中）", "success"),
            "running":  ("🔵 実行中",             "info"),
            "disabled": ("🟡 無効",               "warning"),
            "none":     ("⚫ 未登録",              ""),
            "error":    ("🔴 確認エラー",          "error"),
        }
        _status_label, _status_type = _status_map.get(_status, (f"❓ {_status}", ""))
        st.markdown(f"**現在の状態：** {_status_label}")

        _sa1, _sa2, _sa3, _sa4 = st.columns([0.3, 0.2, 0.2, 0.3])
        with _sa1:
            _reg_h = st.number_input("実行時刻（時）", min_value=0, max_value=23, value=20,
                                      key="er_task_hour", label_visibility="visible")
            _reg_m = st.number_input("実行時刻（分）", min_value=0, max_value=59, value=0,
                                      step=5, key="er_task_min", label_visibility="visible")
        with _sa2:
            st.write("")
            st.write("")
            if st.button("📝 登録 / 更新", key="er_task_reg", use_container_width=True,
                         type="primary"):
                _ok, _msg = _task_register(int(_reg_h), int(_reg_m))
                (st.success if _ok else st.error)(f"{_msg}")
                st.rerun()
        with _sa3:
            st.write("")
            st.write("")
            if st.button("🗑️ 削除", key="er_task_del", use_container_width=True,
                         disabled=(_status == "none")):
                _ok, _msg = _task_delete()
                (st.success if _ok else st.error)(f"{_msg}")
                st.rerun()
        with _sa4:
            st.write("")
            st.write("")
            if st.button("▶️ 今すぐ実行", key="er_task_now", use_container_width=True,
                         disabled=(_status not in ("ready", "disabled"))):
                _ok, _msg = _task_run_now()
                (st.success if _ok else st.error)(f"{_msg}")
            if st.button("🔄 状態を更新", key="er_task_refresh", use_container_width=True):
                st.rerun()

        st.caption(
            f"タスク名: `{_TASK_NAME}`　　"
            "毎日指定時刻に決算カレンダーを取得して `earnings_results.json` に保存します。"
            "（スクリプト: `daily_earnings_report.bat`）"
        )

    # ════════════════════════════════════════════════════════════════════════
    # ④ 保存済みデータ一覧
    # ════════════════════════════════════════════════════════════════════════
    st.divider()
    st.markdown("### 📂 保存済みデータ")

    _saved_list = _list_saved_dates()
    if not _saved_list:
        st.info("まだ保存済みデータはありません。「📥 取得」ボタンで取得してください。")
    else:
        _sl_col, _sl_ref = st.columns([0.6, 0.4])
        with _sl_col:
            _sel_saved = st.selectbox(
                "日付を選択",
                options=_saved_list,
                key="er_saved_select",
                label_visibility="collapsed",
            )
        with _sl_ref:
            if st.button("この日付のデータを表示", key="er_load_saved",
                         use_container_width=True):
                _loaded = _load_saved_earnings(_sel_saved)
                st.session_state["er_rows"]      = _loaded
                st.session_state["er_rows_date"] = _sel_saved
                st.rerun()


# ===========================================================================
# 画面描画: バックテストタブ
# ===========================================================================

def render_backtest_tab() -> None:
    st.title("📈 バックテスト（ドンチャン + EMAクロス）")
    st.markdown("""
    ドンチャンブレイクアウト＋EMAクロス決済戦略の過去パフォーマンスを検証します。
    - 📌 **エントリー**: ドンチャン上抜け → 遅延日数後に出来高・DD・価格継続を確認 → **翌日始値**でエントリー
    - 🔴 **決済**: EMA(fast) < EMA(slow) のデッドクロス → **翌日始値**で決済
    - 🔍 **フィルター**: スクリーナーと同一条件（遅延日数 / 待機DD / 出来高倍率）を適用可能
    - 📊 エントリー時スコアと実際のリターンの相関を分析
    """)
    st.divider()

    # ────────────────────────────────────────────────────────────────────────
    # ① バックテスト設定
    # ────────────────────────────────────────────────────────────────────────
    st.markdown("### ⚙️ バックテスト設定")

    # 基本設定（1行目）— 日付レンジ + ショートカット
    import datetime as _dt

    _today     = _dt.date.today()
    _dt_max    = _today
    _dt_min    = _dt.date(2000, 1, 1)  # yfinance が対応する最古水準

    # ショートカットボタンで開始日を変える
    _sc_labels = {"1ヶ月": 30, "3ヶ月": 90, "6ヶ月": 180,
                  "1年": 365, "3年": 365*3, "5年": 365*5, "10年": 365*10}
    _sc_cols   = st.columns(len(_sc_labels))
    for _sc_col, (_lbl, _days) in zip(_sc_cols, _sc_labels.items()):
        if _sc_col.button(_lbl, key=f"bt_sc_{_lbl}", use_container_width=True):
            st.session_state["bt_start_date"] = _today - _dt.timedelta(days=_days)
            st.session_state["bt_end_date"]   = _today

    _row1_c1, _row1_c2 = st.columns([0.5, 0.5])
    with _row1_c1:
        _d_cols = st.columns(2)
        with _d_cols[0]:
            bt_start = st.date_input(
                "📅 開始日",
                value=st.session_state.get("bt_start_date", _today - _dt.timedelta(days=365*3)),
                min_value=_dt_min,
                max_value=_dt_max,
                key="bt_start_date",
                help="バックテスト開始日（yfinanceの取得可能範囲内）",
            )
        with _d_cols[1]:
            bt_end = st.date_input(
                "📅 終了日",
                value=st.session_state.get("bt_end_date", _today),
                min_value=_dt_min,
                max_value=_dt_max,
                key="bt_end_date",
                help="バックテスト終了日",
            )
        # 日数・開始>終了チェック
        if bt_start >= bt_end:
            st.error("開始日は終了日より前にしてください。")
            bt_start = bt_end - _dt.timedelta(days=365)
        _bt_days = (bt_end - bt_start).days
        st.caption(f"検証期間: **{bt_start}** 〜 **{bt_end}**　（約 {_bt_days} 日 / {_bt_days//365}年{(_bt_days%365)//30}ヶ月）")

    with _row1_c2:
        _ind_cols = st.columns(3)
        with _ind_cols[0]:
            donchian_days = st.number_input(
                "📐 ドンチャン期間（日）",
                min_value=5, max_value=60, value=20, step=1,
                help="N日高値上抜けをエントリー条件とする日数",
            )
        with _ind_cols[1]:
            ema_fast = st.number_input(
                "📉 EMA 短期",
                min_value=2, max_value=50, value=5, step=1,
                help="デッドクロス判定の短期EMA期間",
            )
        with _ind_cols[2]:
            ema_slow = st.number_input(
                "📈 EMA 長期",
                min_value=5, max_value=200, value=20, step=1,
                help="デッドクロス判定の長期EMA期間",
            )

    use_5day = st.checkbox(
        "過去5日以内ブレイクも対象",
        value=False,
        help="delay_days 後のウィンドウを±5日に拡張してブレイクシグナルを探す",
    )

    # start/end を文字列に変換（yfinance に渡す）
    bt_start_str = str(bt_start)
    bt_end_str   = str(bt_end)

    # フィルター設定（2行目）— スクリーナーと同一条件
    st.markdown("#### 🔍 エントリーフィルター（スクリーナーと同一）")
    f1, f2, f3 = st.columns(3)
    with f1:
        bt_delay = st.selectbox(
            "⏱️ 遅延日数（delay_days）",
            options=[0, 3, 5, 10, 20],
            index=0,
            format_func=lambda x: f"{x}日" if x > 0 else "即時（0日）",
            key="bt_delay",
            help="ブレイク後この日数が経過してから条件チェックしエントリー",
        )
    with f2:
        bt_dd = st.selectbox(
            "🛡️ 待機DD閾値（%）",
            options=[-1.0, -2.0, -3.0, -5.0, -7.0, -10.0, -100.0],
            index=6,
            format_func=lambda x: "制限なし" if x == -100.0 else f"{x:.1f}%",
            key="bt_dd",
            help="待機期間中の最大下落率の下限（-3.0% → 3%超の下落はNG）。0日のときは無効。",
        )
    with f3:
        bt_vol = st.slider(
            "🔥 出来高倍率 閾値",
            min_value=1.0, max_value=5.0, value=1.0, step=0.1,
            format="%.1f×",
            key="bt_vol",
            help="ブレイク日の出来高 ÷ N日平均出来高がこの値以上の銘柄のみ通過（1.0=制限なし）",
        )

    st.caption(
        f"📌 エントリー条件: ドンチャン{donchian_days}日 ｜ "
        f"遅延{bt_delay}日 ｜ "
        f"待機DD > {'制限なし' if bt_dd == -100.0 else f'{bt_dd:.1f}%'} ｜ "
        f"出来高 ≥ {bt_vol:.1f}×　／　"
        f"決済: EMA({ema_fast}/{ema_slow}) デッドクロス翌日始値"
    )

    # ────────────────────────────────────────────────────────────────────────
    # ② 銘柄リスト（プリセット / CSV アップロード / 手動入力）
    # ────────────────────────────────────────────────────────────────────────
    st.markdown("### 📋 銘柄リスト")

    # ── プリセット選択 ────────────────────────────────────────────────────
    bt_preset_col, csv_col = st.columns([0.45, 0.55])
    with bt_preset_col:
        bt_preset = st.selectbox("入力方法", PRESET_OPTIONS, key="bt_preset_select")

    # プリセット切り替え時のみテキストを上書き
    # bt_ticker_area（text_area の key）も同時に更新することで表示に即反映される
    if bt_preset != st.session_state.bt_prev_preset:
        if bt_preset == PRESET_OPTIONS[1]:
            preset_str = ", ".join(TOPIX100_TICKERS)
            st.session_state.bt_ticker_input = preset_str
            st.session_state.bt_ticker_area  = preset_str
        elif bt_preset == PRESET_OPTIONS[2]:
            preset_str = ", ".join(NIKKEI225_TICKERS)
            st.session_state.bt_ticker_input = preset_str
            st.session_state.bt_ticker_area  = preset_str
        st.session_state.bt_prev_preset = bt_preset

    # ── CSV アップロード ──────────────────────────────────────────────────
    with csv_col:
        uploaded_csv = st.file_uploader(
            "📂 銘柄CSVをアップロード",
            type=["csv"],
            key="bt_csv_uploader",
            help=(
                "ticker 列（例: 7203.T）または code 列（例: 7203）を含む CSV。\n"
                "アップロードするとテキストエリアの内容を自動置換します。"
            ),
        )

    # CSV が新たにアップロードされたとき: 内容をテキストエリアに反映
    if uploaded_csv is not None:
        # ファイルが変わったときだけ再パース（file_id で判定）
        csv_file_id = getattr(uploaded_csv, "file_id", uploaded_csv.name)
        if st.session_state.get("bt_csv_file_id") != csv_file_id:
            st.session_state.bt_csv_file_id = csv_file_id
            parsed, err = _parse_csv_tickers(uploaded_csv)
            if err:
                st.error(f"❌ {err}")
            else:
                ticker_str = ", ".join(parsed)
                # bt_ticker_input: プリセット保護などの内部変数を更新
                st.session_state.bt_ticker_input = ticker_str
                # bt_ticker_area: st.text_area の key と同名 → ウィジェット表示値を直接更新
                # （この変数が text_area より先にセットされることで次回描画に反映される）
                st.session_state.bt_ticker_area  = ticker_str
                st.success(
                    f"✅ CSV 読み込み成功 — **{len(parsed)} 銘柄**をテキストエリアに反映しました。"
                )

    # ── テキストエリア（プリセット / CSV / 手動 で共有） ─────────────────
    # key="bt_ticker_area" を持つ text_area は st.session_state["bt_ticker_area"] で管理される。
    # CSV / プリセット 切り替え時は上記で bt_ticker_area を直接更新済みなので
    # ここでは常に session_state の値をデフォルトとして渡すだけでよい。
    # （key 指定ウィジェットは value= より session_state[key] を優先するため、
    #   bt_ticker_area がセット済みの場合は value= は無視される）
    if "bt_ticker_area" not in st.session_state:
        st.session_state.bt_ticker_area = st.session_state.bt_ticker_input

    bt_ticker_input = st.text_area(
        "銘柄コード（カンマ区切り）",
        height=80,
        key="bt_ticker_area",
        label_visibility="collapsed",
        help="4桁数字も可（7203 → 7203.T に自動変換）。CSVアップロード後は手動編集も可能。",
    )
    # 手動入力モード時のみ bt_ticker_input（内部変数）を同期
    if bt_preset == PRESET_OPTIONS[0] and uploaded_csv is None:
        st.session_state.bt_ticker_input = bt_ticker_input

    raw_bt = [t.strip() for t in bt_ticker_input.split(",") if t.strip()]
    st.caption(f"銘柄数: **{len(raw_bt)}** 件")

    # ────────────────────────────────────────────────────────────────────────
    # ③ 実行 / クリア ボタン
    # ────────────────────────────────────────────────────────────────────────
    run_col, clr_col, _ = st.columns([0.22, 0.10, 0.68])
    with run_col:
        run_bt = st.button("🚀 バックテスト実行", type="primary", use_container_width=True)
    with clr_col:
        if st.button("クリア", key="bt_clear", use_container_width=True):
            st.session_state.bt_results    = None
            st.session_state.bt_raw_trades = []
            st.rerun()

    # ────────────────────────────────────────────────────────────────────────
    # ④ バックテスト実行処理
    # ────────────────────────────────────────────────────────────────────────
    if run_bt:
        if not raw_bt:
            st.warning("銘柄コードを入力してください。")
        else:
            tickers    = [normalize_ticker(t) for t in raw_bt]
            all_trades: list[dict] = []
            errors:     list[str]  = []

            prog = st.progress(0, text=f"バックテスト開始... (0 / {len(tickers)})")
            stat = st.empty()

            for idx, ticker in enumerate(tickers):
                prog.progress(
                    idx / len(tickers),
                    text=f"処理中... {idx + 1} / {len(tickers)}  |  累計トレード: {len(all_trades)} 件",
                )
                stat.caption(f"🔍 {ticker} を検証中")

                trades, err = backtest_ticker(
                    ticker            = ticker,
                    start_date        = bt_start_str,
                    end_date          = bt_end_str,
                    donchian_days     = int(donchian_days),
                    ema_fast          = int(ema_fast),
                    ema_slow          = int(ema_slow),
                    use_5day_lookback = use_5day,
                    delay_days        = int(bt_delay),
                    vol_mult_thr      = float(bt_vol),
                    dd_threshold      = float(bt_dd),
                )
                all_trades.extend(trades)
                if err:
                    errors.append(f"{ticker}: {err}")

            prog.progress(1.0, text=f"✅ 完了  {len(all_trades)} トレードを収集")
            stat.empty()

            # 生トレードを保存（戦略比較用: _bp/_vr/_ts/_ar/_comp を含む）
            st.session_state.bt_raw_trades = all_trades
            # スコア正規化 & DataFrame 整形
            st.session_state.bt_results = normalize_bt_scores(all_trades)

            if errors:
                with st.expander(f"⚠️ スキップされた銘柄 ({len(errors)} 件)"):
                    for e in errors:
                        st.caption(e)

    # ────────────────────────────────────────────────────────────────────────
    # ⑤ 結果表示
    # ────────────────────────────────────────────────────────────────────────
    if st.session_state.bt_results is None:
        return

    result_df = st.session_state.bt_results
    st.divider()

    if result_df.empty:
        st.info("🔍 有効なトレードが見つかりませんでした。設定を変更して再実行してください。")
        return

    returns      = result_df["return(%)"].dropna()
    n_trades     = len(returns)
    win_rate     = (returns > 0).sum() / n_trades * 100 if n_trades > 0 else 0.0
    avg_return   = float(returns.mean())
    cum_return   = float(returns.sum())
    avg_hold     = float(result_df["holding_days"].dropna().mean())

    # ── サマリーメトリクス ────────────────────────────────────────────────
    st.markdown("### 📊 バックテスト結果サマリー")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("📈 トレード数",    f"{n_trades} 件")
    m2.metric("🎯 勝率",          f"{win_rate:.1f}%")
    m3.metric("📉 平均リターン",  f"{avg_return:+.2f}%")
    m4.metric("💹 累積リターン",  f"{cum_return:+.1f}%")

    # 詳細統計（折りたたみ）
    with st.expander("📋 詳細統計"):
        wins  = returns[returns > 0]
        loses = returns[returns <= 0]
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("勝ちトレード",  f"{len(wins)} 件")
        d2.metric("負けトレード",  f"{len(loses)} 件")
        d3.metric("平均利益",      f"{wins.mean():+.2f}%"  if len(wins)  > 0 else "N/A")
        d4.metric("平均損失",      f"{loses.mean():+.2f}%" if len(loses) > 0 else "N/A")
        st.caption(
            f"平均保有日数: **{avg_hold:.1f}日** ／ "
            f"最大利益: **{returns.max():+.2f}%** ／ "
            f"最大損失: **{returns.min():+.2f}%**"
        )

    st.divider()

    # ── トレード一覧テーブル ──────────────────────────────────────────────
    st.markdown("### 📋 トレード一覧")

    def _color_return(val):
        """リターン列の文字色を勝敗で変える"""
        try:
            v = float(val)
            if v > 0:
                return "color: #4caf50; font-weight: bold"
            elif v < 0:
                return "color: #f44336; font-weight: bold"
        except Exception:
            pass
        return ""

    fmt_trades = {k: v for k, v in {
        "entry_price":  "{:,.4f}",
        "exit_price":   "{:,.4f}",
        "return(%)":    "{:+.2f}%",
        "スコア":       "{:.1f}",
    }.items() if k in result_df.columns}

    styled_trades = (
        result_df.style
        .format(fmt_trades)
        .map(_color_return, subset=["return(%)"] if "return(%)" in result_df.columns else [])
    )
    st.dataframe(styled_trades, use_container_width=True, hide_index=True)

    # ── スコア別リターン分析 ──────────────────────────────────────────────
    if "スコア" in result_df.columns:
        st.divider()
        st.markdown("### 🏆 スコア別リターン分析")
        st.caption(
            "エントリー時のスコア（ブレイク強度・出来高・トレンド・ATR比率・レンジ圧縮の複合）"
            "が高いトレードほど、EMAクロス決済後のリターンが良いかを検証します。"
        )

        score_df = result_df[["スコア", "return(%)"]].dropna()

        if len(score_df) < 5:
            st.info("スコア分析には5件以上のトレードが必要です。")
        else:
            try:
                score_df = score_df.copy()
                # スコアを5分位に分類（重複値があっても drop で対応）
                score_df["スコア帯"] = pd.qcut(
                    score_df["スコア"], q=5,
                    labels=["最低(0-20)", "低(20-40)", "中(40-60)", "高(60-80)", "最高(80-100)"],
                    duplicates="drop",
                )

                score_analysis = (
                    score_df.groupby("スコア帯", observed=True)["return(%)"]
                    .agg(
                        トレード数  = "count",
                        平均リターン = "mean",
                        勝率        = lambda x: (x > 0).sum() / len(x) * 100,
                        最大        = "max",
                        最小        = "min",
                    )
                    .round(2)
                    .reset_index()
                )
                score_analysis.columns = [
                    "スコア帯", "トレード数", "平均リターン(%)", "勝率(%)", "最大(%)", "最小(%)"
                ]

                def _color_avg(val):
                    try:
                        v = float(val)
                        return (
                            "color: #4caf50; font-weight: bold" if v > 0
                            else "color: #f44336; font-weight: bold"
                        )
                    except Exception:
                        return ""

                styled_score = (
                    score_analysis.style
                    .format({
                        "平均リターン(%)": "{:+.2f}%",
                        "勝率(%)":        "{:.1f}%",
                        "最大(%)":        "{:+.2f}%",
                        "最小(%)":        "{:+.2f}%",
                    })
                    .map(_color_avg, subset=["平均リターン(%)"])
                )
                st.dataframe(styled_score, use_container_width=True, hide_index=True)

                # スコア帯別の平均リターンをバーチャートで可視化
                chart_data = score_analysis.set_index("スコア帯")["平均リターン(%)"]
                st.bar_chart(chart_data, use_container_width=True, height=250)

            except Exception as e:
                st.warning(f"スコア分析中にエラーが発生しました: {e}")

    # ── 高度分析（銘柄別 / 相関 / セクター） ────────────────────────────
    _render_advanced_analysis(result_df)

    # ── 複数スコア戦略の比較 ─────────────────────────────────────────────
    render_strategy_comparison(st.session_state.get("bt_raw_trades", []))

    # ── 時間フィルターバックテスト ───────────────────────────────────────
    render_time_filter_backtest()

    # ── パラメータ最適化モード ───────────────────────────────────────────
    render_param_optimization()


# ===========================================================================
# 時間フィルターバックテスト: 1銘柄シミュレーション
# ===========================================================================

DELAY_DAYS_LIST: list[int] = [0, 3, 5, 10, 15, 20, 30]

def delayed_backtest_ticker(
    ticker:          str,
    start_date:      str,
    end_date:        str,
    donchian_days:   int   = 20,
    ema_fast:        int   = 5,
    ema_slow:        int   = 20,
    delay_days_list: list  = DELAY_DAYS_LIST,
    max_dd_pct:      float = 5.0,   # ブレイク後・エントリー前の最大許容ドローダウン(%)
) -> tuple[list[dict], str | None]:
    """
    ドンチャンブレイクアウト後に delay_days 待機してからエントリーする戦略を検証。

    エントリー条件（delay 日後）:
        1. close[entry_i] > breakout_price（依然としてブレイク水準を上回っている）
        2. EMA(fast)[entry_i] > EMA(slow)[entry_i]（上昇トレンド継続）
        3. ブレイク日からエントリー日の間の最大下落が max_dd_pct% 以内

    決済条件:
        EMA(fast) < EMA(slow) → 当日終値で決済
        最終日に保有中 → 終値で決済

    Returns:
        (trades_list, error_message)
        各 trade dict に "delay_days" キーを含む。
    """
    try:
        df = yf.download(ticker, start=start_date, end=end_date,
                         progress=False, auto_adjust=True)
        if df.empty:
            return [], "データなし"
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        min_rows = max(donchian_days + max(delay_days_list) + 5, ema_slow + 10)
        if len(df) < min_rows:
            return [], f"データ不足（{len(df)}日）"

        # ── テクニカル指標（Series → numpy 配列）────────────────────────
        donchian_s   = df["High"].rolling(donchian_days).max().shift(1)
        ema_fast_s   = df["Close"].ewm(span=ema_fast, adjust=False).mean()
        ema_slow_s   = df["Close"].ewm(span=ema_slow, adjust=False).mean()

        close_arr    = df["Close"].to_numpy(dtype=float)
        donchian_arr = donchian_s.to_numpy(dtype=float)
        ema_fast_arr = ema_fast_s.to_numpy(dtype=float)
        ema_slow_arr = ema_slow_s.to_numpy(dtype=float)
        dates        = df.index
        n            = len(df)

        trades: list[dict] = []
        start_i = max(donchian_days + 2, ema_slow + 5)

        # ── 各 delay でシミュレーションを独立実行 ────────────────────────
        for delay in delay_days_list:
            in_trade    = False
            entry_i     = -1
            last_exit_i = -1
            entry_price = 0.0
            bo_price    = 0.0   # ブレイク時の close（エントリー条件の比較基準）

            for i in range(start_i, n):

                # ① ポジション保有中: EMAクロスで決済
                if in_trade:
                    ef = ema_fast_arr[i]
                    es = ema_slow_arr[i]
                    exited     = False
                    exit_price = 0.0
                    reason     = ""

                    if not np.isnan(ef) and not np.isnan(es) and ef < es:
                        exit_price = close_arr[i]
                        reason     = "ema_cross"
                        exited     = True
                    elif i == n - 1:
                        exit_price = close_arr[i]
                        reason     = "end_of_data"
                        exited     = True

                    if exited:
                        ret          = (exit_price - entry_price) / entry_price * 100
                        holding_days = i - entry_i
                        trades[-1].update({
                            "exit_date":    str(dates[i].date()),
                            "exit_price":   round(float(exit_price), 4),
                            "return(%)":    round(ret, 2),
                            "holding_days": holding_days,
                            "exit_reason":  reason,
                        })
                        last_exit_i = i
                        in_trade    = False

                # ② ノーポジション: ブレイクシグナル検出
                else:
                    dc = donchian_arr[i]
                    if np.isnan(dc):
                        continue
                    # ブレイク検出（当日）
                    if close_arr[i] <= dc or i <= last_exit_i:
                        continue

                    # ブレイク発生日 = i、遅延エントリー候補日 = i + delay
                    bo_i      = i
                    entry_idx = bo_i + delay

                    if entry_idx >= n:
                        continue   # データ終端を超える場合はスキップ

                    bo_close = float(close_arr[bo_i])   # ブレイク時の終値

                    # ── エントリー条件チェック（delay 日後） ──────────────
                    c_e   = float(close_arr[entry_idx])
                    ef_e  = float(ema_fast_arr[entry_idx])
                    es_e  = float(ema_slow_arr[entry_idx])

                    # 条件①: まだブレイク水準を上回っているか
                    if c_e <= bo_close:
                        continue

                    # 条件②: EMA トレンド確認（EMA fast > slow）
                    if np.isnan(ef_e) or np.isnan(es_e) or ef_e <= es_e:
                        continue

                    # 条件③: ブレイク後のドローダウンが許容範囲内か
                    if delay > 0:
                        window_close = close_arr[bo_i: entry_idx + 1]
                        peak         = float(np.nanmax(window_close))
                        trough       = float(np.nanmin(window_close))
                        dd_pct       = (trough - peak) / peak * 100  # 負値
                        if abs(dd_pct) > max_dd_pct:
                            continue   # ドローダウンが大きすぎる

                    # ── エントリー実行 ────────────────────────────────────
                    in_trade    = True
                    entry_i     = entry_idx
                    entry_price = c_e

                    trades.append({
                        "delay_days":   delay,
                        "ティッカー":   ticker,
                        "breakout_date": str(dates[bo_i].date()),
                        "entry_date":   str(dates[entry_idx].date()),
                        "entry_price":  round(entry_price, 4),
                        "exit_date":    None,
                        "exit_price":   None,
                        "return(%)":    None,
                        "holding_days": None,
                        "exit_reason":  None,
                    })

                    # 次のブレイク探索は決済後まで待機
                    # （ループは entry_idx の次から継続するが in_trade=True で保護）

        return trades, None

    except Exception as e:
        return [], str(e)


# ===========================================================================
# 時間フィルターバックテスト: 集計ヘルパー
# ===========================================================================

def _summarize_by_delay(trades_df: pd.DataFrame) -> pd.DataFrame:
    """
    delay_days ごとにパフォーマンス指標を集計して比較テーブルを返す。
    """
    rows = []
    for delay, grp in trades_df.groupby("delay_days"):
        rets = grp["return(%)"].dropna().astype(float)
        n    = len(rets)
        if n == 0:
            continue

        total_ret = float(rets.sum())
        avg_ret   = float(rets.mean())
        win_rate  = float((rets > 0).sum() / n * 100)

        # 最大ドローダウン（累積リターン曲線ベース）
        cum      = (1 + rets / 100).cumprod()
        roll_max = cum.cummax()
        max_dd   = float(((cum - roll_max) / roll_max * 100).min())

        # シャープレシオ（簡易）
        std    = float(rets.std())
        sharpe = round(avg_ret / std, 3) if std > 0 else 0.0

        avg_hold = float(grp["holding_days"].dropna().mean())

        rows.append({
            "遅延日数":        delay,
            "トレード数":      n,
            "総リターン(%)":   round(total_ret, 2),
            "平均リターン(%)": round(avg_ret, 2),
            "勝率(%)":         round(win_rate, 1),
            "最大DD(%)":       round(max_dd, 2),
            "シャープ比":      sharpe,
            "平均保有日数":    round(avg_hold, 1),
        })

    return pd.DataFrame(rows)


# ===========================================================================
# 時間フィルターバックテスト: UI 描画
# ===========================================================================

def render_time_filter_backtest() -> None:
    """時間フィルターバックテストのセクション全体を描画する"""
    st.divider()
    st.markdown("## ⏱️ 時間フィルターバックテスト")
    st.markdown("""
    ブレイク検知後すぐにエントリーせず、**一定期間生き残ったトレンドのみエントリー**する戦略を検証します。
    - 📌 ブレイク後 N 日待機 → その時点で **依然ブレイク水準以上 + EMA 上昇** ならエントリー
    - 🛡️ 待機期間中のドローダウンが許容範囲を超えた場合はスキップ
    - 🔴 決済: EMAクロス（EMA5 < EMA20）または最終日
    """)

    # ── 設定パネル ────────────────────────────────────────────────────────
    st.markdown("### ⚙️ 設定")
    cf1, cf2, cf3, cf4 = st.columns(4)
    with cf1:
        import datetime as _dt
        _tf_today = _dt.date.today()
        _tf_d1, _tf_d2 = st.columns(2)
        with _tf_d1:
            tf_start = st.date_input(
                "📅 開始日",
                value=st.session_state.get("tf_start_date",
                                           _tf_today - _dt.timedelta(days=365*3)),
                min_value=_dt.date(2000, 1, 1),
                max_value=_tf_today,
                key="tf_start_date",
            )
        with _tf_d2:
            tf_end = st.date_input(
                "📅 終了日",
                value=st.session_state.get("tf_end_date", _tf_today),
                min_value=_dt.date(2000, 1, 1),
                max_value=_tf_today,
                key="tf_end_date",
            )
        if tf_start >= tf_end:
            st.error("開始日は終了日より前にしてください。")
            tf_start = tf_end - _dt.timedelta(days=365)
        tf_start_str = str(tf_start)
        tf_end_str   = str(tf_end)
    with cf2:
        tf_donchian = st.number_input(
            "📐 ドンチャン期間",
            min_value=5, max_value=60, value=20, step=1,
            key="tf_donchian",
        )
    with cf3:
        tf_max_dd = st.number_input(
            "🛡️ 最大許容DD(%)",
            min_value=0.0, max_value=30.0, value=5.0, step=0.5, format="%.1f",
            help="ブレイク日〜エントリー日の間の最大下落率の上限。超えたらスキップ。",
            key="tf_max_dd",
        )
    with cf4:
        tf_delays = st.multiselect(
            "⏳ 検証する遅延日数",
            options=DELAY_DAYS_LIST,
            default=DELAY_DAYS_LIST,
            key="tf_delays",
            help="複数選択可。0 = 即時エントリー（ベースライン）",
        )

    # ── 銘柄リスト入力 ────────────────────────────────────────────────────
    st.markdown("### 📋 銘柄リスト")
    tf_input = st.text_area(
        "銘柄コード（カンマ区切り）",
        value="7203, 9984, 6758, 8306, 9433",
        height=60,
        key="tf_ticker_input",
        label_visibility="collapsed",
        help="4桁数字も可（7203 → 7203.T に自動変換）",
    )
    tf_raw   = [t.strip() for t in tf_input.split(",") if t.strip()]
    st.caption(f"銘柄数: **{len(tf_raw)}** 件")

    # ── 実行 / クリア ─────────────────────────────────────────────────────
    rb1, rb2, _ = st.columns([0.25, 0.12, 0.63])
    with rb1:
        tf_run = st.button("🚀 時間フィルター実行", type="primary",
                           use_container_width=True, key="tf_run_btn")
    with rb2:
        if st.button("クリア", key="tf_clear_btn", use_container_width=True):
            st.session_state.tf_results = None
            st.rerun()

    # ── 実行処理 ──────────────────────────────────────────────────────────
    if tf_run:
        if not tf_raw:
            st.warning("銘柄コードを入力してください。")
        elif not tf_delays:
            st.warning("遅延日数を1つ以上選択してください。")
        else:
            tickers    = [normalize_ticker(t) for t in tf_raw]
            all_trades: list[dict] = []
            errors:     list[str]  = []

            prog = st.progress(0, text=f"時間フィルターバックテスト開始... (0 / {len(tickers)})")
            stat = st.empty()

            for idx, ticker in enumerate(tickers):
                prog.progress(
                    idx / len(tickers),
                    text=f"処理中... {idx + 1} / {len(tickers)}  |  累計: {len(all_trades)} 件",
                )
                stat.caption(f"🔍 {ticker} を検証中")

                trades, err = delayed_backtest_ticker(
                    ticker          = ticker,
                    start_date      = tf_start_str,
                    end_date        = tf_end_str,
                    donchian_days   = int(tf_donchian),
                    ema_fast        = 5,
                    ema_slow        = 20,
                    delay_days_list = [int(d) for d in tf_delays],
                    max_dd_pct      = float(tf_max_dd),
                )
                all_trades.extend(trades)
                if err:
                    errors.append(f"{ticker}: {err}")

            prog.progress(1.0, text=f"✅ 完了  {len(all_trades)} トレードを収集")
            stat.empty()

            completed = [t for t in all_trades if t.get("return(%)") is not None]
            st.session_state.tf_results = pd.DataFrame(completed) if completed else pd.DataFrame()

            if errors:
                with st.expander(f"⚠️ スキップされた銘柄 ({len(errors)} 件)"):
                    for e in errors:
                        st.caption(e)

    # ── 結果表示 ──────────────────────────────────────────────────────────
    if "tf_results" not in st.session_state or st.session_state.tf_results is None:
        return

    tf_df = st.session_state.tf_results
    st.divider()

    if tf_df.empty:
        st.info("🔍 有効なトレードが見つかりませんでした。設定を変更して再実行してください。")
        return

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ① 遅延日数別パフォーマンス比較テーブル
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    st.markdown("### 📋 時間フィルター分析 — 遅延日数別比較")
    st.caption(
        "遅延日数 0 = 即時エントリー（ベースライン）。"
        "遅延を増やすとトレード数は減るが、より「生き残ったトレンド」に絞り込まれます。"
    )

    summary = _summarize_by_delay(tf_df)

    def _cc_tf(val):
        try:
            v = float(val)
            return "color: #4caf50; font-weight:bold" if v > 0 else "color: #f44336; font-weight:bold"
        except Exception:
            return ""

    fmt_sum = {k: v for k, v in {
        "総リターン(%)":   "{:+.2f}%",
        "平均リターン(%)": "{:+.2f}%",
        "勝率(%)":         "{:.1f}%",
        "最大DD(%)":       "{:+.2f}%",
        "シャープ比":      "{:.3f}",
        "平均保有日数":    "{:.1f}日",
    }.items() if k in summary.columns}

    pos_c = [c for c in ["総リターン(%)", "平均リターン(%)"] if c in summary.columns]
    styled_sum = summary.style.format(fmt_sum)
    if pos_c:
        styled_sum = styled_sum.map(_cc_tf, subset=pos_c)
    st.dataframe(styled_sum, use_container_width=True, hide_index=True)

    # 遅延日数別 平均リターン バーチャート
    try:
        fig_d, ax_d = plt.subplots(figsize=(8, 3))
        vals_d   = summary["平均リターン(%)"].values
        labels_d = [f"{int(d)}日" for d in summary["遅延日数"].values]
        colors_d = ["#4caf50" if v >= 0 else "#f44336" for v in vals_d]
        ax_d.bar(labels_d, vals_d, color=colors_d)
        ax_d.axhline(0, color="gray", linewidth=0.8, linestyle="--")
        ax_d.set_xlabel("遅延日数", fontsize=10)
        ax_d.set_ylabel("平均リターン (%)", fontsize=10)
        ax_d.set_title("遅延日数別 平均リターン比較", fontsize=12)
        fig_d.tight_layout()
        st.pyplot(fig_d)
        plt.close(fig_d)
    except Exception as e:
        st.warning(f"棒グラフ描画エラー: {e}")

    # 遅延日数別 累積リターン曲線
    st.markdown("#### 📈 遅延日数別 累積リターン曲線")
    try:
        fig_c, ax_c = plt.subplots(figsize=(10, 4))
        for delay, grp in tf_df.groupby("delay_days"):
            grp_s = grp.sort_values("entry_date")
            rets  = grp_s["return(%)"].dropna().astype(float).values
            if len(rets) == 0:
                continue
            cum = np.cumsum(rets)
            ax_c.plot(range(len(cum)), cum, label=f"遅延{int(delay)}日", linewidth=1.8)
        ax_c.axhline(0, color="gray", linewidth=0.8, linestyle="--")
        ax_c.set_xlabel("トレード番号（エントリー日順）", fontsize=10)
        ax_c.set_ylabel("累積リターン (%)", fontsize=10)
        ax_c.set_title("遅延日数別 累積リターン曲線", fontsize=12)
        ax_c.legend(fontsize=9, loc="upper left")
        fig_c.tight_layout()
        st.pyplot(fig_c)
        plt.close(fig_c)
    except Exception as e:
        st.warning(f"累積リターングラフ描画エラー: {e}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ② 保有期間分析
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    st.divider()
    st.markdown("### 📅 保有期間分析")
    st.caption("保有日数とリターンの関係を分析します。長期保有が有利かどうかを検証できます。")

    hold_df = tf_df[["holding_days", "return(%)"]].dropna().copy()
    hold_df["holding_days"] = pd.to_numeric(hold_df["holding_days"], errors="coerce")
    hold_df["return(%)"]    = pd.to_numeric(hold_df["return(%)"],    errors="coerce")
    hold_df = hold_df.dropna()

    if hold_df.empty:
        st.info("保有日数データがありません。")
        return

    # 保有日数ごとの平均リターン集計（10日刻みでビニング）
    try:
        max_hold  = int(hold_df["holding_days"].max())
        bin_edges = list(range(0, max_hold + 11, 10))
        if len(bin_edges) < 2:
            bin_edges = [0, max_hold + 1]

        hold_df["保有日数帯"] = pd.cut(
            hold_df["holding_days"],
            bins=bin_edges,
            right=False,
            labels=[f"{b}〜{b+9}日" for b in bin_edges[:-1]],
        )
        hold_agg = (
            hold_df.groupby("保有日数帯", observed=True)["return(%)"]
            .agg(
                トレード数   = "count",
                平均リターン = "mean",
                勝率         = lambda x: (x > 0).sum() / len(x) * 100,
            )
            .round(2)
            .reset_index()
        )
        hold_agg.columns = ["保有日数帯", "トレード数", "平均リターン(%)", "勝率(%)"]

        def _cc_h(val):
            try:
                v = float(val)
                return "color: #4caf50; font-weight:bold" if v > 0 else "color: #f44336; font-weight:bold"
            except Exception:
                return ""

        styled_hold = (
            hold_agg.style
            .format({"平均リターン(%)": "{:+.2f}%", "勝率(%)": "{:.1f}%"})
            .map(_cc_h, subset=["平均リターン(%)"])
        )
        st.dataframe(styled_hold, use_container_width=True, hide_index=True)

    except Exception as e:
        st.warning(f"保有日数集計エラー: {e}")

    # holding_days vs return 散布図（遅延日数別に色分け）
    st.markdown("#### 🔍 保有日数 vs リターン 散布図")
    try:
        fig_h, ax_h = plt.subplots(figsize=(8, 4))
        for delay, grp in tf_df.groupby("delay_days"):
            hd  = pd.to_numeric(grp["holding_days"], errors="coerce")
            ret = pd.to_numeric(grp["return(%)"],    errors="coerce")
            mask = hd.notna() & ret.notna()
            if mask.sum() == 0:
                continue
            ax_h.scatter(hd[mask], ret[mask],
                         alpha=0.45, s=25, edgecolors="none",
                         label=f"遅延{int(delay)}日")

        ax_h.axhline(0, color="gray", linewidth=0.8, linestyle="--")
        ax_h.set_xlabel("保有日数", fontsize=10)
        ax_h.set_ylabel("リターン (%)", fontsize=10)
        ax_h.set_title("保有日数 vs リターン（遅延日数別）", fontsize=12)
        ax_h.legend(fontsize=8, loc="upper right", ncol=2)
        fig_h.tight_layout()
        st.pyplot(fig_h)
        plt.close(fig_h)
    except Exception as e:
        st.warning(f"散布図描画エラー: {e}")


# ===========================================================================
# パラメータ最適化: グリッドサーチ実行
# ===========================================================================

def _run_optimization(
    tickers:         list[str],
    start_date:      str,
    end_date:        str,
    donchian_days:   int,
    delay_list:      list[int],
    dd_list:         list[float],
    vol_ratio_list:  list[float],
    score_th_list:   list[float],
    min_trades:      int   = 10,
    max_dd_floor:    float = -50.0,   # max_dd (%) がこれを下回ったら除外
) -> pd.DataFrame:
    """
    全パラメータ組み合わせでバックテストを実行し、スコア上位の結果を返す。

    score = (total_return / |max_dd|) * sharpe / sqrt(trades)

    フィルター:
        - trades < min_trades → 除外
        - max_dd < max_dd_floor(%) → 除外

    戻り値: スコア降順の DataFrame（全組み合わせの結果）
    """
    # ---------- Step 1: 各 (delay, dd) でトレードデータを収集 ----------
    # delay×dd の組み合わせ分だけダウンロードが走らないよう、
    # max(delay_list) + max(dd_list) でまとめてシミュレーションし後でフィルタリングする。
    # delayed_backtest_ticker は delay_days_list をまとめて処理するので活用する。

    all_trades_by_delay_dd: dict[tuple, list] = {}

    for dd_pct in dd_list:
        # 全 delay をまとめて 1回のダウンロードで処理
        dd_all_trades: list[dict] = []
        for ticker in tickers:
            trades, _ = delayed_backtest_ticker(
                ticker          = ticker,
                start_date      = start_date,
                end_date        = end_date,
                donchian_days   = donchian_days,
                ema_fast        = 5,
                ema_slow        = 20,
                delay_days_list = delay_list,
                max_dd_pct      = dd_pct,
            )
            dd_all_trades.extend(trades)

        for delay in delay_list:
            key = (delay, dd_pct)
            all_trades_by_delay_dd[key] = [
                t for t in dd_all_trades if t.get("delay_days") == delay
            ]

    # ---------- Step 2: 全パラメータ組み合わせでスコア計算 ----------
    results = []

    for delay in delay_list:
        for dd_pct in dd_list:
            base_trades = all_trades_by_delay_dd.get((delay, dd_pct), [])
            if not base_trades:
                continue

            base_df = pd.DataFrame(base_trades)
            rets_all = pd.to_numeric(base_df["return(%)"], errors="coerce").dropna()

            for vol_ratio in vol_ratio_list:
                for score_th in score_th_list:
                    # volume_ratio / score_threshold フィルタ（スコア列が存在しない場合はスキップ）
                    filtered_df = base_df.copy()

                    # volume_ratio フィルタ（出来高比率列が存在すれば適用）
                    if "volume_ratio" in filtered_df.columns and vol_ratio > 0:
                        filtered_df = filtered_df[
                            pd.to_numeric(filtered_df["volume_ratio"], errors="coerce").fillna(0) >= vol_ratio
                        ]

                    # score_threshold フィルタ（スコア列が存在すれば適用）
                    if "score" in filtered_df.columns and score_th > 0:
                        filtered_df = filtered_df[
                            pd.to_numeric(filtered_df["score"], errors="coerce").fillna(0) >= score_th
                        ]

                    rets = pd.to_numeric(filtered_df["return(%)"], errors="coerce").dropna()
                    n_trades = len(rets)

                    if n_trades < min_trades:
                        continue

                    total_ret = float(rets.sum())
                    avg_ret   = float(rets.mean())
                    std_ret   = float(rets.std())

                    # シャープ比（簡易）
                    sharpe = avg_ret / std_ret if std_ret > 0 else 0.0

                    # 最大ドローダウン（累積リターン曲線ベース）
                    cum      = (1 + rets / 100).cumprod()
                    roll_max = cum.cummax()
                    max_dd   = float(((cum - roll_max) / roll_max * 100).min())

                    if max_dd < max_dd_floor:
                        continue

                    # 最適化スコア
                    abs_dd = abs(max_dd) if max_dd != 0 else 1e-6
                    opt_score = (total_ret / abs_dd) * sharpe / math.sqrt(n_trades)

                    results.append({
                        "delay_days":      delay,
                        "max_dd_guard(%)": dd_pct,
                        "vol_ratio":       vol_ratio,
                        "score_threshold": score_th,
                        "トレード数":      n_trades,
                        "総リターン(%)":   round(total_ret, 2),
                        "平均リターン(%)": round(avg_ret, 2),
                        "勝率(%)":         round(float((rets > 0).sum() / n_trades * 100), 1),
                        "最大DD(%)":       round(max_dd, 2),
                        "シャープ比":      round(sharpe, 3),
                        "最適化スコア":    round(opt_score, 4),
                        # トレード列を後のグラフ用に保持
                        "_trades":         filtered_df.to_dict("records"),
                    })

    if not results:
        return pd.DataFrame()

    result_df = pd.DataFrame(results).sort_values("最適化スコア", ascending=False).reset_index(drop=True)
    return result_df


# ===========================================================================
# パラメータ最適化: UI 描画
# ===========================================================================

def render_param_optimization() -> None:
    """パラメータ最適化モードのセクション全体を描画する"""
    st.divider()
    st.markdown("## 🔬 パラメータ最適化モード")
    st.markdown("""
    指定したパラメータ範囲の**全組み合わせ**でバックテストを実行し、最も優れたパラメータ組み合わせを探索します。

    **最適化スコア** = `(総リターン / |最大DD|) × シャープ比 / √トレード数`

    フィルター: トレード数 < 最小件数 → 除外 ／ 最大DD が下限未満 → 除外
    """)

    # ── 設定パネル ────────────────────────────────────────────────────────
    st.markdown("### ⚙️ 最適化パラメータ設定")

    op1, op2 = st.columns(2)
    with op1:
        import datetime as _dt
        _opt_today = _dt.date.today()
        _op_d1, _op_d2 = st.columns(2)
        with _op_d1:
            opt_start = st.date_input(
                "📅 開始日",
                value=st.session_state.get("opt_start_date",
                                           _opt_today - _dt.timedelta(days=365*3)),
                min_value=_dt.date(2000, 1, 1),
                max_value=_opt_today,
                key="opt_start_date",
            )
        with _op_d2:
            opt_end = st.date_input(
                "📅 終了日",
                value=st.session_state.get("opt_end_date", _opt_today),
                min_value=_dt.date(2000, 1, 1),
                max_value=_opt_today,
                key="opt_end_date",
            )
        if opt_start >= opt_end:
            st.error("開始日は終了日より前にしてください。")
            opt_start = opt_end - _dt.timedelta(days=365)
        opt_start_str = str(opt_start)
        opt_end_str   = str(opt_end)
        opt_donchian = st.number_input(
            "📐 ドンチャン期間（固定）",
            min_value=5, max_value=60, value=20, step=1,
            key="opt_donchian",
        )
        opt_min_trades = st.number_input(
            "📊 最小トレード数（フィルター）",
            min_value=1, max_value=200, value=10, step=1,
            help="この件数を下回るパラメータ組み合わせは除外",
            key="opt_min_trades",
        )
        opt_max_dd_floor = st.number_input(
            "🛡️ 最大DD下限（%）フィルター",
            min_value=-100.0, max_value=0.0, value=-50.0, step=1.0, format="%.1f",
            help="最大DDがこの値を下回る組み合わせは除外（例: -50 → 最大DD<-50% なら除外）",
            key="opt_max_dd_floor",
        )

    with op2:
        opt_delay_raw = st.text_input(
            "⏱️ delay_days 候補（カンマ区切り）",
            value="0, 3, 5, 10, 20",
            key="opt_delay_raw",
            help="例: 0, 3, 5, 10, 20, 30",
        )
        opt_dd_raw = st.text_input(
            "🛡️ waiting_dd 候補（カンマ区切り）",
            value="3.0, 5.0, 10.0",
            key="opt_dd_raw",
            help="例: 3.0, 5.0, 10.0, 15.0  ← 待機期間中の許容DD(%)",
        )
        opt_vol_raw = st.text_input(
            "📦 volume_ratio 候補（カンマ区切り）",
            value="0.0",
            key="opt_vol_raw",
            help="例: 0.0, 1.0, 1.5, 2.0  ← 0.0 = フィルターなし（volume_ratio列が必要）",
        )
        opt_score_raw = st.text_input(
            "🎯 score_threshold 候補（カンマ区切り）",
            value="0.0",
            key="opt_score_raw",
            help="例: 0.0, 30, 50, 70  ← 0.0 = フィルターなし（score列が必要）",
        )

    # ── 銘柄リスト ─────────────────────────────────────────────────────
    st.markdown("### 📋 銘柄リスト")
    opt_ticker_raw = st.text_area(
        "銘柄コード（カンマ区切り）",
        value="7203, 9984, 6758, 8306, 9433",
        height=60,
        key="opt_ticker_area",
        label_visibility="collapsed",
        help="4桁数字も可（7203 → 7203.T に自動変換）",
    )
    opt_raw = [t.strip() for t in opt_ticker_raw.split(",") if t.strip()]
    st.caption(f"銘柄数: **{len(opt_raw)}** 件")

    # ── 推定計算量 ─────────────────────────────────────────────────────
    def _parse_floats(raw: str) -> list[float]:
        vals = []
        for v in raw.split(","):
            v = v.strip()
            try:
                vals.append(float(v))
            except ValueError:
                pass
        return vals or [0.0]

    def _parse_ints(raw: str) -> list[int]:
        vals = []
        for v in raw.split(","):
            v = v.strip()
            try:
                vals.append(int(float(v)))
            except ValueError:
                pass
        return vals or [0]

    d_list  = _parse_ints(opt_delay_raw)
    dd_list = _parse_floats(opt_dd_raw)
    vr_list = _parse_floats(opt_vol_raw)
    st_list = _parse_floats(opt_score_raw)
    n_combo = len(d_list) * len(dd_list) * len(vr_list) * len(st_list)
    n_dl    = len(d_list) * len(dd_list)   # 実際のダウンロード = dd × tickers

    st.caption(
        f"パラメータ組み合わせ数: **{n_combo}** 件 ／ "
        f"バックテスト実行回数: **{len(opt_raw) * len(dd_list)}** 回 "
        f"（遅延日数は 1回のダウンロードで一括処理）"
    )

    # ── 実行 / クリア ─────────────────────────────────────────────────
    ob1, ob2, _ = st.columns([0.28, 0.12, 0.60])
    with ob1:
        opt_run = st.button("🚀 最適化実行", type="primary",
                            use_container_width=True, key="opt_run_btn")
    with ob2:
        if st.button("クリア", key="opt_clear_btn", use_container_width=True):
            st.session_state.opt_results = None
            st.rerun()

    # ── 実行処理 ──────────────────────────────────────────────────────
    if opt_run:
        if not opt_raw:
            st.warning("銘柄コードを入力してください。")
        elif n_combo == 0:
            st.warning("パラメータ候補を1つ以上入力してください。")
        else:
            tickers = [normalize_ticker(t) for t in opt_raw]
            total_steps = len(tickers) * len(dd_list)

            prog = st.progress(0, text=f"最適化開始... (0 / {total_steps} バックテスト)")
            stat = st.empty()
            step = 0

            # dd × ticker ループ（遅延は delay_days_list として一括）
            all_trades_by_key: dict[tuple, list] = {
                (delay, dd): [] for delay in d_list for dd in dd_list
            }
            errors: list[str] = []

            for dd_pct in dd_list:
                for ticker in tickers:
                    prog.progress(
                        step / max(total_steps, 1),
                        text=f"処理中... ({step + 1}/{total_steps})  {ticker}  DD≤{dd_pct}%",
                    )
                    stat.caption(f"🔍 {ticker} | waiting_dd={dd_pct}%")

                    trades, err = delayed_backtest_ticker(
                        ticker          = ticker,
                        start_date      = opt_start_str,
                        end_date        = opt_end_str,
                        donchian_days   = int(opt_donchian),
                        ema_fast        = 5,
                        ema_slow        = 20,
                        delay_days_list = d_list,
                        max_dd_pct      = dd_pct,
                    )
                    if err:
                        errors.append(f"{ticker} (DD={dd_pct}%): {err}")
                    for t in trades:
                        key = (t.get("delay_days"), dd_pct)
                        if key in all_trades_by_key:
                            all_trades_by_key[key].append(t)

                    step += 1

            prog.progress(1.0, text="✅ バックテスト完了。スコア計算中...")
            stat.empty()

            if errors:
                with st.expander(f"⚠️ スキップされた銘柄 ({len(errors)} 件)"):
                    for e in errors:
                        st.caption(e)

            # スコア計算
            results = []
            for delay in d_list:
                for dd_pct in dd_list:
                    base_trades = all_trades_by_key.get((delay, dd_pct), [])
                    if not base_trades:
                        continue
                    base_df = pd.DataFrame(base_trades)

                    for vol_ratio in vr_list:
                        for score_th in st_list:
                            filtered_df = base_df.copy()

                            if "volume_ratio" in filtered_df.columns and vol_ratio > 0:
                                filtered_df = filtered_df[
                                    pd.to_numeric(filtered_df["volume_ratio"], errors="coerce").fillna(0) >= vol_ratio
                                ]
                            if "score" in filtered_df.columns and score_th > 0:
                                filtered_df = filtered_df[
                                    pd.to_numeric(filtered_df["score"], errors="coerce").fillna(0) >= score_th
                                ]

                            rets = pd.to_numeric(filtered_df["return(%)"], errors="coerce").dropna()
                            n_tr = len(rets)

                            if n_tr < int(opt_min_trades):
                                continue

                            total_ret = float(rets.sum())
                            avg_ret   = float(rets.mean())
                            std_ret   = float(rets.std())
                            sharpe    = avg_ret / std_ret if std_ret > 0 else 0.0

                            cum      = (1 + rets / 100).cumprod()
                            roll_max = cum.cummax()
                            max_dd   = float(((cum - roll_max) / roll_max * 100).min())

                            if max_dd < float(opt_max_dd_floor):
                                continue

                            abs_dd    = abs(max_dd) if max_dd != 0 else 1e-6
                            opt_score = (total_ret / abs_dd) * sharpe / math.sqrt(n_tr)

                            results.append({
                                "delay_days":      delay,
                                "max_dd_guard(%)": dd_pct,
                                "vol_ratio":       vol_ratio,
                                "score_threshold": score_th,
                                "トレード数":      n_tr,
                                "総リターン(%)":   round(total_ret, 2),
                                "平均リターン(%)": round(avg_ret, 2),
                                "勝率(%)":         round(float((rets > 0).sum() / n_tr * 100), 1),
                                "最大DD(%)":       round(max_dd, 2),
                                "シャープ比":      round(sharpe, 3),
                                "最適化スコア":    round(opt_score, 4),
                                "_rets":           rets.tolist(),
                            })

            if results:
                res_df = pd.DataFrame(results).sort_values(
                    "最適化スコア", ascending=False
                ).reset_index(drop=True)
                st.session_state.opt_results = res_df
            else:
                st.session_state.opt_results = pd.DataFrame()

            st.rerun()

    # ── 結果表示 ──────────────────────────────────────────────────────
    if st.session_state.get("opt_results") is None:
        return

    opt_df = st.session_state.opt_results
    st.divider()

    if opt_df.empty:
        st.info("🔍 条件を満たすパラメータ組み合わせが見つかりませんでした。フィルター条件を緩めてください。")
        return

    st.markdown(f"### 🏆 最適化結果（全 {len(opt_df)} 組み合わせ）")
    st.caption(
        "最適化スコア = (総リターン / |最大DD|) × シャープ比 / √トレード数。"
        "スコアが高いほど「効率的かつ安定したリターン」を達成したパラメータ組み合わせです。"
    )

    # ── スコア上位10件テーブル ────────────────────────────────────────
    st.markdown("#### 🥇 スコア上位 10 件")
    top10 = opt_df.head(10).drop(columns=["_rets"], errors="ignore")

    def _cc_opt(val):
        try:
            v = float(val)
            return "color: #4caf50; font-weight:bold" if v > 0 else "color: #f44336; font-weight:bold"
        except Exception:
            return ""

    fmt_opt = {
        "総リターン(%)":   "{:+.2f}%",
        "平均リターン(%)": "{:+.2f}%",
        "勝率(%)":         "{:.1f}%",
        "最大DD(%)":       "{:+.2f}%",
        "シャープ比":      "{:.3f}",
        "最適化スコア":    "{:.4f}",
    }
    pos_cols = ["総リターン(%)", "平均リターン(%)"]
    styled_top = (
        top10.style
        .format({k: v for k, v in fmt_opt.items() if k in top10.columns})
        .map(_cc_opt, subset=[c for c in pos_cols if c in top10.columns])
    )
    st.dataframe(styled_top, use_container_width=True, hide_index=True)

    # ── 全件ダウンロード ─────────────────────────────────────────────
    csv_data = opt_df.drop(columns=["_rets"], errors="ignore").to_csv(index=False, encoding="utf-8-sig")
    st.download_button(
        label="📥 全結果 CSV ダウンロード",
        data=csv_data,
        file_name="optimization_results.csv",
        mime="text/csv",
        key="opt_download",
    )

    # ── 各パラメータ別の平均スコア（棒グラフ）────────────────────────
    st.markdown("#### 📊 パラメータ別 平均スコア分析")
    param_cols = ["delay_days", "max_dd_guard(%)", "vol_ratio", "score_threshold"]
    param_labels = {
        "delay_days":      "遅延日数",
        "max_dd_guard(%)": "許容DD(%)",
        "vol_ratio":       "出来高比率",
        "score_threshold": "スコア閾値",
    }

    try:
        n_param_plots = len([c for c in param_cols if c in opt_df.columns])
        if n_param_plots > 0:
            fig_p, axes_p = plt.subplots(1, n_param_plots, figsize=(4 * n_param_plots, 3.5))
            if n_param_plots == 1:
                axes_p = [axes_p]
            plot_idx = 0
            for col in param_cols:
                if col not in opt_df.columns:
                    continue
                ax = axes_p[plot_idx]
                grp = opt_df.groupby(col)["最適化スコア"].mean().reset_index()
                labels = [str(v) for v in grp[col].values]
                scores = grp["最適化スコア"].values
                colors = ["#2196F3" if s >= 0 else "#f44336" for s in scores]
                ax.bar(labels, scores, color=colors)
                ax.axhline(0, color="gray", linewidth=0.8, linestyle="--")
                ax.set_xlabel(param_labels.get(col, col), fontsize=9)
                ax.set_ylabel("平均スコア", fontsize=9)
                ax.set_title(f"{param_labels.get(col, col)}別", fontsize=10)
                ax.tick_params(axis="x", labelsize=8, rotation=15)
                plot_idx += 1
            fig_p.tight_layout()
            st.pyplot(fig_p)
            plt.close(fig_p)
    except Exception as e:
        st.warning(f"パラメータ別グラフ描画エラー: {e}")

    # ── 上位 N 件の累積リターングラフ ────────────────────────────────
    st.markdown("#### 📈 上位パラメータ組み合わせ 累積リターン曲線")
    n_top_chart = st.slider(
        "グラフに表示する上位件数",
        min_value=1, max_value=min(10, len(opt_df)), value=min(5, len(opt_df)),
        key="opt_top_n_slider",
    )

    try:
        fig_c2, ax_c2 = plt.subplots(figsize=(10, 4))
        plotted = 0
        for idx, row in opt_df.head(n_top_chart).iterrows():
            rets_list = row.get("_rets", [])
            if not rets_list:
                continue
            rets_arr = np.array(rets_list, dtype=float)
            cum = np.cumsum(rets_arr)
            label = (
                f"#{idx+1} 遅延{int(row['delay_days'])}日 "
                f"DD{row['max_dd_guard(%)']}% "
                f"Sc={row['最適化スコア']:.3f}"
            )
            ax_c2.plot(range(len(cum)), cum, linewidth=1.8, label=label)
            plotted += 1

        if plotted == 0:
            ax_c2.text(0.5, 0.5, "データなし", ha="center", va="center",
                       transform=ax_c2.transAxes, fontsize=14)
        else:
            ax_c2.axhline(0, color="gray", linewidth=0.8, linestyle="--")
            ax_c2.set_xlabel("トレード番号（エントリー日順）", fontsize=10)
            ax_c2.set_ylabel("累積リターン (%)", fontsize=10)
            ax_c2.set_title("上位パラメータ組み合わせ 累積リターン比較", fontsize=12)
            ax_c2.legend(fontsize=7, loc="upper left", ncol=1)

        fig_c2.tight_layout()
        st.pyplot(fig_c2)
        plt.close(fig_c2)
    except Exception as e:
        st.warning(f"累積リターングラフ描画エラー: {e}")

    # ── スコア分布ヒストグラム ────────────────────────────────────────
    st.markdown("#### 📉 最適化スコア分布")
    try:
        fig_hist, ax_hist = plt.subplots(figsize=(8, 3))
        scores_all = opt_df["最適化スコア"].dropna().astype(float)
        ax_hist.hist(scores_all, bins=30, color="#2196F3", edgecolor="white", alpha=0.85)
        ax_hist.axvline(float(scores_all.median()), color="orange",
                        linewidth=1.5, linestyle="--", label=f"中央値 {float(scores_all.median()):.3f}")
        ax_hist.set_xlabel("最適化スコア", fontsize=10)
        ax_hist.set_ylabel("頻度", fontsize=10)
        ax_hist.set_title("全パラメータ組み合わせのスコア分布", fontsize=12)
        ax_hist.legend(fontsize=9)
        fig_hist.tight_layout()
        st.pyplot(fig_hist)
        plt.close(fig_hist)
    except Exception as e:
        st.warning(f"ヒストグラム描画エラー: {e}")


# ===========================================================================
# メイン
# ===========================================================================

def main() -> None:
    init_state()

    st.markdown("""
    <style>
    div[data-testid="stHorizontalBlock"] > div:nth-last-child(-n+4) .stButton > button {
        height: 35px !important;
        min-height: 35px !important;
        padding: 0 2px !important;
        font-size: 12px !important;
        line-height: 1 !important;
        border-radius: 4px !important;
    }
    div[data-testid="stHorizontalBlock"] > div:nth-last-child(-n+4) [data-testid="stVerticalBlock"] {
        gap: 1px !important;
    }
    /* タブバーを固定 */
    .stTabs [data-baseweb="tab-list"] {
        position: -webkit-sticky !important;
        position: sticky !important;
        top: 0 !important;
        z-index: 9999 !important;
        background-color: white !important;
        padding-bottom: 4px !important;
    }
    [data-theme="dark"] .stTabs [data-baseweb="tab-list"] {
        background-color: #0e1117 !important;
    }
    </style>
    """, unsafe_allow_html=True)

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🔍 スクリーニング",
        "👀 監視銘柄",
        "💼 ポジション管理",
        "📈 バックテスト",
        "📰 決算レポート",
    ])

    with tab1:
        render_screener_tab()

    with tab2:
        render_funda_tab()

    with tab3:
        render_position_tab()

    with tab4:
        render_backtest_tab()

    with tab5:
        render_earnings_report_tab()


if __name__ == "__main__":
    main()
