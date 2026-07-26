"""上場来高値銘柄ページ（表示処理のみ）。

株探の「今週の【上場来高値銘柄】」記事を蓄積し、検索・期間指定・出現回数ランキング・
CSV 出力を提供する。

このファイルは UI だけを担当し、
  - 取得処理 -> modules/kabutan_high.py
  - DB 処理  -> modules/db.py
に完全に委譲している。将来 J-Quants / Yahoo Finance / TradingView を足す場合も、
同じ形の collect() を持つモジュールを modules/ に追加して
下の DATA_SOURCES に登録すれば、この画面はほぼ変更せずに使える。
"""

from __future__ import annotations

import os
import sys

import pandas as pd
import streamlit as st

# pages/ から実行されても modules/ を解決できるようにリポジトリルートを通す
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from modules import db  # noqa: E402
from modules import kabutan_high  # noqa: E402

# 将来のデータソース追加はここに足すだけでよい
# （collect(known_ids, max_articles, progress_cb) -> (articles, skipped) を満たすこと）
DATA_SOURCES = {
    "株探 今週の【上場来高値銘柄】": kabutan_high,
}

st.set_page_config(page_title="上場来高値銘柄", page_icon="📈", layout="wide")


# ---------------------------------------------------------------------------
# 初期化
# ---------------------------------------------------------------------------

db.init_db()

# Cloud の再起動で SQLite が消えた場合、GitHub スナップショットから自動復元する
if "high_restore_done" not in st.session_state:
    restored = db.restore_snapshot_if_empty()
    st.session_state["high_restore_done"] = True
    if restored:
        st.toast(f"スナップショットから {restored} 件復元しました")


st.title("📈 上場来高値銘柄")
st.caption(
    "株探「今週の【上場来高値銘柄】」を蓄積して、出現回数・初登場日・直近登場日を集計します。"
)


# ---------------------------------------------------------------------------
# 自動取得
# ---------------------------------------------------------------------------

with st.container(border=True):
    st.subheader("自動取得")

    col_a, col_b = st.columns([1, 2])
    with col_a:
        run = st.button("🔄 自動取得", type="primary", use_container_width=True)
        deep_history = st.checkbox(
            "過去記事も探索する",
            value=False,
            help=(
                "直近 1 か月より前まで遡ります。過去サイトマップを走査するため"
                "数分〜十数分かかります。直近数週ぶんだけで良い場合はオフのままで結構です。"
            ),
        )
    with col_b:
        max_articles = st.slider(
            "1回の実行で取得する記事数の上限",
            min_value=1,
            max_value=120,
            value=8,
            step=1,
            help=(
                "株探の robots.txt が Crawl-delay: 3 を指定しているため、"
                "1記事あたり約3秒かかります。週次記事なので 8 ＝ 約8週ぶんです。"
            ),
        )

    if run:
        progress = st.progress(0.0, text="準備中…")
        status = st.empty()

        def _on_progress(current: int, total: int, message: str) -> None:
            """kabutan_high から呼ばれる進捗コールバック（UI 側の関心事）。"""
            ratio = 0.0 if total <= 0 else min(current / total, 1.0)
            progress.progress(ratio, text=message)

        source = kabutan_high
        known = db.known_article_ids()

        # 取得処理本体。通信エラーはモジュール内でリトライ／スキップされ、
        # ここまで例外は上がってこない（＝途中で止まらない）。
        articles, skipped = source.collect(
            known_ids=known,
            max_articles=max_articles,
            deep_history=deep_history,
            progress_cb=_on_progress,
        )

        # --- DB へ反映 ---
        added_rows = 0
        for art in articles:
            added_rows += db.insert_rows(art.to_rows())
            db.record_article(
                art.article_id,
                art.url,
                art.article_date,
                art.title,
                len(art.stocks),
                status="ok",
            )
        # 対象外・取得失敗だった候補も記録して、次回の再取得を防ぐ
        for sk in skipped:
            db.record_article(
                sk["article_id"], sk["url"], "", "", 0, status=sk["reason"][:40]
            )

        updated_at = db.touch_updated_at()
        pushed = db.push_snapshot()  # Cloud 用スナップショット（ローカルでは何もしない）

        progress.progress(1.0, text="完了")
        st.session_state["high_last_result"] = {
            "articles": len(articles),
            "rows": added_rows,
            "skipped": len(skipped),
            "updated_at": updated_at,
            "pushed": pushed,
        }
        status.success(
            f"取得完了： 新規記事 {len(articles)} 件 / 追加銘柄 {added_rows} 件"
            + (f" / スキップ {len(skipped)} 件" if skipped else "")
        )

# --- 直近の取得結果（st.metric） ---
res = st.session_state.get("high_last_result")
if res:
    m1, m2, m3 = st.columns(3)
    m1.metric("取得記事数", f"{res['articles']} 件")
    m2.metric("追加銘柄数", f"{res['rows']} 件")
    m3.metric("更新日時", res["updated_at"])
    if res["pushed"]:
        st.caption("✅ GitHub スナップショットへ保存しました（再起動後も保持されます）")


# ---------------------------------------------------------------------------
# 蓄積状況サマリ
# ---------------------------------------------------------------------------

s = db.stats()
c1, c2, c3, c4 = st.columns(4)
c1.metric("蓄積記事数", f"{s['articles']} 件")
c2.metric("延べ銘柄数", f"{s['rows']} 件")
c3.metric("ユニーク銘柄", f"{s['codes']} 銘柄")
c4.metric("最終更新", s["updated_at"] or "-")
if s["date_from"]:
    st.caption(f"収録期間： {s['date_from']} 〜 {s['date_to']}")

if s["rows"] == 0:
    st.info("まだデータがありません。上の「🔄 自動取得」を押して取得を開始してください。")
    st.stop()


def _csv(df: pd.DataFrame) -> bytes:
    """Excel で開いても文字化けしないよう BOM 付き UTF-8 で出力する。"""
    return df.to_csv(index=False).encode("utf-8-sig")


# ---------------------------------------------------------------------------
# 最新週
# ---------------------------------------------------------------------------

st.divider()
st.subheader("最新週")

latest = db.load_latest_week()
# 連続週数は全期間で計算する（期間フィルタで切ると連続が途切れて見えてしまうため）
streaks = db.streak_summary()

if latest.empty:
    st.info("最新週のデータがありません。")
else:
    st.caption(f"記事日付： {latest['article_date'].iloc[0]}　（{len(latest)} 銘柄）")

    view = (
        latest.merge(
            streaks[["code", "current_streak", "count"]], on="code", how="left"
        )
        .rename(
            columns={
                "code": "コード",
                "name": "銘柄",
                "article_date": "取得日",
                "sector": "業種",
                "market": "市場",
                "current_streak": "連続",
                "count": "通算",
            }
        )
        # 連続週数の多い順 = 勢いが続いている銘柄が上に来る
        .sort_values(["連続", "通算"], ascending=False, ignore_index=True)
    )[["コード", "銘柄", "業種", "市場", "連続", "通算", "取得日"]]

    # 何週連続で載っているかがひと目で分かるよう、最長記録をヘッドラインに出す
    top = view.iloc[0]
    n_multi = int((view["連続"] >= 2).sum())
    k1, k2 = st.columns(2)
    k1.metric("最長連続", f"{int(top['連続'])} 週連続", delta=f"{top['銘柄']}（{top['コード']}）")
    k2.metric("2週以上連続", f"{n_multi} 銘柄", delta=f"最新週 {len(view)} 銘柄中")

    st.dataframe(
        view,
        use_container_width=True,
        hide_index=True,
        column_config={
            "連続": st.column_config.NumberColumn(
                "連続", format="%d 週", help="最新週から遡って何週連続で掲載されているか"
            ),
            "通算": st.column_config.NumberColumn(
                "通算", format="%d 回", help="収録範囲での掲載回数の合計"
            ),
        },
    )
    st.download_button(
        "⬇ 最新週を CSV 保存",
        _csv(view),
        file_name=f"上場来高値_最新週_{latest['article_date'].iloc[0]}.csv",
        mime="text/csv",
    )


# ---------------------------------------------------------------------------
# 検索 / 期間指定
# ---------------------------------------------------------------------------

st.divider()
st.subheader("検索")

f1, f2, f3 = st.columns([1, 1, 1])
with f1:
    period = st.selectbox("期間指定", list(db.PERIOD_DAYS.keys()), index=4)
with f2:
    code_q = st.text_input("コード検索", placeholder="例: 8035")
with f3:
    name_q = st.text_input("銘柄検索", placeholder="例: 東京エレク")

hist = db.load_history(period=period, code_query=code_q, name_query=name_q)
hist_view = hist.rename(
    columns={
        "article_date": "取得日",
        "code": "コード",
        "name": "銘柄",
        "sector": "業種",
        "market": "市場",
        "title": "記事タイトル",
    }
)[["取得日", "コード", "銘柄", "業種", "市場", "記事タイトル"]]

st.caption(f"該当 {len(hist_view)} 件")
st.dataframe(hist_view, use_container_width=True, hide_index=True, height=380)
st.download_button(
    "⬇ 表示中のデータを CSV 保存",
    _csv(hist_view),
    file_name=f"上場来高値_履歴_{period}.csv",
    mime="text/csv",
    key="dl_hist",
)


# ---------------------------------------------------------------------------
# 集計（出現回数ランキング / 銘柄サマリ）
# ---------------------------------------------------------------------------

st.divider()
st.subheader("集計")

tab_streak, tab_rank, tab_summary = st.tabs(
    ["連続出現ランキング", "出現回数ランキング", "初登場日・直近登場日・登場回数"]
)

with tab_streak:
    st.caption(
        "「連続」は最新週から遡って何週続けて掲載されたか。最新週に載っていない銘柄は 0 です。"
        "　※ 未収録の週を跨ぐ場合は連続とみなしません。"
    )
    streak_view = streaks.rename(
        columns={
            "code": "コード",
            "name": "銘柄",
            "current_streak": "連続",
            "max_streak": "最長連続",
            "count": "通算",
            "first_seen": "初登場日",
            "last_seen": "直近登場日",
        }
    )[["コード", "銘柄", "連続", "最長連続", "通算", "初登場日", "直近登場日"]]

    only_active = st.checkbox("連続中の銘柄だけ表示（連続 ≥ 2）", value=True)
    shown = streak_view[streak_view["連続"] >= 2] if only_active else streak_view

    st.dataframe(
        shown,
        use_container_width=True,
        hide_index=True,
        height=420,
        column_config={
            "連続": st.column_config.ProgressColumn(
                "連続",
                format="%d 週",
                min_value=0,
                max_value=int(streak_view["最長連続"].max()) if not streak_view.empty else 1,
            ),
            "最長連続": st.column_config.NumberColumn("最長連続", format="%d 週"),
            "通算": st.column_config.NumberColumn("通算", format="%d 回"),
        },
    )
    st.download_button(
        "⬇ 連続出現を CSV 保存",
        _csv(shown),
        file_name="上場来高値_連続出現.csv",
        mime="text/csv",
        key="dl_streak",
    )

with tab_rank:
    rank = db.ranking(period=period)
    rank_view = rank.rename(columns={"code": "コード", "name": "銘柄", "count": "出現回数"})
    st.caption(f"期間： {period}")
    st.dataframe(
        rank_view,
        use_container_width=True,
        hide_index=True,
        height=420,
        column_config={
            # 出現回数は棒グラフ付きにして、常連銘柄がひと目で分かるようにする
            "出現回数": st.column_config.ProgressColumn(
                "出現回数",
                format="%d 回",
                min_value=0,
                max_value=int(rank_view["出現回数"].max()) if not rank_view.empty else 1,
            )
        },
    )
    st.download_button(
        "⬇ ランキングを CSV 保存",
        _csv(rank_view),
        file_name=f"上場来高値_ランキング_{period}.csv",
        mime="text/csv",
        key="dl_rank",
    )

with tab_summary:
    summary = db.stock_summary(period=period)
    summary_view = summary.rename(
        columns={
            "code": "コード",
            "name": "銘柄",
            "first_seen": "初登場日",
            "last_seen": "直近登場日",
            "count": "登場回数",
        }
    )
    st.caption(f"期間： {period}　（{len(summary_view)} 銘柄）")
    st.dataframe(summary_view, use_container_width=True, hide_index=True, height=420)
    st.download_button(
        "⬇ 銘柄サマリを CSV 保存",
        _csv(summary_view),
        file_name=f"上場来高値_銘柄サマリ_{period}.csv",
        mime="text/csv",
        key="dl_summary",
    )
