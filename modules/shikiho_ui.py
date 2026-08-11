"""四季報銘柄分析タブの描画（表示処理のみ）。

app.py のタブから `shikiho_ui.render()` として呼ばれる。
データ処理は modules/shikiho.py に委譲する。

タブとして描画するため st.set_page_config は呼ばず、st.stop() も使わない
（タブ内で止めるとアプリ全体が停止するため早期 return する）。
ウィジェットの key はすべて "shk_" 始まりにして app.py 側との衝突を避ける。
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from modules import shikiho

PERIODS = {
    "全期間": None,
    "直近1か月": 30,
    "3か月": 91,
    "6か月": 183,
    "1年": 365,
}


def _csv(df: pd.DataFrame) -> bytes:
    """Excel で開いても文字化けしないよう BOM 付き UTF-8 で出力する。"""
    return df.to_csv(index=False).encode("utf-8-sig")


@st.cache_data(ttl=3600, show_spinner=False)
def _analyze_cached(df: pd.DataFrame):
    """株価取得は 229 銘柄で1〜2分かかるため1時間キャッシュする。

    引数の DataFrame が変わらない限り再取得しない。
    """
    return shikiho.analyze(df)


def _fmt_pct(v) -> str:
    return "-" if pd.isna(v) else f"{v:+.1f}%"


def render() -> None:
    """四季報銘柄分析タブ全体を描画する。"""
    st.subheader("📗 四季報銘柄分析")
    st.caption(
        "四季報オンライン「厳選注目株」で取り上げられた銘柄が、"
        "配信日から現在までにどれだけ動いたかを検証します。"
    )

    # --- データの読み込み: アップロード優先、なければローカルCSV ---
    up = st.file_uploader(
        "CSV を読み込む",
        type=["csv"],
        key="shk_upload",
        help=(
            "列: code, name, published（, title）。"
            "騰落率などを計算済みの CSV（ret 列を含む）を入れると、株価を取得せずそのまま表示します。"
        ),
    )

    analyzed = False
    if up is not None:
        try:
            df, analyzed = shikiho.read_csv(up)
            st.caption(f"読み込み: {up.name}（{len(df)} 件{' / 計算済み' if analyzed else ''}）")
        except Exception as e:
            st.error(f"CSV を読み込めませんでした: {e}")
            return
    else:
        df = shikiho.load_selection()
        if df.empty:
            st.info(
                "銘柄リストがありません。上の「CSV を読み込む」からファイルを指定してください。\n\n"
                "「厳選注目株一覧」は四季報オンラインの有料プラン限定のため自動取得できません。"
                "購読者ご自身が書き出した CSV（列: code, name, published, title）をお使いください。"
                "リポジトリ直下に `shikiho_selection.csv` を置いておけば、次回から自動で読み込まれます。"
            )
            return

    if analyzed:
        # 計算済みCSVはそのまま使う（株価取得を省けるので即座に表示できる）
        res, summary = df, shikiho.summarize(df)
    else:
        with st.spinner("株価を取得して騰落率を計算しています…（初回は1〜2分かかります）"):
            res, summary = _analyze_cached(df)

    if not summary:
        st.error("株価を取得できませんでした。時間をおいて再度お試しください。")
        return

    # --- サマリ ---
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("対象銘柄", f"{summary['n']} 銘柄", delta=f"全 {summary['n_total']} 件中")
    c2.metric("平均騰落率", _fmt_pct(summary["mean"]), delta=f"中央値 {_fmt_pct(summary['median'])}")
    c3.metric("勝率", f"{summary['win_rate']:.1f}%", delta="上昇した銘柄の割合")
    if summary.get("excess_mean") is not None:
        c4.metric(
            f"対{shikiho.BENCHMARK_NAME}",
            _fmt_pct(summary["excess_mean"]),
            delta=f"{shikiho.BENCHMARK_NAME} {_fmt_pct(summary['bench_mean'])}",
            delta_color="off",
        )

    # 配信後にどこまで伸び、どこまで沈んだか。現在値だけでは振れ幅が見えないため併記する。
    if summary.get("high_mean") is not None:
        h1, h2 = st.columns(2)
        h1.metric(
            "配信後の最高値（平均）", _fmt_pct(summary["high_mean"]),
            delta="配信日終値からの最大上昇", delta_color="off",
        )
        h2.metric(
            "配信後の最安値（平均）", _fmt_pct(summary["low_mean"]),
            delta="配信日終値からの最大下落", delta_color="off",
        )

    st.caption(
        f"収録期間： {summary['date_from']:%Y-%m-%d} 〜 {summary['date_to']:%Y-%m-%d}　"
        f"／ 配信日の終値を取得価格とし、最新終値と比較（配信は寄り付き前の 06:00）。"
        f"最高値・最安値は配信日以降の日中値。"
    )

    # ベンチマークに対する評価をはっきり出す。
    # 「上がった」だけでは、相場全体の上昇と区別がつかないため。
    if summary.get("excess_mean") is not None:
        if summary["excess_mean"] < 0:
            st.warning(
                f"平均では上昇していますが、同期間の{shikiho.BENCHMARK_NAME}"
                f"（{_fmt_pct(summary['bench_mean'])}）を **{abs(summary['excess_mean']):.1f} ポイント下回っています**。"
                f"{shikiho.BENCHMARK_NAME}に勝った銘柄は {summary['excess_win']:.1f}% でした。"
            )
        else:
            st.success(
                f"同期間の{shikiho.BENCHMARK_NAME}（{_fmt_pct(summary['bench_mean'])}）を "
                f"{summary['excess_mean']:.1f} ポイント上回っています。"
                f"{shikiho.BENCHMARK_NAME}に勝った銘柄は {summary['excess_win']:.1f}% でした。"
            )

    # --- 期間フィルタ ---
    st.divider()
    f1, f2 = st.columns([1, 2])
    with f1:
        period = st.selectbox("配信日の期間", list(PERIODS.keys()), key="shk_period")
    with f2:
        q = st.text_input("コード・銘柄名で絞り込み", placeholder="例: 6920 / レーザー", key="shk_q")

    view = res.copy()
    days = PERIODS[period]
    if days:
        cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=days)
        view = view[view["published"] >= cutoff]
    if q.strip():
        s = q.strip()
        view = view[
            view["code"].str.contains(s, case=False, na=False)
            | view["name"].str.contains(s, case=False, na=False)
        ]

    rename = {
        "published": "配信日時",
        "code": "コード",
        "name": "銘柄",
        "entry": "配信時株価",
        "now": "現在値",
        "ret": "騰落率",
        "high": "最高値",
        "high_date": "最高値日",
        "high_pct": "最大上昇",
        "low": "最安値",
        "low_date": "最安値日",
        "low_pct": "最大下落",
        "bench_ret": shikiho.BENCHMARK_NAME,
        "excess": "超過",
        "title": "記事タイトル",
    }
    cols = ["配信日時", "コード", "銘柄", "配信時株価", "現在値", "騰落率",
            "最高値", "最大上昇", "最高値日", "最安値", "最大下落", "最安値日",
            shikiho.BENCHMARK_NAME, "超過", "記事タイトル"]
    table = view.rename(columns=rename)
    table = table[[c for c in cols if c in table.columns]].sort_values(
        "騰落率", ascending=False, ignore_index=True
    )

    st.caption(f"該当 {len(table)} 銘柄（騰落率の高い順）")
    st.dataframe(
        table,
        use_container_width=True,
        hide_index=True,
        height=430,
        column_config={
            "配信日時": st.column_config.DatetimeColumn("配信日時", format="YYYY-MM-DD"),
            "配信時株価": st.column_config.NumberColumn("配信時株価", format="%.1f"),
            "現在値": st.column_config.NumberColumn("現在値", format="%.1f"),
            "騰落率": st.column_config.NumberColumn("騰落率", format="%+.1f%%"),
            "最高値": st.column_config.NumberColumn("最高値", format="%.1f"),
            "最大上昇": st.column_config.NumberColumn(
                "最大上昇", format="%+.1f%%", help="配信日終値から最高値までの上昇率"
            ),
            "最安値": st.column_config.NumberColumn("最安値", format="%.1f"),
            "最大下落": st.column_config.NumberColumn(
                "最大下落", format="%+.1f%%", help="配信日終値から最安値までの下落率"
            ),
            shikiho.BENCHMARK_NAME: st.column_config.NumberColumn(
                shikiho.BENCHMARK_NAME, format="%+.1f%%", help="同期間の指数騰落率"
            ),
            "超過": st.column_config.NumberColumn(
                "超過", format="%+.1f%%", help="騰落率 − 指数騰落率"
            ),
        },
    )
    st.download_button(
        "⬇ 表示中のデータを CSV 保存",
        _csv(table),
        file_name=f"四季報銘柄分析_{period}.csv",
        mime="text/csv",
        key="shk_dl",
    )

    # --- ランキングと月次 ---
    st.divider()
    tab_top, tab_worst, tab_high, tab_month = st.tabs(
        ["上昇率 TOP20", "下落率 WORST20", "最大上昇 TOP20", "配信月別"]
    )

    ok = view.dropna(subset=["ret"])

    def _rank_table(d: pd.DataFrame) -> pd.DataFrame:
        out = d.rename(
            columns={
                "published": "配信日",
                "code": "コード",
                "name": "銘柄",
                "ret": "騰落率",
                "high_pct": "最大上昇",
                "low_pct": "最大下落",
                "excess": "超過",
            }
        )
        want = ["配信日", "コード", "銘柄", "騰落率", "最大上昇", "最大下落", "超過"]
        return out[[c for c in want if c in out.columns]]

    RANK_CFG = {
        "配信日": st.column_config.DatetimeColumn("配信日", format="YYYY-MM-DD"),
        "騰落率": st.column_config.NumberColumn("騰落率", format="%+.1f%%"),
        "最大上昇": st.column_config.NumberColumn("最大上昇", format="%+.1f%%"),
        "最大下落": st.column_config.NumberColumn("最大下落", format="%+.1f%%"),
        "超過": st.column_config.NumberColumn("超過", format="%+.1f%%"),
    }

    with tab_top:
        st.dataframe(
            _rank_table(ok.nlargest(20, "ret")),
            use_container_width=True, hide_index=True, column_config=RANK_CFG,
        )

    with tab_worst:
        st.dataframe(
            _rank_table(ok.nsmallest(20, "ret")),
            use_container_width=True, hide_index=True, column_config=RANK_CFG,
        )

    with tab_high:
        st.caption(
            "配信後に一時どこまで上昇したか。現在値では見えない「利益確定の機会」を確認できます。"
        )
        if "high_pct" in ok.columns and ok["high_pct"].notna().any():
            st.dataframe(
                _rank_table(ok.dropna(subset=["high_pct"]).nlargest(20, "high_pct")),
                use_container_width=True, hide_index=True, column_config=RANK_CFG,
            )
        else:
            st.info("最高値データがありません。CSV を再計算してください。")

    with tab_month:
        st.caption("配信月ごとの成績。相場環境の影響を切り分けるために使います。")
        m = shikiho.by_month(view)
        st.dataframe(
            m,
            use_container_width=True,
            hide_index=True,
            column_config={
                "平均騰落率": st.column_config.NumberColumn("平均騰落率", format="%+.1f%%"),
                "勝率": st.column_config.NumberColumn("勝率", format="%.0f%%"),
            },
        )
