"""レーティングタブの描画（表示処理のみ）。

app.py のタブから `rating_ui.render()` として呼ばれる。
データ処理は modules/rating.py に委譲する。

タブとして描画するため st.set_page_config は呼ばず、st.stop() も使わない
（タブ内で止めるとアプリ全体が停止するので早期 return する）。
ウィジェットの key はすべて "rt_" 始まりにして app.py 側との衝突を避ける。

取得元がアクセスを拒否する環境では、リポジトリに置いた
rating_snapshot.json を読む運用に切り替えられるようにしてある。
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from modules import rating

_JUDGE_ORDER = ["引き上げ", "新規", "据え置き", "引き下げ", "不明"]


def _csv(df: pd.DataFrame) -> bytes:
    """Excel で開いても文字化けしないよう BOM 付き UTF-8 で出力する。"""
    return df.to_csv(index=False).encode("utf-8-sig")


def _load(source: str):
    """(DataFrame, 取得元の説明, エラー) を返す。

    source: "archive"（保存済みアーカイブ）/ "live"（今の掲載ぶんだけ）
            / "both"（アーカイブ＋今の掲載ぶんを統合。保存はしない）
    """
    arch, updated = rating.load_snapshot()

    if source == "archive":
        if arch.empty:
            return arch, None, "アーカイブ（rating_snapshot.json）がありません。"
        return arch, f"保存済みアーカイブ（最終更新 {updated}）", None

    try:
        live = rating.fetch_ratings()
    except Exception as e:
        if source == "both" and not arch.empty:
            # 取得できなくてもアーカイブがあれば表示を続ける
            return arch, f"取得失敗のためアーカイブのみ（最終更新 {updated}）", None
        return pd.DataFrame(), None, str(e)

    if source == "live":
        return live, "トレーダーズ・ウェブの現在の掲載ぶん", None

    merged, added = rating.merge_rows(arch, live)
    if arch.empty:
        return merged, "トレーダーズ・ウェブから取得（アーカイブなし）", None
    return merged, f"アーカイブ＋今回取得（未保存の新着 {added:,} 件を含む）", None


def render() -> None:
    st.subheader("⭐ レーティング")
    st.caption(
        "トレーダーズ・ウェブの[注目レーティング]"
        f"({rating.SOURCE_URL}) から証券会社・調査機関の投資判断と目標株価を取得し、"
        "現在株価からの乖離率が高い順に並べます。"
    )
    st.caption(
        "⚠️ 取得元は**当月ぶん（直近3週間程度）しか掲載していません**。"
        "月が替わると今のデータはページから消えるため、"
        "`update_rating.bat` をローカルで定期実行して "
        "`rating_snapshot.json` に蓄積してください（追記式なので過去分は消えません）。"
    )

    # ── アーカイブの収録状況 ──────────────────────────────────────────────
    _arch, _arch_updated = rating.load_snapshot()
    _cov = rating.archive_coverage(_arch)
    if _cov:
        a1, a2, a3, a4 = st.columns(4)
        a1.metric("📦 アーカイブ件数", f"{_cov['件数']:,} 件")
        a2.metric("🏢 銘柄数",         f"{_cov['銘柄数']:,}")
        a3.metric("🗓️ 収録月数",       f"{_cov['月数']} ヶ月")
        a4.metric("📅 収録期間",       f"{_cov['最新']}", delta=f"最古 {_cov['最古']}",
                  delta_color="off")
        st.caption(
            f"収録月: {', '.join(_cov['収録月'])}　／　最終更新: {_arch_updated}"
        )
    else:
        st.warning(
            "📦 アーカイブがまだありません。ローカルで `update_rating.bat` を"
            "実行すると保存が始まります。"
        )

    # ── 取得 ──────────────────────────────────────────────────────────────
    c1, c2 = st.columns([0.42, 0.58])
    with c1:
        source = st.radio(
            "データ",
            ["アーカイブ＋今の掲載ぶん", "保存済みアーカイブのみ", "今の掲載ぶんのみ"],
            key="rt_source",
            help="「アーカイブ＋今の掲載ぶん」は取得したデータをアーカイブと"
                 "統合して表示します（ファイルへの保存は update_rating.bat 側で行います）。",
        )
    with c2:
        do_fetch = st.button("🔄 読み込み", type="primary",
                             use_container_width=True, key="rt_fetch")
        if st.session_state.get("rt_src"):
            st.info(f"表示中: {st.session_state['rt_src']}")

    _SRC_MAP = {
        "アーカイブ＋今の掲載ぶん": "both",
        "保存済みアーカイブのみ":   "archive",
        "今の掲載ぶんのみ":         "live",
    }

    if do_fetch:
        with st.spinner("レーティングを読み込み中..."):
            df, src, err = _load(_SRC_MAP[source])
        if err:
            st.error(f"❌ 読み込みに失敗しました: {err}")
            st.caption(
                "取得元がこの環境からのアクセスを拒否している場合は、"
                "ローカルで `update_rating.bat` を実行して "
                "`rating_snapshot.json` を更新・push し、"
                "「保存済みアーカイブのみ」を選んでください。"
            )
            return
        with st.spinner(f"現在株価を取得中...（{df['コード'].nunique()} 銘柄）"):
            prices = rating.fetch_prices(df["コード"])
        # アーカイブの古い行も含めて、乖離率は常に現在株価で引き直す
        st.session_state["rt_data"] = rating.refresh_upside(df, prices)
        st.session_state["rt_src"]  = src
        st.success(
            f"✅ {len(df):,} 件（{df['コード'].nunique():,} 銘柄）／"
            f"株価取得 {len(prices):,} 銘柄"
        )

    data: pd.DataFrame | None = st.session_state.get("rt_data")
    if data is None or data.empty:
        st.info("「読み込み」を押すと一覧を作成します。")
        return

    st.divider()

    # ── 表示設定 ──────────────────────────────────────────────────────────
    view = st.radio(
        "表示単位",
        ["銘柄ごとに集計", "レーティング発表ごと"],
        horizontal=True, key="rt_view",
        help="同じ銘柄に複数のシンクタンクが目標株価を出しているため、"
             "集計するか1件ずつ並べるかを選べます。",
    )

    f1, f2, f3, f4 = st.columns(4)
    with f1:
        days = st.selectbox(
            "📅 対象期間", options=[3, 5, 7, 14, 30, 60, 90, 180, 9999], index=4,
            format_func=lambda x: "収録全期間" if x > 365 else f"直近{x}日",
            key="rt_days",
            help="アーカイブが伸びると古い目標株価も混ざります。"
                 "現在の株価水準と噛み合った目標だけを見るなら短めにしてください。",
        )
    with f2:
        markets = st.multiselect(
            "🏛️ 市場", options=sorted(data["市場"].dropna().unique()),
            default=["プライム"] if "プライム" in set(data["市場"]) else [],
            key="rt_markets",
        )
    with f3:
        judges = st.multiselect(
            "🔀 判定", options=[j for j in _JUDGE_ORDER if j in set(data["判定"])],
            default=[], key="rt_judges",
            help="未選択なら全件。「引き上げ」だけに絞ると格上げ銘柄が見られます。",
        )
    with f4:
        brokers = st.multiselect(
            "🏦 シンクタンク", options=sorted(data["シンクタンク"].dropna().unique()),
            default=[], key="rt_brokers",
        )

    g1, g2 = st.columns([0.35, 0.65])
    with g1:
        how = st.selectbox(
            "📊 銘柄集計の方法", options=["max", "median", "mean"], index=0,
            format_func=lambda x: {"max": "最も強気な目標", "median": "目標の中央値",
                                   "mean": "目標の平均"}[x],
            key="rt_how",
            disabled=(view != "銘柄ごとに集計"),
        )
    with g2:
        min_up = st.slider(
            "📈 乖離率の下限（%）", min_value=-50, max_value=100, value=0, step=5,
            key="rt_min_up",
        )

    # ── フィルター適用 ────────────────────────────────────────────────────
    d = data.copy()
    if days <= 365 and d["日付"].notna().any():
        cutoff = d["日付"].max() - pd.Timedelta(days=int(days))
        d = d[d["日付"] >= cutoff]
    if markets:
        d = d[d["市場"].isin(markets)]
    if judges:
        d = d[d["判定"].isin(judges)]
    if brokers:
        d = d[d["シンクタンク"].isin(brokers)]

    excluded = int((~d["普通株"].fillna(True).astype(bool)).sum()) if "普通株" in d.columns else 0

    if d.empty:
        st.warning("条件に合うレーティングがありません。フィルターを緩めてください。")
        return

    # ── 一覧 ──────────────────────────────────────────────────────────────
    if view == "銘柄ごとに集計":
        t = rating.aggregate_by_stock(d, how)
        t = t[t["乖離率(%)"].notna() & (t["乖離率(%)"] >= min_up)]
        cols = ["コード", "銘柄名", "市場", "社数", "現在株価", "目標株価",
                "最低目標", "最高目標", "乖離率(%)", "引き上げ", "引き下げ",
                "直近日付", "各社の更新日"]
        t = t[[c for c in cols if c in t.columns]]
        fmt = {"現在株価": "{:,.1f}", "目標株価": "{:,.1f}", "最低目標": "{:,.1f}",
               "最高目標": "{:,.1f}", "乖離率(%)": "{:+.1f}%"}
        st.markdown(f"### 📋 乖離率ランキング（{len(t):,} 銘柄）")
    else:
        t = d[d["乖離率(%)"].notna() & (d["乖離率(%)"] >= min_up)].copy()
        t = t.sort_values("乖離率(%)", ascending=False)
        cols = ["日付", "コード", "銘柄名", "市場", "シンクタンク", "レーティング",
                "判定", "変更前", "目標株価", "目標変化率(%)",
                "記録時株価", "現在株価", "乖離率(%)"]
        t = t[[c for c in cols if c in t.columns]]
        fmt = {"現在株価": "{:,.1f}", "記録時株価": "{:,.1f}", "目標株価": "{:,.1f}",
               "変更前": "{:,.1f}", "乖離率(%)": "{:+.1f}%", "目標変化率(%)": "{:+.1f}%"}
        st.markdown(f"### 📋 乖離率ランキング（{len(t):,} 件）")

    if t.empty:
        st.warning("条件に合う銘柄がありません。乖離率の下限を下げてください。")
        return

    _styled = t.style.format(fmt)
    if "乖離率(%)" in t.columns:
        _styled = _styled.background_gradient(subset=["乖離率(%)"], cmap="RdYlGn")
    _colcfg = {}
    if "日付" in t.columns:
        _colcfg["日付"] = st.column_config.DateColumn(format="MM/DD")
    if "直近日付" in t.columns:
        _colcfg["直近日付"] = st.column_config.DateColumn("直近更新", format="MM/DD")
    if "各社の更新日" in t.columns:
        _colcfg["各社の更新日"] = st.column_config.TextColumn(
            "各社の更新日", width="large",
            help="そのシンクタンクが最後に目標株価を出した日。新しい順に並んでいます。",
        )
    st.dataframe(_styled, use_container_width=True, hide_index=True,
                 column_config=_colcfg or None)

    st.download_button(
        "📥 CSV ダウンロード", data=_csv(t),
        file_name="rating_upside.csv", mime="text/csv", key="rt_dl",
    )

    # ── 注意書き ──────────────────────────────────────────────────────────
    with st.expander("ℹ️ 数字の読み方と注意"):
        st.markdown(
            f"""
- **乖離率** = 目標株価 ÷ 現在株価 − 1。現在株価は Yahoo Finance の直近終値で、
  アーカイブの古い行もすべて**今の株価で引き直しています**。
  「記録時株価」はアーカイブに記録した時点の株価で、参考値です。
- 取得元は**当月ぶんしか掲載していません**。月が替わると消えるため、
  ローカルで `update_rating.bat` を定期実行して `rating_snapshot.json` に
  蓄積してください（追記式・重複は自動で除外）。掲載が3週間ぶんなので
  **最低2週間に1回**走らせれば取りこぼしません。
- レーティングは収録期間内の発表をそのまま並べています。**古い目標株価ほど現在の
  株価水準と噛み合わない**ため、対象期間を絞って見てください。
- 「銘柄ごとに集計」では、同じ銘柄×同じシンクタンクは**最新の1件だけ**を使います。
  「最も強気な目標」を選ぶと乖離率は最大値になるので、社数と最低目標も併せて見てください。
- レーティングの表記はシンクタンクごとに流儀が違います
  （数字系は 1 が最上位、英語系は Buy > Outperform > Neutral など）。
  引き上げ / 引き下げ の判定はこの差を吸収したうえで行っています。
- 5桁コードの種類株・優先出資証券は、目標株価が普通株に対するもので建値が違うため
  ランキングから除外しています{f"（今回 {excluded} 件）" if excluded else ""}。
  同じ発表は普通株の行にも載っているので取りこぼしはありません。
- **目標株価はアナリストの見通しであって、株価がそこへ向かう保証はありません。**
  乖離率が極端に大きい銘柄は、目標が据え置かれたまま株価が大きく下げたケースが混じります。
            """.strip()
        )
