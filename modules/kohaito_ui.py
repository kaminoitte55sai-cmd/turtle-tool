"""高配当株PFタブの描画（表示処理のみ）。

app.py のタブから `kohaito_ui.render()` として呼ばれる。
データ処理は modules/kohaito.py に委譲する。

タブとして描画するため st.set_page_config は呼ばず、st.stop() も使わない
（タブ内で止めるとアプリ全体が停止するので早期 return する）。
ウィジェットの key はすべて "kh_" 始まりにして app.py 側との衝突を避ける。
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from modules import kohaito


def _csv(df: pd.DataFrame) -> bytes:
    """Excel で開いても文字化けしないよう BOM 付き UTF-8 で出力する。"""
    return df.to_csv(index=False).encode("utf-8-sig")


def render() -> None:
    st.subheader("💰 高配当株PF")
    st.caption(
        "某哲也氏のブログの銘柄記事から「分類」「判定」を抽出し、"
        "現在の株価・PER・PBR・配当利回りを添えた候補リストです。"
    )

    up = st.file_uploader(
        "CSV を読み込む",
        type=["csv"],
        key="kh_upload",
        help="列: code, name（, category, judgement, price, per, pbr, yield, comment …）",
    )

    if up is not None:
        try:
            df = kohaito.read_csv(up)
            # 読み込み直したらセッションの内容を差し替える
            if st.session_state.get("kh_src") != up.name:
                st.session_state["kh_src"] = up.name
                st.session_state["kh_data"] = df
            st.caption(f"読み込み: {up.name}（{len(df)} 銘柄）")
        except Exception as e:
            st.error(f"CSV を読み込めませんでした: {e}")
            return
    else:
        df = kohaito.load()
        if df.empty:
            st.info(
                "銘柄リストがありません。上の「CSV を読み込む」からファイルを指定してください。"
                "リポジトリ直下に `boutetsuya_stocks.csv` を置いておけば自動で読み込まれます。"
            )
            return

    # セッション中に価格を更新したらそれを使う
    if "kh_data" not in st.session_state:
        st.session_state["kh_data"] = df
    data = st.session_state["kh_data"]

    s = kohaito.summarize(data)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("銘柄数", f"{s['n']} 銘柄", delta=f"現在値あり {s['n_price']}")
    c2.metric(
        "平均配当利回り",
        "-" if s["mean_yield"] is None else f"{s['mean_yield']:.2f}%",
    )
    c3.metric("利回り4%以上", f"{s['n_high_yield']} 銘柄")
    c4.metric("分類数", f"{s['categories']} 種類")

    if s["date_to"] is not None and pd.notna(s["date_to"]):
        st.caption(
            f"記事日付： {s['date_from']:%Y-%m-%d} 〜 {s['date_to']:%Y-%m-%d}　"
            f"／ 分類・判定・ブログ記載値は各銘柄の最新記事から抽出。"
            f"出典: {kohaito.SOURCE_URL}"
        )

    # 「判定：〜」はブログが2026年以降の記事で使い始めた書式で、
    # それ以前に取り上げられたきりの銘柄には無い。空欄の理由が分かるよう明示する。
    n_judge = int(data["judgement"].notna().sum()) if "judgement" in data else 0
    if n_judge < len(data):
        st.caption(
            f"⚠️ 「判定」はブログが2026年以降の記事で使い始めた書式のため、"
            f"{n_judge}/{len(data)} 銘柄にのみ記載があります。"
            f"それ以前の銘柄は「考察」列に著者の見解を収録しています。"
        )

    if st.button("🔄 株価を更新", key="kh_refresh"):
        with st.spinner("最新の終値を取得しています…"):
            updated, n = kohaito.refresh_prices(data)
        st.session_state["kh_data"] = updated
        if n:
            st.success(f"{n} 銘柄の株価を更新しました。")
            st.rerun()
        else:
            st.warning("株価を取得できませんでした。")

    # --- 絞り込み ---
    st.divider()
    f1, f2, f3, f4 = st.columns([1.2, 1.2, 1, 1])
    with f1:
        cats = [c for c in kohaito.CATEGORY_ORDER if c in set(data["category"].dropna())]
        cats += sorted(set(data["category"].dropna()) - set(cats))
        pick_cat = st.multiselect("分類", cats, key="kh_cat")
    with f2:
        judges = [j for j in kohaito.JUDGEMENT_ORDER if j in set(data["judgement"].dropna())]
        judges += sorted(set(data["judgement"].dropna()) - set(judges))
        pick_judge = st.multiselect("判定", judges, key="kh_judge")
    with f3:
        min_yield = st.number_input(
            "配当利回り下限(%)", min_value=0.0, max_value=15.0, value=0.0, step=0.5,
            key="kh_minyield",
        )
    with f4:
        q = st.text_input("コード・銘柄名", placeholder="例: 1939 / 四電工", key="kh_q")

    view = data.copy()
    if pick_cat:
        view = view[view["category"].isin(pick_cat)]
    if pick_judge:
        view = view[view["judgement"].isin(pick_judge)]
    if min_yield > 0:
        view = view[view["yield"].fillna(-1) >= min_yield]
    if q.strip():
        t = q.strip()
        view = view[
            view["code"].str.contains(t, case=False, na=False)
            | view["name"].str.contains(t, case=False, na=False)
        ]

    # 分類→判定の順で並べる（ブログ内の位置づけが重い順）
    view = view.assign(
        _c=kohaito.sort_key(view["category"], kohaito.CATEGORY_ORDER),
        _j=kohaito.sort_key(view["judgement"], kohaito.JUDGEMENT_ORDER),
    ).sort_values(["_c", "_j", "yield"], ascending=[True, True, False], ignore_index=True)

    table = view.rename(
        columns={
            "code": "コード",
            "name": "銘柄名",
            "price": "現在株価",
            "per": "PER",
            "pbr": "PBR",
            "yield": "配当利回り",
            "category": "分類",
            "judgement": "判定",
            "blog_price": "記事株価",
            "blog_per": "記事PER",
            "blog_yield": "記事利回り",
            "article_date": "記事日",
            "yutai": "株主優待",
            "shareholder_return": "株主還元",
            "comment": "考察",
            "article_url": "記事URL",
        }
    )
    cols = ["コード", "銘柄名", "分類", "判定", "現在株価", "PER", "PBR", "配当利回り",
            "記事株価", "記事PER", "記事利回り", "記事日", "株主優待", "株主還元",
            "考察", "記事URL"]
    table = table[[c for c in cols if c in table.columns]]

    st.caption(f"該当 {len(table)} 銘柄（分類 → 判定 → 利回りの順）")
    st.dataframe(
        table,
        use_container_width=True,
        hide_index=True,
        height=460,
        column_config={
            "現在株価": st.column_config.NumberColumn("現在株価", format="%.1f"),
            "PER": st.column_config.NumberColumn("PER", format="%.1f", help="実績ベース（yfinance）"),
            "PBR": st.column_config.NumberColumn("PBR", format="%.2f"),
            "配当利回り": st.column_config.NumberColumn("配当利回り", format="%.2f%%"),
            "記事株価": st.column_config.NumberColumn("記事株価", format="%.0f"),
            "記事PER": st.column_config.NumberColumn(
                "記事PER", format="%.1f", help="記事執筆時点。会社予想ベースのことが多い"
            ),
            "記事利回り": st.column_config.NumberColumn("記事利回り", format="%.2f%%"),
            "記事日": st.column_config.DatetimeColumn("記事日", format="YYYY-MM-DD"),
            "考察": st.column_config.TextColumn(
                "考察", width="large",
                help="記事の「株価考察」。判定行がない旧記事でも著者の見解がわかる",
            ),
            "記事URL": st.column_config.LinkColumn("記事", display_text="開く"),
        },
    )
    st.download_button(
        "⬇ 表示中のデータを CSV 保存",
        _csv(table),
        file_name="高配当株PF.csv",
        mime="text/csv",
        key="kh_dl",
    )

    # --- 分類別・判定別の内訳 ---
    st.divider()
    t1, t2 = st.tabs(["分類別", "判定別"])
    with t1:
        g = (
            view.groupby("category")
            .agg(銘柄数=("code", "size"), 平均利回り=("yield", "mean"), 平均PER=("per", "mean"))
            .reset_index()
            .rename(columns={"category": "分類"})
        )
        g = g.assign(_o=kohaito.sort_key(g["分類"], kohaito.CATEGORY_ORDER)).sort_values(
            "_o", ignore_index=True
        ).drop(columns="_o")
        st.dataframe(
            g, use_container_width=True, hide_index=True,
            column_config={
                "平均利回り": st.column_config.NumberColumn("平均利回り", format="%.2f%%"),
                "平均PER": st.column_config.NumberColumn("平均PER", format="%.1f"),
            },
        )
    with t2:
        g2 = (
            view.groupby("judgement")
            .agg(銘柄数=("code", "size"), 平均利回り=("yield", "mean"), 平均PBR=("pbr", "mean"))
            .reset_index()
            .rename(columns={"judgement": "判定"})
        )
        g2 = g2.assign(_o=kohaito.sort_key(g2["判定"], kohaito.JUDGEMENT_ORDER)).sort_values(
            "_o", ignore_index=True
        ).drop(columns="_o")
        st.dataframe(
            g2, use_container_width=True, hide_index=True,
            column_config={
                "平均利回り": st.column_config.NumberColumn("平均利回り", format="%.2f%%"),
                "平均PBR": st.column_config.NumberColumn("平均PBR", format="%.2f"),
            },
        )
