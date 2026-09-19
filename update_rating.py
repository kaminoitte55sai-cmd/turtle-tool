"""レーティングのスナップショットをローカルで更新するスクリプト。

Streamlit Cloud から取得元にアクセスできない場合に使う。
ローカルで実行して rating_snapshot.json を書き出し、git commit / push すると
アプリ側は「スナップショットから読む」にチェックを入れて利用できる。

    python update_rating.py            取得してスナップショットを更新
    python update_rating.py --no-price 株価取得を省いてレーティングだけ保存

株価も一緒に保存しておくと、Cloud 側で株価取得ができない場合にも
乖離率がそのまま表示できる。
"""

from __future__ import annotations

import argparse
import sys

from modules import rating


def main() -> int:
    ap = argparse.ArgumentParser(description="レーティングのスナップショットを更新する")
    ap.add_argument("--no-price", action="store_true",
                    help="現在株価の取得を省略する")
    args = ap.parse_args()

    print(f"取得元: {rating.SOURCE_URL}")
    try:
        df = rating.fetch_ratings()
    except Exception as e:
        print(f"取得に失敗しました: {e}", file=sys.stderr)
        return 1

    print(f"  レーティング {len(df):,} 件 / {df['コード'].nunique():,} 銘柄")
    if df["日付"].notna().any():
        print(f"  期間 {df['日付'].min().date()} 〜 {df['日付'].max().date()}")
    print(f"  判定内訳 {df['判定'].value_counts().to_dict()}")

    if args.no_price:
        out = df
    else:
        codes = sorted(set(df["コード"]))
        print(f"  株価を取得中...（普通株のみ）")
        prices = rating.fetch_prices(codes)
        print(f"  株価取得 {len(prices):,} 銘柄")
        out = rating.add_upside(df, prices)

    rating.save_snapshot(out)
    print(f"保存しました: {rating.SNAPSHOT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
