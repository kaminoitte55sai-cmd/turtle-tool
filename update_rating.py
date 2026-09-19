"""レーティングのアーカイブをローカルで更新するスクリプト。

取得元（トレーダーズ・ウェブの注目レーティング）は当月ぶん・直近3週間程度しか
掲載しておらず、月が替わると今のデータはページから消える。
このスクリプトは取得ぶんを rating_snapshot.json へ**追記**して蓄積する。
上書きではないので、走らせるたびに収録期間が伸びていく。

    python update_rating.py              取得してアーカイブに追記
    python update_rating.py --no-price   株価取得を省いてレーティングだけ追記
    python update_rating.py --replace    追記ではなく丸ごと置き換える（通常使わない）

掲載が3週間ぶんなので、**最低でも2週間に1回**は走らせること。
Windows ならタスクスケジューラに update_rating.bat を登録しておくと取りこぼさない。

更新後は git commit / push すると Streamlit Cloud 側にも反映される。
"""

from __future__ import annotations

import argparse
import sys

from modules import rating


def main() -> int:
    ap = argparse.ArgumentParser(
        description="レーティングを取得してアーカイブに追記する")
    ap.add_argument("--no-price", action="store_true",
                    help="現在株価の取得を省略する")
    ap.add_argument("--replace", action="store_true",
                    help="追記ではなくアーカイブを丸ごと置き換える")
    args = ap.parse_args()

    before, updated_at = rating.load_snapshot()
    cov_before = rating.archive_coverage(before)
    if cov_before:
        print(f"既存アーカイブ: {cov_before['件数']:,} 件 / "
              f"{cov_before['最古']} 〜 {cov_before['最新']}"
              f"（{cov_before['月数']} ヶ月ぶん、最終更新 {updated_at}）")
    else:
        print("既存アーカイブ: なし（新規作成します）")

    print(f"取得元: {rating.SOURCE_URL}")
    try:
        df = rating.fetch_ratings()
    except Exception as e:
        print(f"取得に失敗しました: {e}", file=sys.stderr)
        print("アーカイブは変更していません。", file=sys.stderr)
        return 1

    print(f"  今回の掲載ぶん: {len(df):,} 件 / {df['コード'].nunique():,} 銘柄")
    if df["日付"].notna().any():
        print(f"  掲載期間 {df['日付'].min().date()} 〜 {df['日付'].max().date()}")
    print(f"  判定内訳 {df['判定'].value_counts().to_dict()}")

    if args.no_price:
        out = df
    else:
        print("  株価を取得中...（普通株のみ）")
        prices = rating.fetch_prices(sorted(set(df["コード"])))
        print(f"  株価取得 {len(prices):,} 銘柄")
        out = rating.add_upside(df, prices)

    added, total = rating.save_snapshot(out, merge=not args.replace)

    after, _ = rating.load_snapshot()
    cov = rating.archive_coverage(after)
    print()
    if args.replace:
        print(f"アーカイブを置き換えました: {total:,} 件")
    else:
        print(f"アーカイブに {added:,} 件を追加しました（合計 {total:,} 件）")
    if cov:
        print(f"  収録期間 {cov['最古']} 〜 {cov['最新']}"
              f"（{cov['月数']} ヶ月 / {cov['銘柄数']:,} 銘柄）")
        print(f"  収録月 {', '.join(cov['収録月'])}")
    print(f"  保存先 {rating.SNAPSHOT_PATH}")
    if added == 0 and not args.replace:
        print("  ※ 新しいレーティングはありませんでした。")
    print()
    print("git commit / push すると Streamlit Cloud 側にも反映されます。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
