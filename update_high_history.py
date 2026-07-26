"""上場来高値データをローカルで取得し、Streamlit Cloud へ反映するスクリプト。

■ なぜローカルで実行する必要があるのか
株探は Streamlit Cloud からのリクエストを WAF で拒否する（全リクエストが HTTP 405）。
そのため Cloud 上の「🔄 自動取得」ボタンは使えない。
代わりにこのスクリプトをローカルで実行すると:

    ローカルで取得 -> high_history.db 更新 -> high_history_snapshot.json 書き出し
    -> git commit & push -> Cloud が起動時に自動復元

という流れで Cloud 側のデータが更新される。

■ 使い方（リポジトリのルートで実行）
    python update_high_history.py              直近の新着を取得して反映
    python update_high_history.py --deep       過去サイトマップまで遡って探索
    python update_high_history.py --max 20     取得する記事数の上限を指定
    python update_high_history.py --no-push    git push はせずローカルまで

週次記事なので、毎週土曜以降に引数なしで実行すれば1週ぶんが追加される。
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from modules import db  # noqa: E402
from modules import kabutan_high  # noqa: E402

SNAPSHOT_PATH = os.path.join(BASE_DIR, db.SNAPSHOT_REPO_PATH)


def _run_git(*args: str) -> tuple[int, str]:
    """git コマンドを実行して (終了コード, 出力) を返す。"""
    p = subprocess.run(
        ["git", *args], cwd=BASE_DIR, capture_output=True, text=True, encoding="utf-8"
    )
    return p.returncode, (p.stdout or "") + (p.stderr or "")


def seed_from_snapshot_file() -> int:
    """DB が空でスナップショットファイルがあれば、そこから復元する。

    リポジトリを clone し直した直後などに、Cloud 側の蓄積を引き継ぐため。
    """
    with db.get_conn() as conn:
        if conn.execute("SELECT COUNT(*) c FROM high_history").fetchone()["c"] > 0:
            return 0
    if not os.path.exists(SNAPSHOT_PATH):
        return 0

    with open(SNAPSHOT_PATH, encoding="utf-8") as f:
        data = json.load(f)
    n = db.insert_rows(data.get("high_history", []))
    for a in data.get("fetched_articles", []):
        db.record_article(
            a["article_id"],
            a.get("url", ""),
            a.get("article_date", ""),
            a.get("title", ""),
            a.get("n_codes", 0),
            a.get("status", "ok"),
        )
    if data.get("updated_at"):
        db.set_meta("updated_at", data["updated_at"])
    return n


def write_snapshot() -> int:
    """DB の内容を high_history_snapshot.json に書き出し、銘柄行数を返す。

    db.push_snapshot() は GitHub API 経由だが、st.secrets が
    Streamlit ランタイム外では読めないため、ここではファイルに書いて
    通常の git commit で反映させる（トークンを扱わずに済む）。
    """
    with db.get_conn() as conn:
        hist = pd.read_sql_query(
            "SELECT article_date, code, name, title, sector, market FROM high_history", conn
        ).to_dict("records")
        arts = pd.read_sql_query(
            "SELECT article_id, url, article_date, title, n_codes, status, fetched_at "
            "FROM fetched_articles",
            conn,
        ).to_dict("records")

    payload = {
        "high_history": hist,
        "fetched_articles": arts,
        "updated_at": db.get_meta("updated_at"),
    }
    with open(SNAPSHOT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    return len(hist)


def main() -> int:
    ap = argparse.ArgumentParser(description="上場来高値データを取得して Cloud へ反映する")
    ap.add_argument("--deep", action="store_true", help="過去サイトマップまで遡って探索する")
    ap.add_argument("--max", type=int, default=10, help="取得する記事数の上限（既定 10）")
    ap.add_argument("--no-push", action="store_true", help="git commit / push を行わない")
    args = ap.parse_args()

    db.init_db()

    if restored := seed_from_snapshot_file():
        print(f"スナップショットから {restored} 件を復元しました")

    before = db.stats()
    print(f"取得前： 記事 {before['articles']} 件 / 銘柄 {before['rows']} 件")
    print("株探から取得しています（robots.txt の Crawl-delay 3秒を遵守）…")

    def on_progress(current: int, total: int, message: str) -> None:
        print(f"  [{current}/{total}] {message}", flush=True)

    articles, skipped, diag = kabutan_high.collect(
        known_ids=db.known_article_ids(),
        known_dates=db.known_article_dates(),
        max_articles=args.max,
        deep_history=args.deep,
        progress_cb=on_progress,
    )

    # 対象外・取得失敗だった候補は、記事が1件も取れなかった場合でも必ず記録する。
    # ここを早期 return より後ろに置くと、毎回同じ外れ候補を取りに行ってしまう。
    for sk in skipped:
        db.record_article(sk["article_id"], sk["url"], "", "", 0, sk["reason"][:40])

    # --- 取得できなかった場合は原因を出して終了 ---
    if not articles:
        if diag["ok"] == 0 and diag["requests"] > 0:
            print(
                f"\n[失敗] 株探へアクセスできませんでした"
                f"（{diag['requests']} 回試行して成功 0 回）\n"
                f"        エラー内訳: {diag['errors']}\n"
                f"        直前のエラー: {diag['last_error']}",
                file=sys.stderr,
            )
            return 1
        print("\n新しい記事はありませんでした。")
        return 0

    # --- DB へ反映 ---
    added = 0
    for art in articles:
        added += db.insert_rows(art.to_rows())
        db.record_article(
            art.article_id, art.url, art.article_date, art.title, len(art.stocks), "ok"
        )
        print(f"  + {art.article_date}  {art.title[:48]} … {len(art.stocks)}銘柄")

    updated_at = db.touch_updated_at()
    n_rows = write_snapshot()

    after = db.stats()
    print(f"\n取得後： 記事 {after['articles']} 件 / 銘柄 {after['rows']} 件"
          f"（今回 +{len(articles)} 記事 / +{added} 銘柄）")
    print(f"収録期間： {after['date_from']} 〜 {after['date_to']}")
    print(f"スナップショット書き出し： {n_rows} 行 / 更新日時 {updated_at}")

    if args.no_push:
        print("\n--no-push が指定されたため git 操作は行いません。")
        return 0

    # --- git へ反映（Cloud はこれを起動時に復元する）---
    print("\nGitHub へ反映しています…")
    code, out = _run_git("add", db.SNAPSHOT_REPO_PATH)
    if code != 0:
        print(f"[警告] git add に失敗しました: {out}", file=sys.stderr)
        return 1

    code, out = _run_git(
        "commit", "-m", f"上場来高値データ更新（〜{after['date_to']}）"
    )
    if code != 0 and "nothing to commit" in out:
        print("変更はありませんでした。")
        return 0
    if code != 0:
        print(f"[警告] git commit に失敗しました: {out}", file=sys.stderr)
        return 1

    # アプリ自身がポジションJSONを頻繁に自動コミットするため、必ず rebase してから push する。
    # --autostash を付けないと、作業中の未ステージ変更があるだけで rebase が失敗する。
    _run_git("fetch", "origin")
    code, out = _run_git("rebase", "--autostash", "origin/main")
    if code != 0:
        print(f"[警告] rebase に失敗しました。手動で解決してください:\n{out}", file=sys.stderr)
        return 1

    code, out = _run_git("push", "origin", "main")
    if code != 0:
        print(f"[警告] push に失敗しました: {out}", file=sys.stderr)
        return 1

    print("完了しました。Streamlit Cloud は再起動時にこのデータを復元します。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
