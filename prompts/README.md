# 分析プロンプト集

EdinetDB MCP を活用した再現性のある銘柄分析プロンプト集です。

## 使い方

```bash
# 基本構文
claude -p "$(cat prompts/<ファイル名>.md)" --allowedTools edinetdb

# 例: トヨタ(7203)の同業比較
# ※ prompts/peer_compare.md 内の {{TARGET_CODE}} を 7203 に書き換えてから実行
claude -p "$(cat prompts/peer_compare.md)" --allowedTools edinetdb
```

## プロンプト一覧

| ファイル | 用途 | 主要MCPツール |
|---------|------|-------------|
| `stock_analysis.md` | 1銘柄の総合分析レポート | get_company, get_financials, get_earnings, get_text_blocks |
| `peer_compare.md` | 同業他社との比較分析 | screen_companies, search_companies_batch, get_financials |
| `shareholder_scan.md` | 株主構成の詳細スキャン | get_shareholders, get_shareholder_history, get_activist_positions |
| `screening_deep.md` | 財務スクリーニング＋深掘り | screen_companies, get_ranking, get_financials |
| `watchlist_review.md` | ウォッチリスト一括レビュー | get_watchlist, search_companies_batch, get_earnings_calendar |

## 銘柄コードの指定方法

各プロンプトファイル内の `{{TARGET_CODE}}` を対象の**証券コード**（4桁）に書き換えてください。

```bash
# sed で一時的に置き換えて実行する方法（書き換え不要）
claude -p "$(sed 's/{{TARGET_CODE}}/7203/g' prompts/stock_analysis.md)" --allowedTools edinetdb
```

## git での履歴管理

```bash
# プロンプトを改善したらコミット
git add prompts/
git commit -m "prompts: peer_compare に財務健全性スコア列を追加"

# 過去バージョンに戻す
git log prompts/peer_compare.md
git checkout <commit_hash> -- prompts/peer_compare.md
```

## MCP サーバー設定

```bash
# EdinetDB MCP の登録（初回のみ）
claude mcp add --transport http edinetdb https://edinetdb.jp/mcp \
  --header "Authorization: Bearer YOUR_API_KEY"

# 接続確認
claude mcp list
```
