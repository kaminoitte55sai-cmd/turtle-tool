# 財務スクリーニング深掘りプロンプト
# 使い方: claude -p "$(cat prompts/screening_deep.md)" --allowedTools edinetdb

## タスク
EdinetDB MCP (`edinetdb`) を使って、財務指標でスクリーニングし、通過銘柄を深掘り分析してください。

## スクリーニング条件（変更可能）
以下の条件で `screen_companies` を実行してください:

```json
[
  { "metric": "roe",              "operator": "gte", "value": 15   },
  { "metric": "operating_margin", "operator": "gte", "value": 10   },
  { "metric": "revenue_growth",   "operator": "gte", "value": 5    },
  { "metric": "equity_ratio",     "operator": "gte", "value": 40   },
  { "metric": "per",              "operator": "lte", "value": 25   }
]
```

- ソート: `roe` 降順
- 上限: 20件

## 手順

### Step 1: スクリーニング実行
- `screen_companies` で上記条件を適用して候補を抽出

### Step 2: 上位10銘柄の一括取得
- `search_companies_batch` で上位10銘柄の詳細を一括取得

### Step 3: 財務ランキング確認
- `get_ranking` で `roe`・`revenue-growth`・`health-score` のランキングを確認
- スクリーニング通過銘柄のランキング順位を把握

### Step 4: 上位3銘柄の詳細分析
上位3銘柄について:
- `get_financials` で5年トレンド確認
- `get_earnings` で直近決算の進捗確認

## 出力フォーマット

### 1. スクリーニング結果サマリー
- 通過銘柄数: XX社
- 条件の厳しさ評価と必要に応じた条件緩和提案

### 2. 通過銘柄一覧

| 証券コード | 企業名 | 業種 | ROE | 営業利益率 | 売上成長率 | PER | 健全性スコア |
|-----------|--------|------|-----|-----------|-----------|-----|------------|
| | | | | | | | |

### 3. 注目銘柄トップ3（詳細）
各銘柄について:
- **財務ハイライト**: 直近本決算の主要数値
- **成長トレンド**: 5年間の売上・利益の方向性
- **直近決算の進捗**: 通期予想に対する進捗率
- **投資ポイント**: 2〜3行の評価コメント

### 4. 総合まとめ
- 現在の市場環境を踏まえた注目セクターのコメント
- 次のアクション提案（ウォッチリスト登録・追加調査推奨銘柄）

## 出力言語: 日本語
