# daily_earnings_report.ps1
# 今日決算発表のあった企業のレポートを Markdown で生成する
# 使い方: powershell -ExecutionPolicy Bypass -File daily_earnings_report.ps1
# タスクスケジューラで毎日夜間に自動実行することを想定

param(
    [string]$Date = (Get-Date -Format "yyyy-MM-dd"),
    [switch]$Force   # 既存レポートを上書きする場合
)

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$ReportsDir  = Join-Path $ScriptDir "reports"
$LogsDir     = Join-Path $ScriptDir "logs"
$ClaudeExe   = "C:\Users\トシヒロ\AppData\Roaming\Claude\claude-code\2.1.87\claude.exe"
$OutputFile  = Join-Path $ReportsDir "$Date.md"
$LogFile     = Join-Path $LogsDir    "run-$Date.log"

# ── ディレクトリ確認 ─────────────────────────────────────────────────────────
New-Item -ItemType Directory -Force -Path $ReportsDir | Out-Null
New-Item -ItemType Directory -Force -Path $LogsDir    | Out-Null

# ── 既存チェック ─────────────────────────────────────────────────────────────
if ((Test-Path $OutputFile) -and -not $Force) {
    Write-Host "[SKIP] $OutputFile は既に存在します。上書きするには -Force を付けてください。"
    exit 0
}

# ── Claude CLI 確認 ──────────────────────────────────────────────────────────
if (-not (Test-Path $ClaudeExe)) {
    Write-Error "Claude CLI が見つかりません: $ClaudeExe"
    exit 1
}

# ── プロンプト構築 ───────────────────────────────────────────────────────────
$Prompt = @"
今日 $Date に決算発表のあった企業をすべて EdinetDB の get_earnings_calendar ツールで取得し、
各社について以下を整理した Markdown レポートを作成して、
ファイル「$OutputFile」に保存してください。

## 出力フォーマット（1社ずつ）

### {企業名}（{証券コード}）
- **決算種別**: 第N四半期 / 本決算
- **売上高**: X億円（前年同期比 +/-X%）
- **営業利益**: X億円（前年同期比 +/-X%）
- **純利益**: X億円（前年同期比 +/-X%）
- **EPS**: X円
- **通期予想進捗率**: 売上 X% / 営業利益 X%（会社予想比）
- **一言コメント**: 上振れ/下振れ/概ね一致、注目点を1行で

レポートの先頭に以下のヘッダーを付けること:
# 決算速報レポート $Date
生成日時: $(Get-Date -Format "yyyy-MM-dd HH:mm")
対象企業数: N社

データ取得手順:
1. get_earnings_calendar(date="$Date") で発表企業一覧を取得
2. 各社の search_companies で edinetCode を取得（レート制限対策: 各呼び出し間に2秒待機）
3. get_earnings(edinet_code=..., limit=2) で直近決算データを取得
4. データが取得できなかった企業はスキップし、取得できた企業だけをレポート化
5. 最後に「データ取得できなかった企業リスト」セクションを追記

出力は日本語・数値重視で。ファイルはUTF-8で保存すること。
"@

Write-Host "======================================"
Write-Host " 決算レポート生成開始: $Date"
Write-Host "======================================"
Write-Host "出力先: $OutputFile"
Write-Host "ログ:   $LogFile"
Write-Host ""

# ── Claude 実行 ──────────────────────────────────────────────────────────────
$StartTime = Get-Date

try {
    & $ClaudeExe --dangerously-skip-permissions -p $Prompt 2>&1 | Tee-Object -FilePath $LogFile
    $ExitCode = $LASTEXITCODE
} catch {
    Write-Error "Claude 実行エラー: $_"
    $ExitCode = 1
}

$ElapsedSec = [int]((Get-Date) - $StartTime).TotalSeconds

# ── 結果確認 ─────────────────────────────────────────────────────────────────
if ($ExitCode -eq 0 -and (Test-Path $OutputFile)) {
    $LineCount = (Get-Content $OutputFile | Measure-Object -Line).Lines
    Write-Host ""
    Write-Host "[OK] レポート生成完了 ($ElapsedSec 秒)"
    Write-Host "     ファイル: $OutputFile ($LineCount 行)"
} else {
    Write-Host ""
    Write-Host "[ERROR] レポート生成に失敗しました (終了コード: $ExitCode)"
    Write-Host "        ログを確認: $LogFile"
    exit 1
}
