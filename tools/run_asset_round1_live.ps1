$ErrorActionPreference = 'Stop'

$workspace = 'E:\gain'
$logPath = Join-Path $workspace 'reports\asset-discovery-round1-live.log'
$errPath = Join-Path $workspace 'reports\asset-discovery-round1-live.err.log'

Set-Location $workspace

if (Test-Path $logPath) {
    Remove-Item -LiteralPath $logPath -Force
}
if (Test-Path $errPath) {
    Remove-Item -LiteralPath $errPath -Force
}

try {
    python -u -m b3_patterns asset-discover-round1 `
        --start-date 2025-04-10 `
        --end-date 2026-04-10 `
        --entry-rules open close `
        --target-stop-pairs 1:1 2:1 3:1.5 4:2 6:3 `
        --time-cap-days 5 `
        --max-pattern-size 2 `
        --progress-every-tickers 25 `
        --min-profit-factor 1.30 `
        --min-average-trade-return-pct 0.10 `
        --min-trades 200 `
        --min-tickers 20 2>&1 | Tee-Object -FilePath $logPath
} catch {
    $_ | Out-String | Tee-Object -FilePath $errPath -Append
    throw
}
