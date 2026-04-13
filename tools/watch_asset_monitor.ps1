$ErrorActionPreference = 'Continue'

param(
    [int] $IntervalSeconds = 60,
    [int] $TopStrategies = 10
)

$workspace = 'E:\gain'
Set-Location $workspace

Write-Host "Monitor export loop iniciado em $workspace"
Write-Host "Intervalo: $IntervalSeconds segundos | Top strategies: $TopStrategies"
Write-Host "Use Ctrl+C para parar."

while ($true) {
    $startedAt = Get-Date
    Write-Host "[$($startedAt.ToString('yyyy-MM-dd HH:mm:ss'))] Exportando JSON do monitor..."

    python -m b3_patterns asset-monitor-export `
        --tickers-file lista.md `
        --strategies-csv reports/asset-discovery-lista-r2-exit-refined-final.csv `
        --ticker-stats-csv reports/asset-discovery-lista-r2-exit-refined-final-tickers.csv `
        --overall-actions-csv reports/asset-discovery-lista-r2-exit-refined-final-actions.csv `
        --top-strategies $TopStrategies

    $finishedAt = Get-Date
    Write-Host "[$($finishedAt.ToString('yyyy-MM-dd HH:mm:ss'))] Export finalizado. Proxima atualizacao em $IntervalSeconds segundos."
    Start-Sleep -Seconds $IntervalSeconds
}
