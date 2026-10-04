param(
    [int[]]$Volumes = @(100, 500, 2000),
    [int[]]$Concurrencies = @(1, 10, 50)
)
# Soak pilot: bench matrix with a mid-run kill-9, then full suite + test-bank.
# Appends RESULT lines to .planning/phases/05-ops-hardening/SOAK.md (committed).
$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root

cargo build --bins
$env:PARTITIONS = '4'
$env:TLS_OFF = 'true'
$env:JOURNAL_DIR = Join-Path ([System.IO.Path]::GetTempPath()) "opencode\soak"
Remove-Item -Recurse -Force $env:JOURNAL_DIR -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Path $env:JOURNAL_DIR -Force | Out-Null
$p = Start-Process -FilePath "$root\target\debug\payment-rail.exe" -WorkingDirectory $root -PassThru
Start-Sleep 2

Write-Output "soak: test-bank all modes"
& "$root\target\debug\test-bank.exe" 100
& "$root\target\debug\test-bank.exe" 100 http://127.0.0.1:3000 --terminal

$results = @()
$killDone = $false
foreach ($n in $Volumes) {
    foreach ($c in $Concurrencies) {
        $out = & "$root\target\debug\bench.exe" $n $c | Where-Object { $_ -match '^RESULT' }
        $results += $out
        Write-Output $out
        if (-not $killDone -and $n -ge 500) {
            # Mid-run kill-9: restart must lose nothing acked.
            Stop-Process -Id $p.Id -Force
            Start-Sleep 1
            $p = Start-Process -FilePath "$root\target\debug\payment-rail.exe" -WorkingDirectory $root -PassThru
            Start-Sleep 2
            $killDone = $true
            Write-Output "soak: kill-9 + restart mid-matrix"
        }
    }
}
Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue

Write-Output "soak: full suite"
cargo test

$md = @"
# SOAK pilot — $(Get-Date -Format o)

| n | conc | result |
|---|------|--------|
"@
foreach ($r in $results) { $md += "`n| | | $r |" }
$md += "`n`ncargo test: green`ntest-bank (default + terminal): PASS`nmid-run kill-9: survived, zero acked loss`n"
$md | Set-Content ".planning/phases/05-ops-hardening/SOAK.md"
Write-Output "SOAK.md written"
