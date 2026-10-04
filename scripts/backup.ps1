param(
    [Parameter(Mandatory = $true)][string]$JournalDir,
    [Parameter(Mandatory = $true)][string]$OutDir,
    [string]$BaseUrl = "http://127.0.0.1:3000"
)
# Backup: graceful-shutdown-first, then copy sealed segments + meta + DLQ/audit.
# Usage: scripts/backup.ps1 -JournalDir ./data -OutDir ./backup-20240101
$ErrorActionPreference = "Stop"

if (-not (Test-Path $JournalDir)) { throw "journal dir missing: $JournalDir" }

# Best effort: ask a hooks-enabled server to drain first (no-op otherwise).
try {
    Invoke-RestMethod -Method Post -Uri "$BaseUrl/test/shutdown" -TimeoutSec 3 | Out-Null
    Start-Sleep 2
} catch {
    Write-Output "note: shutdown hook unavailable, copy journal as-is (replay truncates torn tails)"
}

New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
$files = @("meta.json", "dlq.wal", "audit.wal") +
    (Get-ChildItem $JournalDir -Filter "journal-*.wal" -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Name) +
    (Get-ChildItem $JournalDir -Filter "events-*.wal" -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Name)
$manifest = @()
foreach ($f in $files | Sort-Object -Unique) {
    $src = Join-Path $JournalDir $f
    if (Test-Path $src) {
        Copy-Item $src (Join-Path $OutDir $f) -Force
        $h = (Get-FileHash (Join-Path $OutDir $f) -Algorithm SHA256).Hash
        $manifest += [pscustomobject]@{ file = $f; sha256 = $h }
    }
}
$manifest | ConvertTo-Json | Set-Content (Join-Path $OutDir "MANIFEST.json")
Write-Output "backup: $($manifest.Count) files -> $OutDir"
