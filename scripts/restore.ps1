param(
    [Parameter(Mandatory = $true)][string]$BackupDir,
    [Parameter(Mandatory = $true)][string]$JournalDir
)
# Restore: verify checksums, copy into a fresh dir, print next step.
# Usage: scripts/restore.ps1 -BackupDir ./backup-20240101 -JournalDir ./data-restored
$ErrorActionPreference = "Stop"

$manifest = Get-Content (Join-Path $BackupDir "MANIFEST.json") -Raw | ConvertFrom-Json
New-Item -ItemType Directory -Path $JournalDir -Force | Out-Null
foreach ($m in $manifest) {
    $src = Join-Path $BackupDir $m.file
    $dst = Join-Path $JournalDir $m.file
    Copy-Item $src $dst -Force
    $h = (Get-FileHash $dst -Algorithm SHA256).Hash
    if ($h -ne $m.sha256) { throw "checksum mismatch: $($m.file)" }
}
Write-Output "restore: $($manifest.Count) files verified -> $JournalDir"
Write-Output "next: start server with JOURNAL_DIR=$JournalDir and query GET /payments/:id"
