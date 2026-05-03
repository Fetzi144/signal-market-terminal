param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$RemoteCommand
)

$ErrorActionPreference = "Stop"

$hostName = if ($env:SMT_PROD_SSH_HOST) { $env:SMT_PROD_SSH_HOST } else { "smt-prod-1" }
$userName = if ($env:SMT_PROD_SSH_USER) { $env:SMT_PROD_SSH_USER } else { "root" }
$keyPath = if ($env:SMT_PROD_SSH_KEY) { $env:SMT_PROD_SSH_KEY } else { Join-Path $HOME ".ssh\hetzner_smt_ed25519_clean" }

if (-not (Test-Path -LiteralPath $keyPath)) {
    throw "Production SSH key not found at '$keyPath'. Set SMT_PROD_SSH_KEY to the private key path."
}

$sshArgs = @(
    "-o", "BatchMode=yes",
    "-o", "IdentitiesOnly=yes",
    "-i", $keyPath,
    "$userName@$hostName"
)

if ($RemoteCommand.Count -gt 0) {
    $sshArgs += ($RemoteCommand -join " ")
}

ssh @sshArgs
