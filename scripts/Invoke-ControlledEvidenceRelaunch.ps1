param(
    [string]$ComposeFile = "docker-compose.prod.yml",
    [string]$CloneProject = "smt-evidence-smoke",
    [string]$LiveProject = "",
    [string]$ReleaseTag = "v0.4.1",
    [string]$MigrationRevision = "038",
    [string]$ContractVersion = "default_strategy_v0.4.1",
    [string]$EvidenceBoundaryId = "v0.4.1",
    [string]$OperatorLogPath = "docs/evidence/default-strategy-operator-log.md",
    [string]$CommitSha = "",
    [int]$WorkerMetricsPort = 9101,
    [int]$WorkerMetricWaitSeconds = 150,
    [string]$CutoverTimestamp = "",
    [switch]$CreateTag,
    [switch]$BootstrapLiveRun,
    [switch]$SkipCloneTeardown
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Compose {
    param(
        [string]$Project,
        [string[]]$Args
    )

    if ([string]::IsNullOrWhiteSpace($Project)) {
        $output = & docker compose -f $ComposeFile @Args
    }
    else {
        $output = & docker compose -f $ComposeFile -p $Project @Args
    }

    if ($LASTEXITCODE -ne 0) {
        throw "docker compose command failed: $($Args -join ' ')"
    }

    return $output
}

function Invoke-ComposeJson {
    param(
        [string]$Project,
        [string[]]$Args
    )

    $raw = Invoke-Compose -Project $Project -Args $Args
    return ($raw -join "`n" | ConvertFrom-Json)
}

function Add-OperatorLogBlock {
    param(
        [string]$Title,
        [string[]]$Lines
    )

    $logDir = Split-Path -Parent $OperatorLogPath
    if (-not [string]::IsNullOrWhiteSpace($logDir) -and -not (Test-Path $logDir)) {
        New-Item -ItemType Directory -Path $logDir -Force | Out-Null
    }

    if (-not (Test-Path $OperatorLogPath)) {
        Set-Content -Path $OperatorLogPath -Value "# Default Strategy Operator Evidence Log`n"
    }

    Add-Content -Path $OperatorLogPath -Value "`n## $Title"
    foreach ($line in $Lines) {
        Add-Content -Path $OperatorLogPath -Value $line
    }
}

function Wait-BackendHealthy {
    param([string]$Project)

    $python = @"
import urllib.request
urllib.request.urlopen('http://localhost:8000/api/v1/health', timeout=5).read()
print('ok')
"@

    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        try {
            Invoke-Compose -Project $Project -Args @("exec", "-T", "backend", "python", "-c", $python) | Out-Null
            return
        }
        catch {
            Start-Sleep -Seconds 2
        }
    }

    throw "Backend did not become healthy for project '$Project'."
}

function Get-BackendJson {
    param(
        [string]$Project,
        [string]$Path
    )

    $python = @"
import json
import urllib.request
data = json.load(urllib.request.urlopen('http://localhost:8000$Path', timeout=10))
print(json.dumps(data))
"@

    $raw = Invoke-Compose -Project $Project -Args @("exec", "-T", "backend", "python", "-c", $python)
    return ($raw -join "`n" | ConvertFrom-Json)
}

function Get-WorkerMetricsText {
    param([string]$Project)

    $python = @"
import urllib.request
print(urllib.request.urlopen('http://localhost:$WorkerMetricsPort/metrics', timeout=10).read().decode(), end='')
"@

    return (Invoke-Compose -Project $Project -Args @("exec", "-T", "worker", "python", "-c", $python)) -join "`n"
}

function Get-MetricValue {
    param(
        [string]$MetricsText,
        [string]$MetricName
    )

    $pattern = "(?m)^" + [regex]::Escape($MetricName) + "\s+([0-9eE\.\+\-]+)$"
    $match = [regex]::Match($MetricsText, $pattern)
    if (-not $match.Success) {
        throw "Metric '$MetricName' not found."
    }

    return [double]$match.Groups[1].Value
}

function Assert-MigrationState {
    param([string]$Project)

    $current = (Invoke-Compose -Project $Project -Args @("exec", "-T", "backend", "alembic", "current")) -join "`n"
    if ($current -notmatch "\b$MigrationRevision\b") {
        throw "Expected alembic current to contain revision $MigrationRevision, got: $current"
    }

    $dbRevision = (Invoke-Compose -Project $Project -Args @("exec", "-T", "db", "psql", "-U", "smt", "-d", "smt", "-tAc", "select version_num from alembic_version;")) -join ""
    $dbRevision = $dbRevision.Trim()
    if ($dbRevision -ne $MigrationRevision) {
        throw "Expected alembic_version to be $MigrationRevision, got: $dbRevision"
    }
}

if ([string]::IsNullOrWhiteSpace($CommitSha)) {
    $CommitSha = (& git rev-parse HEAD).Trim()
}

if ($CreateTag) {
    & git diff --quiet --exit-code
    if ($LASTEXITCODE -ne 0) {
        throw "Worktree must be clean before creating tag $ReleaseTag."
    }

    & git diff --cached --quiet --exit-code
    if ($LASTEXITCODE -ne 0) {
        throw "Index must be clean before creating tag $ReleaseTag."
    }

    $existingTag = (& git tag --list $ReleaseTag).Trim()
    if ([string]::IsNullOrWhiteSpace($existingTag)) {
        & git tag -a $ReleaseTag -m "Release $ReleaseTag"
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to create tag $ReleaseTag."
        }
    }
}

Add-OperatorLogBlock -Title "Evidence Boundary" -Lines @(
    "- Recorded at: $(Get-Date -Format o)",
    "- Boundary id: `"$EvidenceBoundaryId`"",
    "- Release tag: `"$ReleaseTag`"",
    "- Commit SHA: `"$CommitSha`"",
    "- Alembic revision: `"$MigrationRevision`"",
    "- Contract version: `"$ContractVersion`"",
    "- Note: Only post-fix runs count as evidence. Pre-fix artifacts are historical/debug only."
)

Invoke-Compose -Project $CloneProject -Args @("up", "--build", "-d", "db", "backend", "worker") | Out-Null
Wait-BackendHealthy -Project $CloneProject
Assert-MigrationState -Project $CloneProject

$cloneRun = Get-BackendJson -Project $CloneProject -Path "/api/v1/paper-trading/default-strategy/run"
if ($cloneRun.state -ne "no_active_run") {
    throw "Clone should start with no active run."
}

$cloneHealth = Get-BackendJson -Project $CloneProject -Path "/api/v1/paper-trading/strategy-health"
if (-not $cloneHealth.bootstrap_required) {
    throw "Clone strategy-health should require bootstrap before smoke."
}

Start-Sleep -Seconds $WorkerMetricWaitSeconds
$workerMetrics = Get-WorkerMetricsText -Project $CloneProject
$noActiveRunMetric = Get-MetricValue -MetricsText $workerMetrics -MetricName "smt_default_strategy_scheduler_no_active_run_total"
if ($noActiveRunMetric -lt 1) {
    throw "Expected worker no-active-run metric to increment in clone."
}

Invoke-Compose -Project $CloneProject -Args @(
    "exec", "-T", "backend", "pytest",
    "tests/test_default_strategy_measurement.py::test_default_strategy_read_endpoints_do_not_create_rows_without_active_run",
    "tests/test_default_strategy_measurement.py::test_default_strategy_run_requires_explicit_bootstrap",
    "tests/test_default_strategy_measurement.py::test_strategy_health_funnel_reconciles_qualified_opened_skipped_and_pending",
    "tests/test_default_strategy_measurement.py::test_strategy_health_flags_missing_execution_decision_as_integrity_error",
    "tests/test_default_strategy_measurement.py::test_strategy_health_uses_persisted_drawdown_state_for_headline",
    "tests/test_default_strategy_measurement.py::test_strategy_health_never_reports_local_total_exposure_for_shared_global_block",
    "tests/test_reports.py::test_review_generator_surfaces_shared_global_reasons_and_persisted_drawdown",
    "tests/test_trading_intelligence_api.py::test_scheduler_does_not_bootstrap_run_or_stamp_metadata_without_active_run",
    "tests/test_trading_intelligence_api.py::test_scheduler_no_active_run_metric_increments_even_when_no_signals_are_available",
    "-q"
) | Out-Null

$liveRun = Get-BackendJson -Project $LiveProject -Path "/api/v1/paper-trading/default-strategy/run"
if ($liveRun.state -eq "active_run") {
    $retired = Invoke-ComposeJson -Project $LiveProject -Args @(
        "exec", "-T", "backend", "python", "-m", "app.ops.default_strategy_evidence", "retire-active-run"
    )
    Add-OperatorLogBlock -Title "Retired Run" -Lines @(
        "- Recorded at: $(Get-Date -Format o)",
        "- Retired run id: `"$($retired.retired_run.id)`"",
        "- External labels: `"pre_fix_invalid_for_evidence`", `"retired_after_truth_boundary_remediation`""
    )
}

$liveRunAfter = Get-BackendJson -Project $LiveProject -Path "/api/v1/paper-trading/default-strategy/run"
if ($liveRunAfter.state -ne "no_active_run") {
    throw "Live target must show no_active_run before valid bootstrap."
}

$liveHealthAfter = Get-BackendJson -Project $LiveProject -Path "/api/v1/paper-trading/strategy-health"
if (-not $liveHealthAfter.bootstrap_required) {
    throw "Live strategy-health must remain non-mutating before bootstrap."
}

if ($BootstrapLiveRun) {
    if ([string]::IsNullOrWhiteSpace($CutoverTimestamp)) {
        throw "CutoverTimestamp is required when -BootstrapLiveRun is used."
    }

    $bootstrapped = Invoke-ComposeJson -Project $LiveProject -Args @(
        "exec", "-T", "backend", "python", "-m", "app.ops.default_strategy_evidence",
        "bootstrap-run",
        "--launch-boundary-at", $CutoverTimestamp,
        "--evidence-boundary-id", $EvidenceBoundaryId,
        "--release-tag", $ReleaseTag,
        "--commit-sha", $CommitSha,
        "--migration-revision", $MigrationRevision,
        "--contract-version", $ContractVersion,
        "--use-balanced-gate"
    )

    Add-OperatorLogBlock -Title "Bootstrapped Evidence Run" -Lines @(
        "- Recorded at: $(Get-Date -Format o)",
        "- Run id: `"$($bootstrapped.strategy_run.id)`"",
        "- Launch boundary: `"$CutoverTimestamp`"",
        "- Release tag: `"$ReleaseTag`"",
        "- Commit SHA: `"$CommitSha`"",
        "- Alembic revision: `"$MigrationRevision`""
    )
}

if (-not $SkipCloneTeardown) {
    Invoke-Compose -Project $CloneProject -Args @("down", "-v") | Out-Null
}
