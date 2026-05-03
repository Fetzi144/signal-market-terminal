"""Walk-forward alpha gauntlet over resolved signal evidence.

This report is deliberately read-only and paper/research-only. It asks a narrow
question: do any simple, auditable filters over existing resolved signals look
positive in train, validation, and the final chronological holdout?
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.market import Market
from app.models.signal import Signal
from app.reports.strategy_review import _repo_root

ALPHA_GAUNTLET_SCHEMA_VERSION = "alpha_gauntlet_v1"
ALPHA_GAUNTLET_ARTIFACT_DIR = "docs/research-lab/alpha-gauntlet"
DEFAULT_RANK_THRESHOLDS = (None, 0.5, 0.6, 0.7, 0.8, 0.9)
DEFAULT_EV_THRESHOLDS = (None, 0.0, 0.01, 0.02, 0.05)
DEFAULT_MIN_PRICE_THRESHOLDS = (None, 0.05, 0.1)
DEFAULT_MAX_PRICE_THRESHOLDS = (None, 0.8, 0.9)


@dataclass(frozen=True)
class AlphaSignalRow:
    signal_id: str
    fired_at: datetime
    signal_type: str
    platform: str
    profit_loss: float
    clv: float
    resolved_correctly: bool
    direction: str = "unknown"
    timeframe: str = "unknown"
    rank_score: float | None = None
    expected_value: float | None = None
    estimated_probability: float | None = None
    price_at_fire: float | None = None


@dataclass(frozen=True)
class AlphaRule:
    signal_type: str = "all"
    platform: str = "all"
    direction: str = "all"
    timeframe: str = "all"
    price_bucket: str = "all"
    expected_value_bucket: str = "all"
    min_rank_score: float | None = None
    min_expected_value: float | None = None
    min_price_at_fire: float | None = None
    max_price_at_fire: float | None = None

    @property
    def label(self) -> str:
        parts = [f"type={self.signal_type}", f"platform={self.platform}"]
        if self.direction != "all":
            parts.append(f"direction={self.direction}")
        if self.timeframe != "all":
            parts.append(f"timeframe={self.timeframe}")
        if self.price_bucket != "all":
            parts.append(f"price_bucket={self.price_bucket}")
        if self.expected_value_bucket != "all":
            parts.append(f"ev_bucket={self.expected_value_bucket}")
        if self.min_rank_score is not None:
            parts.append(f"rank>={self.min_rank_score:g}")
        if self.min_expected_value is not None:
            parts.append(f"ev>={self.min_expected_value:g}")
        if self.min_price_at_fire is not None:
            parts.append(f"price>={self.min_price_at_fire:g}")
        if self.max_price_at_fire is not None:
            parts.append(f"price<={self.max_price_at_fire:g}")
        return " ".join(parts)


@dataclass(frozen=True)
class AlphaMetrics:
    sample_count: int = 0
    win_rate: float | None = None
    total_profit_loss: float = 0.0
    avg_profit_loss: float | None = None
    avg_clv: float | None = None
    brier_score: float | None = None
    max_drawdown: float = 0.0
    start_at: str | None = None
    end_at: str | None = None


@dataclass(frozen=True)
class AlphaCandidate:
    rule: AlphaRule
    train: AlphaMetrics
    validation: AlphaMetrics
    test: AlphaMetrics
    train_pass: bool
    validation_pass: bool
    test_pass: bool
    verdict: str
    rank_key: tuple[Any, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule": asdict(self.rule) | {"label": self.rule.label},
            "train": asdict(self.train),
            "validation": asdict(self.validation),
            "test": asdict(self.test),
            "train_pass": self.train_pass,
            "validation_pass": self.validation_pass,
            "test_pass": self.test_pass,
            "verdict": self.verdict,
            "rank_key": list(self.rank_key),
        }


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def _truthy_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value in (1, "1", "true", "True", "yes", "YES"):
        return True
    return False


def _safe_dimension(value: Any) -> str:
    text = str(value or "unknown").strip().lower()
    return text or "unknown"


def _iso(value: datetime | None) -> str | None:
    return _ensure_utc(value).isoformat() if value is not None else None


def _price_bucket(value: float | None) -> str:
    if value is None:
        return "price_unknown"
    if value < 0.05:
        return "p00_005"
    if value < 0.10:
        return "p005_010"
    if value < 0.20:
        return "p010_020"
    if value < 0.50:
        return "p020_050"
    if value < 0.80:
        return "p050_080"
    if value < 0.90:
        return "p080_090"
    return "p090_100"


def _expected_value_bucket(value: float | None) -> str:
    if value is None:
        return "ev_unknown"
    if value < 0:
        return "ev_neg"
    if value < 0.01:
        return "ev_000_001"
    if value < 0.02:
        return "ev_001_002"
    if value < 0.05:
        return "ev_002_005"
    return "ev_005_plus"


def _rule_matches(rule: AlphaRule, row: AlphaSignalRow) -> bool:
    if rule.signal_type != "all" and row.signal_type != rule.signal_type:
        return False
    if rule.platform != "all" and row.platform != rule.platform:
        return False
    if rule.direction != "all" and row.direction != rule.direction:
        return False
    if rule.timeframe != "all" and row.timeframe != rule.timeframe:
        return False
    if rule.price_bucket != "all" and _price_bucket(row.price_at_fire) != rule.price_bucket:
        return False
    if (
        rule.expected_value_bucket != "all"
        and _expected_value_bucket(row.expected_value) != rule.expected_value_bucket
    ):
        return False
    if rule.min_rank_score is not None and (row.rank_score is None or row.rank_score < rule.min_rank_score):
        return False
    if rule.min_expected_value is not None and (
        row.expected_value is None or row.expected_value < rule.min_expected_value
    ):
        return False
    if rule.min_price_at_fire is not None and (
        row.price_at_fire is None or row.price_at_fire < rule.min_price_at_fire
    ):
        return False
    if rule.max_price_at_fire is not None and (
        row.price_at_fire is None or row.price_at_fire > rule.max_price_at_fire
    ):
        return False
    return True


def _metrics(rows: list[AlphaSignalRow]) -> AlphaMetrics:
    if not rows:
        return AlphaMetrics()

    profit_values = [row.profit_loss for row in rows]
    clv_values = [row.clv for row in rows]
    total_profit = sum(profit_values)
    wins = sum(1 for row in rows if row.profit_loss > 0)
    predicted = [
        (row.estimated_probability, 1.0 if row.resolved_correctly else 0.0)
        for row in rows
        if row.estimated_probability is not None
    ]
    brier = None
    if predicted:
        brier = sum((probability - outcome) ** 2 for probability, outcome in predicted) / len(predicted)

    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for profit in profit_values:
        cumulative += profit
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)

    return AlphaMetrics(
        sample_count=len(rows),
        win_rate=_round(wins / len(rows)),
        total_profit_loss=_round(total_profit) or 0.0,
        avg_profit_loss=_round(total_profit / len(rows)),
        avg_clv=_round(sum(clv_values) / len(rows)),
        brier_score=_round(brier),
        max_drawdown=_round(max_drawdown) or 0.0,
        start_at=_iso(rows[0].fired_at),
        end_at=_iso(rows[-1].fired_at),
    )


def _split_rows(rows: list[AlphaSignalRow]) -> dict[str, list[AlphaSignalRow]]:
    sorted_rows = sorted(rows, key=lambda row: row.fired_at)
    count = len(sorted_rows)
    if count < 3:
        return {"train": sorted_rows, "validation": [], "test": []}
    train_end = max(1, int(count * 0.5))
    validation_end = max(train_end + 1, int(count * 0.75))
    validation_end = min(validation_end, count - 1)
    return {
        "train": sorted_rows[:train_end],
        "validation": sorted_rows[train_end:validation_end],
        "test": sorted_rows[validation_end:],
    }


def _passes(metrics: AlphaMetrics, *, min_sample: int) -> bool:
    return (
        metrics.sample_count >= min_sample
        and metrics.total_profit_loss > 0
        and metrics.avg_clv is not None
        and metrics.avg_clv > 0
    )


def _generate_train_cohort_rules(
    rows: list[AlphaSignalRow],
    *,
    min_train_sample: int,
) -> list[AlphaRule]:
    templates = (
        ("signal_type", "platform", "direction", "timeframe", "price_bucket", "expected_value_bucket"),
        ("signal_type", "platform", "direction", "price_bucket", "expected_value_bucket"),
        ("signal_type", "platform", "direction", "timeframe", "price_bucket"),
        ("signal_type", "platform", "direction", "price_bucket"),
        ("signal_type", "platform", "direction", "timeframe", "expected_value_bucket"),
        ("signal_type", "platform", "direction", "expected_value_bucket"),
    )
    feature_rows: list[dict[str, Any]] = []
    for row in rows:
        feature_rows.append(
            {
                "signal_type": row.signal_type,
                "platform": row.platform,
                "direction": row.direction,
                "timeframe": row.timeframe,
                "price_bucket": _price_bucket(row.price_at_fire),
                "expected_value_bucket": _expected_value_bucket(row.expected_value),
                "profit_loss": row.profit_loss,
                "clv": row.clv,
            }
        )

    rules: list[AlphaRule] = []
    seen: set[AlphaRule] = set()
    for template in templates:
        buckets: dict[tuple[str, ...], dict[str, float]] = {}
        for row in feature_rows:
            key = tuple(str(row[dimension]) for dimension in template)
            stats = buckets.setdefault(key, {"count": 0, "profit_loss": 0.0, "clv": 0.0})
            stats["count"] += 1
            stats["profit_loss"] += float(row["profit_loss"])
            stats["clv"] += float(row["clv"])
        for key, stats in buckets.items():
            if stats["count"] < min_train_sample:
                continue
            if stats["profit_loss"] <= 0 or stats["clv"] / stats["count"] <= 0:
                continue
            values = dict(zip(template, key, strict=True))
            rule = AlphaRule(
                signal_type=values.get("signal_type", "all"),
                platform=values.get("platform", "all"),
                direction=values.get("direction", "all"),
                timeframe=values.get("timeframe", "all"),
                price_bucket=values.get("price_bucket", "all"),
                expected_value_bucket=values.get("expected_value_bucket", "all"),
            )
            if rule not in seen:
                seen.add(rule)
                rules.append(rule)
    return rules


def generate_alpha_rules(
    rows: list[AlphaSignalRow],
    *,
    training_rows: list[AlphaSignalRow] | None = None,
    min_train_sample: int = 20,
) -> list[AlphaRule]:
    signal_types = ["all"] + sorted({row.signal_type for row in rows if row.signal_type != "unknown"})
    platforms = ["all"] + sorted({row.platform for row in rows if row.platform != "unknown"})
    rules: list[AlphaRule] = []
    seen: set[AlphaRule] = set()
    for signal_type in signal_types:
        for platform in platforms:
            for min_rank in DEFAULT_RANK_THRESHOLDS:
                for min_ev in DEFAULT_EV_THRESHOLDS:
                    for min_price in DEFAULT_MIN_PRICE_THRESHOLDS:
                        for max_price in DEFAULT_MAX_PRICE_THRESHOLDS:
                            if min_price is not None and max_price is not None and min_price >= max_price:
                                continue
                            rule = AlphaRule(
                                signal_type=signal_type,
                                platform=platform,
                                min_rank_score=min_rank,
                                min_expected_value=min_ev,
                                min_price_at_fire=min_price,
                                max_price_at_fire=max_price,
                            )
                            if rule not in seen:
                                seen.add(rule)
                                rules.append(rule)
    for rule in _generate_train_cohort_rules(
        training_rows if training_rows is not None else rows,
        min_train_sample=min_train_sample,
    ):
        if rule not in seen:
            seen.add(rule)
            rules.append(rule)
    return rules


def evaluate_alpha_gauntlet_rows(
    rows: list[AlphaSignalRow],
    *,
    min_train_sample: int = 20,
    min_validation_sample: int = 10,
    min_test_sample: int = 10,
    top_n: int = 20,
) -> dict[str, Any]:
    sorted_rows = sorted(rows, key=lambda row: row.fired_at)
    splits = _split_rows(sorted_rows)
    baseline = {
        "train": asdict(_metrics(splits["train"])),
        "validation": asdict(_metrics(splits["validation"])),
        "test": asdict(_metrics(splits["test"])),
        "all": asdict(_metrics(sorted_rows)),
    }
    split_counts = {name: len(split_rows) for name, split_rows in splits.items()}
    minimum_possible = (
        split_counts["train"] >= min_train_sample
        and split_counts["validation"] >= min_validation_sample
        and split_counts["test"] >= min_test_sample
    )
    if not minimum_possible:
        return {
            "verdict": "insufficient_data",
            "reason": "Not enough resolved signals in chronological train/validation/test slices.",
            "baseline": baseline,
            "split_counts": split_counts,
            "rule_count": 0,
            "selected_candidates": [],
            "surviving_candidates": [],
            "rejected_candidates": [],
            "warnings": ["sample_below_minimum"],
        }

    candidates: list[AlphaCandidate] = []
    for rule in generate_alpha_rules(
        sorted_rows,
        training_rows=splits["train"],
        min_train_sample=min_train_sample,
    ):
        train_rows = [row for row in splits["train"] if _rule_matches(rule, row)]
        validation_rows = [row for row in splits["validation"] if _rule_matches(rule, row)]
        test_rows = [row for row in splits["test"] if _rule_matches(rule, row)]
        train_metrics = _metrics(train_rows)
        validation_metrics = _metrics(validation_rows)
        test_metrics = _metrics(test_rows)
        train_pass = _passes(train_metrics, min_sample=min_train_sample)
        validation_pass = _passes(validation_metrics, min_sample=min_validation_sample)
        test_pass = _passes(test_metrics, min_sample=min_test_sample)
        if train_pass and validation_pass and test_pass:
            verdict = "paper_alpha_candidate"
        elif train_pass and validation_pass:
            verdict = "failed_out_of_sample"
        elif train_pass:
            verdict = "overfit_warning"
        else:
            verdict = "rejected"
        rank_key = (
            1 if validation_pass else 0,
            validation_metrics.total_profit_loss,
            validation_metrics.avg_profit_loss or -999.0,
            validation_metrics.avg_clv or -999.0,
            validation_metrics.sample_count,
            -validation_metrics.max_drawdown,
        )
        candidates.append(
            AlphaCandidate(
                rule=rule,
                train=train_metrics,
                validation=validation_metrics,
                test=test_metrics,
                train_pass=train_pass,
                validation_pass=validation_pass,
                test_pass=test_pass,
                verdict=verdict,
                rank_key=rank_key,
            )
        )

    validation_ranked = sorted(candidates, key=lambda candidate: candidate.rank_key, reverse=True)
    selected = [candidate for candidate in validation_ranked if candidate.train_pass and candidate.validation_pass]
    surviving = [candidate for candidate in selected if candidate.test_pass]
    overfit = [candidate for candidate in validation_ranked if candidate.train_pass and not candidate.validation_pass]

    if surviving:
        verdict = "paper_alpha_candidate"
        reason = "At least one train/validation-selected rule remained positive in the chronological holdout."
    elif selected:
        verdict = "failed_out_of_sample"
        reason = "Some rules passed train and validation, but none survived the final chronological holdout."
    elif overfit:
        verdict = "overfit_warning"
        reason = "At least one rule fit the train slice, but validation rejected it."
    else:
        verdict = "no_edge_found"
        reason = "No simple rule passed the train and validation gates."

    return {
        "verdict": verdict,
        "reason": reason,
        "baseline": baseline,
        "split_counts": split_counts,
        "rule_count": len(candidates),
        "selected_candidates": [candidate.to_dict() for candidate in selected[:top_n]],
        "surviving_candidates": [candidate.to_dict() for candidate in surviving[:top_n]],
        "rejected_candidates": [
            candidate.to_dict()
            for candidate in (selected[top_n:] + overfit + validation_ranked[:top_n])
            if candidate.verdict != "paper_alpha_candidate"
        ][:top_n],
        "warnings": [
            "paper_research_only",
            "signal_level_profit_loss_not_live_execution_adjusted",
            "multiple_testing_risk_use_survivors_as_candidates_not_proof",
        ],
    }


async def load_alpha_signal_rows(
    session: AsyncSession,
    *,
    window_days: int,
    max_signals: int,
    as_of: datetime | None = None,
) -> list[AlphaSignalRow]:
    as_of = as_of or _utcnow()
    window_start = as_of - timedelta(days=window_days)
    query = (
        select(
            Signal.id,
            Signal.fired_at,
            Signal.signal_type,
            Market.platform,
            Signal.profit_loss,
            Signal.clv,
            Signal.resolved_correctly,
            Signal.rank_score,
            Signal.expected_value,
            Signal.estimated_probability,
            Signal.price_at_fire,
            Signal.details,
        )
        .select_from(Signal)
        .outerjoin(Market, Market.id == Signal.market_id)
        .where(
            Signal.resolved_correctly.is_not(None),
            Signal.fired_at.is_not(None),
            Signal.profit_loss.is_not(None),
            Signal.clv.is_not(None),
            Signal.fired_at >= window_start,
            Signal.fired_at <= as_of,
        )
        .order_by(Signal.fired_at.desc())
        .limit(max_signals)
    )
    result = await session.execute(query)
    rows: list[AlphaSignalRow] = []
    for (
        signal_id,
        fired_at,
        signal_type,
        platform,
        profit_loss,
        clv,
        resolved_correctly,
        rank_score,
        expected_value,
        estimated_probability,
        price_at_fire,
        details,
    ) in result.all():
        if fired_at is None or profit_loss is None or clv is None:
            continue
        details = details or {}
        rows.append(
            AlphaSignalRow(
                signal_id=str(signal_id),
                fired_at=_ensure_utc(fired_at),
                signal_type=str(signal_type or "unknown"),
                platform=str(platform or "unknown"),
                profit_loss=float(profit_loss),
                clv=float(clv),
                resolved_correctly=_truthy_bool(resolved_correctly),
                direction=_safe_dimension(details.get("direction")),
                timeframe=_safe_dimension(details.get("timeframe")),
                rank_score=_float(rank_score),
                expected_value=_float(expected_value),
                estimated_probability=_float(estimated_probability),
                price_at_fire=_float(price_at_fire),
            )
        )
    rows.sort(key=lambda row: row.fired_at)
    return rows


def _artifact_stem(
    *,
    as_of: datetime,
    window_days: int,
    min_train_sample: int,
    min_validation_sample: int,
    min_test_sample: int,
) -> str:
    return (
        f"{as_of.date().isoformat()}-alpha-gauntlet-{window_days}d"
        f"-train{min_train_sample}-validation{min_validation_sample}-test{min_test_sample}"
    )


def _render_markdown(payload: dict[str, Any]) -> str:
    result = payload.get("result") or {}
    baseline = result.get("baseline") or {}
    all_metrics = baseline.get("all") or {}
    selected = result.get("selected_candidates") or []
    survivors = result.get("surviving_candidates") or []

    def _candidate_line(candidate: dict[str, Any]) -> str:
        rule = candidate.get("rule") or {}
        test = candidate.get("test") or {}
        validation = candidate.get("validation") or {}
        return (
            f"| `{rule.get('label', '-')}` | `{candidate.get('verdict')}` | "
            f"{validation.get('sample_count', 0)} | {validation.get('total_profit_loss', 0)} | "
            f"{validation.get('avg_clv')} | {test.get('sample_count', 0)} | "
            f"{test.get('total_profit_loss', 0)} | {test.get('avg_clv')} |"
        )

    selected_lines = "\n".join(_candidate_line(candidate) for candidate in selected[:10])
    if not selected_lines:
        selected_lines = "| - | - | - | - | - | - | - | - |"
    survivor_lines = "\n".join(_candidate_line(candidate) for candidate in survivors[:10])
    if not survivor_lines:
        survivor_lines = "| - | - | - | - | - | - | - | - |"

    return f"""# Alpha Gauntlet

**Generated:** {payload.get('generated_at')}
**Window days:** {payload.get('window_days')}
**Resolved rows tested:** {payload.get('row_count')}
**Minimum samples:** train {payload.get('selection_policy', {}).get('min_train_sample')}, validation {payload.get('selection_policy', {}).get('min_validation_sample')}, test {payload.get('selection_policy', {}).get('min_test_sample')}
**Verdict:** `{result.get('verdict')}`
**Reason:** {result.get('reason')}
**Paper only:** `true`
**Live submission permitted:** `false`

## Baseline

- Sample: {all_metrics.get('sample_count', 0)}
- Total signal profit/loss: {all_metrics.get('total_profit_loss', 0)}
- Average signal profit/loss: {all_metrics.get('avg_profit_loss')}
- Average CLV: {all_metrics.get('avg_clv')}
- Max drawdown: {all_metrics.get('max_drawdown')}

## Surviving Candidates

| Rule | Verdict | Validation N | Validation P/L | Validation CLV | Test N | Test P/L | Test CLV |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
{survivor_lines}

## Validation-Selected Candidates

| Rule | Verdict | Validation N | Validation P/L | Validation CLV | Test N | Test P/L | Test CLV |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
{selected_lines}

## Warnings

""" + "\n".join(f"- `{warning}`" for warning in result.get("warnings", []))


async def generate_alpha_gauntlet_artifact(
    session: AsyncSession,
    *,
    window_days: int = 365,
    max_signals: int = 50_000,
    min_train_sample: int = 20,
    min_validation_sample: int = 10,
    min_test_sample: int = 10,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    as_of = as_of or _utcnow()
    rows = await load_alpha_signal_rows(
        session,
        window_days=window_days,
        max_signals=max_signals,
        as_of=as_of,
    )
    result = evaluate_alpha_gauntlet_rows(
        rows,
        min_train_sample=min_train_sample,
        min_validation_sample=min_validation_sample,
        min_test_sample=min_test_sample,
    )
    payload = {
        "schema_version": ALPHA_GAUNTLET_SCHEMA_VERSION,
        "generated_at": as_of.isoformat(),
        "window_days": window_days,
        "max_signals": max_signals,
        "row_count": len(rows),
        "paper_only": True,
        "live_submission_permitted": False,
        "selection_policy": {
            "split": "chronological_50_25_25",
            "min_train_sample": min_train_sample,
            "min_validation_sample": min_validation_sample,
            "min_test_sample": min_test_sample,
            "entry_features_only": [
                "signal_type",
                "platform",
                "rank_score",
                "expected_value",
                "price_at_fire",
                "direction",
                "timeframe",
                "price_bucket",
                "expected_value_bucket",
            ],
            "holdout_note": "Rules are ranked by train/validation evidence before reading the final test verdict.",
        },
        "result": result,
    }
    output_dir = _repo_root() / ALPHA_GAUNTLET_ARTIFACT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = _artifact_stem(
        as_of=as_of,
        window_days=window_days,
        min_train_sample=min_train_sample,
        min_validation_sample=min_validation_sample,
        min_test_sample=min_test_sample,
    )
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload), encoding="utf-8")
    return {
        "alpha_gauntlet_json_path": str(json_path),
        "alpha_gauntlet_markdown_path": str(markdown_path),
        "snapshot": payload,
    }
