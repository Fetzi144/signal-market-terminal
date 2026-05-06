"""Kalshi-only alpha factory for systematic paper-strategy discovery.

The alpha factory is the layer above the walk-forward gauntlet. It turns
surviving historical rules into frozen paper-lane blueprints with explicit
trade expressions, blockers, and next actions. It does not submit orders and
does not tune thresholds from forward results.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Sequence

from sqlalchemy.ext.asyncio import AsyncSession

from app.alpha_rule_specs import (
    ALPHA_KALSHI_4237F81367_FAMILY,
    ALPHA_KALSHI_4237F81367_VERSION,
    ALPHA_KALSHI_D80BDF77A9_FAMILY,
    ALPHA_KALSHI_D80BDF77A9_VERSION,
)
from app.reports.alpha_gauntlet import (
    AlphaSignalRow,
    evaluate_alpha_gauntlet_rows,
    load_alpha_signal_rows,
)
from app.reports.strategy_review import _repo_root

ALPHA_FACTORY_SCHEMA_VERSION = "alpha_factory_v2"
ALPHA_FACTORY_ARTIFACT_DIR = "docs/research-lab/alpha-factory"
DEFAULT_PLATFORM = "kalshi"
DEFAULT_MAX_CANDIDATES = 10
POSITIVE_EV_BUCKETS = {"ev_000_001", "ev_001_002", "ev_002_005", "ev_005_plus"}
PRICE_BUCKET_RANGES = {
    "p00_005": {"min": 0.0, "max": 0.05},
    "p005_010": {"min": 0.05, "max": 0.10},
    "p010_020": {"min": 0.10, "max": 0.20},
    "p020_050": {"min": 0.20, "max": 0.50},
    "p050_080": {"min": 0.50, "max": 0.80},
    "p080_090": {"min": 0.80, "max": 0.90},
    "p090_100": {"min": 0.90, "max": 1.0},
}
EXPECTED_VALUE_BUCKET_RANGES = {
    "ev_neg": {"min": None, "max": 0.0},
    "ev_000_001": {"min": 0.0, "max": 0.01},
    "ev_001_002": {"min": 0.01, "max": 0.02},
    "ev_002_005": {"min": 0.02, "max": 0.05},
    "ev_005_plus": {"min": 0.05, "max": None},
}
TENOR_BUCKET_RANGES_HOURS = {
    "tenor_expired": {"min": None, "max": 0.0},
    "tenor_0_1d": {"min": 0.0, "max": 24.0},
    "tenor_1_3d": {"min": 24.0, "max": 72.0},
    "tenor_3_7d": {"min": 72.0, "max": 168.0},
    "tenor_7_30d": {"min": 168.0, "max": 720.0},
    "tenor_30d_plus": {"min": 720.0, "max": None},
}
MONEY_BUCKET_RANGES = {
    "000_001k": {"min": 0.0, "max": 1_000.0},
    "001k_010k": {"min": 1_000.0, "max": 10_000.0},
    "010k_100k": {"min": 10_000.0, "max": 100_000.0},
    "100k_plus": {"min": 100_000.0, "max": None},
}
CHEAP_YES_FOLLOW_QUARANTINE = {
    "enabled": True,
    "reason_code": "kalshi_cheap_yes_follow_forward_paper_quarantine",
    "detail": (
        "Initial forward paper evidence for kalshi_cheap_yes_follow resolved negative; "
        "related tiny-positive-YES candidates require manual review instead of a new lane."
    ),
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        normalized = _ensure_utc(value)
        return normalized.isoformat() if normalized else None
    if isinstance(value, Decimal):
        return float(value)
    return value


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _float_metric(candidate: dict[str, Any], split: str, metric: str, default: float = 0.0) -> float:
    value = ((candidate.get(split) or {}).get(metric))
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int_metric(candidate: dict[str, Any], split: str, metric: str, default: int = 0) -> int:
    value = ((candidate.get(split) or {}).get(metric))
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_slug(value: str, *, max_length: int = 80) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    return (slug or "candidate")[:max_length].strip("_") or "candidate"


def _rule_digest(rule: dict[str, Any]) -> str:
    encoded = json.dumps(rule, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()[:10]


def _rule_label(rule: dict[str, Any]) -> str:
    return str(rule.get("label") or " ".join(f"{key}={value}" for key, value in sorted(rule.items())))


def _bucket_range(bucket: Any, *, kind: str) -> dict[str, Any] | None:
    bucket_text = str(bucket or "all")
    if bucket_text in {"all", "unknown", "price_unknown", "ev_unknown", "tenor_unknown"}:
        return None
    if kind == "price":
        bounds = PRICE_BUCKET_RANGES.get(bucket_text)
        unit = "yes_price"
    elif kind == "expected_value":
        bounds = EXPECTED_VALUE_BUCKET_RANGES.get(bucket_text)
        unit = "yes_expected_value"
    elif kind == "tenor_hours":
        bounds = TENOR_BUCKET_RANGES_HOURS.get(bucket_text)
        unit = "hours_to_market_end"
    elif kind in {"volume", "liquidity"}:
        prefix = f"{kind}_"
        suffix = bucket_text.removeprefix(prefix)
        bounds = MONEY_BUCKET_RANGES.get(suffix)
        unit = "usd"
    else:
        bounds = None
        unit = kind
    if bounds is None:
        return None
    return {
        "bucket": bucket_text,
        "min_inclusive": bounds.get("min"),
        "max_exclusive": bounds.get("max"),
        "unit": unit,
    }


def _frozen_thresholds(rule: dict[str, Any]) -> dict[str, Any]:
    thresholds: dict[str, Any] = {
        "bucket_semantics": "lower_bound_inclusive_upper_bound_exclusive",
    }
    price_range = _bucket_range(rule.get("price_bucket"), kind="price")
    if price_range is not None:
        thresholds["price_at_fire"] = price_range
    ev_range = _bucket_range(rule.get("expected_value_bucket"), kind="expected_value")
    if ev_range is not None:
        thresholds["expected_value"] = ev_range
    tenor_range = _bucket_range(rule.get("market_tenor_bucket"), kind="tenor_hours")
    if tenor_range is not None:
        thresholds["market_tenor"] = tenor_range
    volume_range = _bucket_range(rule.get("volume_bucket"), kind="volume")
    if volume_range is not None:
        thresholds["last_volume_24h"] = volume_range
    liquidity_range = _bucket_range(rule.get("liquidity_bucket"), kind="liquidity")
    if liquidity_range is not None:
        thresholds["last_liquidity"] = liquidity_range

    explicit_thresholds = {
        key: rule.get(key)
        for key in ("min_rank_score", "min_expected_value", "min_price_at_fire", "max_price_at_fire")
        if rule.get(key) is not None
    }
    if explicit_thresholds:
        thresholds["explicit_thresholds"] = explicit_thresholds
    return thresholds


def _frozen_dimensions(rule: dict[str, Any]) -> dict[str, Any]:
    dimensions: dict[str, Any] = {}
    for key in (
        "signal_type",
        "platform",
        "direction",
        "timeframe",
        "market_category",
        "market_tenor_bucket",
        "volume_bucket",
        "liquidity_bucket",
        "feature_family",
    ):
        value = rule.get(key)
        if value not in (None, "", "all", "unknown"):
            dimensions[key] = value
    return dimensions


def _specificity(rule: dict[str, Any]) -> int:
    score = 0
    for key in (
        "signal_type",
        "platform",
        "direction",
        "timeframe",
        "price_bucket",
        "expected_value_bucket",
        "market_category",
        "market_tenor_bucket",
        "volume_bucket",
        "liquidity_bucket",
    ):
        value = rule.get(key)
        if value not in (None, "", "all", "unknown"):
            score += 1
    for key in ("min_rank_score", "min_expected_value", "min_price_at_fire", "max_price_at_fire"):
        if rule.get(key) is not None:
            score += 1
    return score


def _strategy_expression(rule: dict[str, Any]) -> dict[str, Any]:
    ev_bucket = str(rule.get("expected_value_bucket") or "all")
    min_ev = _decimal(rule.get("min_expected_value"))
    direction = str(rule.get("direction") or "all")
    price_bucket = str(rule.get("price_bucket") or "all")

    if ev_bucket == "ev_neg":
        return {
            "trade_direction": "buy_no",
            "strategy_archetype": "fade_negative_yes_ev",
            "why": "The rule's entry EV bucket is negative for YES, so the paper expression is to fade YES by buying NO.",
        }
    if ev_bucket in POSITIVE_EV_BUCKETS or (min_ev is not None and min_ev >= Decimal("0")):
        return {
            "trade_direction": "buy_yes",
            "strategy_archetype": "follow_positive_yes_ev",
            "why": "The rule's entry EV is positive for YES, so the paper expression is to follow YES.",
        }
    if direction == "down" and price_bucket in {"p005_010", "p010_020"}:
        return {
            "trade_direction": "buy_no",
            "strategy_archetype": "price_action_fade_yes",
            "why": "The rule is a cheap/falling YES cohort, so the default paper expression is buy NO.",
            "inference_warning": "No explicit EV bucket was available; require stricter review before a lane is implemented.",
        }
    return {
        "trade_direction": None,
        "strategy_archetype": "ambiguous_signal_cohort",
        "why": "The surviving rule has positive historical evidence, but the trade side is ambiguous.",
    }


def _lane_match(
    *,
    family: str,
    strategy_version: str,
    match_type: str,
    reason_code: str,
    detail: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "family": family,
        "strategy_version": strategy_version,
        "lane": "paper_forward_gate",
        "match_type": match_type,
        "reason_code": reason_code,
        "detail": detail,
    }
    if family == "kalshi_cheap_yes_follow":
        payload["quarantine"] = dict(CHEAP_YES_FOLLOW_QUARANTINE)
    return payload


def _known_existing_lane(rule: dict[str, Any], *, trade_direction: str | None = None) -> dict[str, Any] | None:
    if (
        rule.get("signal_type") == "price_move"
        and rule.get("platform") in {"kalshi", "all"}
        and rule.get("direction") == "up"
        and rule.get("price_bucket") == "p020_050"
        and rule.get("expected_value_bucket") == "ev_005_plus"
        and rule.get("liquidity_bucket") == "liquidity_010k_100k"
        and trade_direction == "buy_yes"
    ):
        return _lane_match(
            family=ALPHA_KALSHI_4237F81367_FAMILY,
            strategy_version=ALPHA_KALSHI_4237F81367_VERSION,
            match_type="exact_existing_lane",
            reason_code="known_alpha_kalshi_4237f81367",
            detail="Exact rule is already covered by the frozen Alpha Factory paper lane.",
        )

    if (
        rule.get("signal_type") == "price_move"
        and rule.get("platform") in {"kalshi", "all"}
        and rule.get("direction") == "up"
        and str(rule.get("price_bucket") or "all") == "all"
        and rule.get("expected_value_bucket") == "ev_001_002"
        and rule.get("volume_bucket") == "volume_001k_010k"
        and str(rule.get("liquidity_bucket") or "all") == "all"
        and trade_direction == "buy_yes"
    ):
        return _lane_match(
            family=ALPHA_KALSHI_D80BDF77A9_FAMILY,
            strategy_version=ALPHA_KALSHI_D80BDF77A9_VERSION,
            match_type="exact_existing_lane",
            reason_code="known_alpha_kalshi_d80bdf77a9",
            detail="Exact rule is already covered by the frozen Alpha Factory volume paper lane.",
        )

    is_kalshi_price_down = (
        rule.get("signal_type") == "price_move"
        and rule.get("direction") == "down"
        and rule.get("platform") in {"kalshi", "all"}
    )
    if not is_kalshi_price_down:
        return None

    if (
        rule.get("price_bucket") == "p020_050"
        and rule.get("expected_value_bucket") == "ev_neg"
    ):
        return _lane_match(
            family="kalshi_down_yes_fade",
            strategy_version="kalshi_down_yes_fade_v2",
            match_type="exact_existing_lane",
            reason_code="known_kalshi_down_yes_fade",
            detail="Exact rule is already covered by the Kalshi down-YES fade paper lane.",
        )
    if (
        rule.get("timeframe") == "30m"
        and rule.get("price_bucket") == "p005_010"
        and rule.get("expected_value_bucket") == "ev_neg"
    ):
        return _lane_match(
            family="kalshi_very_low_yes_fade",
            strategy_version="kalshi_very_low_yes_fade_v1",
            match_type="exact_existing_lane",
            reason_code="known_kalshi_very_low_yes_fade",
            detail="Exact rule is already covered by the Kalshi very-low-YES fade paper lane.",
        )
    if rule.get("price_bucket") == "p005_010" and trade_direction == "buy_no":
        return _lane_match(
            family="kalshi_very_low_yes_fade",
            strategy_version="kalshi_very_low_yes_fade_v1",
            match_type="covered_existing_lane_variant",
            reason_code="covered_by_kalshi_very_low_yes_fade_variant",
            detail=(
                "Rule is a broader very-low/falling-YES fade variant; compare it inside the existing lane "
                "instead of spawning another paper lane."
            ),
        )
    if rule.get("price_bucket") == "p010_020" and rule.get("expected_value_bucket") == "ev_neg":
        return _lane_match(
            family="kalshi_low_yes_fade",
            strategy_version="kalshi_low_yes_fade_v1",
            match_type="exact_existing_lane",
            reason_code="known_kalshi_low_yes_fade",
            detail="Exact rule is already covered by the Kalshi low-YES fade paper lane.",
        )
    if rule.get("price_bucket") == "p010_020" and trade_direction == "buy_no":
        return _lane_match(
            family="kalshi_low_yes_fade",
            strategy_version="kalshi_low_yes_fade_v1",
            match_type="covered_existing_lane_variant",
            reason_code="covered_by_kalshi_low_yes_fade_variant",
            detail=(
                "Rule is a broader low/falling-YES fade variant; compare it inside the existing lane "
                "instead of spawning another paper lane."
            ),
        )
    if rule.get("price_bucket") == "p00_005" and rule.get("expected_value_bucket") == "ev_000_001":
        return _lane_match(
            family="kalshi_cheap_yes_follow",
            strategy_version="kalshi_cheap_yes_follow_v1",
            match_type="exact_existing_lane",
            reason_code="known_quarantined_kalshi_cheap_yes_follow",
            detail="Exact rule is already covered by the quarantined Kalshi cheap-YES follow paper lane.",
        )
    if trade_direction == "buy_yes" and (
        rule.get("price_bucket") == "p00_005" or rule.get("expected_value_bucket") == "ev_000_001"
    ):
        return _lane_match(
            family="kalshi_cheap_yes_follow",
            strategy_version="kalshi_cheap_yes_follow_v1",
            match_type="quarantined_related_lane_variant",
            reason_code="related_to_quarantined_kalshi_cheap_yes_follow",
            detail=(
                "Rule is a tiny-positive-YES follow variant near the quarantined cheap-YES lane; "
                "do not promote it as a fresh lane until forward evidence is reviewed."
            ),
        )
    return None


def _candidate_sort_key(candidate: dict[str, Any]) -> tuple:
    rule = candidate.get("rule") or {}
    trade_direction = _strategy_expression(rule).get("trade_direction")
    return (
        1 if candidate.get("test_pass") else 0,
        1 if trade_direction else 0,
        _float_metric(candidate, "test", "total_profit_loss"),
        _float_metric(candidate, "test", "avg_clv", default=-999.0),
        _int_metric(candidate, "test", "sample_count"),
        _float_metric(candidate, "validation", "total_profit_loss"),
        _float_metric(candidate, "validation", "avg_clv", default=-999.0),
        _specificity(rule),
        -_float_metric(candidate, "test", "max_drawdown"),
    )


def _is_actionable_new_candidate(candidate: dict[str, Any]) -> bool:
    return bool(
        candidate.get("ready_for_paper_lane")
        and candidate.get("trade_direction")
        and not candidate.get("blockers")
        and not candidate.get("existing_lane")
        and candidate.get("dedupe_status", "new_candidate") == "new_candidate"
    )


def _paper_lane_blueprint(candidate: dict[str, Any]) -> dict[str, Any] | None:
    if not _is_actionable_new_candidate(candidate):
        return None
    rule = dict(candidate.get("rule") or {})
    strategy_version = str(candidate.get("strategy_version") or candidate.get("candidate_id") or "alpha_candidate")
    strategy_family = strategy_version.removesuffix("_v1")
    return {
        "implementation_status": "ready_to_implement",
        "candidate_id": candidate.get("candidate_id"),
        "strategy_family": strategy_family,
        "strategy_version": strategy_version,
        "lane_slug": _safe_slug(strategy_family, max_length=80),
        "rule_digest": candidate.get("rule_digest"),
        "rule_label": candidate.get("rule_label"),
        "trade_direction": candidate.get("trade_direction"),
        "strategy_archetype": candidate.get("strategy_archetype"),
        "paper_only": True,
        "live_orders_enabled": False,
        "pilot_arming_enabled": False,
        "thresholds_frozen": True,
        "frozen_rule": rule,
        "frozen_dimensions": _frozen_dimensions(rule),
        "frozen_thresholds": _frozen_thresholds(rule),
        "current_market_precheck": {
            "required": True,
            "price_source": "fresh_kalshi_orderbook_midpoint",
            "reject_if_current_price_outside_frozen_price_bucket": bool(
                rule.get("price_bucket") not in (None, "", "all", "price_unknown")
            ),
            "reject_if_trade_side_no_longer_has_positive_yes_edge": True,
        },
        "required_surfaces": [
            "frozen_rule_evaluator",
            "strategy_registry_seed",
            "paper_execution_run_loop",
            "profitability_snapshot",
            "research_lab_lane_payload",
            "scheduler_lane_wiring",
        ],
        "promotion_gates": list(candidate.get("promotion_gates") or []),
        "source_metrics": candidate.get("metrics") or {},
    }


def _candidate_payload(candidate: dict[str, Any], *, platform: str, rank: int) -> dict[str, Any]:
    rule = dict(candidate.get("rule") or {})
    expression = _strategy_expression(rule)
    digest = _rule_digest(rule)
    label = _rule_label(rule)
    trade_direction = expression.get("trade_direction")
    existing_lane = _known_existing_lane(rule, trade_direction=trade_direction)
    blockers: list[str] = []
    if not candidate.get("test_pass"):
        blockers.append("failed_chronological_holdout")
    if not trade_direction:
        blockers.append("ambiguous_trade_expression")
    if rule.get("direction") == "all":
        blockers.append("overbroad_alpha_rule")
    if existing_lane is not None and existing_lane.get("match_type") == "covered_existing_lane_variant":
        blockers.append("covered_by_existing_lane_variant")
    quarantine = (existing_lane or {}).get("quarantine") or {}
    if quarantine.get("enabled"):
        blockers.append("matched_quarantined_lane_family")

    strategy_slug = _safe_slug(label, max_length=48)
    strategy_version = (
        existing_lane["strategy_version"]
        if existing_lane is not None
        else f"alpha_{platform}_{strategy_slug}_{digest}_v1"
    )
    ready_for_paper = bool(candidate.get("test_pass") and trade_direction and not blockers)

    next_step = "continue_forward_paper_collection" if existing_lane else "implement_frozen_paper_lane"
    if "matched_quarantined_lane_family" in blockers:
        next_step = "keep_quarantined_lane_paused"
    elif "covered_by_existing_lane_variant" in blockers:
        next_step = "review_existing_lane_variant"
    elif "overbroad_alpha_rule" in blockers:
        next_step = "refine_overbroad_alpha_rule"
    if blockers:
        next_step = (
            next_step
            if existing_lane or "overbroad_alpha_rule" in blockers
            else "review_or_discard_candidate"
        )

    payload = {
        "rank": rank,
        "candidate_id": f"{platform}_alpha_{digest}",
        "family": "alpha_factory",
        "strategy_version": strategy_version,
        "rule_digest": digest,
        "rule": rule,
        "rule_label": label,
        "platform": platform,
        "trade_direction": trade_direction,
        "strategy_archetype": expression.get("strategy_archetype"),
        "expression_rationale": expression.get("why"),
        "inference_warning": expression.get("inference_warning"),
        "existing_lane": existing_lane,
        "dedupe_status": (existing_lane or {}).get("match_type") or "new_candidate",
        "ready_for_paper_lane": ready_for_paper,
        "paper_only": True,
        "live_orders_enabled": False,
        "pilot_arming_enabled": False,
        "thresholds_frozen": True,
        "blockers": blockers,
        "next_step": next_step,
        "metrics": {
            "train": candidate.get("train") or {},
            "validation": candidate.get("validation") or {},
            "test": candidate.get("test") or {},
        },
        "survival": {
            "train_pass": bool(candidate.get("train_pass")),
            "validation_pass": bool(candidate.get("validation_pass")),
            "test_pass": bool(candidate.get("test_pass")),
            "verdict": candidate.get("verdict"),
        },
        "promotion_gates": [
            "implement as a separate paper-only lane",
            "observe at least 30 calendar days forward",
            "collect at least 20 resolved paper trades",
            "require positive execution-adjusted paper P&L",
            "require positive average CLV",
            "pause on 5% paper drawdown or evidence outage",
        ],
    }
    payload["candidate_queue_status"] = (
        "paper_lane_blueprint_ready"
        if _is_actionable_new_candidate(payload)
        else "existing_lane_forward_collection"
        if existing_lane and ready_for_paper
        else "blocked"
        if blockers
        else "holdout_failed"
    )
    payload["paper_lane_blueprint"] = _paper_lane_blueprint(payload)
    return payload


def _next_best_actions(candidates: list[dict[str, Any]], *, platform: str) -> list[dict[str, Any]]:
    if not candidates:
        return [
            {
                "step": "expand_kalshi_alpha_search",
                "priority_score": 80,
                "why_ev": "No surviving Kalshi candidate was found; broaden feature templates or improve resolved-signal coverage before adding execution work.",
                "operator_action": "Run alpha-factory with a larger window, lower minimum samples only for exploration, then require holdout survival again.",
            }
        ]

    actions: list[dict[str, Any]] = []
    for candidate in candidates[:5]:
        existing_lane = candidate.get("existing_lane")
        blockers = set(candidate.get("blockers") or [])
        if "matched_quarantined_lane_family" in blockers:
            step = "keep_quarantined_lane_paused"
            operator_action = (
                f"Do not implement {candidate.get('strategy_version')} as a new lane; "
                "keep the related quarantined paper lane paused until forward evidence is reviewed."
            )
            priority = 75
        elif "covered_by_existing_lane_variant" in blockers:
            step = "review_existing_lane_variant"
            operator_action = (
                f"Treat {candidate.get('candidate_id')} as a variant of "
                f"{existing_lane.get('strategy_version') if existing_lane else 'an existing lane'}; "
                "compare it in lane diagnostics before creating any new paper evaluator."
            )
            priority = 70
        elif "overbroad_alpha_rule" in blockers:
            step = "refine_overbroad_alpha_rule"
            operator_action = (
                f"Do not implement {candidate.get('candidate_id')} directly; require a directional cohort "
                "or lane-specific rule before paper-lane creation."
            )
            priority = 65
        elif existing_lane:
            step = "keep_existing_candidate_lane_collecting_forward_evidence"
            operator_action = (
                f"Keep {existing_lane.get('strategy_version')} paper-only and compare forward CLV/P&L "
                "against the factory's next candidates."
            )
            priority = 95
        elif candidate.get("ready_for_paper_lane"):
            step = "implement_frozen_alpha_paper_lane"
            operator_action = (
                f"Create a paper-only evaluator for {candidate.get('strategy_version')} with "
                f"trade_direction={candidate.get('trade_direction')}."
            )
            priority = 90
        else:
            step = "review_alpha_candidate_expression"
            operator_action = (
                f"Review {candidate.get('candidate_id')} before implementation; blockers="
                f"{candidate.get('blockers') or []}."
            )
            priority = 60
        test = (candidate.get("metrics") or {}).get("test") or {}
        actions.append(
            {
                "step": step,
                "priority_score": priority,
                "why_ev": (
                    f"{candidate.get('rule_label')} survived the chronological holdout on {platform}; "
                    f"test P&L={test.get('total_profit_loss')}, test CLV={test.get('avg_clv')}, "
                    f"test sample={test.get('sample_count')}."
                ),
                "operator_action": operator_action,
                "evidence": {
                    "candidate_id": candidate.get("candidate_id"),
                    "strategy_version": candidate.get("strategy_version"),
                    "trade_direction": candidate.get("trade_direction"),
                    "test": test,
                    "blockers": candidate.get("blockers") or [],
                },
            }
        )
    return actions


def build_alpha_factory_snapshot_from_rows(
    rows: Sequence[AlphaSignalRow],
    *,
    platform: str = DEFAULT_PLATFORM,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    min_train_sample: int = 20,
    min_validation_sample: int = 10,
    min_test_sample: int = 10,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated_at = generated_at or _utcnow()
    normalized_platform = str(platform or DEFAULT_PLATFORM).strip().lower()
    filtered_rows = [
        row
        for row in rows
        if normalized_platform == "all" or str(row.platform or "").strip().lower() == normalized_platform
    ]
    filtered_rows.sort(key=lambda row: row.fired_at)
    result = evaluate_alpha_gauntlet_rows(
        list(filtered_rows),
        min_train_sample=min_train_sample,
        min_validation_sample=min_validation_sample,
        min_test_sample=min_test_sample,
        top_n=max(max_candidates * 50, 500),
    )
    surviving = list(result.get("surviving_candidates") or [])
    surviving.sort(key=_candidate_sort_key, reverse=True)
    selected = list(result.get("selected_candidates") or [])
    selected.sort(key=_candidate_sort_key, reverse=True)

    max_candidate_count = max(1, int(max_candidates))
    candidate_payloads = [
        _candidate_payload(candidate, platform=normalized_platform, rank=index)
        for index, candidate in enumerate(surviving, start=1)
    ]
    candidates = candidate_payloads[:max_candidate_count]
    candidate_queue = [
        candidate
        for candidate in candidate_payloads
        if _is_actionable_new_candidate(candidate)
    ][:max_candidate_count]
    ready_count = sum(1 for candidate in candidate_payloads if candidate.get("ready_for_paper_lane"))
    new_ready_count = len(candidate_queue)
    existing_count = sum(1 for candidate in candidate_payloads if candidate.get("existing_lane"))
    suppressed_count = sum(
        1
        for candidate in candidate_payloads
        if set(candidate.get("blockers") or [])
        & {"covered_by_existing_lane_variant", "matched_quarantined_lane_family", "overbroad_alpha_rule"}
    )
    blockers: list[str] = []
    if not filtered_rows:
        blockers.append("no_kalshi_resolved_signal_history")
    if filtered_rows and not surviving:
        blockers.append("no_surviving_alpha_factory_candidates")
    if surviving and ready_count == 0:
        blockers.append("no_executable_alpha_factory_candidates")

    verdict = "candidate_queue_ready" if ready_count else str(result.get("verdict") or "insufficient_data")
    action_candidates: list[dict[str, Any]] = []
    seen_action_ids: set[str] = set()
    for candidate in [*candidate_queue, *candidates]:
        candidate_id = str(candidate.get("candidate_id") or candidate.get("strategy_version") or candidate.get("rank"))
        if candidate_id in seen_action_ids:
            continue
        seen_action_ids.add(candidate_id)
        action_candidates.append(candidate)
    actions = _next_best_actions(action_candidates, platform=normalized_platform)
    return {
        "schema_version": ALPHA_FACTORY_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "platform": normalized_platform,
        "row_count": len(filtered_rows),
        "input_row_count": len(rows),
        "paper_only": True,
        "live_submission_permitted": False,
        "pilot_arming_permitted": False,
        "threshold_mutation_permitted": False,
        "selection_policy": {
            "source": "walk_forward_alpha_gauntlet",
            "split": "chronological_50_25_25",
            "min_train_sample": min_train_sample,
            "min_validation_sample": min_validation_sample,
            "min_test_sample": min_test_sample,
            "max_candidates": max_candidates,
            "platform_filter": normalized_platform,
            "candidate_rule": "train+validation selection must survive final chronological holdout",
        },
        "gauntlet": result,
        "verdict": verdict,
        "blockers": blockers,
        "candidate_count": len(candidates),
        "candidate_pool_count": len(candidate_payloads),
        "ready_candidate_count": ready_count,
        "new_ready_candidate_count": new_ready_count,
        "candidate_queue_count": len(candidate_queue),
        "candidate_queue": candidate_queue,
        "existing_lane_count": existing_count,
        "suppressed_candidate_count": suppressed_count,
        "top_candidates": candidates,
        "holdout_failures": selected[: min(len(selected), max_candidates)],
        "next_best_actions": actions,
        "warnings": sorted(
            set(
                list(result.get("warnings") or [])
                + [
                    "historical_signal_level_evidence_not_live_profit",
                    "paper_lane_required_before_any_real_money_decision",
                    "candidate_thresholds_must_remain_frozen_after_creation",
                ]
                + (
                    ["known_lane_or_overbroad_variants_suppressed_from_new_candidate_count"]
                    if suppressed_count
                    else []
                )
            )
        ),
    }


async def build_alpha_factory_snapshot(
    session: AsyncSession,
    *,
    window_days: int = 365,
    max_signals: int = 50_000,
    platform: str = DEFAULT_PLATFORM,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
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
    return build_alpha_factory_snapshot_from_rows(
        rows,
        platform=platform,
        max_candidates=max_candidates,
        min_train_sample=min_train_sample,
        min_validation_sample=min_validation_sample,
        min_test_sample=min_test_sample,
        generated_at=as_of,
    ) | {
        "window_days": window_days,
        "max_signals": max_signals,
    }


def alpha_factory_lane_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    candidate_queue = list(snapshot.get("candidate_queue") or [])
    candidates = list(snapshot.get("top_candidates") or [])
    best = next((candidate for candidate in candidate_queue if candidate.get("ready_for_paper_lane")), None)
    best = best or next((candidate for candidate in candidates if candidate.get("ready_for_paper_lane")), None)
    best = best or (candidates[0] if candidates else None)
    blockers = list(snapshot.get("blockers") or [])
    if best is not None:
        blockers.extend(str(blocker) for blocker in best.get("blockers") or [])
    test = ((best or {}).get("metrics") or {}).get("test") or {}
    replay_net_pnl = _decimal(test.get("total_profit_loss"))
    avg_clv = _decimal(test.get("avg_clv"))
    verdict = "research_ready" if best and not blockers and best.get("ready_for_paper_lane") else "insufficient_evidence"
    return {
        "family": "alpha_factory",
        "strategy_version": (best or {}).get("strategy_version") or "alpha_factory_v2",
        "lane": "candidate_discovery",
        "source_kind": "alpha_factory_snapshot",
        "source_ref": snapshot.get("generated_at"),
        "status": "completed",
        "verdict": verdict,
        "realized_pnl": None,
        "mark_to_market_pnl": None,
        "replay_net_pnl": replay_net_pnl,
        "avg_clv": avg_clv,
        "resolved_trades": int(test.get("sample_count") or 0),
        "fill_rate": None,
        "drawdown": _decimal(test.get("max_drawdown")),
        "open_exposure": Decimal("0"),
        "coverage_mode": "chronological_signal_holdout" if best else "not_run",
        "blockers": sorted(set(blockers)),
        "details_json": {
            "platform": snapshot.get("platform"),
            "row_count": snapshot.get("row_count"),
            "candidate_count": snapshot.get("candidate_count"),
            "candidate_pool_count": snapshot.get("candidate_pool_count"),
            "ready_candidate_count": snapshot.get("ready_candidate_count"),
            "new_ready_candidate_count": snapshot.get("new_ready_candidate_count"),
            "candidate_queue_count": snapshot.get("candidate_queue_count"),
            "candidate_queue": candidate_queue,
            "suppressed_candidate_count": snapshot.get("suppressed_candidate_count"),
            "selected_candidate": best,
            "paper_lane_blueprint": (best or {}).get("paper_lane_blueprint"),
            "top_candidates": candidates[:10],
            "next_best_actions": snapshot.get("next_best_actions") or [],
            "warnings": snapshot.get("warnings") or [],
            "gauntlet_verdict": (snapshot.get("gauntlet") or {}).get("verdict"),
        },
    }


def _artifact_stem(*, as_of: datetime, platform: str, window_days: int, max_candidates: int) -> str:
    return f"{as_of.date().isoformat()}-alpha-factory-{platform}-{window_days}d-top{max_candidates}"


def _render_markdown(snapshot: dict[str, Any]) -> str:
    candidate_queue = snapshot.get("candidate_queue") or []
    candidates = snapshot.get("top_candidates") or []
    actions = snapshot.get("next_best_actions") or []

    queue_lines = "\n".join(
        (
            f"| {row.get('rank')} | `{row.get('strategy_version')}` | `{row.get('trade_direction')}` | "
            f"`{((row.get('rule') or {}).get('feature_family') or 'core')}` | `{row.get('rule_digest')}` | "
            f"{((row.get('metrics') or {}).get('test') or {}).get('sample_count', 0)} | "
            f"{((row.get('metrics') or {}).get('test') or {}).get('total_profit_loss', 0)} | "
            f"{((row.get('metrics') or {}).get('test') or {}).get('avg_clv')} | "
            f"`{((row.get('paper_lane_blueprint') or {}).get('implementation_status') or '-')}` |"
        )
        for row in candidate_queue
    )
    if not queue_lines:
        queue_lines = "| - | - | - | - | - | - | - | - | - |"

    candidate_lines = "\n".join(
        (
            f"| {row.get('rank')} | `{row.get('strategy_version')}` | `{row.get('trade_direction') or '-'}` | "
            f"`{row.get('strategy_archetype')}` | {((row.get('metrics') or {}).get('test') or {}).get('sample_count', 0)} | "
            f"{((row.get('metrics') or {}).get('test') or {}).get('total_profit_loss', 0)} | "
            f"{((row.get('metrics') or {}).get('test') or {}).get('avg_clv')} | "
            f"`{row.get('dedupe_status')}` | "
            f"`{row.get('next_step')}` |"
        )
        for row in candidates
    )
    if not candidate_lines:
        candidate_lines = "| - | - | - | - | - | - | - | - | - |"

    action_lines = "\n".join(
        f"- `{row.get('step')}`: {row.get('operator_action')} ({row.get('why_ev')})"
        for row in actions
    ) or "- None"
    blocker_lines = "\n".join(f"- `{blocker}`" for blocker in snapshot.get("blockers") or []) or "- None"
    warning_lines = "\n".join(f"- `{warning}`" for warning in snapshot.get("warnings") or []) or "- None"
    return f"""# Alpha Factory

**Generated:** {snapshot.get('generated_at')}
**Platform:** `{snapshot.get('platform')}`
**Window days:** {snapshot.get('window_days')}
**Rows tested:** {snapshot.get('row_count')}
**Verdict:** `{snapshot.get('verdict')}`
**Candidate pool count:** {snapshot.get('candidate_pool_count')}
**New ready candidates:** {snapshot.get('new_ready_candidate_count')}
**Candidate queue count:** {snapshot.get('candidate_queue_count')}
**Suppressed known/quarantined/overbroad variants:** {snapshot.get('suppressed_candidate_count')}
**Paper only:** `true`
**Live submission permitted:** `false`

## New Candidate Queue

| Rank | Strategy Version | Trade | Feature Family | Rule Digest | Test N | Test P&L | Test CLV | Blueprint |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- |
{queue_lines}

## Top Candidates

| Rank | Strategy Version | Trade | Archetype | Test N | Test P&L | Test CLV | Dedupe | Next Step |
| ---: | --- | --- | --- | ---: | ---: | ---: | --- | --- |
{candidate_lines}

## Top Actions

{action_lines}

## Blockers

{blocker_lines}

## Warnings

{warning_lines}
"""


async def generate_alpha_factory_artifact(
    session: AsyncSession,
    *,
    window_days: int = 365,
    max_signals: int = 50_000,
    platform: str = DEFAULT_PLATFORM,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    min_train_sample: int = 20,
    min_validation_sample: int = 10,
    min_test_sample: int = 10,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    as_of = as_of or _utcnow()
    snapshot = await build_alpha_factory_snapshot(
        session,
        window_days=window_days,
        max_signals=max_signals,
        platform=platform,
        max_candidates=max_candidates,
        min_train_sample=min_train_sample,
        min_validation_sample=min_validation_sample,
        min_test_sample=min_test_sample,
        as_of=as_of,
    )
    output_dir = _repo_root() / ALPHA_FACTORY_ARTIFACT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = _artifact_stem(
        as_of=as_of,
        platform=str(platform or DEFAULT_PLATFORM).strip().lower(),
        window_days=window_days,
        max_candidates=max_candidates,
    )
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.write_text(json.dumps(_json_safe(snapshot), indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(_render_markdown(snapshot), encoding="utf-8")
    return {
        "alpha_factory_json_path": str(json_path),
        "alpha_factory_markdown_path": str(markdown_path),
        "snapshot": snapshot,
        "lane_payload": alpha_factory_lane_payload(snapshot),
    }
