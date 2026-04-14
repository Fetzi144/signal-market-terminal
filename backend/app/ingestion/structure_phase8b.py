from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.ingestion.polymarket_execution_policy import (
    ONE,
    PRICE_Q,
    TEN_THOUSAND,
    ZERO,
    BookLevel,
    _ensure_utc,
    _entry_price_for_direction,
    _estimate_taker_fee_total,
    _latest_param_history,
    _quantize,
    _rebuild_current_book,
    _resolve_taker_fee_rate,
    _to_decimal,
)
from app.metrics import (
    polymarket_structure_current_opportunities,
    polymarket_structure_informational_only_opportunities,
    polymarket_structure_paper_plans,
    polymarket_structure_paper_route_attempts,
    polymarket_structure_pending_approvals,
    polymarket_structure_skipped_groups,
    polymarket_structure_stale_cross_venue_links,
    polymarket_structure_validation_reason_codes,
    polymarket_structure_validation_results,
)
from app.models.market import Outcome
from app.models.market_structure import (
    CrossVenueMarketLink,
    MarketStructureGroup,
    MarketStructureGroupMember,
    MarketStructureOpportunity,
    MarketStructureOpportunityLeg,
    MarketStructurePaperOrder,
    MarketStructurePaperOrderEvent,
    MarketStructurePaperPlan,
    MarketStructureRun,
    MarketStructureValidation,
)
from app.models.paper_trade import PaperTrade
from app.models.polymarket_reconstruction import PolymarketBookReconState
from app.models.snapshot import OrderbookSnapshot
from app.signals.risk import check_exposure

VALIDATION_EXECUTABLE = "executable_candidate"
VALIDATION_INFORMATIONAL = "informational_only"
VALIDATION_BLOCKED = "blocked"

ACTIVE_PLAN_STATUSES = {"approval_pending", "routing_pending", "routed", "partial_failed"}
OPEN_EXPOSURE_PLAN_STATUSES = {"routed", "partial_failed"}
OPEN_PAPER_TRADE_STATUSES = {"open", "submitted", "confirmed"}

HARD_BLOCK_REASONS = {
    "stale_leg_book",
    "missing_depth",
    "leg_slippage_cap_breached",
    "non_actionable_group",
    "incomplete_parity_composition",
    "cross_venue_link_inactive",
    "cross_venue_link_expired",
    "cross_venue_review_required",
    "missing_explicit_no_leg",
    "missing_current_executable_estimate",
    "plan_notional_cap_exceeded",
    "exposure_limit_blocked",
}

REASON_LABELS = {
    "stale_leg_book": "Stale leg or book",
    "missing_depth": "Missing minimum depth",
    "leg_slippage_cap_breached": "Leg slippage cap breached",
    "non_actionable_group": "Group is informational only",
    "incomplete_parity_composition": "Incomplete parity composition",
    "cross_venue_link_inactive": "Cross-venue link inactive",
    "cross_venue_link_expired": "Cross-venue link expired",
    "cross_venue_review_required": "Cross-venue review required",
    "missing_explicit_no_leg": "Missing explicit No leg",
    "missing_current_executable_estimate": "Missing current executable estimate",
    "edge_decayed_below_threshold": "Edge decayed below threshold",
    "no_positive_current_edge": "No positive current edge",
    "plan_notional_cap_exceeded": "Plan notional cap exceeded",
    "plan_too_old": "Plan too old to route",
    "exposure_limit_blocked": "Exposure check blocked plan",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _serialize_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _serialize_decimal(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        value = _ensure_utc(value)
        return value.isoformat() if value is not None else None
    if isinstance(value, dict):
        return {str(key): _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _parse_orderbook_levels(payload: Any, *, reverse: bool) -> list[BookLevel]:
    level_map: dict[Decimal, Decimal] = {}
    if isinstance(payload, dict):
        if "levels" in payload and isinstance(payload["levels"], list):
            payload = payload["levels"]
        else:
            for raw_price, raw_size in payload.items():
                price = _to_decimal(raw_price)
                size = _to_decimal(raw_size)
                if price is not None and size is not None and size > ZERO:
                    level_map[price] = size
            return [
                BookLevel(yes_price=price, size_shares=size)
                for price, size in sorted(level_map.items(), key=lambda item: item[0], reverse=reverse)
            ]
    if isinstance(payload, list):
        for level in payload:
            price = None
            size = None
            if isinstance(level, dict):
                price = _to_decimal(level.get("price"))
                size = _to_decimal(level.get("size"))
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                price = _to_decimal(level[0])
                size = _to_decimal(level[1])
            if price is not None and size is not None and size > ZERO:
                level_map[price] = size
    return [
        BookLevel(yes_price=price, size_shares=size)
        for price, size in sorted(level_map.items(), key=lambda item: item[0], reverse=reverse)
    ]


def _walk_levels_for_shares(
    *,
    levels: list[BookLevel],
    target_shares: Decimal,
    touch_entry_price: Decimal | None,
    direction: str | None = None,
    direct_price_mode: bool = False,
) -> dict[str, Any]:
    remaining_shares = target_shares
    fillable_shares = ZERO
    weighted_price = ZERO
    worst_price = None
    path: list[dict[str, Any]] = []
    for level in levels:
        entry_price = (
            level.yes_price
            if direct_price_mode
            else _entry_price_for_direction(direction or "buy_yes", yes_price=level.yes_price)
        )
        if entry_price <= ZERO:
            continue
        take_shares = min(level.size_shares, remaining_shares)
        if take_shares <= ZERO:
            continue
        fillable_shares += take_shares
        weighted_price += take_shares * entry_price
        worst_price = entry_price
        remaining_shares -= take_shares
        path.append(
            _json_safe(
                {
                    "price": level.yes_price,
                    "entry_price": entry_price,
                    "visible_shares": level.size_shares,
                    "taken_shares": take_shares,
                }
            )
        )
        if remaining_shares <= ZERO:
            break

    avg_entry_price = None
    if fillable_shares > ZERO:
        avg_entry_price = (weighted_price / fillable_shares).quantize(PRICE_Q)
    slippage_bps = None
    if avg_entry_price is not None and touch_entry_price is not None and touch_entry_price > ZERO:
        slippage_bps = (((avg_entry_price - touch_entry_price) / touch_entry_price) * TEN_THOUSAND).quantize(PRICE_Q)
    return {
        "fillable_shares": fillable_shares,
        "avg_entry_price": avg_entry_price,
        "worst_price": _quantize(worst_price, PRICE_Q),
        "slippage_bps": slippage_bps,
        "path": path,
    }


def _edge_bps(edge_total: Decimal | None, reference_cost: Decimal | None) -> Decimal | None:
    if edge_total is None or reference_cost is None or reference_cost <= ZERO:
        return None
    return ((edge_total / reference_cost) * TEN_THOUSAND).quantize(PRICE_Q)


def _normalize_reason_codes(reason_codes: list[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for code in reason_codes:
        if not code or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return normalized


def _edge_bucket(net_edge_bps: Decimal | None) -> str:
    if net_edge_bps is None:
        return "unknown"
    if net_edge_bps < ZERO:
        return "negative"
    if net_edge_bps < Decimal("25"):
        return "0-25bps"
    if net_edge_bps < Decimal("100"):
        return "25-100bps"
    return "100bps_plus"


def _effective_review_status(link: CrossVenueMarketLink | None) -> str | None:
    if link is None:
        return None
    if not link.active or link.review_status == "disabled":
        return "disabled"
    expires_at = _ensure_utc(link.expires_at)
    if expires_at is not None and expires_at < _utcnow():
        return "expired"
    return link.review_status


def _reason_code_from_invalid_reason(invalid_reason: str | None) -> str | None:
    if invalid_reason in {
        "snapshot_stale",
        "missing_recon_state",
        "missing_orderbook_snapshot",
        "book_unreliable",
    }:
        return "stale_leg_book"
    if invalid_reason in {
        "no_visible_depth",
        "insufficient_depth",
        "below_min_order_size",
        "missing_touch_price",
    }:
        return "missing_depth"
    if invalid_reason == "leg_slippage_too_high":
        return "leg_slippage_cap_breached"
    if invalid_reason in {"missing_no_outcome", "missing_outcome_id", "missing_outcome_row"}:
        return "missing_explicit_no_leg"
    if invalid_reason in {"group_non_actionable"}:
        return "non_actionable_group"
    if invalid_reason in {"incomplete_event_composition_filtered"}:
        return "incomplete_parity_composition"
    if invalid_reason in {"missing_cost_estimate"}:
        return "missing_current_executable_estimate"
    return None


def _plan_notional_total(legs: list[dict[str, Any]]) -> Decimal | None:
    if not legs:
        return None
    total = ZERO
    for leg in legs:
        avg_entry = _to_decimal(leg.get("est_avg_entry_price"))
        fillable = _to_decimal(leg.get("est_fillable_size"))
        fee = _to_decimal(leg.get("est_fee")) or ZERO
        target_size = _to_decimal(leg.get("target_size")) or ZERO
        if avg_entry is None:
            return None
        used_size = fillable if fillable is not None else target_size
        total += (used_size * avg_entry) + fee
    return total.quantize(PRICE_Q)


def serialize_structure_validation(row: MarketStructureValidation) -> dict[str, Any]:
    return {
        "id": row.id,
        "opportunity_id": row.opportunity_id,
        "run_id": row.run_id,
        "evaluation_kind": row.evaluation_kind,
        "classification": row.classification,
        "reason_codes_json": row.reason_codes_json,
        "confidence": _serialize_decimal(row.confidence),
        "detected_gross_edge_bps": _serialize_decimal(row.detected_gross_edge_bps),
        "detected_net_edge_bps": _serialize_decimal(row.detected_net_edge_bps),
        "detected_gross_edge_total": _serialize_decimal(row.detected_gross_edge_total),
        "detected_net_edge_total": _serialize_decimal(row.detected_net_edge_total),
        "current_gross_edge_bps": _serialize_decimal(row.current_gross_edge_bps),
        "current_net_edge_bps": _serialize_decimal(row.current_net_edge_bps),
        "current_gross_edge_total": _serialize_decimal(row.current_gross_edge_total),
        "current_net_edge_total": _serialize_decimal(row.current_net_edge_total),
        "gross_edge_decay_total": _serialize_decimal(row.gross_edge_decay_total),
        "net_edge_decay_total": _serialize_decimal(row.net_edge_decay_total),
        "detected_age_seconds": row.detected_age_seconds,
        "max_leg_age_seconds": row.max_leg_age_seconds,
        "stale_leg_count": row.stale_leg_count,
        "executable_leg_count": row.executable_leg_count,
        "total_leg_count": row.total_leg_count,
        "summary_json": row.summary_json,
        "created_at": row.created_at,
    }


def serialize_structure_paper_plan(row: MarketStructurePaperPlan) -> dict[str, Any]:
    return {
        "id": row.id,
        "opportunity_id": row.opportunity_id,
        "validation_id": row.validation_id,
        "run_id": row.run_id,
        "status": row.status,
        "auto_created": row.auto_created,
        "manual_approval_required": row.manual_approval_required,
        "approved_by": row.approved_by,
        "approved_at": row.approved_at,
        "rejected_by": row.rejected_by,
        "rejected_at": row.rejected_at,
        "rejection_reason": row.rejection_reason,
        "routed_at": row.routed_at,
        "completed_at": row.completed_at,
        "package_size": _serialize_decimal(row.package_size),
        "plan_notional_total": _serialize_decimal(row.plan_notional_total),
        "reason_codes_json": row.reason_codes_json,
        "summary_json": row.summary_json,
        "details_json": row.details_json,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def serialize_structure_paper_order(row: MarketStructurePaperOrder) -> dict[str, Any]:
    return {
        "id": row.id,
        "plan_id": row.plan_id,
        "opportunity_leg_id": row.opportunity_leg_id,
        "leg_index": row.leg_index,
        "venue": row.venue,
        "market_id": row.market_id,
        "outcome_id": row.outcome_id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "side": row.side,
        "role": row.role,
        "action_type": row.action_type,
        "order_type_hint": row.order_type_hint,
        "target_size": _serialize_decimal(row.target_size),
        "planned_entry_price": _serialize_decimal(row.planned_entry_price),
        "planned_notional": _serialize_decimal(row.planned_notional),
        "filled_size": _serialize_decimal(row.filled_size),
        "avg_fill_price": _serialize_decimal(row.avg_fill_price),
        "fill_notional": _serialize_decimal(row.fill_notional),
        "status": row.status,
        "error_reason": row.error_reason,
        "details_json": row.details_json,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def serialize_structure_paper_order_event(row: MarketStructurePaperOrderEvent) -> dict[str, Any]:
    return {
        "id": row.id,
        "plan_id": row.plan_id,
        "paper_order_id": row.paper_order_id,
        "event_type": row.event_type,
        "status": row.status,
        "message": row.message,
        "details_json": row.details_json,
        "observed_at": row.observed_at,
        "created_at": row.created_at,
    }


def serialize_cross_venue_link(row: CrossVenueMarketLink | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": row.id,
        "link_key": row.link_key,
        "left_venue": row.left_venue,
        "left_market_id": row.left_market_id,
        "left_outcome_id": row.left_outcome_id,
        "left_condition_id": row.left_condition_id,
        "left_asset_id": row.left_asset_id,
        "left_external_id": row.left_external_id,
        "left_symbol": row.left_symbol,
        "right_venue": row.right_venue,
        "right_market_id": row.right_market_id,
        "right_outcome_id": row.right_outcome_id,
        "right_condition_id": row.right_condition_id,
        "right_asset_id": row.right_asset_id,
        "right_external_id": row.right_external_id,
        "right_symbol": row.right_symbol,
        "mapping_kind": row.mapping_kind,
        "provenance_source": row.provenance_source,
        "owner": row.owner,
        "reviewed_by": row.reviewed_by,
        "review_status": row.review_status,
        "effective_review_status": _effective_review_status(row),
        "confidence": _serialize_decimal(row.confidence),
        "notes": row.notes,
        "last_reviewed_at": row.last_reviewed_at,
        "expires_at": row.expires_at,
        "active": row.active,
        "details_json": row.details_json,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


async def _load_cross_venue_link_for_group(
    session: AsyncSession,
    group: MarketStructureGroup,
) -> CrossVenueMarketLink | None:
    details = group.details_json if isinstance(group.details_json, dict) else {}
    link_id = details.get("link_id")
    if link_id is None:
        return None
    return await session.get(CrossVenueMarketLink, link_id)


async def _reprice_polymarket_leg(
    session: AsyncSession,
    leg: MarketStructureOpportunityLeg,
    *,
    observed_at: datetime,
) -> dict[str, Any]:
    details = leg.details_json if isinstance(leg.details_json, dict) else {}
    pricing_asset_id = details.get("pricing_asset_id") or leg.asset_id
    pricing_condition_id = details.get("pricing_condition_id") or leg.condition_id
    pricing_asset_dim_id = details.get("pricing_asset_dim_id") or leg.asset_dim_id
    if pricing_asset_id is None or pricing_condition_id is None:
        return {
            "leg_index": leg.leg_index,
            "valid": False,
            "invalid_reason": "missing_polymarket_identifiers",
            "observed_at_local": None,
            "event_ts_exchange": None,
        }

    recon_state = (
        await session.execute(
            select(PolymarketBookReconState).where(PolymarketBookReconState.asset_id == pricing_asset_id)
        )
    ).scalar_one_or_none()
    if recon_state is None:
        return {
            "leg_index": leg.leg_index,
            "valid": False,
            "invalid_reason": "missing_recon_state",
            "observed_at_local": None,
            "event_ts_exchange": None,
        }

    snapshot, bids, asks, reliable_book, book_reason = await _rebuild_current_book(session, recon_state)
    snapshot_observed = _ensure_utc(snapshot.observed_at_local) if snapshot is not None else None
    snapshot_event = _ensure_utc(snapshot.event_ts_exchange) if snapshot is not None else None
    if not reliable_book:
        return {
            "leg_index": leg.leg_index,
            "valid": False,
            "invalid_reason": book_reason or "book_unreliable",
            "observed_at_local": snapshot_observed,
            "event_ts_exchange": snapshot_event,
        }

    max_staleness = timedelta(seconds=settings.polymarket_structure_cross_venue_max_staleness_seconds)
    if snapshot_observed is None or observed_at - snapshot_observed > max_staleness:
        return {
            "leg_index": leg.leg_index,
            "valid": False,
            "invalid_reason": "snapshot_stale",
            "observed_at_local": snapshot_observed,
            "event_ts_exchange": snapshot_event,
        }

    param_history = await _latest_param_history(
        session,
        condition_id=pricing_condition_id,
        asset_id=pricing_asset_id,
    )
    min_order_size = param_history.min_order_size if param_history is not None else None
    taker_fee_rate = _resolve_taker_fee_rate(param_history) if param_history is not None else ZERO
    fees_enabled = bool(param_history.fees_enabled) if param_history is not None else False

    touch_yes_price = recon_state.best_ask if leg.side == "buy_yes" else recon_state.best_bid
    if touch_yes_price is None:
        return {
            "leg_index": leg.leg_index,
            "valid": False,
            "invalid_reason": "missing_touch_price",
            "observed_at_local": snapshot_observed,
            "event_ts_exchange": snapshot_event,
        }
    touch_entry_price = _entry_price_for_direction(leg.side, yes_price=touch_yes_price)
    levels = asks if leg.side == "buy_yes" else bids
    if not levels:
        return {
            "leg_index": leg.leg_index,
            "valid": False,
            "invalid_reason": "no_visible_depth",
            "observed_at_local": snapshot_observed,
            "event_ts_exchange": snapshot_event,
        }

    walk = _walk_levels_for_shares(
        levels=levels,
        target_shares=leg.target_size,
        touch_entry_price=touch_entry_price,
        direction=leg.side,
    )
    fillable_shares = walk["fillable_shares"]
    avg_entry_price = walk["avg_entry_price"]
    notional_cost = ((fillable_shares or ZERO) * (avg_entry_price or ZERO)).quantize(PRICE_Q)
    taker_fee_total = _estimate_taker_fee_total(
        fillable_size=notional_cost,
        entry_price=avg_entry_price or ZERO,
        fee_rate=taker_fee_rate,
        fees_enabled=fees_enabled,
    )

    invalid_reason = None
    if avg_entry_price is None or fillable_shares <= ZERO:
        invalid_reason = "no_visible_depth"
    elif fillable_shares < leg.target_size or fillable_shares < Decimal(str(settings.polymarket_structure_min_depth_per_leg)):
        invalid_reason = "insufficient_depth"
    elif min_order_size is not None and fillable_shares < min_order_size:
        invalid_reason = "below_min_order_size"
    elif walk["slippage_bps"] is not None and walk["slippage_bps"] > Decimal(str(settings.polymarket_structure_max_leg_slippage_bps)):
        invalid_reason = "leg_slippage_too_high"

    return {
        "leg_index": leg.leg_index,
        "valid": invalid_reason is None,
        "invalid_reason": invalid_reason,
        "observed_at_local": snapshot_observed,
        "event_ts_exchange": snapshot_event,
        "est_fillable_size": fillable_shares,
        "est_avg_entry_price": avg_entry_price,
        "est_worst_price": walk["worst_price"],
        "est_fee": taker_fee_total,
        "est_slippage_bps": walk["slippage_bps"],
        "details_json": {
            "pricing_asset_id": pricing_asset_id,
            "pricing_condition_id": pricing_condition_id,
            "pricing_asset_dim_id": pricing_asset_dim_id,
            "book_walk": walk["path"],
            "touch_entry_price": touch_entry_price,
            "fees_enabled": fees_enabled,
            "taker_fee_rate": taker_fee_rate,
            "min_order_size": min_order_size,
        },
    }


async def _reprice_generic_leg(
    session: AsyncSession,
    leg: MarketStructureOpportunityLeg,
    *,
    observed_at: datetime,
) -> dict[str, Any]:
    if leg.outcome_id is None:
        return {
            "leg_index": leg.leg_index,
            "valid": False,
            "invalid_reason": "missing_outcome_id",
            "observed_at_local": None,
            "event_ts_exchange": None,
        }

    outcome = await session.get(Outcome, leg.outcome_id)
    if outcome is None:
        return {
            "leg_index": leg.leg_index,
            "valid": False,
            "invalid_reason": "missing_outcome_row",
            "observed_at_local": None,
            "event_ts_exchange": None,
        }

    target_outcome = outcome
    if leg.side == "buy_no":
        sibling_rows = (
            await session.execute(select(Outcome).where(Outcome.market_id == outcome.market_id))
        ).scalars().all()
        no_outcome = next((row for row in sibling_rows if (row.name or "").strip().lower() == "no"), None)
        if no_outcome is None:
            return {
                "leg_index": leg.leg_index,
                "valid": False,
                "invalid_reason": "missing_no_outcome",
                "observed_at_local": None,
                "event_ts_exchange": None,
            }
        target_outcome = no_outcome

    snapshot = (
        await session.execute(
            select(OrderbookSnapshot)
            .where(OrderbookSnapshot.outcome_id == target_outcome.id)
            .order_by(OrderbookSnapshot.captured_at.desc(), OrderbookSnapshot.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if snapshot is None:
        return {
            "leg_index": leg.leg_index,
            "valid": False,
            "invalid_reason": "missing_orderbook_snapshot",
            "observed_at_local": None,
            "event_ts_exchange": None,
        }

    snapshot_time = _ensure_utc(snapshot.captured_at)
    max_staleness = timedelta(seconds=settings.polymarket_structure_cross_venue_max_staleness_seconds)
    if snapshot_time is None or observed_at - snapshot_time > max_staleness:
        return {
            "leg_index": leg.leg_index,
            "valid": False,
            "invalid_reason": "snapshot_stale",
            "observed_at_local": snapshot_time,
            "event_ts_exchange": None,
        }

    asks = _parse_orderbook_levels(snapshot.asks, reverse=False)
    if not asks:
        return {
            "leg_index": leg.leg_index,
            "valid": False,
            "invalid_reason": "no_visible_depth",
            "observed_at_local": snapshot_time,
            "event_ts_exchange": None,
        }

    details = leg.details_json if isinstance(leg.details_json, dict) else {}
    touch_price = asks[0].yes_price
    walk = _walk_levels_for_shares(
        levels=asks,
        target_shares=leg.target_size,
        touch_entry_price=touch_price,
        direct_price_mode=True,
    )
    fillable_shares = walk["fillable_shares"]
    avg_entry_price = walk["avg_entry_price"]
    fee_rate = _to_decimal(details.get("taker_fee_rate")) or ZERO
    fee_total = (((fillable_shares or ZERO) * (avg_entry_price or ZERO)) * fee_rate).quantize(PRICE_Q)
    min_order_size = _to_decimal(details.get("min_order_size"))

    invalid_reason = None
    if avg_entry_price is None or fillable_shares <= ZERO:
        invalid_reason = "no_visible_depth"
    elif fillable_shares < leg.target_size or fillable_shares < Decimal(str(settings.polymarket_structure_min_depth_per_leg)):
        invalid_reason = "insufficient_depth"
    elif min_order_size is not None and fillable_shares < min_order_size:
        invalid_reason = "below_min_order_size"
    elif walk["slippage_bps"] is not None and walk["slippage_bps"] > Decimal(str(settings.polymarket_structure_max_leg_slippage_bps)):
        invalid_reason = "leg_slippage_too_high"

    return {
        "leg_index": leg.leg_index,
        "valid": invalid_reason is None,
        "invalid_reason": invalid_reason,
        "observed_at_local": snapshot_time,
        "event_ts_exchange": None,
        "est_fillable_size": fillable_shares,
        "est_avg_entry_price": avg_entry_price,
        "est_worst_price": walk["worst_price"],
        "est_fee": fee_total,
        "est_slippage_bps": walk["slippage_bps"],
        "details_json": {
            "book_walk": walk["path"],
            "touch_entry_price": touch_price,
            "taker_fee_rate": fee_rate,
            "target_outcome_name": target_outcome.name,
        },
    }


async def reprice_structure_opportunity_leg(
    session: AsyncSession,
    leg: MarketStructureOpportunityLeg,
    *,
    observed_at: datetime,
) -> dict[str, Any]:
    priced = (
        await _reprice_polymarket_leg(session, leg, observed_at=observed_at)
        if leg.venue == "polymarket"
        else await _reprice_generic_leg(session, leg, observed_at=observed_at)
    )
    return {
        "id": leg.id,
        "opportunity_id": leg.opportunity_id,
        "leg_index": leg.leg_index,
        "venue": leg.venue,
        "market_id": leg.market_id,
        "outcome_id": leg.outcome_id,
        "market_dim_id": leg.market_dim_id,
        "asset_dim_id": leg.asset_dim_id,
        "condition_id": leg.condition_id,
        "asset_id": leg.asset_id,
        "side": leg.side,
        "role": leg.role,
        "action_type": leg.action_type,
        "order_type_hint": leg.order_type_hint,
        "target_size": leg.target_size,
        **priced,
    }


def _compute_validation_edges(
    opportunity: MarketStructureOpportunity,
    legs: list[dict[str, Any]],
) -> dict[str, Decimal | None]:
    if not legs:
        return {
            "current_gross_edge_total": None,
            "current_net_edge_total": None,
            "current_gross_edge_bps": None,
            "current_net_edge_bps": None,
        }

    if opportunity.opportunity_type == "neg_risk_direct_vs_basket":
        direct_legs = [leg for leg in legs if leg["role"] == "direct_leg"]
        basket_legs = [leg for leg in legs if leg["role"] == "basket_leg"]
        if len(direct_legs) != 1 or not basket_legs:
            return {
                "current_gross_edge_total": None,
                "current_net_edge_total": None,
                "current_gross_edge_bps": None,
                "current_net_edge_bps": None,
            }
        direct_leg = direct_legs[0]
        direct_gross = (
            ((_to_decimal(direct_leg.get("est_fillable_size")) or ZERO) * (_to_decimal(direct_leg.get("est_avg_entry_price")) or ZERO))
        ).quantize(PRICE_Q)
        direct_net = (direct_gross + (_to_decimal(direct_leg.get("est_fee")) or ZERO)).quantize(PRICE_Q)
        basket_gross = sum(
            ((_to_decimal(leg.get("est_fillable_size")) or ZERO) * (_to_decimal(leg.get("est_avg_entry_price")) or ZERO))
            for leg in basket_legs
        ).quantize(PRICE_Q)
        basket_net = sum(
            (((_to_decimal(leg.get("est_fillable_size")) or ZERO) * (_to_decimal(leg.get("est_avg_entry_price")) or ZERO)) + (_to_decimal(leg.get("est_fee")) or ZERO))
            for leg in basket_legs
        ).quantize(PRICE_Q)
        if direct_net <= basket_net:
            gross_edge_total = (basket_gross - direct_gross).quantize(PRICE_Q)
            net_edge_total = (basket_net - direct_net).quantize(PRICE_Q)
            gross_edge_bps = _edge_bps(gross_edge_total, direct_gross)
            net_edge_bps = _edge_bps(net_edge_total, direct_net)
        else:
            gross_edge_total = (direct_gross - basket_gross).quantize(PRICE_Q)
            net_edge_total = (direct_net - basket_net).quantize(PRICE_Q)
            gross_edge_bps = _edge_bps(gross_edge_total, basket_gross)
            net_edge_bps = _edge_bps(net_edge_total, basket_net)
        return {
            "current_gross_edge_total": gross_edge_total,
            "current_net_edge_total": net_edge_total,
            "current_gross_edge_bps": gross_edge_bps,
            "current_net_edge_bps": net_edge_bps,
        }

    gross_cost = ZERO
    net_cost = ZERO
    for leg in legs:
        entry = _to_decimal(leg.get("est_avg_entry_price"))
        size = _to_decimal(leg.get("est_fillable_size"))
        fee = _to_decimal(leg.get("est_fee")) or ZERO
        if entry is None or size is None:
            return {
                "current_gross_edge_total": None,
                "current_net_edge_total": None,
                "current_gross_edge_bps": None,
                "current_net_edge_bps": None,
            }
        gross_cost += size * entry
        net_cost += (size * entry) + fee
    gross_cost = gross_cost.quantize(PRICE_Q)
    net_cost = net_cost.quantize(PRICE_Q)
    gross_edge_total = (ONE - gross_cost).quantize(PRICE_Q)
    net_edge_total = (ONE - net_cost).quantize(PRICE_Q)
    return {
        "current_gross_edge_total": gross_edge_total,
        "current_net_edge_total": net_edge_total,
        "current_gross_edge_bps": _edge_bps(gross_edge_total, gross_cost),
        "current_net_edge_bps": _edge_bps(net_edge_total, net_cost),
    }


async def evaluate_market_structure_opportunity(
    session: AsyncSession,
    *,
    opportunity: MarketStructureOpportunity,
    run_id: uuid.UUID | None = None,
    evaluation_kind: str = "follow_up",
) -> MarketStructureValidation:
    group = await session.get(MarketStructureGroup, opportunity.group_id)
    legs = (
        await session.execute(
            select(MarketStructureOpportunityLeg)
            .where(MarketStructureOpportunityLeg.opportunity_id == opportunity.id)
            .order_by(MarketStructureOpportunityLeg.leg_index.asc())
        )
    ).scalars().all()
    observed_at = _utcnow()
    repriced_legs = [await reprice_structure_opportunity_leg(session, leg, observed_at=observed_at) for leg in legs]
    edge_data = _compute_validation_edges(opportunity, repriced_legs)
    link = await _load_cross_venue_link_for_group(session, group) if group is not None and group.group_type == "cross_venue_basis" else None

    detected_at = _ensure_utc(opportunity.observed_at_local) or observed_at
    detected_age_seconds = max(0, int((observed_at - detected_at).total_seconds()))
    max_leg_age_seconds = 0
    stale_leg_count = 0
    executable_leg_count = 0
    total_leg_count = len(repriced_legs)
    reason_codes: list[str] = []
    leg_summaries: list[dict[str, Any]] = []
    staleness_limit = settings.polymarket_structure_cross_venue_max_staleness_seconds

    for priced_leg in repriced_legs:
        leg_observed_at = _ensure_utc(priced_leg.get("observed_at_local"))
        leg_age_seconds = (
            max(0, int((observed_at - leg_observed_at).total_seconds()))
            if leg_observed_at is not None
            else None
        )
        if leg_age_seconds is not None:
            max_leg_age_seconds = max(max_leg_age_seconds, leg_age_seconds)
        if leg_age_seconds is None or leg_age_seconds > staleness_limit:
            stale_leg_count += 1
            reason_codes.append("stale_leg_book")
        if priced_leg.get("valid"):
            executable_leg_count += 1
        mapped_reason = _reason_code_from_invalid_reason(priced_leg.get("invalid_reason"))
        if mapped_reason is not None:
            reason_codes.append(mapped_reason)
        leg_summaries.append(
            {
                **_json_safe(priced_leg),
                "age_seconds": leg_age_seconds,
                "stale": leg_age_seconds is None or leg_age_seconds > staleness_limit,
                "reason_code": mapped_reason,
            }
        )

    if group is None or not group.actionable:
        reason_codes.append("non_actionable_group")
    if opportunity.opportunity_type == "event_sum_parity" and not (group.actionable if group is not None else False):
        reason_codes.append("incomplete_parity_composition")
    if link is not None:
        effective_status = _effective_review_status(link)
        if not link.active or effective_status == "disabled":
            reason_codes.append("cross_venue_link_inactive")
        if effective_status == "expired":
            reason_codes.append("cross_venue_link_expired")
        if settings.polymarket_structure_link_review_required and effective_status != "approved":
            reason_codes.append("cross_venue_review_required")

    current_net_edge_total = edge_data["current_net_edge_total"]
    current_net_edge_bps = edge_data["current_net_edge_bps"]
    if current_net_edge_total is None:
        reason_codes.append("missing_current_executable_estimate")
    else:
        if current_net_edge_total <= ZERO:
            reason_codes.append("no_positive_current_edge")
        elif (current_net_edge_bps or ZERO) < Decimal(str(settings.polymarket_structure_min_net_edge_bps)):
            reason_codes.append("edge_decayed_below_threshold")
        plan_notional_total = _plan_notional_total(repriced_legs)
        if (
            plan_notional_total is not None
            and plan_notional_total > Decimal(str(settings.polymarket_structure_max_notional_per_plan))
        ):
            reason_codes.append("plan_notional_cap_exceeded")

    reason_codes = _normalize_reason_codes(reason_codes)
    classification = VALIDATION_EXECUTABLE
    if any(code in HARD_BLOCK_REASONS for code in reason_codes):
        classification = VALIDATION_BLOCKED
    elif any(code in {"no_positive_current_edge", "edge_decayed_below_threshold"} for code in reason_codes):
        classification = VALIDATION_INFORMATIONAL

    confidence = opportunity.confidence or ZERO
    if classification == VALIDATION_INFORMATIONAL:
        confidence = min(confidence, Decimal("0.500000"))
    elif classification == VALIDATION_BLOCKED:
        confidence = ZERO

    summary_json = {
        "opportunity_type": opportunity.opportunity_type,
        "edge_bucket": _edge_bucket(current_net_edge_bps),
        "reason_labels": {code: REASON_LABELS.get(code, code) for code in reason_codes},
        "legs": leg_summaries,
        "link": serialize_cross_venue_link(link),
        "staleness_limit_seconds": staleness_limit,
    }

    validation = MarketStructureValidation(
        opportunity_id=opportunity.id,
        run_id=run_id,
        evaluation_kind=evaluation_kind,
        classification=classification,
        reason_codes_json=reason_codes,
        confidence=confidence,
        detected_gross_edge_bps=opportunity.gross_edge_bps,
        detected_net_edge_bps=opportunity.net_edge_bps,
        detected_gross_edge_total=opportunity.gross_edge_total,
        detected_net_edge_total=opportunity.net_edge_total,
        current_gross_edge_bps=edge_data["current_gross_edge_bps"],
        current_net_edge_bps=edge_data["current_net_edge_bps"],
        current_gross_edge_total=edge_data["current_gross_edge_total"],
        current_net_edge_total=edge_data["current_net_edge_total"],
        gross_edge_decay_total=(
            (opportunity.gross_edge_total - edge_data["current_gross_edge_total"]).quantize(PRICE_Q)
            if opportunity.gross_edge_total is not None and edge_data["current_gross_edge_total"] is not None
            else None
        ),
        net_edge_decay_total=(
            (opportunity.net_edge_total - edge_data["current_net_edge_total"]).quantize(PRICE_Q)
            if opportunity.net_edge_total is not None and edge_data["current_net_edge_total"] is not None
            else None
        ),
        detected_age_seconds=detected_age_seconds,
        max_leg_age_seconds=max_leg_age_seconds if total_leg_count else None,
        stale_leg_count=stale_leg_count,
        executable_leg_count=executable_leg_count,
        total_leg_count=total_leg_count,
        summary_json=_json_safe(summary_json),
    )
    session.add(validation)
    await session.flush()

    polymarket_structure_validation_results.labels(classification=classification).inc()
    for code in reason_codes:
        polymarket_structure_validation_reason_codes.labels(
            classification=classification,
            reason_code=code,
        ).inc()
    return validation


async def list_market_structure_validations(
    session: AsyncSession,
    *,
    opportunity_id: int | None = None,
    classification: str | None = None,
    evaluation_kind: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query = select(MarketStructureValidation)
    if opportunity_id is not None:
        query = query.where(MarketStructureValidation.opportunity_id == opportunity_id)
    if classification:
        query = query.where(MarketStructureValidation.classification == classification)
    if evaluation_kind:
        query = query.where(MarketStructureValidation.evaluation_kind == evaluation_kind)
    rows = (
        await session.execute(
            query.order_by(MarketStructureValidation.created_at.desc(), MarketStructureValidation.id.desc()).limit(limit)
        )
    ).scalars().all()
    return [serialize_structure_validation(row) for row in rows]


async def get_latest_structure_validations(
    session: AsyncSession,
    opportunity_ids: list[int],
) -> dict[int, MarketStructureValidation]:
    if not opportunity_ids:
        return {}
    rows = (
        await session.execute(
            select(MarketStructureValidation)
            .where(MarketStructureValidation.opportunity_id.in_(opportunity_ids))
            .order_by(MarketStructureValidation.opportunity_id.asc(), MarketStructureValidation.created_at.desc(), MarketStructureValidation.id.desc())
        )
    ).scalars().all()
    latest: dict[int, MarketStructureValidation] = {}
    for row in rows:
        latest.setdefault(row.opportunity_id, row)
    return latest


async def _get_plan_orders(
    session: AsyncSession,
    plan_ids: list[uuid.UUID],
) -> dict[uuid.UUID, list[MarketStructurePaperOrder]]:
    if not plan_ids:
        return {}
    rows = (
        await session.execute(
            select(MarketStructurePaperOrder)
            .where(MarketStructurePaperOrder.plan_id.in_(plan_ids))
            .order_by(MarketStructurePaperOrder.plan_id.asc(), MarketStructurePaperOrder.leg_index.asc())
        )
    ).scalars().all()
    results: dict[uuid.UUID, list[MarketStructurePaperOrder]] = {}
    for row in rows:
        results.setdefault(row.plan_id, []).append(row)
    return results


async def _get_plan_events(
    session: AsyncSession,
    plan_ids: list[uuid.UUID],
) -> dict[uuid.UUID, list[MarketStructurePaperOrderEvent]]:
    if not plan_ids:
        return {}
    rows = (
        await session.execute(
            select(MarketStructurePaperOrderEvent)
            .where(MarketStructurePaperOrderEvent.plan_id.in_(plan_ids))
            .order_by(MarketStructurePaperOrderEvent.plan_id.asc(), MarketStructurePaperOrderEvent.observed_at.desc(), MarketStructurePaperOrderEvent.id.desc())
        )
    ).scalars().all()
    results: dict[uuid.UUID, list[MarketStructurePaperOrderEvent]] = {}
    for row in rows:
        results.setdefault(row.plan_id, []).append(row)
    return results


async def get_latest_structure_plans(
    session: AsyncSession,
    opportunity_ids: list[int],
) -> dict[int, MarketStructurePaperPlan]:
    if not opportunity_ids:
        return {}
    rows = (
        await session.execute(
            select(MarketStructurePaperPlan)
            .where(MarketStructurePaperPlan.opportunity_id.in_(opportunity_ids))
            .order_by(MarketStructurePaperPlan.opportunity_id.asc(), MarketStructurePaperPlan.created_at.desc())
        )
    ).scalars().all()
    latest: dict[int, MarketStructurePaperPlan] = {}
    for row in rows:
        current = latest.get(row.opportunity_id)
        if current is None:
            latest[row.opportunity_id] = row
            continue
        if current.status not in ACTIVE_PLAN_STATUSES and row.status in ACTIVE_PLAN_STATUSES:
            latest[row.opportunity_id] = row
    return latest


async def list_market_structure_paper_plans(
    session: AsyncSession,
    *,
    opportunity_id: int | None = None,
    status: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query = select(MarketStructurePaperPlan)
    if opportunity_id is not None:
        query = query.where(MarketStructurePaperPlan.opportunity_id == opportunity_id)
    if status:
        query = query.where(MarketStructurePaperPlan.status == status)
    rows = (
        await session.execute(
            query.order_by(MarketStructurePaperPlan.created_at.desc(), MarketStructurePaperPlan.id.desc()).limit(limit)
        )
    ).scalars().all()
    return [serialize_structure_paper_plan(row) for row in rows]


async def get_market_structure_paper_plan_detail(
    session: AsyncSession,
    *,
    plan_id: uuid.UUID,
) -> dict[str, Any] | None:
    plan = await session.get(MarketStructurePaperPlan, plan_id)
    if plan is None:
        return None
    orders = await _get_plan_orders(session, [plan.id])
    events = await _get_plan_events(session, [plan.id])
    opportunity = await session.get(MarketStructureOpportunity, plan.opportunity_id)
    validation = await session.get(MarketStructureValidation, plan.validation_id) if plan.validation_id is not None else None
    return {
        **serialize_structure_paper_plan(plan),
        "opportunity": None if opportunity is None else {
            "id": opportunity.id,
            "group_id": opportunity.group_id,
            "opportunity_type": opportunity.opportunity_type,
            "observed_at_local": opportunity.observed_at_local,
            "net_edge_bps": _serialize_decimal(opportunity.net_edge_bps),
            "net_edge_total": _serialize_decimal(opportunity.net_edge_total),
        },
        "validation": serialize_structure_validation(validation) if validation is not None else None,
        "orders": [serialize_structure_paper_order(order) for order in orders.get(plan.id, [])],
        "events": [serialize_structure_paper_order_event(event) for event in events.get(plan.id, [])],
    }


async def add_plan_event(
    session: AsyncSession,
    *,
    plan_id: uuid.UUID,
    event_type: str,
    status: str | None,
    message: str | None,
    details_json: dict[str, Any] | None = None,
    paper_order_id: int | None = None,
) -> MarketStructurePaperOrderEvent:
    event = MarketStructurePaperOrderEvent(
        plan_id=plan_id,
        paper_order_id=paper_order_id,
        event_type=event_type,
        status=status,
        message=message,
        details_json=_json_safe(details_json or {}),
    )
    session.add(event)
    await session.flush()
    return event


async def create_market_structure_paper_plan(
    session: AsyncSession,
    *,
    opportunity_id: int,
    validation_id: int | None = None,
    actor: str = "manual",
    auto_created: bool = False,
    run_id: uuid.UUID | None = None,
) -> MarketStructurePaperPlan:
    existing_active = (
        await session.execute(
            select(MarketStructurePaperPlan)
            .where(
                MarketStructurePaperPlan.opportunity_id == opportunity_id,
                MarketStructurePaperPlan.status.in_(tuple(ACTIVE_PLAN_STATUSES)),
            )
            .order_by(MarketStructurePaperPlan.created_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if existing_active is not None:
        return existing_active

    opportunity = await session.get(MarketStructureOpportunity, opportunity_id)
    if opportunity is None:
        raise ValueError(f"Unknown structure opportunity {opportunity_id}")

    validation = await session.get(MarketStructureValidation, validation_id) if validation_id is not None else None
    if validation is None:
        validation = (
            await session.execute(
                select(MarketStructureValidation)
                .where(MarketStructureValidation.opportunity_id == opportunity_id)
                .order_by(MarketStructureValidation.created_at.desc(), MarketStructureValidation.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
    if validation is None:
        validation = await evaluate_market_structure_opportunity(
            session,
            opportunity=opportunity,
            run_id=run_id,
            evaluation_kind="follow_up",
        )

    opportunity_legs = (
        await session.execute(
            select(MarketStructureOpportunityLeg)
            .where(MarketStructureOpportunityLeg.opportunity_id == opportunity_id)
            .order_by(MarketStructureOpportunityLeg.leg_index.asc())
        )
    ).scalars().all()

    validation_reasons = validation.reason_codes_json if isinstance(validation.reason_codes_json, list) else []
    validation_summary = validation.summary_json if isinstance(validation.summary_json, dict) else {}
    priced_legs = validation_summary.get("legs") if isinstance(validation_summary.get("legs"), list) else []
    plan_notional_total = _plan_notional_total(priced_legs)
    status = "routing_pending"
    if validation.classification != VALIDATION_EXECUTABLE:
        status = "blocked"
    elif settings.polymarket_structure_paper_require_manual_approval:
        status = "approval_pending"

    group = await session.get(MarketStructureGroup, opportunity.group_id)
    plan = MarketStructurePaperPlan(
        opportunity_id=opportunity_id,
        validation_id=validation.id,
        run_id=run_id,
        status=status,
        auto_created=auto_created,
        manual_approval_required=settings.polymarket_structure_paper_require_manual_approval,
        package_size=opportunity.package_size,
        plan_notional_total=plan_notional_total,
        reason_codes_json=validation_reasons,
        summary_json=_json_safe(
            {
                "opportunity_type": opportunity.opportunity_type,
                "market_question": group.title if group is not None else opportunity.opportunity_type,
                "edge_bucket": _edge_bucket(validation.current_net_edge_bps),
                "created_by": actor,
            }
        ),
        details_json=_json_safe(
            {
                "validation_summary": validation.summary_json,
            }
        ),
    )
    session.add(plan)
    await session.flush()

    for leg in opportunity_legs:
        priced_leg = next((row for row in priced_legs if row.get("leg_index") == leg.leg_index), {})
        planned_entry_price = _to_decimal(priced_leg.get("est_avg_entry_price")) or leg.est_avg_entry_price
        planned_notional = None
        if planned_entry_price is not None:
            planned_notional = (
                (leg.target_size * planned_entry_price) + (_to_decimal(priced_leg.get("est_fee")) or ZERO)
            ).quantize(PRICE_Q)
        session.add(
            MarketStructurePaperOrder(
                plan_id=plan.id,
                opportunity_leg_id=leg.id,
                leg_index=leg.leg_index,
                venue=leg.venue,
                market_id=leg.market_id,
                outcome_id=leg.outcome_id,
                condition_id=leg.condition_id,
                asset_id=leg.asset_id,
                side=leg.side,
                role=leg.role,
                action_type=leg.action_type,
                order_type_hint=leg.order_type_hint,
                target_size=leg.target_size,
                planned_entry_price=planned_entry_price,
                planned_notional=planned_notional,
                status="planned" if status != "blocked" else "blocked",
                error_reason=None if status != "blocked" else "plan_blocked_at_creation",
                details_json=_json_safe({"pricing_snapshot": priced_leg}),
            )
        )
    await session.flush()

    await add_plan_event(
        session,
        plan_id=plan.id,
        event_type="plan_created",
        status=plan.status,
        message=f"Structure paper plan created by {actor}",
        details_json={"auto_created": auto_created, "reason_codes": validation_reasons},
    )
    polymarket_structure_paper_plans.labels(status=plan.status).inc()
    return plan


async def approve_market_structure_paper_plan(
    session: AsyncSession,
    *,
    plan_id: uuid.UUID,
    actor: str,
) -> MarketStructurePaperPlan:
    plan = await session.get(MarketStructurePaperPlan, plan_id)
    if plan is None:
        raise ValueError(f"Unknown structure paper plan {plan_id}")
    if plan.status != "approval_pending":
        return plan
    plan.status = "routing_pending"
    plan.approved_by = actor
    plan.approved_at = _utcnow()
    await session.flush()
    await add_plan_event(
        session,
        plan_id=plan.id,
        event_type="plan_approved",
        status=plan.status,
        message=f"Plan approved by {actor}",
        details_json={},
    )
    return plan


async def reject_market_structure_paper_plan(
    session: AsyncSession,
    *,
    plan_id: uuid.UUID,
    actor: str,
    reason: str | None = None,
) -> MarketStructurePaperPlan:
    plan = await session.get(MarketStructurePaperPlan, plan_id)
    if plan is None:
        raise ValueError(f"Unknown structure paper plan {plan_id}")
    plan.status = "rejected"
    plan.rejected_by = actor
    plan.rejected_at = _utcnow()
    plan.rejection_reason = reason
    await session.flush()
    await add_plan_event(
        session,
        plan_id=plan.id,
        event_type="plan_rejected",
        status=plan.status,
        message=f"Plan rejected by {actor}",
        details_json={"reason": reason},
    )
    return plan


async def _list_open_structure_exposure_positions(
    session: AsyncSession,
    *,
    exclude_plan_id: uuid.UUID | None = None,
) -> list[dict[str, Any]]:
    plan_rows = (
        await session.execute(
            select(MarketStructurePaperPlan)
            .where(MarketStructurePaperPlan.status.in_(tuple(OPEN_EXPOSURE_PLAN_STATUSES)))
            .order_by(MarketStructurePaperPlan.created_at.desc())
        )
    ).scalars().all()
    positions: list[dict[str, Any]] = []
    for row in plan_rows:
        if exclude_plan_id is not None and row.id == exclude_plan_id:
            continue
        summary = row.summary_json if isinstance(row.summary_json, dict) else {}
        positions.append(
            {
                "size_usd": row.plan_notional_total or ZERO,
                "market_question": summary.get("market_question", "structure_plan"),
                "outcome_id": str(row.id),
            }
        )
    trade_rows = (
        await session.execute(
            select(PaperTrade).where(PaperTrade.status.in_(tuple(OPEN_PAPER_TRADE_STATUSES)))
        )
    ).scalars().all()
    for trade in trade_rows:
        details = trade.details if isinstance(trade.details, dict) else {}
        positions.append(
            {
                "size_usd": trade.size_usd,
                "market_question": details.get("market_question", ""),
                "outcome_id": str(trade.outcome_id),
            }
        )
    return positions


async def _structure_risk_check(
    session: AsyncSession,
    *,
    plan: MarketStructurePaperPlan,
    exclude_plan_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    open_positions = await _list_open_structure_exposure_positions(session, exclude_plan_id=exclude_plan_id)
    bankroll = Decimal(str(settings.default_bankroll))
    resolved_trades = (
        await session.execute(
            select(PaperTrade).where(PaperTrade.status == "resolved")
        )
    ).scalars().all()
    cumulative_pnl = sum((trade.pnl or ZERO) for trade in resolved_trades)
    peak_bankroll = bankroll + (cumulative_pnl if cumulative_pnl > ZERO else ZERO)
    summary = plan.summary_json if isinstance(plan.summary_json, dict) else {}
    return check_exposure(
        open_positions=open_positions,
        new_trade={
            "size_usd": plan.plan_notional_total or ZERO,
            "market_question": summary.get("market_question", "structure_plan"),
            "outcome_id": str(plan.id),
        },
        bankroll=bankroll,
        max_total_pct=Decimal(str(settings.max_total_exposure_pct)),
        max_cluster_pct=Decimal(str(settings.max_cluster_exposure_pct)),
        drawdown_breaker_pct=Decimal(str(settings.drawdown_circuit_breaker_pct)),
        peak_bankroll=peak_bankroll,
        cumulative_pnl=cumulative_pnl,
    )


async def route_market_structure_paper_plan(
    session: AsyncSession,
    *,
    plan_id: uuid.UUID,
    actor: str,
    run_id: uuid.UUID | None = None,
    simulate_failure_leg_index: int | None = None,
) -> MarketStructurePaperPlan:
    plan = await session.get(MarketStructurePaperPlan, plan_id)
    if plan is None:
        raise ValueError(f"Unknown structure paper plan {plan_id}")
    if plan.status not in {"routing_pending", "approval_pending", "partial_failed"}:
        return plan
    if plan.status == "approval_pending" and plan.manual_approval_required:
        return plan
    if plan.created_at is not None:
        age_seconds = int((_utcnow() - _ensure_utc(plan.created_at)).total_seconds())
        if age_seconds > settings.polymarket_structure_plan_max_age_seconds:
            plan.status = "blocked"
            plan.reason_codes_json = _normalize_reason_codes(
                [*list(plan.reason_codes_json or []), "plan_too_old"]
            )
            await session.flush()
            await add_plan_event(
                session,
                plan_id=plan.id,
                event_type="plan_blocked",
                status=plan.status,
                message="Plan exceeded max age before routing",
                details_json={"age_seconds": age_seconds},
            )
            polymarket_structure_paper_route_attempts.labels(status="blocked").inc()
            return plan

    opportunity = await session.get(MarketStructureOpportunity, plan.opportunity_id)
    if opportunity is None:
        plan.status = "blocked"
        await session.flush()
        await add_plan_event(
            session,
            plan_id=plan.id,
            event_type="plan_blocked",
            status=plan.status,
            message="Opportunity missing at route time",
            details_json={},
        )
        polymarket_structure_paper_route_attempts.labels(status="blocked").inc()
        return plan

    validation = await evaluate_market_structure_opportunity(
        session,
        opportunity=opportunity,
        run_id=run_id,
        evaluation_kind="routing_precheck",
    )
    if validation.classification != VALIDATION_EXECUTABLE:
        plan.status = "blocked"
        plan.validation_id = validation.id
        plan.reason_codes_json = validation.reason_codes_json
        await session.flush()
        orders = (
            await session.execute(
                select(MarketStructurePaperOrder)
                .where(MarketStructurePaperOrder.plan_id == plan.id)
                .order_by(MarketStructurePaperOrder.leg_index.asc())
            )
        ).scalars().all()
        for order in orders:
            if order.status == "filled":
                continue
            order.status = "blocked"
            order.error_reason = "routing_precheck_blocked"
        await session.flush()
        await add_plan_event(
            session,
            plan_id=plan.id,
            event_type="plan_blocked",
            status=plan.status,
            message="Routing precheck blocked structure plan",
            details_json={"reason_codes": validation.reason_codes_json},
        )
        polymarket_structure_paper_route_attempts.labels(status="blocked").inc()
        return plan

    risk_result = await _structure_risk_check(session, plan=plan, exclude_plan_id=plan.id)
    if not risk_result["approved"]:
        plan.status = "blocked"
        plan.reason_codes_json = _normalize_reason_codes(
            [*list(plan.reason_codes_json or []), "exposure_limit_blocked"]
        )
        await session.flush()
        await add_plan_event(
            session,
            plan_id=plan.id,
            event_type="plan_blocked",
            status=plan.status,
            message="Exposure check blocked structure plan",
            details_json={"risk_result": _json_safe(risk_result)},
        )
        polymarket_structure_paper_route_attempts.labels(status="blocked").inc()
        return plan

    plan.validation_id = validation.id
    orders = (
        await session.execute(
            select(MarketStructurePaperOrder)
            .where(MarketStructurePaperOrder.plan_id == plan.id)
            .order_by(MarketStructurePaperOrder.leg_index.asc())
        )
    ).scalars().all()
    validation_summary = validation.summary_json if isinstance(validation.summary_json, dict) else {}
    priced_legs = validation_summary.get("legs") if isinstance(validation_summary.get("legs"), list) else []
    filled_count = 0
    failed_count = 0
    for order in orders:
        priced_leg = next((row for row in priced_legs if row.get("leg_index") == order.leg_index), None)
        if priced_leg is None:
            order.status = "blocked"
            order.error_reason = "missing_pricing_snapshot"
            failed_count += 1
            await session.flush()
            await add_plan_event(
                session,
                plan_id=plan.id,
                paper_order_id=order.id,
                event_type="order_blocked",
                status=order.status,
                message="Missing pricing snapshot at route time",
                details_json={},
            )
            continue
        if simulate_failure_leg_index is not None and order.leg_index == simulate_failure_leg_index:
            order.status = "failed"
            order.error_reason = "simulated_route_failure"
            failed_count += 1
            await session.flush()
            await add_plan_event(
                session,
                plan_id=plan.id,
                paper_order_id=order.id,
                event_type="order_failed",
                status=order.status,
                message="Simulated route failure",
                details_json={},
            )
            break
        order.status = "filled"
        order.filled_size = order.target_size
        order.avg_fill_price = _to_decimal(priced_leg.get("est_avg_entry_price"))
        order.fill_notional = (
            ((order.avg_fill_price or ZERO) * order.filled_size) + (_to_decimal(priced_leg.get("est_fee")) or ZERO)
        ).quantize(PRICE_Q)
        order.details_json = _json_safe(
            {
                **(order.details_json if isinstance(order.details_json, dict) else {}),
                "routed_by": actor,
                "validation_leg": priced_leg,
            }
        )
        filled_count += 1
        await session.flush()
        await add_plan_event(
            session,
            plan_id=plan.id,
            paper_order_id=order.id,
            event_type="order_filled",
            status=order.status,
            message=f"Paper routed leg {order.leg_index}",
            details_json={"fill_price": order.avg_fill_price, "fill_notional": order.fill_notional},
        )

    if failed_count and filled_count:
        plan.status = "partial_failed"
    elif failed_count and not filled_count:
        plan.status = "blocked"
    else:
        plan.status = "routed"
        plan.routed_at = _utcnow()
    await session.flush()
    await add_plan_event(
        session,
        plan_id=plan.id,
        event_type="plan_routed" if plan.status == "routed" else "plan_partial_failed",
        status=plan.status,
        message=f"Structure plan processed by {actor}",
        details_json={"filled_count": filled_count, "failed_count": failed_count},
    )
    polymarket_structure_paper_route_attempts.labels(status=plan.status).inc()
    return plan


async def get_market_structure_opportunity_detail(
    session: AsyncSession,
    *,
    opportunity_id: int,
) -> dict[str, Any] | None:
    opportunity = await session.get(MarketStructureOpportunity, opportunity_id)
    if opportunity is None:
        return None
    group = await session.get(MarketStructureGroup, opportunity.group_id)
    members = []
    if group is not None:
        members = (
            await session.execute(
                select(MarketStructureGroupMember)
                .where(MarketStructureGroupMember.group_id == group.id)
                .order_by(MarketStructureGroupMember.id.asc())
            )
        ).scalars().all()
    legs = (
        await session.execute(
            select(MarketStructureOpportunityLeg)
            .where(MarketStructureOpportunityLeg.opportunity_id == opportunity.id)
            .order_by(MarketStructureOpportunityLeg.leg_index.asc())
        )
    ).scalars().all()
    validations = (
        await session.execute(
            select(MarketStructureValidation)
            .where(MarketStructureValidation.opportunity_id == opportunity.id)
            .order_by(MarketStructureValidation.created_at.desc(), MarketStructureValidation.id.desc())
            .limit(10)
        )
    ).scalars().all()
    plans = (
        await session.execute(
            select(MarketStructurePaperPlan)
            .where(MarketStructurePaperPlan.opportunity_id == opportunity.id)
            .order_by(MarketStructurePaperPlan.created_at.desc())
            .limit(10)
        )
    ).scalars().all()
    plan_orders = await _get_plan_orders(session, [plan.id for plan in plans])
    plan_events = await _get_plan_events(session, [plan.id for plan in plans])
    run = await session.get(MarketStructureRun, opportunity.run_id)
    link = await _load_cross_venue_link_for_group(session, group) if group is not None else None

    return {
        "opportunity": {
            "id": opportunity.id,
            "run_id": opportunity.run_id,
            "group_id": opportunity.group_id,
            "opportunity_type": opportunity.opportunity_type,
            "anchor_condition_id": opportunity.anchor_condition_id,
            "anchor_asset_id": opportunity.anchor_asset_id,
            "event_ts_exchange": opportunity.event_ts_exchange,
            "observed_at_local": opportunity.observed_at_local,
            "pricing_method": opportunity.pricing_method,
            "gross_edge_bps": _serialize_decimal(opportunity.gross_edge_bps),
            "net_edge_bps": _serialize_decimal(opportunity.net_edge_bps),
            "gross_edge_total": _serialize_decimal(opportunity.gross_edge_total),
            "net_edge_total": _serialize_decimal(opportunity.net_edge_total),
            "package_size": _serialize_decimal(opportunity.package_size),
            "executable": opportunity.executable,
            "executable_all_legs": opportunity.executable_all_legs,
            "actionable": opportunity.actionable,
            "confidence": _serialize_decimal(opportunity.confidence),
            "invalid_reason": opportunity.invalid_reason,
            "details_json": opportunity.details_json,
            "edge_bucket": _edge_bucket(opportunity.net_edge_bps),
            "created_at": opportunity.created_at,
            "updated_at": opportunity.updated_at,
        },
        "group": None if group is None else {
            "id": group.id,
            "group_key": group.group_key,
            "group_type": group.group_type,
            "primary_venue": group.primary_venue,
            "title": group.title,
            "event_slug": group.event_slug,
            "active": group.active,
            "actionable": group.actionable,
            "details_json": group.details_json,
            "created_at": group.created_at,
            "updated_at": group.updated_at,
        },
        "members": [
            {
                "id": row.id,
                "group_id": row.group_id,
                "member_key": row.member_key,
                "venue": row.venue,
                "market_dim_id": row.market_dim_id,
                "asset_dim_id": row.asset_dim_id,
                "market_id": row.market_id,
                "outcome_id": row.outcome_id,
                "condition_id": row.condition_id,
                "asset_id": row.asset_id,
                "outcome_name": row.outcome_name,
                "member_role": row.member_role,
                "active": row.active,
                "actionable": row.actionable,
                "details_json": row.details_json,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            }
            for row in members
        ],
        "legs": [
            {
                "id": row.id,
                "opportunity_id": row.opportunity_id,
                "leg_index": row.leg_index,
                "venue": row.venue,
                "market_id": row.market_id,
                "outcome_id": row.outcome_id,
                "market_dim_id": row.market_dim_id,
                "asset_dim_id": row.asset_dim_id,
                "condition_id": row.condition_id,
                "asset_id": row.asset_id,
                "side": row.side,
                "role": row.role,
                "action_type": row.action_type,
                "order_type_hint": row.order_type_hint,
                "target_size": _serialize_decimal(row.target_size),
                "est_fillable_size": _serialize_decimal(row.est_fillable_size),
                "est_avg_entry_price": _serialize_decimal(row.est_avg_entry_price),
                "est_worst_price": _serialize_decimal(row.est_worst_price),
                "est_fee": _serialize_decimal(row.est_fee),
                "est_slippage_bps": _serialize_decimal(row.est_slippage_bps),
                "valid": row.valid,
                "invalid_reason": row.invalid_reason,
                "details_json": row.details_json,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            }
            for row in legs
        ],
        "run": None if run is None else {
            "id": run.id,
            "run_type": run.run_type,
            "reason": run.reason,
            "started_at": run.started_at,
            "completed_at": run.completed_at,
            "status": run.status,
            "details_json": run.details_json,
            "rows_inserted_json": run.rows_inserted_json,
        },
        "latest_validation": serialize_structure_validation(validations[0]) if validations else None,
        "validations": [serialize_structure_validation(row) for row in validations],
        "cross_venue_link": serialize_cross_venue_link(link),
        "paper_plans": [
            {
                **serialize_structure_paper_plan(plan),
                "orders": [serialize_structure_paper_order(order) for order in plan_orders.get(plan.id, [])],
                "events": [serialize_structure_paper_order_event(event) for event in plan_events.get(plan.id, [])],
            }
            for plan in plans
        ],
    }


async def lookup_market_structure_opportunities_with_validation(
    session: AsyncSession,
    *,
    group_type: str | None = None,
    opportunity_type: str | None = None,
    event_slug: str | None = None,
    condition_id: str | None = None,
    asset_id: str | None = None,
    venue: str | None = None,
    actionable: bool | None = None,
    classification: str | None = None,
    reason_code: str | None = None,
    edge_bucket: str | None = None,
    plan_status: str | None = None,
    review_status: str | None = None,
    confidence_min: float | None = None,
    executable_only: bool | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query = select(MarketStructureOpportunity).distinct()
    if group_type or event_slug:
        query = query.join(MarketStructureGroup, MarketStructureGroup.id == MarketStructureOpportunity.group_id)
    if condition_id or asset_id or venue:
        query = query.join(MarketStructureOpportunityLeg, MarketStructureOpportunityLeg.opportunity_id == MarketStructureOpportunity.id)
    if group_type:
        query = query.where(MarketStructureGroup.group_type == group_type)
    if opportunity_type:
        query = query.where(MarketStructureOpportunity.opportunity_type == opportunity_type)
    if event_slug:
        query = query.where(MarketStructureGroup.event_slug == event_slug)
    if condition_id:
        query = query.where(MarketStructureOpportunityLeg.condition_id == condition_id)
    if asset_id:
        query = query.where(MarketStructureOpportunityLeg.asset_id == asset_id)
    if venue:
        query = query.where(MarketStructureOpportunityLeg.venue == venue)
    if actionable is not None:
        query = query.where(MarketStructureOpportunity.actionable.is_(actionable))
    rows = (
        await session.execute(
            query.order_by(MarketStructureOpportunity.observed_at_local.desc(), MarketStructureOpportunity.id.desc()).limit(max(limit * 4, limit))
        )
    ).scalars().all()
    latest_validations = await get_latest_structure_validations(session, [row.id for row in rows])
    latest_plans = await get_latest_structure_plans(session, [row.id for row in rows])
    groups = (
        {
            row.id: row
            for row in (
                await session.execute(
                    select(MarketStructureGroup).where(MarketStructureGroup.id.in_([op.group_id for op in rows]))
                )
            ).scalars().all()
        }
        if rows
        else {}
    )
    link_rows: dict[int, CrossVenueMarketLink] = {}
    for row in rows:
        group = groups.get(row.group_id)
        if group is not None and group.group_type == "cross_venue_basis":
            link = await _load_cross_venue_link_for_group(session, group)
            if link is not None:
                link_rows[row.id] = link

    results: list[dict[str, Any]] = []
    for row in rows:
        validation = latest_validations.get(row.id)
        plan = latest_plans.get(row.id)
        link = link_rows.get(row.id)
        validation_reason_codes = validation.reason_codes_json if isinstance(validation, MarketStructureValidation) and isinstance(validation.reason_codes_json, list) else []
        current_edge_bucket = _edge_bucket(validation.current_net_edge_bps if validation is not None else row.net_edge_bps)
        effective_link_status = _effective_review_status(link)
        link_confidence = link.confidence if link is not None else None
        detail = {
            "id": row.id,
            "run_id": row.run_id,
            "group_id": row.group_id,
            "group_type": groups.get(row.group_id).group_type if groups.get(row.group_id) is not None else None,
            "group_title": groups.get(row.group_id).title if groups.get(row.group_id) is not None else None,
            "event_slug": groups.get(row.group_id).event_slug if groups.get(row.group_id) is not None else None,
            "opportunity_type": row.opportunity_type,
            "anchor_condition_id": row.anchor_condition_id,
            "anchor_asset_id": row.anchor_asset_id,
            "event_ts_exchange": row.event_ts_exchange,
            "observed_at_local": row.observed_at_local,
            "pricing_method": row.pricing_method,
            "gross_edge_bps": _serialize_decimal(row.gross_edge_bps),
            "net_edge_bps": _serialize_decimal(row.net_edge_bps),
            "gross_edge_total": _serialize_decimal(row.gross_edge_total),
            "net_edge_total": _serialize_decimal(row.net_edge_total),
            "package_size": _serialize_decimal(row.package_size),
            "executable": row.executable,
            "executable_all_legs": row.executable_all_legs,
            "actionable": row.actionable,
            "confidence": _serialize_decimal(row.confidence),
            "invalid_reason": row.invalid_reason,
            "details_json": row.details_json,
            "edge_bucket": current_edge_bucket,
            "validation_classification": validation.classification if validation is not None else None,
            "validation_reason_codes": validation_reason_codes,
            "validation_current_net_edge_bps": _serialize_decimal(validation.current_net_edge_bps) if validation is not None else None,
            "validation_confidence": _serialize_decimal(validation.confidence) if validation is not None else None,
            "plan_status": plan.status if plan is not None else None,
            "cross_venue_review_status": effective_link_status,
            "cross_venue_confidence": _serialize_decimal(link_confidence),
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }
        if classification and detail["validation_classification"] != classification:
            continue
        if reason_code and reason_code not in validation_reason_codes:
            continue
        if edge_bucket and detail["edge_bucket"] != edge_bucket:
            continue
        if plan_status and detail["plan_status"] != plan_status:
            continue
        if review_status and detail["cross_venue_review_status"] != review_status:
            continue
        if confidence_min is not None:
            parsed_confidence = _to_decimal(detail["cross_venue_confidence"] or detail["validation_confidence"] or detail["confidence"]) or ZERO
            if parsed_confidence < Decimal(str(confidence_min)):
                continue
        if executable_only is True and detail["validation_classification"] != VALIDATION_EXECUTABLE:
            continue
        if executable_only is False and detail["validation_classification"] == VALIDATION_EXECUTABLE:
            continue
        results.append(detail)
        if len(results) >= limit:
            break
    return results


async def fetch_market_structure_phase8b_summary(session: AsyncSession) -> dict[str, Any]:
    now = _utcnow()
    opportunity_rows = (
        await session.execute(
            select(MarketStructureOpportunity)
            .order_by(MarketStructureOpportunity.observed_at_local.desc(), MarketStructureOpportunity.id.desc())
            .limit(500)
        )
    ).scalars().all()
    latest_validations = await get_latest_structure_validations(session, [row.id for row in opportunity_rows])
    latest_plans = await get_latest_structure_plans(session, [row.id for row in opportunity_rows])

    informational_only_count = 0
    blocked_count = 0
    executable_count = 0
    opportunities_by_type: dict[str, int] = {}
    validation_reasons: dict[str, int] = {}
    for row in opportunity_rows:
        opportunities_by_type[row.opportunity_type] = opportunities_by_type.get(row.opportunity_type, 0) + 1
        validation = latest_validations.get(row.id)
        if validation is None:
            continue
        if validation.classification == VALIDATION_EXECUTABLE:
            executable_count += 1
        elif validation.classification == VALIDATION_INFORMATIONAL:
            informational_only_count += 1
        else:
            blocked_count += 1
        for code in validation.reason_codes_json or []:
            validation_reasons[code] = validation_reasons.get(code, 0) + 1

    pending_approvals = sum(1 for row in latest_plans.values() if row.status == "approval_pending")
    active_groups = (
        await session.execute(select(MarketStructureGroup).where(MarketStructureGroup.active.is_(True)))
    ).scalars().all()
    skipped_group_count = sum(1 for row in active_groups if not row.actionable)
    stale_link_count = int(
        (
            await session.execute(
                select(func.count(CrossVenueMarketLink.id)).where(
                    CrossVenueMarketLink.active.is_(True),
                    (
                        (CrossVenueMarketLink.expires_at.is_not(None) & (CrossVenueMarketLink.expires_at < now))
                        | (CrossVenueMarketLink.review_status.in_(["needs_review", "expired"]))
                    ),
                )
            )
        ).scalar_one()
        or 0
    )

    polymarket_structure_stale_cross_venue_links.set(stale_link_count)
    polymarket_structure_skipped_groups.set(skipped_group_count)
    polymarket_structure_informational_only_opportunities.set(informational_only_count)
    polymarket_structure_pending_approvals.set(pending_approvals)
    for opportunity_type in opportunities_by_type:
        executable_for_type = sum(
            1
            for row in opportunity_rows
            if row.opportunity_type == opportunity_type
            and latest_validations.get(row.id) is not None
            and latest_validations[row.id].classification == VALIDATION_EXECUTABLE
        )
        informational_for_type = sum(
            1
            for row in opportunity_rows
            if row.opportunity_type == opportunity_type
            and latest_validations.get(row.id) is not None
            and latest_validations[row.id].classification == VALIDATION_INFORMATIONAL
        )
        blocked_for_type = sum(
            1
            for row in opportunity_rows
            if row.opportunity_type == opportunity_type
            and latest_validations.get(row.id) is not None
            and latest_validations[row.id].classification == VALIDATION_BLOCKED
        )
        polymarket_structure_current_opportunities.labels(
            opportunity_type=opportunity_type,
            classification=VALIDATION_EXECUTABLE,
        ).set(executable_for_type)
        polymarket_structure_current_opportunities.labels(
            opportunity_type=opportunity_type,
            classification=VALIDATION_INFORMATIONAL,
        ).set(informational_for_type)
        polymarket_structure_current_opportunities.labels(
            opportunity_type=opportunity_type,
            classification=VALIDATION_BLOCKED,
        ).set(blocked_for_type)

    return {
        "informational_only_opportunity_count": informational_only_count,
        "blocked_opportunity_count": blocked_count,
        "executable_candidate_count": executable_count,
        "opportunity_counts_by_type": opportunities_by_type,
        "validation_reason_counts": validation_reasons,
        "stale_cross_venue_link_count": stale_link_count,
        "skipped_group_count": skipped_group_count,
        "pending_approval_count": pending_approvals,
    }


async def refresh_structure_runtime_metrics(session: AsyncSession) -> None:
    await fetch_market_structure_phase8b_summary(session)
