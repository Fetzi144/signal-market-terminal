from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.ingestion.polymarket_execution_policy import (
    ONE,
    PRICE_Q,
    SHARE_Q,
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
    polymarket_structure_actionable_opportunities,
    polymarket_structure_augmented_filters,
    polymarket_structure_groups_built,
    polymarket_structure_last_successful_scan_timestamp,
    polymarket_structure_non_executable_rejections,
    polymarket_structure_opportunities_detected,
    polymarket_structure_run_failures,
    polymarket_structure_runs,
)
from app.models.market import Market, Outcome
from app.models.market_structure import (
    CrossVenueMarketLink,
    MarketStructureGroup,
    MarketStructureGroupMember,
    MarketStructureOpportunity,
    MarketStructureOpportunityLeg,
    MarketStructureRun,
)
from app.models.polymarket_metadata import PolymarketAssetDim, PolymarketEventDim, PolymarketMarketDim
from app.models.polymarket_raw import PolymarketBookSnapshot
from app.models.polymarket_reconstruction import PolymarketBookReconState
from app.models.snapshot import OrderbookSnapshot

logger = logging.getLogger(__name__)

PACKAGE_SIZE_SHARES = Decimal("1")


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
        return _ensure_utc(value).isoformat() if _ensure_utc(value) is not None else None
    if isinstance(value, dict):
        return {str(key): _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _as_utc(value: datetime | None) -> datetime | None:
    return _ensure_utc(value)


def _edge_bps(edge_total: Decimal | None, reference_cost: Decimal | None) -> Decimal | None:
    if edge_total is None or reference_cost is None or reference_cost <= ZERO:
        return None
    return ((edge_total / reference_cost) * TEN_THOUSAND).quantize(PRICE_Q)


def _normalize_label(value: str | None) -> str:
    return " ".join((value or "").strip().lower().replace("_", " ").split())


def _classify_market_label(market: PolymarketMarketDim) -> str:
    payload = market.source_payload_json if isinstance(market.source_payload_json, dict) else {}
    labels = [
        _normalize_label(market.question),
        _normalize_label(market.market_slug),
        _normalize_label(market.description),
        _normalize_label(payload.get("groupItemTitle")),
        _normalize_label(payload.get("title")),
        _normalize_label(payload.get("shortTitle")),
    ]
    for label in labels:
        if not label:
            continue
        if "placeholder" in label:
            return "placeholder"
        if label == "other" or label.startswith("other ") or label.endswith(" other") or " other " in label:
            return "other"
    return "named"


def _has_augmented_flags(event: PolymarketEventDim, markets: list[PolymarketMarketDim]) -> bool:
    event_payload = event.source_payload_json if isinstance(event.source_payload_json, dict) else {}
    event_keys = (
        "enableNegRisk",
        "enable_neg_risk",
        "augmentedNegRisk",
        "negRiskAugmented",
        "isAugmentedNegRisk",
    )
    if any(bool(event_payload.get(key)) for key in event_keys):
        return True
    market_keys = event_keys + ("hasAugmentedOutcomes", "augmented")
    for market in markets:
        payload = market.source_payload_json if isinstance(market.source_payload_json, dict) else {}
        if any(bool(payload.get(key)) for key in market_keys):
            return True
    return False


def _market_is_actionable_binary(market: PolymarketMarketDim, yes_asset: PolymarketAssetDim | None, no_asset: PolymarketAssetDim | None) -> bool:
    if yes_asset is None or no_asset is None:
        return False
    if market.active is False or market.archived is True or market.closed is True:
        return False
    if (market.resolution_state or "").lower() in {"resolved", "finalized"}:
        return False
    return True


def _member_key(*parts: Any) -> str:
    return ":".join(str(part) for part in parts if part not in (None, ""))


def _extract_side_decimal(details: dict[str, Any], side: str, *keys: str) -> Decimal | None:
    side_payload = details.get(side)
    if isinstance(side_payload, dict):
        for key in keys:
            value = _to_decimal(side_payload.get(key))
            if value is not None:
                return value
    for key in keys:
        compound_value = _to_decimal(details.get(f"{side}_{key}"))
        if compound_value is not None:
            return compound_value
    return None


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
        entry_price = level.yes_price if direct_price_mode else _entry_price_for_direction(direction or "buy_yes", yes_price=level.yes_price)
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
        "fillable_shares": fillable_shares.quantize(SHARE_Q),
        "avg_entry_price": avg_entry_price,
        "worst_price": _quantize(worst_price, PRICE_Q),
        "slippage_bps": slippage_bps,
        "path": path,
    }


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


@dataclass(slots=True)
class LegPricing:
    venue: str
    side: str
    role: str
    target_size: Decimal
    valid: bool
    market_id: uuid.UUID | None = None
    outcome_id: uuid.UUID | None = None
    market_dim_id: int | None = None
    asset_dim_id: int | None = None
    condition_id: str | None = None
    asset_id: str | None = None
    est_fillable_size: Decimal | None = None
    est_avg_entry_price: Decimal | None = None
    est_worst_price: Decimal | None = None
    est_fee: Decimal = ZERO
    est_slippage_bps: Decimal | None = None
    action_type: str | None = "cross_now"
    order_type_hint: str | None = "FAK"
    invalid_reason: str | None = None
    observed_at_local: datetime | None = None
    event_ts_exchange: datetime | None = None
    details_json: dict[str, Any] = field(default_factory=dict)

    @property
    def gross_cost(self) -> Decimal | None:
        if self.est_fillable_size is None or self.est_avg_entry_price is None:
            return None
        return (self.est_fillable_size * self.est_avg_entry_price).quantize(PRICE_Q)

    @property
    def net_cost(self) -> Decimal | None:
        gross_cost = self.gross_cost
        if gross_cost is None:
            return None
        return (gross_cost + (self.est_fee or ZERO)).quantize(PRICE_Q)


class PolymarketStructureEngineService:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def close(self) -> None:
        return None

    async def run(self, stop_event: asyncio.Event) -> None:
        if not settings.polymarket_structure_engine_enabled:
            logger.info("Polymarket structure engine disabled; skipping worker startup")
            return

        if settings.polymarket_structure_on_startup:
            try:
                await self.build_groups(reason="startup")
                await self.scan_opportunities(reason="startup")
            except Exception:
                logger.warning("Polymarket structure startup build/scan failed", exc_info=True)

        while not stop_event.is_set():
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=max(1, settings.polymarket_structure_interval_seconds),
                )
            except asyncio.TimeoutError:
                try:
                    await self.build_groups(reason="scheduled")
                    await self.scan_opportunities(reason="scheduled")
                except Exception:
                    logger.warning("Polymarket structure scheduled build/scan failed", exc_info=True)

    async def build_groups(
        self,
        *,
        reason: str,
        group_type: str | None = None,
        event_slug: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        scope_limit = min(limit or settings.polymarket_structure_max_groups_per_run, settings.polymarket_structure_max_groups_per_run)
        scope = {
            "group_type": group_type,
            "event_slug": event_slug,
            "limit": scope_limit,
        }
        async with self._session_factory() as session:
            run = MarketStructureRun(
                run_type="group_build",
                reason=reason,
                status="running",
                scope_json=_json_safe(scope),
            )
            session.add(run)
            await session.flush()

            rows_inserted = {
                "groups_upserted": 0,
                "members_upserted": 0,
                "events_seen": 0,
                "links_seen": 0,
                "augmented_groups_filtered": 0,
            }
            try:
                if group_type in (None, "neg_risk_event", "binary_complement", "event_sum_parity"):
                    counts = await self._build_polymarket_groups(
                        session,
                        group_type=group_type,
                        event_slug=event_slug,
                        limit=scope_limit,
                    )
                    for key, value in counts.items():
                        rows_inserted[key] = rows_inserted.get(key, 0) + value

                if group_type in (None, "cross_venue_basis"):
                    counts = await self._build_cross_venue_groups(session, limit=scope_limit)
                    for key, value in counts.items():
                        rows_inserted[key] = rows_inserted.get(key, 0) + value

                run.status = "completed"
                run.completed_at = _utcnow()
                run.rows_inserted_json = _json_safe(rows_inserted)
                run.details_json = {
                    "phase": "8a",
                    "scope": _json_safe(scope),
                }
                await session.commit()
                polymarket_structure_runs.labels(run_type="group_build", status="completed").inc()
                if rows_inserted.get("augmented_groups_filtered", 0):
                    polymarket_structure_augmented_filters.inc(rows_inserted["augmented_groups_filtered"])
                return _serialize_structure_run(run)
            except Exception as exc:
                run.status = "failed"
                run.completed_at = _utcnow()
                run.error_count = 1
                run.rows_inserted_json = _json_safe(rows_inserted)
                run.details_json = {
                    "phase": "8a",
                    "scope": _json_safe(scope),
                    "error": str(exc),
                }
                await session.commit()
                polymarket_structure_runs.labels(run_type="group_build", status="failed").inc()
                polymarket_structure_run_failures.labels(run_type="group_build").inc()
                raise

    async def _build_polymarket_groups(
        self,
        session: AsyncSession,
        *,
        group_type: str | None,
        event_slug: str | None,
        limit: int,
    ) -> dict[str, int]:
        event_query = select(PolymarketEventDim).where(PolymarketEventDim.archived.is_not(True))
        if event_slug:
            event_query = event_query.where(PolymarketEventDim.event_slug == event_slug)
        event_query = event_query.order_by(
            func.coalesce(PolymarketEventDim.last_gamma_sync_at, PolymarketEventDim.updated_at).desc(),
            PolymarketEventDim.id.desc(),
        ).limit(limit)
        events = (await session.execute(event_query)).scalars().all()
        if not events:
            return {"events_seen": 0}

        event_ids = [event.id for event in events]
        markets = (
            await session.execute(
                select(PolymarketMarketDim)
                .where(PolymarketMarketDim.event_dim_id.in_(event_ids))
                .order_by(PolymarketMarketDim.event_dim_id.asc(), PolymarketMarketDim.id.asc())
            )
        ).scalars().all()
        market_ids = [market.id for market in markets]
        assets = []
        if market_ids:
            assets = (
                await session.execute(
                    select(PolymarketAssetDim)
                    .where(PolymarketAssetDim.market_dim_id.in_(market_ids))
                    .order_by(PolymarketAssetDim.market_dim_id.asc(), PolymarketAssetDim.id.asc())
                )
            ).scalars().all()

        markets_by_event: dict[int, list[PolymarketMarketDim]] = {}
        for market in markets:
            if market.event_dim_id is None:
                continue
            markets_by_event.setdefault(market.event_dim_id, []).append(market)

        assets_by_market: dict[int, list[PolymarketAssetDim]] = {}
        for asset in assets:
            if asset.market_dim_id is None:
                continue
            assets_by_market.setdefault(asset.market_dim_id, []).append(asset)

        groups_upserted = 0
        members_upserted = 0
        augmented_groups_filtered = 0

        for event in events:
            event_markets = markets_by_event.get(event.id, [])
            if not event_markets:
                continue

            binary_rows: list[dict[str, Any]] = []
            for market in event_markets:
                market_assets = assets_by_market.get(market.id, [])
                yes_asset = next((asset for asset in market_assets if _normalize_label(asset.outcome_name) == "yes"), None)
                no_asset = next((asset for asset in market_assets if _normalize_label(asset.outcome_name) == "no"), None)
                if yes_asset is None or no_asset is None:
                    continue
                classification = _classify_market_label(market)
                binary_rows.append(
                    {
                        "market": market,
                        "yes_asset": yes_asset,
                        "no_asset": no_asset,
                        "classification": classification,
                        "market_actionable": _market_is_actionable_binary(market, yes_asset, no_asset),
                        "display_name": market.question or market.market_slug or market.condition_id,
                    }
                )

            if not binary_rows:
                continue

            named_rows = [row for row in binary_rows if row["classification"] == "named"]
            placeholder_rows = [row for row in binary_rows if row["classification"] == "placeholder"]
            other_rows = [row for row in binary_rows if row["classification"] == "other"]
            has_augmented_members = bool(placeholder_rows or other_rows or _has_augmented_flags(event, event_markets))
            event_is_active = bool(event.active) and not bool(event.closed) and not bool(event.archived)

            if has_augmented_members:
                augmented_groups_filtered += 1

            common_details = {
                "event_dim_id": event.id,
                "event_neg_risk": bool(event.neg_risk),
                "named_outcome_count": len(named_rows),
                "placeholder_outcome_count": len(placeholder_rows),
                "other_outcome_count": len(other_rows),
                "has_augmented_members": has_augmented_members,
                "allow_augmented_neg_risk": settings.polymarket_structure_allow_augmented_neg_risk,
            }

            if group_type in (None, "neg_risk_event") and bool(event.neg_risk):
                neg_risk_actionable = (
                    event_is_active
                    and len(named_rows) >= 2
                    and (not has_augmented_members or settings.polymarket_structure_allow_augmented_neg_risk)
                )
                group, changed = await self._upsert_group(
                    session,
                    group_key=f"neg-risk:{event.id}",
                    defaults={
                        "group_type": "neg_risk_event",
                        "primary_venue": "polymarket",
                        "event_dim_id": event.id,
                        "title": event.title,
                        "event_slug": event.event_slug,
                        "active": event_is_active,
                        "actionable": neg_risk_actionable,
                        "source_kind": "phase2_registry",
                        "details_json": {
                            **common_details,
                            "informational_only": not neg_risk_actionable,
                            "group_semantics": "buy_no(anchor_named_outcome) vs buy_yes(all_other_named_outcomes)",
                        },
                    },
                )
                groups_upserted += changed
                member_specs: list[dict[str, Any]] = []
                for row in binary_rows:
                    market = row["market"]
                    yes_asset = row["yes_asset"]
                    no_asset = row["no_asset"]
                    classification = row["classification"]
                    member_specs.append(
                        {
                            "member_key": _member_key("pm", "yes", yes_asset.asset_id),
                            "venue": "polymarket",
                            "event_dim_id": event.id,
                            "market_dim_id": market.id,
                            "asset_dim_id": yes_asset.id,
                            "market_id": None,
                            "outcome_id": yes_asset.outcome_id,
                            "condition_id": yes_asset.condition_id,
                            "asset_id": yes_asset.asset_id,
                            "outcome_name": row["display_name"],
                            "outcome_index": yes_asset.outcome_index,
                            "member_role": classification if classification != "named" else "named_outcome",
                            "active": row["market_actionable"],
                            "actionable": classification == "named" and neg_risk_actionable,
                            "details_json": {
                                "binary_side": "yes",
                                "display_name": row["display_name"],
                                "classification": classification,
                            },
                        }
                    )
                    member_specs.append(
                        {
                            "member_key": _member_key("pm", "no", no_asset.asset_id),
                            "venue": "polymarket",
                            "event_dim_id": event.id,
                            "market_dim_id": market.id,
                            "asset_dim_id": no_asset.id,
                            "market_id": None,
                            "outcome_id": no_asset.outcome_id,
                            "condition_id": no_asset.condition_id,
                            "asset_id": no_asset.asset_id,
                            "outcome_name": row["display_name"],
                            "outcome_index": no_asset.outcome_index,
                            "member_role": "binary_no",
                            "active": row["market_actionable"],
                            "actionable": classification == "named" and neg_risk_actionable,
                            "details_json": {
                                "binary_side": "no",
                                "display_name": row["display_name"],
                                "classification": classification,
                                "pricing_asset_id": yes_asset.asset_id,
                                "pricing_asset_dim_id": yes_asset.id,
                                "pricing_condition_id": yes_asset.condition_id,
                            },
                        }
                    )
                members_upserted += await self._sync_group_members(session, group.id, member_specs)
                polymarket_structure_groups_built.labels(group_type="neg_risk_event").inc()

            if group_type in (None, "event_sum_parity") and len(binary_rows) >= 2:
                parity_actionable = event_is_active and len(named_rows) >= 2 and not has_augmented_members
                group, changed = await self._upsert_group(
                    session,
                    group_key=f"event-parity:{event.id}",
                    defaults={
                        "group_type": "event_sum_parity",
                        "primary_venue": "polymarket",
                        "event_dim_id": event.id,
                        "title": event.title,
                        "event_slug": event.event_slug,
                        "active": event_is_active,
                        "actionable": parity_actionable,
                        "source_kind": "phase2_registry",
                        "details_json": {
                            **common_details,
                            "constraint_sum": "1.0",
                            "informational_only": not parity_actionable,
                        },
                    },
                )
                groups_upserted += changed
                member_specs = []
                for row in binary_rows:
                    yes_asset = row["yes_asset"]
                    classification = row["classification"]
                    member_specs.append(
                        {
                            "member_key": _member_key("pm", "parity", yes_asset.asset_id),
                            "venue": "polymarket",
                            "event_dim_id": event.id,
                            "market_dim_id": row["market"].id,
                            "asset_dim_id": yes_asset.id,
                            "market_id": None,
                            "outcome_id": yes_asset.outcome_id,
                            "condition_id": yes_asset.condition_id,
                            "asset_id": yes_asset.asset_id,
                            "outcome_name": row["display_name"],
                            "outcome_index": yes_asset.outcome_index,
                            "member_role": classification if classification != "named" else "named_outcome",
                            "active": row["market_actionable"],
                            "actionable": classification == "named" and parity_actionable,
                            "details_json": {
                                "binary_side": "yes",
                                "display_name": row["display_name"],
                                "classification": classification,
                            },
                        }
                    )
                members_upserted += await self._sync_group_members(session, group.id, member_specs)
                polymarket_structure_groups_built.labels(group_type="event_sum_parity").inc()

            if group_type in (None, "binary_complement"):
                for row in binary_rows:
                    market = row["market"]
                    yes_asset = row["yes_asset"]
                    no_asset = row["no_asset"]
                    classification = row["classification"]
                    complement_actionable = event_is_active and row["market_actionable"] and classification == "named"
                    group, changed = await self._upsert_group(
                        session,
                        group_key=f"complement:polymarket:{market.condition_id}",
                        defaults={
                            "group_type": "binary_complement",
                            "primary_venue": "polymarket",
                            "event_dim_id": event.id,
                            "title": market.question or event.title,
                            "event_slug": event.event_slug,
                            "active": row["market_actionable"],
                            "actionable": complement_actionable,
                            "source_kind": "phase2_registry",
                            "details_json": {
                                "classification": classification,
                                "condition_id": market.condition_id,
                                "market_slug": market.market_slug,
                                "display_name": row["display_name"],
                            },
                        },
                    )
                    groups_upserted += changed
                    member_specs = [
                        {
                            "member_key": _member_key("pm", "complement", "yes", yes_asset.asset_id),
                            "venue": "polymarket",
                            "event_dim_id": event.id,
                            "market_dim_id": market.id,
                            "asset_dim_id": yes_asset.id,
                            "market_id": None,
                            "outcome_id": yes_asset.outcome_id,
                            "condition_id": yes_asset.condition_id,
                            "asset_id": yes_asset.asset_id,
                            "outcome_name": row["display_name"],
                            "outcome_index": yes_asset.outcome_index,
                            "member_role": "binary_yes",
                            "active": row["market_actionable"],
                            "actionable": complement_actionable,
                            "details_json": {
                                "display_name": row["display_name"],
                                "classification": classification,
                            },
                        },
                        {
                            "member_key": _member_key("pm", "complement", "no", no_asset.asset_id),
                            "venue": "polymarket",
                            "event_dim_id": event.id,
                            "market_dim_id": market.id,
                            "asset_dim_id": no_asset.id,
                            "market_id": None,
                            "outcome_id": no_asset.outcome_id,
                            "condition_id": no_asset.condition_id,
                            "asset_id": no_asset.asset_id,
                            "outcome_name": row["display_name"],
                            "outcome_index": no_asset.outcome_index,
                            "member_role": "binary_no",
                            "active": row["market_actionable"],
                            "actionable": complement_actionable,
                            "details_json": {
                                "display_name": row["display_name"],
                                "classification": classification,
                                "pricing_asset_id": yes_asset.asset_id,
                                "pricing_asset_dim_id": yes_asset.id,
                                "pricing_condition_id": yes_asset.condition_id,
                            },
                        },
                    ]
                    members_upserted += await self._sync_group_members(session, group.id, member_specs)
                    polymarket_structure_groups_built.labels(group_type="binary_complement").inc()

        return {
            "groups_upserted": groups_upserted,
            "members_upserted": members_upserted,
            "events_seen": len(events),
            "augmented_groups_filtered": augmented_groups_filtered,
        }

    async def _build_cross_venue_groups(self, session: AsyncSession, *, limit: int) -> dict[str, int]:
        link_rows = (
            await session.execute(
                select(CrossVenueMarketLink)
                .order_by(CrossVenueMarketLink.updated_at.desc(), CrossVenueMarketLink.id.desc())
                .limit(limit)
            )
        ).scalars().all()

        groups_upserted = 0
        members_upserted = 0
        for link in link_rows:
            left_display = await self._describe_link_side(session, link, "left")
            right_display = await self._describe_link_side(session, link, "right")
            group, changed = await self._upsert_group(
                session,
                group_key=f"cross-venue:{link.link_key}",
                defaults={
                    "group_type": "cross_venue_basis",
                    "primary_venue": link.left_venue,
                    "event_dim_id": None,
                    "title": f"{left_display['label']} <> {right_display['label']}",
                    "event_slug": None,
                    "active": bool(link.active),
                    "actionable": bool(link.active),
                    "source_kind": "manual_mapping" if link.mapping_kind in {"manual", "curated"} else "derived",
                    "details_json": {
                        "link_id": link.id,
                        "link_key": link.link_key,
                        "mapping_kind": link.mapping_kind,
                    },
                },
            )
            groups_upserted += changed

            details = link.details_json if isinstance(link.details_json, dict) else {}
            member_specs = [
                {
                    "member_key": _member_key("cross", link.link_key, "left"),
                    "venue": link.left_venue,
                    "event_dim_id": None,
                    "market_dim_id": None,
                    "asset_dim_id": None,
                    "market_id": link.left_market_id,
                    "outcome_id": link.left_outcome_id,
                    "condition_id": link.left_condition_id,
                    "asset_id": link.left_asset_id,
                    "outcome_name": left_display["label"],
                    "outcome_index": None,
                    "member_role": "basket_leg",
                    "active": bool(link.active),
                    "actionable": bool(link.active),
                    "details_json": {
                        "link_side": "left",
                        "display_name": left_display["label"],
                        "taker_fee_rate": _serialize_decimal(_extract_side_decimal(details, "left", "fee_rate", "taker_fee_rate")),
                        "min_order_size": _serialize_decimal(_extract_side_decimal(details, "left", "min_order_size")),
                    },
                },
                {
                    "member_key": _member_key("cross", link.link_key, "right"),
                    "venue": link.right_venue,
                    "event_dim_id": None,
                    "market_dim_id": None,
                    "asset_dim_id": None,
                    "market_id": link.right_market_id,
                    "outcome_id": link.right_outcome_id,
                    "condition_id": link.right_condition_id,
                    "asset_id": link.right_asset_id,
                    "outcome_name": right_display["label"],
                    "outcome_index": None,
                    "member_role": "hedge_leg",
                    "active": bool(link.active),
                    "actionable": bool(link.active),
                    "details_json": {
                        "link_side": "right",
                        "display_name": right_display["label"],
                        "taker_fee_rate": _serialize_decimal(_extract_side_decimal(details, "right", "fee_rate", "taker_fee_rate")),
                        "min_order_size": _serialize_decimal(_extract_side_decimal(details, "right", "min_order_size")),
                    },
                },
            ]
            members_upserted += await self._sync_group_members(session, group.id, member_specs)
            polymarket_structure_groups_built.labels(group_type="cross_venue_basis").inc()

        return {
            "groups_upserted": groups_upserted,
            "members_upserted": members_upserted,
            "links_seen": len(link_rows),
        }

    async def _upsert_group(self, session: AsyncSession, *, group_key: str, defaults: dict[str, Any]) -> tuple[MarketStructureGroup, int]:
        group = (
            await session.execute(
                select(MarketStructureGroup).where(MarketStructureGroup.group_key == group_key)
            )
        ).scalar_one_or_none()
        changed = 0
        if group is None:
            group = MarketStructureGroup(group_key=group_key, **defaults)
            session.add(group)
            changed = 1
        else:
            for key, value in defaults.items():
                setattr(group, key, value)
            changed = 1
        await session.flush()
        return group, changed

    async def _sync_group_members(self, session: AsyncSession, group_id: int, member_specs: list[dict[str, Any]]) -> int:
        existing_rows = (
            await session.execute(
                select(MarketStructureGroupMember).where(MarketStructureGroupMember.group_id == group_id)
            )
        ).scalars().all()
        existing_by_key = {row.member_key: row for row in existing_rows}
        seen_keys: set[str] = set()
        upserted = 0
        for spec in member_specs:
            member_key = spec["member_key"]
            seen_keys.add(member_key)
            row = existing_by_key.get(member_key)
            if row is None:
                row = MarketStructureGroupMember(group_id=group_id, **spec)
                session.add(row)
            else:
                for key, value in spec.items():
                    setattr(row, key, value)
            upserted += 1
        for row in existing_rows:
            if row.member_key in seen_keys:
                continue
            row.active = False
            row.actionable = False
            details = row.details_json if isinstance(row.details_json, dict) else {}
            details["missing_from_latest_build"] = True
            row.details_json = details
        await session.flush()
        return upserted

    async def _describe_link_side(
        self,
        session: AsyncSession,
        link: CrossVenueMarketLink,
        side: str,
    ) -> dict[str, Any]:
        market_id = getattr(link, f"{side}_market_id")
        outcome_id = getattr(link, f"{side}_outcome_id")
        condition_id = getattr(link, f"{side}_condition_id")
        asset_id = getattr(link, f"{side}_asset_id")
        symbol = getattr(link, f"{side}_symbol")
        external_id = getattr(link, f"{side}_external_id")

        label = symbol or external_id or condition_id or asset_id or f"{side}:{getattr(link, f'{side}_venue')}"
        if outcome_id is not None:
            outcome = await session.get(Outcome, outcome_id)
            if outcome is not None:
                label = outcome.name
                if market_id is None:
                    market_id = outcome.market_id
        if market_id is not None:
            market = await session.get(Market, market_id)
            if market is not None:
                label = f"{market.platform}:{market.question} [{label}]"

        return {
            "market_id": market_id,
            "outcome_id": outcome_id,
            "label": label,
        }

    async def scan_opportunities(
        self,
        *,
        reason: str,
        group_type: str | None = None,
        event_slug: str | None = None,
        venue: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        scope_limit = min(limit or settings.polymarket_structure_max_groups_per_run, settings.polymarket_structure_max_groups_per_run)
        scope = {
            "group_type": group_type,
            "event_slug": event_slug,
            "venue": venue,
            "limit": scope_limit,
        }
        async with self._session_factory() as session:
            run = MarketStructureRun(
                run_type="opportunity_scan",
                reason=reason,
                status="running",
                scope_json=_json_safe(scope),
            )
            session.add(run)
            await session.flush()
            rows_inserted = {
                "groups_scanned": 0,
                "opportunities_inserted": 0,
                "legs_inserted": 0,
            }
            try:
                query = select(MarketStructureGroup).where(MarketStructureGroup.active.is_(True))
                if group_type:
                    query = query.where(MarketStructureGroup.group_type == group_type)
                if event_slug:
                    query = query.where(MarketStructureGroup.event_slug == event_slug)
                if venue:
                    query = query.where(MarketStructureGroup.primary_venue == venue)
                groups = (
                    await session.execute(
                        query.order_by(MarketStructureGroup.updated_at.desc(), MarketStructureGroup.id.desc()).limit(scope_limit)
                    )
                ).scalars().all()
                if groups:
                    group_ids = [group.id for group in groups]
                    members = (
                        await session.execute(
                            select(MarketStructureGroupMember)
                            .where(MarketStructureGroupMember.group_id.in_(group_ids))
                            .order_by(MarketStructureGroupMember.group_id.asc(), MarketStructureGroupMember.id.asc())
                        )
                    ).scalars().all()
                else:
                    members = []

                members_by_group: dict[int, list[MarketStructureGroupMember]] = {}
                for member in members:
                    members_by_group.setdefault(member.group_id, []).append(member)

                rows_inserted["groups_scanned"] = len(groups)
                scan_now = _utcnow()
                for group in groups:
                    group_members = members_by_group.get(group.id, [])
                    drafts: list[dict[str, Any]] = []
                    if group.group_type == "neg_risk_event":
                        drafts = await self._scan_neg_risk_group(session, group, group_members, scan_now)
                    elif group.group_type == "binary_complement":
                        drafts = await self._scan_binary_complement_group(session, group, group_members, scan_now)
                    elif group.group_type == "event_sum_parity":
                        drafts = await self._scan_event_sum_parity_group(session, group, group_members, scan_now)
                    elif group.group_type == "cross_venue_basis" and settings.polymarket_structure_include_cross_venue:
                        drafts = await self._scan_cross_venue_group(session, group, group_members, scan_now)

                    for draft in drafts:
                        opportunity = await self._persist_opportunity(session, run.id, group.id, draft)
                        rows_inserted["opportunities_inserted"] += 1
                        rows_inserted["legs_inserted"] += len(draft["legs"])
                        polymarket_structure_opportunities_detected.labels(opportunity_type=opportunity.opportunity_type).inc()
                        if opportunity.actionable:
                            polymarket_structure_actionable_opportunities.labels(opportunity_type=opportunity.opportunity_type).inc()
                        if not opportunity.executable_all_legs:
                            polymarket_structure_non_executable_rejections.labels(opportunity_type=opportunity.opportunity_type).inc()

                run.status = "completed"
                run.completed_at = _utcnow()
                run.rows_inserted_json = _json_safe(rows_inserted)
                run.details_json = {"phase": "8a", "scope": _json_safe(scope)}
                await session.commit()
                polymarket_structure_runs.labels(run_type="opportunity_scan", status="completed").inc()
                polymarket_structure_last_successful_scan_timestamp.set(run.completed_at.timestamp())
                return _serialize_structure_run(run)
            except Exception as exc:
                run.status = "failed"
                run.completed_at = _utcnow()
                run.error_count = 1
                run.rows_inserted_json = _json_safe(rows_inserted)
                run.details_json = {
                    "phase": "8a",
                    "scope": _json_safe(scope),
                    "error": str(exc),
                }
                await session.commit()
                polymarket_structure_runs.labels(run_type="opportunity_scan", status="failed").inc()
                polymarket_structure_run_failures.labels(run_type="opportunity_scan").inc()
                raise

    async def _scan_neg_risk_group(
        self,
        session: AsyncSession,
        group: MarketStructureGroup,
        members: list[MarketStructureGroupMember],
        observed_at: datetime,
    ) -> list[dict[str, Any]]:
        if not group.actionable:
            return []

        named_yes_members = [member for member in members if member.member_role == "named_outcome" and member.active]
        no_members_by_market = {
            member.market_dim_id: member
            for member in members
            if member.member_role == "binary_no" and member.market_dim_id is not None
        }
        if len(named_yes_members) < 2:
            return []

        drafts: list[dict[str, Any]] = []
        for anchor in named_yes_members:
            direct_member = no_members_by_market.get(anchor.market_dim_id)
            if direct_member is None:
                continue
            direct_leg = await self._price_member_leg(
                session,
                member=direct_member,
                side="buy_no",
                role="direct_leg",
                target_size=PACKAGE_SIZE_SHARES,
                observed_at=observed_at,
            )
            basket_legs = []
            for basket_member in named_yes_members:
                if basket_member.market_dim_id == anchor.market_dim_id:
                    continue
                basket_legs.append(
                    await self._price_member_leg(
                        session,
                        member=basket_member,
                        side="buy_yes",
                        role="basket_leg",
                        target_size=PACKAGE_SIZE_SHARES,
                        observed_at=observed_at,
                    )
                )
            all_legs = [direct_leg, *basket_legs]
            executable_all_legs = all(leg.valid for leg in all_legs)
            executable = executable_all_legs if settings.polymarket_structure_require_executable_all_legs else any(leg.valid for leg in all_legs)
            direct_gross = direct_leg.gross_cost
            direct_net = direct_leg.net_cost
            basket_gross = sum((leg.gross_cost or ZERO) for leg in basket_legs)
            basket_net = sum((leg.net_cost or ZERO) for leg in basket_legs)
            invalid_reason = next((leg.invalid_reason for leg in all_legs if not leg.valid), None)
            gross_edge_total = None
            net_edge_total = None
            gross_edge_bps = None
            net_edge_bps = None
            preferred_package = None
            rich_package = None
            if direct_gross is not None and direct_net is not None and basket_legs and all(leg.gross_cost is not None and leg.net_cost is not None for leg in basket_legs):
                if direct_net <= basket_net:
                    preferred_package = "direct"
                    rich_package = "basket"
                    gross_edge_total = (basket_gross - direct_gross).quantize(PRICE_Q)
                    net_edge_total = (basket_net - direct_net).quantize(PRICE_Q)
                    gross_edge_bps = _edge_bps(gross_edge_total, direct_gross)
                    net_edge_bps = _edge_bps(net_edge_total, direct_net)
                else:
                    preferred_package = "basket"
                    rich_package = "direct"
                    gross_edge_total = (direct_gross - basket_gross).quantize(PRICE_Q)
                    net_edge_total = (direct_net - basket_net).quantize(PRICE_Q)
                    gross_edge_bps = _edge_bps(gross_edge_total, basket_gross)
                    net_edge_bps = _edge_bps(net_edge_total, basket_net)
            actionable = (
                executable_all_legs
                and net_edge_total is not None
                and net_edge_total > ZERO
                and (net_edge_bps or ZERO) >= Decimal(str(settings.polymarket_structure_min_net_edge_bps))
            )
            drafts.append(
                {
                    "opportunity_type": "neg_risk_direct_vs_basket",
                    "anchor_condition_id": direct_member.condition_id,
                    "anchor_asset_id": direct_member.asset_id,
                    "event_ts_exchange": max(
                        (leg.event_ts_exchange for leg in all_legs if leg.event_ts_exchange is not None),
                        default=None,
                    ),
                    "observed_at_local": max(
                        (leg.observed_at_local for leg in all_legs if leg.observed_at_local is not None),
                        default=observed_at,
                    ),
                    "pricing_method": "all_cross_now",
                    "gross_edge_bps": gross_edge_bps,
                    "net_edge_bps": net_edge_bps,
                    "gross_edge_total": gross_edge_total,
                    "net_edge_total": net_edge_total,
                    "package_size": PACKAGE_SIZE_SHARES,
                    "executable": executable,
                    "executable_all_legs": executable_all_legs,
                    "actionable": actionable,
                    "confidence": Decimal("1") if executable_all_legs else Decimal("0"),
                    "invalid_reason": invalid_reason,
                    "details_json": {
                        "anchor_outcome_name": anchor.outcome_name,
                        "preferred_package": preferred_package,
                        "rich_package": rich_package,
                        "direct_gross_cost": direct_gross,
                        "direct_net_cost": direct_net,
                        "basket_gross_cost": basket_gross.quantize(PRICE_Q),
                        "basket_net_cost": basket_net.quantize(PRICE_Q),
                        "named_outcome_count": len(named_yes_members),
                        "group_key": group.group_key,
                    },
                    "legs": all_legs,
                }
            )
        return drafts

    async def _scan_binary_complement_group(
        self,
        session: AsyncSession,
        group: MarketStructureGroup,
        members: list[MarketStructureGroupMember],
        observed_at: datetime,
    ) -> list[dict[str, Any]]:
        yes_member = next((member for member in members if member.member_role == "binary_yes"), None)
        no_member = next((member for member in members if member.member_role == "binary_no"), None)
        if yes_member is None or no_member is None:
            return []

        yes_leg = await self._price_member_leg(
            session,
            member=yes_member,
            side="buy_yes",
            role="direct_leg",
            target_size=PACKAGE_SIZE_SHARES,
            observed_at=observed_at,
        )
        no_leg = await self._price_member_leg(
            session,
            member=no_member,
            side="buy_no",
            role="hedge_leg",
            target_size=PACKAGE_SIZE_SHARES,
            observed_at=observed_at,
        )
        legs = [yes_leg, no_leg]
        executable_all_legs = all(leg.valid for leg in legs)
        executable = executable_all_legs if settings.polymarket_structure_require_executable_all_legs else any(leg.valid for leg in legs)
        gross_cost = sum((leg.gross_cost or ZERO) for leg in legs).quantize(PRICE_Q)
        net_cost = sum((leg.net_cost or ZERO) for leg in legs).quantize(PRICE_Q)
        gross_edge_total = (ONE - gross_cost).quantize(PRICE_Q)
        net_edge_total = (ONE - net_cost).quantize(PRICE_Q)
        gross_edge_bps = _edge_bps(gross_edge_total, gross_cost)
        net_edge_bps = _edge_bps(net_edge_total, net_cost)
        invalid_reason = next((leg.invalid_reason for leg in legs if not leg.valid), None)
        if invalid_reason is None and net_edge_total <= ZERO:
            invalid_reason = "complement_bundle_above_parity"
        if invalid_reason is None and not group.actionable:
            invalid_reason = "group_non_actionable"
        actionable = (
            group.actionable
            and executable_all_legs
            and net_edge_total > ZERO
            and (net_edge_bps or ZERO) >= Decimal(str(settings.polymarket_structure_min_net_edge_bps))
        )
        return [
            {
                "opportunity_type": "binary_complement",
                "anchor_condition_id": yes_member.condition_id,
                "anchor_asset_id": no_member.asset_id,
                "event_ts_exchange": max((leg.event_ts_exchange for leg in legs if leg.event_ts_exchange is not None), default=None),
                "observed_at_local": max((leg.observed_at_local for leg in legs if leg.observed_at_local is not None), default=observed_at),
                "pricing_method": "all_cross_now",
                "gross_edge_bps": gross_edge_bps,
                "net_edge_bps": net_edge_bps,
                "gross_edge_total": gross_edge_total,
                "net_edge_total": net_edge_total,
                "package_size": PACKAGE_SIZE_SHARES,
                "executable": executable,
                "executable_all_legs": executable_all_legs,
                "actionable": actionable,
                "confidence": Decimal("1") if executable_all_legs else Decimal("0"),
                "invalid_reason": invalid_reason,
                "details_json": {
                    "bundle_gross_cost": gross_cost,
                    "bundle_net_cost": net_cost,
                    "bundle_payout": ONE,
                    "group_key": group.group_key,
                },
                "legs": legs,
            }
        ]

    async def _scan_event_sum_parity_group(
        self,
        session: AsyncSession,
        group: MarketStructureGroup,
        members: list[MarketStructureGroupMember],
        observed_at: datetime,
    ) -> list[dict[str, Any]]:
        named_members = [member for member in members if member.member_role == "named_outcome"]
        if len(named_members) < 2:
            return []
        legs = [
            await self._price_member_leg(
                session,
                member=member,
                side="buy_yes",
                role="basket_leg",
                target_size=PACKAGE_SIZE_SHARES,
                observed_at=observed_at,
            )
            for member in named_members
        ]
        executable_all_legs = all(leg.valid for leg in legs)
        executable = executable_all_legs if settings.polymarket_structure_require_executable_all_legs else any(leg.valid for leg in legs)
        gross_cost = sum((leg.gross_cost or ZERO) for leg in legs).quantize(PRICE_Q)
        net_cost = sum((leg.net_cost or ZERO) for leg in legs).quantize(PRICE_Q)
        gross_edge_total = (ONE - gross_cost).quantize(PRICE_Q)
        net_edge_total = (ONE - net_cost).quantize(PRICE_Q)
        gross_edge_bps = _edge_bps(gross_edge_total, gross_cost)
        net_edge_bps = _edge_bps(net_edge_total, net_cost)
        invalid_reason = next((leg.invalid_reason for leg in legs if not leg.valid), None)
        if invalid_reason is None and not group.actionable:
            invalid_reason = "incomplete_event_composition_filtered"
        elif invalid_reason is None and net_edge_total <= ZERO:
            invalid_reason = "parity_basket_above_one"
        actionable = (
            group.actionable
            and executable_all_legs
            and net_edge_total > ZERO
            and (net_edge_bps or ZERO) >= Decimal(str(settings.polymarket_structure_min_net_edge_bps))
        )
        return [
            {
                "opportunity_type": "event_sum_parity",
                "anchor_condition_id": None,
                "anchor_asset_id": None,
                "event_ts_exchange": max((leg.event_ts_exchange for leg in legs if leg.event_ts_exchange is not None), default=None),
                "observed_at_local": max((leg.observed_at_local for leg in legs if leg.observed_at_local is not None), default=observed_at),
                "pricing_method": "all_cross_now",
                "gross_edge_bps": gross_edge_bps,
                "net_edge_bps": net_edge_bps,
                "gross_edge_total": gross_edge_total,
                "net_edge_total": net_edge_total,
                "package_size": PACKAGE_SIZE_SHARES,
                "executable": executable,
                "executable_all_legs": executable_all_legs,
                "actionable": actionable,
                "confidence": Decimal("1") if executable_all_legs else Decimal("0"),
                "invalid_reason": invalid_reason,
                "details_json": {
                    "bundle_gross_cost": gross_cost,
                    "bundle_net_cost": net_cost,
                    "named_leg_count": len(named_members),
                    "group_key": group.group_key,
                },
                "legs": legs,
            }
        ]

    async def _scan_cross_venue_group(
        self,
        session: AsyncSession,
        group: MarketStructureGroup,
        members: list[MarketStructureGroupMember],
        observed_at: datetime,
    ) -> list[dict[str, Any]]:
        if len(members) < 2:
            return []
        left_member, right_member = members[0], members[1]
        left_yes = await self._price_member_leg(session, member=left_member, side="buy_yes", role="direct_leg", target_size=PACKAGE_SIZE_SHARES, observed_at=observed_at)
        right_no = await self._price_member_leg(session, member=right_member, side="buy_no", role="hedge_leg", target_size=PACKAGE_SIZE_SHARES, observed_at=observed_at)
        right_yes = await self._price_member_leg(session, member=right_member, side="buy_yes", role="direct_leg", target_size=PACKAGE_SIZE_SHARES, observed_at=observed_at)
        left_no = await self._price_member_leg(session, member=left_member, side="buy_no", role="hedge_leg", target_size=PACKAGE_SIZE_SHARES, observed_at=observed_at)

        packages = [
            ("left_yes_vs_right_no", [left_yes, right_no]),
            ("right_yes_vs_left_no", [right_yes, left_no]),
        ]
        chosen_direction = None
        chosen_legs: list[LegPricing] = []
        chosen_net_cost = None
        chosen_gross_cost = None
        invalid_reason = None
        for direction_name, legs in packages:
            if not all(leg.net_cost is not None and leg.gross_cost is not None for leg in legs):
                if invalid_reason is None:
                    invalid_reason = next((leg.invalid_reason for leg in legs if not leg.valid), "missing_cost_estimate")
                continue
            net_cost = sum((leg.net_cost or ZERO) for leg in legs).quantize(PRICE_Q)
            if chosen_net_cost is None or net_cost < chosen_net_cost:
                chosen_direction = direction_name
                chosen_legs = legs
                chosen_net_cost = net_cost
                chosen_gross_cost = sum((leg.gross_cost or ZERO) for leg in legs).quantize(PRICE_Q)
                invalid_reason = next((leg.invalid_reason for leg in legs if not leg.valid), None)

        if chosen_direction is None:
            return []

        executable_all_legs = all(leg.valid for leg in chosen_legs)
        executable = executable_all_legs if settings.polymarket_structure_require_executable_all_legs else any(leg.valid for leg in chosen_legs)
        gross_edge_total = (ONE - chosen_gross_cost).quantize(PRICE_Q) if chosen_gross_cost is not None else None
        net_edge_total = (ONE - chosen_net_cost).quantize(PRICE_Q) if chosen_net_cost is not None else None
        gross_edge_bps = _edge_bps(gross_edge_total, chosen_gross_cost)
        net_edge_bps = _edge_bps(net_edge_total, chosen_net_cost)
        if invalid_reason is None and not group.actionable:
            invalid_reason = "group_non_actionable"
        elif invalid_reason is None and (net_edge_total is None or net_edge_total <= ZERO):
            invalid_reason = "basis_package_above_one"
        actionable = (
            group.actionable
            and executable_all_legs
            and net_edge_total is not None
            and net_edge_total > ZERO
            and (net_edge_bps or ZERO) >= Decimal(str(settings.polymarket_structure_min_net_edge_bps))
        )
        return [
            {
                "opportunity_type": "cross_venue_basis",
                "anchor_condition_id": left_member.condition_id or right_member.condition_id,
                "anchor_asset_id": left_member.asset_id or right_member.asset_id,
                "event_ts_exchange": max((leg.event_ts_exchange for leg in chosen_legs if leg.event_ts_exchange is not None), default=None),
                "observed_at_local": max((leg.observed_at_local for leg in chosen_legs if leg.observed_at_local is not None), default=observed_at),
                "pricing_method": "all_cross_now",
                "gross_edge_bps": gross_edge_bps,
                "net_edge_bps": net_edge_bps,
                "gross_edge_total": gross_edge_total,
                "net_edge_total": net_edge_total,
                "package_size": PACKAGE_SIZE_SHARES,
                "executable": executable,
                "executable_all_legs": executable_all_legs,
                "actionable": actionable,
                "confidence": Decimal("1") if executable_all_legs else Decimal("0"),
                "invalid_reason": invalid_reason,
                "details_json": {
                    "chosen_direction": chosen_direction,
                    "left_yes_plus_right_no_cost": sum((leg.net_cost or ZERO) for leg in packages[0][1]).quantize(PRICE_Q),
                    "right_yes_plus_left_no_cost": sum((leg.net_cost or ZERO) for leg in packages[1][1]).quantize(PRICE_Q),
                    "group_key": group.group_key,
                },
                "legs": chosen_legs,
            }
        ]

    async def _persist_opportunity(
        self,
        session: AsyncSession,
        run_id: uuid.UUID,
        group_id: int,
        draft: dict[str, Any],
    ) -> MarketStructureOpportunity:
        opportunity = MarketStructureOpportunity(
            run_id=run_id,
            group_id=group_id,
            opportunity_type=draft["opportunity_type"],
            anchor_condition_id=draft.get("anchor_condition_id"),
            anchor_asset_id=draft.get("anchor_asset_id"),
            event_ts_exchange=draft.get("event_ts_exchange"),
            observed_at_local=draft.get("observed_at_local") or _utcnow(),
            pricing_method=draft["pricing_method"],
            gross_edge_bps=draft.get("gross_edge_bps"),
            net_edge_bps=draft.get("net_edge_bps"),
            gross_edge_total=draft.get("gross_edge_total"),
            net_edge_total=draft.get("net_edge_total"),
            package_size=draft.get("package_size"),
            executable=bool(draft.get("executable")),
            executable_all_legs=bool(draft.get("executable_all_legs")),
            actionable=bool(draft.get("actionable")),
            confidence=draft.get("confidence"),
            invalid_reason=draft.get("invalid_reason"),
            details_json=_json_safe(draft.get("details_json") or {}),
        )
        session.add(opportunity)
        await session.flush()
        for index, leg in enumerate(draft["legs"]):
            session.add(
                MarketStructureOpportunityLeg(
                    opportunity_id=opportunity.id,
                    leg_index=index,
                    venue=leg.venue,
                    market_id=leg.market_id,
                    outcome_id=leg.outcome_id,
                    market_dim_id=leg.market_dim_id,
                    asset_dim_id=leg.asset_dim_id,
                    condition_id=leg.condition_id,
                    asset_id=leg.asset_id,
                    side=leg.side,
                    role=leg.role,
                    action_type=leg.action_type,
                    order_type_hint=leg.order_type_hint,
                    target_size=leg.target_size,
                    est_fillable_size=leg.est_fillable_size,
                    est_avg_entry_price=leg.est_avg_entry_price,
                    est_worst_price=leg.est_worst_price,
                    est_fee=leg.est_fee,
                    est_slippage_bps=leg.est_slippage_bps,
                    valid=leg.valid,
                    invalid_reason=leg.invalid_reason,
                    source_execution_candidate_id=None,
                    details_json=_json_safe(leg.details_json),
                )
            )
        await session.flush()
        return opportunity

    async def _price_member_leg(
        self,
        session: AsyncSession,
        *,
        member: MarketStructureGroupMember,
        side: str,
        role: str,
        target_size: Decimal,
        observed_at: datetime,
    ) -> LegPricing:
        if member.venue == "polymarket":
            return await self._price_polymarket_leg(session, member=member, side=side, role=role, target_size=target_size, observed_at=observed_at)
        return await self._price_generic_leg(session, member=member, side=side, role=role, target_size=target_size, observed_at=observed_at)

    async def _price_polymarket_leg(
        self,
        session: AsyncSession,
        *,
        member: MarketStructureGroupMember,
        side: str,
        role: str,
        target_size: Decimal,
        observed_at: datetime,
    ) -> LegPricing:
        member_details = member.details_json if isinstance(member.details_json, dict) else {}
        pricing_asset_id = member_details.get("pricing_asset_id") or member.asset_id
        pricing_condition_id = member_details.get("pricing_condition_id") or member.condition_id
        pricing_asset_dim_id = member_details.get("pricing_asset_dim_id") or member.asset_dim_id
        if pricing_asset_id is None or pricing_condition_id is None:
            return LegPricing(
                venue="polymarket",
                side=side,
                role=role,
                target_size=target_size,
                valid=False,
                market_dim_id=member.market_dim_id,
                asset_dim_id=member.asset_dim_id,
                outcome_id=member.outcome_id,
                condition_id=member.condition_id,
                asset_id=member.asset_id,
                invalid_reason="missing_polymarket_identifiers",
            )

        recon_state = (
            await session.execute(
                select(PolymarketBookReconState).where(PolymarketBookReconState.asset_id == pricing_asset_id)
            )
        ).scalar_one_or_none()
        if recon_state is None:
            return LegPricing(
                venue="polymarket",
                side=side,
                role=role,
                target_size=target_size,
                valid=False,
                market_dim_id=member.market_dim_id,
                asset_dim_id=member.asset_dim_id,
                outcome_id=member.outcome_id,
                condition_id=member.condition_id,
                asset_id=member.asset_id,
                invalid_reason="missing_recon_state",
            )

        snapshot, bids, asks, reliable_book, book_reason = await _rebuild_current_book(session, recon_state)
        if not reliable_book:
            return LegPricing(
                venue="polymarket",
                side=side,
                role=role,
                target_size=target_size,
                valid=False,
                market_dim_id=member.market_dim_id,
                asset_dim_id=member.asset_dim_id,
                outcome_id=member.outcome_id,
                condition_id=member.condition_id,
                asset_id=member.asset_id,
                observed_at_local=_as_utc(snapshot.observed_at_local) if snapshot is not None else None,
                event_ts_exchange=_as_utc(snapshot.event_ts_exchange) if snapshot is not None else None,
                invalid_reason=book_reason or "book_unreliable",
            )

        param_history = await _latest_param_history(
            session,
            condition_id=pricing_condition_id,
            asset_id=pricing_asset_id,
        )
        min_order_size = param_history.min_order_size if param_history is not None else None
        taker_fee_rate = _resolve_taker_fee_rate(param_history) if param_history is not None else ZERO
        fees_enabled = bool(param_history.fees_enabled) if param_history is not None else False

        touch_yes_price = recon_state.best_ask if side == "buy_yes" else recon_state.best_bid
        if touch_yes_price is None:
            return LegPricing(
                venue="polymarket",
                side=side,
                role=role,
                target_size=target_size,
                valid=False,
                market_dim_id=member.market_dim_id,
                asset_dim_id=member.asset_dim_id,
                outcome_id=member.outcome_id,
                condition_id=member.condition_id,
                asset_id=member.asset_id,
                observed_at_local=_as_utc(snapshot.observed_at_local) if snapshot is not None else None,
                event_ts_exchange=_as_utc(snapshot.event_ts_exchange) if snapshot is not None else None,
                invalid_reason="missing_touch_price",
            )
        touch_entry_price = _entry_price_for_direction(side, yes_price=touch_yes_price)
        levels = asks if side == "buy_yes" else bids
        if not levels:
            return LegPricing(
                venue="polymarket",
                side=side,
                role=role,
                target_size=target_size,
                valid=False,
                market_dim_id=member.market_dim_id,
                asset_dim_id=member.asset_dim_id,
                outcome_id=member.outcome_id,
                condition_id=member.condition_id,
                asset_id=member.asset_id,
                observed_at_local=_as_utc(snapshot.observed_at_local) if snapshot is not None else None,
                event_ts_exchange=_as_utc(snapshot.event_ts_exchange) if snapshot is not None else None,
                invalid_reason="no_visible_depth",
            )

        walk = _walk_levels_for_shares(
            levels=levels,
            target_shares=target_size,
            touch_entry_price=touch_entry_price,
            direction=side,
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
        elif fillable_shares < target_size:
            invalid_reason = "insufficient_depth"
        elif min_order_size is not None and fillable_shares < min_order_size:
            invalid_reason = "below_min_order_size"
        elif walk["slippage_bps"] is not None and walk["slippage_bps"] > Decimal(str(settings.polymarket_structure_max_leg_slippage_bps)):
            invalid_reason = "leg_slippage_too_high"

        return LegPricing(
            venue="polymarket",
            side=side,
            role=role,
            target_size=target_size,
            valid=invalid_reason is None,
            market_dim_id=member.market_dim_id,
            asset_dim_id=member.asset_dim_id,
            outcome_id=member.outcome_id,
            condition_id=member.condition_id,
            asset_id=member.asset_id,
            est_fillable_size=fillable_shares,
            est_avg_entry_price=avg_entry_price,
            est_worst_price=walk["worst_price"],
            est_fee=taker_fee_total,
            est_slippage_bps=walk["slippage_bps"],
            invalid_reason=invalid_reason,
            observed_at_local=_as_utc(snapshot.observed_at_local) if snapshot is not None else None,
            event_ts_exchange=_as_utc(snapshot.event_ts_exchange) if snapshot is not None else None,
            details_json={
                "pricing_asset_id": pricing_asset_id,
                "pricing_condition_id": pricing_condition_id,
                "pricing_asset_dim_id": pricing_asset_dim_id,
                "book_walk": walk["path"],
                "touch_entry_price": touch_entry_price,
                "fees_enabled": fees_enabled,
                "taker_fee_rate": taker_fee_rate,
                "min_order_size": min_order_size,
            },
        )

    async def _price_generic_leg(
        self,
        session: AsyncSession,
        *,
        member: MarketStructureGroupMember,
        side: str,
        role: str,
        target_size: Decimal,
        observed_at: datetime,
    ) -> LegPricing:
        if member.outcome_id is None:
            return LegPricing(
                venue=member.venue,
                side=side,
                role=role,
                target_size=target_size,
                valid=False,
                market_id=member.market_id,
                outcome_id=member.outcome_id,
                condition_id=member.condition_id,
                asset_id=member.asset_id,
                invalid_reason="missing_outcome_id",
            )

        outcome = await session.get(Outcome, member.outcome_id)
        if outcome is None:
            return LegPricing(
                venue=member.venue,
                side=side,
                role=role,
                target_size=target_size,
                valid=False,
                market_id=member.market_id,
                outcome_id=member.outcome_id,
                condition_id=member.condition_id,
                asset_id=member.asset_id,
                invalid_reason="missing_outcome_row",
            )

        target_outcome = outcome
        if side == "buy_no":
            sibling_rows = (
                await session.execute(
                    select(Outcome).where(Outcome.market_id == outcome.market_id)
                )
            ).scalars().all()
            no_outcome = next((row for row in sibling_rows if _normalize_label(row.name) == "no"), None)
            if no_outcome is None:
                return LegPricing(
                    venue=member.venue,
                    side=side,
                    role=role,
                    target_size=target_size,
                    valid=False,
                    market_id=outcome.market_id,
                    outcome_id=outcome.id,
                    condition_id=member.condition_id,
                    asset_id=member.asset_id,
                    invalid_reason="missing_no_outcome",
                )
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
            return LegPricing(
                venue=member.venue,
                side=side,
                role=role,
                target_size=target_size,
                valid=False,
                market_id=outcome.market_id,
                outcome_id=target_outcome.id,
                condition_id=member.condition_id,
                asset_id=member.asset_id,
                invalid_reason="missing_orderbook_snapshot",
            )

        snapshot_time = _as_utc(snapshot.captured_at)
        max_staleness = timedelta(seconds=settings.polymarket_structure_cross_venue_max_staleness_seconds)
        if snapshot_time is None or observed_at - snapshot_time > max_staleness:
            return LegPricing(
                venue=member.venue,
                side=side,
                role=role,
                target_size=target_size,
                valid=False,
                market_id=outcome.market_id,
                outcome_id=target_outcome.id,
                condition_id=member.condition_id,
                asset_id=member.asset_id,
                observed_at_local=snapshot_time,
                invalid_reason="snapshot_stale",
            )

        asks = _parse_orderbook_levels(snapshot.asks, reverse=False)
        if not asks:
            return LegPricing(
                venue=member.venue,
                side=side,
                role=role,
                target_size=target_size,
                valid=False,
                market_id=outcome.market_id,
                outcome_id=target_outcome.id,
                condition_id=member.condition_id,
                asset_id=member.asset_id,
                observed_at_local=snapshot_time,
                invalid_reason="no_visible_depth",
            )

        member_details = member.details_json if isinstance(member.details_json, dict) else {}
        touch_price = asks[0].yes_price
        walk = _walk_levels_for_shares(
            levels=asks,
            target_shares=target_size,
            touch_entry_price=touch_price,
            direct_price_mode=True,
        )
        fillable_shares = walk["fillable_shares"]
        avg_entry_price = walk["avg_entry_price"]
        fee_rate = _to_decimal(member_details.get("taker_fee_rate")) or ZERO
        fee_total = (((fillable_shares or ZERO) * (avg_entry_price or ZERO)) * fee_rate).quantize(PRICE_Q)
        min_order_size = _to_decimal(member_details.get("min_order_size"))
        invalid_reason = None
        if avg_entry_price is None or fillable_shares <= ZERO:
            invalid_reason = "no_visible_depth"
        elif fillable_shares < target_size:
            invalid_reason = "insufficient_depth"
        elif min_order_size is not None and fillable_shares < min_order_size:
            invalid_reason = "below_min_order_size"
        elif walk["slippage_bps"] is not None and walk["slippage_bps"] > Decimal(str(settings.polymarket_structure_max_leg_slippage_bps)):
            invalid_reason = "leg_slippage_too_high"

        return LegPricing(
            venue=member.venue,
            side=side,
            role=role,
            target_size=target_size,
            valid=invalid_reason is None,
            market_id=outcome.market_id,
            outcome_id=target_outcome.id,
            condition_id=member.condition_id,
            asset_id=member.asset_id,
            est_fillable_size=fillable_shares,
            est_avg_entry_price=avg_entry_price,
            est_worst_price=walk["worst_price"],
            est_fee=fee_total,
            est_slippage_bps=walk["slippage_bps"],
            invalid_reason=invalid_reason,
            observed_at_local=snapshot_time,
            details_json={
                "book_walk": walk["path"],
                "touch_entry_price": touch_price,
                "taker_fee_rate": fee_rate,
                "target_outcome_name": target_outcome.name,
            },
        )


def _serialize_structure_run(row: MarketStructureRun) -> dict[str, Any]:
    return {
        "id": row.id,
        "run_type": row.run_type,
        "reason": row.reason,
        "started_at": row.started_at,
        "completed_at": row.completed_at,
        "status": row.status,
        "scope_json": row.scope_json,
        "cursor_json": row.cursor_json,
        "rows_inserted_json": row.rows_inserted_json,
        "error_count": row.error_count,
        "details_json": row.details_json,
    }


def _serialize_structure_group(row: MarketStructureGroup) -> dict[str, Any]:
    return {
        "id": row.id,
        "group_key": row.group_key,
        "group_type": row.group_type,
        "primary_venue": row.primary_venue,
        "event_dim_id": row.event_dim_id,
        "title": row.title,
        "event_slug": row.event_slug,
        "active": row.active,
        "actionable": row.actionable,
        "source_kind": row.source_kind,
        "details_json": row.details_json,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _serialize_structure_member(row: MarketStructureGroupMember) -> dict[str, Any]:
    return {
        "id": row.id,
        "group_id": row.group_id,
        "member_key": row.member_key,
        "venue": row.venue,
        "event_dim_id": row.event_dim_id,
        "market_dim_id": row.market_dim_id,
        "asset_dim_id": row.asset_dim_id,
        "market_id": row.market_id,
        "outcome_id": row.outcome_id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "outcome_name": row.outcome_name,
        "outcome_index": row.outcome_index,
        "member_role": row.member_role,
        "active": row.active,
        "actionable": row.actionable,
        "details_json": row.details_json,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _serialize_structure_opportunity(row: MarketStructureOpportunity) -> dict[str, Any]:
    return {
        "id": row.id,
        "run_id": row.run_id,
        "group_id": row.group_id,
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
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _serialize_structure_leg(row: MarketStructureOpportunityLeg) -> dict[str, Any]:
    return {
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
        "source_execution_candidate_id": row.source_execution_candidate_id,
        "details_json": row.details_json,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _serialize_cross_venue_link(row: CrossVenueMarketLink) -> dict[str, Any]:
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
        "active": row.active,
        "details_json": row.details_json,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


async def fetch_market_structure_status(session: AsyncSession) -> dict[str, Any]:
    latest_group_build = (
        await session.execute(
            select(MarketStructureRun)
            .where(MarketStructureRun.run_type == "group_build")
            .order_by(MarketStructureRun.started_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    latest_scan = (
        await session.execute(
            select(MarketStructureRun)
            .where(MarketStructureRun.run_type == "opportunity_scan")
            .order_by(MarketStructureRun.started_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    last_successful_group_build_at = (
        await session.execute(
            select(func.max(MarketStructureRun.completed_at)).where(
                MarketStructureRun.run_type == "group_build",
                MarketStructureRun.status == "completed",
            )
        )
    ).scalar_one_or_none()
    last_successful_scan_at = (
        await session.execute(
            select(func.max(MarketStructureRun.completed_at)).where(
                MarketStructureRun.run_type == "opportunity_scan",
                MarketStructureRun.status == "completed",
            )
        )
    ).scalar_one_or_none()

    recent_cutoff = _utcnow() - timedelta(hours=24)
    opportunity_rows = (
        await session.execute(
            select(MarketStructureOpportunity).where(MarketStructureOpportunity.observed_at_local >= recent_cutoff)
        )
    ).scalars().all()
    actionable_by_type: dict[str, int] = {}
    non_executable_count = 0
    for row in opportunity_rows:
        if row.actionable:
            actionable_by_type[row.opportunity_type] = actionable_by_type.get(row.opportunity_type, 0) + 1
        if not row.executable_all_legs:
            non_executable_count += 1

    neg_risk_groups = (
        await session.execute(
            select(MarketStructureGroup).where(MarketStructureGroup.group_type == "neg_risk_event")
        )
    ).scalars().all()
    informational_augmented_group_count = 0
    for group in neg_risk_groups:
        details = group.details_json if isinstance(group.details_json, dict) else {}
        if details.get("has_augmented_members") and not group.actionable:
            informational_augmented_group_count += 1

    active_group_rows = (
        await session.execute(
            select(MarketStructureGroup).where(MarketStructureGroup.active.is_(True))
        )
    ).scalars().all()
    active_group_counts: dict[str, int] = {}
    for row in active_group_rows:
        active_group_counts[row.group_type] = active_group_counts.get(row.group_type, 0) + 1

    recent_runs = (
        await session.execute(
            select(MarketStructureRun)
            .order_by(MarketStructureRun.started_at.desc())
            .limit(10)
        )
    ).scalars().all()
    active_link_count = int(
        (
            await session.execute(
                select(func.count(CrossVenueMarketLink.id)).where(CrossVenueMarketLink.active.is_(True))
            )
        ).scalar_one()
        or 0
    )

    return {
        "enabled": settings.polymarket_structure_engine_enabled,
        "on_startup": settings.polymarket_structure_on_startup,
        "interval_seconds": settings.polymarket_structure_interval_seconds,
        "min_net_edge_bps": settings.polymarket_structure_min_net_edge_bps,
        "require_executable_all_legs": settings.polymarket_structure_require_executable_all_legs,
        "include_cross_venue": settings.polymarket_structure_include_cross_venue,
        "allow_augmented_neg_risk": settings.polymarket_structure_allow_augmented_neg_risk,
        "max_groups_per_run": settings.polymarket_structure_max_groups_per_run,
        "cross_venue_max_staleness_seconds": settings.polymarket_structure_cross_venue_max_staleness_seconds,
        "max_leg_slippage_bps": settings.polymarket_structure_max_leg_slippage_bps,
        "last_successful_group_build_at": last_successful_group_build_at,
        "last_successful_scan_at": last_successful_scan_at,
        "last_group_build_status": latest_group_build.status if latest_group_build is not None else None,
        "last_group_build_started_at": latest_group_build.started_at if latest_group_build is not None else None,
        "last_scan_status": latest_scan.status if latest_scan is not None else None,
        "last_scan_started_at": latest_scan.started_at if latest_scan is not None else None,
        "recent_actionable_by_type": actionable_by_type,
        "recent_non_executable_count": non_executable_count,
        "informational_augmented_group_count": informational_augmented_group_count,
        "active_group_counts": active_group_counts,
        "active_cross_venue_link_count": active_link_count,
        "recent_runs": [_serialize_structure_run(row) for row in recent_runs],
    }


async def list_market_structure_runs(
    session: AsyncSession,
    *,
    run_type: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    query = select(MarketStructureRun)
    if run_type:
        query = query.where(MarketStructureRun.run_type == run_type)
    rows = (
        await session.execute(
            query.order_by(MarketStructureRun.started_at.desc(), MarketStructureRun.id.desc()).limit(limit)
        )
    ).scalars().all()
    return [_serialize_structure_run(row) for row in rows]


async def lookup_market_structure_groups(
    session: AsyncSession,
    *,
    group_type: str | None,
    event_slug: str | None,
    condition_id: str | None,
    asset_id: str | None,
    venue: str | None,
    actionable: bool | None,
    limit: int,
) -> list[dict[str, Any]]:
    query = select(MarketStructureGroup).distinct()
    if condition_id or asset_id or venue is not None or actionable is not None:
        query = query.join(MarketStructureGroupMember, MarketStructureGroupMember.group_id == MarketStructureGroup.id)
    if group_type:
        query = query.where(MarketStructureGroup.group_type == group_type)
    if event_slug:
        query = query.where(MarketStructureGroup.event_slug == event_slug)
    if condition_id:
        query = query.where(MarketStructureGroupMember.condition_id == condition_id)
    if asset_id:
        query = query.where(MarketStructureGroupMember.asset_id == asset_id)
    if venue:
        query = query.where(MarketStructureGroupMember.venue == venue)
    if actionable is not None:
        query = query.where(MarketStructureGroup.actionable.is_(actionable))
    rows = (
        await session.execute(
            query.order_by(MarketStructureGroup.updated_at.desc(), MarketStructureGroup.id.desc()).limit(limit)
        )
    ).scalars().all()
    return [_serialize_structure_group(row) for row in rows]


async def lookup_market_structure_group_members(
    session: AsyncSession,
    *,
    group_id: int | None,
    group_type: str | None,
    event_slug: str | None,
    condition_id: str | None,
    asset_id: str | None,
    venue: str | None,
    actionable: bool | None,
    limit: int,
) -> list[dict[str, Any]]:
    query = select(MarketStructureGroupMember)
    if group_type or event_slug:
        query = query.join(MarketStructureGroup, MarketStructureGroup.id == MarketStructureGroupMember.group_id)
    if group_id is not None:
        query = query.where(MarketStructureGroupMember.group_id == group_id)
    if group_type:
        query = query.where(MarketStructureGroup.group_type == group_type)
    if event_slug:
        query = query.where(MarketStructureGroup.event_slug == event_slug)
    if condition_id:
        query = query.where(MarketStructureGroupMember.condition_id == condition_id)
    if asset_id:
        query = query.where(MarketStructureGroupMember.asset_id == asset_id)
    if venue:
        query = query.where(MarketStructureGroupMember.venue == venue)
    if actionable is not None:
        query = query.where(MarketStructureGroupMember.actionable.is_(actionable))
    rows = (
        await session.execute(
            query.order_by(MarketStructureGroupMember.updated_at.desc(), MarketStructureGroupMember.id.desc()).limit(limit)
        )
    ).scalars().all()
    return [_serialize_structure_member(row) for row in rows]


async def lookup_market_structure_opportunities(
    session: AsyncSession,
    *,
    group_type: str | None,
    opportunity_type: str | None,
    event_slug: str | None,
    condition_id: str | None,
    asset_id: str | None,
    venue: str | None,
    actionable: bool | None,
    limit: int,
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
            query.order_by(MarketStructureOpportunity.observed_at_local.desc(), MarketStructureOpportunity.id.desc()).limit(limit)
        )
    ).scalars().all()
    return [_serialize_structure_opportunity(row) for row in rows]


async def lookup_market_structure_opportunity_legs(
    session: AsyncSession,
    *,
    opportunity_id: int | None,
    opportunity_type: str | None,
    condition_id: str | None,
    asset_id: str | None,
    venue: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    query = select(MarketStructureOpportunityLeg)
    if opportunity_type:
        query = query.join(MarketStructureOpportunity, MarketStructureOpportunity.id == MarketStructureOpportunityLeg.opportunity_id)
    if opportunity_id is not None:
        query = query.where(MarketStructureOpportunityLeg.opportunity_id == opportunity_id)
    if opportunity_type:
        query = query.where(MarketStructureOpportunity.opportunity_type == opportunity_type)
    if condition_id:
        query = query.where(MarketStructureOpportunityLeg.condition_id == condition_id)
    if asset_id:
        query = query.where(MarketStructureOpportunityLeg.asset_id == asset_id)
    if venue:
        query = query.where(MarketStructureOpportunityLeg.venue == venue)
    rows = (
        await session.execute(
            query.order_by(MarketStructureOpportunityLeg.created_at.desc(), MarketStructureOpportunityLeg.id.desc()).limit(limit)
        )
    ).scalars().all()
    return [_serialize_structure_leg(row) for row in rows]


async def lookup_cross_venue_market_links(
    session: AsyncSession,
    *,
    venue: str | None,
    actionable: bool | None,
    limit: int,
) -> list[dict[str, Any]]:
    query = select(CrossVenueMarketLink)
    if venue:
        query = query.where(
            (CrossVenueMarketLink.left_venue == venue) | (CrossVenueMarketLink.right_venue == venue)
        )
    if actionable is not None:
        query = query.where(CrossVenueMarketLink.active.is_(actionable))
    rows = (
        await session.execute(
            query.order_by(CrossVenueMarketLink.updated_at.desc(), CrossVenueMarketLink.id.desc()).limit(limit)
        )
    ).scalars().all()
    return [_serialize_cross_venue_link(row) for row in rows]


def _default_link_key(payload: dict[str, Any]) -> str:
    left = payload.get("left_condition_id") or payload.get("left_asset_id") or payload.get("left_outcome_id") or payload.get("left_symbol") or payload.get("left_external_id")
    right = payload.get("right_condition_id") or payload.get("right_asset_id") or payload.get("right_outcome_id") or payload.get("right_symbol") or payload.get("right_external_id")
    return _member_key(payload.get("left_venue"), left, payload.get("right_venue"), right)


async def upsert_cross_venue_market_link(
    session: AsyncSession,
    *,
    link_id: int | None = None,
    payload: dict[str, Any],
) -> dict[str, Any]:
    link_key = payload.get("link_key") or _default_link_key(payload)
    if link_id is not None:
        row = await session.get(CrossVenueMarketLink, link_id)
    else:
        row = (
            await session.execute(
                select(CrossVenueMarketLink).where(CrossVenueMarketLink.link_key == link_key)
            )
        ).scalar_one_or_none()
    if row is None:
        row = CrossVenueMarketLink(link_key=link_key)
        session.add(row)
    for key, value in payload.items():
        if key == "link_key" and not value:
            continue
        setattr(row, key, value)
    row.link_key = link_key
    await session.commit()
    await session.refresh(row)
    return _serialize_cross_venue_link(row)


async def trigger_manual_structure_group_build(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    reason: str,
    group_type: str | None = None,
    event_slug: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    service = PolymarketStructureEngineService(session_factory)
    try:
        return await service.build_groups(reason=reason, group_type=group_type, event_slug=event_slug, limit=limit)
    finally:
        await service.close()


async def trigger_manual_structure_opportunity_scan(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    reason: str,
    group_type: str | None = None,
    event_slug: str | None = None,
    venue: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    service = PolymarketStructureEngineService(session_factory)
    try:
        return await service.scan_opportunities(
            reason=reason,
            group_type=group_type,
            event_slug=event_slug,
            venue=venue,
            limit=limit,
        )
    finally:
        await service.close()
