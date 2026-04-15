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
from app.metrics import (
    polymarket_risk_concentration,
    polymarket_risk_exposure_snapshot_failures,
    polymarket_risk_exposure_snapshot_runs,
    polymarket_risk_graph_build_failures,
    polymarket_risk_graph_build_runs,
    polymarket_risk_inventory_budget_utilization,
    polymarket_risk_last_successful_timestamp,
    polymarket_risk_no_quote_recommendations,
    polymarket_risk_optimizer_failures,
    polymarket_risk_optimizer_recommendations,
    polymarket_risk_optimizer_runs,
)
from app.models.market import Market
from app.models.market_structure import (
    CrossVenueMarketLink,
    MarketStructureGroup,
    MarketStructureGroupMember,
    MarketStructurePaperOrder,
    MarketStructurePaperPlan,
)
from app.models.paper_trade import PaperTrade
from app.models.polymarket_live_execution import CapitalReservation, LiveOrder
from app.models.polymarket_maker import PolymarketQuoteRecommendation
from app.models.polymarket_metadata import PolymarketAssetDim, PolymarketEventDim, PolymarketMarketDim
from app.models.polymarket_risk import (
    InventoryControlSnapshot,
    PortfolioExposureSnapshot,
    PortfolioOptimizerRecommendation,
    RiskGraphEdge,
    RiskGraphNode,
    RiskGraphRun,
)

logger = logging.getLogger(__name__)

ZERO = Decimal("0")
ONE = Decimal("1")
TEN_THOUSAND = Decimal("10000")
SIZE_Q = Decimal("0.00000001")
RATIO_Q = Decimal("0.000001")
BPS_Q = Decimal("0.0001")
SNAPSHOT_LOOKBACK = timedelta(hours=6)
OPEN_PAPER_TRADE_STATUSES = {"open"}
OPEN_STRUCTURE_PLAN_STATUSES = {"approval_pending", "routing_pending", "routed", "partial_failed"}
TERMINAL_LIVE_ORDER_STATUSES = {
    "matched",
    "mined",
    "confirmed",
    "canceled",
    "expired",
    "rejected",
    "failed",
    "validation_failed",
}
RELEASED_RESERVATION_STATUSES = {"released", "expired", "canceled"}
GRAPH_BUILD_RUN_TYPE = "graph_build"
EXPOSURE_SNAPSHOT_RUN_TYPE = "exposure_snapshot"
PORTFOLIO_OPTIMIZE_RUN_TYPE = "portfolio_optimize"
DEFAULT_REASON = "manual"


@dataclass(slots=True)
class NodeSpec:
    node_key: str
    node_type: str
    venue: str | None = None
    event_dim_id: int | None = None
    market_dim_id: int | None = None
    asset_dim_id: int | None = None
    condition_id: str | None = None
    asset_id: str | None = None
    label: str | None = None
    active: bool = True
    details_json: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class EdgeSpec:
    left_node_key: str
    right_node_key: str
    edge_type: str
    weight: Decimal | None = None
    active: bool = True
    source_kind: str = "derived"
    details_json: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExposureContribution:
    base_node_id: int
    exposure_kind: str
    gross_notional_usd: Decimal
    canonical_net_notional_usd: Decimal
    buy_notional_usd: Decimal
    sell_notional_usd: Decimal
    share_exposure: Decimal
    reservation_cost_usd: Decimal
    inventory_bucket: str
    details_json: dict[str, Any]


@dataclass(slots=True)
class NodeRollup:
    node: RiskGraphNode
    gross_notional_usd: Decimal = ZERO
    net_notional_usd: Decimal = ZERO
    buy_notional_usd: Decimal = ZERO
    sell_notional_usd: Decimal = ZERO
    share_exposure: Decimal = ZERO
    reservation_cost_usd: Decimal = ZERO
    hedged_fraction: Decimal = ZERO
    source_ids: set[Any] = field(default_factory=set)
    source_kinds: set[str] = field(default_factory=set)


@dataclass(slots=True)
class ExposureState:
    snapshot_at: datetime
    nodes_by_id: dict[int, RiskGraphNode]
    aggregate_rollups: dict[int, NodeRollup]
    source_rollups: list[dict[str, Any]]
    maker_budget_used_usd: Decimal
    taker_budget_used_usd: Decimal
    maker_budget_remaining_usd: Decimal
    taker_budget_remaining_usd: Decimal
    asset_group_weights: dict[int, list[tuple[int, Decimal, str, str]]]
    asset_bucket_memberships: dict[int, dict[str, list[int]]]
    asset_hedge_links: dict[int, list[tuple[int, Decimal, str]]]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return Decimal(stripped)
        except InvalidOperation:
            return None
    return None


def _serialize_decimal(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        normalized = _ensure_utc(value)
        return normalized.isoformat() if normalized is not None else None
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_json_safe(inner) for inner in value]
    if isinstance(value, set):
        return [_json_safe(inner) for inner in sorted(value, key=str)]
    return value


def _normalized_fragment(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    return "".join(char if char.isalnum() else "-" for char in text).strip("-") or None


def _node_key(*parts: str | int | None) -> str:
    return ":".join(str(part) for part in parts if part not in (None, ""))


def _canonical_pair(left: str, right: str) -> tuple[str, str]:
    return (left, right) if left <= right else (right, left)


def _synthetic_asset_id(
    *,
    asset_id: str | None = None,
    outcome_id: uuid.UUID | None = None,
    condition_id: str | None = None,
    market_id: uuid.UUID | None = None,
    external_id: str | None = None,
) -> str | None:
    if asset_id:
        return asset_id
    for prefix, value in (
        ("outcome", str(outcome_id) if outcome_id is not None else None),
        ("condition", condition_id),
        ("market", str(market_id) if market_id is not None else None),
        ("external", external_id),
    ):
        if value:
            return f"{prefix}:{value}"
    return None


def _extract_tag_values(tags_json: Any) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not isinstance(tags_json, list):
        return rows
    for item in tags_json:
        if isinstance(item, dict):
            slug = item.get("slug") or item.get("id")
            label = item.get("label") or item.get("name") or item.get("slug") or item.get("id")
            if label:
                rows.append({"slug": str(slug or label), "label": str(label)})
        elif isinstance(item, str) and item.strip():
            rows.append({"slug": item.strip(), "label": item.strip()})
    return rows


def _group_member_weight(group_type: str, member_role: str) -> Decimal:
    role = (member_role or "").lower()
    group = (group_type or "").lower()
    if group == "binary_complement":
        return Decimal("-1") if role == "binary_no" else Decimal("1")
    if group == "cross_venue_basis":
        return Decimal("-1") if role == "hedge_leg" else Decimal("1")
    if group == "neg_risk_event":
        return Decimal("-1") if role == "binary_no" else Decimal("1")
    return Decimal("1")


def _direction_sign(*, direction: str | None, side: str | None, outcome_name: str | None) -> Decimal:
    text = (direction or side or "").lower()
    if text in {"buy_no", "sell_yes", "no"}:
        return Decimal("-1")
    if text in {"buy_yes", "sell_no", "yes"}:
        return Decimal("1")
    outcome = (outcome_name or "").lower()
    if outcome in {"no", "false"}:
        return Decimal("-1")
    return Decimal("1")


def _quote_inventory_bucket(exposure_kind: str, *, post_only: bool = False, action_type: str | None = None) -> str:
    if exposure_kind == "maker_quote":
        return "maker"
    if exposure_kind == "live_order" and (post_only or action_type in {"post_best", "step_ahead"}):
        return "maker"
    return "taker"


def _combine_reason_code(reason_codes: list[str]) -> str:
    return reason_codes[0] if reason_codes else "allow"


async def _start_run(
    session: AsyncSession,
    *,
    run_type: str,
    reason: str,
    scope_json: dict[str, Any] | None = None,
) -> RiskGraphRun:
    run = RiskGraphRun(
        run_type=run_type,
        reason=reason,
        status="running",
        scope_json=_json_safe(scope_json or {}),
    )
    session.add(run)
    await session.flush()
    return run


async def _finish_run(
    session: AsyncSession,
    run: RiskGraphRun,
    *,
    status: str,
    rows_inserted_json: dict[str, Any] | None = None,
    details_json: dict[str, Any] | None = None,
    error_count: int = 0,
) -> RiskGraphRun:
    run.status = status
    run.completed_at = _utcnow()
    run.rows_inserted_json = _json_safe(rows_inserted_json or {})
    run.details_json = _json_safe(details_json or {})
    run.error_count = error_count
    await session.flush()
    return run


def _node_snapshot_payload(node: RiskGraphNode) -> dict[str, Any]:
    return {
        "id": node.id,
        "node_key": node.node_key,
        "node_type": node.node_type,
        "venue": node.venue,
        "event_dim_id": node.event_dim_id,
        "market_dim_id": node.market_dim_id,
        "asset_dim_id": node.asset_dim_id,
        "condition_id": node.condition_id,
        "asset_id": node.asset_id,
        "label": node.label,
        "active": node.active,
        "details_json": _json_safe(node.details_json),
        "created_at": node.created_at,
        "updated_at": node.updated_at,
    }


def _edge_snapshot_payload(edge: RiskGraphEdge) -> dict[str, Any]:
    return {
        "id": edge.id,
        "left_node_id": edge.left_node_id,
        "right_node_id": edge.right_node_id,
        "edge_type": edge.edge_type,
        "weight": _serialize_decimal(edge.weight),
        "active": edge.active,
        "source_kind": edge.source_kind,
        "details_json": _json_safe(edge.details_json),
        "created_at": edge.created_at,
        "updated_at": edge.updated_at,
    }


def _snapshot_payload(row: PortfolioExposureSnapshot, node: RiskGraphNode | None = None) -> dict[str, Any]:
    payload = {
        "id": row.id,
        "run_id": row.run_id,
        "snapshot_at": row.snapshot_at,
        "node_id": row.node_id,
        "exposure_kind": row.exposure_kind,
        "gross_notional_usd": _serialize_decimal(row.gross_notional_usd),
        "net_notional_usd": _serialize_decimal(row.net_notional_usd),
        "buy_notional_usd": _serialize_decimal(row.buy_notional_usd),
        "sell_notional_usd": _serialize_decimal(row.sell_notional_usd),
        "share_exposure": _serialize_decimal(row.share_exposure),
        "reservation_cost_usd": _serialize_decimal(row.reservation_cost_usd),
        "hedged_fraction": _serialize_decimal(row.hedged_fraction),
        "details_json": _json_safe(row.details_json),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }
    if node is not None:
        payload["node"] = _node_snapshot_payload(node)
    return payload


def _optimizer_payload(row: PortfolioOptimizerRecommendation, node: RiskGraphNode | None = None) -> dict[str, Any]:
    payload = {
        "id": row.id,
        "run_id": row.run_id,
        "node_id": row.node_id,
        "recommendation_type": row.recommendation_type,
        "scope_kind": row.scope_kind,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "target_size_cap_usd": _serialize_decimal(row.target_size_cap_usd),
        "inventory_penalty_bps": _serialize_decimal(row.inventory_penalty_bps),
        "reservation_price_adjustment_bps": _serialize_decimal(row.reservation_price_adjustment_bps),
        "maker_budget_remaining_usd": _serialize_decimal(row.maker_budget_remaining_usd),
        "taker_budget_remaining_usd": _serialize_decimal(row.taker_budget_remaining_usd),
        "reason_code": row.reason_code,
        "details_json": _json_safe(row.details_json),
        "observed_at_local": row.observed_at_local,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }
    if node is not None:
        payload["node"] = _node_snapshot_payload(node)
    return payload


def _inventory_payload(row: InventoryControlSnapshot) -> dict[str, Any]:
    return {
        "id": row.id,
        "snapshot_at": row.snapshot_at,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "control_scope": row.control_scope,
        "maker_budget_usd": _serialize_decimal(row.maker_budget_usd),
        "taker_budget_usd": _serialize_decimal(row.taker_budget_usd),
        "maker_budget_used_usd": _serialize_decimal(row.maker_budget_used_usd),
        "taker_budget_used_usd": _serialize_decimal(row.taker_budget_used_usd),
        "reservation_price_shift_bps": _serialize_decimal(row.reservation_price_shift_bps),
        "quote_skew_direction": row.quote_skew_direction,
        "no_quote": row.no_quote,
        "reason_code": row.reason_code,
        "details_json": _json_safe(row.details_json),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


async def _upsert_runtime_asset_node(
    session: AsyncSession,
    *,
    venue: str,
    asset_id: str,
    condition_id: str | None = None,
    label: str | None = None,
    source_kind: str,
    details_json: dict[str, Any] | None = None,
) -> RiskGraphNode:
    node_key = _node_key("asset", venue, asset_id)
    node = (
        await session.execute(select(RiskGraphNode).where(RiskGraphNode.node_key == node_key))
    ).scalar_one_or_none()
    payload = {
        "managed_by": "phase10_runtime",
        "source_kind": source_kind,
        **(details_json or {}),
    }
    if node is None:
        node = RiskGraphNode(
            node_key=node_key,
            node_type="asset",
            venue=venue,
            condition_id=condition_id,
            asset_id=asset_id,
            label=label or asset_id,
            active=True,
            details_json=payload,
        )
        session.add(node)
    else:
        node.venue = venue
        node.condition_id = condition_id or node.condition_id
        node.asset_id = asset_id
        node.label = label or node.label
        node.active = True
        existing = node.details_json if isinstance(node.details_json, dict) else {}
        node.details_json = {**existing, **payload}
    await session.flush()
    return node


async def _upsert_runtime_market_node(
    session: AsyncSession,
    *,
    venue: str,
    condition_id: str,
    label: str | None = None,
    source_kind: str,
    details_json: dict[str, Any] | None = None,
) -> RiskGraphNode:
    node_key = _node_key("market", venue, condition_id)
    node = (
        await session.execute(select(RiskGraphNode).where(RiskGraphNode.node_key == node_key))
    ).scalar_one_or_none()
    payload = {
        "managed_by": "phase10_runtime",
        "source_kind": source_kind,
        **(details_json or {}),
    }
    if node is None:
        node = RiskGraphNode(
            node_key=node_key,
            node_type="market",
            venue=venue,
            condition_id=condition_id,
            label=label or condition_id,
            active=True,
            details_json=payload,
        )
        session.add(node)
    else:
        node.venue = venue
        node.condition_id = condition_id
        node.label = label or node.label
        node.active = True
        existing = node.details_json if isinstance(node.details_json, dict) else {}
        node.details_json = {**existing, **payload}
    await session.flush()
    return node


async def build_risk_graph(
    session: AsyncSession,
    *,
    reason: str = DEFAULT_REASON,
    scope_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    run = await _start_run(
        session,
        run_type=GRAPH_BUILD_RUN_TYPE,
        reason=reason,
        scope_json=scope_json or {},
    )
    rows_inserted = {"nodes_upserted": 0, "edges_upserted": 0}
    try:
        events = (await session.execute(select(PolymarketEventDim))).scalars().all()
        markets = (await session.execute(select(PolymarketMarketDim))).scalars().all()
        assets = (await session.execute(select(PolymarketAssetDim))).scalars().all()
        groups = (await session.execute(select(MarketStructureGroup))).scalars().all()
        group_members = (await session.execute(select(MarketStructureGroupMember))).scalars().all()
        links = (await session.execute(select(CrossVenueMarketLink))).scalars().all()

        node_specs: dict[str, NodeSpec] = {}
        edge_specs: dict[tuple[str, str, str, str], EdgeSpec] = {}

        def ensure_node(spec: NodeSpec) -> None:
            existing = node_specs.get(spec.node_key)
            if existing is None:
                node_specs[spec.node_key] = spec
                return
            existing.active = existing.active or spec.active
            existing.label = existing.label or spec.label
            existing.venue = existing.venue or spec.venue
            existing.event_dim_id = existing.event_dim_id or spec.event_dim_id
            existing.market_dim_id = existing.market_dim_id or spec.market_dim_id
            existing.asset_dim_id = existing.asset_dim_id or spec.asset_dim_id
            existing.condition_id = existing.condition_id or spec.condition_id
            existing.asset_id = existing.asset_id or spec.asset_id
            existing.details_json = {**existing.details_json, **spec.details_json}

        def ensure_edge(spec: EdgeSpec) -> None:
            left_key, right_key = _canonical_pair(spec.left_node_key, spec.right_node_key)
            edge_specs[(left_key, right_key, spec.edge_type, spec.source_kind)] = EdgeSpec(
                left_node_key=left_key,
                right_node_key=right_key,
                edge_type=spec.edge_type,
                weight=spec.weight,
                active=spec.active,
                source_kind=spec.source_kind,
                details_json=spec.details_json,
            )

        ensure_node(NodeSpec(node_key=_node_key("venue", "global"), node_type="venue", venue="global", label="Global Portfolio", details_json={"managed_by": "phase10_graph"}))
        ensure_node(NodeSpec(node_key=_node_key("venue", "polymarket"), node_type="venue", venue="polymarket", label="Polymarket", details_json={"managed_by": "phase10_graph"}))

        events_by_id = {row.id: row for row in events}
        markets_by_id = {row.id: row for row in markets}
        market_tags_by_market_id: dict[int, list[dict[str, str]]] = {}

        for event in events:
            event_key = _node_key("event", "polymarket", event.gamma_event_id or event.event_slug or event.id)
            ensure_node(
                NodeSpec(
                    node_key=event_key,
                    node_type="event",
                    venue="polymarket",
                    event_dim_id=event.id,
                    label=event.title or event.event_slug or str(event.gamma_event_id or event.id),
                    active=bool(event.active) if event.active is not None else True,
                    details_json={
                        "managed_by": "phase10_graph",
                        "gamma_event_id": event.gamma_event_id,
                        "event_slug": event.event_slug,
                        "category": event.category,
                        "subcategory": event.subcategory,
                        "neg_risk": event.neg_risk,
                    },
                )
            )
            ensure_edge(EdgeSpec(left_node_key=_node_key("venue", "polymarket"), right_node_key=event_key, edge_type="same_event", source_kind="phase2_registry", details_json={"relationship": "venue_contains_event"}))
            for category_value, category_kind in ((event.category, "category"), (event.subcategory, "subcategory")):
                normalized = _normalized_fragment(category_value)
                if normalized is None:
                    continue
                category_key = _node_key("category", "polymarket", normalized)
                ensure_node(NodeSpec(node_key=category_key, node_type="category", venue="polymarket", label=category_value, details_json={"managed_by": "phase10_graph", "category_kind": category_kind}))
                ensure_edge(EdgeSpec(left_node_key=event_key, right_node_key=category_key, edge_type="category_link", source_kind="phase2_registry", details_json={"category_kind": category_kind}))

        for market in markets:
            event = events_by_id.get(market.event_dim_id)
            market_key = _node_key("market", "polymarket", market.condition_id)
            ensure_node(
                NodeSpec(
                    node_key=market_key,
                    node_type="market",
                    venue="polymarket",
                    event_dim_id=market.event_dim_id,
                    market_dim_id=market.id,
                    condition_id=market.condition_id,
                    label=market.question or market.market_slug or market.condition_id,
                    active=bool(market.active) if market.active is not None else True,
                    details_json={"managed_by": "phase10_graph", "market_slug": market.market_slug, "resolution_state": market.resolution_state, "tags": _json_safe(market.tags_json)},
                )
            )
            if event is not None:
                ensure_edge(EdgeSpec(left_node_key=market_key, right_node_key=_node_key("event", "polymarket", event.gamma_event_id or event.event_slug or event.id), edge_type="same_event", source_kind="phase2_registry", details_json={"condition_id": market.condition_id}))
            tag_rows = _extract_tag_values(market.tags_json)
            market_tags_by_market_id[market.id] = tag_rows
            category_names = {_normalized_fragment(event.category if event is not None else None), _normalized_fragment(event.subcategory if event is not None else None)}
            for tag_row in tag_rows:
                normalized = _normalized_fragment(tag_row["slug"]) or _normalized_fragment(tag_row["label"])
                if normalized is None:
                    continue
                node_type = "category" if normalized in category_names else "entity"
                related_key = _node_key(node_type, "polymarket", normalized)
                ensure_node(NodeSpec(node_key=related_key, node_type=node_type, venue="polymarket", label=tag_row["label"], details_json={"managed_by": "phase10_graph", "source": "market_tags", "slug": tag_row["slug"]}))
                ensure_edge(EdgeSpec(left_node_key=market_key, right_node_key=related_key, edge_type="same_entity" if node_type == "entity" else "category_link", source_kind="phase2_registry", details_json={"tag_slug": tag_row["slug"], "tag_label": tag_row["label"]}))

        condition_assets: dict[str, list[PolymarketAssetDim]] = {}
        for asset in assets:
            market = markets_by_id.get(asset.market_dim_id)
            event = events_by_id.get(market.event_dim_id) if market is not None else None
            asset_key = _node_key("asset", "polymarket", asset.asset_id)
            ensure_node(
                NodeSpec(
                    node_key=asset_key,
                    node_type="asset",
                    venue="polymarket",
                    event_dim_id=market.event_dim_id if market is not None else None,
                    market_dim_id=asset.market_dim_id,
                    asset_dim_id=asset.id,
                    condition_id=asset.condition_id,
                    asset_id=asset.asset_id,
                    label=asset.outcome_name or asset.asset_id,
                    active=bool(asset.active) if asset.active is not None else True,
                    details_json={"managed_by": "phase10_graph", "outcome_name": asset.outcome_name, "outcome_index": asset.outcome_index, "winner": asset.winner},
                )
            )
            ensure_edge(EdgeSpec(left_node_key=asset_key, right_node_key=_node_key("venue", "polymarket"), edge_type="same_event", source_kind="phase2_registry", details_json={"relationship": "asset_on_venue"}))
            if market is not None:
                ensure_edge(EdgeSpec(left_node_key=asset_key, right_node_key=_node_key("market", "polymarket", asset.condition_id), edge_type="same_event", source_kind="phase2_registry", details_json={"relationship": "asset_in_market"}))
            if event is not None:
                event_key = _node_key("event", "polymarket", event.gamma_event_id or event.event_slug or event.id)
                ensure_edge(EdgeSpec(left_node_key=asset_key, right_node_key=event_key, edge_type="same_event", source_kind="phase2_registry", details_json={"relationship": "asset_in_event"}))
                for category_value, category_kind in ((event.category, "category"), (event.subcategory, "subcategory")):
                    normalized = _normalized_fragment(category_value)
                    if normalized is None:
                        continue
                    ensure_edge(EdgeSpec(left_node_key=asset_key, right_node_key=_node_key("category", "polymarket", normalized), edge_type="category_link", source_kind="phase2_registry", details_json={"category_kind": category_kind}))
            if market is not None:
                category_names = {_normalized_fragment(event.category if event is not None else None), _normalized_fragment(event.subcategory if event is not None else None)}
                for tag_row in market_tags_by_market_id.get(market.id, []):
                    normalized = _normalized_fragment(tag_row["slug"]) or _normalized_fragment(tag_row["label"])
                    if normalized is None:
                        continue
                    node_type = "category" if normalized in category_names else "entity"
                    ensure_edge(EdgeSpec(left_node_key=asset_key, right_node_key=_node_key(node_type, "polymarket", normalized), edge_type="same_entity" if node_type == "entity" else "category_link", source_kind="phase2_registry", details_json={"tag_slug": tag_row["slug"], "tag_label": tag_row["label"]}))
            condition_assets.setdefault(asset.condition_id, []).append(asset)

        for condition_id, asset_rows in condition_assets.items():
            if len(asset_rows) < 2:
                continue
            keys = [_node_key("asset", "polymarket", row.asset_id) for row in asset_rows]
            for index, left_key in enumerate(keys):
                for right_key in keys[index + 1:]:
                    ensure_edge(EdgeSpec(left_node_key=left_key, right_node_key=right_key, edge_type="complement", weight=Decimal("-1"), source_kind="phase2_registry", details_json={"condition_id": condition_id}))

        groups_by_id = {group.id: group for group in groups}
        for member in group_members:
            group = groups_by_id.get(member.group_id)
            if group is None:
                continue
            synthetic_asset = _synthetic_asset_id(
                asset_id=member.asset_id,
                outcome_id=member.outcome_id,
                condition_id=member.condition_id,
                market_id=member.market_id,
            )
            if synthetic_asset is None:
                continue
            member_node_key = _node_key("asset", member.venue or group.primary_venue or "unknown", synthetic_asset)
            ensure_node(
                NodeSpec(
                    node_key=member_node_key,
                    node_type="asset",
                    venue=member.venue,
                    event_dim_id=member.event_dim_id,
                    market_dim_id=member.market_dim_id,
                    asset_dim_id=member.asset_dim_id,
                    condition_id=member.condition_id,
                    asset_id=synthetic_asset,
                    label=member.outcome_name or synthetic_asset,
                    active=member.active,
                    details_json={"managed_by": "phase10_graph", "member_role": member.member_role, "group_id": member.group_id, "source": "structure_group_member"},
                )
            )
            if member.venue:
                ensure_node(NodeSpec(node_key=_node_key("venue", member.venue), node_type="venue", venue=member.venue, label=member.venue.title(), details_json={"managed_by": "phase10_graph"}))
                ensure_edge(EdgeSpec(left_node_key=member_node_key, right_node_key=_node_key("venue", member.venue), edge_type="same_event", source_kind="structure_engine", details_json={"relationship": "asset_on_venue"}))
            if group.group_type in {"neg_risk_event", "event_sum_parity", "binary_complement", "cross_venue_basis"}:
                group_node_key = _node_key("conversion_group", group.group_key)
                ensure_node(
                    NodeSpec(
                        node_key=group_node_key,
                        node_type="conversion_group",
                        venue=group.primary_venue,
                        event_dim_id=group.event_dim_id,
                        label=group.title or group.group_key,
                        active=group.active,
                        details_json={"managed_by": "phase10_graph", "group_id": group.id, "group_key": group.group_key, "group_type": group.group_type},
                    )
                )
                ensure_edge(
                    EdgeSpec(
                        left_node_key=group_node_key,
                        right_node_key=member_node_key,
                        edge_type="conversion_equivalent",
                        weight=_group_member_weight(group.group_type, member.member_role),
                        source_kind=group.source_kind,
                        details_json={"group_id": group.id, "group_key": group.group_key, "group_type": group.group_type, "member_role": member.member_role},
                    )
                )

        for link in links:
            left_asset = _synthetic_asset_id(asset_id=link.left_asset_id, outcome_id=link.left_outcome_id, condition_id=link.left_condition_id, market_id=link.left_market_id, external_id=link.left_external_id)
            right_asset = _synthetic_asset_id(asset_id=link.right_asset_id, outcome_id=link.right_outcome_id, condition_id=link.right_condition_id, market_id=link.right_market_id, external_id=link.right_external_id)
            if left_asset is None or right_asset is None:
                continue
            left_key = _node_key("asset", link.left_venue, left_asset)
            right_key = _node_key("asset", link.right_venue, right_asset)
            ensure_node(NodeSpec(node_key=left_key, node_type="asset", venue=link.left_venue, condition_id=link.left_condition_id, asset_id=left_asset, label=link.left_symbol or left_asset, active=link.active, details_json={"managed_by": "phase10_graph", "source": "cross_venue_link", "link_key": link.link_key}))
            ensure_node(NodeSpec(node_key=right_key, node_type="asset", venue=link.right_venue, condition_id=link.right_condition_id, asset_id=right_asset, label=link.right_symbol or right_asset, active=link.active, details_json={"managed_by": "phase10_graph", "source": "cross_venue_link", "link_key": link.link_key}))
            ensure_edge(
                EdgeSpec(
                    left_node_key=left_key,
                    right_node_key=right_key,
                    edge_type="cross_venue_hedge",
                    weight=ONE - (Decimal(str(settings.polymarket_cross_venue_hedge_haircut_bps)) / TEN_THOUSAND),
                    source_kind="manual_mapping" if link.mapping_kind in {"manual", "curated"} else "derived",
                    details_json={"link_id": link.id, "link_key": link.link_key, "mapping_kind": link.mapping_kind, "review_status": link.review_status},
                )
            )

        existing_nodes = (await session.execute(select(RiskGraphNode))).scalars().all()
        node_rows_by_key = {row.node_key: row for row in existing_nodes}
        for spec in node_specs.values():
            row = node_rows_by_key.get(spec.node_key)
            if row is None:
                row = RiskGraphNode(node_key=spec.node_key, node_type=spec.node_type)
                session.add(row)
            row.node_type = spec.node_type
            row.venue = spec.venue
            row.event_dim_id = spec.event_dim_id
            row.market_dim_id = spec.market_dim_id
            row.asset_dim_id = spec.asset_dim_id
            row.condition_id = spec.condition_id
            row.asset_id = spec.asset_id
            row.label = spec.label
            row.active = spec.active
            row.details_json = _json_safe(spec.details_json)
            rows_inserted["nodes_upserted"] += 1
        await session.flush()

        all_nodes = (await session.execute(select(RiskGraphNode))).scalars().all()
        node_ids = {row.node_key: row.id for row in all_nodes}
        existing_edges = (await session.execute(select(RiskGraphEdge))).scalars().all()
        edge_rows_by_key = {(row.left_node_id, row.right_node_id, row.edge_type, row.source_kind): row for row in existing_edges}
        for spec in edge_specs.values():
            left_node_id = node_ids[spec.left_node_key]
            right_node_id = node_ids[spec.right_node_key]
            key = (left_node_id, right_node_id, spec.edge_type, spec.source_kind)
            row = edge_rows_by_key.get(key)
            if row is None:
                row = RiskGraphEdge(left_node_id=left_node_id, right_node_id=right_node_id, edge_type=spec.edge_type, source_kind=spec.source_kind)
                session.add(row)
            row.weight = spec.weight
            row.active = spec.active
            row.details_json = _json_safe(spec.details_json)
            rows_inserted["edges_upserted"] += 1

        await _finish_run(session, run, status="completed", rows_inserted_json=rows_inserted, details_json={"events_seen": len(events), "markets_seen": len(markets), "assets_seen": len(assets), "groups_seen": len(groups), "links_seen": len(links)})
        await session.commit()
        polymarket_risk_graph_build_runs.labels(status="completed").inc()
        polymarket_risk_last_successful_timestamp.labels(run_type=GRAPH_BUILD_RUN_TYPE).set(run.completed_at.timestamp())
        return {
            "run": {
                "id": run.id,
                "run_type": run.run_type,
                "reason": run.reason,
                "status": run.status,
                "started_at": run.started_at,
                "completed_at": run.completed_at,
                "scope_json": run.scope_json,
                "rows_inserted_json": run.rows_inserted_json,
                "error_count": run.error_count,
                "details_json": run.details_json,
            },
            "rows_inserted": rows_inserted,
        }
    except Exception as exc:
        await _finish_run(session, run, status="failed", rows_inserted_json=rows_inserted, details_json={"error": str(exc)}, error_count=1)
        await session.commit()
        polymarket_risk_graph_build_runs.labels(status="failed").inc()
        polymarket_risk_graph_build_failures.inc()
        raise


async def _ensure_graph_ready(session: AsyncSession) -> None:
    if not settings.polymarket_risk_graph_enabled:
        return
    node_count = (await session.execute(select(func.count(RiskGraphNode.id)))).scalar_one() or 0
    if int(node_count) == 0:
        await build_risk_graph(session, reason="startup", scope_json={"auto_bootstrap": True})


async def _load_node_context(
    session: AsyncSession,
) -> tuple[
    dict[int, RiskGraphNode],
    dict[str, RiskGraphNode],
    dict[str, RiskGraphNode],
    dict[str, RiskGraphNode],
    dict[int, list[tuple[int, str, Decimal | None, dict[str, Any]]]],
]:
    nodes = (await session.execute(select(RiskGraphNode).where(RiskGraphNode.active.is_(True)))).scalars().all()
    edges = (await session.execute(select(RiskGraphEdge).where(RiskGraphEdge.active.is_(True)))).scalars().all()
    nodes_by_id = {row.id: row for row in nodes}
    nodes_by_key = {row.node_key: row for row in nodes}
    asset_nodes_by_asset_id = {row.asset_id: row for row in nodes if row.node_type == "asset" and row.asset_id}
    market_nodes_by_condition_id = {row.condition_id: row for row in nodes if row.node_type == "market" and row.condition_id}
    adjacency: dict[int, list[tuple[int, str, Decimal | None, dict[str, Any]]]] = {}
    for edge in edges:
        details = edge.details_json if isinstance(edge.details_json, dict) else {}
        adjacency.setdefault(edge.left_node_id, []).append((edge.right_node_id, edge.edge_type, edge.weight, details))
        adjacency.setdefault(edge.right_node_id, []).append((edge.left_node_id, edge.edge_type, edge.weight, details))
    return nodes_by_id, nodes_by_key, asset_nodes_by_asset_id, market_nodes_by_condition_id, adjacency


async def _resolve_base_node(
    session: AsyncSession,
    *,
    nodes_by_key: dict[str, RiskGraphNode],
    asset_nodes_by_asset_id: dict[str, RiskGraphNode],
    market_nodes_by_condition_id: dict[str, RiskGraphNode],
    venue: str,
    asset_id: str | None,
    condition_id: str | None,
    label: str | None,
    outcome_id: uuid.UUID | None = None,
    market_id: uuid.UUID | None = None,
    source_kind: str,
) -> RiskGraphNode | None:
    effective_asset_id = asset_id or _synthetic_asset_id(outcome_id=outcome_id, condition_id=condition_id, market_id=market_id)
    if effective_asset_id:
        node = asset_nodes_by_asset_id.get(effective_asset_id)
        if node is not None:
            return node
        node = await _upsert_runtime_asset_node(session, venue=venue, asset_id=effective_asset_id, condition_id=condition_id, label=label, source_kind=source_kind)
        asset_nodes_by_asset_id[effective_asset_id] = node
        nodes_by_key[node.node_key] = node
        return node
    if condition_id:
        node = market_nodes_by_condition_id.get(condition_id)
        if node is not None:
            return node
        node = await _upsert_runtime_market_node(session, venue=venue, condition_id=condition_id, label=label, source_kind=source_kind)
        market_nodes_by_condition_id[condition_id] = node
        nodes_by_key[node.node_key] = node
        return node
    return None


def _bucket_targets_for_node(
    node: RiskGraphNode,
    *,
    nodes_by_id: dict[int, RiskGraphNode],
    adjacency: dict[int, list[tuple[int, str, Decimal | None, dict[str, Any]]]],
) -> dict[str, list[tuple[int, Decimal]]]:
    buckets: dict[str, list[tuple[int, Decimal]]] = {
        "market": [],
        "event": [],
        "category": [],
        "entity": [],
        "conversion_group": [],
        "venue": [],
    }
    for neighbor_id, edge_type, weight, _details in adjacency.get(node.id, []):
        neighbor = nodes_by_id.get(neighbor_id)
        if neighbor is None:
            continue
        if neighbor.node_type == "market" and edge_type == "same_event":
            buckets["market"].append((neighbor.id, ONE))
        elif neighbor.node_type == "event" and edge_type == "same_event":
            buckets["event"].append((neighbor.id, ONE))
        elif neighbor.node_type == "category" and edge_type == "category_link":
            buckets["category"].append((neighbor.id, ONE))
        elif neighbor.node_type == "entity" and edge_type == "same_entity":
            buckets["entity"].append((neighbor.id, ONE))
        elif neighbor.node_type == "conversion_group" and edge_type == "conversion_equivalent":
            buckets["conversion_group"].append((neighbor.id, weight or ONE))
        elif neighbor.node_type == "venue" and edge_type == "same_event":
            buckets["venue"].append((neighbor.id, ONE))
    if node.venue:
        for maybe_venue in nodes_by_id.values():
            if maybe_venue.node_type == "venue" and maybe_venue.venue == node.venue:
                buckets["venue"].append((maybe_venue.id, ONE))
                break
    return buckets


def _add_to_rollup(rollup: NodeRollup, contribution: ExposureContribution, *, signed_net: Decimal) -> None:
    rollup.gross_notional_usd += contribution.gross_notional_usd
    rollup.net_notional_usd += signed_net
    rollup.buy_notional_usd += contribution.buy_notional_usd
    rollup.sell_notional_usd += contribution.sell_notional_usd
    rollup.share_exposure += contribution.share_exposure
    rollup.reservation_cost_usd += contribution.reservation_cost_usd
    source_id = contribution.details_json.get("source_id")
    if source_id is not None:
        rollup.source_ids.add(source_id)
    rollup.source_kinds.add(contribution.exposure_kind)


def _hedged_fraction_from_gross_and_net(gross: Decimal, net: Decimal) -> Decimal:
    if gross <= ZERO:
        return ZERO
    raw = ONE - (abs(net) / gross)
    if raw < ZERO:
        return ZERO
    if raw > ONE:
        return ONE
    return raw.quantize(RATIO_Q)


async def _build_contributions(
    session: AsyncSession,
    *,
    snapshot_at: datetime,
    nodes_by_id: dict[int, RiskGraphNode],
    nodes_by_key: dict[str, RiskGraphNode],
    asset_nodes_by_asset_id: dict[str, RiskGraphNode],
    market_nodes_by_condition_id: dict[str, RiskGraphNode],
) -> list[ExposureContribution]:
    contributions: list[ExposureContribution] = []
    asset_by_outcome: dict[uuid.UUID, PolymarketAssetDim] = {
        row.outcome_id: row
        for row in (await session.execute(select(PolymarketAssetDim).where(PolymarketAssetDim.outcome_id.is_not(None)))).scalars().all()
        if row.outcome_id is not None
    }
    market_by_id = {row.id: row for row in (await session.execute(select(Market))).scalars().all()}

    if settings.polymarket_risk_graph_include_paper_positions:
        paper_trades = (await session.execute(select(PaperTrade).where(PaperTrade.status.in_(tuple(OPEN_PAPER_TRADE_STATUSES))))).scalars().all()
        for trade in paper_trades:
            asset_dim = asset_by_outcome.get(trade.outcome_id)
            market = market_by_id.get(trade.market_id)
            venue = market.platform if market is not None else "polymarket"
            asset_id = asset_dim.asset_id if asset_dim is not None else _synthetic_asset_id(outcome_id=trade.outcome_id)
            condition_id = asset_dim.condition_id if asset_dim is not None else None
            base_node = await _resolve_base_node(session, nodes_by_key=nodes_by_key, asset_nodes_by_asset_id=asset_nodes_by_asset_id, market_nodes_by_condition_id=market_nodes_by_condition_id, venue=venue, asset_id=asset_id, condition_id=condition_id, label=(asset_dim.outcome_name if asset_dim is not None else None) or asset_id, outcome_id=trade.outcome_id, market_id=trade.market_id, source_kind="paper_position")
            if base_node is None:
                continue
            signed = _direction_sign(direction=trade.direction, side=None, outcome_name=asset_dim.outcome_name if asset_dim is not None else None) * trade.size_usd
            contributions.append(ExposureContribution(base_node_id=base_node.id, exposure_kind="paper_position", gross_notional_usd=trade.size_usd, canonical_net_notional_usd=signed, buy_notional_usd=trade.size_usd if signed >= ZERO else ZERO, sell_notional_usd=trade.size_usd if signed < ZERO else ZERO, share_exposure=trade.shares or ZERO, reservation_cost_usd=ZERO, inventory_bucket="taker", details_json={"source_id": str(trade.id), "opened_at": trade.opened_at, "direction": trade.direction, "venue": venue}))

    if settings.polymarket_risk_graph_include_live_orders:
        live_orders = (await session.execute(select(LiveOrder))).scalars().all()
        for order in live_orders:
            if order.status in TERMINAL_LIVE_ORDER_STATUSES:
                continue
            requested_size = _to_decimal(order.submitted_size) or _to_decimal(order.requested_size) or ZERO
            filled_size = _to_decimal(order.filled_size) or ZERO
            open_size = max(requested_size - filled_size, ZERO)
            if open_size <= ZERO:
                continue
            entry_price = _to_decimal(order.target_price) or _to_decimal(order.limit_price) or _to_decimal(order.avg_fill_price)
            notional = (open_size * entry_price).quantize(SIZE_Q) if entry_price is not None else open_size
            base_node = await _resolve_base_node(session, nodes_by_key=nodes_by_key, asset_nodes_by_asset_id=asset_nodes_by_asset_id, market_nodes_by_condition_id=market_nodes_by_condition_id, venue="polymarket", asset_id=order.asset_id, condition_id=order.condition_id, label=order.asset_id, source_kind="live_order")
            if base_node is None:
                continue
            sign = _direction_sign(direction=order.side, side=order.side, outcome_name=None)
            contributions.append(ExposureContribution(base_node_id=base_node.id, exposure_kind="live_order", gross_notional_usd=notional, canonical_net_notional_usd=sign * notional, buy_notional_usd=notional if sign >= ZERO else ZERO, sell_notional_usd=notional if sign < ZERO else ZERO, share_exposure=open_size, reservation_cost_usd=ZERO, inventory_bucket=_quote_inventory_bucket("live_order", post_only=bool(order.post_only), action_type=order.action_type), details_json={"source_id": str(order.id), "side": order.side, "post_only": order.post_only, "action_type": order.action_type, "status": order.status}))

    if settings.polymarket_risk_graph_include_reservations:
        reservations = (await session.execute(select(CapitalReservation))).scalars().all()
        for reservation in reservations:
            if reservation.status in RELEASED_RESERVATION_STATUSES:
                continue
            open_amount = _to_decimal(reservation.open_amount) or ZERO
            if open_amount <= ZERO:
                continue
            base_node = await _resolve_base_node(session, nodes_by_key=nodes_by_key, asset_nodes_by_asset_id=asset_nodes_by_asset_id, market_nodes_by_condition_id=market_nodes_by_condition_id, venue="polymarket", asset_id=reservation.asset_id, condition_id=reservation.condition_id, label=reservation.asset_id or reservation.condition_id, source_kind="capital_reservation")
            if base_node is None:
                continue
            contributions.append(ExposureContribution(base_node_id=base_node.id, exposure_kind="capital_reservation", gross_notional_usd=open_amount, canonical_net_notional_usd=ZERO, buy_notional_usd=ZERO, sell_notional_usd=ZERO, share_exposure=ZERO, reservation_cost_usd=open_amount, inventory_bucket="taker", details_json={"source_id": reservation.id, "reservation_kind": reservation.reservation_kind, "status": reservation.status}))

    structure_orders = (await session.execute(select(MarketStructurePaperOrder).join(MarketStructurePaperPlan, MarketStructurePaperPlan.id == MarketStructurePaperOrder.plan_id).where(MarketStructurePaperPlan.status.in_(tuple(OPEN_STRUCTURE_PLAN_STATUSES))))).scalars().all()
    for order in structure_orders:
        notional = _to_decimal(order.planned_notional) or (((_to_decimal(order.target_size) or ZERO) * (_to_decimal(order.planned_entry_price) or ZERO)).quantize(SIZE_Q))
        if notional <= ZERO:
            continue
        base_node = await _resolve_base_node(session, nodes_by_key=nodes_by_key, asset_nodes_by_asset_id=asset_nodes_by_asset_id, market_nodes_by_condition_id=market_nodes_by_condition_id, venue=order.venue, asset_id=order.asset_id, condition_id=order.condition_id, label=order.asset_id or order.condition_id, outcome_id=order.outcome_id, market_id=order.market_id, source_kind="structure_plan")
        if base_node is None:
            continue
        sign = _direction_sign(direction=order.side, side=order.side, outcome_name=None)
        contributions.append(ExposureContribution(base_node_id=base_node.id, exposure_kind="structure_plan", gross_notional_usd=notional, canonical_net_notional_usd=sign * notional, buy_notional_usd=notional if sign >= ZERO else ZERO, sell_notional_usd=notional if sign < ZERO else ZERO, share_exposure=_to_decimal(order.target_size) or ZERO, reservation_cost_usd=ZERO, inventory_bucket="taker", details_json={"source_id": order.id, "plan_id": str(order.plan_id), "side": order.side, "role": order.role, "venue": order.venue}))

    quote_cutoff = snapshot_at - timedelta(seconds=settings.polymarket_quote_optimizer_max_age_seconds)
    recent_quotes = (await session.execute(select(PolymarketQuoteRecommendation).where(PolymarketQuoteRecommendation.created_at >= quote_cutoff, PolymarketQuoteRecommendation.recommendation_action == "recommend_quote").order_by(PolymarketQuoteRecommendation.asset_id.asc(), PolymarketQuoteRecommendation.created_at.desc()))).scalars().all()
    latest_quote_by_key: dict[tuple[str, str], PolymarketQuoteRecommendation] = {}
    for quote in recent_quotes:
        latest_quote_by_key.setdefault((quote.condition_id, quote.asset_id), quote)
    for quote in latest_quote_by_key.values():
        notional = _to_decimal(quote.recommended_notional) or ZERO
        if notional <= ZERO:
            continue
        base_node = await _resolve_base_node(session, nodes_by_key=nodes_by_key, asset_nodes_by_asset_id=asset_nodes_by_asset_id, market_nodes_by_condition_id=market_nodes_by_condition_id, venue="polymarket", asset_id=quote.asset_id, condition_id=quote.condition_id, label=quote.asset_id, source_kind="maker_quote")
        if base_node is None:
            continue
        sign = _direction_sign(direction=quote.recommended_side, side=quote.recommended_side, outcome_name=None)
        contributions.append(ExposureContribution(base_node_id=base_node.id, exposure_kind="maker_quote", gross_notional_usd=notional, canonical_net_notional_usd=sign * notional, buy_notional_usd=notional if sign >= ZERO else ZERO, sell_notional_usd=notional if sign < ZERO else ZERO, share_exposure=_to_decimal(quote.recommended_size) or ZERO, reservation_cost_usd=ZERO, inventory_bucket="maker", details_json={"source_id": str(quote.id), "recommended_side": quote.recommended_side, "recommendation_status": quote.status}))

    return contributions


async def _compute_exposure_state(
    session: AsyncSession,
    *,
    snapshot_at: datetime | None = None,
) -> ExposureState:
    effective_snapshot_at = _ensure_utc(snapshot_at) or _utcnow()
    await _ensure_graph_ready(session)
    nodes_by_id, nodes_by_key, asset_nodes_by_asset_id, market_nodes_by_condition_id, adjacency = await _load_node_context(session)
    contributions = await _build_contributions(session, snapshot_at=effective_snapshot_at, nodes_by_id=nodes_by_id, nodes_by_key=nodes_by_key, asset_nodes_by_asset_id=asset_nodes_by_asset_id, market_nodes_by_condition_id=market_nodes_by_condition_id)
    nodes_by_id, nodes_by_key, asset_nodes_by_asset_id, market_nodes_by_condition_id, adjacency = await _load_node_context(session)

    aggregate_rollups: dict[int, NodeRollup] = {node_id: NodeRollup(node=node) for node_id, node in nodes_by_id.items()}
    source_rows: list[dict[str, Any]] = []
    maker_budget_used = ZERO
    taker_budget_used = ZERO
    asset_group_weights: dict[int, list[tuple[int, Decimal, str, str]]] = {}
    asset_bucket_membership_sets: dict[int, dict[str, set[int]]] = {}
    asset_hedge_link_sets: dict[int, set[tuple[int, str]]] = {}
    asset_hedge_links: dict[int, list[tuple[int, Decimal, str]]] = {}

    for contribution in contributions:
        base_node = nodes_by_id.get(contribution.base_node_id)
        if base_node is None:
            continue
        _add_to_rollup(aggregate_rollups[base_node.id], contribution, signed_net=contribution.canonical_net_notional_usd)
        if contribution.inventory_bucket == "maker":
            maker_budget_used += contribution.gross_notional_usd
        elif contribution.inventory_bucket == "taker":
            taker_budget_used += contribution.gross_notional_usd

        bucket_targets = _bucket_targets_for_node(base_node, nodes_by_id=nodes_by_id, adjacency=adjacency)
        memberships = asset_bucket_membership_sets.setdefault(
            base_node.id,
            {bucket: set() for bucket in ("market", "event", "category", "entity", "conversion_group", "venue")},
        )
        for target_id, _weight in bucket_targets["market"]:
            memberships["market"].add(target_id)
            _add_to_rollup(aggregate_rollups[target_id], contribution, signed_net=contribution.canonical_net_notional_usd)
        for target_id, _weight in bucket_targets["event"]:
            memberships["event"].add(target_id)
            _add_to_rollup(aggregate_rollups[target_id], contribution, signed_net=contribution.canonical_net_notional_usd)
        for target_id, _weight in bucket_targets["category"]:
            memberships["category"].add(target_id)
            _add_to_rollup(aggregate_rollups[target_id], contribution, signed_net=contribution.canonical_net_notional_usd)
        for target_id, _weight in bucket_targets["entity"]:
            memberships["entity"].add(target_id)
            _add_to_rollup(aggregate_rollups[target_id], contribution, signed_net=contribution.canonical_net_notional_usd)
        for target_id, _weight in bucket_targets["venue"]:
            memberships["venue"].add(target_id)
            _add_to_rollup(aggregate_rollups[target_id], contribution, signed_net=contribution.canonical_net_notional_usd)
        for target_id, weight in bucket_targets["conversion_group"]:
            memberships["conversion_group"].add(target_id)
            _add_to_rollup(aggregate_rollups[target_id], contribution, signed_net=(weight or ONE) * contribution.gross_notional_usd)
            details = aggregate_rollups[target_id].node.details_json if isinstance(aggregate_rollups[target_id].node.details_json, dict) else {}
            asset_group_weights.setdefault(base_node.id, []).append((target_id, weight or ONE, str(details.get("group_type") or ""), str(details.get("group_key") or "")))

        for neighbor_id, edge_type, weight, _details in adjacency.get(base_node.id, []):
            neighbor = nodes_by_id.get(neighbor_id)
            if neighbor is None or neighbor.node_type != "asset":
                continue
            if edge_type not in {"complement", "cross_venue_hedge"}:
                continue
            seen = asset_hedge_link_sets.setdefault(base_node.id, set())
            key = (neighbor_id, edge_type)
            if key in seen:
                continue
            seen.add(key)
            asset_hedge_links.setdefault(base_node.id, []).append((neighbor_id, abs(weight or ONE), edge_type))

        source_rows.append({
            "node_id": base_node.id,
            "exposure_kind": contribution.exposure_kind,
            "gross_notional_usd": contribution.gross_notional_usd,
            "net_notional_usd": contribution.canonical_net_notional_usd,
            "buy_notional_usd": contribution.buy_notional_usd,
            "sell_notional_usd": contribution.sell_notional_usd,
            "share_exposure": contribution.share_exposure,
            "reservation_cost_usd": contribution.reservation_cost_usd,
            "hedged_fraction": ZERO,
            "details_json": contribution.details_json,
        })

    for rollup in aggregate_rollups.values():
        rollup.hedged_fraction = _hedged_fraction_from_gross_and_net(rollup.gross_notional_usd, rollup.net_notional_usd)
    for node_id, hedge_links in asset_hedge_links.items():
        rollup = aggregate_rollups.get(node_id)
        if rollup is None or rollup.gross_notional_usd <= ZERO or rollup.net_notional_usd == ZERO:
            continue
        remaining = abs(rollup.net_notional_usd)
        explicit_hedged = ZERO
        for neighbor_id, hedge_weight, _edge_type in hedge_links:
            if remaining <= ZERO:
                break
            partner = aggregate_rollups.get(neighbor_id)
            if partner is None or partner.net_notional_usd == ZERO:
                continue
            if rollup.net_notional_usd * partner.net_notional_usd >= ZERO:
                continue
            overlap = min(abs(rollup.net_notional_usd), abs(partner.net_notional_usd))
            covered = min(remaining, overlap * max(min(hedge_weight, ONE), ZERO))
            explicit_hedged += covered
            remaining -= covered
        if explicit_hedged <= ZERO:
            continue
        explicit_fraction = _hedged_fraction_from_gross_and_net(
            rollup.gross_notional_usd,
            max(abs(rollup.net_notional_usd) - explicit_hedged, ZERO),
        )
        rollup.hedged_fraction = max(rollup.hedged_fraction, explicit_fraction)

    maker_budget_remaining = max(Decimal(str(settings.polymarket_maker_inventory_budget_usd)) - maker_budget_used, ZERO)
    taker_budget_remaining = max(Decimal(str(settings.polymarket_taker_inventory_budget_usd)) - taker_budget_used, ZERO)
    asset_bucket_memberships = {
        node_id: {bucket: sorted(target_ids) for bucket, target_ids in bucket_sets.items()}
        for node_id, bucket_sets in asset_bucket_membership_sets.items()
    }
    return ExposureState(snapshot_at=effective_snapshot_at, nodes_by_id=nodes_by_id, aggregate_rollups=aggregate_rollups, source_rollups=source_rows, maker_budget_used_usd=maker_budget_used.quantize(SIZE_Q), taker_budget_used_usd=taker_budget_used.quantize(SIZE_Q), maker_budget_remaining_usd=maker_budget_remaining.quantize(SIZE_Q), taker_budget_remaining_usd=taker_budget_remaining.quantize(SIZE_Q), asset_group_weights=asset_group_weights, asset_bucket_memberships=asset_bucket_memberships, asset_hedge_links=asset_hedge_links)


async def create_exposure_snapshot(
    session: AsyncSession,
    *,
    reason: str = DEFAULT_REASON,
    snapshot_at: datetime | None = None,
    scope_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    run = await _start_run(session, run_type=EXPOSURE_SNAPSHOT_RUN_TYPE, reason=reason, scope_json=scope_json or {})
    rows_inserted = {"source_rows": 0, "aggregate_rows": 0}
    try:
        state = await _compute_exposure_state(session, snapshot_at=snapshot_at)
        for row in state.source_rollups:
            session.add(PortfolioExposureSnapshot(run_id=run.id, snapshot_at=state.snapshot_at, node_id=row["node_id"], exposure_kind=row["exposure_kind"], gross_notional_usd=row["gross_notional_usd"], net_notional_usd=row["net_notional_usd"], buy_notional_usd=row["buy_notional_usd"], sell_notional_usd=row["sell_notional_usd"], share_exposure=row["share_exposure"], reservation_cost_usd=row["reservation_cost_usd"], hedged_fraction=row["hedged_fraction"], details_json=_json_safe(row["details_json"])))
            rows_inserted["source_rows"] += 1

        for node_id, rollup in state.aggregate_rollups.items():
            if rollup.gross_notional_usd <= ZERO and rollup.reservation_cost_usd <= ZERO:
                continue
            session.add(PortfolioExposureSnapshot(run_id=run.id, snapshot_at=state.snapshot_at, node_id=node_id, exposure_kind="aggregate", gross_notional_usd=rollup.gross_notional_usd.quantize(SIZE_Q), net_notional_usd=rollup.net_notional_usd.quantize(SIZE_Q), buy_notional_usd=rollup.buy_notional_usd.quantize(SIZE_Q), sell_notional_usd=rollup.sell_notional_usd.quantize(SIZE_Q), share_exposure=rollup.share_exposure.quantize(SIZE_Q), reservation_cost_usd=rollup.reservation_cost_usd.quantize(SIZE_Q), hedged_fraction=rollup.hedged_fraction.quantize(RATIO_Q), details_json={"node_key": rollup.node.node_key, "node_type": rollup.node.node_type, "source_kinds": sorted(rollup.source_kinds), "source_count": len(rollup.source_ids)}))
            rows_inserted["aggregate_rows"] += 1

        await _finish_run(session, run, status="completed", rows_inserted_json=rows_inserted, details_json={"maker_budget_used_usd": _serialize_decimal(state.maker_budget_used_usd), "taker_budget_used_usd": _serialize_decimal(state.taker_budget_used_usd)})
        await session.commit()
        polymarket_risk_exposure_snapshot_runs.labels(status="completed").inc()
        polymarket_risk_last_successful_timestamp.labels(run_type=EXPOSURE_SNAPSHOT_RUN_TYPE).set(run.completed_at.timestamp())
        return {
            "run": {
                "id": run.id,
                "run_type": run.run_type,
                "reason": run.reason,
                "status": run.status,
                "started_at": run.started_at,
                "completed_at": run.completed_at,
                "scope_json": run.scope_json,
                "rows_inserted_json": run.rows_inserted_json,
                "error_count": run.error_count,
                "details_json": run.details_json,
            },
            "rows_inserted": rows_inserted,
        }
    except Exception as exc:
        await _finish_run(session, run, status="failed", rows_inserted_json=rows_inserted, details_json={"error": str(exc)}, error_count=1)
        await session.commit()
        polymarket_risk_exposure_snapshot_runs.labels(status="failed").inc()
        polymarket_risk_exposure_snapshot_failures.inc()
        raise


def _find_related_bucket_rollups(
    state: ExposureState,
    node: RiskGraphNode,
) -> dict[str, list[NodeRollup]]:
    result: dict[str, list[NodeRollup]] = {
        "event": [],
        "entity": [],
        "conversion_group": [],
        "venue": [],
        "category": [],
        "market": [],
        "asset": [],
    }
    own_rollup = state.aggregate_rollups.get(node.id)
    if own_rollup is not None:
        result["asset"].append(own_rollup)
    memberships = state.asset_bucket_memberships.get(node.id, {})
    for bucket_name in ("event", "entity", "conversion_group", "venue", "category", "market"):
        for target_id in memberships.get(bucket_name, []):
            rollup = state.aggregate_rollups.get(target_id)
            if rollup is not None:
                result[bucket_name].append(rollup)
    if any(result[bucket_name] for bucket_name in ("event", "entity", "conversion_group", "venue", "category", "market")):
        return result
    for rollup in state.aggregate_rollups.values():
        if rollup.node.id == node.id:
            continue
        if rollup.node.node_type == "event" and node.event_dim_id is not None and rollup.node.event_dim_id == node.event_dim_id:
            result["event"].append(rollup)
        elif rollup.node.node_type == "entity":
            details = rollup.node.details_json if isinstance(rollup.node.details_json, dict) else {}
            node_details = node.details_json if isinstance(node.details_json, dict) else {}
            if details.get("slug") and details.get("slug") in str(node_details):
                result["entity"].append(rollup)
        elif rollup.node.node_type == "venue" and node.venue is not None and rollup.node.venue == node.venue:
            result["venue"].append(rollup)
        elif rollup.node.node_type == "market" and node.condition_id is not None and rollup.node.condition_id == node.condition_id:
            result["market"].append(rollup)
        elif rollup.node.node_type == "conversion_group":
            memberships = state.asset_group_weights.get(node.id, [])
            if any(group_id == rollup.node.id for group_id, _weight, _group_type, _group_key in memberships):
                result["conversion_group"].append(rollup)
    return result


def _evaluate_control_for_asset(
    state: ExposureState,
    *,
    node: RiskGraphNode,
    proposed_notional_usd: Decimal,
    direction_sign: Decimal,
    context_kind: str,
    scope_kind: str,
) -> dict[str, Any]:
    related = _find_related_bucket_rollups(state, node)
    reason_codes: list[str] = []
    recommendation_type = "allow"
    quote_skew_direction = "none"
    penalty_bps = ZERO
    reservation_adjustment_bps = ZERO
    target_size_cap_usd = proposed_notional_usd

    event_cap = Decimal(str(settings.polymarket_max_event_exposure_usd))
    entity_cap = Decimal(str(settings.polymarket_max_entity_exposure_usd))
    conversion_cap = Decimal(str(settings.polymarket_max_conversion_group_exposure_usd))
    maker_budget = Decimal(str(settings.polymarket_maker_inventory_budget_usd))
    taker_budget = Decimal(str(settings.polymarket_taker_inventory_budget_usd))

    asset_rollup = state.aggregate_rollups.get(node.id)
    current_asset_net = asset_rollup.net_notional_usd if asset_rollup is not None else ZERO
    projected_asset_net = current_asset_net + (direction_sign * proposed_notional_usd)
    asset_hedged_fraction = asset_rollup.hedged_fraction if asset_rollup is not None else ZERO

    def enforce_cap(rollups: list[NodeRollup], cap: Decimal, reason_code: str, signed_notional: Decimal) -> None:
        nonlocal recommendation_type, target_size_cap_usd
        if cap <= ZERO:
            return
        for rollup in rollups:
            projected = rollup.net_notional_usd + signed_notional
            current_abs = abs(rollup.net_notional_usd)
            projected_abs = abs(projected)
            if projected_abs <= cap:
                continue
            remaining = max(cap - current_abs, ZERO)
            reason_codes.append(reason_code)
            if current_abs >= cap or remaining <= ZERO:
                recommendation_type = "no_quote" if context_kind == "maker_quote" else "block"
                target_size_cap_usd = ZERO
                return
            recommendation_type = "reduce_size"
            target_size_cap_usd = min(target_size_cap_usd, remaining)

    enforce_cap(related.get("event", []), event_cap, "event_cap_exceeded", direction_sign * proposed_notional_usd)
    enforce_cap(related.get("entity", []), entity_cap, "entity_cap_exceeded", direction_sign * proposed_notional_usd)

    for group_id, weight, _group_type, _group_key in state.asset_group_weights.get(node.id, []):
        group_rollup = state.aggregate_rollups.get(group_id)
        if group_rollup is None:
            continue
        projected = group_rollup.net_notional_usd + ((weight or ONE) * proposed_notional_usd)
        current_abs = abs(group_rollup.net_notional_usd)
        if abs(projected) <= conversion_cap:
            continue
        remaining = max(conversion_cap - current_abs, ZERO)
        reason_codes.append("conversion_group_cap_exceeded")
        if current_abs >= conversion_cap or remaining <= ZERO:
            recommendation_type = "no_quote" if context_kind == "maker_quote" else "block"
            target_size_cap_usd = ZERO
            break
        recommendation_type = "reduce_size"
        target_size_cap_usd = min(target_size_cap_usd, remaining)

    maker_projected = state.maker_budget_used_usd + (proposed_notional_usd if context_kind == "maker_quote" else ZERO)
    taker_projected = state.taker_budget_used_usd + (proposed_notional_usd if context_kind != "maker_quote" else ZERO)
    maker_utilization = (maker_projected / maker_budget) if maker_budget > ZERO else ONE
    taker_utilization = (taker_projected / taker_budget) if taker_budget > ZERO else ONE
    max_utilization = max(maker_utilization, taker_utilization)
    penalty_bps = (Decimal("50") * max_utilization).quantize(BPS_Q)
    if asset_hedged_fraction < Decimal("0.25"):
        penalty_bps += Decimal("12.5000")
        reason_codes.append("hedge_incomplete")
    if max_utilization >= Decimal(str(settings.polymarket_no_quote_toxicity_threshold)):
        reason_codes.append("inventory_toxicity_exceeded")
        recommendation_type = "no_quote" if context_kind == "maker_quote" else "block"
        target_size_cap_usd = ZERO
        quote_skew_direction = "both_wider" if context_kind == "maker_quote" else "none"
    elif max_utilization >= Decimal("0.75") and recommendation_type == "allow":
        recommendation_type = "skew_quote" if context_kind == "maker_quote" else "hedge_preferred"

    if projected_asset_net == ZERO:
        reservation_adjustment_bps = ZERO
    elif abs(projected_asset_net) > abs(current_asset_net):
        reservation_adjustment_bps = -penalty_bps
        if context_kind == "maker_quote":
            quote_skew_direction = "bid_down"
    else:
        reservation_adjustment_bps = min(penalty_bps / Decimal("2"), Decimal("15.0000"))
        if context_kind == "maker_quote":
            quote_skew_direction = "maker_bias_buy" if direction_sign > ZERO else "maker_bias_sell"

    if recommendation_type == "allow" and asset_hedged_fraction < Decimal("0.20") and proposed_notional_usd > ZERO:
        recommendation_type = "hedge_preferred"

    reason_code = _combine_reason_code(sorted(set(reason_codes)))
    return {
        "recommendation_type": recommendation_type,
        "scope_kind": scope_kind,
        "reason_code": reason_code,
        "reason_codes": sorted(set(reason_codes)),
        "target_size_cap_usd": max(target_size_cap_usd, ZERO).quantize(SIZE_Q),
        "inventory_penalty_bps": penalty_bps.quantize(BPS_Q),
        "reservation_price_adjustment_bps": reservation_adjustment_bps.quantize(BPS_Q),
        "maker_budget_remaining_usd": state.maker_budget_remaining_usd,
        "taker_budget_remaining_usd": state.taker_budget_remaining_usd,
        "quote_skew_direction": quote_skew_direction,
        "no_quote": recommendation_type in {"no_quote", "block"} and context_kind == "maker_quote",
        "asset_net_notional_usd": current_asset_net.quantize(SIZE_Q),
        "projected_asset_net_notional_usd": projected_asset_net.quantize(SIZE_Q),
        "asset_hedged_fraction": asset_hedged_fraction.quantize(RATIO_Q),
    }


async def compute_quote_inventory_controls(
    session: AsyncSession,
    *,
    condition_id: str,
    asset_id: str,
    recommended_side: str | None,
    recommended_notional: Decimal | None,
    snapshot_at: datetime | None = None,
) -> dict[str, Any]:
    if not settings.polymarket_risk_graph_enabled:
        return {
            "applied": False,
            "recommendation_type": "allow",
            "reason_code": "risk_graph_disabled",
            "reason_codes": [],
            "target_size_cap_usd": _to_decimal(recommended_notional) or ZERO,
            "inventory_penalty_bps": ZERO,
            "reservation_price_adjustment_bps": ZERO,
            "maker_budget_remaining_usd": Decimal(str(settings.polymarket_maker_inventory_budget_usd)),
            "taker_budget_remaining_usd": Decimal(str(settings.polymarket_taker_inventory_budget_usd)),
            "quote_skew_direction": "none",
            "no_quote": False,
        }
    state = await _compute_exposure_state(session, snapshot_at=snapshot_at)
    node = next((rollup.node for rollup in state.aggregate_rollups.values() if rollup.node.node_type == "asset" and rollup.node.condition_id == condition_id and rollup.node.asset_id == asset_id), None)
    if node is None:
        node = await _upsert_runtime_asset_node(session, venue="polymarket", asset_id=asset_id, condition_id=condition_id, label=asset_id, source_kind="maker_quote")
        state = await _compute_exposure_state(session, snapshot_at=snapshot_at)
    controls = _evaluate_control_for_asset(state, node=node, proposed_notional_usd=_to_decimal(recommended_notional) or ZERO, direction_sign=_direction_sign(direction=recommended_side, side=recommended_side, outcome_name=node.label), context_kind="maker_quote", scope_kind="asset")
    return {"applied": True, **controls}


async def run_portfolio_optimizer(
    session: AsyncSession,
    *,
    reason: str = DEFAULT_REASON,
    observed_at: datetime | None = None,
    scope_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    run = await _start_run(session, run_type=PORTFOLIO_OPTIMIZE_RUN_TYPE, reason=reason, scope_json=scope_json or {})
    rows_inserted = {"recommendations": 0, "inventory_controls": 0}
    try:
        state = await _compute_exposure_state(session, snapshot_at=observed_at)
        observed = _ensure_utc(observed_at) or state.snapshot_at
        recent_quotes = (await session.execute(select(PolymarketQuoteRecommendation).where(PolymarketQuoteRecommendation.created_at >= observed - SNAPSHOT_LOOKBACK).order_by(PolymarketQuoteRecommendation.created_at.desc()))).scalars().all()
        seen_assets: set[tuple[str, str]] = set()

        session.add(InventoryControlSnapshot(snapshot_at=observed, control_scope="global", maker_budget_usd=Decimal(str(settings.polymarket_maker_inventory_budget_usd)), taker_budget_usd=Decimal(str(settings.polymarket_taker_inventory_budget_usd)), maker_budget_used_usd=state.maker_budget_used_usd, taker_budget_used_usd=state.taker_budget_used_usd, reservation_price_shift_bps=ZERO, quote_skew_direction="none", no_quote=False, reason_code="inventory_budget_snapshot", details_json={"maker_budget_remaining_usd": _serialize_decimal(state.maker_budget_remaining_usd), "taker_budget_remaining_usd": _serialize_decimal(state.taker_budget_remaining_usd)}))
        rows_inserted["inventory_controls"] += 1

        for quote in recent_quotes:
            if not quote.condition_id or not quote.asset_id:
                continue
            key = (quote.condition_id, quote.asset_id)
            if key in seen_assets:
                continue
            seen_assets.add(key)
            node = next((rollup.node for rollup in state.aggregate_rollups.values() if rollup.node.node_type == "asset" and rollup.node.condition_id == quote.condition_id and rollup.node.asset_id == quote.asset_id), None)
            if node is None:
                continue
            controls = _evaluate_control_for_asset(state, node=node, proposed_notional_usd=_to_decimal(quote.recommended_notional) or ZERO, direction_sign=_direction_sign(direction=quote.recommended_side, side=quote.recommended_side, outcome_name=node.label), context_kind="maker_quote", scope_kind="asset")
            session.add(PortfolioOptimizerRecommendation(run_id=run.id, node_id=node.id, recommendation_type=controls["recommendation_type"], scope_kind="asset", condition_id=quote.condition_id, asset_id=quote.asset_id, target_size_cap_usd=controls["target_size_cap_usd"], inventory_penalty_bps=controls["inventory_penalty_bps"], reservation_price_adjustment_bps=controls["reservation_price_adjustment_bps"], maker_budget_remaining_usd=controls["maker_budget_remaining_usd"], taker_budget_remaining_usd=controls["taker_budget_remaining_usd"], reason_code=controls["reason_code"], observed_at_local=observed, details_json={"reason_codes": controls["reason_codes"], "quote_skew_direction": controls["quote_skew_direction"], "no_quote": controls["no_quote"], "asset_net_notional_usd": _serialize_decimal(controls["asset_net_notional_usd"]), "asset_hedged_fraction": _serialize_decimal(controls["asset_hedged_fraction"]), "quote_recommendation_id": str(quote.id)}))
            rows_inserted["recommendations"] += 1
            session.add(InventoryControlSnapshot(snapshot_at=observed, condition_id=quote.condition_id, asset_id=quote.asset_id, control_scope="asset", maker_budget_usd=Decimal(str(settings.polymarket_maker_inventory_budget_usd)), taker_budget_usd=Decimal(str(settings.polymarket_taker_inventory_budget_usd)), maker_budget_used_usd=state.maker_budget_used_usd, taker_budget_used_usd=state.taker_budget_used_usd, reservation_price_shift_bps=controls["reservation_price_adjustment_bps"], quote_skew_direction=controls["quote_skew_direction"], no_quote=controls["no_quote"], reason_code=controls["reason_code"], details_json={"reason_codes": controls["reason_codes"], "target_size_cap_usd": _serialize_decimal(controls["target_size_cap_usd"])}))
            rows_inserted["inventory_controls"] += 1
            polymarket_risk_optimizer_recommendations.labels(recommendation_type=controls["recommendation_type"], reason_code=controls["reason_code"]).inc()
            if controls["recommendation_type"] in {"no_quote", "block"}:
                polymarket_risk_no_quote_recommendations.inc()

        top_rollups = sorted((rollup for rollup in state.aggregate_rollups.values() if rollup.node.node_type in {"event", "entity", "conversion_group"} and rollup.gross_notional_usd > ZERO), key=lambda row: row.gross_notional_usd * (ONE - row.hedged_fraction), reverse=True)[:10]
        for rollup in top_rollups:
            cap = Decimal(str(settings.polymarket_max_event_exposure_usd))
            if rollup.node.node_type == "entity":
                cap = Decimal(str(settings.polymarket_max_entity_exposure_usd))
            elif rollup.node.node_type == "conversion_group":
                cap = Decimal(str(settings.polymarket_max_conversion_group_exposure_usd))
            concentration = ((rollup.gross_notional_usd * (ONE - rollup.hedged_fraction)) / cap) if cap > ZERO else ONE
            polymarket_risk_concentration.labels(bucket_type=rollup.node.node_type, node_key=rollup.node.node_key).set(float(concentration))

        maker_util = (state.maker_budget_used_usd / Decimal(str(settings.polymarket_maker_inventory_budget_usd))) if Decimal(str(settings.polymarket_maker_inventory_budget_usd)) > ZERO else ONE
        taker_util = (state.taker_budget_used_usd / Decimal(str(settings.polymarket_taker_inventory_budget_usd))) if Decimal(str(settings.polymarket_taker_inventory_budget_usd)) > ZERO else ONE
        polymarket_risk_inventory_budget_utilization.labels(bucket="maker").set(float(maker_util))
        polymarket_risk_inventory_budget_utilization.labels(bucket="taker").set(float(taker_util))

        await _finish_run(session, run, status="completed", rows_inserted_json=rows_inserted, details_json={"maker_budget_used_usd": _serialize_decimal(state.maker_budget_used_usd), "taker_budget_used_usd": _serialize_decimal(state.taker_budget_used_usd)})
        await session.commit()
        polymarket_risk_optimizer_runs.labels(status="completed").inc()
        polymarket_risk_last_successful_timestamp.labels(run_type=PORTFOLIO_OPTIMIZE_RUN_TYPE).set(run.completed_at.timestamp())
        return {
            "run": {
                "id": run.id,
                "run_type": run.run_type,
                "reason": run.reason,
                "status": run.status,
                "started_at": run.started_at,
                "completed_at": run.completed_at,
                "scope_json": run.scope_json,
                "rows_inserted_json": run.rows_inserted_json,
                "error_count": run.error_count,
                "details_json": run.details_json,
            },
            "rows_inserted": rows_inserted,
        }
    except Exception as exc:
        await _finish_run(session, run, status="failed", rows_inserted_json=rows_inserted, details_json={"error": str(exc)}, error_count=1)
        await session.commit()
        polymarket_risk_optimizer_runs.labels(status="failed").inc()
        polymarket_risk_optimizer_failures.inc()
        raise


async def fetch_polymarket_risk_graph_status(session: AsyncSession) -> dict[str, Any]:
    now = _utcnow()
    latest_runs = (await session.execute(select(RiskGraphRun).order_by(RiskGraphRun.started_at.desc()).limit(25))).scalars().all()
    by_type: dict[str, RiskGraphRun] = {}
    for row in latest_runs:
        by_type.setdefault(row.run_type, row)

    latest_snapshot_run = by_type.get(EXPOSURE_SNAPSHOT_RUN_TYPE)
    latest_optimizer_run = by_type.get(PORTFOLIO_OPTIMIZE_RUN_TYPE)
    latest_graph_run = by_type.get(GRAPH_BUILD_RUN_TYPE)

    latest_success_snapshot = (await session.execute(select(PortfolioExposureSnapshot).join(RiskGraphRun, RiskGraphRun.id == PortfolioExposureSnapshot.run_id).where(PortfolioExposureSnapshot.exposure_kind == "aggregate", RiskGraphRun.run_type == EXPOSURE_SNAPSHOT_RUN_TYPE, RiskGraphRun.status == "completed").order_by(PortfolioExposureSnapshot.snapshot_at.desc(), PortfolioExposureSnapshot.id.desc()).limit(50))).scalars().all()
    nodes = {row.id: row for row in (await session.execute(select(RiskGraphNode))).scalars().all()}
    concentrated_payload = []
    for row in sorted(latest_success_snapshot, key=lambda item: (_to_decimal(item.gross_notional_usd) or ZERO) * (ONE - (_to_decimal(item.hedged_fraction) or ZERO)), reverse=True):
        node = nodes.get(row.node_id)
        if node is None or node.node_type not in {"event", "entity", "conversion_group", "venue"}:
            continue
        concentrated_payload.append({"node_key": node.node_key, "node_type": node.node_type, "label": node.label, "gross_notional_usd": _serialize_decimal(row.gross_notional_usd), "net_notional_usd": _serialize_decimal(row.net_notional_usd), "hedged_fraction": _serialize_decimal(row.hedged_fraction)})
        if len(concentrated_payload) >= 5:
            break

    recent_blocks = (await session.execute(select(PortfolioOptimizerRecommendation).where(PortfolioOptimizerRecommendation.observed_at_local >= now - timedelta(hours=24), PortfolioOptimizerRecommendation.recommendation_type.in_(("block", "no_quote"))).order_by(PortfolioOptimizerRecommendation.observed_at_local.desc()).limit(25))).scalars().all()
    block_reason_counts: dict[str, int] = {}
    for row in recent_blocks:
        block_reason_counts[row.reason_code] = block_reason_counts.get(row.reason_code, 0) + 1

    inventory = (await session.execute(select(InventoryControlSnapshot).where(InventoryControlSnapshot.control_scope == "global").order_by(InventoryControlSnapshot.snapshot_at.desc(), InventoryControlSnapshot.id.desc()).limit(1))).scalar_one_or_none()
    maker_used = _to_decimal(inventory.maker_budget_used_usd) if inventory is not None else ZERO
    maker_budget = _to_decimal(inventory.maker_budget_usd) if inventory is not None else Decimal(str(settings.polymarket_maker_inventory_budget_usd))
    taker_used = _to_decimal(inventory.taker_budget_used_usd) if inventory is not None else ZERO
    taker_budget = _to_decimal(inventory.taker_budget_usd) if inventory is not None else Decimal(str(settings.polymarket_taker_inventory_budget_usd))

    return {
        "enabled": settings.polymarket_risk_graph_enabled,
        "on_startup": settings.polymarket_risk_graph_on_startup,
        "interval_seconds": settings.polymarket_risk_graph_interval_seconds,
        "portfolio_optimizer_enabled": settings.polymarket_portfolio_optimizer_enabled,
        "portfolio_optimizer_interval_seconds": settings.polymarket_portfolio_optimizer_interval_seconds,
        "advisory_only": True,
        "live_disabled_by_default": not settings.polymarket_live_trading_enabled,
        "last_successful_graph_build_at": latest_graph_run.completed_at if latest_graph_run is not None and latest_graph_run.status == "completed" else None,
        "last_successful_exposure_snapshot_at": latest_snapshot_run.completed_at if latest_snapshot_run is not None and latest_snapshot_run.status == "completed" else None,
        "last_successful_optimizer_run_at": latest_optimizer_run.completed_at if latest_optimizer_run is not None and latest_optimizer_run.status == "completed" else None,
        "last_graph_build_status": latest_graph_run.status if latest_graph_run is not None else None,
        "last_exposure_snapshot_status": latest_snapshot_run.status if latest_snapshot_run is not None else None,
        "last_optimizer_status": latest_optimizer_run.status if latest_optimizer_run is not None else None,
        "top_concentrated_exposures": concentrated_payload,
        "recent_block_reason_counts_24h": block_reason_counts,
        "maker_budget_used_usd": _serialize_decimal(maker_used),
        "maker_budget_usd": _serialize_decimal(maker_budget),
        "taker_budget_used_usd": _serialize_decimal(taker_used),
        "taker_budget_usd": _serialize_decimal(taker_budget),
        "maker_budget_utilization": float((maker_used / maker_budget) if maker_budget > ZERO else ZERO),
        "taker_budget_utilization": float((taker_used / taker_budget) if taker_budget > ZERO else ZERO),
        "recent_runs": [{"id": row.id, "run_type": row.run_type, "reason": row.reason, "started_at": row.started_at, "completed_at": row.completed_at, "status": row.status, "scope_json": row.scope_json, "rows_inserted_json": row.rows_inserted_json, "error_count": row.error_count, "details_json": row.details_json} for row in latest_runs[:10]],
    }


async def list_risk_graph_runs(session: AsyncSession, *, run_type: str | None = None, reason: str | None = None, start: datetime | None = None, end: datetime | None = None, limit: int = 50) -> list[dict[str, Any]]:
    query = select(RiskGraphRun)
    if run_type:
        query = query.where(RiskGraphRun.run_type == run_type)
    if reason:
        query = query.where(RiskGraphRun.reason == reason)
    if start is not None:
        query = query.where(RiskGraphRun.started_at >= _ensure_utc(start))
    if end is not None:
        query = query.where(RiskGraphRun.started_at <= _ensure_utc(end))
    rows = (await session.execute(query.order_by(RiskGraphRun.started_at.desc()).limit(limit))).scalars().all()
    return [{"id": row.id, "run_type": row.run_type, "reason": row.reason, "started_at": row.started_at, "completed_at": row.completed_at, "status": row.status, "scope_json": row.scope_json, "rows_inserted_json": row.rows_inserted_json, "error_count": row.error_count, "details_json": row.details_json} for row in rows]


async def lookup_risk_graph_nodes(session: AsyncSession, *, node_type: str | None = None, condition_id: str | None = None, asset_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    query = select(RiskGraphNode)
    if node_type:
        query = query.where(RiskGraphNode.node_type == node_type)
    if condition_id:
        query = query.where(RiskGraphNode.condition_id == condition_id)
    if asset_id:
        query = query.where(RiskGraphNode.asset_id == asset_id)
    rows = (await session.execute(query.order_by(RiskGraphNode.updated_at.desc(), RiskGraphNode.id.desc()).limit(limit))).scalars().all()
    return [_node_snapshot_payload(row) for row in rows]


async def lookup_risk_graph_edges(session: AsyncSession, *, edge_type: str | None = None, condition_id: str | None = None, asset_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    query = select(RiskGraphEdge)
    if edge_type:
        query = query.where(RiskGraphEdge.edge_type == edge_type)
    rows = (await session.execute(query.order_by(RiskGraphEdge.updated_at.desc(), RiskGraphEdge.id.desc()).limit(limit))).scalars().all()
    if not rows:
        return []
    node_ids = {row.left_node_id for row in rows} | {row.right_node_id for row in rows}
    nodes = {row.id: row for row in (await session.execute(select(RiskGraphNode).where(RiskGraphNode.id.in_(node_ids)))).scalars().all()}
    payload: list[dict[str, Any]] = []
    for row in rows:
        left = nodes.get(row.left_node_id)
        right = nodes.get(row.right_node_id)
        if condition_id and not any(node is not None and node.condition_id == condition_id for node in (left, right)):
            continue
        if asset_id and not any(node is not None and node.asset_id == asset_id for node in (left, right)):
            continue
        item = _edge_snapshot_payload(row)
        item["left_node"] = _node_snapshot_payload(left) if left is not None else None
        item["right_node"] = _node_snapshot_payload(right) if right is not None else None
        payload.append(item)
        if len(payload) >= limit:
            break
    return payload


async def list_portfolio_exposure_snapshots(session: AsyncSession, *, node_type: str | None = None, condition_id: str | None = None, asset_id: str | None = None, start: datetime | None = None, end: datetime | None = None, limit: int = 100) -> list[dict[str, Any]]:
    query = select(PortfolioExposureSnapshot, RiskGraphNode).join(RiskGraphNode, RiskGraphNode.id == PortfolioExposureSnapshot.node_id)
    if node_type:
        query = query.where(RiskGraphNode.node_type == node_type)
    if condition_id:
        query = query.where(RiskGraphNode.condition_id == condition_id)
    if asset_id:
        query = query.where(RiskGraphNode.asset_id == asset_id)
    if start is not None:
        query = query.where(PortfolioExposureSnapshot.snapshot_at >= _ensure_utc(start))
    if end is not None:
        query = query.where(PortfolioExposureSnapshot.snapshot_at <= _ensure_utc(end))
    rows = (await session.execute(query.order_by(PortfolioExposureSnapshot.snapshot_at.desc(), PortfolioExposureSnapshot.id.desc()).limit(limit))).all()
    return [_snapshot_payload(snapshot, node) for snapshot, node in rows]


async def list_portfolio_optimizer_recommendations(session: AsyncSession, *, recommendation_type: str | None = None, reason_code: str | None = None, condition_id: str | None = None, asset_id: str | None = None, start: datetime | None = None, end: datetime | None = None, limit: int = 100) -> list[dict[str, Any]]:
    query = select(PortfolioOptimizerRecommendation, RiskGraphNode).join(RiskGraphNode, RiskGraphNode.id == PortfolioOptimizerRecommendation.node_id, isouter=True)
    if recommendation_type:
        query = query.where(PortfolioOptimizerRecommendation.recommendation_type == recommendation_type)
    if reason_code:
        query = query.where(PortfolioOptimizerRecommendation.reason_code == reason_code)
    if condition_id:
        query = query.where(PortfolioOptimizerRecommendation.condition_id == condition_id)
    if asset_id:
        query = query.where(PortfolioOptimizerRecommendation.asset_id == asset_id)
    if start is not None:
        query = query.where(PortfolioOptimizerRecommendation.observed_at_local >= _ensure_utc(start))
    if end is not None:
        query = query.where(PortfolioOptimizerRecommendation.observed_at_local <= _ensure_utc(end))
    rows = (await session.execute(query.order_by(PortfolioOptimizerRecommendation.observed_at_local.desc(), PortfolioOptimizerRecommendation.id.desc()).limit(limit))).all()
    return [_optimizer_payload(recommendation, node) for recommendation, node in rows]


async def list_inventory_control_snapshots(session: AsyncSession, *, condition_id: str | None = None, asset_id: str | None = None, start: datetime | None = None, end: datetime | None = None, limit: int = 100) -> list[dict[str, Any]]:
    query = select(InventoryControlSnapshot)
    if condition_id:
        query = query.where(InventoryControlSnapshot.condition_id == condition_id)
    if asset_id:
        query = query.where(InventoryControlSnapshot.asset_id == asset_id)
    if start is not None:
        query = query.where(InventoryControlSnapshot.snapshot_at >= _ensure_utc(start))
    if end is not None:
        query = query.where(InventoryControlSnapshot.snapshot_at <= _ensure_utc(end))
    rows = (await session.execute(query.order_by(InventoryControlSnapshot.snapshot_at.desc(), InventoryControlSnapshot.id.desc()).limit(limit))).scalars().all()
    return [_inventory_payload(row) for row in rows]


async def assess_structure_plan_risk(session: AsyncSession, *, plan: MarketStructurePaperPlan) -> dict[str, Any] | None:
    if not settings.polymarket_risk_graph_enabled:
        return None
    order_rows = (await session.execute(select(MarketStructurePaperOrder).where(MarketStructurePaperOrder.plan_id == plan.id))).scalars().all()
    if not order_rows:
        return None
    anchor_order = order_rows[0]
    state = await _compute_exposure_state(session)
    node = next((rollup.node for rollup in state.aggregate_rollups.values() if rollup.node.node_type in {"asset", "market"} and rollup.node.venue == anchor_order.venue and ((anchor_order.asset_id and rollup.node.asset_id == anchor_order.asset_id) or (anchor_order.condition_id and rollup.node.condition_id == anchor_order.condition_id))), None)
    if node is None:
        return None
    controls = _evaluate_control_for_asset(state, node=node, proposed_notional_usd=_to_decimal(plan.plan_notional_total) or ZERO, direction_sign=_direction_sign(direction=anchor_order.side, side=anchor_order.side, outcome_name=node.label), context_kind="structure_plan", scope_kind="market")
    approved = controls["recommendation_type"] not in {"block", "no_quote"}
    return {
        "approved": approved,
        "approved_size_usd": controls["target_size_cap_usd"] if approved else ZERO,
        "reason": controls["reason_code"] if not approved else "phase10_risk_allow",
        "reason_code": "risk_shared_global_block" if not approved else "phase10_risk_allow",
        "drawdown_active": False,
        "risk_mode": "graph",
        "risk_source": "risk_graph",
        "risk_scope": "shared_global",
        "original_reason_code": controls["reason_code"],
        "original_reason": controls["reason_code"],
        "recommendation": controls,
    }


async def assess_paper_trade_risk(session: AsyncSession, *, outcome_id: uuid.UUID, market_id: uuid.UUID, direction: str, proposed_notional_usd: Decimal) -> dict[str, Any] | None:
    if not settings.polymarket_risk_graph_enabled:
        return None
    asset_dim = (await session.execute(select(PolymarketAssetDim).where(PolymarketAssetDim.outcome_id == outcome_id))).scalar_one_or_none()
    market = await session.get(Market, market_id)
    if asset_dim is None and market is None:
        return None
    state = await _compute_exposure_state(session)
    node = next((rollup.node for rollup in state.aggregate_rollups.values() if rollup.node.node_type in {"asset", "market"} and ((asset_dim is not None and rollup.node.asset_id == asset_dim.asset_id) or (asset_dim is not None and rollup.node.condition_id == asset_dim.condition_id))), None)
    if node is None:
        return None
    controls = _evaluate_control_for_asset(state, node=node, proposed_notional_usd=proposed_notional_usd, direction_sign=_direction_sign(direction=direction, side=direction, outcome_name=asset_dim.outcome_name if asset_dim is not None else node.label), context_kind="paper_trade", scope_kind="asset")
    approved = controls["recommendation_type"] not in {"block", "no_quote"}
    return {
        "approved": approved,
        "approved_size_usd": controls["target_size_cap_usd"] if approved else ZERO,
        "reason": controls["reason_code"] if not approved else "phase10_risk_allow",
        "reason_code": "risk_shared_global_block" if not approved else "phase10_risk_allow",
        "drawdown_active": False,
        "risk_mode": "graph",
        "risk_source": "risk_graph",
        "risk_scope": "shared_global",
        "original_reason_code": controls["reason_code"],
        "original_reason": controls["reason_code"],
        "recommendation": controls,
    }


class PolymarketRiskGraphService:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._session_factory = session_factory
        self._last_graph_run_at: datetime | None = None
        self._last_optimizer_run_at: datetime | None = None

    async def close(self) -> None:
        return None

    async def _run_graph_cycle(self, *, reason: str) -> None:
        async with self._session_factory() as session:
            await build_risk_graph(session, reason=reason, scope_json={"service": "worker"})
            await create_exposure_snapshot(session, reason=reason, scope_json={"service": "worker"})
        self._last_graph_run_at = _utcnow()

    async def _run_optimizer_cycle(self, *, reason: str) -> None:
        async with self._session_factory() as session:
            await run_portfolio_optimizer(session, reason=reason, scope_json={"service": "worker"})
        self._last_optimizer_run_at = _utcnow()

    async def run(self, stop_event: asyncio.Event) -> None:
        if not settings.polymarket_risk_graph_enabled and not settings.polymarket_portfolio_optimizer_enabled:
            return
        if settings.polymarket_risk_graph_enabled and settings.polymarket_risk_graph_on_startup:
            try:
                await self._run_graph_cycle(reason="startup")
            except Exception:
                logger.exception("Phase 10 risk graph startup run failed")
        if settings.polymarket_portfolio_optimizer_enabled:
            try:
                await self._run_optimizer_cycle(reason="startup")
            except Exception:
                logger.exception("Phase 10 optimizer startup run failed")

        graph_interval = max(settings.polymarket_risk_graph_interval_seconds, 1)
        optimizer_interval = max(settings.polymarket_portfolio_optimizer_interval_seconds, 1)
        sleep_seconds = min(graph_interval, optimizer_interval)

        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=sleep_seconds)
                continue
            except asyncio.TimeoutError:
                pass

            now = _utcnow()
            if settings.polymarket_risk_graph_enabled and (self._last_graph_run_at is None or (now - self._last_graph_run_at).total_seconds() >= graph_interval):
                try:
                    await self._run_graph_cycle(reason="scheduled")
                except Exception:
                    logger.exception("Scheduled Phase 10 risk graph cycle failed")
            if settings.polymarket_portfolio_optimizer_enabled and (self._last_optimizer_run_at is None or (now - self._last_optimizer_run_at).total_seconds() >= optimizer_interval):
                try:
                    await self._run_optimizer_cycle(reason="scheduled")
                except Exception:
                    logger.exception("Scheduled Phase 10 optimizer cycle failed")
