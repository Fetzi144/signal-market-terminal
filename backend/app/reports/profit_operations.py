"""Paper-only operator actions for moving profit evidence forward."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.connectors import get_connector
from app.connectors.polymarket import PolymarketTokenNotFoundError
from app.ingestion.polymarket_settlement import get_polymarket_canonical_settlement
from app.ingestion.polymarket_stream import upsert_watch_asset
from app.models.execution_decision import ExecutionDecision
from app.models.market import Market, Outcome
from app.models.paper_trade import PaperTrade
from app.models.polymarket_metadata import PolymarketAssetDim
from app.models.polymarket_stream import PolymarketWatchAsset
from app.models.signal import Signal
from app.models.snapshot import OrderbookSnapshot, PriceSnapshot
from app.paper_trading.engine import resolve_trades
from app.strategy_runs.service import get_active_strategy_run

ZERO = Decimal("0")
ONE = Decimal("1")
HALF = Decimal("0.5")
CONTEXT_REPAIR_REASON_CODES = (
    "execution_missing_orderbook_context",
    "execution_stale_orderbook_context",
    "execution_orderbook_context_unavailable",
)
ORDERBOOK_REPAIR_WATCH_REASON = "profit_orderbook_context_repair"
ORDERBOOK_REPAIR_PRIORITY = 100


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    normalized = _ensure_utc(value)
    return normalized.isoformat() if normalized is not None else None


def _money(value: Any) -> float:
    return round(float(value or 0), 2)


def _float(value: Any) -> float | None:
    return round(float(value), 6) if value is not None else None


def _bool_from_price(value: Decimal | None) -> bool | None:
    if value is None:
        return None
    return value >= HALF


def _safe_question(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if len(text) <= 180 else f"{text[:177]}..."


def _parse_decimal(value) -> Decimal | None:
    if value in (None, "", []):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _best_price(levels) -> Decimal | None:
    if not levels:
        return None
    try:
        return Decimal(str(levels[0][0]))
    except Exception:
        return None


def _depth_within_pct(levels, *, side: str, pct: Decimal = Decimal("0.10")) -> Decimal | None:
    if not levels:
        return None
    best = _best_price(levels)
    if best is None or best <= ZERO:
        return None
    total = ZERO
    for level in levels:
        if len(level) < 2:
            continue
        price = _parse_decimal(level[0])
        size = _parse_decimal(level[1])
        if price is None or size is None:
            continue
        if side == "bid" and price >= best * (ONE - pct):
            total += size
        elif side == "ask" and price <= best * (ONE + pct):
            total += size
        else:
            break
    return total if total > ZERO else None


def _open_trade_bucket(market: Market | None, *, now: datetime) -> str:
    if market is None:
        return "missing_market"
    end_date = _ensure_utc(market.end_date)
    if end_date is None:
        return "missing_end_date"
    if end_date < now:
        return "overdue"
    days_to_end = (end_date - now).total_seconds() / 86400
    if days_to_end <= 7:
        return "short_horizon"
    if days_to_end <= 30:
        return "operating_window"
    return "long_dated_capital_drag"


async def _resolution_candidate_for_row(
    session: AsyncSession,
    *,
    trade: PaperTrade,
    market: Market | None,
    outcome: Outcome | None,
    signal: Signal | None,
    asset_dim: PolymarketAssetDim | None,
    now: datetime,
) -> dict[str, Any]:
    outcome_won: bool | None = None
    settlement_source = "unavailable"
    blocker = "settlement_unavailable"
    coverage_limited = False

    if signal is not None and signal.resolution_price is not None:
        outcome_won = _bool_from_price(signal.resolution_price)
        settlement_source = "signal_resolution_price"
        blocker = None
    elif market is not None and market.platform == "polymarket" and asset_dim is not None:
        settlement = await get_polymarket_canonical_settlement(
            session,
            condition_id=asset_dim.condition_id,
            asset_id=asset_dim.asset_id,
        )
        if settlement.resolved and settlement.outcome_price is not None:
            outcome_won = _bool_from_price(settlement.outcome_price)
            settlement_source = f"polymarket_{settlement.source_kind}"
            blocker = None
        elif settlement.resolved and settlement.outcome_price is None:
            settlement_source = f"polymarket_{settlement.source_kind}"
            blocker = "settlement_coverage_limited"
            coverage_limited = True
        else:
            settlement_source = f"polymarket_{settlement.source_kind}"

    return {
        "trade_id": str(trade.id),
        "signal_id": str(trade.signal_id),
        "outcome_id": str(trade.outcome_id),
        "market_id": str(trade.market_id),
        "platform": market.platform if market is not None else None,
        "platform_id": market.platform_id if market is not None else None,
        "market_question": _safe_question(market.question if market is not None else (trade.details or {}).get("market_question")),
        "bucket": _open_trade_bucket(market, now=now),
        "size_usd": _money(trade.size_usd),
        "opened_at": _iso(trade.opened_at),
        "market_end_date": _iso(market.end_date if market is not None else None),
        "outcome_token_id": outcome.token_id if outcome is not None else None,
        "outcome_won": outcome_won,
        "resolvable": outcome_won is not None,
        "settlement_source": settlement_source,
        "coverage_limited": coverage_limited,
        "blocker": blocker,
    }


async def run_resolution_accelerator(
    session: AsyncSession,
    *,
    apply: bool = False,
    limit: int = 200,
) -> dict[str, Any]:
    """Resolve open paper trades when local settlement evidence is already available."""
    now = _utcnow()
    strategy_run = await get_active_strategy_run(session, settings.default_strategy_name)
    query = (
        select(PaperTrade, Market, Outcome, Signal, PolymarketAssetDim)
        .outerjoin(Market, Market.id == PaperTrade.market_id)
        .outerjoin(Outcome, Outcome.id == PaperTrade.outcome_id)
        .outerjoin(Signal, Signal.id == PaperTrade.signal_id)
        .outerjoin(PolymarketAssetDim, PolymarketAssetDim.asset_id == Outcome.token_id)
        .where(PaperTrade.status == "open")
        .order_by(PaperTrade.opened_at.asc(), PaperTrade.id.asc())
        .limit(limit)
    )
    if strategy_run is not None:
        query = query.where(PaperTrade.strategy_run_id == strategy_run.id)
    rows = (await session.execute(query)).all()

    candidates = [
        await _resolution_candidate_for_row(
            session,
            trade=trade,
            market=market,
            outcome=outcome,
            signal=signal,
            asset_dim=asset_dim,
            now=now,
        )
        for trade, market, outcome, signal, asset_dim in rows
    ]

    resolved_trade_count = 0
    attempted_outcomes: set[tuple[str, bool]] = set()
    if apply:
        for candidate in candidates:
            if not candidate["resolvable"]:
                continue
            key = (candidate["outcome_id"], bool(candidate["outcome_won"]))
            if key in attempted_outcomes:
                continue
            attempted_outcomes.add(key)
            resolved_trade_count += await resolve_trades(
                session,
                uuid.UUID(candidate["outcome_id"]),
                bool(candidate["outcome_won"]),
                strategy_run_id=strategy_run.id if strategy_run is not None else None,
            )
        if resolved_trade_count:
            await session.commit()

    buckets: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        bucket = candidate["bucket"]
        row = buckets.setdefault(bucket, {"trade_count": 0, "open_exposure": 0.0, "resolvable_count": 0})
        row["trade_count"] += 1
        row["open_exposure"] = round(row["open_exposure"] + candidate["size_usd"], 2)
        if candidate["resolvable"]:
            row["resolvable_count"] += 1

    return {
        "generated_at": now.isoformat(),
        "operation": "resolution_accelerator",
        "mode": "apply" if apply else "dry_run",
        "paper_only": True,
        "live_submission_permitted": False,
        "strategy_run_id": str(strategy_run.id) if strategy_run is not None else None,
        "candidates_considered": len(candidates),
        "resolvable_count": sum(1 for candidate in candidates if candidate["resolvable"]),
        "coverage_limited_count": sum(1 for candidate in candidates if candidate["coverage_limited"]),
        "resolved_trade_count": resolved_trade_count,
        "buckets": buckets,
        "candidates": candidates[:50],
    }


async def _load_orderbook_context_candidates(
    session: AsyncSession,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    strategy_run = await get_active_strategy_run(session, settings.default_strategy_name)
    if strategy_run is None:
        return []

    latest_ob = (
        select(OrderbookSnapshot.outcome_id, func.max(OrderbookSnapshot.captured_at).label("latest_orderbook_at"))
        .group_by(OrderbookSnapshot.outcome_id)
        .subquery()
    )
    rows = (
        await session.execute(
            select(
                ExecutionDecision,
                Signal,
                Market,
                Outcome,
                PolymarketWatchAsset,
                latest_ob.c.latest_orderbook_at,
            )
            .join(Signal, Signal.id == ExecutionDecision.signal_id)
            .join(Market, Market.id == Signal.market_id)
            .join(Outcome, Outcome.id == Signal.outcome_id)
            .outerjoin(PolymarketWatchAsset, PolymarketWatchAsset.outcome_id == Outcome.id)
            .outerjoin(latest_ob, latest_ob.c.outcome_id == Outcome.id)
            .where(
                ExecutionDecision.strategy_run_id == strategy_run.id,
                ExecutionDecision.reason_code.in_(CONTEXT_REPAIR_REASON_CODES),
                Market.platform == "polymarket",
                Outcome.token_id.is_not(None),
            )
            .order_by(
                ExecutionDecision.net_expected_pnl_usd.desc(),
                ExecutionDecision.decision_at.desc(),
                ExecutionDecision.id.asc(),
            )
            .limit(limit)
        )
    ).all()

    candidates = []
    seen_outcomes: set[uuid.UUID] = set()
    for decision, signal, market, outcome, watch_asset, latest_orderbook_at in rows:
        if outcome.id in seen_outcomes:
            continue
        seen_outcomes.add(outcome.id)
        candidates.append(
            {
                "execution_decision_id": str(decision.id),
                "signal_id": str(signal.id),
                "outcome_id": str(outcome.id),
                "market_id": str(market.id),
                "token_id": outcome.token_id,
                "platform_id": market.platform_id,
                "market_question": _safe_question(market.question),
                "reason_code": decision.reason_code,
                "decision_status": decision.decision_status,
                "decision_at": _iso(decision.decision_at),
                "net_expected_pnl_usd": _money(decision.net_expected_pnl_usd),
                "net_ev_per_share": _float(decision.net_ev_per_share),
                "watch_asset_id": str(watch_asset.id) if watch_asset is not None else None,
                "watch_enabled": bool(watch_asset.watch_enabled) if watch_asset is not None else False,
                "watch_reason": watch_asset.watch_reason if watch_asset is not None else None,
                "watch_priority": watch_asset.priority if watch_asset is not None else None,
                "latest_orderbook_at": _iso(latest_orderbook_at),
            }
        )
    return candidates


async def _capture_targeted_polymarket_orderbooks(
    session: AsyncSession,
    *,
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    token_rows = [
        (uuid.UUID(candidate["outcome_id"]), str(candidate["token_id"]))
        for candidate in candidates
        if candidate.get("token_id")
    ]
    if not token_rows:
        return {"requested_tokens": 0, "price_snapshots": 0, "orderbook_snapshots": 0, "errors": []}

    connector = get_connector("polymarket")
    now = _utcnow()
    errors: list[dict[str, str]] = []
    price_count = 0
    orderbook_count = 0
    try:
        midpoints = await connector.fetch_midpoints([token_id for _outcome_id, token_id in token_rows])
        for outcome_id, token_id in token_rows:
            midpoint = midpoints.get(token_id)
            if midpoint is not None:
                session.add(PriceSnapshot(outcome_id=outcome_id, price=midpoint, captured_at=now))
                price_count += 1
            try:
                orderbook = await connector.fetch_orderbook(token_id)
            except PolymarketTokenNotFoundError as exc:
                errors.append({"token_id": token_id, "error": str(exc)})
                continue
            except Exception as exc:
                errors.append({"token_id": token_id, "error": str(exc)[:300]})
                continue
            session.add(
                OrderbookSnapshot(
                    outcome_id=outcome_id,
                    bids=orderbook.bids,
                    asks=orderbook.asks,
                    spread=orderbook.spread,
                    depth_bid_10pct=_depth_within_pct(orderbook.bids, side="bid"),
                    depth_ask_10pct=_depth_within_pct(orderbook.asks, side="ask"),
                    captured_at=now,
                )
            )
            orderbook_count += 1
        await session.commit()
    finally:
        await connector.close()

    return {
        "requested_tokens": len(token_rows),
        "price_snapshots": price_count,
        "orderbook_snapshots": orderbook_count,
        "errors": errors[:20],
    }


async def run_orderbook_context_repair(
    session: AsyncSession,
    *,
    apply: bool = False,
    capture_orderbooks: bool = False,
    limit: int = 100,
) -> dict[str, Any]:
    """Ensure context-blocked Polymarket outcomes are watched, optionally capturing books now."""
    now = _utcnow()
    candidates = await _load_orderbook_context_candidates(session, limit=limit)
    watch_assets_ensured = 0
    if apply:
        for candidate in candidates:
            watch_asset = await upsert_watch_asset(
                session,
                outcome_id=uuid.UUID(candidate["outcome_id"]),
                asset_id=None,
                watch_enabled=True,
                watch_reason=ORDERBOOK_REPAIR_WATCH_REASON,
                priority=ORDERBOOK_REPAIR_PRIORITY,
            )
            candidate["watch_asset_id"] = str(watch_asset.id)
            candidate["watch_enabled"] = True
            candidate["watch_reason"] = ORDERBOOK_REPAIR_WATCH_REASON
            candidate["watch_priority"] = ORDERBOOK_REPAIR_PRIORITY
            watch_assets_ensured += 1
        if watch_assets_ensured:
            await session.commit()

    capture_result = None
    if capture_orderbooks and not apply:
        capture_result = {"status": "skipped", "reason": "capture_orderbooks_requires_apply"}
    elif capture_orderbooks:
        capture_result = await _capture_targeted_polymarket_orderbooks(session, candidates=candidates)

    return {
        "generated_at": now.isoformat(),
        "operation": "orderbook_context_repair",
        "mode": "apply" if apply else "dry_run",
        "paper_only": True,
        "live_submission_permitted": False,
        "candidate_outcomes": len(candidates),
        "watch_assets_ensured": watch_assets_ensured,
        "capture_orderbooks": bool(capture_orderbooks),
        "capture_result": capture_result,
        "candidates": candidates[:50],
    }


def operation_result_to_json(result: dict[str, Any]) -> str:
    return json.dumps(result, indent=2, sort_keys=True)
