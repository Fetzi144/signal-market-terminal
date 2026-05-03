"""Targeted signal-resolution backfill for ended markets.

The scheduler resolves recently-settled markets, but alpha research needs a
bounded operator pass over markets that already ended locally and still have
unresolved signals. This module fetches settlement for those specific markets,
then reuses the canonical `resolve_signals` logic so CLV/P&L semantics stay in
one place.
"""
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors import get_connector
from app.ingestion.resolution import resolve_signals
from app.models.market import Market
from app.models.signal import Signal

SETTLED_KALSHI_STATUSES = {"settled", "finalized"}
TERMINAL_PRICE = Decimal("0.999")
ZEROISH_PRICE = Decimal("0.001")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _safe_text(value: Any, *, limit: int = 180) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if len(text) <= limit else f"{text[: limit - 3]}..."


def _parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _parse_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _chunked(values: list[str], size: int) -> Iterable[list[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def _normalize_side(value: Any) -> str:
    return str(value or "").strip().lower()


def _winner_from_explicit_polymarket_field(
    *,
    winner: Any,
    outcomes: list[Any],
    token_ids: list[Any],
) -> str | None:
    if winner is None:
        return None
    winner_text = str(winner).strip()
    if not winner_text:
        return None
    for outcome in outcomes:
        if _normalize_side(outcome) == _normalize_side(winner_text):
            return str(outcome)
    for index, token_id in enumerate(token_ids):
        if str(token_id) == winner_text and index < len(outcomes):
            return str(outcomes[index])
    try:
        winner_index = int(winner_text)
    except ValueError:
        return None
    if 0 <= winner_index < len(outcomes):
        return str(outcomes[winner_index])
    return None


def _winner_from_polymarket_prices(*, outcomes: list[Any], prices: list[Any]) -> str | None:
    parsed_prices = [_parse_decimal(value) for value in prices]
    if len(outcomes) != len(parsed_prices) or not parsed_prices:
        return None
    winning_indexes = [
        index
        for index, price in enumerate(parsed_prices)
        if price is not None and price >= TERMINAL_PRICE
    ]
    if len(winning_indexes) != 1:
        return None
    winner_index = winning_indexes[0]
    losing_prices = [
        price
        for index, price in enumerate(parsed_prices)
        if index != winner_index and price is not None
    ]
    if losing_prices and any(price > ZEROISH_PRICE for price in losing_prices):
        return None
    return str(outcomes[winner_index])


def parse_polymarket_resolution(payload: dict[str, Any]) -> tuple[dict[str, str] | None, str | None]:
    """Return a resolve_signals-compatible payload or a blocker reason."""
    platform_id = str(payload.get("id") or payload.get("market_id") or "").strip()
    if not platform_id:
        return None, "missing_platform_id"

    outcomes = _parse_json_list(payload.get("outcomes"))
    token_ids = _parse_json_list(payload.get("clobTokenIds") or payload.get("clob_token_ids"))
    prices = _parse_json_list(payload.get("outcomePrices") or payload.get("outcome_prices"))

    explicit_winner = _winner_from_explicit_polymarket_field(
        winner=payload.get("winner") or payload.get("winningOutcome") or payload.get("winning_outcome"),
        outcomes=outcomes,
        token_ids=token_ids,
    )
    if explicit_winner is not None:
        return {"platform_id": platform_id, "winner": explicit_winner}, None

    finalish = any(
        bool(payload.get(key))
        for key in ("closed", "archived", "resolved")
    ) or _normalize_side(payload.get("resolution_state")) in {"resolved", "settled", "finalized"}
    if not finalish:
        return None, "market_not_final"

    price_winner = _winner_from_polymarket_prices(outcomes=outcomes, prices=prices)
    if price_winner is None:
        return None, "no_unique_terminal_price"
    return {"platform_id": platform_id, "winner": price_winner}, None


async def _alpha_ready_count(session: AsyncSession) -> int:
    result = await session.execute(
        select(func.count(Signal.id)).where(
            Signal.resolved_correctly.is_not(None),
            Signal.profit_loss.is_not(None),
            Signal.clv.is_not(None),
        )
    )
    return int(result.scalar_one() or 0)


async def _load_target_markets(
    session: AsyncSession,
    *,
    platform: str,
    limit: int,
) -> list[dict[str, Any]]:
    unresolved_count = func.count(Signal.id)
    rows = (
        await session.execute(
            select(
                Market.platform_id,
                Market.question,
                Market.end_date,
                unresolved_count.label("unresolved_signal_count"),
            )
            .join(Signal, Signal.market_id == Market.id)
            .where(
                Market.platform == platform,
                Market.platform_id.is_not(None),
                Market.end_date.is_not(None),
                Market.end_date < _utcnow(),
                Signal.resolved_correctly.is_(None),
                Signal.price_at_fire.is_not(None),
            )
            .group_by(Market.platform_id, Market.question, Market.end_date)
            .order_by(unresolved_count.desc(), Market.end_date.asc(), Market.platform_id.asc())
            .limit(limit)
        )
    ).all()
    return [
        {
            "platform_id": str(platform_id),
            "question": _safe_text(question),
            "end_date": end_date.isoformat() if end_date is not None else None,
            "unresolved_signal_count": int(unresolved_signal_count or 0),
        }
        for platform_id, question, end_date, unresolved_signal_count in rows
    ]


async def _fetch_kalshi_resolutions(connector: Any, target_markets: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], Counter]:
    blockers: Counter = Counter()
    resolved: list[dict[str, Any]] = []
    by_ticker = {row["platform_id"]: row for row in target_markets}
    for batch in _chunked(list(by_ticker), 200):
        try:
            response = await connector._request_with_retry(
                "get",
                f"{connector.api_base}/markets",
                params={"tickers": ",".join(batch)},
            )
        except Exception:
            blockers["fetch_failed"] += len(batch)
            continue

        seen: set[str] = set()
        for market_data in response.json().get("markets") or []:
            ticker = str(market_data.get("ticker") or "")
            if not ticker:
                blockers["missing_ticker"] += 1
                continue
            seen.add(ticker)
            status = _normalize_side(market_data.get("status"))
            result = _normalize_side(market_data.get("result"))
            if status not in SETTLED_KALSHI_STATUSES:
                blockers[f"status_{status or 'unknown'}"] += 1
                continue
            if result not in {"yes", "no"}:
                blockers["missing_result"] += 1
                continue
            resolved.append({"platform_id": ticker, "winning_outcome": result})
        blockers["not_returned"] += len(set(batch) - seen)
    return resolved, blockers


async def _fetch_polymarket_resolutions(connector: Any, target_markets: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], Counter]:
    blockers: Counter = Counter()
    resolved: list[dict[str, Any]] = []
    for row in target_markets:
        platform_id = row["platform_id"]
        try:
            response = await connector._request_with_retry(
                "get",
                f"{connector.gamma_base}/markets/{platform_id}",
            )
        except Exception:
            blockers["fetch_failed"] += 1
            continue
        payload = response.json()
        if isinstance(payload, list):
            payload = payload[0] if payload else {}
        resolution, blocker = parse_polymarket_resolution(payload if isinstance(payload, dict) else {})
        if resolution is not None:
            resolved.append(resolution)
        else:
            blockers[blocker or "unresolved"] += 1
    return resolved, blockers


async def _fetch_platform_resolutions(
    session: AsyncSession,
    *,
    platform: str,
    limit: int,
) -> dict[str, Any]:
    target_markets = await _load_target_markets(session, platform=platform, limit=limit)
    if not target_markets:
        return {
            "platform": platform,
            "target_market_count": 0,
            "target_signal_count": 0,
            "resolved_markets": [],
            "blockers": {},
            "sample_targets": [],
        }

    connector = get_connector(platform)
    try:
        if platform == "kalshi":
            resolved_markets, blockers = await _fetch_kalshi_resolutions(connector, target_markets)
        elif platform == "polymarket":
            resolved_markets, blockers = await _fetch_polymarket_resolutions(connector, target_markets)
        else:
            resolved_markets, blockers = [], Counter({"unsupported_platform": len(target_markets)})
    finally:
        await connector.close()

    return {
        "platform": platform,
        "target_market_count": len(target_markets),
        "target_signal_count": sum(row["unresolved_signal_count"] for row in target_markets),
        "resolved_markets": resolved_markets,
        "resolved_market_count": len(resolved_markets),
        "blockers": dict(blockers),
        "sample_targets": target_markets[:20],
    }


async def run_signal_resolution_backfill(
    session: AsyncSession,
    *,
    platform: str = "all",
    limit: int = 2000,
    apply: bool = False,
) -> dict[str, Any]:
    """Fetch settlement for ended markets with unresolved signals and optionally apply it."""
    started_at = _utcnow()
    requested_platform = str(platform or "all").strip().lower()
    platforms = ["kalshi", "polymarket"] if requested_platform == "all" else [requested_platform]
    alpha_ready_before = await _alpha_ready_count(session)

    platform_results: list[dict[str, Any]] = []
    resolved_signal_count = 0
    for platform_name in platforms:
        result = await _fetch_platform_resolutions(session, platform=platform_name, limit=limit)
        if apply and result.get("resolved_markets"):
            result["applied_signal_count"] = await resolve_signals(
                session,
                platform_name,
                result["resolved_markets"],
            )
            resolved_signal_count += int(result["applied_signal_count"])
        else:
            result["applied_signal_count"] = 0
        result["resolved_markets_sample"] = result.pop("resolved_markets", [])[:50]
        platform_results.append(result)

    alpha_ready_after = await _alpha_ready_count(session)
    return {
        "generated_at": _utcnow().isoformat(),
        "started_at": started_at.isoformat(),
        "operation": "signal_resolution_backfill",
        "mode": "apply" if apply else "dry_run",
        "paper_only": True,
        "live_submission_permitted": False,
        "platform": requested_platform,
        "limit_per_platform": limit,
        "alpha_ready_before": alpha_ready_before,
        "alpha_ready_after": alpha_ready_after,
        "alpha_ready_delta": alpha_ready_after - alpha_ready_before,
        "resolved_signal_count": resolved_signal_count,
        "platform_results": platform_results,
    }
