"""Scanner storage reporting and bounded retention pruning."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import engine
from app.models.polymarket_raw import (
    PolymarketBboEvent,
    PolymarketBookDelta,
    PolymarketBookSnapshot,
    PolymarketOpenInterestHistory,
    PolymarketRawCaptureRun,
    PolymarketTradeTape,
)
from app.models.polymarket_stream import (
    PolymarketIngestIncident,
    PolymarketMarketEvent,
    PolymarketNormalizedEvent,
    PolymarketResyncRun,
)
from app.models.signal import Signal, SignalEvaluation
from app.models.snapshot import OrderbookSnapshot, PriceSnapshot


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat() if value.tzinfo else value.replace(tzinfo=timezone.utc).isoformat()
    return value


def _pretty_bytes(value: int | None) -> str | None:
    if value is None:
        return None
    units = ("B", "KB", "MB", "GB", "TB")
    amount = float(value)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:.1f} {unit}" if unit != "B" else f"{int(amount)} B"
        amount /= 1024
    return f"{amount:.1f} TB"


async def _postgres_relation_size(session: AsyncSession, table_name: str) -> int | None:
    bind = session.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        return None
    value = (
        await session.execute(
            text("SELECT pg_total_relation_size(to_regclass(:table_name))"),
            {"table_name": table_name},
        )
    ).scalar()
    return int(value) if value is not None else None


async def _postgres_database_size(session: AsyncSession) -> int | None:
    bind = session.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        return None
    value = (await session.execute(text("SELECT pg_database_size(current_database())"))).scalar()
    return int(value) if value is not None else None


async def _count(session: AsyncSession, model: Any, *filters: Any) -> int:
    query = select(func.count()).select_from(model)
    if filters:
        query = query.where(*filters)
    return int((await session.execute(query)).scalar_one() or 0)


async def _batch_delete(
    session: AsyncSession,
    model: Any,
    pk_col: Any,
    *filters: Any,
    batch_size: int,
) -> int:
    total = 0
    effective_batch_size = min(max(1, int(batch_size)), 30_000)
    while True:
        ids = (
            await session.execute(
                select(pk_col)
                .where(*filters)
                .order_by(pk_col.asc())
                .limit(effective_batch_size)
            )
        ).scalars().all()
        if not ids:
            break
        result = await session.execute(delete(model).where(pk_col.in_(ids)))
        await session.commit()
        total += int(result.rowcount or 0)
    return total


async def build_scanner_storage_snapshot(
    session: AsyncSession,
    *,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    as_of = as_of or _utcnow()
    raw_cutoff = as_of - timedelta(days=settings.polymarket_raw_retention_days)
    price_cutoff = as_of - timedelta(days=settings.retention_price_snapshots_days)
    orderbook_cutoff = as_of - timedelta(days=settings.retention_orderbook_snapshots_days)
    signal_cutoff = as_of - timedelta(days=settings.retention_signals_days)

    tables = [
        ("price_snapshots", PriceSnapshot),
        ("orderbook_snapshots", OrderbookSnapshot),
        ("signals", Signal),
        ("signal_evaluations", SignalEvaluation),
        ("polymarket_market_events", PolymarketMarketEvent),
        ("polymarket_normalized_events", PolymarketNormalizedEvent),
        ("polymarket_book_deltas", PolymarketBookDelta),
        ("polymarket_book_snapshots", PolymarketBookSnapshot),
        ("polymarket_bbo_events", PolymarketBboEvent),
        ("polymarket_trade_tape", PolymarketTradeTape),
        ("polymarket_open_interest_history", PolymarketOpenInterestHistory),
        ("polymarket_raw_capture_runs", PolymarketRawCaptureRun),
        ("polymarket_ingest_incidents", PolymarketIngestIncident),
        ("polymarket_resync_runs", PolymarketResyncRun),
    ]
    table_rows: list[dict[str, Any]] = []
    for table_name, model in tables:
        total_rows = await _count(session, model)
        size_bytes = await _postgres_relation_size(session, table_name)
        table_rows.append(
            {
                "table": table_name,
                "rows": total_rows,
                "size_bytes": size_bytes,
                "size_pretty": _pretty_bytes(size_bytes),
            }
        )
    table_rows.sort(key=lambda row: int(row.get("size_bytes") or 0), reverse=True)

    resolved_signal_ids = (
        select(Signal.id)
        .where(Signal.resolved.is_(True), Signal.fired_at < signal_cutoff)
        .subquery()
    )
    retention_candidates = [
        {
            "table": "price_snapshots",
            "cutoff": price_cutoff,
            "retention_days": settings.retention_price_snapshots_days,
            "candidate_rows": await _count(session, PriceSnapshot, PriceSnapshot.captured_at < price_cutoff),
        },
        {
            "table": "orderbook_snapshots",
            "cutoff": orderbook_cutoff,
            "retention_days": settings.retention_orderbook_snapshots_days,
            "candidate_rows": await _count(session, OrderbookSnapshot, OrderbookSnapshot.captured_at < orderbook_cutoff),
        },
        {
            "table": "signals",
            "cutoff": signal_cutoff,
            "retention_days": settings.retention_signals_days,
            "candidate_rows": await _count(session, Signal, Signal.resolved.is_(True), Signal.fired_at < signal_cutoff),
        },
        {
            "table": "signal_evaluations",
            "cutoff": signal_cutoff,
            "retention_days": settings.retention_signals_days,
            "candidate_rows": int(
                (
                    await session.execute(
                        select(func.count())
                        .select_from(SignalEvaluation)
                        .where(SignalEvaluation.signal_id.in_(select(resolved_signal_ids.c.id)))
                    )
                ).scalar_one()
                or 0
            ),
        },
        {
            "table": "polymarket_market_events",
            "cutoff": raw_cutoff,
            "retention_days": settings.polymarket_raw_retention_days,
            "candidate_rows": await _count(session, PolymarketMarketEvent, PolymarketMarketEvent.received_at_local < raw_cutoff),
        },
        {
            "table": "polymarket_book_snapshots",
            "cutoff": raw_cutoff,
            "retention_days": settings.polymarket_raw_retention_days,
            "candidate_rows": await _count(session, PolymarketBookSnapshot, PolymarketBookSnapshot.observed_at_local < raw_cutoff),
        },
        {
            "table": "polymarket_book_deltas",
            "cutoff": raw_cutoff,
            "retention_days": settings.polymarket_raw_retention_days,
            "candidate_rows": await _count(session, PolymarketBookDelta, PolymarketBookDelta.ingest_ts_db < raw_cutoff),
        },
        {
            "table": "polymarket_bbo_events",
            "cutoff": raw_cutoff,
            "retention_days": settings.polymarket_raw_retention_days,
            "candidate_rows": await _count(session, PolymarketBboEvent, PolymarketBboEvent.ingest_ts_db < raw_cutoff),
        },
        {
            "table": "polymarket_trade_tape",
            "cutoff": raw_cutoff,
            "retention_days": settings.polymarket_raw_retention_days,
            "candidate_rows": await _count(session, PolymarketTradeTape, PolymarketTradeTape.observed_at_local < raw_cutoff),
        },
        {
            "table": "polymarket_open_interest_history",
            "cutoff": raw_cutoff,
            "retention_days": settings.polymarket_raw_retention_days,
            "candidate_rows": await _count(
                session,
                PolymarketOpenInterestHistory,
                PolymarketOpenInterestHistory.observed_at_local < raw_cutoff,
            ),
        },
        {
            "table": "polymarket_raw_capture_runs",
            "cutoff": raw_cutoff,
            "retention_days": settings.polymarket_raw_retention_days,
            "candidate_rows": await _count(
                session,
                PolymarketRawCaptureRun,
                PolymarketRawCaptureRun.completed_at.is_not(None),
                PolymarketRawCaptureRun.completed_at < raw_cutoff,
            ),
        },
        {
            "table": "polymarket_ingest_incidents",
            "cutoff": raw_cutoff,
            "retention_days": settings.polymarket_raw_retention_days,
            "candidate_rows": await _count(session, PolymarketIngestIncident, PolymarketIngestIncident.created_at < raw_cutoff),
        },
        {
            "table": "polymarket_resync_runs",
            "cutoff": raw_cutoff,
            "retention_days": settings.polymarket_raw_retention_days,
            "candidate_rows": await _count(session, PolymarketResyncRun, PolymarketResyncRun.started_at < raw_cutoff),
        },
    ]
    candidate_by_table = {row["table"]: row["candidate_rows"] for row in retention_candidates}
    total_candidate_rows = sum(int(row["candidate_rows"] or 0) for row in retention_candidates)
    database_size_bytes = await _postgres_database_size(session)
    return _json_safe(
        {
            "generated_at": as_of,
            "database_size_bytes": database_size_bytes,
            "database_size_pretty": _pretty_bytes(database_size_bytes),
            "retention": {
                "price_snapshots_days": settings.retention_price_snapshots_days,
                "orderbook_snapshots_days": settings.retention_orderbook_snapshots_days,
                "signals_days": settings.retention_signals_days,
                "polymarket_raw_days": settings.polymarket_raw_retention_days,
            },
            "tables": table_rows,
            "retention_candidates": retention_candidates,
            "candidate_rows_by_table": candidate_by_table,
            "total_candidate_rows": total_candidate_rows,
            "safe_apply_command": (
                "python -m app.reports scanner-storage --apply "
                "--include-raw-events"
            ),
            "notes": [
                "Dry-run by default; --apply is required to delete rows.",
                "Postgres does not release table file space to the OS without VACUUM FULL, which this tool does not run.",
                "VACUUM ANALYZE is optional index/statistics maintenance and is not needed for the first prune.",
            ],
        }
    )


async def run_scanner_storage_retention(
    session: AsyncSession,
    *,
    apply: bool = False,
    include_raw_events: bool = False,
    batch_size: int = 5000,
    vacuum_analyze: bool = False,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    as_of = as_of or _utcnow()
    before = await build_scanner_storage_snapshot(session, as_of=as_of)
    if not apply:
        return {
            "mode": "dry_run",
            "apply": False,
            "include_raw_events": bool(include_raw_events),
            "batch_size": batch_size,
            "snapshot": before,
        }

    raw_cutoff = as_of - timedelta(days=settings.polymarket_raw_retention_days)
    price_cutoff = as_of - timedelta(days=settings.retention_price_snapshots_days)
    orderbook_cutoff = as_of - timedelta(days=settings.retention_orderbook_snapshots_days)
    signal_cutoff = as_of - timedelta(days=settings.retention_signals_days)

    deleted: dict[str, int] = {}
    deleted["price_snapshots"] = await _batch_delete(
        session,
        PriceSnapshot,
        PriceSnapshot.id,
        PriceSnapshot.captured_at < price_cutoff,
        batch_size=batch_size,
    )
    deleted["orderbook_snapshots"] = await _batch_delete(
        session,
        OrderbookSnapshot,
        OrderbookSnapshot.id,
        OrderbookSnapshot.captured_at < orderbook_cutoff,
        batch_size=batch_size,
    )

    old_signal_ids = (
        await session.execute(
            select(Signal.id).where(Signal.resolved.is_(True), Signal.fired_at < signal_cutoff)
        )
    ).scalars().all()
    if old_signal_ids:
        result = await session.execute(delete(SignalEvaluation).where(SignalEvaluation.signal_id.in_(old_signal_ids)))
        deleted["signal_evaluations"] = int(result.rowcount or 0)
        result = await session.execute(delete(Signal).where(Signal.id.in_(old_signal_ids)))
        deleted["signals"] = int(result.rowcount or 0)
        await session.commit()
    else:
        deleted["signal_evaluations"] = 0
        deleted["signals"] = 0

    deleted["polymarket_book_snapshots"] = await _batch_delete(
        session,
        PolymarketBookSnapshot,
        PolymarketBookSnapshot.id,
        PolymarketBookSnapshot.observed_at_local < raw_cutoff,
        batch_size=batch_size,
    )
    deleted["polymarket_book_deltas"] = await _batch_delete(
        session,
        PolymarketBookDelta,
        PolymarketBookDelta.id,
        PolymarketBookDelta.ingest_ts_db < raw_cutoff,
        batch_size=batch_size,
    )
    deleted["polymarket_bbo_events"] = await _batch_delete(
        session,
        PolymarketBboEvent,
        PolymarketBboEvent.id,
        PolymarketBboEvent.ingest_ts_db < raw_cutoff,
        batch_size=batch_size,
    )
    deleted["polymarket_trade_tape"] = await _batch_delete(
        session,
        PolymarketTradeTape,
        PolymarketTradeTape.id,
        PolymarketTradeTape.observed_at_local < raw_cutoff,
        batch_size=batch_size,
    )
    deleted["polymarket_open_interest_history"] = await _batch_delete(
        session,
        PolymarketOpenInterestHistory,
        PolymarketOpenInterestHistory.id,
        PolymarketOpenInterestHistory.observed_at_local < raw_cutoff,
        batch_size=batch_size,
    )
    deleted["polymarket_raw_capture_runs"] = await _batch_delete(
        session,
        PolymarketRawCaptureRun,
        PolymarketRawCaptureRun.id,
        PolymarketRawCaptureRun.completed_at.is_not(None),
        PolymarketRawCaptureRun.completed_at < raw_cutoff,
        batch_size=batch_size,
    )
    deleted["polymarket_ingest_incidents"] = await _batch_delete(
        session,
        PolymarketIngestIncident,
        PolymarketIngestIncident.id,
        PolymarketIngestIncident.created_at < raw_cutoff,
        batch_size=batch_size,
    )
    deleted["polymarket_resync_runs"] = await _batch_delete(
        session,
        PolymarketResyncRun,
        PolymarketResyncRun.id,
        PolymarketResyncRun.started_at < raw_cutoff,
        batch_size=batch_size,
    )
    if include_raw_events:
        deleted["polymarket_market_events"] = await _batch_delete(
            session,
            PolymarketMarketEvent,
            PolymarketMarketEvent.id,
            PolymarketMarketEvent.received_at_local < raw_cutoff,
            batch_size=batch_size,
        )
    else:
        deleted["polymarket_market_events"] = 0

    vacuumed: list[str] = []
    if vacuum_analyze:
        bind = session.get_bind()
        if bind is not None and bind.dialect.name == "postgresql":
            await session.commit()
            async with engine.connect() as connection:
                autocommit_connection = await connection.execution_options(isolation_level="AUTOCOMMIT")
                await autocommit_connection.execute(text("SET maintenance_work_mem = '32MB'"))
                for table_name, count in deleted.items():
                    if count <= 0:
                        continue
                    await autocommit_connection.execute(text(f"VACUUM ANALYZE {table_name}"))
                    vacuumed.append(table_name)

    after = await build_scanner_storage_snapshot(session, as_of=as_of)
    return {
        "mode": "applied",
        "apply": True,
        "include_raw_events": bool(include_raw_events),
        "batch_size": batch_size,
        "deleted": deleted,
        "total_deleted": sum(deleted.values()),
        "vacuum_analyze": bool(vacuum_analyze),
        "vacuumed_tables": vacuumed,
        "before": before,
        "after": after,
    }
