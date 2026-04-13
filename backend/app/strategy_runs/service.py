from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.default_strategy import get_default_strategy_contract
from app.models.signal import Signal
from app.models.strategy_run import StrategyRun


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def get_default_strategy_bootstrap_start_at() -> datetime | None:
    return _ensure_utc(settings.default_strategy_start_at)


def serialize_strategy_run(strategy_run: StrategyRun | None) -> dict | None:
    if strategy_run is None:
        return None
    return {
        "id": str(strategy_run.id),
        "strategy_name": strategy_run.strategy_name,
        "status": strategy_run.status,
        "started_at": _ensure_utc(strategy_run.started_at).isoformat() if strategy_run.started_at else None,
        "ended_at": _ensure_utc(strategy_run.ended_at).isoformat() if strategy_run.ended_at else None,
        "contract_snapshot": strategy_run.contract_snapshot or {},
        "created_at": _ensure_utc(strategy_run.created_at).isoformat() if strategy_run.created_at else None,
    }


async def get_active_strategy_run(
    session: AsyncSession,
    strategy_name: str,
) -> StrategyRun | None:
    result = await session.execute(
        select(StrategyRun)
        .where(
            StrategyRun.strategy_name == strategy_name,
            StrategyRun.status == "active",
        )
        .order_by(StrategyRun.created_at.desc())
    )
    return result.scalars().first()


async def ensure_active_default_strategy_run(
    session: AsyncSession,
    *,
    bootstrap_started_at: datetime | None = None,
) -> StrategyRun:
    return await _ensure_active_default_strategy_run(session, bootstrap_started_at=bootstrap_started_at)


async def _infer_bootstrap_started_at(session: AsyncSession) -> datetime | None:
    signal_query = select(Signal.fired_at)
    if settings.default_strategy_signal_type:
        signal_query = signal_query.where(Signal.signal_type == settings.default_strategy_signal_type)
    signal_query = signal_query.order_by(Signal.fired_at.asc()).limit(1)
    result = await session.execute(signal_query)
    return _ensure_utc(result.scalar_one_or_none())


async def _ensure_active_default_strategy_run(
    session: AsyncSession,
    *,
    bootstrap_started_at: datetime | None = None,
) -> StrategyRun:
    active_run = await get_active_strategy_run(session, settings.default_strategy_name)
    if active_run is not None:
        return active_run

    started_at = _ensure_utc(bootstrap_started_at) or get_default_strategy_bootstrap_start_at()
    if started_at is None:
        started_at = await _infer_bootstrap_started_at(session)
    if started_at is None:
        started_at = datetime.now(timezone.utc)
    contract_snapshot = get_default_strategy_contract(started_at=started_at)
    contract_snapshot["bootstrap_source"] = "DEFAULT_STRATEGY_START_AT"

    active_run = StrategyRun(
        strategy_name=settings.default_strategy_name,
        status="active",
        started_at=started_at,
        contract_snapshot=contract_snapshot,
    )
    session.add(active_run)
    await session.flush()
    return active_run
