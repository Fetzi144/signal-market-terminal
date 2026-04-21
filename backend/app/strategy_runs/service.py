from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.default_strategy import get_default_strategy_contract
from app.models.signal import Signal
from app.models.strategy_run import StrategyRun
from app.paper_trading.strategy_run_state import (
    initialize_strategy_run_state,
    serialize_strategy_run_state,
)
from app.strategies.registry import get_current_strategy_version

ACTIVE_RUN_STATUS = "active"
CLOSED_RUN_STATUS = "closed"

BOOTSTRAP_SOURCE_EXPLICIT = "EXPLICIT_LAUNCH_BOUNDARY"
BOOTSTRAP_SOURCE_CONFIG = "DEFAULT_STRATEGY_START_AT"
BOOTSTRAP_SOURCE_ARGUMENT = "BOOTSTRAP_STARTED_AT"
BOOTSTRAP_SOURCE_SIGNAL = "EARLIEST_SIGNAL_FIRED_AT"
BOOTSTRAP_SOURCE_NOW = "CURRENT_TIME"


class ActiveStrategyRunExistsError(RuntimeError):
    """Raised when a caller explicitly opens a run while another one is active."""


@dataclass(frozen=True)
class StrategyRunBootstrap:
    started_at: datetime
    source: str
    anchor_at: datetime


def _clean_contract_metadata(contract_metadata: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(contract_metadata, dict):
        return {}
    cleaned: dict[str, Any] = {}
    for key, value in contract_metadata.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        cleaned[key] = value
    return cleaned


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def get_default_strategy_launch_boundary() -> datetime | None:
    return _ensure_utc(settings.default_strategy_start_at)


def get_default_strategy_bootstrap_start_at() -> datetime | None:
    return get_default_strategy_launch_boundary()


def serialize_strategy_run(strategy_run: StrategyRun | None) -> dict | None:
    if strategy_run is None:
        return None
    contract_snapshot = strategy_run.contract_snapshot or {}
    return {
        "id": str(strategy_run.id),
        "strategy_name": strategy_run.strategy_name,
        "strategy_family": strategy_run.strategy_family,
        "strategy_version_id": strategy_run.strategy_version_id,
        "strategy_version_key": contract_snapshot.get("strategy_version_key"),
        "strategy_version_label": contract_snapshot.get("strategy_version_label"),
        "strategy_version_status": contract_snapshot.get("strategy_version_status"),
        "status": strategy_run.status,
        "started_at": _ensure_utc(strategy_run.started_at).isoformat() if strategy_run.started_at else None,
        "ended_at": _ensure_utc(strategy_run.ended_at).isoformat() if strategy_run.ended_at else None,
        "peak_equity": float(strategy_run.peak_equity) if strategy_run.peak_equity is not None else None,
        "current_equity": float(strategy_run.current_equity) if strategy_run.current_equity is not None else None,
        "max_drawdown": float(strategy_run.max_drawdown) if strategy_run.max_drawdown is not None else None,
        "drawdown_pct": float(strategy_run.drawdown_pct) if strategy_run.drawdown_pct is not None else None,
        "contract_snapshot": contract_snapshot,
        "state": serialize_strategy_run_state(strategy_run),
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
            StrategyRun.status == ACTIVE_RUN_STATUS,
        )
        .order_by(StrategyRun.created_at.desc())
    )
    return result.scalars().first()


async def ensure_active_default_strategy_run(
    session: AsyncSession,
    *,
    bootstrap_started_at: datetime | None = None,
) -> StrategyRun:
    active_run = await get_active_strategy_run(session, settings.default_strategy_name)
    if active_run is not None:
        return active_run
    return await open_default_strategy_run(
        session,
        bootstrap_started_at=bootstrap_started_at,
    )


async def _infer_bootstrap_started_at(session: AsyncSession) -> datetime | None:
    signal_query = select(Signal.fired_at)
    if settings.default_strategy_signal_type:
        signal_query = signal_query.where(Signal.signal_type == settings.default_strategy_signal_type)
    signal_query = signal_query.order_by(Signal.fired_at.asc()).limit(1)
    result = await session.execute(signal_query)
    return _ensure_utc(result.scalar_one_or_none())


async def _resolve_default_strategy_bootstrap(
    session: AsyncSession,
    *,
    launch_boundary_at: datetime | None = None,
    bootstrap_started_at: datetime | None = None,
) -> StrategyRunBootstrap:
    explicit_launch_boundary = _ensure_utc(launch_boundary_at)
    if explicit_launch_boundary is not None:
        return StrategyRunBootstrap(
            started_at=explicit_launch_boundary,
            source=BOOTSTRAP_SOURCE_EXPLICIT,
            anchor_at=explicit_launch_boundary,
        )

    configured_launch_boundary = get_default_strategy_launch_boundary()
    if configured_launch_boundary is not None:
        return StrategyRunBootstrap(
            started_at=configured_launch_boundary,
            source=BOOTSTRAP_SOURCE_CONFIG,
            anchor_at=configured_launch_boundary,
        )

    bootstrap_candidate = _ensure_utc(bootstrap_started_at)
    if bootstrap_candidate is not None:
        return StrategyRunBootstrap(
            started_at=bootstrap_candidate,
            source=BOOTSTRAP_SOURCE_ARGUMENT,
            anchor_at=bootstrap_candidate,
        )

    inferred_started_at = await _infer_bootstrap_started_at(session)
    if inferred_started_at is not None:
        return StrategyRunBootstrap(
            started_at=inferred_started_at,
            source=BOOTSTRAP_SOURCE_SIGNAL,
            anchor_at=inferred_started_at,
        )

    current_time = datetime.now(timezone.utc)
    return StrategyRunBootstrap(
        started_at=current_time,
        source=BOOTSTRAP_SOURCE_NOW,
        anchor_at=current_time,
    )


async def open_default_strategy_run(
    session: AsyncSession,
    *,
    launch_boundary_at: datetime | None = None,
    bootstrap_started_at: datetime | None = None,
    contract_metadata: dict[str, Any] | None = None,
) -> StrategyRun:
    active_run = await get_active_strategy_run(session, settings.default_strategy_name)
    if active_run is not None:
        raise ActiveStrategyRunExistsError(
            f"Strategy {settings.default_strategy_name} already has an active run ({active_run.id})"
        )

    bootstrap = await _resolve_default_strategy_bootstrap(
        session,
        launch_boundary_at=launch_boundary_at,
        bootstrap_started_at=bootstrap_started_at,
    )
    strategy_version = await get_current_strategy_version(session, "default_strategy")
    contract_snapshot = get_default_strategy_contract(started_at=bootstrap.started_at)
    contract_snapshot["bootstrap_source"] = bootstrap.source
    contract_snapshot["bootstrap_anchor_at"] = bootstrap.anchor_at.isoformat()
    contract_snapshot["strategy_family"] = "default_strategy"
    contract_snapshot["strategy_version_key"] = strategy_version.version_key if strategy_version is not None else None
    contract_snapshot["strategy_version_label"] = strategy_version.version_label if strategy_version is not None else None
    contract_snapshot["strategy_version_status"] = strategy_version.version_status if strategy_version is not None else None
    contract_snapshot.update(_clean_contract_metadata(contract_metadata))

    strategy_run = StrategyRun(
        strategy_name=settings.default_strategy_name,
        strategy_family="default_strategy",
        strategy_version_id=strategy_version.id if strategy_version is not None else None,
        status=ACTIVE_RUN_STATUS,
        started_at=bootstrap.started_at,
        contract_snapshot=contract_snapshot,
    )
    initialize_strategy_run_state(strategy_run)
    session.add(strategy_run)
    await session.flush()
    return strategy_run


async def close_strategy_run(
    session: AsyncSession,
    strategy_run: StrategyRun,
    *,
    ended_at: datetime | None = None,
) -> StrategyRun:
    resolved_end_at = _ensure_utc(ended_at) or datetime.now(timezone.utc)
    if strategy_run.status != CLOSED_RUN_STATUS:
        strategy_run.status = CLOSED_RUN_STATUS
    if strategy_run.ended_at is None:
        strategy_run.ended_at = resolved_end_at
    await session.flush()
    return strategy_run


async def close_active_default_strategy_run(
    session: AsyncSession,
    *,
    ended_at: datetime | None = None,
) -> StrategyRun | None:
    active_run = await get_active_strategy_run(session, settings.default_strategy_name)
    if active_run is None:
        return None
    return await close_strategy_run(session, active_run, ended_at=ended_at)
