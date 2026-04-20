"""Post-signal evaluation: check price at each horizon and record outcomes."""
import logging
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.signal import Signal, SignalEvaluation
from app.models.snapshot import PriceSnapshot

logger = logging.getLogger(__name__)


def _ensure_utc(dt: datetime) -> datetime:
    """Normalize a datetime to UTC. Naive datetimes are assumed UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


HORIZONS = {
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "24h": timedelta(hours=24),
}

# Keep evaluation passes bounded so a backlog cannot monopolize the worker.
EVALUATION_SIGNAL_BATCH_SIZE = 1000

# How far from the target time we'll accept a snapshot
SNAPSHOT_TOLERANCE = timedelta(minutes=5)
PRICE_CHANGE_PCT_QUANTUM = Decimal("0.0001")
MAX_ABS_PRICE_CHANGE_PCT = Decimal("9999.9999")


def _bounded_price_change_pct(*, price_change: Decimal, price_at_fire: Decimal, signal_id: uuid.UUID, horizon: str) -> Decimal:
    if price_at_fire <= 0:
        return Decimal("0.0000")

    price_change_pct = ((price_change / price_at_fire) * 100).quantize(PRICE_CHANGE_PCT_QUANTUM)
    if price_change_pct > MAX_ABS_PRICE_CHANGE_PCT:
        logger.warning(
            "Clamping signal evaluation price_change_pct for signal %s horizon %s from %s to %s",
            signal_id,
            horizon,
            price_change_pct,
            MAX_ABS_PRICE_CHANGE_PCT,
        )
        return MAX_ABS_PRICE_CHANGE_PCT
    if price_change_pct < -MAX_ABS_PRICE_CHANGE_PCT:
        logger.warning(
            "Clamping signal evaluation price_change_pct for signal %s horizon %s from %s to %s",
            signal_id,
            horizon,
            price_change_pct,
            -MAX_ABS_PRICE_CHANGE_PCT,
        )
        return -MAX_ABS_PRICE_CHANGE_PCT
    return price_change_pct


async def _persist_signal_evaluation(session: AsyncSession, evaluation: SignalEvaluation) -> None:
    session.add(evaluation)
    await session.flush()


async def _evaluate_signal_horizon(
    session: AsyncSession,
    signal: Signal,
    *,
    horizon_key: str,
    horizon_delta: timedelta,
    now: datetime,
) -> tuple[int, bool]:
    fired_at = _ensure_utc(signal.fired_at)
    target_time = fired_at + horizon_delta

    # Not yet time for this horizon.
    if target_time > now:
        return 0, False

    with session.no_autoflush:
        existing = await session.execute(
            select(SignalEvaluation.id).where(
                SignalEvaluation.signal_id == signal.id,
                SignalEvaluation.horizon == horizon_key,
            )
        )
        if existing.scalar_one_or_none() is not None:
            return 0, True

        if signal.outcome_id is None or signal.price_at_fire is None:
            return 0, True

        snap = await _closest_snapshot(session, signal.outcome_id, target_time)

    if snap is None:
        # No data yet, might arrive later.
        return 0, False

    price_change = snap.price - signal.price_at_fire
    price_change_pct = _bounded_price_change_pct(
        price_change=price_change,
        price_at_fire=signal.price_at_fire,
        signal_id=signal.id,
        horizon=horizon_key,
    )

    evaluation = SignalEvaluation(
        id=uuid.uuid4(),
        signal_id=signal.id,
        horizon=horizon_key,
        price_at_eval=snap.price,
        price_change=price_change,
        price_change_pct=price_change_pct,
        evaluated_at=now,
    )

    async with session.begin_nested():
        await _persist_signal_evaluation(session, evaluation)

    return 1, True


async def evaluate_signals(session: AsyncSession) -> int:
    """Evaluate unresolved signals at each horizon. Returns evaluations created."""
    now = datetime.now(timezone.utc)
    created = 0
    resolved_changed = False
    stats = {"created": 0, "failed": 0}
    session.sync_session.info["signal_evaluation_stats"] = stats

    for horizon_key, horizon_delta in HORIZONS.items():
        signals = await _load_due_signals_for_horizon(
            session,
            horizon_key=horizon_key,
            horizon_delta=horizon_delta,
            now=now,
            limit=EVALUATION_SIGNAL_BATCH_SIZE,
        )
        for signal in signals:
            try:
                created_delta, _ = await _evaluate_signal_horizon(
                    session,
                    signal,
                    horizon_key=horizon_key,
                    horizon_delta=horizon_delta,
                    now=now,
                )
            except Exception:
                stats["failed"] += 1
                logger.warning(
                    "Signal evaluation failed for signal %s horizon %s",
                    signal.id,
                    horizon_key,
                    exc_info=True,
                )
                continue

            created += created_delta
            stats["created"] = created
            if await _maybe_mark_signal_resolved(session, signal, now=now):
                resolved_changed = True

    if created or resolved_changed:
        await session.commit()
    if created:
        logger.info("Created %d signal evaluations", created)

    return created


async def _load_due_signals_for_horizon(
    session: AsyncSession,
    *,
    horizon_key: str,
    horizon_delta: timedelta,
    now: datetime,
    limit: int,
) -> list[Signal]:
    due_before = now - horizon_delta
    existing_horizon = (
        select(SignalEvaluation.id)
        .where(
            SignalEvaluation.signal_id == Signal.id,
            SignalEvaluation.horizon == horizon_key,
        )
        .exists()
    )
    result = await session.execute(
        select(Signal)
        .where(
            Signal.resolved.is_(False),
            Signal.fired_at <= due_before,
            ~existing_horizon,
        )
        .order_by(Signal.fired_at.desc(), Signal.id.desc())
        .limit(limit)
    )
    return result.scalars().all()


async def _maybe_mark_signal_resolved(
    session: AsyncSession,
    signal: Signal,
    *,
    now: datetime,
) -> bool:
    if signal.resolved:
        return False

    fired_at = _ensure_utc(signal.fired_at)
    if any(fired_at + horizon_delta > now for horizon_delta in HORIZONS.values()):
        return False

    if signal.outcome_id is None or signal.price_at_fire is None:
        signal.resolved = True
        return True

    result = await session.execute(
        select(SignalEvaluation.horizon).where(SignalEvaluation.signal_id == signal.id)
    )
    completed_horizons = set(result.scalars().all())
    if all(horizon_key in completed_horizons for horizon_key in HORIZONS):
        signal.resolved = True
        return True
    return False


async def _closest_snapshot(
    session: AsyncSession, outcome_id: uuid.UUID, target_time: datetime
) -> PriceSnapshot | None:
    """Find the snapshot closest to target_time within tolerance."""
    lower = target_time - SNAPSHOT_TOLERANCE
    upper = target_time + SNAPSHOT_TOLERANCE

    # Fetch candidates within tolerance window, pick the one closest to target
    result = await session.execute(
        select(PriceSnapshot)
        .where(
            PriceSnapshot.outcome_id == outcome_id,
            PriceSnapshot.captured_at >= lower,
            PriceSnapshot.captured_at <= upper,
        )
    )
    candidates = result.scalars().all()
    if not candidates:
        return None

    # Normalize all timestamps to UTC before comparison
    target_utc = _ensure_utc(target_time)
    return min(candidates, key=lambda s: abs((_ensure_utc(s.captured_at) - target_utc).total_seconds()))
