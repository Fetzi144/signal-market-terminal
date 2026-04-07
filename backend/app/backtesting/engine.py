"""Backtesting replay engine: replays historical price snapshots through the detector suite."""
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.backtest import BacktestRun, BacktestSignal
from app.models.snapshot import OrderbookSnapshot, PriceSnapshot
from app.ranking.scorer import compute_rank_score
from app.signals.base import SnapshotWindow

logger = logging.getLogger(__name__)

# Match production detection interval
REPLAY_STEP_MINUTES = 2

# Detectors that support snapshot-based replay
REPLAY_DETECTOR_TYPES = {"price_move", "volume_spike"}


class BacktestEngine:
    async def run(self, session: AsyncSession, backtest_run: BacktestRun) -> dict:
        """Execute a full backtest replay and return the result summary."""
        backtest_run.status = "running"
        backtest_run.started_at = datetime.now(timezone.utc)
        await session.commit()

        try:
            result = await self._execute(session, backtest_run)
            backtest_run.status = "completed"
            backtest_run.completed_at = datetime.now(timezone.utc)
            backtest_run.result_summary = result
            await session.commit()
            logger.info(
                "Backtest %s completed: %d signals, %.1f%% win rate",
                backtest_run.id,
                result["total_signals"],
                result["win_rate"] * 100,
            )
            return result
        except Exception:
            backtest_run.status = "failed"
            backtest_run.completed_at = datetime.now(timezone.utc)
            backtest_run.result_summary = {"error": "Engine execution failed"}
            await session.commit()
            logger.error("Backtest %s failed", backtest_run.id, exc_info=True)
            raise

    async def _execute(self, session: AsyncSession, run: BacktestRun) -> dict:
        """Core replay loop."""
        detectors = self._build_detectors(run.detector_configs or {})
        rank_threshold = Decimal(str(run.rank_threshold))

        # Pre-load all price snapshots in the date range
        price_snaps = await self._load_price_snapshots(session, run.start_date, run.end_date)
        orderbook_snaps = await self._load_orderbook_snapshots(session, run.start_date, run.end_date)

        # Build resolution lookup: outcome_id -> resolved_correctly
        resolution_map = await self._build_resolution_map(session, price_snaps)

        # Dedupe tracking: (signal_type, outcome_id, 15-min bucket) -> already fired
        seen: set[tuple[str, str, str]] = set()

        all_bt_signals: list[BacktestSignal] = []

        # Step through time in REPLAY_STEP_MINUTES increments
        current_time = run.start_date
        while current_time <= run.end_date:
            window = SnapshotWindow(
                price_snapshots=[s for s in price_snaps if s.captured_at <= current_time],
                orderbook_snapshots=[s for s in orderbook_snaps if s.captured_at <= current_time],
                window_start=run.start_date,
                window_end=current_time,
            )

            for detector in detectors:
                try:
                    candidates = await detector.detect(session, snapshot_window=window)
                except Exception:
                    logger.warning(
                        "Detector %s failed at %s",
                        type(detector).__name__, current_time, exc_info=True,
                    )
                    continue

                for c in candidates:
                    rank = compute_rank_score(c.signal_score, c.confidence)
                    if rank < rank_threshold:
                        continue

                    # Dedupe: one signal per type per outcome per 15-min bucket
                    bucket = _dedupe_bucket(current_time)
                    dedupe_key = (c.signal_type, c.outcome_id, bucket.isoformat())
                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)

                    # Look up resolution for this outcome
                    outcome_uuid = uuid.UUID(c.outcome_id)
                    resolved = resolution_map.get(outcome_uuid)

                    bt_signal = BacktestSignal(
                        id=uuid.uuid4(),
                        backtest_run_id=run.id,
                        signal_type=c.signal_type,
                        timeframe="30m",
                        outcome_id=outcome_uuid,
                        fired_at=current_time,
                        signal_score=c.signal_score,
                        confidence=c.confidence,
                        rank_score=rank,
                        resolved_correctly=resolved,
                        price_at_fire=c.price_at_fire,
                        details=c.details,
                    )
                    all_bt_signals.append(bt_signal)

            current_time += timedelta(minutes=REPLAY_STEP_MINUTES)

        # Bulk insert signals
        for sig in all_bt_signals:
            session.add(sig)
        await session.flush()

        # Compute summary
        return self._compute_summary(all_bt_signals)

    def _build_detectors(self, detector_configs: dict) -> list:
        """Instantiate detectors with optional config overrides."""
        from app.signals.price_move import PriceMoveDetector
        from app.signals.volume_spike import VolumeSpikeDetector

        detectors = []

        pm_cfg = detector_configs.get("price_move", {})
        detectors.append(PriceMoveDetector(
            threshold_pct=pm_cfg.get("threshold_pct"),
            window_minutes=pm_cfg.get("window_minutes"),
        ))

        vs_cfg = detector_configs.get("volume_spike", {})
        detectors.append(VolumeSpikeDetector(
            multiplier=vs_cfg.get("multiplier"),
            baseline_hours=vs_cfg.get("baseline_hours"),
        ))

        return detectors

    async def _load_price_snapshots(
        self, session: AsyncSession, start: datetime, end: datetime
    ) -> list[PriceSnapshot]:
        """Load all price snapshots in the date range."""
        result = await session.execute(
            select(PriceSnapshot)
            .where(
                PriceSnapshot.captured_at >= start,
                PriceSnapshot.captured_at <= end,
            )
            .order_by(PriceSnapshot.captured_at)
        )
        snaps = result.scalars().all()
        logger.info("Loaded %d price snapshots for backtest", len(snaps))
        return snaps

    async def _load_orderbook_snapshots(
        self, session: AsyncSession, start: datetime, end: datetime
    ) -> list[OrderbookSnapshot]:
        """Load all orderbook snapshots in the date range."""
        result = await session.execute(
            select(OrderbookSnapshot)
            .where(
                OrderbookSnapshot.captured_at >= start,
                OrderbookSnapshot.captured_at <= end,
            )
            .order_by(OrderbookSnapshot.captured_at)
        )
        snaps = result.scalars().all()
        logger.info("Loaded %d orderbook snapshots for backtest", len(snaps))
        return snaps

    async def _build_resolution_map(
        self, session: AsyncSession, price_snaps: list[PriceSnapshot]
    ) -> dict[uuid.UUID, bool | None]:
        """Build a lookup of outcome_id -> resolved_correctly using existing signal resolution data.

        For backtesting, we check if the market's outcome has been resolved and what the result was.
        We look at existing Signal records that have been resolved to build the ground truth.
        """
        from app.models.signal import Signal

        outcome_ids = {s.outcome_id for s in price_snaps}
        if not outcome_ids:
            return {}

        # Get the resolution status from existing resolved signals
        result = await session.execute(
            select(Signal.outcome_id, Signal.resolved_correctly, Signal.details)
            .where(
                Signal.outcome_id.in_(outcome_ids),
                Signal.resolved_correctly.isnot(None),
            )
            .distinct(Signal.outcome_id)
        )

        resolution_map: dict[uuid.UUID, bool | None] = {}
        for outcome_id, resolved_correctly, details in result.all():
            # Use the first resolved signal's direction to determine correctness
            resolution_map[outcome_id] = resolved_correctly

        return resolution_map

    def _compute_summary(self, signals: list[BacktestSignal]) -> dict:
        """Compute aggregated statistics from backtest signals."""
        total = len(signals)
        if total == 0:
            return {
                "total_signals": 0,
                "win_rate": 0.0,
                "signals_per_day": 0.0,
                "accuracy_by_type": {},
                "false_positive_rate": 0.0,
            }

        resolved = [s for s in signals if s.resolved_correctly is not None]
        correct = [s for s in resolved if s.resolved_correctly is True]
        incorrect = [s for s in resolved if s.resolved_correctly is False]

        win_rate = len(correct) / len(resolved) if resolved else 0.0

        # Accuracy by signal type
        by_type: dict[str, dict] = defaultdict(lambda: {"total": 0, "correct": 0})
        for s in resolved:
            by_type[s.signal_type]["total"] += 1
            if s.resolved_correctly:
                by_type[s.signal_type]["correct"] += 1

        accuracy_by_type = {
            t: {
                "total": d["total"],
                "correct": d["correct"],
                "win_rate": round(d["correct"] / d["total"], 4) if d["total"] > 0 else 0.0,
            }
            for t, d in by_type.items()
        }

        # Time span for signals_per_day
        if total >= 2:
            dates = [s.fired_at for s in signals]
            span_days = max((max(dates) - min(dates)).total_seconds() / 86400, 1.0)
        else:
            span_days = 1.0

        false_positive_rate = len(incorrect) / len(resolved) if resolved else 0.0

        return {
            "total_signals": total,
            "resolved_signals": len(resolved),
            "correct_signals": len(correct),
            "win_rate": round(win_rate, 4),
            "signals_per_day": round(total / span_days, 2),
            "accuracy_by_type": accuracy_by_type,
            "false_positive_rate": round(false_positive_rate, 4),
        }


def _dedupe_bucket(dt: datetime) -> datetime:
    """Truncate to 15-minute bucket (matches production dedupe logic)."""
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)
