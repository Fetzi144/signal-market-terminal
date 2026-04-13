"""Order Flow Imbalance (OFI) detector: fires when buy/sell pressure in the
orderbook is significantly imbalanced while price remains flat.

Supports multi-timeframe analysis: runs detection across each configured
timeframe (e.g. 15m, 30m, 1h) and tags signals with their timeframe.
"""
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.market import Market, Outcome
from app.models.snapshot import OrderbookSnapshot, PriceSnapshot
from app.signals.base import BaseDetector, SignalCandidate, SnapshotWindow, timeframe_to_minutes
from app.signals.probability import compute_estimated_probability

logger = logging.getLogger(__name__)


class OrderFlowImbalanceDetector(BaseDetector):
    def __init__(self, *, timeframes: list[str] | None = None):
        tf = timeframes or settings.ofi_timeframes.split(",")
        super().__init__(timeframes=tf)

    async def detect(
        self, session: AsyncSession, *, snapshot_window: SnapshotWindow | None = None
    ) -> list[SignalCandidate]:
        if not settings.ofi_enabled:
            return []

        all_candidates: list[SignalCandidate] = []
        for tf in self.timeframes:
            candidates = await self._detect_timeframe(session, tf, snapshot_window=snapshot_window)
            all_candidates.extend(candidates)
        return all_candidates

    async def _detect_timeframe(
        self,
        session: AsyncSession,
        timeframe: str,
        *,
        snapshot_window: SnapshotWindow | None = None,
    ) -> list[SignalCandidate]:
        now = datetime.now(timezone.utc)
        min_snapshots = settings.ofi_min_snapshots
        ofi_threshold = Decimal(str(settings.ofi_threshold))
        flat_minutes = timeframe_to_minutes(timeframe)
        flat_window = timedelta(minutes=flat_minutes)

        if snapshot_window is not None:
            ob_snapshots = snapshot_window.orderbook_snapshots
            price_snapshots = snapshot_window.price_snapshots
            return await self._detect_from_lists(
                session, ob_snapshots, price_snapshots,
                min_snapshots, ofi_threshold, flat_window, now, timeframe,
            )

        return await self._detect_from_db(
            session, min_snapshots, ofi_threshold, flat_window, now, timeframe,
        )

    async def _detect_from_db(
        self,
        session: AsyncSession,
        min_snapshots: int,
        ofi_threshold: Decimal,
        flat_window: timedelta,
        now: datetime,
        timeframe: str,
    ) -> list[SignalCandidate]:
        """Live mode: query DB for recent orderbook snapshots."""
        from collections import defaultdict

        window_start = now - timedelta(hours=1)

        result = await session.execute(
            select(OrderbookSnapshot)
            .where(
                OrderbookSnapshot.captured_at >= window_start,
                OrderbookSnapshot.depth_bid_10pct.isnot(None),
                OrderbookSnapshot.depth_ask_10pct.isnot(None),
            )
            .order_by(OrderbookSnapshot.outcome_id, OrderbookSnapshot.captured_at)
        )
        all_snaps = result.scalars().all()

        by_outcome: dict[object, list] = defaultdict(list)
        for snap in all_snaps:
            by_outcome[snap.outcome_id].append(snap)

        candidates: list[SignalCandidate] = []

        for outcome_id, snaps in by_outcome.items():
            if len(snaps) < min_snapshots:
                continue
            candidate = await self._evaluate_outcome(
                session, outcome_id, snaps, ofi_threshold, flat_window, now, timeframe,
            )
            if candidate is not None:
                candidates.append(candidate)

        logger.info("OrderFlowImbalanceDetector[%s]: %d candidates", timeframe, len(candidates))
        return candidates

    async def _detect_from_lists(
        self,
        session: AsyncSession,
        ob_snapshots: list,
        price_snapshots: list,
        min_snapshots: int,
        ofi_threshold: Decimal,
        flat_window: timedelta,
        now: datetime,
        timeframe: str,
    ) -> list[SignalCandidate]:
        """Replay mode: use pre-loaded snapshot lists."""
        from collections import defaultdict

        by_outcome: dict[object, list] = defaultdict(list)
        for snap in ob_snapshots:
            if snap.depth_bid_10pct is not None and snap.depth_ask_10pct is not None:
                by_outcome[snap.outcome_id].append(snap)

        candidates: list[SignalCandidate] = []

        for outcome_id, snaps in by_outcome.items():
            sorted_snaps = sorted(snaps, key=lambda s: s.captured_at)
            if len(sorted_snaps) < min_snapshots:
                continue

            candidate = await self._evaluate_outcome(
                session, outcome_id, sorted_snaps, ofi_threshold, flat_window, now, timeframe,
                price_snapshots=price_snapshots,
            )
            if candidate is not None:
                candidates.append(candidate)

        logger.info("OrderFlowImbalanceDetector[%s]: %d candidates (replay)", timeframe, len(candidates))
        return candidates

    async def _evaluate_outcome(
        self,
        session: AsyncSession,
        outcome_id,
        ob_snaps: list,
        ofi_threshold: Decimal,
        flat_window: timedelta,
        now: datetime,
        timeframe: str,
        price_snapshots: list | None = None,
    ) -> SignalCandidate | None:
        """Evaluate a single outcome for OFI signal."""
        if len(ob_snaps) < 2:
            return None

        # Use earliest and latest snapshots to compute depth changes
        earliest = ob_snaps[0]
        latest = ob_snaps[-1]

        bid_depth_prev = Decimal(str(earliest.depth_bid_10pct))
        ask_depth_prev = Decimal(str(earliest.depth_ask_10pct))
        bid_depth_curr = Decimal(str(latest.depth_bid_10pct))
        ask_depth_curr = Decimal(str(latest.depth_ask_10pct))

        bid_depth_change = bid_depth_curr - bid_depth_prev
        ask_depth_change = ask_depth_curr - ask_depth_prev

        denominator = abs(bid_depth_change) + abs(ask_depth_change)
        if denominator == 0:
            return None

        ofi = (bid_depth_change - ask_depth_change) / denominator

        if abs(ofi) < ofi_threshold:
            return None

        # Check that price is flat in the flat window
        flat_start = now - flat_window
        if price_snapshots is not None:
            # Replay mode
            outcome_prices = [
                s for s in price_snapshots
                if s.outcome_id == outcome_id and s.captured_at >= flat_start
            ]
            outcome_prices.sort(key=lambda s: s.captured_at)
        else:
            # Live mode
            result = await session.execute(
                select(PriceSnapshot)
                .where(
                    PriceSnapshot.outcome_id == outcome_id,
                    PriceSnapshot.captured_at >= flat_start,
                )
                .order_by(PriceSnapshot.captured_at)
            )
            outcome_prices = result.scalars().all()

        if len(outcome_prices) < 2:
            return None

        first_price = Decimal(str(outcome_prices[0].price))
        last_price = Decimal(str(outcome_prices[-1].price))

        if first_price < Decimal("0.01"):
            return None

        price_change_pct = abs(last_price - first_price) / first_price
        # Price is "flat" if it moved less than 3%
        if price_change_pct >= Decimal("0.03"):
            return None

        # Look up market context
        outcome_row = await session.execute(
            select(Outcome, Market)
            .join(Market, Outcome.market_id == Market.id)
            .where(Outcome.id == outcome_id)
        )
        row = outcome_row.first()
        if row is None:
            return None
        outcome, market = row

        # Signal score: OFI of 0.6 = max score
        signal_score = min(Decimal("1.0"), abs(ofi) / Decimal("0.6"))

        # Confidence: penalize thin orderbooks (low total depth)
        total_depth = bid_depth_curr + ask_depth_curr
        confidence = Decimal("1.0")
        if total_depth < 1000:
            confidence *= Decimal("0.4")
        elif total_depth < 5000:
            confidence *= Decimal("0.7")

        direction = "up" if ofi > 0 else "down"

        # Probability engine: OFI captures informed flow not yet priced in.
        # Adjustment = ofi_magnitude * (1 - price_change_magnitude) * calibration_factor
        # Flat price + high OFI = large adjustment (informed money accumulating).
        # Price already moved + OFI = smaller adjustment (already partially priced in).
        calibration_factor = Decimal("1.0")
        price_flatness = Decimal("1") - min(Decimal("1"), price_change_pct / Decimal("0.03"))
        ofi_direction = Decimal("1") if ofi > 0 else Decimal("-1")
        raw_adjustment = ofi_direction * abs(ofi) * price_flatness * Decimal("0.12") * calibration_factor
        est_prob, adj_applied = compute_estimated_probability(last_price, raw_adjustment)

        return SignalCandidate(
            signal_type="order_flow_imbalance",
            market_id=str(market.id),
            outcome_id=str(outcome.id),
            signal_score=signal_score.quantize(Decimal("0.001")),
            confidence=confidence.quantize(Decimal("0.001")),
            price_at_fire=last_price,
            received_at_local=max(latest.captured_at, outcome_prices[-1].captured_at),
            source_platform=market.platform,
            source_token_id=outcome.token_id,
            source_event_type="orderbook_snapshot",
            timeframe=timeframe,
            estimated_probability=est_prob,
            probability_adjustment=adj_applied,
            details={
                "direction": direction,
                "ofi_value": str(ofi.quantize(Decimal("0.001"))),
                "bid_depth_current": str(bid_depth_curr),
                "ask_depth_current": str(ask_depth_curr),
                "bid_depth_previous": str(bid_depth_prev),
                "ask_depth_previous": str(ask_depth_prev),
                "price_current": str(last_price),
                "price_flatness": str(price_flatness.quantize(Decimal("0.001"))),
                "timeframe": timeframe,
                "market_question": market.question,
                "outcome_name": outcome.name,
            },
        )
