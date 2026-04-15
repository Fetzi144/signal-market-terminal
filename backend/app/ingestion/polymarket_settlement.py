from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.polymarket_metadata import PolymarketMarketDim, PolymarketMarketParamHistory

SETTLED_STATES = {"resolved", "finalized", "settled", "completed"}
ZERO_PRICE = Decimal("0.00000000")
ONE_PRICE = Decimal("1.00000000")


@dataclass(frozen=True, slots=True)
class PolymarketCanonicalSettlement:
    condition_id: str
    asset_id: str | None
    resolved: bool
    resolution_state: str | None
    winning_asset_id: str | None
    outcome_price: Decimal | None
    source_kind: str

    @property
    def coverage_limited(self) -> bool:
        return self.outcome_price is None


def _resolved_from_state(*, resolved: bool | None = None, resolution_state: str | None = None, winning_asset_id: str | None = None) -> bool:
    if resolved is True:
        return True
    if winning_asset_id:
        return True
    if resolution_state is None:
        return False
    return resolution_state.strip().lower() in SETTLED_STATES


def _settlement_price(*, asset_id: str | None, winning_asset_id: str | None, resolved: bool) -> Decimal | None:
    if not resolved or asset_id is None or not winning_asset_id:
        return None
    return ONE_PRICE if asset_id == winning_asset_id else ZERO_PRICE


async def get_polymarket_canonical_settlement(
    session: AsyncSession,
    *,
    condition_id: str,
    asset_id: str | None,
) -> PolymarketCanonicalSettlement:
    latest_history = (
        await session.execute(
            select(PolymarketMarketParamHistory)
            .where(
                PolymarketMarketParamHistory.condition_id == condition_id,
                or_(
                    PolymarketMarketParamHistory.winning_asset_id.is_not(None),
                    PolymarketMarketParamHistory.resolution_state.is_not(None),
                ),
            )
            .order_by(
                PolymarketMarketParamHistory.observed_at_local.desc(),
                PolymarketMarketParamHistory.id.desc(),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if latest_history is not None:
        resolved = _resolved_from_state(
            resolution_state=latest_history.resolution_state,
            winning_asset_id=latest_history.winning_asset_id,
        )
        return PolymarketCanonicalSettlement(
            condition_id=condition_id,
            asset_id=asset_id,
            resolved=resolved,
            resolution_state=latest_history.resolution_state,
            winning_asset_id=latest_history.winning_asset_id,
            outcome_price=_settlement_price(
                asset_id=asset_id,
                winning_asset_id=latest_history.winning_asset_id,
                resolved=resolved,
            ),
            source_kind="param_history",
        )

    market_dim = (
        await session.execute(
            select(PolymarketMarketDim)
            .where(PolymarketMarketDim.condition_id == condition_id)
            .limit(1)
        )
    ).scalar_one_or_none()
    if market_dim is not None:
        resolved = _resolved_from_state(
            resolved=market_dim.resolved,
            resolution_state=market_dim.resolution_state,
            winning_asset_id=market_dim.winning_asset_id,
        )
        return PolymarketCanonicalSettlement(
            condition_id=condition_id,
            asset_id=asset_id,
            resolved=resolved,
            resolution_state=market_dim.resolution_state,
            winning_asset_id=market_dim.winning_asset_id,
            outcome_price=_settlement_price(
                asset_id=asset_id,
                winning_asset_id=market_dim.winning_asset_id,
                resolved=resolved,
            ),
            source_kind="market_dim",
        )

    return PolymarketCanonicalSettlement(
        condition_id=condition_id,
        asset_id=asset_id,
        resolved=False,
        resolution_state=None,
        winning_asset_id=None,
        outcome_price=None,
        source_kind="unavailable",
    )
