from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.execution.polymarket_live_state import compute_outstanding_reservations
from app.models.polymarket_live_execution import CapitalReservation, LiveOrder

ZERO = Decimal("0")


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_json_safe(inner) for inner in value]
    return value


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(_json_safe(payload), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def reservation_kind_for_order(order: LiveOrder) -> str:
    return "buy_usdc" if str(order.side).upper() == "BUY" else "sell_shares"


def reservation_amount_for_order(order: LiveOrder) -> Decimal:
    if reservation_kind_for_order(order) == "sell_shares":
        return order.requested_size
    price = order.limit_price or order.target_price
    if price is None or price <= ZERO:
        return ZERO
    return (order.requested_size * price).quantize(Decimal("0.00000001"))


class PolymarketCapitalReservationService:
    async def latest_row_for_order(self, session: AsyncSession, *, live_order_id) -> CapitalReservation | None:
        return (
            await session.execute(
                select(CapitalReservation)
                .where(CapitalReservation.live_order_id == live_order_id)
                .order_by(CapitalReservation.observed_at_local.desc(), CapitalReservation.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()

    async def reserve_for_intent(
        self,
        session: AsyncSession,
        *,
        order: LiveOrder,
        details: dict[str, Any] | None = None,
    ) -> tuple[bool, str | None, CapitalReservation | None]:
        requested_amount = reservation_amount_for_order(order)
        if requested_amount <= ZERO:
            row = await self._append_row(
                session,
                order=order,
                requested_amount=ZERO,
                reserved_amount=ZERO,
                released_amount=ZERO,
                open_amount=ZERO,
                status="failed",
                source_kind="intent",
                details={
                    "reason": "reservation_amount_zero",
                    **(details or {}),
                },
            )
            return False, "reservation_amount_zero", row

        outstanding = await compute_outstanding_reservations(session)
        limit_value = Decimal(str(settings.polymarket_max_outstanding_notional_usd))
        if limit_value > ZERO and outstanding + requested_amount > limit_value:
            row = await self._append_row(
                session,
                order=order,
                requested_amount=requested_amount,
                reserved_amount=ZERO,
                released_amount=ZERO,
                open_amount=ZERO,
                status="failed",
                source_kind="intent",
                details={
                    "reason": "max_outstanding_notional_exceeded",
                    "current_outstanding": str(outstanding),
                    "limit": str(limit_value),
                    **(details or {}),
                },
            )
            return False, "max_outstanding_notional_exceeded", row

        row = await self._append_row(
            session,
            order=order,
            requested_amount=requested_amount,
            reserved_amount=requested_amount,
            released_amount=ZERO,
            open_amount=requested_amount,
            status="pending",
            source_kind="intent",
            details=details,
        )
        return True, None, row

    async def promote_on_submit(
        self,
        session: AsyncSession,
        *,
        order: LiveOrder,
        details: dict[str, Any] | None = None,
    ) -> CapitalReservation:
        latest = await self.latest_row_for_order(session, live_order_id=order.id)
        requested_amount = latest.requested_amount if latest is not None else reservation_amount_for_order(order)
        open_amount = latest.open_amount if latest is not None else requested_amount
        return await self._append_row(
            session,
            order=order,
            requested_amount=requested_amount,
            reserved_amount=requested_amount,
            released_amount=requested_amount - open_amount,
            open_amount=open_amount,
            status="active" if open_amount > ZERO else "released",
            source_kind="submit_ack",
            details=details,
        )

    async def release_on_cancel(
        self,
        session: AsyncSession,
        *,
        order: LiveOrder,
        details: dict[str, Any] | None = None,
        source_kind: str = "cancel_update",
    ) -> CapitalReservation | None:
        latest = await self.latest_row_for_order(session, live_order_id=order.id)
        if latest is None or latest.open_amount <= ZERO:
            return latest
        return await self._append_row(
            session,
            order=order,
            requested_amount=latest.requested_amount,
            reserved_amount=latest.reserved_amount,
            released_amount=latest.released_amount + latest.open_amount,
            open_amount=ZERO,
            status="released",
            source_kind=source_kind,
            details=details,
        )

    async def apply_fill_update(
        self,
        session: AsyncSession,
        *,
        order: LiveOrder,
        fill_size_delta: Decimal,
        fill_price: Decimal | None,
        details: dict[str, Any] | None = None,
        source_kind: str = "fill_update",
    ) -> CapitalReservation | None:
        latest = await self.latest_row_for_order(session, live_order_id=order.id)
        if latest is None or latest.open_amount <= ZERO:
            return latest

        release_amount = fill_size_delta
        if reservation_kind_for_order(order) == "buy_usdc":
            price = fill_price or order.avg_fill_price or order.limit_price or order.target_price
            if price is None or price <= ZERO:
                release_amount = ZERO
            else:
                release_amount = (fill_size_delta * price).quantize(Decimal("0.00000001"))

        new_open = max(latest.open_amount - release_amount, ZERO)
        return await self._append_row(
            session,
            order=order,
            requested_amount=latest.requested_amount,
            reserved_amount=latest.reserved_amount,
            released_amount=latest.released_amount + min(release_amount, latest.open_amount),
            open_amount=new_open,
            status="released" if new_open <= ZERO else "active",
            source_kind=source_kind,
            details=details,
        )

    async def _append_row(
        self,
        session: AsyncSession,
        *,
        order: LiveOrder,
        requested_amount: Decimal,
        reserved_amount: Decimal,
        released_amount: Decimal,
        open_amount: Decimal,
        status: str,
        source_kind: str,
        details: dict[str, Any] | None,
    ) -> CapitalReservation:
        payload = {
            "live_order_id": str(order.id),
            "source_kind": source_kind,
            "status": status,
            "requested_amount": str(requested_amount),
            "reserved_amount": str(reserved_amount),
            "released_amount": str(released_amount),
            "open_amount": str(open_amount),
            "details": _json_safe(details or {}),
        }
        fingerprint = _stable_hash(payload)
        existing = (
            await session.execute(
                select(CapitalReservation).where(CapitalReservation.fingerprint == fingerprint)
            )
        ).scalar_one_or_none()
        if existing is not None:
            return existing

        row = CapitalReservation(
            live_order_id=order.id,
            condition_id=order.condition_id,
            asset_id=order.asset_id,
            reservation_kind=reservation_kind_for_order(order),
            requested_amount=requested_amount,
            reserved_amount=reserved_amount,
            released_amount=released_amount,
            open_amount=open_amount,
            status=status,
            source_kind=source_kind,
            details_json=_json_safe(details or {}),
            fingerprint=fingerprint,
        )
        session.add(row)
        await session.flush()
        return row
