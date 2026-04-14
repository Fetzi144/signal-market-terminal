from __future__ import annotations

import asyncio

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.execution.polymarket_control_plane import evaluate_recent_live_shadow, expire_stale_approvals
from app.execution.polymarket_gateway import PolymarketGateway
from app.execution.polymarket_heartbeat import PolymarketHeartbeatService


class PolymarketPilotSupervisor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        gateway: PolymarketGateway | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._heartbeat = PolymarketHeartbeatService(gateway=gateway)

    async def close(self) -> None:
        return None

    async def tick_once(self, session: AsyncSession) -> dict[str, int | str]:
        expired = await expire_stale_approvals(session)
        heartbeat = await self._heartbeat.run_once(session)
        evaluations = await evaluate_recent_live_shadow(session)
        return {
            "expired_approvals": expired,
            "shadow_evaluations": evaluations,
            "heartbeat_status": str(heartbeat.get("status") or "idle"),
        }

    async def run(self, stop_event: asyncio.Event) -> None:
        interval = max(1, settings.polymarket_heartbeat_interval_seconds)
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                async with self._session_factory() as session:
                    try:
                        await self.tick_once(session)
                        await session.commit()
                    except Exception:
                        await session.rollback()
