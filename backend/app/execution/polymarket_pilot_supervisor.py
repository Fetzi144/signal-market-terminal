from __future__ import annotations

import asyncio

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.execution.polymarket_control_plane import (
    evaluate_recent_live_shadow,
    expire_stale_approvals,
    get_active_pilot_config,
    get_open_pilot_run,
    pause_active_pilot,
)
from app.execution.polymarket_gateway import PolymarketGateway
from app.execution.polymarket_heartbeat import PolymarketHeartbeatService
from app.execution.polymarket_pilot_evidence import PolymarketPilotEvidenceService


class PolymarketPilotSupervisor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        gateway: PolymarketGateway | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._heartbeat = PolymarketHeartbeatService(gateway=gateway)
        self._evidence = PolymarketPilotEvidenceService()

    async def close(self) -> None:
        return None

    async def tick_once(self, session: AsyncSession) -> dict[str, int | str]:
        expired = await expire_stale_approvals(session)
        heartbeat = await self._heartbeat.run_once(session)
        evaluations = await evaluate_recent_live_shadow(session)
        lot_sync = await self._evidence.sync_position_lots(session)
        guardrails = await self._evidence.enforce_periodic_guardrails(session)
        generated = await self._evidence.maybe_generate_scheduled_artifacts(session)
        if any(event.get("action_taken") == "pause_pilot" for event in guardrails):
            active_config = await get_active_pilot_config(session)
            active_run = await get_open_pilot_run(session, pilot_config_id=active_config.id) if active_config is not None else None
            if active_run is not None and active_run.status != "paused":
                await pause_active_pilot(
                    session,
                    reason="guardrail",
                    details={"guardrail_types": [event["guardrail_type"] for event in guardrails]},
                    incident_type="pilot_guardrail_pause",
                )
        return {
            "expired_approvals": expired,
            "shadow_evaluations": evaluations,
            "heartbeat_status": str(heartbeat.get("status") or "idle"),
            "fills_processed": int(lot_sync.get("fills_processed") or 0),
            "guardrail_count": len(guardrails),
            "scorecards_generated": len(generated.get("scorecards") or []),
            "readiness_reports_generated": len(generated.get("readiness_reports") or []),
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
