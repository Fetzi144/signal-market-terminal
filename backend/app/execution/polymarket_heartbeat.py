from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.execution.polymarket_control_plane import (
    active_live_order_count,
    get_active_pilot_config,
    get_open_pilot_run,
    pause_active_pilot,
    record_control_plane_incident,
    set_heartbeat_status,
)
from app.execution.polymarket_gateway import PolymarketGateway
from app.execution.polymarket_pilot_evidence import PolymarketPilotEvidenceService

_pilot_evidence = PolymarketPilotEvidenceService()


class PolymarketHeartbeatService:
    def __init__(self, *, gateway: PolymarketGateway | None = None) -> None:
        self._gateway = gateway or PolymarketGateway()

    async def should_run(self, session: AsyncSession) -> tuple[bool, dict[str, Any]]:
        config = await get_active_pilot_config(session)
        run = await get_open_pilot_run(session, pilot_config_id=config.id) if config is not None else None
        open_live_orders = await active_live_order_count(session, pilot_config=config)
        needed = bool(
            settings.polymarket_heartbeat_enabled
            and config is not None
            and config.armed
            and run is not None
            and run.status != "paused"
            and open_live_orders > 0
        )
        return needed, {
            "pilot_config": config,
            "pilot_run": run,
            "open_live_orders": open_live_orders,
        }

    async def run_once(self, session: AsyncSession) -> dict[str, Any]:
        needed, context = await self.should_run(session)
        if not needed:
            await set_heartbeat_status(session, healthy=None, error=None)
            return {"status": "idle", **context}
        try:
            await self._gateway.healthcheck()
            await set_heartbeat_status(session, healthy=True, error=None)
            return {"status": "healthy", **context}
        except Exception as exc:
            await set_heartbeat_status(session, healthy=False, error=str(exc))
            await record_control_plane_incident(
                session,
                severity="error",
                incident_type="heartbeat_missed",
                details={"error": str(exc)},
                pilot_run=context["pilot_run"],
                strategy_family=(
                    context["pilot_config"].strategy_family
                    if context.get("pilot_config") is not None
                    else settings.polymarket_pilot_default_strategy_family
                ),
            )
            strategy_family = (
                context["pilot_config"].strategy_family
                if context.get("pilot_config") is not None
                else settings.polymarket_pilot_default_strategy_family
            )
            await _pilot_evidence.record_guardrail_event(
                session,
                strategy_family=strategy_family,
                guardrail_type="heartbeat_degraded",
                severity="error",
                action_taken="pause_pilot",
                pilot_run=context.get("pilot_run"),
                trigger_value=context.get("open_live_orders"),
                threshold_value=0,
                details={"error": str(exc)},
            )
            await pause_active_pilot(
                session,
                reason="incident",
                details={"error": str(exc)},
                incident_type="heartbeat_missed",
            )
            return {"status": "degraded", "error": str(exc), **context}
