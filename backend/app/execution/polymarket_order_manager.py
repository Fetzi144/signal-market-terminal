from __future__ import annotations

import hashlib
import json
from datetime import timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.execution.polymarket_capital_reservation import (
    PolymarketCapitalReservationService,
)
from app.execution.polymarket_control_plane import (
    evaluate_live_submission,
    get_active_pilot_config,
    get_open_pilot_run,
    is_restart_window_error,
    pause_active_pilot,
    record_approval_event,
    record_submission_block,
    register_restart_pause,
)
from app.execution.polymarket_gateway import GatewayOrderRequest, GatewayUnavailableError, PolymarketGateway
from app.execution.polymarket_live_state import (
    LIVE_ORDER_TERMINAL_STATUSES,
    effective_category_allowlist,
    effective_kill_switch_enabled,
    effective_market_allowlist,
    fetch_live_state_row,
    serialize_live_order,
    set_gateway_status,
)
from app.execution.polymarket_pilot_evidence import PolymarketPilotEvidenceService
from app.ingestion.polymarket_common import utcnow
from app.metrics import (
    polymarket_live_cancel_failures,
    polymarket_live_cancels,
    polymarket_live_order_intents_created,
    polymarket_live_submissions_attempted,
    polymarket_live_submissions_blocked,
    polymarket_live_submissions_failed,
)
from app.models.execution_decision import ExecutionDecision
from app.models.polymarket_execution_policy import PolymarketExecutionActionCandidate
from app.models.polymarket_live_execution import LiveOrder, LiveOrderEvent
from app.models.polymarket_metadata import (
    PolymarketAssetDim,
    PolymarketEventDim,
    PolymarketMarketDim,
    PolymarketMarketParamHistory,
)
from app.models.polymarket_reconstruction import PolymarketBookReconState
from app.models.signal import Signal
from app.risk.budgets import append_budget_demotion_event, record_budget_gate_evaluation
from app.strategies.registry import get_current_strategy_version

ZERO = Decimal("0")
SIZE_Q = Decimal("0.0001")
_pilot_evidence = PolymarketPilotEvidenceService()
BUDGET_BREACH_REASON_CODES = {
    "family_cap_exceeded",
    "cluster_cap_exceeded",
    "capacity_ceiling_exceeded",
    "risk_of_ruin_exceeded",
}


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


def _normalized_text(value: str | None) -> str | None:
    if value is None:
        return None
    return str(value).strip().lower()


def _price_is_tick_aligned(price: Decimal, tick_size: Decimal | None) -> bool:
    if tick_size is None or tick_size <= ZERO:
        return True
    try:
        units = price / tick_size
    except Exception:
        return False
    return units == units.quantize(Decimal("1"))


def _client_order_id_for_decision(decision_id) -> str:
    return f"pm-{str(decision_id)}"


class PolymarketOrderManager:
    def __init__(
        self,
        *,
        gateway: PolymarketGateway | None = None,
        reservation_service: PolymarketCapitalReservationService | None = None,
    ) -> None:
        self._gateway = gateway or PolymarketGateway()
        self._reservations = reservation_service or PolymarketCapitalReservationService()

    async def _handle_budget_breach(
        self,
        session: AsyncSession,
        *,
        order: LiveOrder,
        reason_code: str | None,
        reservation_row,
    ) -> None:
        if reason_code not in BUDGET_BREACH_REASON_CODES:
            return
        budget_metadata = (
            reservation_row.budget_metadata_json
            if reservation_row is not None and isinstance(reservation_row.budget_metadata_json, dict)
            else {}
        )
        status = budget_metadata.get("status") if isinstance(budget_metadata.get("status"), dict) else {}
        summary = {
            **status,
            "breach": True,
            "blocked_reason_code": reason_code,
            "reason_codes": budget_metadata.get("reason_codes") or status.get("reason_codes") or [reason_code],
            "live_order_id": str(order.id),
        }
        await record_budget_gate_evaluation(
            session,
            strategy_family=order.strategy_family or "exec_policy",
            strategy_version_id=int(order.strategy_version_id) if order.strategy_version_id is not None else None,
            reason_code=reason_code,
            summary_json=summary,
            observed_at=order.updated_at or order.created_at,
        )
        if status.get("risk_budget_policy", {}).get("breach_actions", {}).get("record_demotion", True):
            await append_budget_demotion_event(
                session,
                strategy_family=order.strategy_family or "exec_policy",
                strategy_version_id=int(order.strategy_version_id) if order.strategy_version_id is not None else None,
                reason_code=reason_code,
                details_json={
                    "live_order_id": str(order.id),
                    "client_order_id": order.client_order_id,
                    "budget_metadata": budget_metadata,
                },
                observed_at=order.updated_at or order.created_at,
            )
        await _pilot_evidence.record_guardrail_event(
            session,
            strategy_family=order.strategy_family or "exec_policy",
            guardrail_type="capital_budget_breach",
            severity="error",
            action_taken="block",
            live_order=order,
            trigger_value=budget_metadata.get("requested_notional_usd"),
            threshold_value=(
                status.get("effective_outstanding_cap_usd")
                or status.get("effective_capacity_ceiling_usd")
                or status.get("effective_max_order_notional_usd")
            ),
            details={
                "reason_code": reason_code,
                "budget_metadata": budget_metadata,
            },
        )
        active_config = await get_active_pilot_config(session)
        if active_config is None:
            return
        if str(active_config.strategy_family or "").strip().lower() != str(order.strategy_family or "").strip().lower():
            return
        live_action = status.get("risk_budget_policy", {}).get("breach_actions", {}).get("live")
        if live_action == "pause_pilot":
            await pause_active_pilot(
                session,
                reason=reason_code,
                operator_identity="risk_budget_guardrail",
                details={
                    "live_order_id": str(order.id),
                    "client_order_id": order.client_order_id,
                    "budget_metadata": budget_metadata,
                },
                incident_type="risk_budget_breach",
                live_order=order,
            )

    async def create_order_intent(
        self,
        session: AsyncSession,
        *,
        execution_decision_id,
    ) -> dict[str, Any]:
        decision = await session.get(ExecutionDecision, execution_decision_id)
        if decision is None:
            raise LookupError(f"Execution decision not found: {execution_decision_id}")

        client_order_id = _client_order_id_for_decision(decision.id)
        existing = (
            await session.execute(
                select(LiveOrder).where(LiveOrder.client_order_id == client_order_id)
            )
        ).scalar_one_or_none()
        if existing is not None:
            return serialize_live_order(existing)

        candidate = await self._resolve_candidate(session, decision=decision)
        signal = await session.get(Signal, decision.signal_id) if decision.signal_id is not None else None
        state = await fetch_live_state_row(session)
        pilot_config = await get_active_pilot_config(session)
        pilot_run = await get_open_pilot_run(session, pilot_config_id=pilot_config.id) if pilot_config is not None else None
        target_asset = await self._resolve_target_asset(session, decision=decision, candidate=candidate)
        market_dim = await self._resolve_market_dim(session, candidate=candidate, asset=target_asset)
        event_dim = await self._resolve_event_dim(session, market_dim=market_dim)
        recon_state = await self._resolve_recon_state(session, asset_id=target_asset.asset_id if target_asset is not None else None)
        param_history = await self._latest_param_history(
            session,
            condition_id=target_asset.condition_id if target_asset is not None else candidate.condition_id if candidate is not None else None,
            asset_id=target_asset.asset_id if target_asset is not None else candidate.asset_id if candidate is not None else None,
        )
        strategy_version = await get_current_strategy_version(session, "exec_policy")

        order, validation_issues = self._build_order_row(
            decision=decision,
            signal=signal,
            candidate=candidate,
            asset=target_asset,
            market_dim=market_dim,
            event_dim=event_dim,
            recon_state=recon_state,
            param_history=param_history,
            state=state,
            pilot_config=pilot_config,
            pilot_run=pilot_run,
            strategy_version_id=strategy_version.id if strategy_version is not None else None,
        )
        session.add(order)
        await session.flush()
        polymarket_live_order_intents_created.inc()

        await self._record_event(
            session,
            order=order,
            source_kind="internal",
            event_type="intent_created",
            details={
                "execution_decision_id": str(decision.id),
                "validation_issues": validation_issues,
            },
        )

        if validation_issues:
            order.status = "validation_failed"
            order.validation_error = "; ".join(validation_issues)
            order.blocked_reason_code = validation_issues[0]
            await self._record_event(
                session,
                order=order,
                source_kind="internal",
                event_type="validation_failed",
                new_status="validation_failed",
                details={"issues": validation_issues},
            )
            if "stale_execution_decision" in validation_issues:
                await _pilot_evidence.record_guardrail_event(
                    session,
                    strategy_family=order.strategy_family or "exec_policy",
                    guardrail_type="decision_age",
                    severity="warning",
                    action_taken="block",
                    live_order=order,
                    details={"issues": validation_issues},
                )
            return serialize_live_order(order)

        reserved, reservation_reason, reservation_row = await self._reservations.reserve_for_intent(
            session,
            order=order,
            details={"client_order_id": order.client_order_id},
        )
        if reservation_row is not None:
            await self._record_event(
                session,
                order=order,
                source_kind="internal",
                event_type="reservation_updated",
                details={
                    "reservation_id": reservation_row.id,
                    "reservation_status": reservation_row.status,
                    "regime_label": reservation_row.regime_label,
                },
            )
        if not reserved:
            order.status = "submit_blocked"
            order.validation_error = reservation_reason
            order.blocked_reason_code = reservation_reason
            await self._record_event(
                session,
                order=order,
                source_kind="internal",
                event_type="reservation_blocked",
                new_status="submit_blocked",
                details={
                    "reason": reservation_reason,
                    "reservation_id": reservation_row.id if reservation_row is not None else None,
                    "budget_metadata": (
                        reservation_row.budget_metadata_json
                        if reservation_row is not None
                        else None
                    ),
                },
            )
            await self._handle_budget_breach(
                session,
                order=order,
                reason_code=reservation_reason,
                reservation_row=reservation_row,
            )
            if reservation_reason == "max_outstanding_notional_exceeded":
                await _pilot_evidence.record_guardrail_event(
                    session,
                    strategy_family=order.strategy_family or "exec_policy",
                    guardrail_type="max_outstanding_notional",
                    severity="error",
                    action_taken="block",
                    live_order=order,
                    details={"reason": reservation_reason},
                )
            return serialize_live_order(order)

        if order.kill_switch_blocked or order.allowlist_blocked:
            order.status = "submit_blocked"
            block_reason = "kill_switch_enabled" if order.kill_switch_blocked else "allowlist_blocked"
            order.blocked_reason_code = block_reason
            await self._record_event(
                session,
                order=order,
                source_kind="internal",
                event_type="safety_blocked",
                new_status="submit_blocked",
                details={"reason": block_reason},
            )

        if order.manual_approval_required:
            await record_approval_event(
                session,
                live_order=order,
                pilot_run_id=order.pilot_run_id,
                action="queued",
                reason_code="manual_approval_required",
                details={"approval_expires_at": order.approval_expires_at},
            )

        return serialize_live_order(order)

    async def approve_order(
        self,
        session: AsyncSession,
        *,
        live_order_id,
        approved_by: str,
    ) -> dict[str, Any]:
        order = await session.get(LiveOrder, live_order_id)
        if order is None:
            raise LookupError(f"Live order not found: {live_order_id}")
        if order.status in LIVE_ORDER_TERMINAL_STATUSES:
            return serialize_live_order(order)
        if order.approved_at is not None:
            return serialize_live_order(order)

        order.approved_by = approved_by
        order.approved_at = utcnow()
        order.approval_state = "approved"
        next_status = order.status
        if order.status == "approval_pending":
            next_status = "submit_blocked" if (order.kill_switch_blocked or order.allowlist_blocked) else "submission_pending"
        await record_approval_event(
            session,
            live_order=order,
            pilot_run_id=order.pilot_run_id,
            action="approved",
            operator_identity=approved_by,
            details={"approved_at": order.approved_at},
        )
        await self._record_event(
            session,
            order=order,
            source_kind="internal",
            event_type="manual_approved",
            new_status=next_status,
            details={"approved_by": approved_by},
        )
        return serialize_live_order(order)

    async def reject_order(
        self,
        session: AsyncSession,
        *,
        live_order_id,
        rejected_by: str,
        reason: str,
    ) -> dict[str, Any]:
        order = await session.get(LiveOrder, live_order_id)
        if order is None:
            raise LookupError(f"Live order not found: {live_order_id}")
        if order.status in LIVE_ORDER_TERMINAL_STATUSES:
            return serialize_live_order(order)

        order.validation_error = reason
        order.blocked_reason_code = "manual_rejected"
        order.approval_state = "rejected"
        await record_approval_event(
            session,
            live_order=order,
            pilot_run_id=order.pilot_run_id,
            action="rejected",
            operator_identity=rejected_by,
            reason_code="manual_rejected",
            details={"reason": reason},
        )
        await self._record_event(
            session,
            order=order,
            source_kind="internal",
            event_type="manual_rejected",
            new_status="rejected",
            details={"rejected_by": rejected_by, "reason": reason},
        )
        await self._reservations.release_on_cancel(
            session,
            order=order,
            details={"rejected_by": rejected_by, "reason": reason},
            source_kind="cancel_update",
        )
        return serialize_live_order(order)

    async def submit_order(
        self,
        session: AsyncSession,
        *,
        live_order_id,
        operator: str | None = None,
    ) -> dict[str, Any]:
        order = await session.get(LiveOrder, live_order_id)
        if order is None:
            raise LookupError(f"Live order not found: {live_order_id}")
        if order.status in {"submitted", "live", "partially_filled", "matched", "mined", "confirmed"}:
            return serialize_live_order(order)
        if order.status in LIVE_ORDER_TERMINAL_STATUSES and order.status != "submit_blocked":
            return serialize_live_order(order)
        if order.status == "submit_blocked" and order.blocked_reason_code in (
            BUDGET_BREACH_REASON_CODES | {"max_outstanding_notional_exceeded"}
        ):
            await record_submission_block(
                session,
                order=order,
                reason=order.blocked_reason_code,
                operator_identity=operator,
            )
            return serialize_live_order(order)

        dynamic_block = await self._revalidate_submit_time(session, order=order)
        if dynamic_block is not None:
            polymarket_live_submissions_blocked.labels(reason=dynamic_block).inc()
            await record_submission_block(session, order=order, reason=dynamic_block, operator_identity=operator)
            return serialize_live_order(order)

        polymarket_live_submissions_attempted.inc()
        request = GatewayOrderRequest(
            asset_id=order.asset_id,
            side=order.side,
            price=order.limit_price or order.target_price or ZERO,
            size=order.requested_size,
            client_order_id=order.client_order_id,
            order_type=order.order_type,
            post_only=order.post_only,
        )
        if order.dry_run:
            order.submitted_at = utcnow()
            order.submitted_size = order.requested_size
            await self._record_event(
                session,
                order=order,
                source_kind="internal",
                event_type="dry_run_submit_simulated",
                new_status="submitted",
                payload={
                    "asset_id": request.asset_id,
                    "side": request.side,
                    "price": str(request.price),
                    "size": str(request.size),
                    "client_order_id": request.client_order_id,
                    "order_type": request.order_type,
                    "post_only": request.post_only,
                },
                details={"operator": operator},
            )
            await self._reservations.promote_on_submit(
                session,
                order=order,
                details={"operator": operator, "dry_run": True},
            )
            return serialize_live_order(order)

        try:
            result = await self._gateway.submit_order(request)
            order.venue_order_id = result.venue_order_id
            order.submitted_at = result.submitted_at or utcnow()
            order.submitted_size = result.submitted_size or order.requested_size
            active_run = await get_open_pilot_run(session, pilot_config_id=order.pilot_config_id) if order.pilot_config_id is not None else None
            if active_run is not None:
                active_run.status = "running"
            await set_gateway_status(session, reachable=True, error=None)
            await self._record_event(
                session,
                order=order,
                source_kind="gateway_submit",
                event_type="submit_ack",
                new_status=self._map_submit_status(result.venue_status),
                venue_status=result.venue_status,
                payload=result.payload,
                details={"operator": operator},
            )
            await self._reservations.promote_on_submit(
                session,
                order=order,
                details={"operator": operator, "venue_order_id": order.venue_order_id},
            )
            return serialize_live_order(order)
        except GatewayUnavailableError as exc:
            polymarket_live_submissions_blocked.labels(reason="gateway_unavailable").inc()
            order.submission_error = str(exc)
            order.blocked_reason_code = "gateway_unavailable"
            await set_gateway_status(session, reachable=False, error=str(exc))
            await record_submission_block(session, order=order, reason="gateway_unavailable", operator_identity=operator)
            return serialize_live_order(order)
        except Exception as exc:
            if is_restart_window_error(exc):
                order.submission_error = str(exc)
                await register_restart_pause(session, error=str(exc), live_order=order)
                await record_submission_block(session, order=order, reason="restart_pause_active", operator_identity=operator)
                return serialize_live_order(order)
            polymarket_live_submissions_failed.inc()
            order.submission_error = str(exc)
            await set_gateway_status(session, reachable=False, error=str(exc))
            await self._record_event(
                session,
                order=order,
                source_kind="gateway_submit",
                event_type="submit_failed",
                new_status="failed",
                details={"operator": operator, "error": str(exc)},
            )
            return serialize_live_order(order)

    async def cancel_order(
        self,
        session: AsyncSession,
        *,
        live_order_id,
        operator: str | None = None,
    ) -> dict[str, Any]:
        order = await session.get(LiveOrder, live_order_id)
        if order is None:
            raise LookupError(f"Live order not found: {live_order_id}")
        if order.status == "canceled":
            return serialize_live_order(order)
        if order.status in {"rejected", "failed", "validation_failed"}:
            return serialize_live_order(order)

        polymarket_live_cancels.inc()

        if order.venue_order_id is None or order.dry_run or order.status in {"approval_pending", "submission_pending", "submit_blocked"}:
            await self._record_event(
                session,
                order=order,
                source_kind="internal",
                event_type="manual_cancel",
                new_status="canceled",
                details={"operator": operator, "reason": "local_cancel"},
            )
            await self._reservations.release_on_cancel(
                session,
                order=order,
                details={"operator": operator, "reason": "local_cancel"},
            )
            return serialize_live_order(order)

        try:
            result = await self._gateway.cancel_order(order.venue_order_id)
            await self._record_event(
                session,
                order=order,
                source_kind="gateway_cancel",
                event_type="cancel_ack",
                new_status="canceled",
                venue_status=result.venue_status,
                payload=result.payload,
                details={"operator": operator},
            )
            await self._reservations.release_on_cancel(
                session,
                order=order,
                details={"operator": operator, "venue_order_id": order.venue_order_id},
            )
            return serialize_live_order(order)
        except Exception as exc:
            if is_restart_window_error(exc):
                await register_restart_pause(session, error=str(exc), live_order=order)
            polymarket_live_cancel_failures.inc()
            await self._record_event(
                session,
                order=order,
                source_kind="gateway_cancel",
                event_type="cancel_failed",
                details={"operator": operator, "error": str(exc)},
            )
            return serialize_live_order(order)

    async def _resolve_candidate(
        self,
        session: AsyncSession,
        *,
        decision: ExecutionDecision,
    ) -> PolymarketExecutionActionCandidate | None:
        if decision.chosen_action_candidate_id is not None:
            candidate = await session.get(PolymarketExecutionActionCandidate, decision.chosen_action_candidate_id)
            if candidate is not None:
                return candidate
        result = await session.execute(
            select(PolymarketExecutionActionCandidate)
            .where(
                PolymarketExecutionActionCandidate.execution_decision_id == decision.id,
                PolymarketExecutionActionCandidate.action_type == decision.chosen_action_type,
            )
            .order_by(PolymarketExecutionActionCandidate.id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def _resolve_target_asset(
        self,
        session: AsyncSession,
        *,
        decision: ExecutionDecision,
        candidate: PolymarketExecutionActionCandidate | None,
    ) -> PolymarketAssetDim | None:
        if candidate is None:
            return None
        base_asset = None
        if candidate.asset_dim_id is not None:
            base_asset = await session.get(PolymarketAssetDim, candidate.asset_dim_id)
        if base_asset is None:
            base_asset = (
                await session.execute(
                    select(PolymarketAssetDim).where(PolymarketAssetDim.asset_id == candidate.asset_id)
                )
            ).scalar_one_or_none()
        if base_asset is None:
            return None
        if candidate.side != "buy_no":
            return base_asset

        peer_assets = (
            await session.execute(
                select(PolymarketAssetDim)
                .where(PolymarketAssetDim.condition_id == base_asset.condition_id)
                .order_by(PolymarketAssetDim.id.asc())
            )
        ).scalars().all()
        if len(peer_assets) <= 1:
            return base_asset

        base_name = _normalized_text(base_asset.outcome_name)
        for peer in peer_assets:
            if peer.id == base_asset.id:
                continue
            peer_name = _normalized_text(peer.outcome_name)
            if base_name == "yes" and peer_name == "no":
                return peer
            if base_name == "no" and peer_name == "yes":
                return peer
        for peer in peer_assets:
            if peer.id != base_asset.id:
                return peer
        return base_asset

    async def _resolve_market_dim(
        self,
        session: AsyncSession,
        *,
        candidate: PolymarketExecutionActionCandidate | None,
        asset: PolymarketAssetDim | None,
    ) -> PolymarketMarketDim | None:
        market_dim_id = asset.market_dim_id if asset is not None else candidate.market_dim_id if candidate is not None else None
        if market_dim_id is None:
            return None
        return await session.get(PolymarketMarketDim, market_dim_id)

    async def _resolve_event_dim(
        self,
        session: AsyncSession,
        *,
        market_dim: PolymarketMarketDim | None,
    ) -> PolymarketEventDim | None:
        if market_dim is None or market_dim.event_dim_id is None:
            return None
        return await session.get(PolymarketEventDim, market_dim.event_dim_id)

    async def _resolve_recon_state(self, session: AsyncSession, *, asset_id: str | None) -> PolymarketBookReconState | None:
        if not asset_id:
            return None
        return (
            await session.execute(
                select(PolymarketBookReconState).where(PolymarketBookReconState.asset_id == asset_id)
            )
        ).scalar_one_or_none()

    async def _latest_param_history(
        self,
        session: AsyncSession,
        *,
        condition_id: str | None,
        asset_id: str | None,
    ) -> PolymarketMarketParamHistory | None:
        if condition_id is None:
            return None
        query = select(PolymarketMarketParamHistory).where(
            PolymarketMarketParamHistory.condition_id == condition_id,
        )
        if asset_id is not None:
            query = query.where(PolymarketMarketParamHistory.asset_id == asset_id)
        query = query.order_by(
            PolymarketMarketParamHistory.observed_at_local.desc(),
            PolymarketMarketParamHistory.id.desc(),
        ).limit(1)
        return (await session.execute(query)).scalar_one_or_none()

    def _build_order_row(
        self,
        *,
        decision: ExecutionDecision,
        signal: Signal | None,
        candidate: PolymarketExecutionActionCandidate | None,
        asset: PolymarketAssetDim | None,
        market_dim: PolymarketMarketDim | None,
        event_dim: PolymarketEventDim | None,
        recon_state: PolymarketBookReconState | None,
        param_history: PolymarketMarketParamHistory | None,
        state,
        pilot_config,
        pilot_run,
        strategy_version_id: int | None,
    ) -> tuple[LiveOrder, list[str]]:
        validation_issues: list[str] = []
        chosen_action_type = decision.chosen_action_type or (candidate.action_type if candidate is not None else None)
        if chosen_action_type in (None, "skip"):
            validation_issues.append("chosen_action_not_actionable")

        target_price = decision.chosen_target_price or (candidate.est_avg_entry_price if candidate is not None else None)
        if target_price is None or target_price <= ZERO:
            validation_issues.append("missing_target_price")

        requested_notional = decision.chosen_target_size or (candidate.target_size if candidate is not None else None)
        if requested_notional is None or requested_notional <= ZERO:
            validation_issues.append("missing_target_size")

        requested_size = ZERO
        if target_price is not None and target_price > ZERO and requested_notional is not None:
            requested_size = (requested_notional / target_price).quantize(SIZE_Q)
            if requested_size <= ZERO:
                validation_issues.append("requested_size_zero")

        tick_size = param_history.tick_size if param_history is not None else None
        min_order_size = param_history.min_order_size if param_history is not None else None
        if target_price is not None and not _price_is_tick_aligned(target_price, tick_size):
            validation_issues.append("tick_size_violation")
        if min_order_size is not None and requested_size > ZERO and requested_size < min_order_size:
            validation_issues.append("below_min_order_size")

        if settings.polymarket_execution_policy_require_live_book:
            if decision.missing_orderbook_context:
                validation_issues.append("missing_live_book_context")
            if decision.stale_orderbook_context:
                validation_issues.append("stale_live_book_context")
            if recon_state is None or recon_state.status != "live":
                validation_issues.append("untrusted_recon_state")

        if decision.decision_at < utcnow() - timedelta(seconds=settings.polymarket_live_decision_max_age_seconds):
            validation_issues.append("stale_execution_decision")

        order_type_hint = decision.chosen_order_type_hint or (candidate.order_type_hint if candidate is not None else None) or "limit"
        post_only = chosen_action_type in {"post_best", "step_ahead"} or order_type_hint == "post_only"
        if post_only and recon_state is not None and target_price is not None:
            best_ask = recon_state.best_ask
            if best_ask is not None and target_price >= best_ask:
                validation_issues.append("post_only_marketable")

        state_market_allowlist = effective_market_allowlist(state)
        state_category_allowlist = effective_category_allowlist(state)
        market_blocked = False
        if state_market_allowlist:
            allowed_values = {str(value).lower() for value in state_market_allowlist}
            candidate_values = {
                str(value).lower()
                for value in (
                    asset.condition_id if asset is not None else None,
                    market_dim.condition_id if market_dim is not None else None,
                    market_dim.market_slug if market_dim is not None else None,
                )
                if value
            }
            market_blocked = candidate_values.isdisjoint(allowed_values)
        category_blocked = False
        if state_category_allowlist:
            allowed_categories = {str(value).lower() for value in state_category_allowlist}
            observed_category = _normalized_text(event_dim.category if event_dim is not None else None)
            category_blocked = observed_category not in allowed_categories if observed_category is not None else True

        dry_run = settings.polymarket_live_dry_run or not settings.polymarket_live_trading_enabled
        manual_approval_required = (
            bool(pilot_config.manual_approval_required)
            if pilot_config is not None and pilot_config.strategy_family == "exec_policy"
            else settings.polymarket_live_manual_approval_required
        )
        approval_requested_at = utcnow() if manual_approval_required else None
        approval_expires_at = (
            approval_requested_at + timedelta(seconds=settings.polymarket_pilot_approval_ttl_seconds)
            if approval_requested_at is not None
            else None
        )
        order = LiveOrder(
            execution_decision_id=decision.id,
            signal_id=decision.signal_id,
            market_dim_id=market_dim.id if market_dim is not None else candidate.market_dim_id if candidate is not None else None,
            asset_dim_id=asset.id if asset is not None else candidate.asset_dim_id if candidate is not None else None,
            condition_id=asset.condition_id if asset is not None else candidate.condition_id if candidate is not None else "",
            asset_id=asset.asset_id if asset is not None else candidate.asset_id if candidate is not None else "",
            outcome_id=asset.outcome_id if asset is not None else candidate.outcome_id if candidate is not None else None,
            client_order_id=_client_order_id_for_decision(decision.id),
            side="BUY",
            action_type=chosen_action_type or "unknown",
            order_type=order_type_hint,
            post_only=post_only,
            limit_price=target_price,
            target_price=target_price,
            requested_size=requested_size,
            filled_size=ZERO,
            status="approval_pending" if settings.polymarket_live_manual_approval_required else "submission_pending",
            dry_run=dry_run,
            strategy_family="exec_policy",
            strategy_version_id=strategy_version_id,
            pilot_config_id=pilot_config.id if pilot_config is not None and pilot_config.strategy_family == "exec_policy" else None,
            pilot_run_id=pilot_run.id if pilot_run is not None and pilot_config is not None and pilot_config.strategy_family == "exec_policy" else None,
            manual_approval_required=manual_approval_required,
            approval_state="queued" if manual_approval_required else "not_required",
            approval_requested_at=approval_requested_at,
            approval_expires_at=approval_expires_at,
            kill_switch_blocked=effective_kill_switch_enabled(state),
            allowlist_blocked=market_blocked or category_blocked,
            policy_version=decision.chosen_policy_version or (candidate.policy_version if candidate is not None else None),
            decision_reason_json=decision.decision_reason_json,
        )
        order.status = "approval_pending" if manual_approval_required else "submission_pending"
        if validation_issues:
            order.status = "validation_failed"
            order.validation_error = "; ".join(validation_issues)
        elif order.kill_switch_blocked or order.allowlist_blocked:
            order.status = "submit_blocked"
        return order, validation_issues

    async def _revalidate_submit_time(self, session: AsyncSession, *, order: LiveOrder) -> str | None:
        state = await fetch_live_state_row(session)
        order.kill_switch_blocked = effective_kill_switch_enabled(state)
        order.allowlist_blocked = not await self._allowlist_allows(session, order=order, state=state)
        if order.kill_switch_blocked:
            return "kill_switch_enabled"
        if order.allowlist_blocked:
            return "allowlist_blocked"
        decision = (
            await session.get(ExecutionDecision, order.execution_decision_id)
            if order.execution_decision_id is not None
            else None
        )
        live_pilot_block = await evaluate_live_submission(session, order=order, decision=decision)
        if live_pilot_block is not None:
            return live_pilot_block
        if decision is None:
            return "missing_execution_decision"
        if decision.decision_at < utcnow() - timedelta(seconds=settings.polymarket_live_decision_max_age_seconds):
            return "stale_execution_decision"
        if settings.polymarket_execution_policy_require_live_book:
            if decision.missing_orderbook_context or decision.stale_orderbook_context:
                return "untrusted_live_book_context"
            recon_state = await self._resolve_recon_state(session, asset_id=order.asset_id)
            if recon_state is None or recon_state.status != "live":
                return "untrusted_recon_state"
        latest_reservation = await self._reservations.latest_row_for_order(session, live_order_id=order.id)
        if latest_reservation is None or latest_reservation.open_amount <= ZERO:
            return "reservation_missing"
        if not order.dry_run and not settings.polymarket_live_trading_enabled:
            return "live_trading_disabled"
        if not order.dry_run and not self._gateway.has_submit_credentials:
            return "missing_submit_credentials"
        return None

    async def _allowlist_allows(self, session: AsyncSession, *, order: LiveOrder, state) -> bool:
        market_allowlist = effective_market_allowlist(state)
        category_allowlist = effective_category_allowlist(state)
        if not market_allowlist and not category_allowlist:
            return True
        market_dim = (
            await session.get(PolymarketMarketDim, order.market_dim_id)
            if order.market_dim_id is not None
            else None
        )
        event_dim = (
            await session.get(PolymarketEventDim, market_dim.event_dim_id)
            if market_dim is not None and market_dim.event_dim_id is not None
            else None
        )
        if market_allowlist:
            allowed_values = {value.lower() for value in market_allowlist}
            observed_values = {
                value.lower()
                for value in (order.condition_id, market_dim.market_slug if market_dim is not None else None)
                if value
            }
            if observed_values.isdisjoint(allowed_values):
                return False
        if category_allowlist:
            observed_category = _normalized_text(event_dim.category if event_dim is not None else None)
            if observed_category is None or observed_category not in {value.lower() for value in category_allowlist}:
                return False
        return True

    async def _record_event(
        self,
        session: AsyncSession,
        *,
        order: LiveOrder,
        source_kind: str,
        event_type: str,
        new_status: str | None = None,
        venue_status: str | None = None,
        payload: dict[str, Any] | list[Any] | None = None,
        details: dict[str, Any] | None = None,
        raw_user_event_id: int | None = None,
    ) -> LiveOrderEvent:
        observed_at = utcnow()
        fingerprint = _stable_hash(
            {
                "live_order_id": str(order.id),
                "source_kind": source_kind,
                "event_type": event_type,
                "venue_status": venue_status,
                "raw_user_event_id": raw_user_event_id,
                "payload": _json_safe(payload or {}),
                "details": _json_safe(details or {}),
            }
        )
        existing = (
            await session.execute(
                select(LiveOrderEvent).where(LiveOrderEvent.fingerprint == fingerprint)
            )
        ).scalar_one_or_none()
        if existing is not None:
            return existing

        if new_status is not None:
            order.status = new_status
        order.last_event_at = observed_at
        if new_status in LIVE_ORDER_TERMINAL_STATUSES:
            order.completed_at = observed_at

        event = LiveOrderEvent(
            live_order_id=order.id,
            raw_user_event_id=raw_user_event_id,
            source_kind=source_kind,
            event_type=event_type,
            venue_status=venue_status,
            observed_at_local=observed_at,
            payload_json=_json_safe(payload) if payload is not None else None,
            details_json=_json_safe(details or {}),
            fingerprint=fingerprint,
        )
        session.add(event)
        await session.flush()
        return event

    def _map_submit_status(self, venue_status: str | None) -> str:
        if venue_status is None:
            return "submitted"
        normalized = venue_status.lower()
        if normalized in {"live", "open"}:
            return "live"
        if normalized in {"matched", "filled"}:
            return "matched"
        if normalized in {"rejected"}:
            return "rejected"
        return "submitted"
