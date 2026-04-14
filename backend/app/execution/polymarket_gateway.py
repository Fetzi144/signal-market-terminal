from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from app.config import settings
from app.ingestion.polymarket_common import parse_polymarket_timestamp


class GatewayUnavailableError(RuntimeError):
    """Raised when the Polymarket trading gateway cannot be used."""


@dataclass(slots=True)
class GatewayOrderRequest:
    asset_id: str
    side: str
    price: Decimal
    size: Decimal
    client_order_id: str
    order_type: str
    post_only: bool


@dataclass(slots=True)
class GatewaySubmitResult:
    venue_order_id: str | None
    venue_status: str | None
    payload: dict[str, Any]
    submitted_size: Decimal | None = None
    submitted_at: datetime | None = None


@dataclass(slots=True)
class GatewayCancelResult:
    venue_order_id: str | None
    venue_status: str | None
    payload: dict[str, Any]


class PolymarketGateway:
    """Repo-local wrapper around the Polymarket trading client surface."""

    def __init__(self, *, client: Any | None = None) -> None:
        self._client = client

    @property
    def has_user_credentials(self) -> bool:
        return bool(
            settings.polymarket_api_key
            and settings.polymarket_api_secret
            and settings.polymarket_api_passphrase
        )

    @property
    def has_submit_credentials(self) -> bool:
        return bool(settings.polymarket_private_key and settings.polymarket_funder_address)

    def is_configured(self) -> bool:
        return self._client is not None or self.has_user_credentials or self.has_submit_credentials

    def user_stream_subscription_payload(self, markets: list[str]) -> dict[str, Any]:
        if not self.has_user_credentials:
            raise GatewayUnavailableError("Polymarket user-stream credentials are not configured")
        return {
            "auth": {
                "apiKey": settings.polymarket_api_key,
                "secret": settings.polymarket_api_secret,
                "passphrase": settings.polymarket_api_passphrase,
            },
            "markets": markets,
            "type": "user",
        }

    async def healthcheck(self) -> dict[str, Any]:
        if not self.is_configured():
            raise GatewayUnavailableError("Polymarket gateway is not configured")
        orders = await self.fetch_user_orders(limit=1)
        return {"reachable": True, "sample_count": len(orders)}

    async def submit_order(self, request: GatewayOrderRequest) -> GatewaySubmitResult:
        client = self._require_client(for_submit=True)
        if hasattr(client, "submit_order"):
            result = await self._call(client.submit_order, request)
            return self._coerce_submit_result(result)

        if hasattr(client, "create_and_post_order"):
            result = await self._call(client.create_and_post_order, self._sdk_order_args(request))
            return self._coerce_submit_result(result)

        create_method = getattr(client, "create_order", None)
        post_method = getattr(client, "post_order", None) or getattr(client, "postOrder", None)
        if create_method is None or post_method is None:
            raise GatewayUnavailableError("Configured Polymarket client does not expose an order submit surface")

        order = await self._call(create_method, self._sdk_order_args(request))
        try:
            result = await self._call(post_method, order, request.order_type)
        except TypeError:
            try:
                result = await self._call(post_method, order, orderType=request.order_type)
            except TypeError:
                result = await self._call(
                    post_method,
                    order,
                    orderType=request.order_type,
                    postOnly=request.post_only,
                )
        return self._coerce_submit_result(result)

    async def cancel_order(self, venue_order_id: str) -> GatewayCancelResult:
        client = self._require_client(for_submit=True)
        if hasattr(client, "cancel_order"):
            result = await self._call(client.cancel_order, venue_order_id)
            return self._coerce_cancel_result(result, venue_order_id=venue_order_id)

        if hasattr(client, "cancel_orders"):
            result = await self._call(client.cancel_orders, [venue_order_id])
            return self._coerce_cancel_result(result, venue_order_id=venue_order_id)

        raise GatewayUnavailableError("Configured Polymarket client does not expose a cancel surface")

    async def fetch_order_status(self, venue_order_id: str) -> dict[str, Any]:
        client = self._require_client()
        method = getattr(client, "get_order", None) or getattr(client, "get_active_order", None)
        if method is None:
            raise GatewayUnavailableError("Configured Polymarket client does not expose an order lookup surface")
        result = await self._call(method, venue_order_id)
        return self._jsonable(result)

    async def fetch_user_orders(self, *, limit: int = 50) -> list[dict[str, Any]]:
        client = self._require_client()
        method = (
            getattr(client, "get_orders", None)
            or getattr(client, "get_open_orders", None)
            or getattr(client, "list_orders", None)
        )
        if method is None:
            raise GatewayUnavailableError("Configured Polymarket client does not expose a user-orders surface")
        try:
            result = await self._call(method, limit=limit)
        except TypeError:
            result = await self._call(method)
        return self._coerce_list(result)

    async def fetch_user_trades(self, *, limit: int = 100, asset_id: str | None = None) -> list[dict[str, Any]]:
        client = self._require_client()
        method = getattr(client, "get_trades", None) or getattr(client, "list_trades", None)
        if method is None:
            raise GatewayUnavailableError("Configured Polymarket client does not expose a user-trades surface")
        params = {"limit": limit}
        if asset_id:
            params["asset_id"] = asset_id
        try:
            result = await self._call(method, **params)
        except TypeError:
            result = await self._call(method)
        return self._coerce_list(result)

    def _require_client(self, *, for_submit: bool = False) -> Any:
        if self._client is not None:
            return self._client
        if for_submit and not self.has_submit_credentials:
            raise GatewayUnavailableError("Polymarket submit credentials are not configured")
        if not self.has_user_credentials and not self.has_submit_credentials:
            raise GatewayUnavailableError("Polymarket gateway credentials are not configured")
        self._client = self._create_default_client()
        return self._client

    def _create_default_client(self) -> Any:
        try:
            from py_clob_client.client import ClobClient
        except ImportError as exc:
            raise GatewayUnavailableError("py-clob-client is not installed") from exc

        kwargs: dict[str, Any] = {
            "host": settings.polymarket_clob_host,
            "chain_id": settings.polymarket_chain_id,
        }
        if settings.polymarket_private_key:
            kwargs["key"] = settings.polymarket_private_key
            kwargs["signature_type"] = settings.polymarket_signature_type
        if settings.polymarket_funder_address:
            kwargs["funder"] = settings.polymarket_funder_address

        client = ClobClient(**kwargs)

        if self.has_user_credentials:
            try:
                from py_clob_client.clob_types import ApiCreds
            except ImportError:
                ApiCreds = None  # type: ignore[assignment]
            if ApiCreds is not None and hasattr(client, "set_api_creds"):
                client.set_api_creds(
                    ApiCreds(
                        api_key=settings.polymarket_api_key,
                        api_secret=settings.polymarket_api_secret,
                        api_passphrase=settings.polymarket_api_passphrase,
                    )
                )
        elif self.has_submit_credentials and hasattr(client, "create_or_derive_api_creds") and hasattr(client, "set_api_creds"):
            creds = client.create_or_derive_api_creds()
            client.set_api_creds(creds)

        return client

    def _sdk_order_args(self, request: GatewayOrderRequest) -> Any:
        try:
            from py_clob_client.clob_types import OrderArgs
            from py_clob_client.constants import BUY, SELL
        except ImportError as exc:
            raise GatewayUnavailableError("py-clob-client order helpers are unavailable") from exc

        side = BUY if str(request.side).upper() == "BUY" else SELL
        return OrderArgs(
            price=float(request.price),
            size=float(request.size),
            side=side,
            token_id=request.asset_id,
        )

    async def _call(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(fn, *args, **kwargs)

    def _coerce_submit_result(self, result: Any) -> GatewaySubmitResult:
        payload = self._jsonable(result)
        return GatewaySubmitResult(
            venue_order_id=self._first_text(payload, "orderID", "order_id", "id"),
            venue_status=self._first_text(payload, "status", "order_status"),
            payload=payload,
            submitted_size=self._first_decimal(payload, "size", "original_size"),
            submitted_at=self._first_datetime(payload, "timestamp", "submitted_at", "created_at"),
        )

    def _coerce_cancel_result(self, result: Any, *, venue_order_id: str | None) -> GatewayCancelResult:
        payload = self._jsonable(result)
        return GatewayCancelResult(
            venue_order_id=self._first_text(payload, "orderID", "order_id", "id") or venue_order_id,
            venue_status=self._first_text(payload, "status", "order_status"),
            payload=payload,
        )

    def _coerce_list(self, result: Any) -> list[dict[str, Any]]:
        payload = self._jsonable(result)
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            for key in ("data", "results", "orders", "trades"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        return []

    def _jsonable(self, result: Any) -> dict[str, Any] | list[Any]:
        if result is None:
            return {}
        if isinstance(result, (dict, list)):
            return result
        if hasattr(result, "model_dump"):
            return result.model_dump()
        if hasattr(result, "dict"):
            return result.dict()
        if hasattr(result, "__dict__"):
            return {
                key: value
                for key, value in result.__dict__.items()
                if not key.startswith("_")
            }
        if hasattr(result, "__dataclass_fields__"):
            return asdict(result)
        return {"value": str(result)}

    def _first_text(self, payload: dict[str, Any] | list[Any], *keys: str) -> str | None:
        if not isinstance(payload, dict):
            return None
        for key in keys:
            value = payload.get(key)
            if value is not None:
                return str(value)
        return None

    def _first_decimal(self, payload: dict[str, Any] | list[Any], *keys: str) -> Decimal | None:
        if not isinstance(payload, dict):
            return None
        for key in keys:
            value = payload.get(key)
            if value in (None, ""):
                continue
            try:
                return Decimal(str(value))
            except Exception:
                continue
        return None

    def _first_datetime(self, payload: dict[str, Any] | list[Any], *keys: str) -> datetime | None:
        if not isinstance(payload, dict):
            return None
        for key in keys:
            value = payload.get(key)
            if isinstance(value, datetime):
                return value
            parsed = parse_polymarket_timestamp(value)
            if parsed is not None:
                return parsed
        return None
