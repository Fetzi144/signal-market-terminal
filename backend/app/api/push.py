"""Push notification subscription API."""
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import get_db
from app.models.push_subscription import PushSubscription

router = APIRouter(prefix="/api/v1/push", tags=["push"])


class PushSubscribeRequest(BaseModel):
    endpoint: str
    keys: dict  # {p256dh: ..., auth: ...}


class PushSubscribeResponse(BaseModel):
    status: str
    vapid_public_key: str


@router.post("/subscribe", response_model=PushSubscribeResponse)
async def subscribe_push(body: PushSubscribeRequest, db: AsyncSession = Depends(get_db)):
    """Register a browser push subscription."""
    # Upsert: if endpoint already exists, update keys
    existing = await db.execute(
        select(PushSubscription).where(PushSubscription.endpoint == body.endpoint)
    )
    sub = existing.scalar_one_or_none()
    if sub:
        sub.keys = body.keys
    else:
        sub = PushSubscription(endpoint=body.endpoint, keys=body.keys)
        db.add(sub)
    await db.commit()
    return PushSubscribeResponse(status="subscribed", vapid_public_key=settings.push_vapid_public_key)


@router.delete("/subscribe")
async def unsubscribe_push(body: PushSubscribeRequest, db: AsyncSession = Depends(get_db)):
    """Remove a push subscription."""
    await db.execute(
        delete(PushSubscription).where(PushSubscription.endpoint == body.endpoint)
    )
    await db.commit()
    return {"status": "unsubscribed"}


@router.get("/vapid-key")
async def get_vapid_key():
    """Return the VAPID public key for the frontend to use when subscribing."""
    return {"vapid_public_key": settings.push_vapid_public_key}
