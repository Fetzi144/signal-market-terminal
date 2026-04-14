from __future__ import annotations

import os
import socket
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, or_
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.scheduler_lease import SchedulerLease


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def lease_owner_label() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def build_lease_owner_token(*, lease_name: str | None = None) -> str:
    if lease_name:
        return f"{lease_name}:{lease_owner_label()}:{uuid.uuid4()}"
    return f"{lease_owner_label()}:{uuid.uuid4()}"


async def _upsert_named_lease(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    lease_name: str,
    owner_token: str,
    lease_seconds: int,
    allow_takeover: bool,
) -> bool:
    now = utcnow()
    expires_at = now + timedelta(seconds=lease_seconds)

    async with session_factory() as session:
        bind = session.sync_session.get_bind()
        values = {
            "scheduler_name": lease_name,
            "owner_token": owner_token,
            "acquired_at": now,
            "heartbeat_at": now,
            "expires_at": expires_at,
        }
        if bind.dialect.name == "postgresql":
            stmt = postgresql_insert(SchedulerLease).values(values)
            stmt = stmt.on_conflict_do_update(
                index_elements=[SchedulerLease.scheduler_name],
                set_=values,
                where=(
                    SchedulerLease.owner_token == owner_token
                    if not allow_takeover
                    else or_(
                        SchedulerLease.owner_token == owner_token,
                        SchedulerLease.expires_at < now,
                    )
                ),
            )
            await session.execute(stmt)
        elif bind.dialect.name == "sqlite":
            stmt = sqlite_insert(SchedulerLease).values(values)
            stmt = stmt.on_conflict_do_update(
                index_elements=[SchedulerLease.scheduler_name],
                set_=values,
                where=(
                    SchedulerLease.owner_token == owner_token
                    if not allow_takeover
                    else or_(
                        SchedulerLease.owner_token == owner_token,
                        SchedulerLease.expires_at < now,
                    )
                ),
            )
            await session.execute(stmt)
        else:
            lease = await session.get(SchedulerLease, lease_name)
            if lease is None:
                session.add(SchedulerLease(**values))
            elif lease.owner_token == owner_token or (allow_takeover and lease.expires_at < now):
                lease.owner_token = owner_token
                lease.acquired_at = now
                lease.heartbeat_at = now
                lease.expires_at = expires_at
            else:
                await session.rollback()
                return False

        await session.commit()
        lease = await session.get(SchedulerLease, lease_name)
        return lease is not None and lease.owner_token == owner_token


async def acquire_named_lease(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    lease_name: str,
    owner_token: str,
    lease_seconds: int,
) -> bool:
    return await _upsert_named_lease(
        session_factory,
        lease_name=lease_name,
        owner_token=owner_token,
        lease_seconds=lease_seconds,
        allow_takeover=True,
    )


async def renew_named_lease(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    lease_name: str,
    owner_token: str,
    lease_seconds: int,
) -> bool:
    return await _upsert_named_lease(
        session_factory,
        lease_name=lease_name,
        owner_token=owner_token,
        lease_seconds=lease_seconds,
        allow_takeover=False,
    )


async def release_named_lease(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    lease_name: str,
    owner_token: str,
) -> bool:
    async with session_factory() as session:
        result = await session.execute(
            delete(SchedulerLease).where(
                SchedulerLease.scheduler_name == lease_name,
                SchedulerLease.owner_token == owner_token,
            )
        )
        await session.commit()
        return bool(result.rowcount)
