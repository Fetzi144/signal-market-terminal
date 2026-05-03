from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import DateTime, ForeignKey, Index, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ResearchBatch(Base):
    __tablename__ = "research_batches"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    batch_key: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    preset: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_days: Mapped[int] = mapped_column(Integer, nullable=False, default=30)
    max_markets: Mapped[int] = mapped_column(Integer, nullable=False, default=500)
    universe_fingerprint: Mapped[str] = mapped_column(String(128), nullable=False)
    families_json: Mapped[list | dict | str | None] = mapped_column(JSONB)
    config_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    universe_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    rows_inserted_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    lane_results: Mapped[list["ResearchLaneResult"]] = relationship(
        back_populates="batch",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_research_batches_status_created", "status", "created_at"),
        Index("ix_research_batches_preset_window", "preset", "window_start", "window_end"),
        Index("ix_research_batches_universe", "universe_fingerprint"),
    )


class ResearchLaneResult(Base):
    __tablename__ = "research_lane_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    batch_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("research_batches.id", ondelete="CASCADE"),
        nullable=False,
    )
    family: Mapped[str] = mapped_column(String(64), nullable=False)
    strategy_version: Mapped[str | None] = mapped_column(String(128))
    lane: Mapped[str] = mapped_column(String(64), nullable=False)
    source_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    source_ref: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="completed")
    verdict: Mapped[str] = mapped_column(String(64), nullable=False, default="insufficient_evidence")
    rank_position: Mapped[int | None] = mapped_column(Integer)
    rank_key: Mapped[dict | list | str | None] = mapped_column(JSONB)
    realized_pnl: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    mark_to_market_pnl: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    replay_net_pnl: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    avg_clv: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    resolved_trades: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    fill_rate: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    drawdown: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    open_exposure: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    coverage_mode: Mapped[str | None] = mapped_column(String(64))
    blockers_json: Mapped[list | dict | str | None] = mapped_column(JSONB)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    batch: Mapped[ResearchBatch] = relationship(back_populates="lane_results")

    __table_args__ = (
        UniqueConstraint(
            "batch_id",
            "family",
            "lane",
            "source_kind",
            name="uq_research_lane_results_batch_lane_source",
        ),
        Index("ix_research_lane_results_batch_rank", "batch_id", "rank_position"),
        Index("ix_research_lane_results_family_created", "family", "created_at"),
        Index("ix_research_lane_results_verdict", "verdict"),
    )
