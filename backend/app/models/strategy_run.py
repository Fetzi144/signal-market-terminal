import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import DateTime, ForeignKey, Index, Numeric, String, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class StrategyRun(Base):
    __tablename__ = "strategy_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    strategy_name: Mapped[str] = mapped_column(String(128), nullable=False)
    strategy_family: Mapped[str | None] = mapped_column(String(64))
    strategy_version_id: Mapped[int | None] = mapped_column(
        ForeignKey("strategy_versions.id", ondelete="SET NULL")
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    peak_equity: Mapped[Decimal | None] = mapped_column(Numeric(12, 2))
    current_equity: Mapped[Decimal | None] = mapped_column(Numeric(12, 2))
    max_drawdown: Mapped[Decimal | None] = mapped_column(Numeric(12, 2))
    drawdown_pct: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    contract_snapshot: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        Index("ix_strategy_runs_name_status", "strategy_name", "status"),
        Index("ix_strategy_runs_family_status", "strategy_family", "status"),
        Index("ix_strategy_runs_strategy_version", "strategy_version_id", "created_at"),
        Index("ix_strategy_runs_created", "created_at"),
        Index(
            "uq_strategy_runs_active_name",
            "strategy_name",
            unique=True,
            sqlite_where=text("status = 'active'"),
            postgresql_where=text("status = 'active'"),
        ),
    )
