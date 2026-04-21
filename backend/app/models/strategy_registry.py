from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


VERSION_STATUS_BENCHMARK = "benchmark"
VERSION_STATUS_CANDIDATE = "candidate"
VERSION_STATUS_PROMOTED = "promoted"
VERSION_STATUS_DEMOTED = "demoted"

AUTONOMY_TIER_SHADOW_ONLY = "shadow_only"
AUTONOMY_TIER_ASSISTED_LIVE = "assisted_live"
AUTONOMY_TIER_BOUNDED_AUTO_SUBMIT = "bounded_auto_submit"
AUTONOMY_TIER_BOUNDED_UNATTENDED = "bounded_unattended"


class StrategyFamilyRegistry(Base):
    __tablename__ = "strategy_families_registry"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    family: Mapped[str] = mapped_column(String(64), nullable=False)
    label: Mapped[str] = mapped_column(String(128), nullable=False)
    posture: Mapped[str] = mapped_column(String(32), nullable=False)
    configured: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    review_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    primary_surface: Mapped[str] = mapped_column(String(64), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    disabled_reason: Mapped[str | None] = mapped_column(Text)
    family_kind: Mapped[str] = mapped_column(String(32), nullable=False, default="strategy")
    seeded_from: Mapped[str] = mapped_column(String(32), nullable=False, default="builtin")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        UniqueConstraint("family", name="uq_strategy_families_registry_family"),
        Index("ix_strategy_families_registry_posture", "posture", "updated_at"),
    )


class StrategyVersion(Base):
    __tablename__ = "strategy_versions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    family_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("strategy_families_registry.id", ondelete="CASCADE"),
        nullable=False,
    )
    version_key: Mapped[str] = mapped_column(String(128), nullable=False)
    version_label: Mapped[str] = mapped_column(String(128), nullable=False)
    strategy_name: Mapped[str | None] = mapped_column(String(128))
    version_status: Mapped[str] = mapped_column(String(32), nullable=False)
    autonomy_tier: Mapped[str] = mapped_column(String(32), nullable=False, default=AUTONOMY_TIER_SHADOW_ONLY)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_frozen: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    config_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    provenance_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        UniqueConstraint("version_key", name="uq_strategy_versions_version_key"),
        Index("ix_strategy_versions_family_current", "family_id", "is_current", "updated_at"),
        Index("ix_strategy_versions_status", "version_status", "updated_at"),
    )


class PromotionGatePolicy(Base):
    __tablename__ = "promotion_gate_policies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    policy_key: Mapped[str] = mapped_column(String(128), nullable=False)
    label: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    policy_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        UniqueConstraint("policy_key", name="uq_promotion_gate_policies_policy_key"),
        Index("ix_promotion_gate_policies_status", "status", "updated_at"),
    )


class PromotionEvaluation(Base):
    __tablename__ = "promotion_evaluations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    family_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("strategy_families_registry.id", ondelete="CASCADE"),
        nullable=False,
    )
    strategy_version_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("strategy_versions.id", ondelete="CASCADE"),
        nullable=False,
    )
    gate_policy_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("promotion_gate_policies.id", ondelete="SET NULL"),
    )
    evaluation_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    evaluation_status: Mapped[str] = mapped_column(String(32), nullable=False)
    autonomy_tier: Mapped[str] = mapped_column(String(32), nullable=False, default=AUTONOMY_TIER_SHADOW_ONLY)
    evaluation_window_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    evaluation_window_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    provenance_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    summary_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_promotion_evaluations_version_created", "strategy_version_id", "created_at"),
        Index("ix_promotion_evaluations_family_created", "family_id", "created_at"),
        Index("ix_promotion_evaluations_status", "evaluation_status", "created_at"),
    )


class DemotionEvent(Base):
    __tablename__ = "demotion_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    family_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("strategy_families_registry.id", ondelete="CASCADE"),
        nullable=False,
    )
    strategy_version_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("strategy_versions.id", ondelete="CASCADE"),
        nullable=False,
    )
    prior_autonomy_tier: Mapped[str | None] = mapped_column(String(32))
    fallback_autonomy_tier: Mapped[str] = mapped_column(String(32), nullable=False)
    reason_code: Mapped[str] = mapped_column(String(128), nullable=False)
    cooling_off_ends_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_demotion_events_version_observed", "strategy_version_id", "observed_at_local"),
        Index("ix_demotion_events_family_observed", "family_id", "observed_at_local"),
        Index("ix_demotion_events_reason_observed", "reason_code", "observed_at_local"),
    )

