"""Risk management module: exposure limits, correlation clusters, drawdown protection.

Enforces portfolio-level constraints before opening new positions:
- Total exposure cap (default 30% of bankroll)
- Per-cluster exposure cap (default 15% of bankroll) for correlated markets
- Drawdown circuit breaker (halves sizes when P&L drops below threshold)
"""
import logging
import re
from decimal import Decimal

logger = logging.getLogger(__name__)

ZERO = Decimal("0")
LOCAL_PAPER_BOOK_SCOPE = "local_paper_book"


def _local_reason_code(reason: str) -> str:
    if reason.startswith("Total exposure limit reached"):
        return "risk_local_total_exposure"
    if reason.startswith("Cluster exposure limit reached"):
        return "risk_local_cluster_exposure"
    if reason == "Zero or negative size":
        return "risk_local_invalid_size"
    return "risk_local_rejected"


def _result(
    *,
    approved: bool,
    approved_size_usd: Decimal,
    reason: str,
    drawdown_active: bool,
) -> dict:
    reason_code = _local_reason_code(reason)
    return {
        "approved": approved,
        "approved_size_usd": approved_size_usd,
        "reason": reason,
        "reason_code": reason_code,
        "risk_source": "paper_book",
        "risk_scope": LOCAL_PAPER_BOOK_SCOPE,
        "original_reason_code": reason_code,
        "original_reason": reason,
        "drawdown_active": drawdown_active,
    }


def _extract_keywords(question: str) -> set[str]:
    """Extract meaningful keywords from a market question for correlation clustering."""
    # Normalize and split
    words = re.findall(r"[a-zA-Z]+", question.lower())
    # Filter out stop words
    stop_words = {
        "will", "the", "be", "in", "on", "at", "to", "of", "a", "an", "is",
        "by", "for", "or", "and", "this", "that", "it", "with", "from", "as",
        "do", "does", "did", "has", "have", "had", "if", "than", "more",
        "before", "after", "above", "below", "yes", "no", "market",
    }
    return {w for w in words if len(w) > 2 and w not in stop_words}


def compute_keyword_overlap(question_a: str, question_b: str) -> float:
    """Jaccard similarity between keyword sets of two market questions."""
    keywords_a = _extract_keywords(question_a)
    keywords_b = _extract_keywords(question_b)
    if not keywords_a or not keywords_b:
        return 0.0
    intersection = keywords_a & keywords_b
    union = keywords_a | keywords_b
    return len(intersection) / len(union)


def check_exposure(
    open_positions: list[dict],
    new_trade: dict,
    bankroll: Decimal,
    max_total_pct: Decimal = Decimal("0.30"),
    max_cluster_pct: Decimal = Decimal("0.15"),
    drawdown_breaker_pct: Decimal = Decimal("0.15"),
    peak_bankroll: Decimal | None = None,
    cumulative_pnl: Decimal = ZERO,
) -> dict:
    """Check if a new trade passes risk limits.

    Args:
        open_positions: List of dicts with keys: size_usd, market_question, outcome_id
        new_trade: Dict with keys: size_usd, market_question, outcome_id
        bankroll: Current bankroll
        max_total_pct: Max total exposure as fraction of bankroll
        max_cluster_pct: Max exposure in correlated market cluster
        drawdown_breaker_pct: Drawdown threshold to halve sizes
        peak_bankroll: Highest bankroll value seen (for drawdown calc)
        cumulative_pnl: Running P&L

    Returns dict with:
        approved: bool
        approved_size_usd: Decimal (may be reduced)
        reason: str (if not approved or reduced)
        drawdown_active: bool
    """
    requested_size = Decimal(str(new_trade["size_usd"]))
    if requested_size <= ZERO:
        return _result(
            approved=False,
            approved_size_usd=ZERO,
            reason="Zero or negative size",
            drawdown_active=False,
        )

    # Drawdown circuit breaker
    drawdown_active = False
    if peak_bankroll is not None and peak_bankroll > ZERO:
        drawdown = (peak_bankroll - (bankroll + cumulative_pnl)) / peak_bankroll
        if drawdown >= drawdown_breaker_pct:
            drawdown_active = True
            requested_size = (requested_size / 2).quantize(Decimal("0.01"))
            logger.warning(
                "Drawdown breaker active (%.1f%% drawdown), halving size to $%s",
                float(drawdown * 100), requested_size,
            )

    # Total exposure check
    current_exposure = sum(Decimal(str(p["size_usd"])) for p in open_positions)
    max_total = bankroll * max_total_pct
    remaining_capacity = max_total - current_exposure

    if remaining_capacity <= ZERO:
        return _result(
            approved=False,
            approved_size_usd=ZERO,
            reason=f"Total exposure limit reached (${current_exposure:.2f} / ${max_total:.2f})",
            drawdown_active=drawdown_active,
        )

    if requested_size > remaining_capacity:
        requested_size = remaining_capacity.quantize(Decimal("0.01"))

    # Cluster exposure check
    new_question = new_trade.get("market_question", "")
    cluster_exposure = ZERO
    similarity_threshold = 0.3

    for pos in open_positions:
        pos_question = pos.get("market_question", "")
        similarity = compute_keyword_overlap(new_question, pos_question)
        if similarity >= similarity_threshold:
            cluster_exposure += Decimal(str(pos["size_usd"]))

    max_cluster = bankroll * max_cluster_pct
    cluster_remaining = max_cluster - cluster_exposure

    if cluster_remaining <= ZERO:
        return _result(
            approved=False,
            approved_size_usd=ZERO,
            reason=f"Cluster exposure limit reached (${cluster_exposure:.2f} / ${max_cluster:.2f})",
            drawdown_active=drawdown_active,
        )

    if requested_size > cluster_remaining:
        requested_size = cluster_remaining.quantize(Decimal("0.01"))

    return _result(
        approved=True,
        approved_size_usd=requested_size,
        reason="approved" if not drawdown_active else "approved (drawdown-reduced)",
        drawdown_active=drawdown_active,
    )
