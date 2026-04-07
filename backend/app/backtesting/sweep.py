"""Parameter sweep: run a backtest across a cartesian product of parameter combinations."""
import itertools
import logging
import uuid
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.backtesting.engine import BacktestEngine
from app.models.backtest import BacktestRun

logger = logging.getLogger(__name__)

MAX_SWEEP_COMBINATIONS = 50


def _build_combinations(sweep_params: dict) -> list[dict]:
    """Convert sweep_params into a list of flat config dicts.

    Input example:
        {
            "price_move.threshold_pct": [0.03, 0.05, 0.07],
            "rank_threshold": [0.5, 0.6, 0.7],
        }

    Output: list of dicts like:
        {"price_move.threshold_pct": 0.03, "rank_threshold": 0.5}, ...
    """
    if not sweep_params:
        return [{}]

    keys = list(sweep_params.keys())
    value_lists = [sweep_params[k] for k in keys]
    combos = list(itertools.product(*value_lists))
    return [dict(zip(keys, combo)) for combo in combos]


def _flat_to_detector_configs(flat: dict) -> tuple[dict, float | None]:
    """Unpack a flat combo dict into (detector_configs, rank_threshold).

    Dot-notation keys like "price_move.threshold_pct" become nested:
        {"price_move": {"threshold_pct": value}}
    The special key "rank_threshold" is returned separately.
    """
    detector_configs: dict = {}
    rank_threshold: float | None = None

    for key, value in flat.items():
        if key == "rank_threshold":
            rank_threshold = float(value)
        elif "." in key:
            detector, param = key.split(".", 1)
            detector_configs.setdefault(detector, {})[param] = value
        else:
            # Top-level detector key without a sub-param — store as-is
            detector_configs[key] = value

    return detector_configs, rank_threshold


async def parameter_sweep(
    session: AsyncSession,
    name_prefix: str,
    start_date: datetime,
    end_date: datetime,
    base_detector_configs: dict,
    base_rank_threshold: float,
    sweep_params: dict,
) -> list[BacktestRun]:
    """Run a backtest for each combination in sweep_params.

    Returns the list of BacktestRun objects (all started, some may still be running
    when this function returns — each engine.run() is awaited serially).
    """
    combos = _build_combinations(sweep_params)
    if len(combos) > MAX_SWEEP_COMBINATIONS:
        logger.warning(
            "Sweep requested %d combinations; capping at %d",
            len(combos), MAX_SWEEP_COMBINATIONS,
        )
        combos = combos[:MAX_SWEEP_COMBINATIONS]

    logger.info("Starting parameter sweep: %d combinations", len(combos))
    engine = BacktestEngine()
    runs: list[BacktestRun] = []

    for i, combo in enumerate(combos):
        overridden_configs, overridden_rank = _flat_to_detector_configs(combo)

        # Merge base configs with combo overrides
        merged_configs = {**base_detector_configs}
        for det, params in overridden_configs.items():
            if isinstance(params, dict):
                merged_configs[det] = {**merged_configs.get(det, {}), **params}
            else:
                merged_configs[det] = params

        rank_threshold = overridden_rank if overridden_rank is not None else base_rank_threshold

        # Human-readable name for this combination
        combo_label = ", ".join(f"{k}={v}" for k, v in combo.items())
        run_name = f"{name_prefix} [{i + 1}/{len(combos)}: {combo_label}]"

        run = BacktestRun(
            id=uuid.uuid4(),
            name=run_name,
            start_date=start_date,
            end_date=end_date,
            detector_configs=merged_configs,
            rank_threshold=rank_threshold,
            status="pending",
        )
        session.add(run)
        await session.flush()

        try:
            await engine.run(session, run)
        except Exception:
            logger.error("Sweep combination %d failed", i + 1, exc_info=True)

        runs.append(run)

    return runs
