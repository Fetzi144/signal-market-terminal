"""Backtest replay modes and detector-config metadata helpers."""

DETECTOR_REPLAY_MODE = "detector_replay"
STRATEGY_COMPARISON_REPLAY_MODE = "strategy_comparison"
REPLAY_MODE_META_KEY = "_replay_mode"

VALID_REPLAY_MODES = {
    DETECTOR_REPLAY_MODE,
    STRATEGY_COMPARISON_REPLAY_MODE,
}


def resolve_replay_mode(detector_configs: dict | None) -> str:
    if not isinstance(detector_configs, dict):
        return DETECTOR_REPLAY_MODE
    replay_mode = detector_configs.get(REPLAY_MODE_META_KEY)
    if replay_mode in VALID_REPLAY_MODES:
        return replay_mode
    return DETECTOR_REPLAY_MODE


def strip_meta_detector_configs(detector_configs: dict | None) -> dict:
    if not isinstance(detector_configs, dict):
        return {}
    return {
        key: value
        for key, value in detector_configs.items()
        if not str(key).startswith("_")
    }


def with_replay_mode(detector_configs: dict | None, replay_mode: str) -> dict | None:
    configs = dict(detector_configs or {})
    if replay_mode in VALID_REPLAY_MODES:
        configs[REPLAY_MODE_META_KEY] = replay_mode
    return configs or None
