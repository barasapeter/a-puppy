# signal_core.py

import random


def infer_market_bias(stats: dict, current_streak_direction: str) -> str:
    continuation = stats.get("continuation", 0)
    reversal = stats.get("reversal", 0)
    is_up = current_streak_direction == "UP"

    if continuation == 0 and reversal == 0:
        return "SKIP"

    behavior = "REVERSAL" if reversal > continuation else "CONTINUATION"

    follow_up = is_up if behavior == "CONTINUATION" else not is_up
    return "CALL" if follow_up else "PUT"
