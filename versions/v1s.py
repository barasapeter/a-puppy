# signal_core.py


def infer_market_bias(
    stats: dict,
    current_streak_direction: str,
    min_samples: int = 0,
    min_edge: float = 0.0,
) -> str:
    """
    Decide CALL / PUT / SKIP from the stats of the CURRENT streak direction only.

    stats = {
        "UP":   {"continuation": int, "reversal": int, "neutral": int},
        "DOWN": {"continuation": int, "reversal": int, "neutral": int},
    }

    min_samples : minimum decided (continuation + reversal) samples required
                  in this direction's bucket before trading.
    min_edge    : minimum gap between the winning and losing rate
                  (e.g. 0.05 = 5 percentage points) before trading.
    """
    bucket = stats.get(current_streak_direction)
    if not bucket:
        return "SKIP"

    continuation = bucket.get("continuation", 0)
    reversal = bucket.get("reversal", 0)
    decided = continuation + reversal

    if decided == 0 or decided < min_samples:
        return "SKIP"

    if abs(continuation - reversal) / decided < min_edge:
        return "SKIP"

    is_up = current_streak_direction == "UP"
    behavior = "REVERSAL" if reversal > continuation else "CONTINUATION"

    follow_up = is_up if behavior == "CONTINUATION" else not is_up
    return "CALL" if follow_up else "PUT"
