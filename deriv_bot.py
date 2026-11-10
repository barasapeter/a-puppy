"""
Deriv tick-streak bot (optimised rewrite, per-direction bias).

Key ideas
---------
* LOOKAHEAD ("virtual streak"): a buy never executes on the signal tick, it
  executes on the tick that arrives after the server has processed it (usually
  1-2 ticks later).  So we fire the order *early*, assuming the next
  LOOKAHEAD ticks continue the streak:

      virtual streak = real streak + LOOKAHEAD
      fire when virtual streak >= STREAK

* PER-DIRECTION BIAS: continuation/reversal statistics are kept separately for
  UP streaks and DOWN streaks.  The trade decision for an UP streak uses only
  the UP statistics, and for a DOWN streak only the DOWN statistics.

* AUTO_LOOKAHEAD: every resolved contract reports `entry_tick_time`.  We turn
  that into "ticks between signal and entry" and keep a rolling median, so the
  look-ahead tunes itself to your real latency.

* The busy flag is set BEFORE the order is sent (no double orders while a buy
  confirmation is in flight) and is always released on errors / reconnects.

* [SEGMENT] SLIDING STATS SEGMENTS: when a direction's decided samples
  (continuation + reversal) reach SEGMENT_LIMIT, that direction's stats are
  rebuilt from only its latest SEGMENT_KEEP decided outcomes and the segment
  counter goes up.  The stats then grow again by (SEGMENT_LIMIT - SEGMENT_KEEP)
  new samples until the limit is hit again, and so on.

* [RECOVERY] After a WIN, if the net P&L is still negative (long losing streak
  and payout < 100%), the next stake is sized so that one more win lifts the
  net P&L above zero:  stake = deficit / payout_rate + BASE_STAKE.  Martingale
  doubling then continues from that stake on losses; a win that leaves the net
  P&L >= 0 resets everything to BASE_STAKE.

* [TIMEOUT] If a confirmed contract is still unresolved UNRESOLVED_TIMEOUT_TICKS
  ticks after its buy confirmation, the circuit stays locked (busy) and the bot
  asks the server for the contract's real status (one-shot proposal_open_contract),
  one query per tick.  A settled answer goes through the normal resolution path
  (real win/loss, JSON logs included).  Only after QUERY_MAX_FAILURES failed
  queries (API error, no answer, or still not settled) is the contract treated
  as an ASSUMED LOSS: it advances P&L / loss streak / martingale like a real
  loss, URSVD goes up by one, the circuit reopens, and nothing is written to
  the JSON logs.  A result that arrives afterwards is only reported.

* [LOSSLOG] Every resolved contract is appended as a compact checkpoint report
  (signal tick, expected execution tick, confirmation / entry tick, direction
  checks, outcome, and a tick tape from the streak start to the exit).
  LOST contracts go to LOSS_LOG_FILE, WON contracts to WIN_LOG_FILE (both JSON
  arrays).  Purely observational.

* [INVERT] ADAPTIVE SIGNAL INVERSION: infer_market_bias still produces the
  original signal (PUT / CALL / SKIP) exactly as before.  Just before a PUT or
  CALL is sent, the bot looks at the VIRTUAL win rate (how the ORIGINAL signals
  would have performed) and decides whether to flip the signal:

      virtual win rate <= ACTIVE threshold  -> inversion ON  (PUT<->CALL)
      virtual win rate  > ACTIVE threshold  -> inversion OFF
      no virtual trades yet                 -> inversion OFF

  SKIP is never inverted and never traded.  The inversion state is stored on
  the order when it is created and never changes.  On resolution each trade
  updates, exactly once:
      real wins/losses          - what actually happened
      virtual wins/losses       - what the original signal would have done
                                  (outcome reversed for inverted trades)
      inverted / non-inverted   - actual outcomes split by inversion state
  The existing cumulative wins/losses and everything else are untouched.

* [ROLLING] The virtual win rate that drives inversion is the ACTIVE virtual
  win rate, chosen by VIRTUAL_WIN_RATE_MODE:
      "WINDOW" - only the latest VIRTUAL_WIN_RATE_WINDOW resolved virtual
                 outcomes (default 12); the oldest one drops out as soon as a
                 new one arrives
      "ALL"    - every resolved virtual outcome of the session
  Lifetime virtual wins/losses are still kept (reporting only) and never drive
  the inversion decision in WINDOW mode.

* [THRESHOLD] EVIDENCE-WEIGHTED, REVERSIBLE THRESHOLD: INVERSION_THRESHOLD is
  the permanent BASELINE.  The threshold actually compared with the virtual
  win rate (VWR) is the ACTIVE threshold, which is recomputed after EVERY
  resolved contract from a stack of weighted loss entries and the current VWR.

  p = active VWR as a fraction, clamped to [THRESHOLD_P_FLOOR, THRESHOLD_P_CEIL].

  Real LOSS (p = VWR after this trade was recorded):
      S_TRIG = ln(1 / THRESHOLD_ALPHA)
      normal trade   : s = -ln(1 - p)      inverted trade : s = -ln(p)
      w = min(THRESHOLD_MAX_STEP_WEIGHT, s / S_TRIG)
      push {"inverted", "weight", "position"} on the stack
  Real WIN: streak resets and the most recent stack entry whose "inverted"
      flag equals the resolved trade's flag is popped (none -> nothing pops).

  Recompute after every resolution:
      U = sum of weights of normal entries, D = sum for inverted entries
      N = clamp(U - D, -1, 1)
      T_up = max(baseline, VWR% + MARGIN)   T_dn = min(baseline, VWR% - MARGIN)
      N >= 0 : thr = baseline + (T_up - baseline) * N
      N <  0 : thr = baseline - (baseline - T_dn) * |N|
      active = clamp(thr, THRESHOLD_MIN, THRESHOLD_MAX); empty stack = baseline.

  Stack entries expire after THRESHOLD_ENTRY_TTL resolved trades, and
  [WARMUP] inversion cannot turn ON before INVERSION_MIN_SAMPLES virtual
  outcomes exist.

  The direction comes from the trade's own stored `inverted` flag
  (trade_meta), never from the bot's current inversion state.  The detector
  runs inside record_inversion_result, i.e. through the one existing
  resolution path, so every resolved contract (normal, status query or
  assumed loss) affects it exactly once and a late result never does.

* [BRAKE] LOSS-STREAK BRAKE: the threshold feedback can flip the bot between
  normal and inverted mode after EVERY loss (a normal loss raises the threshold,
  an inverted loss lowers it), which just alternates sides in a choppy market.
  Once the consecutive real-loss streak reaches LOSS_STREAK_BRAKE_AFTER:
      "HOLD" - the inversion mode of the last resolved trade is kept instead of
               being re-decided, BUT ONLY while the evidence is ambiguous, i.e.
               the virtual win rate is within LOSS_STREAK_BRAKE_HOLD_MARGIN
               points of the active threshold.  A decisive virtual win rate
               (further away than the margin) always goes through the normal
               threshold rule, so the brake can never pin the bot to a mode
               that the virtual win rate clearly says is losing.
      "SKIP" - the next LOSS_STREAK_BRAKE_SKIP_SIGNALS signals are skipped, then
               one trade is allowed again (and the skip counter re-arms while
               the streak continues)
  A real win ends the brake.  The martingale is NOT affected by the brake.
"""

import asyncio
import json
import math
import os
import time
import traceback
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from statistics import median_low
import sys

import websockets

from auth import get_ws_url
from signal_core import infer_market_bias

try:  # optional speed-up: pip install orjson
    import orjson

    loads = orjson.loads

    def dumps(obj):
        return orjson.dumps(obj).decode()  # Deriv expects text frames

except ImportError:
    loads = json.loads
    dumps = json.dumps

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
ACCOUNT_TYPE = "demo"
SYMBOLS = ["1HZ100V"]
CURRENCY = "USD"

STREAK = 3  # target streak length (virtual, includes look-ahead ticks)
BIAS_WINDOW = 1  # ticks measured after the (virtual) streak completes

LOOKAHEAD = 1  # ticks assumed to continue the streak (0 = legacy behaviour)
AUTO_LOOKAHEAD = False  # tune LOOKAHEAD from measured entry lag
MAX_LOOKAHEAD = 3

# Per-direction decision guards (0 / 0.0 = same behaviour as before)
MIN_SAMPLES = 1  # min decided samples in the current direction's bucket
MIN_EDGE = 0.05  # 0.05  # min gap between reversal and continuation rate (0.05 = 5 pts)

# [SEGMENT] Sliding stats segments (per direction)
SEGMENT_ENABLED = True
SEGMENT_LIMIT = 50  # when decided samples in a direction reach this -> new segment
SEGMENT_KEEP = 20  # latest decided samples carried over into the new segment
# Growth per segment = SEGMENT_LIMIT - SEGMENT_KEEP

USE_PROPOSALS = False  # pre-priced proposals: skips server-side pricing on buy
PRINT_TICKS = True

BASE_STAKE = 1
MARTINGALE_MULTIPLIER = 2
MAX_MARTINGALE_STEPS = 5
MAX_STAKE = 70.0
TARGET_PL = 15.0

# [TIMEOUT] unresolved-contract watchdog
UNRESOLVED_TIMEOUT_TICKS = 10  # ticks after buy confirmation before querying
STATUS_QUERY_ENABLED = True  # ask the server for the real contract status first
QUERY_MAX_FAILURES = 3  # failed status queries before falling back
ASSUME_LOSS_ON_TIMEOUT = True  # final fallback: count the contract as lost

# [RECOVERY] size the post-win stake so the net P&L is forced positive
RECOVERY_ENABLED = True

# [LOSSLOG]
LOSS_LOG_ENABLED = False
LOSS_LOG_FILE = "loss_events.json"  # JSON array, one object per lost trade
WIN_LOG_ENABLED = False
WIN_LOG_FILE = "win_events.json"  # JSON array, one object per won trade
TICK_LOG_SIZE = 40  # recent ticks kept in memory for the tape

# [INVERT] adaptive signal inversion: ON when virtual win rate <= threshold
# (percent).  This is the permanent BASELINE; the threshold actually used is
# the ACTIVE threshold, adjusted by the [THRESHOLD] detector below.
INVERSION_THRESHOLD = 46.0
# [WARMUP] inversion can only turn ON once this many virtual outcomes exist.  With
# 1-9 samples the virtual win rate jumps 10-50 points per trade, so the
# threshold test degenerates into "did the original signal win last time?" and
# the bot alternates normal / inverted on every trade (see the 8-loss opening
# streak).  0 = no warm-up (old behaviour).
INVERSION_MIN_SAMPLES = 5
# [SIGNAL] a PUT/CALL signal is only traded when its direction's bias is
# statistically distinguishable from a coin flip:
#     z = |continuation - reversal| / sqrt(continuation + reversal)
# MIN_EDGE alone passes ~55/45 splits at N~40 (z ~ 0.6), which is pure noise.
# 0 = off (old behaviour).
SIGNAL_MIN_Z = 0.5 #1.0

# [THRESHOLD] evidence-weighted, reversible adjustment of the active threshold
THRESHOLD_ALPHA = (
    0.05  # 0 < ALPHA < 1; a run less likely than this by chance gets the full push
)
THRESHOLD_MARGIN = 1.5 #1.0  # percentage points, >= 0
THRESHOLD_MAX_STEP_WEIGHT = (
    0.5  # 0 < x <= 1; 0.5 means never fully flips in fewer than 2 losses
)
THRESHOLD_P_FLOOR = 0.05  # lower clamp of the virtual win rate fraction
THRESHOLD_P_CEIL = 0.95  # upper clamp of the virtual win rate fraction
THRESHOLD_MIN = 20.0  # wide safety rail: the active threshold never goes below this
THRESHOLD_MAX = 90.0  # wide safety rail: the active threshold never goes above this
THRESHOLD_PRECISION = 6  # decimals kept: prevents float drift from the baseline
# Stack entries older than this many RESOLVED trades are dropped.  Without it an
# entry of one type (e.g. inverted losses from an early phase) is only removed by
# a win of the same type, which may never come, and its weight cancels every
# later push of the opposite type forever (detector stays pinned at baseline).
THRESHOLD_ENTRY_TTL = 10

# [BRAKE] loss-streak brake (stops the normal <-> inverted flip-flop in a streak)
LOSS_STREAK_BRAKE_ENABLED = False
LOSS_STREAK_BRAKE_AFTER = 7  # consecutive real losses before the brake engages
LOSS_STREAK_BRAKE_ACTION = "HOLD"  # "HOLD" = keep last mode, "SKIP" = skip signals
# HOLD only applies while |virtual win rate - active threshold| <= this many
# percentage points (ambiguous zone); beyond it the threshold rule decides.
LOSS_STREAK_BRAKE_HOLD_MARGIN = 3.0
LOSS_STREAK_BRAKE_SKIP_SIGNALS = 3  # signals skipped per cycle in SKIP mode

# [ROLLING] how the ACTIVE virtual win rate (the one that drives inversion) is
# calculated:  "WINDOW" = latest N resolved virtual outcomes, "ALL" = all of them
VIRTUAL_WIN_RATE_MODE = "ALL"  # "WINDOW" or "ALL"
VIRTUAL_WIN_RATE_WINDOW = 18  # N for WINDOW mode (positive integer)

REQ_TICKS, REQ_BUY, REQ_POC = 1, 5005, 7007
REQ_POC_QUERY = 7008  # [TIMEOUT] one-shot status query
REQ_PROPOSAL = {"CALL": 8001, "PUT": 8002}

DIRECTIONS = ("UP", "DOWN")

# [INVERT] PUT <-> CALL (SKIP is never inverted)
INVERSE_SIGNAL = {"PUT": "CALL", "CALL": "PUT"}


def timestamp(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%H:%M:%S")


def new_stats():
    """Fresh per-direction statistics container."""
    return {d: {"continuation": 0, "reversal": 0, "neutral": 0} for d in DIRECTIONS}


# [SEGMENT] helpers
def new_history():
    """Per-direction ordered outcomes of the current segment."""
    return {d: [] for d in DIRECTIONS}


def new_segments():
    """Per-direction segment counter (starts at segment 1)."""
    return {d: 1 for d in DIRECTIONS}


# [LOSSLOG] helpers
def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _fmt_tick(t):
    """'07:10:29 | 1170.03 | UP [2]'"""
    if t is None:
        return None
    return f"{timestamp(t['epoch'])} | {t['price']} | {t['dir']} [{t['cnt']}]"


# [INVERT] helpers
def _rate_text(wins, losses):
    """'55.6% (5W 4L)' or 'n/a (0W 0L)' when nothing has been counted yet."""
    total = wins + losses
    if not total:
        return f"n/a ({wins}W {losses}L)"
    return f"{wins / total * 100:.1f}% ({wins}W {losses}L)"


# [THRESHOLD] helpers
def _round_pct(value):
    """Round a percentage so repeated add / subtract can never drift."""
    return round(float(value), THRESHOLD_PRECISION)


def _pct_text(value):
    """48.5 -> '48.5%', 44.0 -> '44.0%', 48.25 -> '48.25%'."""
    text = f"{value:.4f}".rstrip("0")
    if text.endswith("."):
        text += "0"
    return text + "%"


def _signed_pct_text(value):
    """+2.5 -> '+2.5%', -3.0 -> '-3.0%', 0 -> '0.0%'."""
    if value == 0:
        return "0.0%"
    return ("+" if value > 0 else "-") + _pct_text(abs(value))


# [ROLLING] configuration validation (fails loudly instead of miscalculating)
def validate_virtual_rate_config():
    if VIRTUAL_WIN_RATE_MODE not in ("WINDOW", "ALL"):
        raise ValueError(
            f"VIRTUAL_WIN_RATE_MODE must be 'WINDOW' or 'ALL', "
            f"got {VIRTUAL_WIN_RATE_MODE!r}"
        )
    if VIRTUAL_WIN_RATE_MODE == "WINDOW" and (
        isinstance(VIRTUAL_WIN_RATE_WINDOW, bool)
        or not isinstance(VIRTUAL_WIN_RATE_WINDOW, int)
        or VIRTUAL_WIN_RATE_WINDOW <= 0
    ):
        raise ValueError(
            f"VIRTUAL_WIN_RATE_WINDOW must be a positive integer in WINDOW mode, "
            f"got {VIRTUAL_WIN_RATE_WINDOW!r}"
        )


validate_virtual_rate_config()


# [THRESHOLD] configuration validation
def validate_threshold_config():
    if isinstance(THRESHOLD_ALPHA, bool) or not (0.0 < THRESHOLD_ALPHA < 1.0):
        raise ValueError(
            f"THRESHOLD_ALPHA must satisfy 0 < ALPHA < 1, got {THRESHOLD_ALPHA!r}"
        )
    if (
        isinstance(THRESHOLD_MARGIN, bool)
        or not isinstance(THRESHOLD_MARGIN, (int, float))
        or THRESHOLD_MARGIN < 0
    ):
        raise ValueError(
            f"THRESHOLD_MARGIN must be a number >= 0, got {THRESHOLD_MARGIN!r}"
        )
    if isinstance(THRESHOLD_MAX_STEP_WEIGHT, bool) or not (
        0.0 < THRESHOLD_MAX_STEP_WEIGHT <= 1.0
    ):
        raise ValueError(
            f"THRESHOLD_MAX_STEP_WEIGHT must satisfy 0 < x <= 1, "
            f"got {THRESHOLD_MAX_STEP_WEIGHT!r}"
        )
    if not (0.0 < THRESHOLD_P_FLOOR <= THRESHOLD_P_CEIL < 1.0):
        raise ValueError(
            f"THRESHOLD_P_FLOOR / THRESHOLD_P_CEIL must satisfy "
            f"0 < FLOOR <= CEIL < 1, got {THRESHOLD_P_FLOOR!r} / {THRESHOLD_P_CEIL!r}"
        )
    if not (0.0 <= THRESHOLD_MIN <= THRESHOLD_MAX <= 100.0):
        raise ValueError(
            f"THRESHOLD_MIN / THRESHOLD_MAX must satisfy "
            f"0 <= MIN <= MAX <= 100, got {THRESHOLD_MIN!r} / {THRESHOLD_MAX!r}"
        )
    if not (THRESHOLD_MIN <= INVERSION_THRESHOLD <= THRESHOLD_MAX):
        raise ValueError(
            f"INVERSION_THRESHOLD ({INVERSION_THRESHOLD!r}) must lie within "
            f"[THRESHOLD_MIN, THRESHOLD_MAX] = [{THRESHOLD_MIN!r}, {THRESHOLD_MAX!r}]"
        )
    if (
        isinstance(THRESHOLD_PRECISION, bool)
        or not isinstance(THRESHOLD_PRECISION, int)
        or THRESHOLD_PRECISION < 0
    ):
        raise ValueError(
            f"THRESHOLD_PRECISION must be a non-negative integer, "
            f"got {THRESHOLD_PRECISION!r}"
        )
    if (
        isinstance(THRESHOLD_ENTRY_TTL, bool)
        or not isinstance(THRESHOLD_ENTRY_TTL, int)
        or THRESHOLD_ENTRY_TTL <= 0
    ):
        raise ValueError(
            f"THRESHOLD_ENTRY_TTL must be a positive integer, "
            f"got {THRESHOLD_ENTRY_TTL!r}"
        )
    if (
        isinstance(INVERSION_MIN_SAMPLES, bool)
        or not isinstance(INVERSION_MIN_SAMPLES, int)
        or INVERSION_MIN_SAMPLES < 0
    ):
        raise ValueError(
            f"INVERSION_MIN_SAMPLES must be a non-negative integer, "
            f"got {INVERSION_MIN_SAMPLES!r}"
        )
    if (
        isinstance(SIGNAL_MIN_Z, bool)
        or not isinstance(SIGNAL_MIN_Z, (int, float))
        or SIGNAL_MIN_Z < 0
    ):
        raise ValueError(f"SIGNAL_MIN_Z must be a number >= 0, got {SIGNAL_MIN_Z!r}")
    if (
        VIRTUAL_WIN_RATE_MODE == "WINDOW"
        and INVERSION_MIN_SAMPLES > VIRTUAL_WIN_RATE_WINDOW
    ):
        raise ValueError(
            f"INVERSION_MIN_SAMPLES ({INVERSION_MIN_SAMPLES}) cannot exceed "
            f"VIRTUAL_WIN_RATE_WINDOW ({VIRTUAL_WIN_RATE_WINDOW}) in WINDOW mode"
        )


validate_threshold_config()


# [BRAKE] configuration validation
def validate_brake_config():
    if LOSS_STREAK_BRAKE_ACTION not in ("HOLD", "SKIP"):
        raise ValueError(
            f"LOSS_STREAK_BRAKE_ACTION must be 'HOLD' or 'SKIP', "
            f"got {LOSS_STREAK_BRAKE_ACTION!r}"
        )
    for name, value in (
        ("LOSS_STREAK_BRAKE_AFTER", LOSS_STREAK_BRAKE_AFTER),
        ("LOSS_STREAK_BRAKE_SKIP_SIGNALS", LOSS_STREAK_BRAKE_SKIP_SIGNALS),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer, got {value!r}")
    if (
        isinstance(LOSS_STREAK_BRAKE_HOLD_MARGIN, bool)
        or not isinstance(LOSS_STREAK_BRAKE_HOLD_MARGIN, (int, float))
        or LOSS_STREAK_BRAKE_HOLD_MARGIN < 0
    ):
        raise ValueError(
            f"LOSS_STREAK_BRAKE_HOLD_MARGIN must be a number >= 0, "
            f"got {LOSS_STREAK_BRAKE_HOLD_MARGIN!r}"
        )


validate_brake_config()


# --------------------------------------------------------------------------- #
# State containers
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class Window:
    direction: str
    start_price: float | None  # None until the window opens
    wait: int  # ticks left before the window opens
    ticks_seen: int = 0


@dataclass(slots=True)
class SymbolState:
    prev: float | None = None
    direction: str | None = None
    count: int = 0
    alerted: bool = False
    tick_idx: int = 0
    recent: deque = field(default_factory=lambda: deque(maxlen=30))  # (epoch, idx)
    windows: list = field(default_factory=list)
    stats: dict = field(default_factory=new_stats)
    history: dict = field(default_factory=new_history)  # [SEGMENT]
    segments: dict = field(default_factory=new_segments)  # [SEGMENT]
    # [LOSSLOG] rolling tape: {"idx", "epoch", "price", "dir", "cnt"}
    tick_log: deque = field(default_factory=lambda: deque(maxlen=TICK_LOG_SIZE))

    def reset_stream(self):
        """Clear tick state after a reconnect (stats are kept for the session)."""
        self.prev = None
        self.direction = None
        self.count = 0
        self.alerted = False
        self.recent.clear()
        self.windows.clear()


@dataclass(slots=True)
class Order:
    symbol: str
    contract_type: str  # the signal actually executed
    signal_idx: int
    t_sent: float
    contract_id: int | None = None
    signal: dict | None = None  # [LOSSLOG] snapshot when the signal fired
    confirm_idx: int | None = None  # [LOSSLOG] tick idx when the buy was confirmed
    query_attempts: int = 0  # [TIMEOUT] status queries sent
    query_failures: int = 0  # [TIMEOUT] queries that errored / unanswered / unsettled
    query_outstanding: bool = False  # [TIMEOUT] a query is awaiting its answer
    original_signal: str | None = None  # [INVERT] signal returned by infer_market_bias
    inverted: bool = False  # [INVERT] fixed at creation, never changed afterwards


# --------------------------------------------------------------------------- #
# Bot
# --------------------------------------------------------------------------- #
class Bot:
    def __init__(self):
        self.ws = None
        self.reconnects = 0
        self.symbols = {s: SymbolState() for s in SYMBOLS}

        # look-ahead calibration
        self.lookahead = LOOKAHEAD
        self.lag_samples = deque(maxlen=5)

        # order tracking
        self.busy = False  # True from "send" until resolved / failed
        self.order = None
        self.resolved = set()
        self.assumed_losses = set()  # [TIMEOUT] contracts counted as assumed losses
        self.proposals = {}  # (symbol, contract_type) -> (proposal_id, stake)

        # money management
        self.stake = BASE_STAKE
        self.mg_step = 0

        # statistics
        self.wins = self.losses = self.unresolved = 0
        self.net_pl = self.max_pl = self.min_pl = 0.0
        self.win_streak = self.loss_streak = 0
        self.longest_win = self.longest_loss = 0
        self.target_hits = 0
        self.basket = 0.0
        self.min_bankroll = BASE_STAKE  # [RECOVERY] peak (deficit + next stake)

        # [INVERT] independent inversion statistics
        self.trade_meta = {}  # contract_id -> {"original", "executed", "inverted"}
        self.real_wins = self.real_losses = 0  # actual execution outcomes
        self.virtual_wins = self.virtual_losses = 0  # LIFETIME original-signal outcomes
        # [ROLLING] ordered virtual outcomes (True = virtual WIN) and the ACTIVE
        # counters derived from them; the active counters drive inversion
        self.virtual_history = deque(
            maxlen=(
                VIRTUAL_WIN_RATE_WINDOW if VIRTUAL_WIN_RATE_MODE == "WINDOW" else None
            )
        )
        self.active_virtual_wins = self.active_virtual_losses = 0
        self.inv_wins = self.inv_losses = 0  # actual outcomes, inverted trades
        self.noninv_wins = self.noninv_losses = 0  # actual outcomes, non-inverted

        # [THRESHOLD] baseline (never modified) and dynamically adjusted threshold
        self.baseline_threshold = _round_pct(INVERSION_THRESHOLD)
        self.active_threshold = self.baseline_threshold
        # LIFO stack of outstanding loss entries; each entry:
        #   {"inverted": bool, "weight": float, "position": n, "seq": resolution no.}
        self.threshold_stack = []
        self.threshold_loss_streak = 0  # consecutive REAL losses (feeds the brake)
        self.threshold_resolutions = (
            0  # resolved trades seen by the detector (entry age)
        )
        self.last_threshold_change = None  # description of the latest movement

        # [BRAKE] last resolved trade's inversion mode and SKIP-mode cooldown
        self.last_trade_inverted = None
        self.brake_skips_left = 0

    # ----------------------------------------------------------------- utils
    def trigger_count(self):
        return max(1, STREAK - self.lookahead)

    def release(self):
        self.busy = False
        self.order = None

    async def send(self, payload):
        await self.ws.send(dumps(payload))

    @staticmethod
    def _bucket_text(label, bucket):
        total = sum(bucket.values())
        if not total:
            return f"{label}: n/a (0)"
        c, r, n = bucket["continuation"], bucket["reversal"], bucket["neutral"]
        return (
            f"{label}: Cont {c / total * 100:.1f}% ({c}) | "
            f"Rev {r / total * 100:.1f}% ({r}) | "
            f"Neu {n / total * 100:.1f}% ({n}) | N={total}"
        )

    @classmethod
    def stats_line(cls, symbol, stats):
        grand = sum(sum(b.values()) for b in stats.values())
        if not grand:
            return None
        parts = [cls._bucket_text(d, stats[d]) for d in DIRECTIONS]
        return f"SESSION BIAS | {symbol} | " + "  ||  ".join(parts)

    # [INVERT] ----------------------------------------------------------
    def inversion_warming_up(self):
        """[WARMUP] True while there are too few virtual outcomes to trust the rate."""
        total = self.active_virtual_wins + self.active_virtual_losses
        return total < INVERSION_MIN_SAMPLES

    def inversion_active(self):
        """
        Inversion decision, based EXCLUSIVELY on the virtual win rate:
        ON when virtual win rate <= ACTIVE threshold (inclusive), OFF above.
        With no virtual trades yet there is no rate: inversion stays OFF.
        Integer-weighted cross-multiplication avoids float rounding at the
        threshold boundary.
        """
        # [ROLLING] uses the ACTIVE counters (window or all), never the lifetime ones
        total = self.active_virtual_wins + self.active_virtual_losses
        if not total or self.inversion_warming_up():  # [WARMUP]
            return False
        # [THRESHOLD] the dynamically adjusted threshold, not the baseline
        return self.active_virtual_wins * 100 <= self.active_threshold * total

    # [SIGNAL] ----------------------------------------------------------
    @staticmethod
    def bias_z(bucket):
        """z-score of |continuation - reversal| against a 50/50 coin flip."""
        c, r = bucket["continuation"], bucket["reversal"]
        n = c + r
        return abs(c - r) / math.sqrt(n) if n else 0.0

    # [BRAKE] -----------------------------------------------------------
    def brake_active(self):
        return (
            LOSS_STREAK_BRAKE_ENABLED
            and self.threshold_loss_streak >= LOSS_STREAK_BRAKE_AFTER
        )

    def virtual_rate_pct(self):
        """Active virtual win rate in percent, or None with no samples."""
        total = self.active_virtual_wins + self.active_virtual_losses
        if not total:
            return None
        return self.active_virtual_wins * 100 / total

    def brake_holds(self):
        """
        True when the HOLD brake overrides the threshold rule: brake engaged,
        HOLD action, a previous mode exists, and the virtual win rate lies
        inside the ambiguous zone around the active threshold.
        """
        if (
            not self.brake_active()
            or LOSS_STREAK_BRAKE_ACTION != "HOLD"
            or self.last_trade_inverted is None
        ):
            return False
        rate = self.virtual_rate_pct()
        if rate is None:
            return False
        return (
            round(abs(rate - self.active_threshold), 6) <= LOSS_STREAK_BRAKE_HOLD_MARGIN
        )

    def decide_inversion(self):
        """
        Inversion state for the next order.  The threshold rule decides, except
        while the HOLD brake is engaged AND the evidence is ambiguous: then the
        last trade's mode is kept.
        """
        if self.inversion_warming_up():  # [WARMUP]
            return False
        if self.brake_holds():
            return self.last_trade_inverted
        return self.inversion_active()

    def brake_should_skip(self):
        """SKIP mode: True (and counts one skipped signal) while cooling down."""
        if (
            self.brake_active()
            and LOSS_STREAK_BRAKE_ACTION == "SKIP"
            and self.brake_skips_left > 0
        ):
            self.brake_skips_left -= 1
            return True
        return False

    def brake_text(self):
        if not LOSS_STREAK_BRAKE_ENABLED:
            return "DISABLED"
        if not self.brake_active():
            return f"inactive (engages at {LOSS_STREAK_BRAKE_AFTER} consecutive losses)"
        if LOSS_STREAK_BRAKE_ACTION == "HOLD":
            rate = self.virtual_rate_pct()
            if self.brake_holds():
                mode = "INVERTED" if self.last_trade_inverted else "NORMAL"
                return (
                    f"ACTIVE (HOLD: virtual {rate:.1f}% is within "
                    f"{LOSS_STREAK_BRAKE_HOLD_MARGIN:g} pts of threshold, "
                    f"next trade keeps {mode} mode)"
                )
            if rate is None:
                return "ACTIVE (HOLD: no virtual samples, threshold rule decides)"
            return (
                f"ACTIVE (HOLD standing down: virtual {rate:.1f}% is "
                f"{abs(rate - self.active_threshold):.1f} pts from threshold, "
                f"> {LOSS_STREAK_BRAKE_HOLD_MARGIN:g}; threshold rule decides)"
            )
        return f"ACTIVE (SKIP: {self.brake_skips_left} signal(s) left to skip)"

    def record_virtual_outcome(self, virtual_win):
        """
        [ROLLING] Record ONE virtual outcome: lifetime counters always grow; the
        ordered history / active counters keep only the latest N in WINDOW mode.
        """
        if virtual_win:
            self.virtual_wins += 1
        else:
            self.virtual_losses += 1

        history = self.virtual_history
        if history.maxlen is not None and len(history) == history.maxlen:
            oldest = history.popleft()  # leaves the window immediately
            if oldest:
                self.active_virtual_wins -= 1
            else:
                self.active_virtual_losses -= 1

        history.append(bool(virtual_win))
        if virtual_win:
            self.active_virtual_wins += 1
        else:
            self.active_virtual_losses += 1

    # [THRESHOLD] -------------------------------------------------------
    def threshold_vwr_fraction(self):
        """
        Active virtual win rate as a fraction clamped to
        [THRESHOLD_P_FLOOR, THRESHOLD_P_CEIL], or None with no virtual samples.
        """
        total = self.active_virtual_wins + self.active_virtual_losses
        if not total:
            return None
        p = self.active_virtual_wins / total
        return min(THRESHOLD_P_CEIL, max(THRESHOLD_P_FLOOR, p))

    def threshold_loss_weight(self, inverted):
        """
        Weight of ONE real loss: how surprising the loss is under the current
        virtual win rate, scaled so that probability ALPHA gives weight 1 and
        capped at THRESHOLD_MAX_STEP_WEIGHT.
        """
        p = self.threshold_vwr_fraction()
        if p is None:
            return 0.0
        s_trig = math.log(1.0 / THRESHOLD_ALPHA)
        s = -math.log(p) if inverted else -math.log(1.0 - p)
        return min(THRESHOLD_MAX_STEP_WEIGHT, s / s_trig)

    def compute_active_threshold(self):
        """
        Active threshold from the stack and the current virtual win rate.
        Empty stack (or no virtual samples) -> exactly the baseline.
        """
        if not self.threshold_stack:
            return self.baseline_threshold
        p = self.threshold_vwr_fraction()
        if p is None:
            return self.baseline_threshold

        up = sum(e["weight"] for e in self.threshold_stack if not e["inverted"])
        down = sum(e["weight"] for e in self.threshold_stack if e["inverted"])
        net = max(-1.0, min(1.0, up - down))

        baseline = self.baseline_threshold
        vwr_pct = p * 100.0
        t_up = max(baseline, vwr_pct + THRESHOLD_MARGIN)
        t_dn = min(baseline, vwr_pct - THRESHOLD_MARGIN)
        if net >= 0:
            thr = baseline + (t_up - baseline) * net
        else:
            thr = baseline - (baseline - t_dn) * abs(net)
        return _round_pct(min(THRESHOLD_MAX, max(THRESHOLD_MIN, thr)))

    def update_threshold(self, inverted, won, tie=False):
        """
        Deterioration detector.  Called exactly once per resolved real trade
        (from record_inversion_result, after record_virtual_outcome has updated
        the active virtual win rate).  `inverted` is the trade's OWN stored
        execution flag, never the bot's current inversion state.

        LOSS: streak += 1 (still feeds the brake), push a weighted entry.
        WIN : streak resets to 0 and the most recent stack entry whose
              "inverted" flag equals the resolved trade's flag is popped.
              Without one, nothing is popped.
        After either, the active threshold is recomputed from the stack and
        the current virtual win rate.
        """
        inverted = bool(inverted)

        # drop stale evidence first (entries older than THRESHOLD_ENTRY_TTL trades)
        self.threshold_resolutions += 1
        self.threshold_stack = [
            e
            for e in self.threshold_stack
            if self.threshold_resolutions - e["seq"] < THRESHOLD_ENTRY_TTL
        ]
        before = self.active_threshold

        if won:
            self.threshold_loss_streak = 0  # next loss starts a new streak
            self.brake_skips_left = 0  # [BRAKE] a real win ends the brake

            # most recent outstanding entry of THIS trade's type
            index = None
            for i in range(len(self.threshold_stack) - 1, -1, -1):
                if self.threshold_stack[i]["inverted"] == inverted:
                    index = i
                    break

            if index is None:
                self.active_threshold = self.compute_active_threshold()
                self.last_threshold_change = {
                    "kind": "none",
                    "change": 0.0,
                    "intended": 0.0,
                    "position": 0,
                    "entry_inverted": inverted,
                }
                return

            entry = self.threshold_stack.pop(index)
            self.active_threshold = self.compute_active_threshold()
            change = _round_pct(self.active_threshold - before)
            self.last_threshold_change = {
                "kind": "reversal",
                "change": change,
                "intended": change,
                "position": entry["position"],
                "entry_inverted": entry["inverted"],
            }
            return

        # real loss
        self.threshold_loss_streak += 1
        position = self.threshold_loss_streak
        weight = self.threshold_loss_weight(inverted)

        # an inverted TIE is no evidence against inversion (the original signal
        # lost on it too), so it adds nothing to the stack
        if not (inverted and tie):
            self.threshold_stack.append(
                {
                    "inverted": inverted,
                    "weight": weight,
                    "position": position,
                    "seq": self.threshold_resolutions,
                }
            )
        self.active_threshold = self.compute_active_threshold()
        if self.brake_active():  # [BRAKE] (re)arm the SKIP-mode cooldown
            self.brake_skips_left = LOSS_STREAK_BRAKE_SKIP_SIGNALS
        change = _round_pct(self.active_threshold - before)
        self.last_threshold_change = {
            "kind": "loss",
            "change": change,
            "intended": change,
            "position": position,
            "entry_inverted": inverted,
        }

    def threshold_change_text(self):
        """One-line description of the latest threshold movement."""
        info = self.last_threshold_change
        if info is None:
            return "none yet"
        change, intended = info["change"], info["intended"]
        clamp = (
            ""
            if change == intended
            else f" (wanted {_signed_pct_text(intended)}, clamped)"
        )
        if info["kind"] == "loss":
            side = "inverted" if info["entry_inverted"] else "normal"
            word = (
                "increased"
                if change > 0
                else "decreased" if change < 0 else "unchanged"
            )
            return (
                f"{_signed_pct_text(change)}{clamp} "
                f"(loss #{info['position']}, {side} trade: threshold {word})"
            )
        if info["kind"] == "reversal":
            side = "inverted" if info["entry_inverted"] else "normal"
            article = "an" if info["entry_inverted"] else "a"
            word = (
                "increased"
                if change > 0
                else "decreased" if change < 0 else "unchanged"
            )
            return (
                f"{_signed_pct_text(change)}{clamp} "
                f"(win reversed loss #{info['position']} adjustment of {article} "
                f"{side} trade: threshold {word})"
            )
        side = "inverted" if info["entry_inverted"] else "normal"
        return f"0.0% (win: no outstanding {side} adjustment to reverse)"

    def record_inversion_result(self, meta, status, tie=False):
        """
        Update real / virtual / inverted / non-inverted counters for ONE
        resolved trade, and feed the [THRESHOLD] detector.  Callers guarantee
        a single call per contract (the `resolved` set guards the normal path,
        assume_loss marks the contract resolved first).
        """
        if status not in ("won", "lost") or meta is None:
            return
        won = status == "won"
        inverted = meta["inverted"]
        self.last_trade_inverted = bool(inverted)  # [BRAKE]

        # real: what actually happened
        if won:
            self.real_wins += 1
        else:
            self.real_losses += 1

        # virtual: what the ORIGINAL signal would have done.  A non-inverted
        # trade is the original signal; an inverted trade is its opposite, so
        # the outcome is reversed.
        # A TIE (entry == exit) loses for BOTH directions, so the original signal
        # would have lost too: an inverted tie is a virtual LOSS, not a win.
        virtual_win = (won != inverted) and not tie
        self.record_virtual_outcome(virtual_win)  # [ROLLING]

        # inverted / non-inverted: actual outcomes
        if inverted:
            if won:
                self.inv_wins += 1
            else:
                self.inv_losses += 1
        else:
            if won:
                self.noninv_wins += 1
            else:
                self.noninv_losses += 1

        # [THRESHOLD] real outcome + the trade's own execution flag
        self.update_threshold(inverted, won, tie)

    def inversion_report(self, meta, status, contract_id):
        """Report lines, built AFTER the counters include the current trade."""
        if status not in ("won", "lost"):
            return
        if meta is None:
            print(
                f"[INVERT] no inversion metadata for contract {contract_id}: "
                f"not counted in inversion statistics"
            )
            return
        outcome = "WIN" if status == "won" else "LOSS"
        inv_state = "ON" if self.decide_inversion() else "OFF"  # [BRAKE]
        rule_state = "ON" if self.inversion_active() else "OFF"
        if self.brake_holds():
            inv_reason = (
                f"held by brake; threshold rule says {rule_state} "
                f"at <= {_pct_text(self.active_threshold)}"
            )
        elif self.inversion_warming_up():  # [WARMUP]
            inv_reason = (
                f"warm-up: {len(self.virtual_history)}/{INVERSION_MIN_SAMPLES} "
                f"virtual samples needed"
            )
        else:
            inv_reason = f"threshold <= {_pct_text(self.active_threshold)}"
        print("   ---------------- ADAPTIVE INVERSION ----------------")
        print(f"   Original signal: {meta['original']}")
        print(f"   Executed signal: {meta['executed']}")
        print(f"   Inverted: {'YES' if meta['inverted'] else 'NO'}")
        print(f"   Actual outcome: {outcome}")
        print(f"   Real Win Rate: {_rate_text(self.real_wins, self.real_losses)}")
        samples = len(self.virtual_history)
        if VIRTUAL_WIN_RATE_MODE == "WINDOW":
            mode_text = f"Mode: WINDOW | Samples: {samples}/{VIRTUAL_WIN_RATE_WINDOW}"
        else:
            mode_text = f"Mode: ALL | Samples: {samples}"
        print(
            f"   Virtual Win Rate: "
            f"{_rate_text(self.active_virtual_wins, self.active_virtual_losses)}"
            f" | {mode_text}"
        )
        print(
            f"   Lifetime Virtual Win Rate: "
            f"{_rate_text(self.virtual_wins, self.virtual_losses)}"
        )
        print(f"   Inverted Trades: {self.inv_wins}W {self.inv_losses}L")
        print(f"   Non-Inverted Trades: {self.noninv_wins}W {self.noninv_losses}L")
        print(f"   Cumulative: {self.wins}W {self.losses}L")
        # [THRESHOLD] detector state, immediately after this resolution
        print(f"   Baseline Threshold: {_pct_text(self.baseline_threshold)}")
        print(f"   Active Threshold: {_pct_text(self.active_threshold)}")
        print(f"   Current Loss Streak: {self.threshold_loss_streak}")
        print(f"   Outstanding Adjustments: {len(self.threshold_stack)}")
        print(f"   Last Adjustment: {self.threshold_change_text()}")
        print(f"   Loss-Streak Brake: {self.brake_text()}")  # [BRAKE]
        print(f"   Inversion for next signal: {inv_state} " f"({inv_reason})")
        print("   ----------------------------------------------------")

    # [SEGMENT] ---------------------------------------------------------
    @staticmethod
    def segment_text(st):
        """e.g. 'UP seg 2 | DOWN seg 1'"""
        return " | ".join(f"{d} seg {st.segments[d]}" for d in DIRECTIONS)

    def roll_segment(self, st, symbol, direction, epoch, logs):
        """
        If this direction's decided samples reached SEGMENT_LIMIT, rebuild its
        stats from the latest SEGMENT_KEEP decided outcomes and start the next
        segment.
        """
        if not SEGMENT_ENABLED or SEGMENT_LIMIT <= 0:
            return

        bucket = st.stats[direction]
        decided = bucket["continuation"] + bucket["reversal"]
        if decided < SEGMENT_LIMIT:
            return

        keep_n = max(0, min(SEGMENT_KEEP, SEGMENT_LIMIT - 1))

        kept, kept_decided = [], 0
        for outcome in reversed(st.history[direction]):
            if kept_decided >= keep_n:
                break
            kept.append(outcome)
            if outcome != "neutral":
                kept_decided += 1
        kept.reverse()

        # rebuild the bucket from the carried-over outcomes only
        bucket["continuation"] = bucket["reversal"] = bucket["neutral"] = 0
        for outcome in kept:
            bucket[outcome] += 1
        st.history[direction] = kept

        old_seg = st.segments[direction]
        st.segments[direction] = old_seg + 1

        logs.append(
            f"\n>>> SEGMENT CHANGE | {timestamp(epoch)} | {symbol} | {direction} | "
            f"Segment {old_seg} -> {st.segments[direction]} | "
            f"N reached {decided} -> kept latest {kept_decided} | "
            f"Next segment ends at N={SEGMENT_LIMIT} "
            f"(+{SEGMENT_LIMIT - kept_decided} samples)"
        )

    # [LOSSLOG] ---------------------------------------------------------
    def build_signal_info(
        self,
        st,
        direction,
        contract_type,
        mode,
        look,
        decided,
        original_signal=None,
        inverted=False,
    ):
        """Light snapshot of the signal tick. Called AFTER the order is sent."""
        try:
            bucket = st.stats[direction]
            last = st.tick_log[-1]
            return {
                "idx": last["idx"],
                "direction": direction,
                "real_streak": st.count,
                "assumed": look,
                "contract": contract_type,
                "original_signal": original_signal,  # [INVERT]
                "inverted": inverted,  # [INVERT]
                "stake": self.stake,
                "step": self.mg_step,
                "mode": mode,
                "bias": {
                    "dir": direction,
                    "cont": bucket["continuation"],
                    "rev": bucket["reversal"],
                    "N": decided,
                    "seg": st.segments[direction],
                },
            }
        except Exception:
            # # traceback.print_exc()
            return None

    @staticmethod
    def _tick_by_idx(st, idx):
        for t in st.tick_log:
            if t["idx"] == idx:
                return t
        return None

    def build_trade_record(self, order, poc, contract_id, kind):
        """Compact checkpoint report for one resolved trade (kind: loss / win)."""
        st = self.symbols.get(order.symbol) if order is not None else None
        sig = order.signal if order is not None else None

        base = {
            "logged_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            f"{kind}_no": self.losses if kind == "loss" else self.wins,
            # consecutive-result count INCLUDING this trade (apply_result has run)
            f"on_{kind}_streak": (
                self.loss_streak if kind == "loss" else self.win_streak
            ),
            "contract_id": contract_id,
            "symbol": order.symbol if order is not None else None,
        }
        if st is None or sig is None:
            base["note"] = "signal context unavailable (e.g. reconnect mid-trade)"
            base["outcome"] = self._outcome(poc)
            return base

        sig_idx = sig["idx"]
        look = sig["assumed"]
        sdir = sig["direction"]
        exp_idx = sig_idx + look

        entry_time = _to_int(poc.get("entry_tick_time"))
        exit_time = _to_int(poc.get("exit_tick_time"))
        entry_spot = _to_float(poc.get("entry_spot"))
        exit_spot = _to_float(poc.get("exit_spot"))

        # ---- locate the real entry tick (epoch match, else price match) ----
        entry_t = None
        for t in st.tick_log:
            if (
                t["idx"] > sig_idx
                and entry_time is not None
                and t["epoch"] == entry_time
            ):
                entry_t = t
                break
        if entry_t is None and entry_spot is not None:
            for t in st.tick_log:
                if t["idx"] > sig_idx and t["price"] == entry_spot:
                    entry_t = t
                    break

        # ---- locate the exit tick ----
        exit_t = None
        if entry_t is not None:
            for t in st.tick_log:
                if t["idx"] <= entry_t["idx"]:
                    continue
                if (exit_time is not None and t["epoch"] == exit_time) or (
                    exit_time is None
                    and exit_spot is not None
                    and t["price"] == exit_spot
                ):
                    exit_t = t
                    break

        sig_t = self._tick_by_idx(st, sig_idx)
        exp_t = self._tick_by_idx(st, exp_idx)
        conf_t = (
            self._tick_by_idx(st, order.confirm_idx)
            if order.confirm_idx is not None
            else None
        )

        # ---- direction checks ----
        exp_ok = None if exp_t is None else exp_t["dir"] == sdir
        entry_ok = None if entry_t is None else entry_t["dir"] == sdir
        virtual_held = None
        if entry_t is not None:
            between = [t for t in st.tick_log if sig_idx < t["idx"] <= entry_t["idx"]]
            virtual_held = bool(between) and all(t["dir"] == sdir for t in between)

        # ---- tape: streak start -> exit, tagged ----
        start_idx = sig_idx - sig["real_streak"] + 1
        end_idx = exit_t["idx"] if exit_t is not None else st.tick_idx
        tags = {}

        def tag(idx, label):
            if idx is not None:
                tags.setdefault(idx, []).append(label)

        tag(sig_idx, "SIGNAL")
        tag(exp_idx, "EXPECTED")
        tag(order.confirm_idx, "CONFIRMED")
        tag(entry_t["idx"] if entry_t else None, "ENTRY")
        tag(exit_t["idx"] if exit_t else None, "EXIT")
        tape = []
        for t in st.tick_log:
            if start_idx <= t["idx"] <= end_idx:
                line = _fmt_tick(t)
                if t["idx"] in tags:
                    line += "  <- " + "+".join(tags[t["idx"]])
                tape.append(line)

        record = dict(base)
        record["1_signal"] = {
            "tick": _fmt_tick(sig_t),
            "price": sig_t["price"] if sig_t else None,
            "streak": f"{sdir} real {sig['real_streak']} + assumed {look}",
            "order": f"{sig['contract']} stake {sig['stake']:.2f} step {sig['step']} ({sig['mode']})",
            "original_signal": sig.get("original_signal"),  # [INVERT]
            "executed_signal": sig["contract"],  # [INVERT]
            "inverted": "YES" if sig.get("inverted") else "NO",  # [INVERT]
            "bias": sig["bias"],
        }
        record["2_expected_tick"] = {
            "tick": _fmt_tick(exp_t),
            "price": exp_t["price"] if exp_t else None,
            "matches_streak_dir": exp_ok,
        }
        record["3_execution"] = {
            "confirmed_tick": _fmt_tick(conf_t),
            "confirmed_at_expected_tick": (
                None if order.confirm_idx is None else order.confirm_idx == exp_idx
            ),
            "entry_tick": _fmt_tick(entry_t),
            "entry_at_expected_tick": (
                None if entry_t is None else entry_t["idx"] == exp_idx
            ),
            "entry_ticks_after_expected": (
                None if entry_t is None else entry_t["idx"] - exp_idx
            ),
            "entry_ticks_after_signal": (
                None if entry_t is None else entry_t["idx"] - sig_idx
            ),
        }
        record["4_direction_check"] = {
            "expected_tick_matched_streak": exp_ok,
            "entry_tick_matched_streak": entry_ok,
            "virtual_streak_held_until_entry": virtual_held,
        }
        record["outcome"] = self._outcome(poc)
        record["tape"] = tape
        return record

    @staticmethod
    def _outcome(poc):
        entry = _to_float(poc.get("entry_spot"))
        exit_ = _to_float(poc.get("exit_spot"))
        return {
            "entry": entry,
            "exit": exit_,
            "exit_minus_entry": (
                None if entry is None or exit_ is None else round(exit_ - entry, 8)
            ),
            "profit": _to_float(poc.get("profit")),
        }

    @staticmethod
    def save_record(record, path):
        """Append one record to the JSON array file (atomic replace)."""
        data = []
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if not isinstance(data, list):
                    raise ValueError("loss log is not a JSON array")
            except Exception:
                backup = f"{path}.corrupt-{int(time.time())}"
                os.replace(path, backup)
                print(f"[LOSSLOG] unreadable log moved to {backup}")
                data = []

        data.append(record)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)

    def log_trade(self, order, poc, contract_id, status):
        """Never raises: logging must not disturb trading."""
        if status == "lost":
            kind, enabled, path, count = (
                "loss",
                LOSS_LOG_ENABLED,
                LOSS_LOG_FILE,
                self.losses,
            )
        elif status == "won":
            kind, enabled, path, count = "win", WIN_LOG_ENABLED, WIN_LOG_FILE, self.wins
        else:
            return
        if not enabled:
            return
        try:
            record = self.build_trade_record(order, poc, contract_id, kind)
            self.save_record(record, path)
            print(f"[LOSSLOG] {kind} #{count} saved -> {path}")
        except Exception as error:
            # traceback.print_exc()
            print(f"[LOSSLOG] failed to save {kind} record: {str(error)}")

    # ------------------------------------------------------------ tick logic
    def update_windows(self, st, symbol, price, epoch, logs):
        """Resolve bias windows. Pure computation; log lines are buffered."""
        if not st.windows:
            return

        keep = []
        for w in st.windows:
            if w.start_price is None:  # still waiting for the entry tick
                w.wait -= 1
                if w.wait <= 0:
                    w.start_price = price
                keep.append(w)
                continue

            w.ticks_seen += 1
            if w.ticks_seen < BIAS_WINDOW:
                keep.append(w)
                continue

            start = w.start_price
            if price == start:
                outcome = "neutral"
            elif (price > start) == (w.direction == "UP"):
                outcome = "continuation"
            else:
                outcome = "reversal"

            # per-direction bucket
            st.stats[w.direction][outcome] += 1
            st.history[w.direction].append(outcome)  # [SEGMENT]

            logs.append(
                f"\nBIAS RESULT | {timestamp(epoch)} | {symbol} | "
                f"Original: {w.direction} | Start: {start} | End: {price} | "
                f"Change: {price - start:+.5f} | Outcome: {outcome.upper()} | "
                f"Window: {BIAS_WINDOW} ticks"
            )
            logs.append(self.stats_line(symbol, st.stats))

            # [SEGMENT] slide the stats if this direction hit the limit
            self.roll_segment(st, symbol, w.direction, epoch, logs)

        st.windows = keep

    async def on_tick(self, tick):
        symbol = tick["symbol"]
        st = self.symbols.get(symbol)
        if st is None:
            return

        price = float(tick["quote"])
        epoch = tick["epoch"]

        st.tick_idx += 1
        st.recent.append((epoch, st.tick_idx))
        prev, st.prev = st.prev, price

        # [LOSSLOG] rolling tape (observational only); "cnt" is filled below
        if prev is None:
            tape_dir = "-"
        elif price > prev:
            tape_dir = "UP"
        elif price < prev:
            tape_dir = "DOWN"
        else:
            tape_dir = "FLAT"
        st.tick_log.append(
            {
                "idx": st.tick_idx,
                "epoch": epoch,
                "price": price,
                "dir": tape_dir,
                "cnt": 0,
            }
        )

        # [TIMEOUT] watchdog: free the circuit if a contract never resolved
        await self.check_unresolved_timeout(st, symbol)

        if prev is None:
            print(f"{timestamp(epoch)} | {symbol} | {price} | Waiting")
            return

        logs = []
        self.update_windows(st, symbol, price, epoch, logs)

        if price > prev:
            direction = "UP"
        elif price < prev:
            direction = "DOWN"
        else:  # flat tick breaks the streak
            st.direction, st.count, st.alerted = None, 0, False
            if logs:
                print("\n".join(logs))
            return

        if st.direction == direction:
            st.count += 1
        else:
            st.direction, st.count, st.alerted = direction, 1, False

        st.tick_log[-1]["cnt"] = st.count  # [LOSSLOG]

        note = ""
        if st.count >= self.trigger_count() and not st.alerted:
            st.alerted = True
            look = self.lookahead

            # Bias window mirrors the real trade: opens on the entry tick
            # (look ticks from now) and closes BIAS_WINDOW ticks later.
            # It is recorded in the bucket of this streak's direction.
            st.windows.append(Window(direction, price if look == 0 else None, look))

            contract_type = infer_market_bias(
                stats=st.stats,
                current_streak_direction=direction,
                min_samples=MIN_SAMPLES,
                min_edge=MIN_EDGE,
            )

            bucket = st.stats[direction]
            decided = bucket["continuation"] + bucket["reversal"]

            # [SIGNAL] a bias indistinguishable from a coin flip is not traded
            bias_z = self.bias_z(bucket)
            weak = (
                contract_type != "SKIP" and SIGNAL_MIN_Z > 0 and bias_z < SIGNAL_MIN_Z
            )

            # ---- hot path: send BEFORE any printing ----
            if contract_type == "SKIP":
                note = f" | [SKIP] {direction} bias not decisive (N={decided})"
            elif weak:
                note = (
                    f" | [SKIP] {direction} bias not significant "
                    f"(z={bias_z:.2f} < {SIGNAL_MIN_Z:g}, N={decided})"
                )
            elif self.busy:
                note = " | [SKIP] contract still open"
            elif self.brake_should_skip():  # [BRAKE] SKIP-mode cooldown
                note = (
                    f" | [SKIP] loss-streak brake (streak "
                    f"{self.threshold_loss_streak}, {self.brake_skips_left} more to skip)"
                )
            else:
                # [INVERT] decide with the virtual win rate available right now;
                # SKIP never reaches this branch, so it can never be inverted.
                original_signal = contract_type
                inverted = original_signal in INVERSE_SIGNAL and self.decide_inversion()
                if inverted:
                    contract_type = INVERSE_SIGNAL[original_signal]

                mode = await self.place_order(
                    symbol, contract_type, st.tick_idx, original_signal, inverted
                )

                # [LOSSLOG] attach the signal snapshot AFTER the send
                if LOSS_LOG_ENABLED and self.order is not None:
                    self.order.signal = self.build_signal_info(
                        st,
                        direction,
                        contract_type,
                        mode,
                        look,
                        decided,
                        original_signal,
                        inverted,
                    )

                note = (
                    f" | [ORDER {contract_type}] stake {self.stake:.2f} {CURRENCY} | "
                    f"step {self.mg_step} | {mode} | {direction}-bias N={decided} | "
                    f"seg {st.segments[direction]}"  # [SEGMENT]
                    f" | orig {original_signal} inv {'YES' if inverted else 'NO'}"  # [INVERT]
                )

            note = (
                f" | STREAK {STREAK} REACHED (real {st.count} + "
                f"assumed {look}){note} | Monitoring next {BIAS_WINDOW} ticks"
            )

        if PRINT_TICKS or note:
            logs.append(
                f"{timestamp(epoch)} [{self.reconnects}] | {symbol} | {price:.2f} | "
                f"{direction:<4} [{st.count}]{note}"
            )
        if logs:
            print("\n".join(logs))

    # --------------------------------------------------------------- orders
    async def place_order(
        self, symbol, contract_type, signal_idx, original_signal=None, inverted=False
    ):
        stake = self.stake
        prop = self.proposals.get((symbol, contract_type)) if USE_PROPOSALS else None

        if prop and prop[1] == stake:
            payload = {"buy": prop[0], "price": stake, "req_id": REQ_BUY}
            mode = "proposal"
        else:
            payload = {
                "buy": "1",
                "price": stake,
                "parameters": {
                    "amount": round(stake, 2),
                    "basis": "stake",
                    "contract_type": contract_type,
                    "currency": CURRENCY,
                    "duration": 1,
                    "duration_unit": "t",
                    "underlying_symbol": symbol,
                },
                "req_id": REQ_BUY,
            }
            mode = "direct"

        # Flag first, send second: no window for a duplicate order.
        self.busy = True
        self.order = Order(symbol, contract_type, signal_idx, time.monotonic())
        # [INVERT] stored explicitly at creation; never derived from the outcome
        self.order.original_signal = (
            original_signal if original_signal is not None else contract_type
        )
        self.order.inverted = bool(inverted)
        await self.send(payload)
        return mode

    async def refresh_proposals(self):
        """(Re)subscribe CALL/PUT proposals at the current stake."""
        self.proposals.clear()
        await self.send({"forget_all": "proposal"})
        for symbol in SYMBOLS:
            for ctype, req_id in REQ_PROPOSAL.items():
                await self.send(
                    {
                        "proposal": 1,
                        "amount": self.stake,
                        "basis": "stake",
                        "contract_type": ctype,
                        "currency": CURRENCY,
                        "duration": 1,
                        "duration_unit": "t",
                        "underlying_symbol": symbol,
                        "subscribe": 1,
                        "req_id": req_id,
                    }
                )

    def on_proposal(self, data):
        prop = data.get("proposal") or {}
        echo = data.get("echo_req") or {}
        try:
            stake = float(echo["amount"])
            key = (echo["underlying_symbol"], echo["contract_type"])
        except (KeyError, TypeError, ValueError):
            return
        if stake == self.stake and prop.get("id"):  # ignore stale stakes
            self.proposals[key] = (prop["id"], stake)

    async def subscribe_contract(self, contract_id):
        await self.send(
            {
                "proposal_open_contract": 1,
                "contract_id": int(contract_id),
                "subscribe": 1,
                "req_id": REQ_POC,
            }
        )

    async def on_buy(self, data):
        info = data.get("buy") or {}
        contract_id = info.get("contract_id")
        order = self.order

        if contract_id is None or order is None:
            print("[!] Buy response did not contain a contract ID.")
            self.release()
            return

        order.contract_id = int(contract_id)

        # [INVERT] keep the inversion record tied to the contract until it resolves
        self.trade_meta[order.contract_id] = {
            "original": order.original_signal,
            "executed": order.contract_type,
            "inverted": order.inverted,
        }

        rtt = (time.monotonic() - order.t_sent) * 1000
        balance = info.get("balance_after")
        balance_text = f"{float(balance):.2f} USD" if balance is not None else "n/a"

        # [LOSSLOG] the latest tick received when the confirmation arrived
        st = self.symbols.get(order.symbol)
        if st is not None:
            order.confirm_idx = st.tick_idx

        await self.subscribe_contract(contract_id)
        print(
            f"\n[+] ORDER CONFIRMED | Contract ID: {contract_id} | "
            f"Balance: {balance_text} | Buy RTT: {rtt:.0f} ms"
        )

    # ------------------------------------------------------------ resolution
    def record_lag(self, order, entry_time):
        """Ticks between the signal tick and the contract's entry tick."""
        st = self.symbols.get(order.symbol)
        if st is None or entry_time is None:
            return None

        for epoch, idx in st.recent:
            if epoch == entry_time:
                lag = idx - order.signal_idx
                self.lag_samples.append(lag)
                if AUTO_LOOKAHEAD and len(self.lag_samples) >= 3:
                    self.lookahead = max(
                        0, min(MAX_LOOKAHEAD, median_low(self.lag_samples))
                    )
                return lag
        return None

    # [TIMEOUT] ---------------------------------------------------------
    async def check_unresolved_timeout(self, st, symbol):
        """
        Called on every tick. Once a confirmed contract is overdue the circuit
        stays locked while its real status is queried (one query per tick);
        only QUERY_MAX_FAILURES failed queries lead to an assumed loss.
        """
        order = self.order
        if (
            not (STATUS_QUERY_ENABLED or ASSUME_LOSS_ON_TIMEOUT)
            or order is None
            or order.contract_id is None
            or order.confirm_idx is None
            or order.symbol != symbol
        ):
            return
        waited = st.tick_idx - order.confirm_idx
        if waited < UNRESOLVED_TIMEOUT_TICKS:
            return

        if STATUS_QUERY_ENABLED:
            # the previous query got no answer before this tick: it failed
            if order.query_outstanding:
                order.query_outstanding = False
                order.query_failures += 1
                print(
                    f"[QUERY] attempt {order.query_attempts}/{QUERY_MAX_FAILURES} "
                    f"for contract {order.contract_id}: no answer"
                )

            if order.query_failures < QUERY_MAX_FAILURES:
                order.query_attempts += 1
                order.query_outstanding = True
                print(
                    f"\n[QUERY] Contract {order.contract_id} unresolved {waited} ticks "
                    f"after confirmation | status query "
                    f"{order.query_attempts}/{QUERY_MAX_FAILURES} | circuit stays locked"
                )
                await self.query_contract(order.contract_id)
                return

        if ASSUME_LOSS_ON_TIMEOUT:
            await self.assume_loss(order, st, waited)

    async def query_contract(self, contract_id):
        """One-shot status request (no subscribe): bypasses a stalled stream."""
        await self.send(
            {
                "proposal_open_contract": 1,
                "contract_id": int(contract_id),
                "req_id": REQ_POC_QUERY,
            }
        )

    def on_query_unsettled(self, data, poc):
        """A status query was answered, but the contract is not settled yet."""
        if data.get("req_id") != REQ_POC_QUERY:
            return
        order = self.order
        if (
            order is None
            or not order.query_outstanding
            or poc.get("contract_id") != order.contract_id
        ):
            return
        order.query_outstanding = False
        order.query_failures += 1
        print(
            f"[QUERY] attempt {order.query_attempts}/{QUERY_MAX_FAILURES}: "
            f"contract {order.contract_id} not settled yet "
            f"(status: {poc.get('status')}, expired: {poc.get('is_expired')})"
        )

    async def assume_loss(self, order, st, waited):
        """Treat an unresolved contract as lost and reopen the circuit."""
        contract_id = order.contract_id
        self.resolved.add(contract_id)  # a late result must not count twice
        self.assumed_losses.add(contract_id)

        old_stake = self.stake
        profit = -old_stake
        info = self.apply_result("lost", profit)
        self.unresolved += 1
        self.update_min_bankroll()

        # [INVERT] an assumed loss is treated as a loss everywhere else, so it
        # is counted once here too (the late result is ignored by on_late_poc).
        # [THRESHOLD] the same single call also feeds the threshold detector.
        meta = self.trade_meta.pop(contract_id, None)
        self.record_inversion_result(meta, "lost")

        print(
            f"\n[!] ASSUMED LOSS | Contract {contract_id} still unresolved "
            f"{waited} ticks after confirmation | "
            f"{order.query_failures} failed status queries | not written to JSON"
        )
        if info:
            print(info)

        decided = self.wins + self.losses
        win_rate = self.wins / decided * 100 if decided else 0
        target_status = (
            "⏰"
            if self.net_pl < TARGET_PL and self.target_hits == 0
            else f"✅ [{self.target_hits}] [BSKT {self.basket:.2f} USD]"
        )
        emoji = "❗" * self.loss_streak + f" [CLS {self.loss_streak}]"

        print(f"==========================================={emoji}")
        print(
            f"   TRADE RESOLVED (ASSUMED LOSS) | ID: {contract_id}    "
            f"[URSVD {self.unresolved}]"
        )
        print(
            f"   Result: LOST (ASSUMED)  MXLS{self.longest_loss} MXWS{self.longest_win}"
        )
        print(f"   Current P&L: {profit:+.2f} USD MXPL {self.max_pl:.2f}")
        print(
            f"   Win rate: {win_rate:.0f}% {self.wins}W {self.losses}L     [MINPL {self.min_pl:.2f}]"
        )
        print(f"   Segment: {self.segment_text(st)}")
        print("==========================================")
        print(f"   Min bankroll: {self.min_bankroll:.2f} USD ")
        print(f"   Net P&L: {self.net_pl:+.2f} USD     [TGTPL {target_status}]")
        print("==========================================")
        self.inversion_report(meta, "lost", contract_id)  # [INVERT]
        print()

        self.release()

        if USE_PROPOSALS and self.stake != old_stake:
            await self.refresh_proposals()

    async def on_late_poc(self, data, poc, contract_id):
        """A contract already counted as an assumed loss reported back."""
        sub_id = (data.get("subscription") or {}).get("id")
        if sub_id:
            await self.send({"forget": sub_id})
        if poc.get("is_sold"):
            self.assumed_losses.discard(contract_id)
            print(
                f"[LATE RESULT] Contract {contract_id} finally resolved: "
                f"{str(poc.get('status')).upper()} {_to_float(poc.get('profit'))} USD "
                f"(ignored: already counted as an assumed loss)"
            )

    def update_min_bankroll(self):
        """[RECOVERY] Peak capital needed: amount lost so far + next stake."""
        need = max(0.0, -self.net_pl) + self.stake
        if need > self.min_bankroll:
            self.min_bankroll = need

    def apply_result(self, status, profit):
        """Update P&L and martingale state. Returns an info message."""
        self.net_pl += profit
        self.max_pl = max(self.max_pl, self.net_pl)
        self.min_pl = min(self.min_pl, self.net_pl)

        if status == "won":
            self.wins += 1
            self.win_streak += 1
            self.loss_streak = 0
            self.longest_win = max(self.longest_win, self.win_streak)
            traded_stake = self.stake  # stake of the trade that just won
            self.stake, self.mg_step = BASE_STAKE, 0

            # [RECOVERY] still below zero after a win -> size the next stake
            # so a single win recovers the deficit and ends above zero.
            if RECOVERY_ENABLED and self.net_pl < 0 and profit > 0 and traded_stake > 0:
                rate = profit / traded_stake  # realised payout rate
                deficit = -self.net_pl
                recover_part = deficit / rate
                stake = math.ceil(round((recover_part + BASE_STAKE) * 100, 6)) / 100
                capped = stake > MAX_STAKE
                self.stake = min(stake, MAX_STAKE)
                return (
                    f"[RECOVERY] Net P&L {self.net_pl:+.2f} USD still negative after win. "
                    f"Next stake: {self.stake:.2f} USD = {recover_part:.2f} "
                    f"(recover {deficit:.2f} @ {rate * 100:.0f}% payout) + "
                    f"{BASE_STAKE:.2f} (base)"
                    + (" [capped at MAX_STAKE]" if capped else "")
                    + "."
                )
            return f"Stake reset to {self.stake:.2f} USD."

        if status == "lost":
            self.losses += 1
            self.loss_streak += 1
            self.win_streak = 0
            self.longest_loss = max(self.longest_loss, self.loss_streak)

            next_stake = self.stake * MARTINGALE_MULTIPLIER
            if self.mg_step < MAX_MARTINGALE_STEPS and next_stake <= MAX_STAKE:
                self.stake = next_stake
                self.mg_step += 1
                return f"Next stake: {self.stake:.2f} USD (step {self.mg_step})."

            reason = (
                "Maximum recovery steps reached"
                if self.mg_step >= MAX_MARTINGALE_STEPS
                else "Stake cap would be exceeded"
            )
            self.stake, self.mg_step = BASE_STAKE, 0
            return f"[MARTINGALE] {reason}. Resetting to {BASE_STAKE:.2f} USD."

        self.unresolved += 1
        return ""

    async def on_poc(self, data):
        poc = data.get("proposal_open_contract") or {}

        late_id = poc.get("contract_id")  # [TIMEOUT]
        if late_id is not None and late_id in self.assumed_losses:
            await self.on_late_poc(data, poc, late_id)
            return

        if not poc.get("is_sold"):
            self.on_query_unsettled(data, poc)  # [TIMEOUT]
            return

        contract_id = poc.get("contract_id")
        if contract_id is None or contract_id in self.resolved:
            return
        self.resolved.add(contract_id)

        sub_id = (data.get("subscription") or {}).get("id")
        if sub_id:
            await self.send({"forget": sub_id})
        elif data.get("req_id") == REQ_POC_QUERY:  # [TIMEOUT]
            # one-shot reply carries no subscription id: drop the stalled stream
            await self.send({"forget_all": "proposal_open_contract"})
            print(f"[QUERY] Contract {contract_id} resolved via status query")

        status = poc.get("status")
        profit = float(poc.get("profit", 0.0))

        order = self.order
        lag = None
        if order is not None and order.contract_id == contract_id:
            lag = self.record_lag(order, poc.get("entry_tick_time"))

        # [SEGMENT] capture segment info for the report
        seg_symbol = order.symbol if order is not None else SYMBOLS[0]
        seg_state = self.symbols.get(seg_symbol)
        seg_text = self.segment_text(seg_state) if seg_state is not None else "n/a"

        old_stake = self.stake
        info = self.apply_result(status, profit)
        self.update_min_bankroll()  # [RECOVERY]

        # [INVERT] update real / virtual / inverted / non-inverted counters once.
        # [THRESHOLD] the same call feeds the threshold detector, using the
        # trade's own stored `inverted` flag.  The contract is already in
        # `resolved`, so a repeated event never gets here.
        meta = self.trade_meta.pop(contract_id, None)
        entry_spot, exit_spot = _to_float(poc.get("entry_spot")), _to_float(
            poc.get("exit_spot")
        )
        tie = entry_spot is not None and entry_spot == exit_spot
        self.record_inversion_result(meta, status, tie)

        if info:
            print(info)
        if tie and status == "lost":
            print(
                "[TIE] entry == exit: loses for BOTH directions "
                "(counted as a virtual loss, not as evidence against inversion)"
            )

        # [LOSSLOG] losses -> loss file, wins -> win file; the order must be
        # the one that just resolved
        if status in ("lost", "won"):
            trade_order = (
                order
                if order is not None and order.contract_id == contract_id
                else None
            )
            self.log_trade(trade_order, poc, contract_id, status)

        # ---- report ----
        decided = self.wins + self.losses
        win_rate = self.wins / decided * 100 if decided else 0
        min_bankroll = self.min_bankroll  # [RECOVERY] deficit + next stake, peak

        target_status = (
            "⏰"
            if self.net_pl < TARGET_PL and self.target_hits == 0
            else f"✅ [{self.target_hits}] [BSKT {self.basket:.2f} USD]"
        )
        emoji = "❗" * self.loss_streak if status == "lost" else "🏆" * self.win_streak
        emoji += (
            f" [C{'L' if self.loss_streak else 'W'}S "
            f"{self.loss_streak or self.win_streak}]"
        )
        lag_text = "n/a" if lag is None else f"+{lag} tick(s)"

        print(
            f"\nEntry {poc.get('entry_spot')} -> Exit {poc.get('exit_spot')} | "
            f"Buy {poc.get('buy_price')} | Entry lag: {lag_text} | "
            f"Look-ahead now: {self.lookahead}"
        )
        print(f"==========================================={emoji}")
        print(f"   TRADE RESOLVED | ID: {contract_id}    [URSVD {self.unresolved}]")
        print(
            f"   Result: {str(status).upper()}  MXLS{self.longest_loss} MXWS{self.longest_win}"
        )
        print(f"   Current P&L: {profit:+.2f} USD MXPL {self.max_pl:.2f}")
        print(
            f"   Win rate: {win_rate:.0f}% {self.wins}W {self.losses}L     [MINPL {self.min_pl:.2f}]"
        )
        print(f"   Segment: {seg_text}")  # [SEGMENT]
        print("==========================================")
        print(f"   Min bankroll: {min_bankroll:.2f} USD ")
        print(f"   Net P&L: {self.net_pl:+.2f} USD     [TGTPL {target_status}]")
        print("==========================================")
        self.inversion_report(meta, status, contract_id)  # [INVERT]
        print()

        if self.net_pl >= TARGET_PL:
            self.basket += self.net_pl
            self.target_hits += 1
            self.net_pl = 0.0
            print("Net PL has been reset.")

        self.release()

        if USE_PROPOSALS and self.stake != old_stake:
            await self.refresh_proposals()

    def on_error(self, data):
        err = data["error"]
        print(f"\n[!] API error response: {err.get('message', err)}")
        if (
            err.get("message", err)
            == "Your account balance is insufficient to buy this contract."
        ):
            print("Account blown 💀. Exiting gracefully...")
            sys.exit(0)
        if data.get("req_id") == REQ_POC_QUERY:  # [TIMEOUT]
            order = self.order
            if order is not None and order.query_outstanding:
                order.query_outstanding = False
                order.query_failures += 1
                print(
                    f"[QUERY] attempt {order.query_attempts}/{QUERY_MAX_FAILURES} "
                    f"for contract {order.contract_id}: API error"
                )
            return
        # A failed buy must not leave the bot blocked forever.
        if data.get("req_id") == REQ_BUY and (
            self.order is None or self.order.contract_id is None
        ):
            self.release()

    # ------------------------------------------------------------ connection
    async def heartbeat(self):
        """App-level ping every 20s for the Deriv balancer."""
        try:
            while True:
                await asyncio.sleep(20)
                await self.send({"ping": 1})
        except (asyncio.CancelledError, websockets.ConnectionClosed):
            pass

    async def on_connect(self):
        for st in self.symbols.values():
            st.reset_stream()
        self.proposals.clear()

        order = self.order
        if order is not None and order.contract_id is not None:
            # A contract was open when we dropped: re-attach to its result.
            await self.subscribe_contract(order.contract_id)
        else:
            self.release()  # unknown fate of an in-flight buy; unblock trading

        await self.send({"ticks": SYMBOLS, "subscribe": 1, "req_id": REQ_TICKS})
        if USE_PROPOSALS:
            await self.refresh_proposals()

    async def read_loop(self):
        async for raw in self.ws:
            data = loads(raw)
            msg_type = data.get("msg_type")

            if msg_type == "tick":  # hot path first
                await self.on_tick(data["tick"])
            elif "error" in data:
                self.on_error(data)
            elif msg_type == "buy":
                if data.get("req_id") == REQ_BUY:
                    await self.on_buy(data)
            elif msg_type == "proposal_open_contract":
                await self.on_poc(data)
            elif msg_type == "proposal":
                self.on_proposal(data)

    async def run(self):
        delay = 2
        while True:
            try:
                print("Fetching WebSocket URL...")
                url = await asyncio.to_thread(get_ws_url, account_type=ACCOUNT_TYPE)

                async with websockets.connect(
                    url, open_timeout=15, ping_interval=30, ping_timeout=10
                ) as ws:
                    self.ws = ws
                    print("Connected to Deriv")
                    print(
                        f"STREAK={STREAK} | BIAS_WINDOW={BIAS_WINDOW} | "
                        f"LOOKAHEAD={self.lookahead} (auto={AUTO_LOOKAHEAD}) | "
                        f"PROPOSALS={USE_PROPOSALS} | "
                        f"MIN_SAMPLES={MIN_SAMPLES} | MIN_EDGE={MIN_EDGE}"
                    )
                    print(  # [SEGMENT]
                        f"SEGMENTS={SEGMENT_ENABLED} | "
                        f"SEGMENT_LIMIT={SEGMENT_LIMIT} | SEGMENT_KEEP={SEGMENT_KEEP}"
                    )
                    print(  # [TIMEOUT]
                        f"UNRESOLVED_TIMEOUT_TICKS={UNRESOLVED_TIMEOUT_TICKS} | "
                        f"STATUS_QUERY={STATUS_QUERY_ENABLED} "
                        f"(max failures {QUERY_MAX_FAILURES}) | "
                        f"ASSUME_LOSS_ON_TIMEOUT={ASSUME_LOSS_ON_TIMEOUT}"
                    )
                    print(  # [LOSSLOG]
                        f"LOSS_LOG={LOSS_LOG_ENABLED} | LOSS_LOG_FILE={LOSS_LOG_FILE} | "
                        f"WIN_LOG={WIN_LOG_ENABLED} | WIN_LOG_FILE={WIN_LOG_FILE}"
                    )
                    print(  # [INVERT]
                        f"INVERSION_THRESHOLD={INVERSION_THRESHOLD:.0f}% (baseline) "
                        f"(invert PUT/CALL when virtual win rate <= active threshold)"
                    )
                    print(  # [THRESHOLD]
                        f"THRESHOLD_ALPHA={THRESHOLD_ALPHA} | "
                        f"THRESHOLD_MARGIN={THRESHOLD_MARGIN} | "
                        f"THRESHOLD_MAX_STEP_WEIGHT={THRESHOLD_MAX_STEP_WEIGHT} | "
                        f"THRESHOLD_P_FLOOR={THRESHOLD_P_FLOOR} | "
                        f"THRESHOLD_P_CEIL={THRESHOLD_P_CEIL} | "
                        f"THRESHOLD_ENTRY_TTL={THRESHOLD_ENTRY_TTL} | "
                        f"INVERSION_MIN_SAMPLES={INVERSION_MIN_SAMPLES} | "
                        f"THRESHOLD_MIN={THRESHOLD_MIN} | THRESHOLD_MAX={THRESHOLD_MAX} | "
                        f"ACTIVE_THRESHOLD={_pct_text(self.active_threshold)} "
                        f"(outstanding adjustments: {len(self.threshold_stack)})"
                    )
                    print(  # [SIGNAL]
                        f"SIGNAL_MIN_Z={SIGNAL_MIN_Z:g} "
                        f"(skip PUT/CALL when |cont-rev|/sqrt(N) is below this)"
                    )
                    print(  # [BRAKE]
                        f"LOSS_STREAK_BRAKE={LOSS_STREAK_BRAKE_ENABLED} | "
                        f"AFTER={LOSS_STREAK_BRAKE_AFTER} | "
                        f"ACTION={LOSS_STREAK_BRAKE_ACTION} | "
                        f"HOLD_MARGIN={LOSS_STREAK_BRAKE_HOLD_MARGIN:g} | "
                        f"SKIP_SIGNALS={LOSS_STREAK_BRAKE_SKIP_SIGNALS}"
                    )
                    print(  # [ROLLING]
                        f"VIRTUAL_WIN_RATE_MODE={VIRTUAL_WIN_RATE_MODE}"
                        + (
                            f" | VIRTUAL_WIN_RATE_WINDOW={VIRTUAL_WIN_RATE_WINDOW}"
                            if VIRTUAL_WIN_RATE_MODE == "WINDOW"
                            else ""
                        )
                    )
                    await self.on_connect()
                    delay = 2

                    pinger = asyncio.create_task(self.heartbeat())
                    try:
                        await self.read_loop()
                    finally:
                        pinger.cancel()

                print("Connection closed.")
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # # traceback.print_exc()
                print(f"Connection error occurred: {str(error)}")

            self.reconnects += 1
            print(f"Reconnecting in {delay} seconds...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


if __name__ == "__main__":
    try:
        asyncio.run(Bot().run())
    except KeyboardInterrupt:
        print("Stopped")
