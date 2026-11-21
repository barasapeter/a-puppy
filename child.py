"""
Deriv CHILD bot. The parent bot is the reference and signal generator; the
child receives every executed order over the parent's live feed and decides
for itself what to do with it.

* FRESH SESSION: every start is a clean slate. P&L, stake/martingale, streaks,
  inversion buckets and thresholds live only inside this process; nothing is
  shared with the parent or with other children, and nothing is saved to disk.
  Run as many children as you like (each in its own terminal / process).
* SIGNAL: copy the parent's signal, invert it, run your own rule, or use the
  child's OWN adaptive threshold inversion (same maths as the parent, fed only
  by the child's own resolved trades).
* STAKE: the child's own martingale + recovery (same maths as the parent, on
  the child's own results), or mirror / fixed / multiplier.
* REPORTING: every resolved child trade is printed in exactly the parent's
  report format (Reporter class).

Run the parent first (it hosts the feed), then:   python child_bot.py [NAME]
"""

import asyncio
import json
import math
import os
import sys
import time
import uuid
from collections import deque

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import websockets

from auth import get_ws_url

try:  # optional speed-up: pip install orjson
    import orjson

    loads = orjson.loads

    def dumps(obj):
        return orjson.dumps(obj).decode()

except ImportError:
    loads = json.loads
    dumps = json.dumps

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
# Name shown in the parent's log: `python child_bot.py alpha` or CHILD_NAME=alpha
NAME = (
    sys.argv[1]
    if len(sys.argv) > 1
    else os.environ.get("CHILD_NAME") or f"child-{uuid.uuid4().hex[:4]}"
)

PARENT_URL = "ws://127.0.0.1:8765"  # must match PUBLISH_HOST / PUBLISH_PORT
PARENT_TOKEN = "change-me"  # must match PUBLISH_TOKEN ("" if the parent has none)

# Which Deriv account the child trades on. By default this calls the same
# auth.get_ws_url as the parent with this account_type. If your second account
# needs a different token, set the full URL in the DERIV_CHILD_WS_URL
# environment variable instead (it overrides get_ws_url).
CHILD_ACCOUNT_TYPE = os.environ.get("CHILD_ACCOUNT_TYPE", "demo")
CHILD_WS_URL = os.environ.get("DERIV_CHILD_WS_URL")
CURRENCY = "USD"
CHILD_SYMBOLS = ["1HZ100V"]  # ticks watched only to measure entry lag

# What to execute (override per child: CHILD_SIGNAL_MODE=invert python child_bot.py):
#   "copy"     = the parent's final signal
#   "invert"   = the opposite of the parent's final signal
#   "custom"   = whatever custom_decision() below returns
#   "adaptive" = the child's OWN threshold inversion: the parent's final signal
#                is the "original" signal; per streak direction (UP / DOWN) the
#                child decides whether to invert it, learning only from the
#                CHILD's own resolved trades (see [INVERSION] below).
SIGNAL_MODE = os.environ.get("CHILD_SIGNAL_MODE", "adaptive")

# How much to stake (override: CHILD_STAKE_MODE=fixed python child_bot.py):
#   "martingale" = the child's own martingale + recovery on ITS OWN results
#   "mirror"     = same stake as the parent
#   "fixed"      = FIXED_STAKE on every trade
#   "multiplier" = parent stake x STAKE_MULTIPLIER
STAKE_MODE = os.environ.get("CHILD_STAKE_MODE", "martingale")
FIXED_STAKE = 1.0
STAKE_MULTIPLIER = 1.0
MIN_STAKE = 1.0
MAX_STAKE = 1500.0

# [MARTINGALE] used when STAKE_MODE == "martingale" (same meaning as the parent)
BASE_STAKE = 1
MARTINGALE_MULTIPLIER = 2
MAX_MARTINGALE_STEPS = 10
RECOVERY_ENABLED = True

TARGET_PL = 666615.0  # child stops once ITS net P&L reaches this
MAX_SESSION_LOSS = None  # e.g. 300.0: stop the child once its net P&L <= -300
MAX_EVENT_AGE_S = 5.0  # ignore order events older than this (None = off);
#                       needs synced clocks if the parent is on another machine
BUSY_TIMEOUT_S = 30.0  # unresolved contract: query its status, then give up
PRINT_PARENT_RESULTS = False  # one extra line per parent result (off = minimal)

# [INVERSION] used by SIGNAL_MODE == "adaptive". Same meaning and defaults as
# the parent's settings; tune them independently for the child.
INVERSION_THRESHOLD = 46.0  # baseline, per streak direction
INVERSION_HYSTERESIS_GAP = 3  # percentage points
THRESHOLD_LOSS_FACTORS = (3.0, 3.5, 4.5)
THRESHOLD_FACTOR_INCREMENT = 2.0
THRESHOLD_MIN = 20.0
THRESHOLD_MAX = 90.0
THRESHOLD_PRECISION = 6

REQ_TICKS, REQ_BUY, REQ_POC, REQ_POC_QUERY = 6001, 6005, 6007, 6008
INVERSE_SIGNAL = {"PUT": "CALL", "CALL": "PUT"}
DIRECTIONS = ("UP", "DOWN")

if SIGNAL_MODE not in ("copy", "invert", "custom", "adaptive"):
    raise ValueError('SIGNAL_MODE must be "copy", "invert", "custom" or "adaptive"')
if STAKE_MODE not in ("martingale", "mirror", "fixed", "multiplier"):
    raise ValueError(
        'STAKE_MODE must be "martingale", "mirror", "fixed" or "multiplier"'
    )
if not (0.0 <= THRESHOLD_MIN <= INVERSION_THRESHOLD <= THRESHOLD_MAX <= 100.0):
    raise ValueError("INVERSION_THRESHOLD must lie inside the threshold bounds")


def custom_decision(event, state):
    """Used when SIGNAL_MODE == "custom". Return "CALL", "PUT", or None to skip.

    event: the parent's order event, e.g.
        event["contract"]   final signal the parent executed ("CALL"/"PUT")
        event["original"]   signal before inversion / guard
        event["inverted"]   True if the parent executed the opposite of original
        event["direction"]  streak direction ("UP"/"DOWN")
        event["stake"], event["step"], event["guard"], event["bias_n"]
    state: the parent's metrics at that moment, e.g.
        state["loss_streak"], state["win_streak"], state["net_pl"],
        state["buckets"]["UP"]["virtual_rate"], ["active_threshold"],
        ["inversion_mode"], ... (same for "DOWN")

    Example: invert only after the parent has lost 3 in a row:
        if state["loss_streak"] >= 3:
            return INVERSE_SIGNAL[event["contract"]]
    """
    return event["contract"]


# --------------------------------------------------------------------------- #
# Formatting helpers (identical to the parent's)
# --------------------------------------------------------------------------- #
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


def _rate_text(wins, losses):
    total = wins + losses
    if not total:
        return f"n/a ({wins}W {losses}L)"
    return f"{wins / total * 100:.1f}% ({wins}W {losses}L)"


def _round_pct(value):
    return round(float(value), THRESHOLD_PRECISION)


def _pct_text(value):
    text = f"{value:.4f}".rstrip("0")
    if text.endswith("."):
        text += "0"
    return text + "%"


def _signed_pct_text(value):
    if value == 0:
        return "0.0%"
    return ("+" if value > 0 else "-") + _pct_text(abs(value))


# --------------------------------------------------------------------------- #
# Adaptive inversion (the parent's maths, fed by the child's own trades)
# --------------------------------------------------------------------------- #
class Bucket:
    """Inversion state of ONE streak direction (same fields as the parent's)."""

    def __init__(self):
        self.real_wins = self.real_losses = 0
        self.virtual_wins = self.virtual_losses = 0
        self.inv_wins = self.inv_losses = 0
        self.noninv_wins = self.noninv_losses = 0
        self.baseline_threshold = _round_pct(INVERSION_THRESHOLD)
        self.active_threshold = self.baseline_threshold
        self.inversion_mode = False
        self.threshold_ledger = []  # append-only threshold events
        self.threshold_event_id = 0
        self.last_threshold_change = None


class AdaptiveInversion:
    """The parent's threshold inversion for the child.

    "Original" signal = the parent's final signal. The virtual outcome of a
    child trade is what that original signal would have done: a non-inverted
    trade is the original, an inverted trade is its opposite.
    """

    def __init__(self):
        self.b = {d: Bucket() for d in DIRECTIONS}

    def rate(self, direction):
        b = self.b[direction]
        total = b.virtual_wins + b.virtual_losses
        return None if total == 0 else b.virtual_wins * 100.0 / total

    def decide(self, direction):
        """Hysteresis on the direction's full-session virtual win rate."""
        b = self.b[direction]
        rate = self.rate(direction)
        if rate is None:
            b.inversion_mode = False
        elif rate <= b.active_threshold - INVERSION_HYSTERESIS_GAP:
            b.inversion_mode = True
        elif rate >= b.active_threshold + INVERSION_HYSTERESIS_GAP:
            b.inversion_mode = False
        return b.inversion_mode  # inside the band: keep the previous state

    def summary(self, direction):
        b = self.b[direction]
        rate = self.rate(direction)
        rate_text = "n/a" if rate is None else f"{rate:.1f}%"
        return (
            f"{direction} virt {rate_text} ({b.virtual_wins}W {b.virtual_losses}L) "
            f"thr {_pct_text(b.active_threshold)} "
            f"inv {'ON' if b.inversion_mode else 'OFF'}"
        )

    def reversed_loss_event_ids(self, direction):
        return {
            e["reverses_event_id"]
            for e in self.b[direction].threshold_ledger
            if e["kind"] == "reversal"
        }

    def threshold_unreversed_losses(self, direction, inverted):
        reversed_ids = self.reversed_loss_event_ids(direction)
        return sum(
            1
            for e in self.b[direction].threshold_ledger
            if e["kind"] == "loss"
            and e["inverted"] is bool(inverted)
            and e["event_id"] not in reversed_ids
        )

    @staticmethod
    def threshold_factor(position):
        if position <= len(THRESHOLD_LOSS_FACTORS):
            return float(THRESHOLD_LOSS_FACTORS[position - 1])
        return float(
            THRESHOLD_LOSS_FACTORS[-1]
            + THRESHOLD_FACTOR_INCREMENT * (position - len(THRESHOLD_LOSS_FACTORS))
        )

    def recompute_active_threshold(self, direction):
        b = self.b[direction]
        net = sum(float(e["delta"]) for e in b.threshold_ledger)
        raw = b.baseline_threshold + net
        b.active_threshold = _round_pct(min(THRESHOLD_MAX, max(THRESHOLD_MIN, raw)))
        return b.active_threshold

    def update_threshold(self, direction, inverted, won):
        """Append one real outcome and its exact threshold effect to the
        ledger of the trade's own streak direction (never edited)."""
        b = self.b[direction]
        inverted = bool(inverted)
        before = b.active_threshold
        b.threshold_event_id += 1
        reversed_ids = self.reversed_loss_event_ids(direction)

        if won:
            match = None
            for e in reversed(b.threshold_ledger):  # newest unreversed loss
                if (
                    e["kind"] == "loss"
                    and e["inverted"] is inverted
                    and e["event_id"] not in reversed_ids
                ):
                    match = e
                    break

            if match is None:
                b.threshold_ledger.append(
                    {
                        "event_id": b.threshold_event_id,
                        "kind": "win_no_match",
                        "inverted": inverted,
                        "delta": 0.0,
                        "reverses_event_id": None,
                    }
                )
                b.last_threshold_change = {
                    "kind": "win_no_match",
                    "change": 0.0,
                    "intended": 0.0,
                    "entry_inverted": inverted,
                    "position": 0,
                }
                return

            delta = -float(match["delta"])
            b.threshold_ledger.append(
                {
                    "event_id": b.threshold_event_id,
                    "kind": "reversal",
                    "inverted": inverted,
                    "delta": delta,
                    "factor": float(match["factor"]),
                    "reverses_event_id": match["event_id"],
                    "position": match["position"],
                }
            )
            self.recompute_active_threshold(direction)
            actual = _round_pct(b.active_threshold - before)
            b.last_threshold_change = {
                "kind": "reversal",
                "change": actual,
                "intended": delta,
                "entry_inverted": inverted,
                "position": match["position"],
                "factor": float(match["factor"]),
            }
            return

        position = self.threshold_unreversed_losses(direction, inverted) + 1
        factor = self.threshold_factor(position)
        delta = -factor if inverted else factor
        b.threshold_ledger.append(
            {
                "event_id": b.threshold_event_id,
                "kind": "loss",
                "inverted": inverted,
                "delta": delta,
                "factor": factor,
                "position": position,
            }
        )
        self.recompute_active_threshold(direction)
        actual = _round_pct(b.active_threshold - before)
        b.last_threshold_change = {
            "kind": "loss",
            "change": actual,
            "intended": delta,
            "entry_inverted": inverted,
            "position": position,
            "factor": factor,
        }

    def threshold_change_text(self, direction):
        info = self.b[direction].last_threshold_change
        if info is None:
            return "none yet"
        change = info["change"]
        intended = info["intended"]
        clamp = (
            ""
            if change == intended
            else f" (ledger delta {_signed_pct_text(intended)}; active threshold bounded)"
        )
        side = "inverted" if info["entry_inverted"] else "normal"
        if info["kind"] == "loss":
            return f"{_signed_pct_text(change)}{clamp} (loss #{info['position']}, {side}: factor {info['factor']:g})"
        if info["kind"] == "reversal":
            return f"{_signed_pct_text(change)}{clamp} (win reversed loss #{info['position']} {side} factor {info['factor']:g})"
        return f"0.0% (win: no unreversed {side} loss adjustment exists)"

    def record_result(self, meta, status):
        """Count ONE resolved child trade in its own direction's bucket."""
        if status not in ("won", "lost") or meta is None:
            return
        direction = meta.get("direction")
        if direction not in self.b:
            print(
                f"[INVERT] contract has no valid direction ({direction!r}): "
                f"inversion buckets not updated"
            )
            return
        b = self.b[direction]
        won = status == "won"
        inverted = meta["inverted"]

        if won:
            b.real_wins += 1
        else:
            b.real_losses += 1

        # virtual: what the ORIGINAL signal would have done
        if won != inverted:
            b.virtual_wins += 1
        else:
            b.virtual_losses += 1

        if inverted:
            if won:
                b.inv_wins += 1
            else:
                b.inv_losses += 1
        else:
            if won:
                b.noninv_wins += 1
            else:
                b.noninv_losses += 1

        self.update_threshold(direction, inverted, won)


# --------------------------------------------------------------------------- #
# Reporting interface: prints a resolved child trade in the parent's format
# --------------------------------------------------------------------------- #
class Reporter:
    def __init__(self, child):
        self.c = child

    def resolved(self, poc, contract_id, status, profit, meta, lag, seg_text, lookahead):
        c = self.c
        decided = c.wins + c.losses
        win_rate = c.wins / decided * 100 if decided else 0
        min_bankroll = c.min_bankroll

        target_status = (
            "⏰"
            if c.net_pl < TARGET_PL and c.target_hits == 0
            else f"✅ [{c.target_hits}] [BSKT {c.basket:.2f} USD]"
        )
        emoji = "❗" * c.loss_streak if status == "lost" else "🏆" * c.win_streak
        emoji += (
            f" [C{'L' if c.loss_streak else 'W'}S "
            f"{c.loss_streak or c.win_streak}]"
        )
        lag_text = "n/a" if lag is None else f"+{lag} tick(s)"

        print(
            f"\nEntry {poc.get('entry_spot')} -> Exit {poc.get('exit_spot')} | "
            f"Buy {poc.get('buy_price')} | Entry lag: {lag_text} | "
            f"Look-ahead now: {lookahead}"
        )
        print(f"==========================================={emoji}")
        print(f"   TRADE RESOLVED | ID: {contract_id}    [URSVD {c.unresolved}]")
        print(
            f"   Result: {str(status).upper()}  MXLS{c.longest_loss} MXWS{c.longest_win}"
        )
        print(f"   Current P&L: {profit:+.2f} USD MXPL {c.max_pl:.2f}")
        print(
            f"   Win rate: {win_rate:.0f}% {c.wins}W {c.losses}L     [MINPL {c.min_pl:.2f}]"
        )
        print(f"   Segment: {seg_text}")
        if c.streak_line():
            print(f"   {c.streak_line()}")
        print("==========================================")
        print(f"   Min bankroll: {min_bankroll:.2f} USD ")
        print(f"   Net P&L: {c.net_pl:+.2f} USD     [TGTPL {target_status}]")
        print("==========================================")
        self.inversion(meta, status, contract_id)
        print()

    def inversion(self, meta, status, contract_id):
        c = self.c
        inv = c.inversion
        if status not in ("lost", "won"):
            return
        if meta is None:
            print(
                f"[INVERT] no inversion metadata for contract {contract_id}: not counted"
            )
            return
        direction = meta.get("direction")
        if direction not in inv.b:
            print(f"[INVERT] contract {contract_id}: unknown direction {direction!r}")
            return
        b = inv.b[direction]
        outcome = "WIN" if status == "won" else "LOSS"
        modes = {d: ("ON" if inv.decide(d) else "OFF") for d in DIRECTIONS}
        rate = inv.rate(direction)
        print("   ---------------- ADAPTIVE INVERSION ----------------")
        print(f"   Streak direction (bucket): {direction}")
        print(f"   Original signal: {meta['original']}")
        print(f"   Executed signal: {meta['executed']}")
        print(f"   Inverted: {'YES' if meta['inverted'] else 'NO'}")
        print(f"   Actual outcome: {outcome}")
        print(f"   [{direction}] Real Win Rate: {_rate_text(b.real_wins, b.real_losses)}")
        print(
            f"   [{direction}] Virtual Win Rate (ALL): "
            f"{_rate_text(b.virtual_wins, b.virtual_losses)}"
        )
        print(f"   [{direction}] Inverted Trades: {b.inv_wins}W {b.inv_losses}L")
        print(f"   [{direction}] Non-Inverted Trades: {b.noninv_wins}W {b.noninv_losses}L")
        print(f"   Cumulative (all directions): {c.wins}W {c.losses}L")
        print(f"   [{direction}] Baseline Threshold: {_pct_text(b.baseline_threshold)}")
        print(f"   [{direction}] Active Threshold: {_pct_text(b.active_threshold)}")
        print(
            f"   [{direction}] Threshold Ledger Events: "
            f"{len(b.threshold_ledger)} (append-only)"
        )
        print(
            f"   [{direction}] Unreversed Normal Losses: "
            f"{inv.threshold_unreversed_losses(direction, False)}"
        )
        print(
            f"   [{direction}] Unreversed Inverted Losses: "
            f"{inv.threshold_unreversed_losses(direction, True)}"
        )
        print(f"   [{direction}] Last Adjustment: {inv.threshold_change_text(direction)}")
        if rate is None:
            reason = "no resolved virtual outcomes"
        elif rate <= b.active_threshold - INVERSION_HYSTERESIS_GAP:
            reason = f"virtual {rate:.1f}% <= ON boundary {b.active_threshold - INVERSION_HYSTERESIS_GAP:.1f}%"
        elif rate >= b.active_threshold + INVERSION_HYSTERESIS_GAP:
            reason = f"virtual {rate:.1f}% >= OFF boundary {b.active_threshold + INVERSION_HYSTERESIS_GAP:.1f}%"
        else:
            reason = f"hysteresis band; retaining previous mode ({modes[direction]})"
        print(
            f"   Inversion for next {direction}-streak signal: "
            f"{modes[direction]} ({reason})"
        )
        for d in DIRECTIONS:
            o = inv.b[d]
            r = inv.rate(d)
            r_text = "n/a" if r is None else f"{r:.1f}%"
            print(
                f"   >> {d} bucket: virtual {r_text} "
                f"({o.virtual_wins}W {o.virtual_losses}L) | "
                f"threshold {_pct_text(o.active_threshold)} | "
                f"inversion {modes[d]}"
            )
        print("   ----------------------------------------------------")


# --------------------------------------------------------------------------- #
# Child
# --------------------------------------------------------------------------- #
class Child:
    def __init__(self):
        self.ws = None
        self.stop = None  # asyncio.Event, created in run()
        self.parent_state = None  # last metrics snapshot from the parent

        # order tracking
        self.busy = False
        self.contract_id = None
        self.busy_since = 0.0
        self.query_tries = 0
        self.resolved = set()
        self.pending_meta = None  # metadata of the order just sent
        self.metas = {}  # contract_id -> metadata until the contract resolves
        self.tick_epochs = {s: deque(maxlen=60) for s in CHILD_SYMBOLS}

        # money management + statistics: fresh per process, never shared
        self.stake = BASE_STAKE
        self.mg_step = 0
        self.wins = self.losses = self.unresolved = 0
        self.net_pl = self.max_pl = self.min_pl = 0.0
        self.win_streak = self.loss_streak = 0
        self.longest_win = self.longest_loss = 0
        self.target_hits = 0
        self.basket = 0.0
        self.min_bankroll = BASE_STAKE
        self.loss_streak_counts = {}
        self.win_streak_counts = {}

        self.inversion = AdaptiveInversion()  # the child's own buckets
        self.reporter = Reporter(self)

    # ----------------------------------------------------------------- utils
    async def send(self, payload):
        await self.ws.send(dumps(payload))

    def release(self):
        self.busy = False
        self.contract_id = None
        self.query_tries = 0
        self.pending_meta = None

    @staticmethod
    def count_streak(counts, previous, current):
        if previous > 0:
            left = counts.get(previous, 0) - 1
            if left > 0:
                counts[previous] = left
            else:
                counts.pop(previous, None)
        counts[current] = counts.get(current, 0) + 1

    def streak_line(self):
        parts = []
        for label, counts in (
            ("LS", self.loss_streak_counts),
            ("WS", self.win_streak_counts),
        ):
            if counts:
                top = sorted(counts.items(), reverse=True)[:3]
                parts.append(f"{label} " + " ".join(f"{n}x{c}" for n, c in top))
        return " | ".join(parts)

    def decide_signal(self, event):
        """Returns (signal, inverted, inversion_note)."""
        parent = event["contract"]
        if SIGNAL_MODE == "copy":
            signal = parent
        elif SIGNAL_MODE == "invert":
            signal = INVERSE_SIGNAL.get(parent)
        elif SIGNAL_MODE == "adaptive":
            direction = event.get("direction")
            if direction in DIRECTIONS and parent in INVERSE_SIGNAL:
                invert = self.inversion.decide(direction)  # only THIS direction
                signal = INVERSE_SIGNAL[parent] if invert else parent
                return signal, invert, self.inversion.summary(direction)
            signal = parent
        else:
            signal = custom_decision(event, event.get("state") or {})
        return signal, signal is not None and signal != parent, ""

    def decide_stake(self, event):
        if STAKE_MODE == "martingale":
            stake = self.stake
        elif STAKE_MODE == "fixed":
            stake = FIXED_STAKE
        elif STAKE_MODE == "multiplier":
            stake = float(event["stake"]) * STAKE_MULTIPLIER
        else:  # mirror
            stake = float(event["stake"])
        return round(min(MAX_STAKE, max(MIN_STAKE, stake)), 2)

    # [MARTINGALE] (identical to the parent's, on the child's own results) --
    def update_min_bankroll(self):
        need = max(0.0, -self.net_pl) + self.stake
        if need > self.min_bankroll:
            self.min_bankroll = need

    def apply_result(self, status, profit):
        """Update P&L and the stake. Returns an info message."""
        self.net_pl += profit
        self.max_pl = max(self.max_pl, self.net_pl)
        self.min_pl = min(self.min_pl, self.net_pl)

        if status == "won":
            self.wins += 1
            prev_win_streak = self.win_streak
            self.win_streak += 1
            self.count_streak(
                self.win_streak_counts, prev_win_streak, self.win_streak
            )
            self.loss_streak = 0
            self.longest_win = max(self.longest_win, self.win_streak)
            traded_stake = self.stake  # stake of the trade that just won
            self.stake, self.mg_step = BASE_STAKE, 0

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
            prev_loss_streak = self.loss_streak
            self.loss_streak += 1
            self.count_streak(
                self.loss_streak_counts, prev_loss_streak, self.loss_streak
            )
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

    def stop_if_target_reached(self):
        if self.net_pl < TARGET_PL:
            return False
        reached = self.net_pl
        self.basket += reached
        self.target_hits += 1
        self.net_pl = 0.0
        print(
            f"\nProfit target reached: +{reached:.2f} USD (target: {TARGET_PL:.2f} USD)."
        )
        print(f"Session basket: {self.basket:.2f} USD.")
        print("Stopping child. No further trades will be opened.")
        self.stop.set()
        return True

    def stop_if_session_loss_reached(self):
        if MAX_SESSION_LOSS is None or self.net_pl > -MAX_SESSION_LOSS:
            return False
        print(
            f"\nSession loss limit reached: {self.net_pl:+.2f} USD "
            f"(limit: -{MAX_SESSION_LOSS:.2f} USD)."
        )
        print("Stopping child. No further trades will be opened.")
        self.stop.set()
        return True

    def entry_lag(self, meta, poc):
        """Ticks between the signal tick and the contract's entry tick."""
        if meta is None:
            return None
        epochs = self.tick_epochs.get(meta.get("symbol"))
        entry = _to_int(poc.get("entry_tick_time"))
        signal_epoch = meta.get("signal_epoch")
        if epochs is None or entry is None or signal_epoch is None or entry not in epochs:
            return None
        return sum(1 for e in epochs if signal_epoch < e <= entry)

    # ---------------------------------------------------------- parent feed
    async def on_parent_message(self, msg):
        kind = msg.get("type")
        if kind == "hello":
            self.parent_state = msg.get("state")
            print("[CHILD] parent snapshot received")
        elif kind == "order":
            self.parent_state = msg.get("state")
            await self.on_order(msg)
        elif kind == "result":
            self.parent_state = msg.get("state")
            if PRINT_PARENT_RESULTS:
                st = self.parent_state or {}
                print(
                    f"[PARENT] {str(msg.get('status')).upper()} "
                    f"{msg.get('profit', 0):+.2f} | net {st.get('net_pl', 0):+.2f}"
                )

    async def on_order(self, ev):
        if self.stop.is_set():
            return
        if MAX_EVENT_AGE_S is not None and time.time() - ev.get("ts", 0) > MAX_EVENT_AGE_S:
            print("[CHILD] SKIP stale order event")
            return
        if self.ws is None:
            print("[CHILD] SKIP Deriv connection not ready")
            return
        if self.busy:
            print("[CHILD] SKIP previous child contract still open")
            return

        signal, inverted, inv_note = self.decide_signal(ev)
        if signal not in ("CALL", "PUT"):
            print(f"[CHILD] SKIP parent {ev['contract']} -> no trade ({SIGNAL_MODE})")
            return
        stake = self.decide_stake(ev)
        if STAKE_MODE != "martingale":
            self.stake = stake  # keeps P&L / bankroll maths on the stake used

        # Flag first, send second: no window for a duplicate order.
        self.busy = True
        self.contract_id = None
        self.busy_since = time.monotonic()
        self.query_tries = 0
        self.pending_meta = {  # fixed now, never derived from the outcome
            "original": ev["contract"],
            "executed": signal,
            "inverted": inverted,
            "direction": ev.get("direction"),
            "symbol": ev["symbol"],
            "signal_epoch": ev.get("epoch"),
            "stake": stake,
        }
        await self.send(
            {
                "buy": "1",
                "price": stake,
                "parameters": {
                    "amount": stake,
                    "basis": "stake",
                    "contract_type": signal,
                    "currency": CURRENCY,
                    "duration": 1,
                    "duration_unit": "t",
                    "underlying_symbol": ev["symbol"],
                },
                "req_id": REQ_BUY,
            }
        )
        print(
            f"[CHILD] ORDER {signal} stake {stake:.2f} | parent {ev['contract']} | "
            f"{ev['direction']} | inv {'YES' if inverted else 'NO'}"
            + (f" [{inv_note}]" if inv_note else "")
        )

    # --------------------------------------------------------- deriv events
    async def on_buy(self, data):
        info = data.get("buy") or {}
        cid = info.get("contract_id")
        if cid is None:
            print("[CHILD] buy response had no contract ID")
            self.release()
            return
        self.contract_id = int(cid)
        self.metas[self.contract_id] = self.pending_meta
        self.pending_meta = None
        await self.send(
            {
                "proposal_open_contract": 1,
                "contract_id": self.contract_id,
                "subscribe": 1,
                "req_id": REQ_POC,
            }
        )

    async def on_poc(self, data):
        poc = data.get("proposal_open_contract") or {}
        cid = poc.get("contract_id")
        if not poc.get("is_sold") or cid is None or cid in self.resolved:
            return
        self.resolved.add(cid)

        try:
            sub_id = (data.get("subscription") or {}).get("id")
            if sub_id:
                await self.send({"forget": sub_id})
            elif data.get("req_id") == REQ_POC_QUERY:
                await self.send({"forget_all": "proposal_open_contract"})

            status = poc.get("status")
            profit = float(poc.get("profit", 0.0))
            meta = self.metas.pop(cid, None)
            lag = self.entry_lag(meta, poc)

            info = self.apply_result(status, profit)
            if STAKE_MODE != "martingale" and meta is not None:
                self.stake = meta["stake"]  # the stake actually used, not the sim
            self.update_min_bankroll()

            # the child's own result moves the child's own buckets / thresholds
            self.inversion.record_result(meta, status)

            if info and STAKE_MODE == "martingale":
                print(info)

            ps = self.parent_state or {}
            self.reporter.resolved(
                poc,
                cid,
                status,
                profit,
                meta,
                lag,
                ps.get("segment", "n/a"),
                ps.get("lookahead", "n/a"),
            )

            self.stop_if_target_reached()
            self.stop_if_session_loss_reached()
        finally:
            if self.contract_id == cid:
                self.release()

    def on_error(self, data):
        err = data["error"]
        message = err.get("message", err)
        print(f"[CHILD] API error: {message}")
        if message == "Your account balance is insufficient to buy this contract.":
            print("[CHILD] balance exhausted. Stopping.")
            self.stop.set()
            return
        if data.get("req_id") == REQ_BUY and self.contract_id is None:
            self.release()

    # ------------------------------------------------------------- loops
    async def watchdog(self):
        """Never stay locked on a contract that does not resolve."""
        while True:
            await asyncio.sleep(2)
            if not self.busy or self.ws is None:
                continue
            if time.monotonic() - self.busy_since < BUSY_TIMEOUT_S:
                continue
            if self.contract_id is not None and self.query_tries < 1:
                self.query_tries += 1
                self.busy_since = time.monotonic()
                print(f"[CHILD] contract {self.contract_id} overdue: querying status")
                try:
                    await self.send(
                        {
                            "proposal_open_contract": 1,
                            "contract_id": self.contract_id,
                            "req_id": REQ_POC_QUERY,
                        }
                    )
                except Exception as error:
                    print(f"[CHILD] status query failed: {str(error)}")
            else:
                print("[CHILD] contract still unresolved: releasing (result unknown)")
                self.unresolved += 1
                self.release()

    async def heartbeat(self):
        try:
            while True:
                await asyncio.sleep(20)
                await self.send({"ping": 1})
        except (asyncio.CancelledError, websockets.ConnectionClosed):
            pass

    async def deriv_loop(self):
        delay = 2
        while True:
            try:
                if CHILD_WS_URL:
                    url = CHILD_WS_URL
                else:
                    url = await asyncio.to_thread(
                        get_ws_url, account_type=CHILD_ACCOUNT_TYPE
                    )
                async with websockets.connect(
                    url, open_timeout=15, ping_interval=30, ping_timeout=10
                ) as ws:
                    self.ws = ws
                    print(f"[CHILD] connected to Deriv ({CHILD_ACCOUNT_TYPE})")
                    delay = 2
                    if self.busy and self.contract_id is not None:
                        await self.send(
                            {
                                "proposal_open_contract": 1,
                                "contract_id": self.contract_id,
                                "subscribe": 1,
                                "req_id": REQ_POC,
                            }
                        )
                    else:
                        self.release()
                    await self.send(
                        {"ticks": CHILD_SYMBOLS, "subscribe": 1, "req_id": REQ_TICKS}
                    )

                    pinger = asyncio.create_task(self.heartbeat())
                    try:
                        async for raw in ws:
                            data = loads(raw)
                            msg_type = data.get("msg_type")
                            if msg_type == "tick":
                                tick = data.get("tick") or {}
                                epochs = self.tick_epochs.get(tick.get("symbol"))
                                if epochs is not None and "epoch" in tick:
                                    epochs.append(tick["epoch"])
                            elif "error" in data:
                                self.on_error(data)
                            elif msg_type == "buy":
                                if data.get("req_id") == REQ_BUY:
                                    await self.on_buy(data)
                            elif msg_type == "proposal_open_contract":
                                await self.on_poc(data)
                    finally:
                        pinger.cancel()
                print("[CHILD] Deriv connection closed")
            except asyncio.CancelledError:
                raise
            except Exception as error:
                print(f"[CHILD] Deriv connection error: {str(error)}")
            finally:
                self.ws = None
            print(f"[CHILD] reconnecting to Deriv in {delay}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    async def parent_loop(self):
        delay = 2
        while True:
            try:
                async with websockets.connect(
                    PARENT_URL, open_timeout=10, ping_interval=20, ping_timeout=20
                ) as ws:
                    await ws.send(dumps({"token": PARENT_TOKEN, "name": NAME}))
                    print(f"[CHILD] connected to parent {PARENT_URL}")
                    delay = 2
                    async for raw in ws:
                        await self.on_parent_message(loads(raw))
                print("[CHILD] parent connection closed")
            except asyncio.CancelledError:
                raise
            except Exception as error:
                print(f"[CHILD] parent connection error: {str(error)}")
            print(f"[CHILD] reconnecting to parent in {delay}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 15)

    async def run(self):
        self.stop = asyncio.Event()
        print(
            f"CHILD {NAME} | fresh session | SIGNAL_MODE={SIGNAL_MODE} | "
            f"STAKE_MODE={STAKE_MODE} | ACCOUNT={CHILD_ACCOUNT_TYPE}"
        )
        if SIGNAL_MODE == "adaptive":
            print(
                f"INVERSION_THRESHOLD={INVERSION_THRESHOLD:.0f}% (per direction) | "
                f"HYSTERESIS_GAP={INVERSION_HYSTERESIS_GAP}pp | "
                f"THRESHOLD_FACTORS={THRESHOLD_LOSS_FACTORS} | "
                f"BOUNDS=[{THRESHOLD_MIN}, {THRESHOLD_MAX}]"
            )
        tasks = [
            asyncio.create_task(self.deriv_loop()),
            asyncio.create_task(self.parent_loop()),
            asyncio.create_task(self.watchdog()),
        ]
        try:
            await self.stop.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(Child().run())
    except KeyboardInterrupt:
        print("Stopped")
