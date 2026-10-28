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

* [LOSSLOG] Every resolved contract is appended as a compact checkpoint report
  (signal tick, expected execution tick, confirmation / entry tick, direction
  checks, outcome, and a tick tape from the streak start to the exit).
  LOST contracts go to LOSS_LOG_FILE, WON contracts to WIN_LOG_FILE (both JSON
  arrays).  Purely observational.
"""

import asyncio
import json
import os
import time
import traceback
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from statistics import median_low

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
MIN_SAMPLES = 10  # min decided samples in the current direction's bucket
MIN_EDGE = 0.10  # 0.05  # min gap between reversal and continuation rate (0.05 = 5 pts)

# [SEGMENT] Sliding stats segments (per direction)
SEGMENT_ENABLED = True
SEGMENT_LIMIT = 60  # when decided samples in a direction reach this -> new segment
SEGMENT_KEEP = 10  # latest decided samples carried over into the new segment
# Growth per segment = SEGMENT_LIMIT - SEGMENT_KEEP

USE_PROPOSALS = False  # pre-priced proposals: skips server-side pricing on buy
PRINT_TICKS = True

BASE_STAKE = 1.0
MARTINGALE_MULTIPLIER = 2.0
MAX_MARTINGALE_STEPS = 100
MAX_STAKE = 4500.0
TARGET_PL = 15.0

# [LOSSLOG]
LOSS_LOG_ENABLED = True
LOSS_LOG_FILE = "loss_events.json"  # JSON array, one object per lost trade
WIN_LOG_ENABLED = True
WIN_LOG_FILE = "win_events.json"  # JSON array, one object per won trade
TICK_LOG_SIZE = 40  # recent ticks kept in memory for the tape

REQ_TICKS, REQ_BUY, REQ_POC = 1, 5005, 7007
REQ_PROPOSAL = {"CALL": 8001, "PUT": 8002}

DIRECTIONS = ("UP", "DOWN")


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
    contract_type: str
    signal_idx: int
    t_sent: float
    contract_id: int | None = None
    signal: dict | None = None  # [LOSSLOG] snapshot when the signal fired
    confirm_idx: int | None = None  # [LOSSLOG] tick idx when the buy was confirmed


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
    def build_signal_info(self, st, direction, contract_type, mode, look, decided):
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
            traceback.print_exc()
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
            if t["idx"] > sig_idx and entry_time is not None and t["epoch"] == entry_time:
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
                    exit_time is None and exit_spot is not None and t["price"] == exit_spot
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
            between = [
                t for t in st.tick_log if sig_idx < t["idx"] <= entry_t["idx"]
            ]
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
            kind, enabled, path, count = "loss", LOSS_LOG_ENABLED, LOSS_LOG_FILE, self.losses
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
            traceback.print_exc()
            print(f"[LOSSLOG] failed to save {kind} record: {error}")

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

            # ---- hot path: send BEFORE any printing ----
            if contract_type == "SKIP":
                note = f" | [SKIP] {direction} bias not decisive (N={decided})"
            elif self.busy:
                note = " | [SKIP] contract still open"
            else:
                mode = await self.place_order(symbol, contract_type, st.tick_idx)

                # [LOSSLOG] attach the signal snapshot AFTER the send
                if LOSS_LOG_ENABLED and self.order is not None:
                    self.order.signal = self.build_signal_info(
                        st, direction, contract_type, mode, look, decided
                    )

                note = (
                    f" | [ORDER {contract_type}] stake {self.stake:.2f} {CURRENCY} | "
                    f"step {self.mg_step} | {mode} | {direction}-bias N={decided} | "
                    f"seg {st.segments[direction]}"  # [SEGMENT]
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
    async def place_order(self, symbol, contract_type, signal_idx):
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
                    "amount": stake,
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
            self.stake, self.mg_step = BASE_STAKE, 0
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
        if not poc.get("is_sold"):
            return

        contract_id = poc.get("contract_id")
        if contract_id is None or contract_id in self.resolved:
            return
        self.resolved.add(contract_id)

        sub_id = (data.get("subscription") or {}).get("id")
        if sub_id:
            await self.send({"forget": sub_id})

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
        if info:
            print(info)

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
        mult = MARTINGALE_MULTIPLIER
        min_bankroll = BASE_STAKE * (mult ** (self.longest_loss + 1) - 1) / (mult - 1)

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
        print(f"   Min bankroll: {min_bankroll} USD ")
        print(f"   Net P&L: {self.net_pl:+.2f} USD     [TGTPL {target_status}]")
        print("==========================================\n")

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
                    print(  # [LOSSLOG]
                        f"LOSS_LOG={LOSS_LOG_ENABLED} | LOSS_LOG_FILE={LOSS_LOG_FILE} | "
                        f"WIN_LOG={WIN_LOG_ENABLED} | WIN_LOG_FILE={WIN_LOG_FILE}"
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
                traceback.print_exc()
                print(f"Connection error occurred: {error}")

            self.reconnects += 1
            print(f"Reconnecting in {delay} seconds...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


if __name__ == "__main__":
    try:
        asyncio.run(Bot().run())
    except KeyboardInterrupt:
        print("Stopped")
