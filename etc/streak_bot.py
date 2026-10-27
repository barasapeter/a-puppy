"""
Deriv tick-streak bot (optimised, fail-closed).

Key ideas
---------
* LOOKAHEAD ("virtual streak"): a buy never executes on the signal tick, it
  executes on the tick that arrives after the server has processed it (usually
  1-2 ticks later).  So we fire the order *early*, assuming the next
  LOOKAHEAD ticks continue the streak:

      virtual streak = real streak + LOOKAHEAD
      fire when virtual streak >= STREAK

  The bias window measures the same entry/exit pair the real trade uses.

* AUTO_LOOKAHEAD: the entry lag of every resolved contract is measured and a
  rolling median tunes LOOKAHEAD to your real latency.

* ONE CONTRACT AT A TIME, FAIL-CLOSED.  No new order is ever sent unless
  can_trade() is True, which requires ALL of:
      - the account was verified (no open contracts) since the last connect
      - no order is awaiting confirmation or resolution
      - no stray / adopted contract is still open
  The lock is released only on POSITIVE evidence:
      - the contract's outcome arrived (stream, poll or profit table), or
      - the server explicitly rejected the buy, or
      - an unconfirmed buy was checked repeatedly against both the portfolio
        and the profit table over a settle period and found nowhere.
  A resolution watchdog keeps polling forever; it never gives up and unlocks.
  The token needs Read + Trade scopes (portfolio / profit_table are read calls).
"""

import asyncio
import json
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
AUTO_LOOKAHEAD = True  # tune LOOKAHEAD from measured entry lag
MAX_LOOKAHEAD = 3

USE_PROPOSALS = False  # pre-priced proposals: skips server-side pricing on buy
PRINT_TICKS = True

BASE_STAKE = 1.0
MARTINGALE_MULTIPLIER = 2.0
MAX_MARTINGALE_STEPS = 10
MAX_STAKE = 100.0
TARGET_PL = 15.0

# Resolution watchdog (seconds unless noted)
BUY_CONFIRM_TIMEOUT = 6  # no buy response after this long -> start verifying
RESOLVE_TIMEOUT = 8  # confirmed but no outcome after this long -> recover
RESOLVE_CHECK_INTERVAL = 3  # gap between recovery attempts (first FAST_CHECKS)
SLOW_CHECK_INTERVAL = 10  # gap afterwards; it never stops checking
FAST_CHECKS = 6
ALERT_EVERY = 6  # loud "LOCKED" warning every N checks
VERIFY_RETRY = 5  # resend the account check if it got no reply

# Unconfirmed buys: released only after this much clean evidence
UNFILLED_MIN_SECONDS = 20
UNFILLED_CLEAN_CHECKS = 4  # e.g. 2 portfolio + 2 profit-table checks, all empty
STRICT_UNCONFIRMED = False  # True: never auto-release; stay locked until restart
CLOCK_SLACK = 5  # tolerated local/server clock difference when matching orders

REQ_TICKS, REQ_POC = 1, 7007
REQ_POC_POLL, REQ_PROFIT, REQ_PF_START, REQ_PF_ORDER = 7008, 7009, 7010, 7011
REQ_PROPOSAL = {"CALL": 8001, "PUT": 8002}
BUY_REQ_BASE = 100000  # every buy gets a unique req_id above this


def timestamp(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%H:%M:%S")


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
    stats: dict = field(
        default_factory=lambda: {"continuation": 0, "reversal": 0, "neutral": 0}
    )

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
    t_sent: float  # monotonic
    req_id: int
    sent_epoch: float  # wall clock, to match against server records
    contract_id: int | None = None
    confirmed_at: float = 0.0
    last_check: float = 0.0
    checks: int = 0
    clean_checks: int = 0  # evidence (unconfirmed orders) that nothing was filled


def matches_order(order, rec):
    """Does a portfolio / profit-table record look like this unconfirmed order?"""
    try:
        if float(rec.get("purchase_time", 0)) < order.sent_epoch - CLOCK_SLACK:
            return False
    except (TypeError, ValueError):
        return False
    label = (
        rec.get("shortcode") or rec.get("symbol") or rec.get("underlying_symbol") or ""
    )
    return not label or order.symbol in str(label)


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

        # order / lock tracking
        self.verified = False  # account checked since the last connect
        self.verify_sent = 0.0
        self.order = None
        self.strays = set()  # contracts we did not place in this run / late buys
        self.resolved = set()
        self.buy_seq = BUY_REQ_BASE
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

    def lock_reason(self):
        """Empty string means trading is allowed."""
        if not self.verified:
            return "verifying account"
        if self.order is not None:
            if self.order.contract_id is None:
                return "previous order unconfirmed"
            return "previous contract unresolved"
        if self.strays:
            return "other contract still open"
        return ""

    def can_trade(self):
        return not self.lock_reason()

    def release(self):
        self.order = None

    async def send(self, payload):
        await self.ws.send(dumps(payload))

    @staticmethod
    def stats_line(symbol, stats):
        total = sum(stats.values())
        if not total:
            return None
        c, r, n = stats["continuation"], stats["reversal"], stats["neutral"]
        return (
            f"SESSION BIAS | {symbol} | "
            f"Continuation: {c / total * 100:.1f}% ({c}) | "
            f"Reversal: {r / total * 100:.1f}% ({r}) | "
            f"Neutral: {n / total * 100:.1f}% ({n}) | Completed: {total}"
        )

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
            st.stats[outcome] += 1

            logs.append(
                f"\nBIAS RESULT | {timestamp(epoch)} | {symbol} | "
                f"Original: {w.direction} | Start: {start} | End: {price} | "
                f"Change: {price - start:+.5f} | Outcome: {outcome.upper()} | "
                f"Window: {BIAS_WINDOW} ticks"
            )
            logs.append(self.stats_line(symbol, st.stats))

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

        note = ""
        if st.count >= self.trigger_count() and not st.alerted:
            st.alerted = True
            look = self.lookahead

            # Bias window mirrors the real trade: opens on the entry tick
            # (look ticks from now) and closes BIAS_WINDOW ticks later.
            st.windows.append(
                Window(direction, price if look == 0 else None, look)
            )

            contract_type = infer_market_bias(
                stats=st.stats, current_streak_direction=direction
            )

            # ---- hot path: send BEFORE any printing ----
            reason = self.lock_reason()
            if contract_type == "SKIP":
                note = " | [SKIP] no bias data yet"
            elif reason:
                note = f" | [SKIP] LOCKED: {reason}"
            else:
                mode = await self.place_order(symbol, contract_type, st.tick_idx)
                note = (
                    f" | [ORDER {contract_type}] stake {self.stake:.2f} {CURRENCY} | "
                    f"step {self.mg_step} | {mode}"
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
        self.buy_seq += 1
        req_id = self.buy_seq
        prop = self.proposals.get((symbol, contract_type)) if USE_PROPOSALS else None

        if prop and prop[1] == stake:
            payload = {"buy": prop[0], "price": stake, "req_id": req_id}
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
                "req_id": req_id,
            }
            mode = "direct"

        # Lock first, send second. If the send itself fails the order stays
        # registered (fate unknown) and the watchdog verifies it - never assume.
        self.order = Order(
            symbol,
            contract_type,
            signal_idx,
            time.monotonic(),
            req_id,
            time.time(),
        )
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

        if contract_id is None:
            # Error replies go through on_error; anything else stays locked and
            # the watchdog verifies the order.
            print("[!] Buy response did not contain a contract ID.")
            return

        cid = int(contract_id)
        known = order.contract_id if order is not None else None
        if cid == known or cid in self.resolved or cid in self.strays:
            return  # duplicate / already tracked

        mine = order is not None and data.get("req_id") == order.req_id
        if mine and order.contract_id is None:
            order.contract_id = cid
            order.confirmed_at = time.monotonic()
            order.checks = 0
            rtt = (order.confirmed_at - order.t_sent) * 1000
            balance = info.get("balance_after")
            balance_text = f"{float(balance):.2f} USD" if balance is not None else "n/a"
            await self.subscribe_contract(cid)
            print(
                f"\n[+] ORDER CONFIRMED | Contract ID: {cid} | "
                f"Balance: {balance_text} | Buy RTT: {rtt:.0f} ms"
            )
        else:
            # A buy we no longer expect (e.g. a very late fill): track it so it
            # blocks trading until it settles.
            self.strays.add(cid)
            await self.subscribe_contract(cid)
            print(f"\n[!] LATE/UNEXPECTED CONTRACT {cid} adopted - trading locked until it settles.")

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
        status = poc.get("status")
        if not (poc.get("is_sold") or status in ("won", "lost")):
            return

        sub_id = (data.get("subscription") or {}).get("id")
        if sub_id:
            await self.send({"forget": sub_id})

        contract_id = poc.get("contract_id")
        if contract_id is None:
            return
        await self.finalize(
            int(contract_id), status, float(poc.get("profit", 0.0)), poc
        )

    async def finalize(self, contract_id, status, profit, poc, source="stream"):
        """Single resolution path, used by the stream and every fallback."""
        if contract_id in self.resolved:
            return
        self.resolved.add(contract_id)

        if contract_id in self.strays:
            self.strays.discard(contract_id)
            print(
                f"\n[STRAY CONTRACT RESOLVED] ID {contract_id} | "
                f"{str(status).upper()} | {profit:+.2f} USD "
                f"(not counted in stats) [via {source}]"
            )
            return

        order = self.order
        if order is None or order.contract_id != contract_id:
            return  # not the contract we are waiting for

        lag = self.record_lag(order, poc.get("entry_tick_time"))

        old_stake = self.stake
        info = self.apply_result(status, profit)
        if info:
            print(info)

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
        print(
            f"   TRADE RESOLVED | ID: {contract_id}    "
            f"[URSVD {self.unresolved}] [via {source}]"
        )
        print(f"   Result: {str(status).upper()}  [MXLS {self.longest_loss}]")
        print(f"   Current P&L: {profit:+.2f} USD [MXPL {self.max_pl}]")
        print(f"   Win rate: {win_rate:.0f}%      [MINPL {self.min_pl}]")
        print("==========================================")
        print(f"   Min bankroll: {min_bankroll} USD ")
        print(f"   Net P&L: {self.net_pl:+.2f} USD     [TGTPL {target_status}]")
        print("==========================================\n")

        if self.net_pl >= TARGET_PL:
            self.basket += self.net_pl
            self.target_hits += 1
            self.net_pl = 0.0
            print("Net PL has been reset.")

        self.release()  # the ONLY normal path that unlocks trading

        if USE_PROPOSALS and self.stake != old_stake:
            await self.refresh_proposals()

    def on_error(self, data):
        err = data["error"]
        print(f"\n[!] API error response: {err.get('message', err)}")

        req = data.get("req_id")
        order = self.order
        if order is not None and req == order.req_id and order.contract_id is None:
            # The server explicitly rejected THIS buy: nothing was purchased.
            print("[i] Buy rejected by server - nothing was bought, unlocking.")
            self.release()
        elif req == REQ_PF_START:
            print("[!] Account check failed - staying LOCKED and retrying.")

    # -------------------------------------------------- account verification
    async def request_verification(self):
        self.verify_sent = time.monotonic()
        await self.send({"portfolio": 1, "req_id": REQ_PF_START})

    async def adopt_unconfirmed(self, contracts):
        """Attach an open contract to an order whose buy reply never arrived."""
        order = self.order
        if order is None or order.contract_id is not None:
            return False
        for rec in contracts:
            cid = int(rec["contract_id"])
            if cid in self.resolved or not matches_order(order, rec):
                continue
            order.contract_id = cid
            order.confirmed_at = time.monotonic()
            order.checks = 0
            await self.subscribe_contract(cid)
            print(f"[WATCHDOG] Unconfirmed order found open on server: contract {cid}")
            return True
        return False

    async def on_portfolio(self, data):
        contracts = (data.get("portfolio") or {}).get("contracts") or []
        req = data.get("req_id")
        order = self.order

        if req == REQ_PF_START:
            adopted = await self.adopt_unconfirmed(contracts)
            for rec in contracts:
                cid = int(rec["contract_id"])
                if (
                    cid in self.resolved
                    or cid in self.strays
                    or (order is not None and cid == order.contract_id)
                ):
                    continue
                if adopted and order is not None and cid == order.contract_id:
                    continue
                self.strays.add(cid)
                await self.subscribe_contract(cid)
                print(
                    f"[!] Open contract {cid} found on the account - "
                    f"trading locked until it settles."
                )
            self.verified = True
            print(
                f"[VERIFIED] Account check done | open contracts: {len(contracts)} | "
                f"trading {'LOCKED' if self.lock_reason() else 'allowed'}"
            )

        elif req == REQ_PF_ORDER and order is not None:
            if order.contract_id is None:
                if not await self.adopt_unconfirmed(contracts):
                    order.clean_checks += 1
                    self.maybe_release_unfilled(order)
            else:
                still_open = any(
                    int(r["contract_id"]) == order.contract_id for r in contracts
                )
                print(
                    f"[WATCHDOG] Contract {order.contract_id} is "
                    f"{'STILL OPEN on the server - waiting' if still_open else 'closed on the server - looking up its result'}"
                )

    def maybe_release_unfilled(self, order):
        """Unlock an unconfirmed buy only after repeated clean evidence."""
        if order.contract_id is not None:
            return
        if time.monotonic() - order.t_sent < UNFILLED_MIN_SECONDS:
            return
        if order.clean_checks < UNFILLED_CLEAN_CHECKS:
            return
        if STRICT_UNCONFIRMED:
            if order.clean_checks == UNFILLED_CLEAN_CHECKS:
                print(
                    "[WATCHDOG] Order appears unfilled but STRICT_UNCONFIRMED is on "
                    "- staying locked. Verify on Deriv, then restart the bot."
                )
            return
        print(
            f"[WATCHDOG] Order {order.req_id} verified as never filled "
            f"({order.clean_checks} clean checks) - unlocking."
        )
        self.release()

    # -------------------------------------------------------------- watchdog
    async def watchdog(self):
        """Never gives up: keeps verifying until the lock can be justified-released."""
        while True:
            try:
                await asyncio.sleep(1)
                now = time.monotonic()

                if not self.verified and now - self.verify_sent >= VERIFY_RETRY:
                    await self.request_verification()

                order = self.order
                if order is None:
                    continue

                interval = (
                    RESOLVE_CHECK_INTERVAL
                    if order.checks < FAST_CHECKS
                    else SLOW_CHECK_INTERVAL
                )
                if now - order.last_check < interval:
                    continue

                if order.contract_id is None:
                    if now - order.t_sent >= BUY_CONFIRM_TIMEOUT:
                        await self.recover_unconfirmed(order, now)
                elif now - order.confirmed_at >= RESOLVE_TIMEOUT:
                    await self.recover_open(order, now)

            except (asyncio.CancelledError, websockets.ConnectionClosed):
                return
            except Exception:  # keep the watchdog alive no matter what
                traceback.print_exc()

    def alert(self, order):
        if order.checks and order.checks % ALERT_EVERY == 0:
            print(
                f"\n[!!] TRADING LOCKED for {time.monotonic() - order.t_sent:.0f}s: "
                f"{'order unconfirmed' if order.contract_id is None else f'contract {order.contract_id} unresolved'}. "
                f"No new orders will be placed. Still checking."
            )

    async def recover_unconfirmed(self, order, now):
        """Buy reply never arrived: look for the order in profit table + portfolio."""
        order.last_check = now
        order.checks += 1
        self.alert(order)

        if order.checks % 2:
            print(
                f"\n[WATCHDOG] No buy confirmation after {now - order.t_sent:.0f}s - "
                f"checking profit table (check {order.checks})"
            )
            await self.send(
                {
                    "profit_table": 1,
                    "description": 1,
                    "limit": 10,
                    "sort": "DESC",
                    "req_id": REQ_PROFIT,
                }
            )
        else:
            print(f"[WATCHDOG] Checking open contracts (check {order.checks})")
            await self.send({"portfolio": 1, "req_id": REQ_PF_ORDER})

    async def recover_open(self, order, now):
        """Contract confirmed but its outcome never arrived. Cycles three sources."""
        order.last_check = now
        order.checks += 1
        self.alert(order)
        cid = order.contract_id
        step = (order.checks - 1) % 3
        waited = now - order.confirmed_at

        if step == 0:  # one-shot poll (no subscribe: no duplicate streams)
            print(
                f"\n[WATCHDOG] Contract {cid} unresolved after {waited:.0f}s - "
                f"polling its status (check {order.checks})"
            )
            await self.send(
                {"proposal_open_contract": 1, "contract_id": cid, "req_id": REQ_POC_POLL}
            )
        elif step == 1:  # independent source: settled-contract history
            print(f"[WATCHDOG] Checking profit table for contract {cid}")
            await self.send(
                {
                    "profit_table": 1,
                    "description": 1,
                    "contract_id": cid,
                    "limit": 1,
                    "req_id": REQ_PROFIT,
                }
            )
        else:  # is it even still open?
            await self.send({"portfolio": 1, "req_id": REQ_PF_ORDER})

    async def on_profit_table(self, data):
        order = self.order
        if order is None:
            return

        txs = (data.get("profit_table") or {}).get("transactions") or []
        for tx in txs:
            cid = tx.get("contract_id")
            if cid is None:
                continue
            cid = int(cid)
            if cid in self.resolved:
                continue

            if order.contract_id is not None:
                if cid != order.contract_id:
                    continue
            elif not matches_order(order, tx):
                continue  # an older trade, not the one we just sent
            else:
                order.contract_id = cid  # adopt the unconfirmed order
                order.confirmed_at = time.monotonic()

            buy_price = float(tx.get("buy_price", 0.0))
            profit = float(tx.get("sell_price", 0.0)) - buy_price
            await self.finalize(
                cid,
                "won" if profit > 0 else "lost",
                profit,
                {"buy_price": buy_price},
                source="profit_table",
            )
            return

        # No matching record: for an unconfirmed order this is "clean" evidence.
        if order.contract_id is None:
            order.clean_checks += 1
            self.maybe_release_unfilled(order)

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
        self.verified = False  # must re-verify the account on every connect

        now = time.monotonic()
        order = self.order
        if order is not None:
            # NEVER release here: re-attach and let the watchdog settle it.
            order.last_check = 0.0
            order.checks = 0
            order.clean_checks = 0
            if order.contract_id is not None:
                order.confirmed_at = now
                await self.subscribe_contract(order.contract_id)
            else:
                order.t_sent = now  # restart the settle period after the drop
        for cid in list(self.strays):
            await self.subscribe_contract(cid)

        await self.send({"ticks": SYMBOLS, "subscribe": 1, "req_id": REQ_TICKS})
        await self.request_verification()
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
                await self.on_buy(data)
            elif msg_type == "proposal_open_contract":
                await self.on_poc(data)
            elif msg_type == "portfolio":
                await self.on_portfolio(data)
            elif msg_type == "profit_table":
                await self.on_profit_table(data)
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
                        f"PROPOSALS={USE_PROPOSALS}"
                    )
                    await self.on_connect()
                    delay = 2

                    background = [
                        asyncio.create_task(self.heartbeat()),
                        asyncio.create_task(self.watchdog()),
                    ]
                    try:
                        await self.read_loop()
                    finally:
                        for task in background:
                            task.cancel()

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
