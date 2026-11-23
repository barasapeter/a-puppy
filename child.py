"""
Deriv CHILD bot.

The child knows NOTHING about how the parent produces its signals. The parent
feed carries only the final signal (CALL or PUT, plus the symbol) and the
child simply executes it on its own Deriv connection.

* FRESH SESSION: every start is a clean slate. P&L, stake, streaks and the
  take-profit live only inside this process; nothing is shared with the parent
  or with other children, and nothing is saved to disk.
* TAKE PROFIT: each child has its own TP. When the child's net P&L reaches it,
  that child stops trading and exits; the parent and other children keep running.
* REPORTING: one compact report per resolved trade, plus a one-line status
  such as "[Parent Active] Waiting for signal...".

USAGE
    python child_bot.py [NAME] [TP]
    python child_bot.py --help

COMMAND-LINE ARGUMENTS  (both optional, positional, in this order)
    NAME         Label for this child, shown in the parent's log.
                 Default: $CHILD_NAME, otherwise a random "child-xxxx".
    TP           Take profit in USD. The child stops once its own net P&L
                 reaches it (the parent and other children keep running).
                 Default: $CHILD_TP, otherwise 666615.
    -h, --help   Show this help and exit.

    Examples
        python child_bot.py                 random name, default TP
        python child_bot.py alpha           named "alpha", default TP
        python child_bot.py alpha 25        named "alpha", stops at +25 USD

ENVIRONMENT VARIABLES  (all optional)
    CHILD_NAME           Same as NAME  (the command-line argument wins).
    CHILD_TP             Same as TP    (the command-line argument wins).
    CHILD_STAKE_MODE     "martingale" (default) or "fixed".
    CHILD_ACCOUNT_TYPE   Account type passed to auth.get_ws_url (default "demo").
    DERIV_CHILD_WS_URL   Full Deriv WebSocket URL. Overrides get_ws_url, so a
                         child can trade on a different account / token.

    Setting them on Windows
        cmd:         set CHILD_STAKE_MODE=fixed
                     python child_bot.py alpha 25
        PowerShell:  $env:CHILD_STAKE_MODE = "fixed"; python child_bot.py alpha 25

Everything else (parent URL and token, stake sizes, martingale, loss limit)
is a constant in the CONFIGURATION section of this file.

Run the parent first (it hosts the feed), then start each child in its own
terminal.
"""

import asyncio
import json
import math
import os
import sys
import time
import uuid

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
if any(a in ("-h", "--help") for a in sys.argv[1:]):
    print(__doc__)
    sys.exit(0)
if len(sys.argv) > 3:
    sys.exit("Too many arguments. Usage: python child_bot.py [NAME] [TP]  (see --help)")

# Name shown in the parent's log: `python child_bot.py alpha` or CHILD_NAME=alpha
NAME = (
    sys.argv[1]
    if len(sys.argv) > 1
    else os.environ.get("CHILD_NAME") or f"child-{uuid.uuid4().hex[:4]}"
)

# This child's take profit (net P&L in USD at which it stops):
# `python child_bot.py alpha 25`, or CHILD_TP=25, or the default below.
try:
    TARGET_PL = float(
        sys.argv[2] if len(sys.argv) > 2 else os.environ.get("CHILD_TP", 15.0)
    )
except ValueError:
    sys.exit("Invalid TP (must be a number). Usage: python child_bot.py [NAME] [TP]")

PARENT_URL = "ws://127.0.0.1:8765"  # must match PUBLISH_HOST / PUBLISH_PORT
PARENT_TOKEN = "change-me"  # must match PUBLISH_TOKEN ("" if the parent has none)

# Which Deriv account the child trades on. By default this calls the same
# auth.get_ws_url as the parent with this account_type. If your second account
# needs a different token, set the full URL in the DERIV_CHILD_WS_URL
# environment variable instead (it overrides get_ws_url).
CHILD_ACCOUNT_TYPE = os.environ.get("CHILD_ACCOUNT_TYPE", "demo")
CHILD_WS_URL = os.environ.get("DERIV_CHILD_WS_URL")
CURRENCY = "USD"

# Stake (override per child: CHILD_STAKE_MODE=fixed python child_bot.py):
#   "martingale" = the child's own martingale + recovery on ITS OWN results
#   "fixed"      = FIXED_STAKE on every trade
STAKE_MODE = os.environ.get("CHILD_STAKE_MODE", "martingale")
FIXED_STAKE = 1.0
MIN_STAKE = 1.0
MAX_STAKE = 1500.0

# [MARTINGALE] used when STAKE_MODE == "martingale"
BASE_STAKE = 1
MARTINGALE_MULTIPLIER = 2
MAX_MARTINGALE_STEPS = 10
RECOVERY_ENABLED = True

MAX_SESSION_LOSS = None  # e.g. 300.0: stop this child once its net P&L <= -300
MAX_EVENT_AGE_S = 5.0  # ignore signals older than this (None = off);
#                       needs synced clocks if the parent is on another machine
BUSY_TIMEOUT_S = 30.0  # unresolved contract: query its status, then give up

REQ_BUY, REQ_POC, REQ_POC_QUERY = 6005, 6007, 6008

if STAKE_MODE not in ("martingale", "fixed"):
    raise ValueError('STAKE_MODE must be "martingale" or "fixed"')
if TARGET_PL <= 0:
    raise ValueError("take profit (TP) must be positive")


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# Reporting interface
# --------------------------------------------------------------------------- #
class Reporter:
    """Resolved-trade report in the parent's format, minus the parent's mechanics."""

    def __init__(self, child):
        self.c = child

    def resolved(self, poc, contract_id, status, profit, order):
        c = self.c
        order = order or {}
        decided = c.wins + c.losses
        win_rate = c.wins / decided * 100 if decided else 0

        target_status = f"✅ [1] [BSKT {c.net_pl:.2f} USD]" if c.tp_reached else "⏰"
        emoji = "❗" * c.loss_streak if status == "lost" else "🏆" * c.win_streak
        emoji += (
            f" [C{'L' if c.loss_streak else 'W'}S " f"{c.loss_streak or c.win_streak}]"
        )

        print(
            f"\nEntry {poc.get('entry_spot')} -> Exit {poc.get('exit_spot')} | "
            f"Buy {poc.get('buy_price', order.get('stake'))} | "
            f"Signal {order.get('signal', 'n/a')}"
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
        if c.streak_line():
            print(f"   {c.streak_line()}")
        print("==========================================")
        print(f"   Min bankroll: {c.min_bankroll:.2f} USD ")
        print(f"   Net P&L: {c.net_pl:+.2f} USD     [TGTPL {target_status}]")
        print("==========================================\n\n")
        print()


# --------------------------------------------------------------------------- #
# Child
# --------------------------------------------------------------------------- #
class Child:
    def __init__(self):
        self.ws = None
        self.stop = None  # asyncio.Event, created in run()
        self.parent_connected = False
        self.state_text = None

        # order tracking
        self.busy = False
        self.contract_id = None
        self.busy_since = 0.0
        self.query_tries = 0
        self.resolved = set()
        self.pending_order = None  # signal / symbol / stake of the order just sent
        self.orders = {}  # contract_id -> that info until the contract resolves

        # money management + statistics: fresh per process, never shared
        self.stake = BASE_STAKE
        self.mg_step = 0
        self.wins = self.losses = self.unresolved = 0
        self.net_pl = self.max_pl = self.min_pl = 0.0
        self.win_streak = self.loss_streak = 0
        self.longest_win = self.longest_loss = 0
        self.tp_reached = False
        self.min_bankroll = BASE_STAKE
        self.loss_streak_counts = {}
        self.win_streak_counts = {}

        self.reporter = Reporter(self)

    # ----------------------------------------------------------------- utils
    async def send(self, payload):
        await self.ws.send(dumps(payload))

    def release(self):
        self.busy = False
        self.contract_id = None
        self.query_tries = 0
        self.pending_order = None

    def set_state(self, text):
        """Print the child's status line only when it changes."""
        if text != self.state_text:
            self.state_text = text
            print(text)

    def show_idle(self):
        self.set_state(
            "[Parent Active] Waiting for signal..."
            if self.parent_connected
            else "[Parent Offline] Reconnecting..."
        )

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

    def decide_stake(self):
        stake = FIXED_STAKE if STAKE_MODE == "fixed" else self.stake
        return round(min(MAX_STAKE, max(MIN_STAKE, stake)), 2)

    # [MARTINGALE] on the child's own results --------------------------------
    def update_min_bankroll(self):
        need = max(0.0, -self.net_pl) + self.stake
        if need > self.min_bankroll:
            self.min_bankroll = need

    def apply_result(self, status, profit):
        """Update P&L, streaks and the next stake."""
        self.net_pl += profit
        self.max_pl = max(self.max_pl, self.net_pl)
        self.min_pl = min(self.min_pl, self.net_pl)

        if status == "won":
            self.wins += 1
            prev_win_streak = self.win_streak
            self.win_streak += 1
            self.count_streak(self.win_streak_counts, prev_win_streak, self.win_streak)
            self.loss_streak = 0
            self.longest_win = max(self.longest_win, self.win_streak)
            traded_stake = self.stake  # stake of the trade that just won
            self.stake, self.mg_step = BASE_STAKE, 0

            # still below zero after a win -> size the next stake so a single
            # win recovers the deficit and ends above zero
            if RECOVERY_ENABLED and self.net_pl < 0 and profit > 0 and traded_stake > 0:
                rate = profit / traded_stake  # realised payout rate
                deficit = -self.net_pl
                stake = math.ceil(round((deficit / rate + BASE_STAKE) * 100, 6)) / 100
                self.stake = min(stake, MAX_STAKE)
            return

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
            else:
                self.stake, self.mg_step = BASE_STAKE, 0
            return

        self.unresolved += 1

    # ---------------------------------------------------------- parent feed
    async def on_parent_message(self, msg):
        if msg.get("type") == "signal":
            await self.on_signal(msg)

    async def on_signal(self, ev):
        signal = ev.get("contract")
        symbol = ev.get("symbol")
        if self.stop.is_set() or signal not in ("CALL", "PUT") or not symbol:
            return
        if (
            MAX_EVENT_AGE_S is not None
            and time.time() - ev.get("ts", 0) > MAX_EVENT_AGE_S
        ):
            print(f"[Parent Active] Signal {signal} ignored (stale)")
            return
        if self.ws is None:
            print(f"[Child] Signal {signal} ignored (Deriv not connected)")
            return
        if self.busy:
            print(f"[Parent Active] Signal {signal} ignored (trade still open)")
            return

        stake = self.decide_stake()
        if STAKE_MODE == "fixed":
            self.stake = stake  # keeps the bankroll maths on the stake used

        # Flag first, send second: no window for a duplicate order.
        self.busy = True
        self.contract_id = None
        self.busy_since = time.monotonic()
        self.query_tries = 0
        self.pending_order = {"signal": signal, "symbol": symbol, "stake": stake}
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
                    "underlying_symbol": symbol,
                },
                "req_id": REQ_BUY,
            }
        )
        self.set_state(
            f"[Parent Active] Signal {signal} received | stake {stake:.2f} | trade open"
        )

    # --------------------------------------------------------- deriv events
    async def on_buy(self, data):
        info = data.get("buy") or {}
        cid = info.get("contract_id")
        if cid is None:
            print("[Child] buy response had no contract ID")
            self.release()
            self.show_idle()
            return
        self.contract_id = int(cid)
        self.orders[self.contract_id] = self.pending_order
        self.pending_order = None
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
            order = self.orders.pop(cid, None)

            self.apply_result(status, profit)
            if STAKE_MODE == "fixed" and order is not None:
                self.stake = order["stake"]  # the stake actually used
            self.update_min_bankroll()

            self.tp_reached = self.net_pl >= TARGET_PL
            self.reporter.resolved(poc, cid, status, profit, order)

            if self.tp_reached:
                print(
                    f"[Child] TP reached: {self.net_pl:+.2f} USD "
                    f"(TP {TARGET_PL:.2f} USD). Child stopped; parent keeps running."
                )
                self.stop.set()
            elif MAX_SESSION_LOSS is not None and self.net_pl <= -MAX_SESSION_LOSS:
                print(
                    f"[Child] Loss limit reached: {self.net_pl:+.2f} USD "
                    f"(limit -{MAX_SESSION_LOSS:.2f} USD). Child stopped; parent keeps running."
                )
                self.stop.set()
        finally:
            if self.contract_id == cid:
                self.release()
                if not self.stop.is_set():
                    self.show_idle()

    def on_error(self, data):
        err = data["error"]
        message = err.get("message", err)
        print(f"[Child] API error: {message}")
        if message == "Your account balance is insufficient to buy this contract.":
            print("[Child] balance exhausted. Stopping.")
            self.stop.set()
            return
        if data.get("req_id") == REQ_BUY and self.contract_id is None:
            self.release()
            self.show_idle()

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
                print(f"[Child] contract {self.contract_id} overdue: querying status")
                try:
                    await self.send(
                        {
                            "proposal_open_contract": 1,
                            "contract_id": self.contract_id,
                            "req_id": REQ_POC_QUERY,
                        }
                    )
                except Exception as error:
                    print(f"[Child] status query failed: {str(error)}")
            else:
                print("[Child] contract still unresolved: releasing (result unknown)")
                self.unresolved += 1
                self.release()
                self.show_idle()

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
                    print(f"[Child] connected to Deriv ({CHILD_ACCOUNT_TYPE})")
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

                    pinger = asyncio.create_task(self.heartbeat())
                    try:
                        async for raw in ws:
                            data = loads(raw)
                            msg_type = data.get("msg_type")
                            if "error" in data:
                                self.on_error(data)
                            elif msg_type == "buy":
                                if data.get("req_id") == REQ_BUY:
                                    await self.on_buy(data)
                            elif msg_type == "proposal_open_contract":
                                await self.on_poc(data)
                    finally:
                        pinger.cancel()
                print("[Child] Deriv connection closed")
            except asyncio.CancelledError:
                raise
            except Exception as error:
                print(f"[Child] Deriv connection error: {str(error)}")
            finally:
                self.ws = None
            print(f"[Child] reconnecting to Deriv in {delay}s")
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
                    self.parent_connected = True
                    delay = 2
                    if not self.busy:
                        self.show_idle()
                    async for raw in ws:
                        await self.on_parent_message(loads(raw))
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            finally:
                was_connected = self.parent_connected
                self.parent_connected = False
            if was_connected:
                self.set_state(f"[Parent Offline] Reconnecting in {delay}s...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 15)

    async def run(self):
        self.stop = asyncio.Event()
        print(
            f"CHILD {NAME} | fresh session | STAKE_MODE={STAKE_MODE} | "
            f"TP=+{TARGET_PL:.2f} USD | ACCOUNT={CHILD_ACCOUNT_TYPE}"
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
