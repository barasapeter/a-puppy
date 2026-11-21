"""
Deriv CHILD bot: connects to the parent bot's live feed and executes on its
own Deriv connection exactly what the parent executes.

Per order it can COPY the parent's final signal, INVERT it, or apply your own
rule using every metric the parent publishes (thresholds, virtual rates,
loss streak, guard evidence, ...). Stake can mirror the parent, be fixed, or
be a multiple of the parent's stake.

Run the parent first (it hosts the feed), then:  python child_bot.py
"""

import asyncio
import json
import os
import sys
import time

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
PARENT_URL = "ws://127.0.0.1:8765"  # must match PUBLISH_HOST / PUBLISH_PORT
PARENT_TOKEN = "change-me"  # must match PUBLISH_TOKEN ("" if the parent has none)

# Which Deriv account the child trades on. By default this calls the same
# auth.get_ws_url as the parent with this account_type. If your second account
# needs a different token, set the full URL in the DERIV_CHILD_WS_URL
# environment variable instead (it overrides get_ws_url).
CHILD_ACCOUNT_TYPE = "demo"
CHILD_WS_URL = os.environ.get("DERIV_CHILD_WS_URL")
CURRENCY = "USD"

# What to execute:
#   "copy"   = the parent's final signal
#   "invert" = the opposite of the parent's final signal
#   "custom" = whatever custom_decision() below returns
SIGNAL_MODE = "copy"

# How much to stake:
#   "mirror"     = same stake as the parent
#   "fixed"      = FIXED_STAKE on every trade
#   "multiplier" = parent stake x STAKE_MULTIPLIER
STAKE_MODE = "mirror"
FIXED_STAKE = 1.0
STAKE_MULTIPLIER = 1.0
MIN_STAKE = 1.0
MAX_STAKE = 1500.0

MAX_SESSION_LOSS = None  # e.g. 300.0: stop the child once net P&L <= -300
MAX_EVENT_AGE_S = 5.0  # ignore order events older than this (None = off);
#                       needs synced clocks if the parent is on another machine
BUSY_TIMEOUT_S = 30.0  # unresolved contract: query its status, then give up
PRINT_PARENT_RESULTS = True

REQ_BUY, REQ_POC, REQ_POC_QUERY = 6005, 6007, 6008
INVERSE_SIGNAL = {"PUT": "CALL", "CALL": "PUT"}


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


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class Child:
    def __init__(self):
        self.ws = None
        self.stop = None  # asyncio.Event, created in run()
        self.parent_state = None

        self.busy = False
        self.contract_id = None
        self.busy_since = 0.0
        self.query_tries = 0
        self.resolved = set()

        self.wins = self.losses = 0
        self.net_pl = 0.0

    # ----------------------------------------------------------------- utils
    async def send(self, payload):
        await self.ws.send(dumps(payload))

    def release(self):
        self.busy = False
        self.contract_id = None
        self.query_tries = 0

    def decide_signal(self, event):
        parent = event["contract"]
        if SIGNAL_MODE == "copy":
            return parent
        if SIGNAL_MODE == "invert":
            return INVERSE_SIGNAL.get(parent)
        return custom_decision(event, event.get("state") or {})

    def decide_stake(self, event):
        base = float(event["stake"])
        if STAKE_MODE == "fixed":
            stake = FIXED_STAKE
        elif STAKE_MODE == "multiplier":
            stake = base * STAKE_MULTIPLIER
        else:
            stake = base
        return round(min(MAX_STAKE, max(MIN_STAKE, stake)), 2)

    # ---------------------------------------------------------- parent feed
    async def on_parent_message(self, msg):
        kind = msg.get("type")
        if kind == "hello":
            self.parent_state = msg.get("state")
            print(f"[CHILD] parent snapshot received | mode {SIGNAL_MODE} | stake {STAKE_MODE}")
        elif kind == "order":
            self.parent_state = msg.get("state")
            await self.on_order(msg)
        elif kind == "result":
            self.parent_state = msg.get("state")
            if PRINT_PARENT_RESULTS:
                st = self.parent_state or {}
                print(
                    f"[PARENT] {str(msg.get('status')).upper()} "
                    f"{msg.get('profit', 0):+.2f} | net {st.get('net_pl', 0):+.2f} | "
                    f"{st.get('wins')}W {st.get('losses')}L | {st.get('streaks')}"
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

        signal = self.decide_signal(ev)
        if signal not in ("CALL", "PUT"):
            print(f"[CHILD] SKIP parent {ev['contract']} -> no trade ({SIGNAL_MODE})")
            return
        stake = self.decide_stake(ev)

        # Flag first, send second: no window for a duplicate order.
        self.busy = True
        self.contract_id = None
        self.busy_since = time.monotonic()
        self.query_tries = 0
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
            f"[CHILD] ORDER {signal} stake {stake:.2f} | parent {ev['contract']} "
            f"(orig {ev['original']}, inv {'YES' if ev['inverted'] else 'NO'}) | "
            f"{ev['direction']}-streak | mode {SIGNAL_MODE}"
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

        sub_id = (data.get("subscription") or {}).get("id")
        if sub_id:
            await self.send({"forget": sub_id})
        elif data.get("req_id") == REQ_POC_QUERY:
            await self.send({"forget_all": "proposal_open_contract"})

        status = poc.get("status")
        profit = _to_float(poc.get("profit")) or 0.0
        if status == "won":
            self.wins += 1
        elif status == "lost":
            self.losses += 1
        self.net_pl += profit

        decided = self.wins + self.losses
        rate = self.wins / decided * 100 if decided else 0
        print(
            f"[CHILD] {str(status).upper()} {profit:+.2f} | net {self.net_pl:+.2f} | "
            f"{self.wins}W {self.losses}L ({rate:.0f}%)"
        )
        if self.contract_id == cid or self.contract_id is None:
            self.release()

        if MAX_SESSION_LOSS is not None and self.net_pl <= -MAX_SESSION_LOSS:
            print(f"[CHILD] session loss limit reached ({self.net_pl:+.2f}). Stopping.")
            self.stop.set()

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
                    await ws.send(dumps({"token": PARENT_TOKEN}))
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
