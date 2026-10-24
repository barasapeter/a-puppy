import asyncio
import json
from datetime import datetime, timezone

import websockets

from auth import get_ws_url

SYMBOLS = ["1HZ100V"]

STREAK = 3
BIAS_WINDOW = 1
TRADE_STAKE = 1.0  # $1 stake configuration

previous_prices = {}
streaks = {}
active_windows = {}
session_stats = {}


def timestamp(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%H:%M:%S")


def get_stats(symbol):
    return session_stats.setdefault(
        symbol, {"continuation": 0, "reversal": 0, "neutral": 0}
    )


def print_stats(symbol):
    stats = get_stats(symbol)
    total = sum(stats.values())

    if total == 0:
        return

    continuation_pct = stats["continuation"] / total * 100
    reversal_pct = stats["reversal"] / total * 100
    neutral_pct = stats["neutral"] / total * 100

    print(
        f"SESSION BIAS | {symbol} | "
        f"Continuation: {continuation_pct:.1f}% "
        f"({stats['continuation']}) | "
        f"Reversal: {reversal_pct:.1f}% "
        f"({stats['reversal']}) | "
        f"Neutral: {neutral_pct:.1f}% "
        f"({stats['neutral']}) | "
        f"Completed: {total}"
    )


def update_windows(symbol, price, epoch):
    windows = active_windows.setdefault(symbol, [])
    remaining = []

    for window in windows:
        window["ticks_seen"] += 1

        if window["ticks_seen"] < BIAS_WINDOW:
            remaining.append(window)
            continue

        start_price = window["start_price"]
        direction = window["direction"]

        if price > start_price:
            outcome = "CONTINUATION" if direction == "UP" else "REVERSAL"
        elif price < start_price:
            outcome = "CONTINUATION" if direction == "DOWN" else "REVERSAL"
        else:
            outcome = "NEUTRAL"

        stats = get_stats(symbol)
        stats[outcome.lower()] += 1

        change = price - start_price

        print(
            f"\nBIAS RESULT | {timestamp(epoch)} | {symbol} | "
            f"Original: {direction} | "
            f"Start: {start_price} | End: {price} | "
            f"Change: {change:+.5f} | "
            f"Outcome: {outcome} | "
            f"Window: {BIAS_WINDOW} ticks"
        )

        print_stats(symbol)

    active_windows[symbol] = remaining


def process_tick(tick, reconnect_attempts):
    """Processes tick telemetry. Returns a signal dict if a trade is triggered."""
    symbol = tick["symbol"]
    price = float(tick["quote"])
    epoch = tick["epoch"]

    previous = previous_prices.get(symbol)
    previous_prices[symbol] = price

    time_text = timestamp(epoch)

    if previous is None:
        print(f"{time_text} | {symbol} | {price} | Waiting")
        return None

    update_windows(symbol, price, epoch)

    if price > previous:
        direction = "UP"
    elif price < previous:
        direction = "DOWN"
    else:
        state = streaks.setdefault(
            symbol, {"direction": None, "count": 0, "alerted": False}
        )
        state["direction"] = None
        state["count"] = 0
        state["alerted"] = False

        print(f"{time_text} | {symbol} | {price} | FLAT []")
        return None

    state = streaks.setdefault(
        symbol, {"direction": None, "count": 0, "alerted": False}
    )

    if state["direction"] == direction:
        state["count"] += 1
    else:
        state["direction"] = direction
        state["count"] = 1
        state["alerted"] = False

    bar = "[" + str(state["count"]) + "]"
    message = (
        f"{time_text} [{reconnect_attempts}] | {symbol} | {price:.2f} | "
        f"{direction:<4} {bar}"
    )

    trade_signal = None

    if state["count"] >= STREAK and not state["alerted"]:
        message += f" | STREAK {STREAK} REACHED"

        active_windows.setdefault(symbol, []).append(
            {"direction": direction, "start_price": price, "ticks_seen": 0}
        )

        state["alerted"] = True
        message += f" | Monitoring next {BIAS_WINDOW} ticks -> TRIGGERING PUT"

        # Package trade payload information to hand over to async handler
        trade_signal = {
            "symbol": symbol,
            "contract_type": "PUT",
            "amount": TRADE_STAKE,
            "current_quote": price,
        }

    print(message)
    return trade_signal


async def send_ping(ws):
    """Sends app-level heartbeats every 20s to satisfy the Deriv balancer."""
    try:
        while True:
            await asyncio.sleep(20)
            await ws.send(json.dumps({"ping": 1}))
    except (asyncio.CancelledError, websockets.ConnectionClosed):
        pass


async def read_stream(ws, reconnect_attempts):
    """Handles receiving messages and handles order routing over the active channel."""
    async for message in ws:
        data = json.loads(message)

        if "error" in data:
            print(
                f"\n[!] API error response: {data['error'].get('message', data['error'])}"
            )
            continue

        msg_type = data.get("msg_type")

        # 1. Handle incoming continuous stream data
        if msg_type == "tick":
            signal = process_tick(data["tick"], reconnect_attempts)

            # If a streak signal returned instructions, execute immediately!
            if signal:
                buy_payload = {
                    "buy": "1",  # "1" tells the server we are passing parameters inline
                    "price": float(
                        signal["amount"]
                    ),  # Top-level max purchase price (stake amount)
                    "parameters": {
                        "amount": float(signal["amount"]),
                        "basis": "stake",
                        "contract_type": signal["contract_type"],
                        "currency": "USD",
                        "duration": 1,
                        "duration_unit": "t",
                        "underlying_symbol": signal[
                            "symbol"
                        ],  # FIXED: Must be 'underlying_symbol', not 'symbol'
                    },
                    "req_id": 5005,
                }
                print(
                    f"--> [SENDING ORDER] Placed {signal['contract_type']} order on {signal['symbol']}..."
                )
                await ws.send(json.dumps(buy_payload))

        # 2. Handle specific async contract purchase callbacks
        elif msg_type == "buy" and data.get("req_id") == 5005:
            buy_info = data.get("buy", {})
            print(
                f"\n[+] ORDER CONFIRMED | Contract ID: {buy_info.get('contract_id')} | "
                f"Balance Remaining: {buy_info.get('balance_after'):.2f} USD\n"
            )


async def stream():
    ACCOUNT_TYPE = "demo"
    RECONNECT_ATTEMPTS = 0

    while True:
        try:
            print("Fetching clean WebSocket URL...")
            URL = get_ws_url(account_type=ACCOUNT_TYPE)

            async with websockets.connect(
                URL, open_timeout=15, ping_interval=30, ping_timeout=10
            ) as ws:
                print("Connected to Deriv")
                print(
                    f"STREAK={STREAK} | BIAS_WINDOW={BIAS_WINDOW} | STAKE=${TRADE_STAKE}"
                )

                previous_prices.clear()
                streaks.clear()
                active_windows.clear()

                # Start the tick telemetry registration
                await ws.send(
                    json.dumps({"ticks": SYMBOLS, "subscribe": 1, "req_id": 1})
                )

                # Process telemetry loop and heartbeats in parallel
                await asyncio.gather(read_stream(ws, RECONNECT_ATTEMPTS), send_ping(ws))

        except asyncio.CancelledError:
            raise
        except Exception as error:
            print(f"Connection error occurred: {error}")
            RECONNECT_ATTEMPTS += 1
            print("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(stream())
    except KeyboardInterrupt:
        print("Stopped")
