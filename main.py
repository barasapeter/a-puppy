import asyncio
import json
import traceback
from datetime import datetime, timezone

import websockets

from auth import get_ws_url
from signal_core import infer_market_bias

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


def process_tick(tick, **kwargs):
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
        f"{time_text} [{kwargs.get("reconnect_attempts")}] | {symbol} | {price:.2f} | "
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
            "contract_type": infer_market_bias(
                stats=get_stats(symbol), current_streak_direction=direction
            ),
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
    """Handles receiving messages and order routing over the active channel."""
    wins = 0
    losses = 0
    unresolved = 0
    net_pl = 0.0
    current_win_streak = 0
    current_loss_streak = 0
    longest_win_streak = 0
    longest_loss_streak = 0
    resolved_contracts = set()

    base_stake = 1.0
    current_stake = base_stake
    martingale_multiplier = 2.0
    max_martingale_steps = 10
    martingale_step = 0
    max_stake = 100.0

    max_pl = 0
    min_pl = 0

    target_reached_count = 0
    basket_amt = 0

    async for message in ws:
        data = json.loads(message)

        if "error" in data:
            print(
                f"\n[!] API error response: {data['error'].get('message', data['error'])}"
            )
            continue

        msg_type = data.get("msg_type")

        if msg_type == "tick":
            signal = process_tick(
                data["tick"],
                reconnect_attempts=reconnect_attempts,
            )

            if signal:
                stake = current_stake

                buy_payload = {
                    "buy": "1",
                    "price": float(stake),
                    "parameters": {
                        "amount": float(stake),
                        "basis": "stake",
                        "contract_type": signal["contract_type"],
                        "currency": "USD",
                        "duration": 1,
                        "duration_unit": "t",
                        "underlying_symbol": signal["symbol"],
                    },
                    "req_id": 5005,
                }

                if signal["contract_type"] == "SKIP":
                    print("--> [SKIP] Order skipped")

                else:
                    print(
                        f"--> [SENDING ORDER] Placing "
                        f"{signal['contract_type']} order on "
                        f"{signal['symbol']} | Stake: {stake:.2f} USD | "
                        f"Martingale step: {martingale_step}"
                    )

                    await ws.send(json.dumps(buy_payload))

        elif msg_type == "buy" and data.get("req_id") == 5005:
            buy_info = data.get("buy", {})
            contract_id = buy_info.get("contract_id")
            balance_after = buy_info.get("balance_after")

            if contract_id is None:
                print("[!] Buy response did not contain a contract ID.")
                continue

            balance_text = (
                f"{float(balance_after):.2f} USD"
                if balance_after is not None
                else "Unavailable"
            )

            print(
                f"\n[+] ORDER CONFIRMED | Contract ID: {contract_id} | "
                f"Balance: {balance_text}"
            )

            poc_payload = {
                "proposal_open_contract": 1,
                "contract_id": int(contract_id),
                "subscribe": 1,
                "req_id": 7007,
            }

            await ws.send(json.dumps(poc_payload))

        elif msg_type == "proposal_open_contract":
            poc = data.get("proposal_open_contract", {})

            if poc.get("is_sold"):
                c_id = poc.get("contract_id")

                if c_id is None or c_id in resolved_contracts:
                    continue

                resolved_contracts.add(c_id)

                status = poc.get("status")
                profit = float(poc.get("profit", 0.0))
                net_pl += profit

                if net_pl > max_pl:
                    max_pl = net_pl

                if net_pl < min_pl:
                    min_pl = net_pl

                if status == "won":
                    wins += 1
                    current_win_streak += 1
                    current_loss_streak = 0
                    longest_win_streak = max(longest_win_streak, current_win_streak)

                    current_stake = base_stake
                    martingale_step = 0

                    print(
                        f"[MARTINGALE] Win detected. "
                        f"Stake reset to {current_stake:.2f} USD."
                    )

                elif status == "lost":
                    losses += 1
                    current_loss_streak += 1
                    current_win_streak = 0
                    longest_loss_streak = max(longest_loss_streak, current_loss_streak)

                    if martingale_step < max_martingale_steps:
                        next_stake = current_stake * martingale_multiplier

                        if next_stake <= max_stake:
                            current_stake = next_stake
                            martingale_step += 1
                            print(
                                f"[MARTINGALE] Loss detected. "
                                f"Next stake: {current_stake:.2f} USD "
                                f"(step {martingale_step})."
                            )
                        else:
                            current_stake = base_stake
                            martingale_step = 0
                            print(
                                "[MARTINGALE] Stake cap would be exceeded. "
                                f"Resetting to {base_stake:.2f} USD."
                            )
                    else:
                        current_stake = base_stake
                        martingale_step = 0
                        print(
                            "[MARTINGALE] Maximum recovery steps reached. "
                            f"Resetting to {base_stake:.2f} USD."
                        )

                else:
                    unresolved += 1

                min_bankroll = (
                    base_stake
                    * (martingale_multiplier ** (longest_loss_streak + 1) - 1)
                    / (martingale_multiplier - 1)
                )
                win_rate = (
                    f"{(wins / (wins + losses) * 100 if wins + losses else 0):.0f}"
                )

                target_status = (
                    "⛔"
                    if net_pl < 15
                    else f"✅ [{target_reached_count}] [BSKT {basket_amt} USD]"
                )

                print("\n==========================================")
                print(f"   TRADE RESOLVED | ID: {c_id}    [URSVD {unresolved}]")
                print(f"   Result: {str(status).upper()}  [MXLS {longest_loss_streak}]")
                print(f"   Current P&L: {profit:+.2f} USD [MXPL {max_pl}]")
                print(f"   Win rate: " f"{win_rate}%      [MINPL {min_pl}]")
                print("==========================================")
                print(f"   Min bankroll: {min_bankroll} USD")
                print(f"   Net P&L: {net_pl:+.2f} USD     [TGTPL {target_status}]")
                print("==========================================\n")

                if net_pl >= 15:
                    basket_amt += net_pl
                    target_reached_count += 1
                    net_pl = 0
                    print("Net PL has been reset.")


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
            traceback.print_exc()

            print(f"Connection error occurred: {error}")
            RECONNECT_ATTEMPTS += 1
            print("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(stream())
    except KeyboardInterrupt:
        print("Stopped")
