import asyncio
import json
import time
import os
from collections import defaultdict, deque

import requests
import websockets


# =========================
# Telegram - Environment Variables
# =========================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "6311722848")


# =========================
# MEXC Futures
# =========================

WS_URL = "wss://contract.mexc.com/edge"
CONTRACT_INFO_URL = "https://contract.mexc.com/api/v1/contract/detail"
TICKER_URL = "https://contract.mexc.com/api/v1/contract/ticker"


# =========================
# Entry / Exit Range Settings
# =========================

LOOKBACK_CANDLES = 10

MIN_RANGE_POINTS = 20
MAX_RANGE_POINTS = 80

MIN_ENTRY_EXIT_DISTANCE_POINTS = 20
MIN_ENTRY_EXIT_DISTANCE_PERCENT = 0.25

ENTRY_ZONE_PERCENT = 0.20

MAX_TREND_RATIO = 0.25

MIN_DIRECTION_CHANGES = 4

MIN_TOUCHES_HIGH = 2
MIN_TOUCHES_LOW = 2

MAX_GREEN_RED_DIFFERENCE = 4

MIN_RANGE_USDT_VOLUME = 1.0

MIN_24H_USDT_VOLUME = 200_000
MAX_24H_USDT_VOLUME = 6_000_000

MIN_NEAR_LIQUIDITY_USDT = 5

MAX_SPREAD_POINTS = 25

COOLDOWN_SECONDS = 120

MAX_SYMBOLS_TO_WATCH = 300

QUOTE = "_USDT"
MAX_PRICE_TO_WATCH = 10.0
DEFAULT_POINT_SIZE = 0.000001


# =========================
# Data Storage
# =========================

current_candle = {}
candles = defaultdict(lambda: deque(maxlen=50))
depth = {}
symbol_meta = {}
ticker_24h = {}
last_alert = defaultdict(float)


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram token or chat id is missing.")
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML"
        }

        r = requests.post(url, data=data, timeout=10)
        print("Telegram:", r.text[:200])

    except Exception as e:
        print("Telegram error:", e)


def fetch_24h_tickers():
    global ticker_24h

    print("Fetching 24h tickers...")

    try:
        r = requests.get(TICKER_URL, timeout=20)
        data = r.json()
    except Exception as e:
        print("Ticker fetch error:", e)
        ticker_24h = {}
        return

    rows = data.get("data", [])

    if isinstance(rows, dict):
        rows = [rows]

    result = {}

    for item in rows:
        symbol = item.get("symbol")

        if not symbol:
            continue

        try:
            amount24 = float(item.get("amount24", 0))
        except Exception:
            amount24 = 0.0

        result[symbol] = {
            "amount24": amount24,
            "lastPrice": item.get("lastPrice"),
            "bid1": item.get("bid1"),
            "ask1": item.get("ask1"),
        }

    ticker_24h = result
    print("24h tickers loaded:", len(ticker_24h))


def fetch_contracts():
    print("Fetching MEXC Futures contracts...")

    fetch_24h_tickers()

    try:
        r = requests.get(CONTRACT_INFO_URL, timeout=20)
        data = r.json()
    except Exception as e:
        print("Contract fetch error:", e)
        return []

    contracts = data.get("data", [])

    if isinstance(contracts, dict):
        contracts = [contracts]

    results = []

    for c in contracts:
        symbol = c.get("symbol")

        if not symbol:
            continue

        if not symbol.endswith(QUOTE):
            continue

        state = c.get("state")

        if state not in [0, "0", None]:
            continue

        try:
            price_unit = float(c.get("priceUnit") or DEFAULT_POINT_SIZE)
        except Exception:
            price_unit = DEFAULT_POINT_SIZE

        try:
            max_leverage = int(float(c.get("maxLeverage") or 1))
        except Exception:
            max_leverage = 1

        last_price = c.get("lastPrice") or c.get("price") or 0

        if not last_price and symbol in ticker_24h:
            last_price = ticker_24h[symbol].get("lastPrice") or 0

        try:
            last_price = float(last_price)
        except Exception:
            last_price = 0.0

        amount24 = ticker_24h.get(symbol, {}).get("amount24", 0.0)

        if amount24 < MIN_24H_USDT_VOLUME:
            continue

        if amount24 > MAX_24H_USDT_VOLUME:
            continue

        if max_leverage < 50:
            continue

        if last_price > MAX_PRICE_TO_WATCH and last_price > 0:
            continue

        symbol_meta[symbol] = {
            "price_unit": price_unit,
            "max_leverage": max_leverage,
            "last_price": last_price,
            "amount24": amount24
        }

        results.append(symbol)

    results = sorted(
        results,
        key=lambda s: symbol_meta.get(s, {}).get("amount24", 0)
    )

    return results[:MAX_SYMBOLS_TO_WATCH]


def get_point_size(symbol):
    meta_point = symbol_meta.get(symbol, {}).get("price_unit", None)

    try:
        if meta_point and float(meta_point) > 0:
            return float(meta_point)
    except Exception:
        pass

    last_price = symbol_meta.get(symbol, {}).get("last_price", 0)

    try:
        last_price = float(last_price)
    except Exception:
        last_price = 0

    if 0.001 <= last_price < 0.01:
        return 0.000001

    if 0.0001 <= last_price < 0.001:
        return 0.000001

    return DEFAULT_POINT_SIZE


def minute_bucket(ts):
    return int(ts // 60) * 60


def update_1m_candle(symbol, price, usdt_volume=0.0):
    now = time.time()
    bucket = minute_bucket(now)

    c = current_candle.get(symbol)

    if c is None:
        current_candle[symbol] = {
            "start": bucket,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": usdt_volume,
        }
        return

    if c["start"] == bucket:
        c["high"] = max(c["high"], price)
        c["low"] = min(c["low"], price)
        c["close"] = price
        c["volume"] += usdt_volume
        return

    candles[symbol].append(c)

    current_candle[symbol] = {
        "start": bucket,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": usdt_volume,
    }


def calc_near_liquidity_usdt(symbol, current_price):
    d = depth.get(symbol)

    if not d:
        return 0.0, 0.0, None, None

    bids = d.get("bids", [])
    asks = d.get("asks", [])

    bid_liq = 0.0
    ask_liq = 0.0
    best_bid = None
    best_ask = None

    if bids:
        try:
            best_bid = float(bids[0][0])
        except Exception:
            best_bid = None

        for row in bids[:10]:
            try:
                p = float(row[0])
                q = float(row[1])

                if current_price > 0:
                    distance = abs(current_price - p) / current_price

                    if distance <= 0.005:
                        bid_liq += p * q

            except Exception:
                pass

    if asks:
        try:
            best_ask = float(asks[0][0])
        except Exception:
            best_ask = None

        for row in asks[:10]:
            try:
                p = float(row[0])
                q = float(row[1])

                if current_price > 0:
                    distance = abs(p - current_price) / current_price

                    if distance <= 0.005:
                        ask_liq += p * q

            except Exception:
                pass

    return bid_liq, ask_liq, best_bid, best_ask


def candle_direction(c, point_size):
    diff = c["close"] - c["open"]
    points = int(round(diff / point_size))

    if points > 0:
        return 1

    if points < 0:
        return -1

    return 0


def count_direction_changes_from_candles(selected, point_size):
    dirs = []

    for c in selected:
        d = candle_direction(c, point_size)

        if d != 0:
            dirs.append(d)

    if len(dirs) < 2:
        return 0

    changes = 0
    last = dirs[0]

    for d in dirs[1:]:
        if d != last:
            changes += 1
            last = d

    return changes


def detect_entry_exit_range(symbol, point_size):
    c_list = list(candles[symbol])

    if symbol in current_candle:
        c_list.append(current_candle[symbol])

    if len(c_list) < LOOKBACK_CANDLES:
        return None

    selected = c_list[-LOOKBACK_CANDLES:]

    highs = [c["high"] for c in selected]
    lows = [c["low"] for c in selected]
    closes = [c["close"] for c in selected]

    high = max(highs)
    low = min(lows)
    current = closes[-1]

    range_points = int(round((high - low) / point_size))

    if range_points < MIN_RANGE_POINTS:
        return None

    if range_points > MAX_RANGE_POINTS:
        return None

    entry_exit_distance_points = range_points

    if entry_exit_distance_points < MIN_ENTRY_EXIT_DISTANCE_POINTS:
        return None

    if low <= 0:
        return None

    entry_exit_percent = (high - low) / low * 100

    if entry_exit_percent < MIN_ENTRY_EXIT_DISTANCE_PERCENT:
        return None

    trend_points = abs(int(round((closes[-1] - closes[0]) / point_size)))
    trend_ratio = trend_points / max(range_points, 1)

    if trend_ratio > MAX_TREND_RATIO:
        return None

    green = 0
    red = 0
    flat = 0

    for c in selected:
        d = candle_direction(c, point_size)

        if d > 0:
            green += 1
        elif d < 0:
            red += 1
        else:
            flat += 1

    if green == 0 or red == 0:
        return None

    if abs(green - red) > MAX_GREEN_RED_DIFFERENCE:
        return None

    direction_changes = count_direction_changes_from_candles(selected, point_size)

    if direction_changes < MIN_DIRECTION_CHANGES:
        return None

    zone_points = max(2, int(range_points * ENTRY_ZONE_PERCENT))
    zone_size = zone_points * point_size

    buy_zone = low + zone_size
    sell_zone = high - zone_size

    touches_high = 0
    touches_low = 0

    for c in selected:
        if c["high"] >= sell_zone:
            touches_high += 1

        if c["low"] <= buy_zone:
            touches_low += 1

    if touches_high < MIN_TOUCHES_HIGH:
        return None

    if touches_low < MIN_TOUCHES_LOW:
        return None

    total_volume = sum(c["volume"] for c in selected)

    if total_volume < MIN_RANGE_USDT_VOLUME:
        return None

    signal_type = None

    if current <= buy_zone:
        signal_type = "NEAR BUY ZONE"
        distance_to_exit_points = int(round((high - current) / point_size))
        distance_to_exit_percent = (high - current) / current * 100

    elif current >= sell_zone:
        signal_type = "NEAR SELL ZONE"
        distance_to_exit_points = int(round((current - low) / point_size))
        distance_to_exit_percent = (current - low) / low * 100

    else:
        return None

    if distance_to_exit_points < MIN_ENTRY_EXIT_DISTANCE_POINTS:
        return None

    return {
        "signal_type": signal_type,
        "current": current,
        "high": high,
        "low": low,
        "buy_zone": buy_zone,
        "sell_zone": sell_zone,
        "range_points": range_points,
        "entry_exit_distance_points": entry_exit_distance_points,
        "entry_exit_percent": entry_exit_percent,
        "distance_to_exit_points": distance_to_exit_points,
        "distance_to_exit_percent": distance_to_exit_percent,
        "trend_points": trend_points,
        "trend_ratio": trend_ratio,
        "direction_changes": direction_changes,
        "touches_high": touches_high,
        "touches_low": touches_low,
        "green": green,
        "red": red,
        "flat": flat,
        "volume": total_volume,
        "candles": len(selected),
    }


def analyze_symbol(symbol):
    now = time.time()

    if now - last_alert[symbol] < COOLDOWN_SECONDS:
        return

    if symbol not in current_candle:
        return

    current_price = current_candle[symbol]["close"]

    if current_price <= 0:
        return

    amount24 = symbol_meta.get(symbol, {}).get("amount24", 0.0)

    if amount24 < MIN_24H_USDT_VOLUME:
        return

    if amount24 > MAX_24H_USDT_VOLUME:
        return

    point_size = get_point_size(symbol)

    if point_size <= 0:
        point_size = DEFAULT_POINT_SIZE

    pattern = detect_entry_exit_range(symbol, point_size)

    if not pattern:
        return

    bid_liq, ask_liq, best_bid, best_ask = calc_near_liquidity_usdt(
        symbol,
        current_price
    )

    if bid_liq < MIN_NEAR_LIQUIDITY_USDT:
        return

    if ask_liq < MIN_NEAR_LIQUIDITY_USDT:
        return

    spread_points = "Unknown"

    if best_bid and best_ask:
        spread_points = int(round((best_ask - best_bid) / point_size))

        if spread_points > MAX_SPREAD_POINTS:
            return

    msg = (
        "🚨 <b>Entry / Exit Range Alert</b>\n\n"
        f"Pair: <b>{symbol.replace('_', '')}</b>\n"
        f"Signal: <b>{pattern['signal_type']}</b>\n"
        f"24h USDT volume: <b>{round(amount24, 2)}</b>\n"
        f"1m candles checked: <b>{pattern['candles']}</b>\n\n"
        f"Current price: <b>{round(pattern['current'], 10)}</b>\n"
        f"Buy zone: <b>{round(pattern['buy_zone'], 10)}</b>\n"
        f"Sell zone: <b>{round(pattern['sell_zone'], 10)}</b>\n"
        f"Range low: {round(pattern['low'], 10)}\n"
        f"Range high: {round(pattern['high'], 10)}\n\n"
        f"Entry/exit distance: <b>{pattern['entry_exit_distance_points']} points</b>\n"
        f"Entry/exit percent: <b>{round(pattern['entry_exit_percent'], 3)}%</b>\n"
        f"Distance to opposite side: <b>{pattern['distance_to_exit_points']} points</b>\n"
        f"Distance to opposite side percent: <b>{round(pattern['distance_to_exit_percent'], 3)}%</b>\n\n"
        f"Green candles: {pattern['green']}\n"
        f"Red candles: {pattern['red']}\n"
        f"Direction changes: {pattern['direction_changes']}\n"
        f"High touches: {pattern['touches_high']}\n"
        f"Low touches: {pattern['touches_low']}\n"
        f"Trend ratio: {round(pattern['trend_ratio'], 2)}\n"
        f"Range volume: {round(pattern['volume'], 4)} USDT\n"
        f"Near bid liquidity: {round(bid_liq, 4)} USDT\n"
        f"Near ask liquidity: {round(ask_liq, 4)} USDT\n"
        f"Spread: {spread_points} points\n\n"
        "Status: Stable range with clear distance between entry and exit zones.\n\n"
        "⚠️ This is only an alert, not financial advice."
    )

    send_telegram(msg)
    print(msg)

    last_alert[symbol] = now


async def subscribe_symbols(ws, symbols):
    for symbol in symbols:
        try:
            await ws.send(json.dumps({
                "method": "sub.deal",
                "param": {"symbol": symbol},
                "gzip": False
            }))

            await asyncio.sleep(0.03)

            await ws.send(json.dumps({
                "method": "sub.depth",
                "param": {"symbol": symbol},
                "gzip": False
            }))

            await asyncio.sleep(0.03)

        except Exception as e:
            print("Subscribe error:", symbol, e)


async def ping_loop(ws):
    while True:
        try:
            await ws.send(json.dumps({"method": "ping"}))
        except Exception:
            return

        await asyncio.sleep(15)


async def main():
    if not TELEGRAM_BOT_TOKEN:
        print("ERROR: TELEGRAM_BOT_TOKEN is missing.")
        print("Add TELEGRAM_BOT_TOKEN in hosting Environment Variables.")
        return

    if not TELEGRAM_CHAT_ID:
        print("ERROR: TELEGRAM_CHAT_ID is missing.")
        print("Add TELEGRAM_CHAT_ID in hosting Environment Variables.")
        return

    symbols = fetch_contracts()

    print("Symbols count:", len(symbols))
    print(symbols[:50])

    if not symbols:
        send_telegram("❌ No symbols found after filters")
        return

    send_telegram(
        "✅ MEXC Entry/Exit Range bot started\n"
        f"Symbols count: {len(symbols)}\n"
        f"24h volume filter: {MIN_24H_USDT_VOLUME} - {MAX_24H_USDT_VOLUME} USDT\n"
        f"Range filter: {MIN_RANGE_POINTS} - {MAX_RANGE_POINTS} points\n"
        f"Min entry/exit distance: {MIN_ENTRY_EXIT_DISTANCE_POINTS} points\n"
        "Pattern: stable movement with clear distance between buy and sell zones."
    )

    while True:
        try:
            print("Connecting to WebSocket...")

            async with websockets.connect(
                WS_URL,
                ping_interval=None,
                close_timeout=10,
                max_queue=1024
            ) as ws:
                print("Connected ✅")

                await subscribe_symbols(ws, symbols)

                ping_task = asyncio.create_task(ping_loop(ws))

                try:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        channel = msg.get("channel")
                        symbol = msg.get("symbol")

                        if channel == "push.depth" and symbol:
                            data = msg.get("data", {})

                            depth[symbol] = {
                                "bids": data.get("bids", []),
                                "asks": data.get("asks", [])
                            }

                            analyze_symbol(symbol)

                        elif channel == "push.deal" and symbol:
                            data = msg.get("data", [])

                            if isinstance(data, dict):
                                data = [data]

                            for item in data:
                                try:
                                    price = float(item.get("p"))
                                    qty = float(item.get("v") or 0)
                                    usdt_volume = price * qty

                                    update_1m_candle(
                                        symbol,
                                        price,
                                        usdt_volume
                                    )

                                except Exception:
                                    pass

                            analyze_symbol(symbol)

                finally:
                    ping_task.cancel()

        except Exception as e:
            print("Connection error:", e)
            print("Retrying after 5 seconds...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
