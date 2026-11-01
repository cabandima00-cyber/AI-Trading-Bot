import time
import sqlite3
from collections import defaultdict, deque
import socketio
import threading
import os
import queue

SUPABASE_QUEUE = queue.Queue(maxsize=50000)
import re
import requests
import telebot
import pandas as pd
from supabase import create_client
SUPABASE_URL = os.getenv("SUPABASE_URL")
from urllib.parse import urlparse

print(
    "Supabase URL path:",
    repr(urlparse(SUPABASE_URL).path),
    flush=True
)
print("Supabase URL configured:", bool(SUPABASE_URL), flush=True)
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")
def get_candle_timestamp(row):
    try:
        # Binance
        if "close_time" in row.index:
            value = row["close_time"]

            if pd.notna(value):
                return int(value) // 1000

        # Twelve Data
        if "datetime" in row.index:
            value = pd.to_datetime(
                row["datetime"],
                utc=True
            )
            return int(value.timestamp())

        # BinoDex OTC
        if "time" in row.index:
            value = pd.to_datetime(
                row["time"],
                utc=True
            )
            return int(value.timestamp())

        return None

    except Exception as e:
        print("Candle timestamp error:", e)
        return None

supabase = None

try:
    if SUPABASE_URL and SUPABASE_SECRET_KEY:
        supabase = create_client(
            SUPABASE_URL,
            SUPABASE_SECRET_KEY
        )
        print("Supabase connected successfully!")
    else:
        print("Supabase credentials not found!")

except Exception as error:
    print("Supabase connection error:", error)
OTC_TIMEFRAMES = {
    "15sec": 15,
    "30sec": 30,
    "1min": 60,
    "5min": 300,
    "15min": 900,
    "30min": 1800,
    "1h": 3600,
    "4h": 14400,
    "1day": 86400,
    "1week": 604800
}

def init_otc_database():
    conn = sqlite3.connect("otc_history.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS otc_ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            price REAL NOT NULL
        )
    """)

    conn.commit()
    conn.close()


init_otc_database()
def init_otc_database():
    conn = sqlite3.connect("otc_history.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS otc_ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            price REAL NOT NULL
        )
    """)

    conn.commit()
    conn.close()


init_otc_database()


def init_backtest_db():
    try:
        with sqlite3.connect("backtest.db") as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS backtest_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    exit_price REAL,
                    stop_loss REAL,
                    take_profit REAL,
                    result TEXT DEFAULT 'PENDING',
                    profit_percent REAL DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    closed_at INTEGER
                )
            """)

            conn.commit()

        print("Backtesting database initialized")

    except Exception as e:
        print("Backtesting DB Error:", e)


init_backtest_db()
def save_backtest_trade(
    symbol,
    interval,
    direction,
    entry_price,
    stop_loss=None,
    take_profit=None
):
    try:
        import time

        with sqlite3.connect("backtest.db") as conn:

            existing = conn.execute("""
                SELECT id
                FROM backtest_trades
                WHERE symbol = ?
                AND interval = ?
                AND result = 'PENDING'
                LIMIT 1
            """, (symbol, interval)).fetchone()

            if existing:
                print(
                    f"Backtest duplicate skipped: "
                    f"{symbol} {interval}"
                )
                return False

            conn.execute("""
                INSERT INTO backtest_trades (
                    symbol,
                    interval,
                    direction,
                    entry_price,
                    stop_loss,
                    take_profit,
                    result,
                    profit_percent,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, 'PENDING', 0, ?)
            """, (
                symbol,
                interval,
                direction,
                float(entry_price),
                float(stop_loss) if stop_loss is not None else None,
                float(take_profit) if take_profit is not None else None,
                int(time.time())
            ))

        return True

    except Exception as e:
        print("Backtest Save Error:", e)
        return False

def get_otc_database_count():
    with sqlite3.connect("otc_history.db") as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM otc_ticks")
        return cursor.fetchone()[0]


def load_otc_history():
    OTC_TICKS.clear()

    # Сначала загружаем историю из Supabase
    if supabase is not None:
        try:
            offset = 0
            total = 0

            while True:
                response = (
                    supabase.schema("public").table("otc_ticks")
                    .select("symbol,timestamp,price")
                    .order("timestamp")
                    .range(offset, offset + 999)
                    .execute()
                )

                rows = response.data or []

                for row in rows:
                    symbol = row["symbol"].upper()

                    OTC_TICKS[symbol].append({
                        "timestamp": int(row["timestamp"]),
                        "price": float(row["price"])
                    })

                total += len(rows)

                if len(rows) < 1000:
                    break

                offset += 1000

            if total > 0:
                print(f"Supabase history loaded: {total} ticks")
                return

        except Exception as error:
            print("Supabase history loading error:", repr(error))

    # Если облако недоступно или пустое — загружаем SQLite
    try:
        with sqlite3.connect("otc_history.db") as conn:
            rows = conn.execute("""
                SELECT symbol, timestamp, price
                FROM otc_ticks
                ORDER BY timestamp ASC
            """).fetchall()

        for symbol, timestamp, price in rows:
            OTC_TICKS[symbol].append({
                "timestamp": timestamp,
                "price": price
            })

        print(f"SQLite history loaded: {len(rows)} ticks")

    except sqlite3.Error as error:
        print("SQLite history loading error:", error)

def save_otc_tick(data):
    try:
        symbol = data.get("symbol", "").upper()
        price = float(data.get("price"))
        timestamp = int(data.get("timestamp"))

        if not symbol or price <= 0:
            return

        tick = {
            "symbol": symbol,
            "timestamp": timestamp,
            "price": price
        }

        OTC_TICKS[symbol].append({
            "timestamp": timestamp,
            "price": price
        })

        with sqlite3.connect("otc_history.db") as conn:
            conn.execute(
                """
                INSERT INTO otc_ticks (symbol, timestamp, price)
                VALUES (?, ?, ?)
                """,
                (symbol, timestamp, price)
            )

        if supabase is not None:
            try:
                SUPABASE_QUEUE.put_nowait(tick)
            except queue.Full:
                print("Supabase queue is full!")

    except (TypeError, ValueError, AttributeError) as error:
        print("OTC tick error:", error)

    except sqlite3.Error as error:
        print("OTC database error:", error)
def supabase_worker():
    while True:
        if supabase is None:
            time.sleep(5)
            continue

        batch = []

        try:
            first_tick = SUPABASE_QUEUE.get(timeout=5)
            batch.append(first_tick)

            while len(batch) < 100:
                try:
                    batch.append(SUPABASE_QUEUE.get_nowait())
                except queue.Empty:
                    break

            # Повторяем отправку, пока пакет не сохранится
            while True:
                try:
                    supabase.table("otc_ticks").insert(batch).execute()
                    print(f"Supabase: saved {len(batch)} ticks")
                    break

                except Exception as error:
                    print("Supabase retry error:", repr(error))
                    time.sleep(10)

            for _ in batch:
                SUPABASE_QUEUE.task_done()

        except queue.Empty:
            continue

        except Exception as error:
            print("Supabase worker error:", repr(error))
            time.sleep(5)
def run_otc_stream():
    import time

    sio = socketio.Client(reconnection=True)

    otc_pairs = [
        "EUR/USD",
        "GBP/USD",
        "AUD/USD",
        "USD/JPY",
        "EUR/JPY",
        "EUR/GBP",
        "GBP/JPY",
        "AUD/JPY",
        "USD/CAD",
        "USD/CHF",
        "NZD/USD",
        "EUR/AUD",
        "GBP/AUD",
        "EUR/CHF",
        "GBP/CHF"
    ]

    @sio.on("graphic", namespace="/graphic")
    def on_graphic(data):
        if isinstance(data, dict):
            save_otc_tick(data)

    @sio.on("connect", namespace="/graphic")
    def on_connect():
        for pair in otc_pairs:
            sio.emit(
                "graphic",
                {
                    "method": "SUBSCRIBE",
                    "symbol": pair,
                    "interval": "1m",
                    "otc": True
                },
                namespace="/graphic"
            )

        print("BinoDex OTC stream connected!")

    while True:
        try:
            if not sio.connected:
                sio.connect(
                    "https://api-coins.binodex.app",
                    socketio_path="market",
                    transports=["websocket"],
                    namespaces=["/graphic"],
                    wait_timeout=10
                )

            sio.wait()

        except Exception as error:
            print("BinoDex stream error:", error)

        finally:
            if sio.connected:
                sio.disconnect()

        time.sleep(5)

def build_otc_candles(symbol, interval="1min"):
    seconds = OTC_TIMEFRAMES.get(interval)

    if seconds is None:
        raise ValueError("Неподдерживаемый таймфрейм")

    ticks = list(OTC_TICKS.get(symbol.upper(), []))

    if not ticks:
        raise ValueError("Пока нет котировок для этой пары")

    df = pd.DataFrame(ticks)

    df["time"] = pd.to_datetime(
        df["timestamp"], unit="ms", utc=True
    )

    df["candle_time"] = pd.to_datetime(
        (df["timestamp"] // (seconds * 1000)) * (seconds * 1000),
        unit="ms",
        utc=True
    )

    candles = df.groupby("candle_time").agg(
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        volume=("price", "count")
    ).reset_index()

    candles.rename(columns={"candle_time": "time"}, inplace=True)

    return candles

OTC_TICKS = defaultdict(lambda: deque(maxlen=50000))

from ta.momentum import RSIIndicator
from ta.trend import MACD, ADXIndicator

TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
TWELVE_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

if not TOKEN:
    raise ValueError("Не найден BOT_TOKEN")

bot = telebot.TeleBot(TOKEN)

BINANCE_API = "https://data-api.binance.vision/api/v3/klines"
TWELVE_API = "https://api.twelvedata.com/time_series"

FOREX_PAIRS = {
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD",
    "USDCAD", "USDCHF", "NZDUSD", "EURJPY",
    "GBPJPY", "EURGBP"
}


def get_forex_data(symbol, interval="1h"):
    if not TWELVE_API_KEY:
        raise ValueError("Не найден TWELVE_DATA_API_KEY в GitHub Secrets")

    forex_symbol = symbol[:3] + "/" + symbol[3:]

    time.sleep(8)
    response = requests.get(
        TWELVE_API,
        params={
            "symbol": forex_symbol,
            "interval": interval,
            "outputsize": 200,
            "apikey": TWELVE_API_KEY
        },
        timeout=20
    )

    response.raise_for_status()
    data = response.json()

    if "values" not in data:
        raise ValueError(f"Ошибка Twelve Data: {data}")

    df = pd.DataFrame(data["values"])

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col])

    df = df.iloc[::-1].reset_index(drop=True)

    return df


def get_binodex_data(symbol, interval="1h"):
    api_key = os.getenv("OTC_API_KEY")

    if not api_key:
        raise ValueError("Не найден OTC_API_KEY в GitHub Secrets")

    timeframe_map = {
        "1min": 60,
        "5min": 300,
        "15min": 900,
        "30min": 1800,
        "1h": 3600,
        "4h": 14400,
        "1day": 86400
    }

    tf = timeframe_map.get(interval)

    if tf is None:
        raise ValueError("Этот таймфрейм не поддерживается BinoDex OTC")

    response = requests.get(
        "https://otcharts.com/v1/candles",
        headers={
            "Authorization": f"Bearer {api_key}",
            "User-Agent": "AI-Market-Signal-Bot/1.0"
        },
        params={
            "venue": "binodex",
            "symbol": symbol,
            "tf": tf,
            "limit": 200
        },
        timeout=20
    )

    response.raise_for_status()
    data = response.json()

    candles = data.get("candles", [])

    if len(candles) < 60:
        raise ValueError("Недостаточно OTC-свечей для анализа")

    df = pd.DataFrame(candles)

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    df["time"] = pd.to_datetime(
        df["time"],
        unit="s",
        utc=True
    )

    df = df.dropna(subset=["open", "high", "low", "close"])
    df = df.reset_index(drop=True)

    if len(df) < 60:
        raise ValueError("Недостаточно корректных OTC-свечей")

    return df


def get_crypto_data(symbol, interval="1h"):
    interval_map = {
        "1min": "1m",
        "5min": "5m",
        "15min": "15m",
        "30min": "30m",
        "1h": "1h",
        "4h": "4h",
        "1day": "1d",
        "1week": "1w"
    }

    binance_interval = interval_map.get(interval)

    if not binance_interval:
        raise ValueError("Неподдерживаемый таймфрейм Binance")

    response = requests.get(
        "https://data-api.binance.vision/api/v3/klines",
        params={
            "symbol": symbol,
            "interval": binance_interval,
            "limit": 200
        },
        timeout=20
    )

    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame(data, columns=[
        "time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col])

    return df


def analyze_smc(df, swing=3):
    """
    SMC structure analysis.
    Uses closed candles only.
    """

    result = {
        "structure": "NEUTRAL",
        "bos": "NONE",
        "choch": "NONE",
        "sweep": "NONE",
        "direction": "NEUTRAL",
        "high_level": None,
        "low_level": None
    }

    if df is None or len(df) < 2 * swing + 10:
        return result

    data = df.iloc[:-1].copy()

    highs = pd.to_numeric(data["high"], errors="coerce")
    lows = pd.to_numeric(data["low"], errors="coerce")
    closes = pd.to_numeric(data["close"], errors="coerce")

    if highs.isna().any() or lows.isna().any() or closes.isna().any():
        return result

    high_pivots = []
    low_pivots = []

    for i in range(swing, len(data) - swing):

        high_window = highs.iloc[i-swing:i+swing+1]
        low_window = lows.iloc[i-swing:i+swing+1]

        if highs.iloc[i] == high_window.max():
            high_pivots.append((i, float(highs.iloc[i])))

        if lows.iloc[i] == low_window.min():
            low_pivots.append((i, float(lows.iloc[i])))

    if len(high_pivots) < 2 or len(low_pivots) < 2:
        return result

    previous_high = high_pivots[-2][1]
    latest_high = high_pivots[-1][1]

    previous_low = low_pivots[-2][1]
    latest_low = low_pivots[-1][1]

    result["high_level"] = latest_high
    result["low_level"] = latest_low

    if latest_high > previous_high and latest_low > previous_low:
        result["structure"] = "BULLISH"

    elif latest_high < previous_high and latest_low < previous_low:
        result["structure"] = "BEARISH"

    previous_close = float(closes.iloc[-2])
    current_close = float(closes.iloc[-1])

    current_high = float(highs.iloc[-1])
    current_low = float(lows.iloc[-1])

    bullish_break = (
        previous_close <= latest_high
        and current_close > latest_high
    )

    bearish_break = (
        previous_close >= latest_low
        and current_close < latest_low
    )

    if bullish_break:
        result["bos"] = "BULLISH"

    elif bearish_break:
        result["bos"] = "BEARISH"

    if result["structure"] == "BEARISH" and bullish_break:
        result["choch"] = "BULLISH"

    elif result["structure"] == "BULLISH" and bearish_break:
        result["choch"] = "BEARISH"

    bullish_sweep = (
        current_low < latest_low
        and current_close > latest_low
    )

    bearish_sweep = (
        current_high > latest_high
        and current_close < latest_high
    )

    if bullish_sweep:
        result["sweep"] = "BULLISH"

    elif bearish_sweep:
        result["sweep"] = "BEARISH"

    if result["choch"] != "NONE":
        result["direction"] = result["choch"]

    elif result["bos"] != "NONE":
        result["direction"] = result["bos"]

    elif result["sweep"] != "NONE":
        result["direction"] = result["sweep"]

    else:
        result["direction"] = result["structure"]

    return result


def analyze_liquidity(df, lookback=30, tolerance=0.001):
    """
    Advanced Liquidity Detection.
    Detects BSL, SSL, Equal Highs/Lows and liquidity grabs.
    Uses closed candles only.
    """

    result = {
        "bsl": None,
        "ssl": None,
        "equal_highs": False,
        "equal_lows": False,
        "grab": "NONE",
        "direction": "NEUTRAL"
    }

    if df is None or len(df) < lookback + 5:
        return result

    data = df.iloc[:-1].tail(lookback).copy()

    highs = pd.to_numeric(data["high"], errors="coerce")
    lows = pd.to_numeric(data["low"], errors="coerce")
    closes = pd.to_numeric(data["close"], errors="coerce")

    if highs.isna().any() or lows.isna().any() or closes.isna().any():
        return result

    current_high = float(highs.iloc[-1])
    current_low = float(lows.iloc[-1])
    current_close = float(closes.iloc[-1])

    previous_highs = highs.iloc[:-1]
    previous_lows = lows.iloc[:-1]

    bsl = float(previous_highs.max())
    ssl = float(previous_lows.min())

    result["bsl"] = bsl
    result["ssl"] = ssl

    # Equal Highs / Equal Lows
    equal_high_count = sum(
        abs(float(x) - bsl) / max(abs(bsl), 1e-12) <= tolerance
        for x in previous_highs
    )

    equal_low_count = sum(
        abs(float(x) - ssl) / max(abs(ssl), 1e-12) <= tolerance
        for x in previous_lows
    )

    result["equal_highs"] = equal_high_count >= 2
    result["equal_lows"] = equal_low_count >= 2

    # Liquidity Grab: wick breaks the level, close returns inside
    bullish_grab = (
        current_low < ssl
        and current_close > ssl
    )

    bearish_grab = (
        current_high > bsl
        and current_close < bsl
    )

    if bullish_grab and not bearish_grab:
        result["grab"] = "SSL"
        result["direction"] = "BULLISH"

    elif bearish_grab and not bullish_grab:
        result["grab"] = "BSL"
        result["direction"] = "BEARISH"

    return result


    previous_high = high_pivots[-2][1]
    latest_high = high_pivots[-1][1]

    previous_low = low_pivots[-2][1]
    latest_low = low_pivots[-1][1]

    result["high_level"] = latest_high
    result["low_level"] = latest_low

    # Определяем структуру рынка
    if latest_high > previous_high and latest_low > previous_low:
        result["structure"] = "BULLISH"

    elif latest_high < previous_high and latest_low < previous_low:
        result["structure"] = "BEARISH"

    # Последние две закрытые свечи
    previous_close = closes[-2]
    current_close = closes[-1]

    current_high = highs[-1]
    current_low = lows[-1]

    # Пробой структуры
    bullish_break = (
        previous_close <= latest_high
        and current_close > latest_high
    )

    bearish_break = (
        previous_close >= latest_low
        and current_close < latest_low
    )

    if bullish_break:
        result["bos"] = "BULLISH"

    elif bearish_break:
        result["bos"] = "BEARISH"

    # CHOCH — пробой против предыдущей структуры
    if result["structure"] == "BEARISH" and bullish_break:
        result["choch"] = "BULLISH"

    elif result["structure"] == "BULLISH" and bearish_break:
        result["choch"] = "BEARISH"

    # Снятие ликвидности
    bullish_sweep = (
        current_low < latest_low
        and current_close > latest_low
    )

    bearish_sweep = (
        current_high > latest_high
        and current_close < latest_high
    )

    if bullish_sweep:
        result["sweep"] = "BULLISH"

    elif bearish_sweep:
        result["sweep"] = "BEARISH"

    # Итоговое направление SMC
    if result["choch"] != "NONE":
        result["direction"] = result["choch"]

    elif result["bos"] != "NONE":
        result["direction"] = result["bos"]

    elif result["sweep"] != "NONE":
        result["direction"] = result["sweep"]

    else:
        result["direction"] = result["structure"]

    return result
def analyze_market_regime(df):
    result = {
        "regime": "UNKNOWN",
        "trend": "NEUTRAL",
        "volatility": "NORMAL",
        "adx": 0.0,
        "atr_percent": 0.0
    }

    if df is None or len(df) < 60:
        return result

    try:
        data = df.iloc[:-1].copy()

        high = pd.to_numeric(data["high"], errors="coerce")
        low = pd.to_numeric(data["low"], errors="coerce")
        close = pd.to_numeric(data["close"], errors="coerce")

        if high.isna().any() or low.isna().any() or close.isna().any():
            return result

        adx_series = ADXIndicator(
            high=high,
            low=low,
            close=close,
            window=14
        ).adx()

        adx = float(adx_series.iloc[-1])

        ema20 = float(close.ewm(span=20, adjust=False).mean().iloc[-1])
        ema50 = float(close.ewm(span=50, adjust=False).mean().iloc[-1])

        previous_close = close.shift(1)

        tr = pd.concat([
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs()
        ], axis=1).max(axis=1)

        atr = tr.rolling(14).mean()

        atr_percent = float(
            (atr.iloc[-1] / close.iloc[-1]) * 100
        )

        atr_percent_history = (
            (atr / close * 100)
            .replace([float("inf"), float("-inf")], float("nan"))
            .dropna()
        )

        if len(atr_percent_history) < 20:
            return result

        atr_median = float(
            atr_percent_history.tail(50).median()
        )

        result["adx"] = round(adx, 2)
        result["atr_percent"] = round(atr_percent, 4)

        if atr_median > 0 and atr_percent > atr_median * 1.5:
            result["volatility"] = "HIGH"
            result["regime"] = "HIGH VOLATILITY"

        elif adx >= 20 and ema20 > ema50:
            result["trend"] = "BULLISH"
            result["regime"] = "TRENDING UP"

        elif adx >= 20 and ema20 < ema50:
            result["trend"] = "BEARISH"
            result["regime"] = "TRENDING DOWN"

        else:
            result["regime"] = "RANGING"

        return result

    except Exception as e:
        print("Market Regime Error:", e)
        return result

def analyze(symbol, interval="1h"):
    is_otc = symbol.upper().startswith("OTC:")

    if is_otc:
        symbol = symbol[4:].strip().upper()

        if not re.fullmatch(r"[A-Z0-9#/_-]{3,30}", symbol):
            return "❌ Неверный формат OTC-пары."

        df = build_otc_candles(symbol, interval)
        market = "BINODEX OTC"

    else:
        symbol = symbol.upper().replace("/", "")

        if not re.fullmatch(r"[A-Z0-9]{5,20}", symbol):
            return "❌ Неверный формат пары."

        if symbol in FOREX_PAIRS:
            df = get_forex_data(symbol, interval)
            market = "FOREX"
        else:
            df = get_crypto_data(symbol, interval)
            market = "CRYPTO"

   
    if len(df) < 60:
        current = len(df)
        missing = 60 - current

        return (
            f"⏳ Недостаточно свечей для анализа.\n\n"
            f"📊 Таймфрейм: {interval}\n"
            f"🕯 Накоплено свечей: {current}/60\n"
            f"📥 Осталось накопить: {missing}\n\n"
            f"Дождись накопления истории и попробуй снова."
        )


    # Старший таймфрейм
    higher_timeframes = {
        "15sec": "1min",
        "30sec": "1min",
        "1min": "5min",
        "5min": "15min",
        "15min": "1h",
        "30min": "4h",
        "1h": "4h",
        "4h": "1day",
        "1day": "1week",
        "1week": None
    }

    higher_interval = higher_timeframes.get(interval)

    higher_trend = "NOT_USED"

    if higher_interval:
        if is_otc:
            higher_df = build_otc_candles(
                symbol, higher_interval
            )
        elif market == "FOREX":
            higher_df = get_forex_data(
                symbol, higher_interval
            )
        else:
            higher_df = get_crypto_data(
                symbol, higher_interval
            )

        
        if len(higher_df) < 60:
            if is_otc:
                higher_trend = "NOT_USED"
            else:
                return (
                    f"⏳ Недостаточно свечей старшего "
                    f"таймфрейма {higher_interval}.\n"
                    f"Нужно минимум 60."
                )

        if len(higher_df) < 60:
            if is_otc:
                higher_trend = "NOT_USED"
            else:
                return (
                    f"⏳ Недостаточно свечей старшего "
                    f"таймфрейма {higher_interval}.\n"
                    f"Нужно минимум 60."
                )

        else:
            higher_close = higher_df["close"]

            higher_ema20 = higher_close.ewm(
                span=20, adjust=False
            ).mean()

            higher_ema50 = higher_close.ewm(
                span=50, adjust=False
            ).mean()

            h_price = higher_close.iloc[-2]
            h_ema20 = higher_ema20.iloc[-2]
            h_ema50 = higher_ema50.iloc[-2]

            if h_price > h_ema20 > h_ema50:
                higher_trend = "UP"

            elif h_price < h_ema20 < h_ema50:
                higher_trend = "DOWN"

            else:
                higher_trend = "SIDEWAYS"


    # Основные индикаторы
    close = df["close"]

    df["ema20"] = close.ewm(
        span=20, adjust=False
    ).mean()

    df["ema50"] = close.ewm(
        span=50, adjust=False
    ).mean()

    df["rsi"] = RSIIndicator(
        close, window=14
    ).rsi()

    macd = MACD(close)

    df["macd"] = macd.macd()
    df["macd_signal"] = macd.macd_signal()

    adx_indicator = ADXIndicator(
        high=df["high"],
        low=df["low"],
        close=close,
        window=14
    )

    df["adx"] = adx_indicator.adx()

    previous_close = close.shift(1)

    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - previous_close).abs(),
        (df["low"] - previous_close).abs()
    ], axis=1).max(axis=1)

    df["atr"] = tr.rolling(14).mean()

    
    if "volume" in df.columns:
        df["volume_sma"] = df["volume"].rolling(20).mean()
    else:
        df["volume"] = 0
        df["volume_sma"] = 0
    row = df.iloc[-2]

    smc = analyze_smc(df)
    liquidity = analyze_liquidity(df)
    regime = analyze_market_regime(df)
    volume_confirm = True

    if market != "FOREX":
        current_volume = row["volume"]
        average_volume = row["volume_sma"]

        volume_confirm = (
            pd.notna(current_volume)
            and pd.notna(average_volume)
            and average_volume > 0
            and current_volume > average_volume
        )

    price = row["close"]
    rsi = row["rsi"]
    atr = row["atr"]
    adx = row["adx"]

    if pd.isna(atr) or pd.isna(rsi) or pd.isna(adx):
        raise ValueError("Недостаточно индикаторных данных")

    buy_conditions = (
        price > row["ema20"] > row["ema50"]
        and 50 < rsi < 68
        and row["macd"] > row["macd_signal"]
        and adx >= 20
        and higher_trend in ("UP", "NOT_USED")
        and volume_confirm
        and smc["direction"] == "BULLISH"
    )

    sell_conditions = (
        price < row["ema20"] < row["ema50"]
        and 32 < rsi < 50
        and row["macd"] < row["macd_signal"]
        and adx >= 20
        and higher_trend in ("DOWN", "NOT_USED")
        and volume_confirm
        and smc["direction"] == "BEARISH"
    )

    strength = 0

    if price > row["ema20"] > row["ema50"] or price < row["ema20"] < row["ema50"]:
        strength += 20

    if 50 < rsi < 68 or 32 < rsi < 50:
        strength += 15

    if (row["macd"] > row["macd_signal"] and price > row["ema20"]) or \
       (row["macd"] < row["macd_signal"] and price < row["ema20"]):
        strength += 20

    if adx >= 20:
        strength += 15

    if higher_trend in ("UP", "DOWN"):
        strength += 20

    if volume_confirm:
        strength += 10

    strength = min(strength, 100)

    if buy_conditions:
        signal = "🟢 BUY"
        stop = price - atr * 1.5
        target = price + atr * 3

    elif sell_conditions:
        signal = "🔴 SELL"
        stop = price + atr * 1.5
        target = price - atr * 3

    else:
        signal = "🟡 HOLD"
        stop = None
        target = None
    # BACKTEST: сохраняем только торговые сигналы
    if signal == "🟢 BUY":
        save_backtest_trade(
            symbol=symbol,
            interval=interval,
            direction="BUY",
            entry_price=price,
            stop_loss=stop,
            take_profit=target
        )

    elif signal == "🔴 SELL":
        save_backtest_trade(
            symbol=symbol,
            interval=interval,
            direction="SELL",
            entry_price=price,
            stop_loss=stop,
            take_profit=target
        )

    if higher_trend == "UP":
        trend_text = "🟢 Восходящий"
    elif higher_trend == "DOWN":
        trend_text = "🔴 Нисходящий"
    elif higher_trend == "SIDEWAYS":
        trend_text = "🟡 Боковой"
    else:
        trend_text = "Не применяется"

    result = (
        f"🤖 AI MARKET ANALYSIS\n\n"
        f"📊 Рынок: {market}\n"
        f"💹 Пара: {symbol}\n"
        f"⏱ Таймфрейм: {interval}\n"
        f"💰 Цена: {price:.8g}\n\n"
        f"📊 Сигнал: {signal}\n"
        f"💪 Сила сигнала: {strength}/100\n"
        f"📈 RSI: {rsi:.2f}\n"
        f"📊 ADX: {adx:.2f}\n"
        f"\n🧠 SMC ANALYSIS\n"
        f"Структура: {smc['structure']}\n"
        f"BOS: {smc['bos']}\n"
        f"CHOCH: {smc['choch']}\n"
        f"Liquidity Sweep: {smc['sweep']}\n"
        f"SMC Direction: {smc['direction']}\n"
        f"\n💧 LIQUIDITY ANALYSIS\n"
        f"BSL: {liquidity['bsl']}\n"
        f"SSL: {liquidity['ssl']}\n"
        f"Equal Highs: {liquidity['equal_highs']}\n"
        f"Equal Lows: {liquidity['equal_lows']}\n"
        f"Liquidity Grab: {liquidity['grab']}\n"
        f"Liquidity Direction: {liquidity['direction']}\n"
        f"\n🌍 MARKET REGIME\n"
        f"Режим рынка: {regime['regime']}\n"
        f"Тренд: {regime['trend']}\n"
        f"Волатильность: {regime['volatility']}\n"
        f"ADX: {regime['adx']}\n"
        f"ATR %: {regime['atr_percent']}\n"
        f"EMA 20: {row['ema20']:.8g}\n"
        f"EMA 50: {row['ema50']:.8g}\n"
        f"MACD: {row['macd']:.8g}\n\n"
        f"🔎 Старший ТФ: {higher_interval or '—'}\n"
        f"📊 Тренд старшего ТФ: {trend_text}\n\n"
        f"⚠️ Аналитический сигнал, не гарантия прибыли."
    )
    if stop is not None:
        result += (
            f"🛑 Stop Loss: {stop:.8g}\n"
            f"🎯 Take Profit: {target:.8g}\n\n"
        )

    result += "⚠️ Аналитический сигнал, не гарантия прибыли."

    return result




@bot.message_handler(commands=["otc"])
def otc_markets(message):
    api_key = os.getenv("OTC_API_KEY")

    if not api_key:
        bot.reply_to(message, "❌ OTC API-ключ не найден.")
        return

    try:
        response = requests.get(
            "https://otcharts.com/v1/symbols",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"venue": "binodex"},
            timeout=20
        )

        if response.status_code != 200:
            bot.reply_to(
                message,
                f"❌ OTCharts HTTP {response.status_code}\n"
                f"{response.text[:500]}"
            )
            return

        data = response.json()
        symbols = data.get("symbols", [])

        if not symbols:
            bot.reply_to(message, "❌ OTC-пары не найдены.")
            return

        result = "📊 BINODEX OTC ПАРЫ\n\n"

        for item in symbols:
            if isinstance(item, dict):
                name = item.get("symbol", "Неизвестно")
            else:
                name = item

            result += f"• {name}\n"

        bot.reply_to(message, result[:4000])

    except Exception as error:
        print("OTC error:", repr(error))
        bot.reply_to(
            message,
            f"❌ Ошибка OTC: {type(error).__name__}"
        )
@bot.message_handler(commands=["start"])
def start(message):
    active_chats.add(message.chat.id)

    bot.reply_to(
        message,
        "🤖 AI MARKET SIGNAL BOT\n\n"
        "Криптовалюты + Forex!\n\n"
        "Используй:\n"
        "/signal BTCUSDT\n"
        "/signal ETHUSDT\n"
        "/signal EURUSD\n"
        "/signal GBPUSD\n"
        "/markets"
    )


active_chats = set()
last_signals = {}

CRYPTO_PAIRS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT",
    "SOLUSDT", "XRPUSDT"
]

ALL_PAIRS = CRYPTO_PAIRS + sorted(FOREX_PAIRS)


def automatic_scanner():
    while True:
        for symbol in ALL_PAIRS:
            try:
                result = analyze(symbol, "1h")

                if "Сигнал: 🟢 BUY" in result:
                    current_signal = "BUY"
                elif "Сигнал: 🔴 SELL" in result:
                    current_signal = "SELL"
                else:
                    continue

                if last_signals.get(symbol) == current_signal:
                    continue

                for chat_id in list(active_chats):
                    bot.send_message(chat_id, result)

                last_signals[symbol] = current_signal

            except Exception as error:
                print(f"Scanner error {symbol}: {error}")

        time.sleep(1800)
@bot.message_handler(commands=["help"])
def help_command(message):
    bot.reply_to(
        message,
        "/start — запуск\n"
        "/help — помощь\n"
        "/markets — список пар\n"
        "/signal BTCUSDT — криптовалюты\n"
        "/signal EURUSD — Forex"
    )


@bot.message_handler(commands=["markets"])
def markets(message):
    bot.reply_to(
        message,
        "₿ CRYPTO:\n"
        "BTCUSDT\nETHUSDT\nBNBUSDT\nSOLUSDT\nXRPUSDT\n\n"
        "💱 FOREX:\n"
        + "\n".join(sorted(FOREX_PAIRS))
    )


@bot.message_handler(commands=["signal"])
def signal_command(message):
    parts = message.text.split()

    symbol = parts[1].upper() if len(parts) > 1 else "BTCUSDT"
    interval = parts[2].lower() if len(parts) > 2 else "1h"

    allowed_intervals = [
        "15sec", "30sec",
        "1min", "5min", "15min", "30min",
        "1h", "4h", "1day", "1week"
    ]

    if interval not in allowed_intervals:
        bot.send_message(
            message.chat.id,
            "❌ Неверный таймфрейм.\n"
            "Доступно: 15sec, 30sec, 1min, 5min, "
            "15min, 30min, 1h, 4h, 1day, 1week"
        )
        return

    if symbol.startswith("OTC:"):
        otc_symbol = symbol[4:]

        if not otc_symbol.endswith("-OTC"):
            otc_symbol += "-OTC"

        symbol = "OTC:" + otc_symbol

    try:
        bot.send_message(
            message.chat.id,
            f"🔎 Анализирую {symbol} ({interval})..."
        )

        result = analyze(symbol, interval)

        bot.send_message(message.chat.id, result)

    except Exception as error:
        print("Analysis error:", repr(error))

        bot.send_message(
            message.chat.id,
            f"❌ Ошибка анализа:\n"
            f"{type(error).__name__}\n"
            f"{str(error)[:800]}"
        )

@bot.message_handler(commands=["otctest"])
def otc_test(message):
    import time

    bot.reply_to(message, "🔄 Проверяю подключение к BinoDex...")

    sio = socketio.Client(reconnection=False)
    ticks = []

    @sio.on("graphic", namespace="/graphic")
    def on_graphic(data):
        if isinstance(data, dict):
            ticks.append(data)
            save_otc_tick(data)

    try:
        sio.connect(
            "https://api-coins.binodex.app",
            socketio_path="market",
            transports=["websocket"],
            namespaces=["/graphic"],
            wait_timeout=10
        )

        sio.emit(
            "graphic",
            {
                "method": "SUBSCRIBE",
                "symbol": "EUR/USD",
                "interval": "1m",
                "otc": True
            },
            namespace="/graphic"
        )

        time.sleep(8)
        sio.disconnect()

        if ticks:
            last = ticks[-1]
            bot.reply_to(
                message,
                f"✅ BinoDex подключён!\n"
                f"Пара: {last.get('symbol')}\n"
                f"Цена: {last.get('price')}\n"
                f"Получено обновлений: {len(ticks)}"
            )
        else:
            bot.reply_to(
                message,
                "⚠️ Подключение есть, но котировки не получены."
            )

    except Exception as error:
        if sio.connected:
            sio.disconnect()

        bot.reply_to(message, f"❌ Ошибка BinoDex: {error}")

@bot.message_handler(commands=["otcpairs"])
def otc_pairs_status(message):
    pairs = [
        "EUR/USD-OTC",
        "GBP/USD-OTC",
        "AUD/USD-OTC",
        "USD/JPY-OTC",
        "EUR/JPY-OTC",
        "EUR/GBP-OTC",
        "GBP/JPY-OTC",
        "AUD/JPY-OTC",
        "USD/CAD-OTC",
        "USD/CHF-OTC",
        "NZD/USD-OTC",
        "EUR/AUD-OTC",
        "GBP/AUD-OTC",
        "EUR/CHF-OTC",
        "GBP/CHF-OTC"
    ]

    result = "📊 КОТИРОВКИ BINODEX OTC\n\n"

    for pair in pairs:
        ticks = OTC_TICKS.get(pair, [])

        if ticks:
            last = ticks[-1]
            price = f"{float(last['price']):.6f}".rstrip("0").rstrip(".")

            result += (
                f"✅ {pair}\n"
                f"Цена: {price}\n"
                f"Тиков: {len(ticks)}\n\n"
            )
        else:
            result += f"⏳ {pair} — котировок пока нет\n\n"

    bot.reply_to(message, result)

@bot.message_handler(commands=["otcdb"])
def otc_database_status(message):
    try:
        count = get_otc_database_count()

        bot.reply_to(
            message,
            f"🗄 БАЗА BINODEX OTC\n\n"
            f"📊 Всего сохранено тиков: {count}"
        )

    except Exception as error:
        bot.reply_to(
            message,
            f"❌ Ошибка базы данных: {error}"
        )


print("ПРОВЕРКА: bot.py дошёл до загрузки OTC!", flush=True)
load_otc_history()
print("Проверка: загрузка OTC истории завершена.")

print("AI Market Signal Bot started!", flush=True)

threading.Thread(
    target=supabase_worker,
    daemon=True
).start()
threading.Thread(
    target=run_otc_stream,
    daemon=True
).start()

print("BinoDex stream thread started!")

threading.Thread(
    target=automatic_scanner,
    daemon=True
).start()


@bot.message_handler(commands=["otctimeframe"])
def otc_timeframe_menu(message):
    parts = message.text.split()

    if len(parts) < 2:
        bot.reply_to(
            message,
            "Используй команду:\n/otctimeframe EUR/USD"
        )
        return

    symbol = parts[1].upper().replace("OTC:", "")

    if not symbol.endswith("-OTC"):
        symbol += "-OTC"

    if not re.fullmatch(r"[A-Z0-9#/_-]{3,30}", symbol):
        bot.reply_to(message, "❌ Неверный формат OTC-пары.")
        return

    intervals = [
        ("15 сек", "15sec"),
        ("30 сек", "30sec"),
        ("1 мин", "1min"),
        ("5 мин", "5min"),
        ("15 мин", "15min"),
        ("30 мин", "30min"),
        ("1 час", "1h"),
        ("4 часа", "4h"),
        ("1 день", "1day"),
        ("1 неделя", "1week")
    ]

    keyboard = telebot.types.InlineKeyboardMarkup(row_width=2)

    for label, interval in intervals:
        keyboard.add(
            telebot.types.InlineKeyboardButton(
                text=label,
                callback_data=f"otctf|{symbol}|{interval}"
            )
        )

    bot.reply_to(
        message,
        f"📊 OTC-пара: {symbol}\n\nВыбери таймфрейм:",
        reply_markup=keyboard
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("otctf|"))
def otc_timeframe_callback(call):
    try:
        _, symbol, interval = call.data.split("|")

        bot.answer_callback_query(call.id)

        bot.send_message(
            call.message.chat.id,
            f"🔎 Анализирую OTC:{symbol} ({interval})..."
        )

        result = analyze(f"OTC:{symbol}", interval)

        bot.send_message(call.message.chat.id, result)

    except Exception as error:
        print("OTC timeframe error:", repr(error))
        bot.send_message(
            call.message.chat.id,
            f"❌ Ошибка: {type(error).__name__}"
        )

bot.infinity_polling(skip_pending=True)
