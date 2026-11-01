from collections import defaultdict, deque
import socketio
import threading
import os
import re
import requests
import telebot
import pandas as pd
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

def save_otc_tick(data):
    try:
        symbol = data.get("symbol", "").upper()
        price = float(data.get("price"))
        timestamp = int(data.get("timestamp"))

        if not symbol or price <= 0:
            return

        OTC_TICKS[symbol].append({
            "timestamp": timestamp,
            "price": price
        })

    except (TypeError, ValueError, AttributeError):
        pass
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

    df["candle_time"] = df["time"].dt.floor(f"{seconds}s")

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
def analyze(symbol, interval="1h"):
    is_otc = symbol.upper().startswith("OTC:")

    if is_otc:
        symbol = symbol[4:].strip().upper()

        if not re.fullmatch(r"[A-Z0-9#/_-]{3,30}", symbol):
            return "❌ Неверный формат OTC-пары."

        df = get_binodex_data(symbol, interval)
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

    close = df["close"]

    df["ema20"] = close.ewm(span=20, adjust=False).mean()
    df["ema50"] = close.ewm(span=50, adjust=False).mean()

    df["rsi"] = RSIIndicator(close, window=14).rsi()

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

    row = df.iloc[-2]

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
    )

    sell_conditions = (
        price < row["ema20"] < row["ema50"]
        and 32 < rsi < 50
        and row["macd"] < row["macd_signal"]
        and adx >= 20
    )

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

    result = (
        f"🤖 AI MARKET ANALYSIS\n\n"
        f"📊 Рынок: {market}\n"
        f"💹 Пара: {symbol}\n"
        f"⏱ Таймфрейм: {interval}\n"
        f"💰 Цена: {price:.8g}\n\n"
        f"📊 Сигнал: {signal}\n"
        f"📈 RSI: {rsi:.2f}\n"
        f"📊 ADX: {adx:.2f}\n"
        f"EMA 20: {row['ema20']:.8g}\n"
        f"EMA 50: {row['ema50']:.8g}\n"
        f"MACD: {row['macd']:.8g}\n\n"
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
        "1min", "5min", "15min", "30min",
        "1h", "4h", "1day", "1week"
    ]

    if interval not in allowed_intervals:
        bot.send_message(
            message.chat.id,
            "❌ Неверный таймфрейм.\n"
            "Доступно: 1min, 5min, 15min, 30min, "
            "1h, 4h, 1day, 1week"
        )
        return

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
            bot.reply_to(message, "⚠️ Подключение есть, но котировки не получены.")

    except Exception as error:
        if sio.connected:
            sio.disconnect()

        bot.reply_to(message, f"❌ Ошибка BinoDex: {error}")
print("AI Market Signal Bot started!")

threading.Thread(
    target=automatic_scanner,
    daemon=True
).start()

bot.infinity_polling(skip_pending=True)
