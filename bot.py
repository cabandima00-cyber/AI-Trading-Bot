import threading
import os
import re
import requests
import telebot
import pandas as pd

from ta.momentum import RSIIndicator
from ta.trend import MACD

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

    if data.get("status") == "error":
        raise ValueError(data.get("message", "Ошибка Twelve Data"))

    values = data.get("values")

    if not values or len(values) < 60:
        raise ValueError("Недостаточно данных Forex")

    df = pd.DataFrame(values)

    df["datetime"] = pd.to_datetime(df["datetime"])

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col])

    df = df.sort_values("datetime").reset_index(drop=True)

    return df


def get_crypto_data(symbol, interval="1h"):
    response = requests.get(
        BINANCE_API,
        params={
            "symbol": symbol,
           "interval": {
    "1min": "1m",
    "5min": "5m",
    "15min": "15m",
    "30min": "30m",
    "1h": "1h",
    "4h": "4h",
    "1day": "1d",
    "1week": "1w"
}.get(interval, interval),
            "limit": 200
        },
        timeout=20
    )

    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list) or len(data) < 60:
        raise ValueError("Недостаточно данных криптовалюты")

    df = pd.DataFrame(data, columns=[
        "time", "open", "high", "low", "close",
        "volume", "close_time", "quote_volume",
        "trades", "taker_buy", "taker_quote", "ignore"
    ])

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col])

    return df
def analyze(symbol, interval="1h"):
    symbol = symbol.upper().replace("/", "")

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

    previous_close = close.shift(1)

    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - previous_close).abs(),
        (df["low"] - previous_close).abs()
    ], axis=1).max(axis=1)

    df["atr"] = tr.rolling(14).mean()

    # Последняя полностью закрытая свеча
    row = df.iloc[-2]

    price = row["close"]
    rsi = row["rsi"]
    atr = row["atr"]

    if pd.isna(atr) or pd.isna(rsi):
        raise ValueError("Недостаточно индикаторных данных")

    score = 0

    if price > row["ema20"] > row["ema50"]:
        score += 1
    elif price < row["ema20"] < row["ema50"]:
        score -= 1

    if 50 < rsi < 70:
        score += 1
    elif 30 < rsi < 50:
        score -= 1

    if row["macd"] > row["macd_signal"]:
        score += 1
    elif row["macd"] < row["macd_signal"]:
        score -= 1

    if score >= 2:
        signal = "🟢 BUY"
        stop = price - atr * 1.5
        target = price + atr * 3

    elif score <= -2:
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
print("AI Market Signal Bot started!")

threading.Thread(
    target=automatic_scanner,
    daemon=True
).start()

bot.infinity_polling(skip_pending=True)
