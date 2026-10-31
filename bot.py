import os
import re
import time
import requests
import telebot
import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import MACD

TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")

if not TOKEN:
    raise ValueError("Не найден BOT_TOKEN")

bot = telebot.TeleBot(TOKEN)

BINANCE_API = "https://api.binance.com/api/v3/klines"

def analyze(symbol):
    symbol = symbol.upper()

    if not re.fullmatch(r"[A-Z0-9]{5,20}", symbol):
        return "❌ Неверный формат торговой пары."

    response = requests.get(
        BINANCE_API,
        params={
            "symbol": symbol,
            "interval": "1h",
            "limit": 200
        },
        timeout=15
    )
    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list) or len(data) < 60:
        return "❌ Недостаточно данных для анализа."

    df = pd.DataFrame(data, columns=[
        "time", "open", "high", "low", "close",
        "volume", "close_time", "quote_volume",
        "trades", "taker_buy", "taker_quote", "ignore"
    ])

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col])

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

    # Используем последнюю закрытую свечу
    row = df.iloc[-2]

    price = row["close"]
    rsi = row["rsi"]
    atr = row["atr"]

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
        f"💹 Пара: {symbol}\n"
        f"⏱ Таймфрейм: 1 час\n"
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
    bot.reply_to(
        message,
        "🤖 AI Market Signal Bot запущен!\n\n"
        "Используй /signal BTCUSDT для анализа."
    )


@bot.message_handler(commands=["help"])
def help_command(message):
    bot.reply_to(
        message,
        "/start — запуск\n"
        "/help — помощь\n"
        "/signal BTCUSDT — анализ пары\n"
        "/markets — список популярных пар"
    )


@bot.message_handler(commands=["markets"])
def markets(message):
    bot.reply_to(
        message,
        "📊 Популярные пары:\n"
        "BTCUSDT\nETHUSDT\nBNBUSDT\nSOLUSDT\nXRPUSDT"
    )


@bot.message_handler(commands=["signal"])
def signal_command(message):
    parts = message.text.split()

    symbol = parts[1] if len(parts) > 1 else "BTCUSDT"

    try:
        bot.send_message(message.chat.id, "🔎 Анализирую рынок...")
        result = analyze(symbol)
        bot.send_message(message.chat.id, result)

  except Exception as error:
    print("Analysis error:", repr(error))

    bot.send_message(
        message.chat.id,
        f"❌ Ошибка анализа:\n{type(error).__name__}\n{str(error)[:800]}"
    )


print("AI Market Signal Bot started!")
bot.infinity_polling(skip_pending=True)
