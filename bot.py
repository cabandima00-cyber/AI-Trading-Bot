import os
import telebot

TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")

if not TOKEN:
    raise ValueError("Не найден токен Telegram!")

bot = telebot.TeleBot(TOKEN)

@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(
        message,
        "🤖 Привет! Я AI Market Signal Bot!\n\n"
        "📈 Анализ криптовалют\n"
        "💱 Анализ Forex\n"
        "📊 Анализ акций\n\n"
        "Используй /help для команд."
    )

@bot.message_handler(commands=['help'])
def help_command(message):
    bot.reply_to(
        message,
        "Доступные команды:\n"
        "/start — запуск бота\n"
        "/help — помощь\n"
        "/status — статус бота"
    )

@bot.message_handler(commands=['status'])
def status(message):
    bot.reply_to(message, "🟢 Бот работает!")

print("AI Market Signal Bot запущен!")
bot.infinity_polling()
