import aiohttp
import requests
import urllib.parse
from models import Opportunity
from config import Config
from engine import logger


base_url = f"{Config.TELEGRAM_API_URL}/{Config.TELEGRAM_BOT_TOKEN}"

async def send_telegram_opportunity(op: Opportunity, strategy: str, chat_id = None):
    try:
        msg = build_opportunity_message(op, strategy)
        
        # Build inline keyboard with buttons
        buttons = []

        if strategy == "cross":
            buttons.append([
                {"text": "🚀 Instant Execute", "callback_data": f"exec|instant|{op.id}"},
                {"text": "🔁 Transfer + Execute", "callback_data": f"exec|transfer|{op.id}"}
            ])
        else:  # triangular
            buttons.append([
                {"text": "🚀 Execute", "callback_data": f"exec|triangular|{op.id}"}
            ])

        payload = {
            "chat_id": chat_id or Config.TELEGRAM_CHAT_ID,
            "text": msg,
            "parse_mode": "Markdown",
            "reply_markup": {"inline_keyboard": buttons}
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(f"{base_url}/sendMessage", json=payload) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error(f"Failed to send Telegram alert: {error_text}")
    except Exception as e:
        logger.error(f"Telegram alert error: {e}")


def build_opportunity_message(op: Opportunity, strategy: str) -> str:
    if strategy == "cross":
        return (
            f"*Cross Exchange Arbitrage Opportunity*\n\n"
            f"*Buy on:* {op.buy_exchange} @ {op.buy_price:.4f}\n"
            f"*Sell on:* {op.sell_exchange} @ {op.sell_price:.4f}\n"
            f"*Pair:* `{op.trading_pair}`\n"
            f"*Profit:* {op.profit_percentage:.2f}% (~${op.profit_usd:.2f})"
        )
    else:
        return (
            f"*Triangular Arbitrage Opportunity*\n\n"
            f"*Exchange:* {op.exchange}\n"
            f"*Path:* `{op.assets[0]} ➡️ {op.assets[1]} ➡️ {op.assets[2]} ➡️ {op.assets[0]}`\n"
            f"*Profit:* {op.profit_percentage:.2f}% (~${op.final_amount - op.initial_amount:.2f})"
        )

# def send_telegram_message(chat_id, text):
#     url = f"{base_url}/sendMessage"
#     payload = {
#         "chat_id": chat_id,
#         "text": text,
#         "parse_mode": "Markdown"
#     }
#     requests.post(url, json=payload)

# --- IGNORE ---
# https://api.telegram.org/bot<YOUR_BOT_TOKEN>/setWebhook?url=https://yourdomain.com/telegram-webhook