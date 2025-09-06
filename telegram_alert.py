import logging
import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
# from aiogram import executor
from models import Opportunity, TradeResult
from config import Config
from engine import logger


class TelegramNotifier:
    def __init__(self, chat_id, execution_endpoint: str = "http://localhost:5001/execute"):
        self.bot = Bot(token=Config.TELEGRAM_BOT_TOKEN)
        self.dp = Dispatcher()
        self.chat_id = chat_id
        self.execution_endpoint = execution_endpoint

        # Register handlers
        self.dp.callback_query.register(self.handle_execute_button, lambda c: c.data.startswith("EXECUTE_"))

    async def send_opportunity_alert(self, opportunity: Opportunity):
        """
        Send an opportunity alert message with inline buttons to execute trade.
        """
        message_text = self._format_opportunity_message(opportunity)
        keyboard = self._build_action_buttons(opportunity)

        await self.bot.send_message(chat_id=self.chat_id, text=message_text, parse_mode="HTML", reply_markup=keyboard)

    def _format_opportunity_message(self, opp: Opportunity) -> str:
        if opp.trading_pair:  # Cross-exchange opportunity
            text = (
                f"♻️ <b>New Cross-Exchange Opportunity Detected!</b>\n"
                f"Pair: {opp.trading_pair}\n"
                f"Buy on: {opp.buy_exchange} @ {opp.buy_price:.6f}\n"
                f"Sell on: {opp.sell_exchange} @ {opp.sell_price:.6f}\n"
                f"Profit: {opp.profit_percentage:.2f}% ({opp.profit_usd:.2f} USD)\n"
                f"Volume: {opp.volume}\n"
                f"Detected At: {opp.detected_at}"
            )
        else:  # Triangular opportunity
            path_str = " -> ".join(opp.trading_path or [])
            text = (
                f"🔺 <b>New Triangular Opportunity Detected!</b>\n"
                f"Exchange: {opp.exchange}\n"
                f"Path: {path_str}\n"
                f"Profit: {opp.profit_percentage:.2f}%\n"
                f"Initial Amount: {opp.initial_amount}\n"
                f"Final Amount: {opp.final_amount}\n"
                f"Detected At: {opp.detected_at}"
            )
        return text

    def _build_action_buttons(self, opp: Opportunity) -> InlineKeyboardMarkup:
        """
        Builds inline keyboard buttons for executing trades.
        For cross-exchange: buttons for instant or transfer strategies.
        For triangular: single execute button.
        """
        keyboard = InlineKeyboardMarkup(row_width=2)
        opp_type = "cross" if opp.trading_pair else "triangular"

        if opp_type == "cross":
            keyboard.insert(
                InlineKeyboardButton(
                    text="Execute Instant",
                    callback_data=f"EXECUTE_{opp_type}_{opp.id}_instant"
                )
            )
            keyboard.insert(
                InlineKeyboardButton(
                    text="Execute Transfer",
                    callback_data=f"EXECUTE_{opp_type}_{opp.id}_transfer"
                )
            )
        else:  # Triangular only one execute option
            keyboard.add(
                InlineKeyboardButton(
                    text="Execute",
                    callback_data=f"EXECUTE_{opp_type}_{opp.id}_default"
                )
            )
        return keyboard

    async def handle_execute_button(self, callback_query: CallbackQuery):
        await callback_query.answer()
        chat_id = callback_query.message.chat.id

        try:
            _, opp_type, opportunity_id, strategy = callback_query.data.split("_")
            trade_type = "triangular" if opp_type == "triangular" else "cross"

            payload = {
                "type": trade_type,
                "strategy": strategy,
                "opportunity_id": opportunity_id,
                "chat_id": chat_id
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(self.execution_endpoint, json=payload) as resp:
                    data = await resp.json()

            if resp.status == 200:
                text = "✅ Trade request accepted, executing..."
            else:
                text = f"⚠️ Execution failed: {data.get('message', 'Unknown error')}"

        except Exception as e:
            logger.error(f"Error handling execute button: {e}")
            text = f"❌ Error during execution: {str(e)}"

        # Edit the message to show execution status and remove buttons
        await callback_query.message.edit_text(text, parse_mode="HTML")

    async def send_trade_result(self, chat_id: int, trade_result: TradeResult):
        """
        Send a simple notification message with trade result.
        """
        status = trade_result.status.capitalize()
        profit = f"${trade_result.profit_usd:.2f}" if trade_result.profit_usd else "N/A"
        message = (
            f"📊 <b>Trade Result</b>\n"
            f"Status: {status}\n"
            f"Message: {trade_result.message}\n"
            f"Trade ID: {trade_result.trade_id or 'N/A'}\n"
            f"Profit: {profit}"
        )
        await self.bot.send_message(chat_id=chat_id or self.chat_id, text=message, parse_mode="HTML")

    async def start_polling(self):
        await self.dp.start_polling(self.bot, skip_updates=True)

