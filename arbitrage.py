import aiohttp
import asyncio
import traceback

# Custom imports
from config import Config
from cross import CrossExchange
from triangular import Triangular
from engine import logger, Engine
from models import TradeResult
from telegram_alert import TelegramNotifier


# --- Main Application Class ---
class ArbitrageApp:
    def __init__(self, config: Config):
        self.config = config
        self.headers = {
            'api_key': self.config.APP_TOKEN,
            'Content-Type': 'application/json'
        }
        self.notifier = TelegramNotifier(self.config.TELEGRAM_CHAT_ID) if self.config.TELEGRAM_ALERTS_ENABLED else None
        self.engine = Engine(config.BASE44_API_URL, config.APP_TOKEN, self.notifier)
        self.cross_engine = CrossExchange(self.engine, self.config)
        self.triangular_engine = Triangular(self.engine)
        self.trade_lock = asyncio.Lock()

    async def run_scanners(self):
        logger.info("Initializing exchanges...")
        await self.engine.initialize_exchanges()

        logger.info("Starting all arbitrage scanners...")
        scanner_tasks = [
            asyncio.create_task(self.cross_engine.start()),
            asyncio.create_task(self.triangular_engine.start()),
            asyncio.create_task(self.engine.watch_base44_config()),
            asyncio.create_task(self.engine.update_account_balances_periodically())
        ]

        if self.notifier:
            scanner_tasks.append(asyncio.create_task(self.notifier.start_polling()))

        await asyncio.gather(*scanner_tasks)

    async def execute_trade_logic(self, trade_request: dict) -> TradeResult:
        async with self.trade_lock:
            op_type = trade_request['type']
            entity_name = 'ArbitrageOpportunity' if op_type == 'cross' else 'TriangularOpportunity'

            try:
                async with aiohttp.ClientSession() as session:
                    url = f"{self.config.BASE44_API_URL}/entities/{entity_name}/{trade_request['opportunity_id']}"
                    async with session.get(url, headers=self.headers) as resp:
                        if resp.status != 200:
                            return TradeResult("error", f"Opportunity not found or expired. Status: {resp.status}")
                        op = await resp.json()

                if op_type == 'cross':
                    return await self.cross_engine._execute_cross_exchange_trade(op, trade_request)
                elif op_type == 'triangular':
                    return await self.triangular_engine._execute_triangular_trade(op, trade_request)

                return TradeResult("error", "Invalid trade type")

            except Exception as e:
                logger.error(f"FATAL error during trade execution pipeline: {e}")
                logger.error(traceback.format_exc())
                return TradeResult("error", "An unexpected server error occurred during execution.")
