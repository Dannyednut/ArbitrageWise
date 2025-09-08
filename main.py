import aiohttp
import asyncio
import traceback
from threading import Thread
from dataclasses import asdict
from quart import Quart, jsonify, request
import uvicorn

# Custom imports
from config import Config
from cross import CrossExchange
from triangular import Triangular
from engine import logger, Engine
from models import TradeResult
from telegram_alert import TelegramNotifier


# --- Main Application Class ---
class ArbitrageApp:
    def __init__(self, config):
        self.config = config
        self.headers = {
            'api_key': self.config.APP_TOKEN,
            'Content-Type': 'application/json'
        }
        self.notifier = TelegramNotifier(Config.TELEGRAM_CHAT_ID) if Config.TELEGRAM_ALERTS_ENABLED else None
        self.engine = Engine(config.BASE44_API_URL, config.APP_TOKEN, self.notifier)
        self.cross_engine = CrossExchange(config.SYMBOLS, self.engine)
        self.triangular_engine = Triangular(self.engine)
        self.trade_lock = asyncio.Lock()

    async def run_scanners(self):
        logger.info("Initializing exchanges...")
        await self.engine.initialize_exchanges()

        logger.info("Starting all arbitrage scanners...")
        scanner_tasks = [
            asyncio.create_task(self.cross_engine.start()),
            asyncio.create_task(self.triangular_engine.start()),
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


# --- Quart App and Global App Instance ---
app = Quart(__name__)
arbitrage_app = ArbitrageApp(Config)

@app.route('/')
async def index():
    return "Hello there, I'm busy working"

@app.route('/balances', methods=['GET'])
async def get_balances():
    if not arbitrage_app.cross_engine.account_balances:
        return jsonify({"status": "error", "message": "Balances not yet fetched."}), 503
    return jsonify(arbitrage_app.cross_engine.account_balances)

@app.route('/execute', methods=['POST'])
async def execute_trade_endpoint():
    data = await request.get_json()
    if not data or not all(k in data for k in ['opportunity_id', 'type', 'strategy']):
        return jsonify({"status": "error", "message": "Missing required parameters"}), 400

    try:
        result = await arbitrage_app.execute_trade_logic(data)
        status_code = 200 if result.status == 'success' else 400

        if 'chat_id' in data and arbitrage_app.notifier:
            await arbitrage_app.notifier.send_trade_result(data['chat_id'], result)

        return jsonify(asdict(result)), status_code

    except asyncio.TimeoutError:
        return jsonify({"status": "error", "message": "Trade execution timed out."}), 504
    except Exception as e:
        logger.error(f"API endpoint error: {e}")
        return jsonify({"status": "error", "message": "An internal server error occurred."}), 500

@app.route('/telegram-webhook', methods=['POST'])
async def telegram_webhook():
    data = await request.get_json()

    if 'callback_query' in data:
        callback = data['callback_query']
        chat_id = callback['message']['chat']['id']
        data_str = callback['data']  # e.g., "exec|instant|abc123"

        parts = data_str.split('|')
        if len(parts) != 3:
            return jsonify(ok=True)

        action, subtype, opportunity_id = parts

        if action == "exec":
            exec_payload = {
                "type": "cross" if subtype in ("instant", "transfer") else "triangular",
                "strategy": subtype,
                "opportunity_id": opportunity_id
            }

            try:
                result = await arbitrage_app.execute_trade_logic(exec_payload)
                if result.status != "success":
                    logger.error(f"Trade execution from Telegram button failed: {result.message}")
                    result = TradeResult("error", f"Trade execution failed: {result.message}")
            except Exception as e:
                logger.error(f"Error executing trade from Telegram button: {e}")
                result = TradeResult("error", "Failed to execute trade from Telegram button.")

            await arbitrage_app.notifier.send_trade_result(chat_id, result)

    return jsonify(ok=True)


# --- Main Entry Point ---
if __name__ == "__main__":

    async def main():
        try:
            # Start the scanner tasks
            scanner_task = asyncio.create_task(arbitrage_app.run_scanners())
            # Start Quart API server
            await app.run_task(host=Config.FLASK_HOST, port=Config.FLASK_PORT)
            await scanner_task
        except KeyboardInterrupt:
            logger.info("Shutdown signal received. Exiting.")
        finally:
            await arbitrage_app.engine.stop()
            logger.info("Application shut down gracefully.")

    asyncio.run(main())
