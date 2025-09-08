import aiohttp
import asyncio
import traceback
from threading import Thread
from dataclasses import asdict
from flask import Flask, jsonify, request

# Import your custom modules
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
        # Instantiate both scanner engines
        self.notifier = TelegramNotifier(Config.TELEGRAM_CHAT_ID) if Config.TELEGRAM_ALERTS_ENABLED else None
        self.engine = Engine(config.BASE44_API_URL, config.APP_TOKEN, self.notifier)
        self.cross_engine = CrossExchange(config.SYMBOLS, self.engine)
        self.triangular_engine = Triangular(self.engine)
        self.trade_lock = asyncio.Lock() # Prevents multiple trades from executing at once

    async def run_scanners(self):
        """Initializes and runs both scanning engines concurrently."""
        logger.info("Initializing exchanges...")
        await self.engine.initialize_exchanges()

        logger.info("Starting all arbitrage scanners...")
        # The engines will initialize their own exchanges
        scanner_tasks = [
            asyncio.create_task(self.cross_engine.start()),
            asyncio.create_task(self.triangular_engine.start()),
            asyncio.create_task(self.notifier.start_polling()),
            asyncio.create_task(self.engine.update_account_balances_periodically())
        ]
        await asyncio.gather(*scanner_tasks)

    async def execute_trade_logic(self, trade_request: dict) -> TradeResult:
        """Main router for all trade execution requests with safety lock."""
        async with self.trade_lock:
            op_type = trade_request['type']
            entity_name = 'ArbitrageOpportunity' if op_type == 'cross' else 'TriangularOpportunity'
            
            try:
                # Fetch the latest opportunity details from Base44 to ensure it's still valid
                async with aiohttp.ClientSession() as session:
                    url = f"{self.config.BASE44_API_URL}/entities/{entity_name}/{trade_request['opportunity_id']}"
                    async with session.get(url, headers=self.headers) as resp:
                        if resp.status != 200:
                            return TradeResult("error", f"Opportunity not found or expired. Status: {resp.status}")
                        op = await resp.json()

                # Route to the correct execution function
                if op_type == 'cross':
                    return await self.cross_engine._execute_cross_exchange_trade(op, trade_request)
                elif op_type == 'triangular':
                    return await self.triangular_engine._execute_triangular_trade(op, trade_request)
                
                return TradeResult("error", "Invalid trade type")

            except Exception as e:
                logger.error(f"FATAL error during trade execution pipeline: {e}")
                logger.error(traceback.format_exc())
                return TradeResult("error", "An unexpected server error occurred during execution.")

    

# --- Flask App and Global App Instance ---
app = Flask(__name__)
arbitrage_app = ArbitrageApp(Config)
# This will hold the running asyncio event loop for the scanners
scanner_loop = None

@app.route('/')
def index():
    return ("Hello there, I'm busy working")

@app.route('/balances', methods=['GET'])
def get_balances():
    if not arbitrage_app.cross_engine.account_balances:
        return jsonify({"status": "error", "message": "Balances not yet fetched."}), 503
    return jsonify(arbitrage_app.cross_engine.account_balances)

@app.route('/execute', methods=['POST'])
def execute_trade_endpoint():
    data = request.json
    if not data or not all(k in data for k in ['opportunity_id', 'type', 'strategy']):
        return jsonify({"status": "error", "message": "Missing required parameters"}), 400
    
    if not scanner_loop or not scanner_loop.is_running():
        return jsonify({"status": "error", "message": "Scanner engine is not running."}), 503

    # Safely schedule the coroutine on the scanner's event loop
    future = asyncio.run_coroutine_threadsafe(arbitrage_app.execute_trade_logic(data), scanner_loop)
    
    try:
        # Wait for the result with a timeout
        result = future.result(timeout=60) 
        status_code = 200 if result.status == 'success' else 400
        if 'chat_id' in data and arbitrage_app.notifier:
            # Send trade result notification to the user who requested
            arbitrage_app.notifier.send_trade_result(data['chat_id'], result)
        return jsonify(asdict(result)), status_code
    except asyncio.TimeoutError:
        return jsonify({"status": "error", "message": "Trade execution timed out."}), 504
    except Exception as e:
        logger.error(f"API endpoint error: {e}")
        return jsonify({"status": "error", "message": "An internal server error occurred."}), 500
    
@app.route('/telegram-webhook', methods=['POST'])
def telegram_webhook():
    data = request.json

    # Handle callback query from button
    if 'callback_query' in data:
        callback = data['callback_query']
        chat_id = callback['message']['chat']['id']
        data_str = callback['data']  # e.g., "exec|instant|abc123"

        parts = data_str.split('|')
        if len(parts) != 3:
            return jsonify(ok=True)

        action, subtype, opportunity_id = parts

        if action == "exec":
            # Build mock request for your /execute endpoint
            exec_payload = {
                "type": "cross" if subtype in ("instant", "transfer") else "triangular",
                "strategy": subtype,
                "opportunity_id": opportunity_id
            }

            # Call the coroutine to execute trade
            future = asyncio.run_coroutine_threadsafe(
                arbitrage_app.execute_trade_logic(exec_payload),
                scanner_loop
            )

            try:
                result = future.result(timeout=30)
                if result.status != "success":
                    logger.error(f"Trade execution from Telegram button failed: {result.message}")
                    result = TradeResult("error", f"Trade execution failed: {result.message}")
            except Exception as e:
                logger.error(f"Error executing trade from Telegram button: {e}")
                result = TradeResult("error", "Failed to execute trade from Telegram button.")

            # Send response message
            arbitrage_app.notifier.send_trade_result(chat_id, result)

    return jsonify(ok=True)


def run_flask():
    logger.info(f"Flask API server starting on http://{Config.FLASK_HOST}:{Config.FLASK_PORT}")
    app.run(host=Config.FLASK_HOST, port=Config.FLASK_PORT)

# --- Main Entry Point ---
if __name__ == "__main__":
    try:
        # Run Flask in a separate thread
        flask_thread = Thread(target=run_flask, daemon=True)
        flask_thread.start()
        
        # Run the main asyncio scanner loop
        scanner_loop = asyncio.get_event_loop()
        scanner_loop.run_until_complete(arbitrage_app.run_scanners())

    except KeyboardInterrupt:
        logger.info("Shutdown signal received. Exiting.")
    finally:
        # Perform cleanup
        if scanner_loop.is_running():
            logger.info("Closing exchanges and cleaning up...")
            # Schedule the cleanup coroutine to run on the event loop
            scanner_loop.run_until_complete(arbitrage_app.engine.stop())
        scanner_loop.close()
        logger.info("Application shut down gracefully.")
            