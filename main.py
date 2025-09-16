import asyncio
from dataclasses import asdict
from quart import Quart, jsonify, request

# Custom imports
from config import Config
from models import TradeResult
from arbitrage import ArbitrageApp, logger


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

# @app.route('/telegram-webhook', methods=['POST'])
# async def telegram_webhook():
#     data = await request.get_json()

#     if 'callback_query' in data:
#         callback = data['callback_query']
#         chat_id = callback['message']['chat']['id']
#         data_str = callback['data']  # e.g., "exec|instant|abc123"

#         parts = data_str.split('|')
#         if len(parts) != 3:
#             return jsonify(ok=True)

#         action, subtype, opportunity_id = parts

#         if action == "exec":
#             exec_payload = {
#                 "type": "cross" if subtype in ("instant", "transfer") else "triangular",
#                 "strategy": subtype,
#                 "opportunity_id": opportunity_id
#             }

#             try:
#                 result = await arbitrage_app.execute_trade_logic(exec_payload)
#                 if result.status != "success":
#                     logger.error(f"Trade execution from Telegram button failed: {result.message}")
#                     result = TradeResult("error", f"Trade execution failed: {result.message}")
#             except Exception as e:
#                 logger.error(f"Error executing trade from Telegram button: {e}")
#                 result = TradeResult("error", "Failed to execute trade from Telegram button.")

#             await arbitrage_app.notifier.send_trade_result(chat_id, result)

#     return jsonify(ok=True)


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
