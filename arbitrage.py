import asyncio
import ccxt.pro as ccxt
import json
import logging
import time
import traceback
import os
import aiohttp

from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from flask import Flask, jsonify, request
from threading import Thread
from dotenv import load_dotenv
from arbitrage_engine import ArbitrageEngine
from config import Config

# --- Basic Setup ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Data Structures ---
@dataclass
class TradeResult:
    status: str
    message: str
    trade_id: Optional[str] = None
    profit_usd: Optional[float] = None

# --- Main Engine ---
class Arbitrage:
    def __init__(
            self, 
            base44_api_url: str, 
            app_token: str,
            symbols: list = None,
            profit_threshold: float = None,
            max_trade_amount: float = None,
            exchanges: Dict[str, ccxt.Exchange] = {}
        ):
        # (Unchanged from previous version)
        self.base44_api_url = base44_api_url 
        self.app_token = app_token 
        self.headers = {'Authorization': f'Bearer {self.app_token}'}
        self.exchanges = exchanges
        self.symbols = symbols
        self.min_profit_threshold = profit_threshold
        self.running = False
        self.account_balances = {}
        self.scanner = ArbitrageEngine(self.base44_api_url, self.app_token)

    # --- (Scanner and Initializer functions are unchanged) ---
    async def initialize_exchanges(self):
        # ... existing implementation ...
        await self.scanner.initialize_exchanges()
    
    async def update_account_balances(self):
        """Fetch and update balances for all connected exchanges."""
        logger.info("Updating account balances...")
        all_balances = {}
        for name, exchange in self.scanner.exchanges.items():
            try:
                balance = await exchange.fetch_balance()
                # Filter for non-zero balances
                all_balances[name] = {
                    asset: data['total'] 
                    for asset, data in balance['total'].items() if data > 0
                }
            except Exception as e:
                logger.error(f"Could not fetch balance for {name}: {e}")
        self.account_balances = all_balances
        logger.info("Account balances updated.")
    
    async def run_scanner(self):
        """Main scanner loop."""
        await self.scanner.scan()


    # ---vvv NEW AND UPDATED TRADE EXECUTION LOGIC vvv---

    async def _log_trade_to_base44(self, entity_data: dict) -> Optional[str]:
        """Logs a completed or pending trade to the 'Trade' entity."""
        try:
            async with aiohttp.ClientSession() as session:
                response = await session.post(
                    f'{self.base44_api_url}/api/entities/Trade',
                    headers=self.headers,
                    json=entity_data
                )
                if response.status == 200 or response.status == 201:
                    result = await response.json()
                    logger.info(f"Successfully logged trade to base44. ID: {result.get('id')}")
                    return result.get('id')
                else:
                    logger.error(f"Failed to log trade to base44. Status: {response.status}, Response: {await response.text()}")
            return None
        except Exception as e:
            logger.error(f"Exception while logging trade: {e}")
            return None

    async def _execute_instant_arbitrage(self, op: dict, trade_amount_usd: float) -> TradeResult:
        """Executes a pre-funded, cross-exchange arbitrage trade."""
        buy_ex_name, sell_ex_name = op['buy_exchange'], op['sell_exchange']
        buy_exchange, sell_exchange = self.exchanges[buy_ex_name], self.exchanges[sell_ex_name]
        pair = op['trading_pair']
        base_asset, quote_asset = pair.split('/')

        # 1. Pre-Trade Checks
        await self.update_account_balances()
        buy_ex_balance = self.account_balances.get(buy_ex_name, {})
        sell_ex_balance = self.account_balances.get(sell_ex_name, {})
        
        required_quote_balance = trade_amount_usd
        # Use a recent price to estimate crypto amount needed
        amount_to_trade = trade_amount_usd / op['buy_price']

        if buy_ex_balance.get(quote_asset, 0) < required_quote_balance:
            return TradeResult("error", f"Insufficient {quote_asset} on {buy_ex_name}")
        if sell_ex_balance.get(base_asset, 0) < amount_to_trade:
            return TradeResult("error", f"Insufficient {base_asset} on {sell_ex_name}")
            
        # 2. Execute Orders Simultaneously
        try:
            logger.info(f"Executing simultaneous orders for {pair}...")
            buy_order_task = buy_exchange.create_market_buy_order(pair, amount_to_trade)
            sell_order_task = sell_exchange.create_market_sell_order(pair, amount_to_trade)
            
            results = await asyncio.gather(buy_order_task, sell_order_task, return_exceptions=True)
            
            buy_result, sell_result = results
            if isinstance(buy_result, Exception) or isinstance(sell_result, Exception):
                # Handle partial failure if one order succeeded
                raise Exception(f"One or more orders failed. Buy: {buy_result}, Sell: {sell_result}")

            # 3. Log Success
            profit = (sell_result['cost'] - buy_result['cost']) - (sell_result.get('fee', {}).get('cost', 0) + buy_result.get('fee', {}).get('cost', 0))
            trade_log = {
                "opportunity_id": op['id'], "trading_pair": pair, "buy_exchange": buy_ex_name,
                "sell_exchange": sell_ex_name, "buy_price": buy_result['average'], "sell_price": sell_result['average'],
                "quantity": buy_result['filled'], "profit_usd": profit, "status": "completed", "strategy": "instant"
            }
            trade_id = await self._log_trade_to_base44(trade_log)
            return TradeResult("success", "Instant arbitrage executed successfully.", trade_id, profit)

        except Exception as e:
            logger.error(f"Error during instant execution: {e}")
            return TradeResult("error", str(e))

    async def _execute_transfer_and_trade(self, op: dict) -> TradeResult:
        """Logs the intent for a transfer-based trade."""
        logger.info(f"Logging 'Transfer & Trade' for opportunity {op['id']}")
        trade_log = {
            "opportunity_id": op['id'], "trading_pair": op['trading_pair'], "buy_exchange": op['buy_exchange'],
            "sell_exchange": op['sell_exchange'], "buy_price": op['buy_price'], "sell_price": op['sell_price'],
            "quantity": 0, "profit_usd": op['profit_usd'], "status": "pending_transfer", "strategy": "transfer"
        }
        trade_id = await self._log_trade_to_base44(trade_log)
        if trade_id:
            return TradeResult("success", "Trade logged. Manual transfer required.", trade_id)
        else:
            return TradeResult("error", "Failed to log the pending trade.")

    async def _execute_triangular_arbitrage(self, op: dict, trade_amount_usd: float) -> TradeResult:
        """Executes a 3-leg sequential trade on a single exchange."""
        exchange = self.exchanges[op['exchange']]
        asset1, asset2, asset3 = op['assets']
        pair1, pair2, pair3 = op['trading_path']

        # 1. Pre-Trade Checks
        await self.update_account_balances()
        balance = self.account_balances.get(op['exchange'], {})
        if balance.get(asset1, 0) < trade_amount_usd:
            return TradeResult("error", f"Insufficient {asset1} on {op['exchange']}")

        # 2. Execute Orders Sequentially
        try:
            # Leg 1: Buy asset2 with asset1
            logger.info(f"Leg 1: Buying {pair1}...")
            leg1_order = await exchange.create_market_buy_order(pair1, trade_amount_usd / op['buy_price']) # A bit of a simplification
            amount_asset2 = leg1_order['filled'] * (1 - leg1_order.get('fee', {}).get('rate', 0.001))
            
            # Leg 2: Buy asset3 with asset2
            logger.info(f"Leg 2: Buying {pair2}...")
            leg2_order = await exchange.create_market_buy_order(pair2, amount_asset2)
            amount_asset3 = leg2_order['filled'] * (1 - leg2_order.get('fee', {}).get('rate', 0.001))

            # Leg 3: Sell asset3 for asset1
            logger.info(f"Leg 3: Selling {pair3}...")
            leg3_order = await exchange.create_market_sell_order(pair3, amount_asset3)
            final_amount_asset1 = leg3_order['cost'] * (1 - leg3_order.get('fee', {}).get('rate', 0.001))

            profit = final_amount_asset1 - trade_amount_usd
            trade_log = {
                "opportunity_id": op['id'], "trading_pair": ' -> '.join(op['assets']), "buy_exchange": op['exchange'],
                "sell_exchange": op['exchange'], "quantity": trade_amount_usd,
                "profit_usd": profit, "status": "completed", "strategy": "triangular"
            }
            trade_id = await self._log_trade_to_base44(trade_log)
            return TradeResult("success", "Triangular trade executed.", trade_id, profit)

        except Exception as e:
            logger.error(f"Error during triangular execution: {e}")
            # Potentially add logic here to sell any acquired asset back to the original asset to prevent getting stuck
            return TradeResult("error", f"Failed on one of the legs: {e}")

    async def execute_trade_logic(self, trade_request: dict) -> TradeResult:
        """Main router for all trade execution requests."""
        opportunity_id = trade_request['opportunity_id']
        op_type = trade_request['type']
        strategy = trade_request['strategy']
        trade_amount_usd = float(trade_request.get('amount', 100.0)) # Default to $100 trade

        # Fetch opportunity from base44 DB
        entity_name = 'ArbitrageOpportunity' if op_type == 'cross' else 'TriangularOpportunity'
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.get(f'{self.base44_api_url}/api/entities/{entity_name}/{opportunity_id}', headers=self.headers)
                op = await resp.json()
        except Exception as e:
            return TradeResult("error", f"Could not fetch opportunity details: {e}")

        # Route to the correct execution function
        if op_type == 'cross':
            if strategy == 'instant':
                return await self._execute_instant_arbitrage(op, trade_amount_usd)
            elif strategy == 'transfer':
                return await self._execute_transfer_and_trade(op)
        elif op_type == 'triangular':
            return await self._execute_triangular_arbitrage(op, trade_amount_usd)
        
        return TradeResult("error", "Invalid trade type or strategy")

# --- API Server (Flask) ---
# ... (Flask app setup is unchanged) ...
app = Flask(__name__)
# ... (engine instantiation is unchanged) ...
engine = Arbitrage(
    base44_api_url=Config.BASE44_API_URL,
    app_token=Config.APP_TOKEN,
    symbols=Config.SYMBOLS,
    profit_threshold=Config.MIN_PROFIT_THRESHOLD,
    max_trade_amount=Config.MAX_TRADE_AMOUNT
)

@app.route('/')
def index():
    return ('Hello there, I\'m busy working')
@app.route('/balances', methods=['GET'])
def get_balances():
    return jsonify(engine.account_balances)

@app.route('/execute', methods=['POST'])
def execute_trade_endpoint():
    data = request.json # Expects { opportunity_id, type, strategy, amount }
    
    if not all(k in data for k in ['opportunity_id', 'type', 'strategy']):
        return jsonify({"status": "error", "message": "Missing parameters"}), 400
        
    future = asyncio.run_coroutine_threadsafe(engine.execute_trade_logic(data), engine_loop)
    result = future.result()
    return jsonify(asdict(result))

def run_flask_app():
    # Note: For production, use a proper WSGI server like Gunicorn
    app.run(port=5000, host='0.0.0.0')

# --- Main Entry Point ---
# ... (Main entry point is unchanged) ...
if __name__ == "__main__":
    # Run the scanner in a separate thread
    scanner_thread = Thread(target=lambda: asyncio.run(engine.run_scanner()), daemon=True)
    scanner_thread.start()
    
    # Get the event loop the engine is running on
    engine_loop = asyncio.get_event_loop()
    
    # Run the Flask API server in the main thread
    run_flask_app()