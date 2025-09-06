import asyncio
import traceback
from engine import Engine, logger
from models import TradeResult, Opportunity
from typing import List, Dict
from decimal import Decimal
from datetime import datetime

class CrossExchange(Engine):
    def __init__(self, symbols: list, engine: Engine):
        self.engine = engine
        self.price_cache = {}
        self.symbols = symbols or ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'ADA/USDT']
        self.reconnect_delay = 30  # Wait 30 seconds before retrying a failed connection

    async def start_price_streams(self):
        """Initializes and manages all WebSocket price stream tasks."""
        logger.info("Starting cross-exchange price streams...")
        tasks = []
        
        for exchange_name, exchange in self.engine.exchanges.items():
            for symbol in self.symbols:
                # Create a dedicated, perpetual task for each stream
                task = asyncio.create_task(
                    self.stream_manager(exchange_name, exchange, symbol),
                )
                tasks.append(task)
        
        # Start the separate task for continuously detecting opportunities
        tasks.append(asyncio.create_task(self.continuous_opportunity_detection()))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def stream_manager(self, exchange_name: str, exchange, symbol: str):
        """Manages the lifecycle of a single WebSocket stream with automatic reconnection."""
        while self.engine.running:
            try:
                logger.info(f"[{exchange_name}] Subscribing to {symbol}...")
                await self.stream_orderbook(exchange_name, exchange, symbol)
            except Exception as e:
                logger.error(f"[{exchange_name}] Unhandled exception in stream for {symbol}: {e}. Reconnecting in {self.reconnect_delay}s.")
                await self.engine.reconnect(exchange, self.reconnect_delay)

    async def stream_orderbook(self, exchange_name: str, exchange, symbol: str):
        """The core loop that watches for order book updates from a single stream."""
        while self.engine.running:
            # This is the main blocking call that waits for a new update
            orderbook = await exchange.watch_order_book(symbol)
            
            if exchange_name not in self.price_cache:
                self.price_cache[exchange_name] = {}
            
            # Ensure the order book data is valid before processing
            if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                self.price_cache[exchange_name][symbol] = {
                    'bid': orderbook['bids'][0][0],
                    'ask': orderbook['asks'][0][0],
                    'timestamp': orderbook['timestamp']
                }
                logger.debug(f"Price Update: {exchange_name} {symbol} | Bid: {self.price_cache[exchange_name][symbol]['bid']}, Ask: {self.price_cache[exchange_name][symbol]['ask']}")
            else:
                logger.warning(f"[{exchange_name}] Received invalid order book for {symbol}")

    async def continuous_opportunity_detection(self):
        """Continuously analyzes the in-memory price cache for arbitrage opportunities."""
        while self.engine.running:
            try:
                for symbol in self.symbols:
                    opportunities = self.analyze_symbol_opportunities(symbol)
                    for opp in opportunities:
                        # The base Engine class handles the logic for finding opportunities
                        logger.info(f"Cross-Exchange Opportunity: {opp.trading_pair} | Profit: {opp.profit_percentage:.3f}%")
                        await self.engine.save_opportunity(opp, 'ArbitrageOpportunity')
                
                # Analyze opportunities every 2 seconds
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Error in continuous opportunity detection loop: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(10)

    def analyze_symbol_opportunities(self, symbol: str) -> list:
        """Collects all cached prices for a given symbol and finds opportunities."""
        prices = []
        
        for exchange_name, exchange_data in self.price_cache.items():
            if symbol in exchange_data and exchange_data[symbol].get('bid') and exchange_data[symbol].get('ask'):
                prices.append({
                    'exchange': exchange_name,
                    'symbol': symbol,
                    **exchange_data[symbol]
                })
        
        if len(prices) >= 2:
            return self._find_arbitrage_opportunities(prices)
        
        return []
    
    def _find_arbitrage_opportunities(self, price_data: List[Dict]) -> List[Opportunity]:
        """Analyze price data to find arbitrage opportunities"""
        opportunities = []
        
        if len(price_data) < 2:
            return opportunities
        
        # IMPROVED: Filter out None/invalid prices
        valid_prices = [p for p in price_data if p and p.get('bid') and p.get('ask')]
        if len(valid_prices) < 2:
            return opportunities
        
        sorted_by_ask = sorted(valid_prices, key=lambda x: x['ask'])
        sorted_by_bid = sorted(valid_prices, key=lambda x: x['bid'], reverse=True)
        
        best_buy = sorted_by_ask[0]
        best_sell = sorted_by_bid[0]
        
        if best_buy['exchange'] == best_sell['exchange']:
            return opportunities
        
        buy_price = best_buy['ask']
        sell_price = best_sell['bid']
        
        if sell_price > buy_price:
            profit_percentage = ((sell_price - buy_price) / buy_price) * 100
            
            if profit_percentage >= self.engine.min_profit_threshold:
                trade_amount = 1000
                quantity = trade_amount / buy_price
                gross_profit = quantity * (sell_price - buy_price)
                estimated_fees = trade_amount * 0.002
                net_profit = gross_profit - estimated_fees
                
                opportunity = Opportunity(
                    trading_pair=best_buy['symbol'],
                    buy_exchange=best_buy['exchange'],
                    sell_exchange=best_sell['exchange'],
                    buy_price=buy_price,
                    sell_price=sell_price,
                    profit_percentage=profit_percentage,
                    profit_usd=net_profit,
                    volume=quantity,
                    detected_at=datetime.now().isoformat()
                )
                opportunities.append(opportunity)
        return opportunities
    
    async def _execute_cross_exchange_trade(self, op: dict, req: dict) -> TradeResult:
        """Handles both 'instant' and 'transfer' cross-exchange strategies."""
        strategy = req['strategy']
        amount = float(req.get('amount', self.config.MAX_TRADE_AMOUNT))

        if strategy == 'instant':
            return await self._execute_instant_arbitrage(op, amount)
        elif strategy == 'transfer':
            return await self._execute_transfer_arbitrage(op, amount)
        return TradeResult("error", "Unknown cross-exchange strategy")

    async def _execute_instant_arbitrage(self, op: dict, amount_usd: float) -> TradeResult:
        buy_ex_name, sell_ex_name = op['buy_exchange'], op['sell_exchange']
        buy_exchange, sell_exchange = self.engine.exchanges.get(buy_ex_name), self.engine.exchanges.get(sell_ex_name)
        pair = op['trading_pair']
        base, quote = pair.split('/')
        
        if not all([buy_exchange, sell_exchange]):
            return TradeResult("error", "One of the exchanges is not initialized.")
        
        # 1. Pre-Trade Checks
        if self.engine.account_balances.get(buy_ex_name, {}).get(quote, 0) < amount_usd:
            return TradeResult("error", f"Insufficient {quote} on {buy_ex_name}")
        
        amount_to_trade_base = amount_usd / op['buy_price']
        if self.engine.account_balances.get(sell_ex_name, {}).get(base, 0) < amount_to_trade_base:
            return TradeResult("error", f"Insufficient {base} on {sell_ex_name}")

        # 2. Execute Orders
        try:
            logger.info(f"⚡️ ATTEMPTING INSTANT ARBITRAGE: {pair} | {amount_usd:.2f} {quote}")
            buy_task = buy_exchange.create_market_buy_order(pair, amount_to_trade_base)
            sell_task = sell_exchange.create_market_sell_order(pair, amount_to_trade_base)
            
            results = await asyncio.gather(buy_task, sell_task, return_exceptions=True)
            buy_order, sell_order = results

            if isinstance(buy_order, Exception) or isinstance(sell_order, Exception):
                # Critical: Handle partial failure
                logger.error(f"PARTIAL FAILURE: Buy: {buy_order} | Sell: {sell_order}")
                # Here you would add logic to manually fix the position (e.g., sell what you just bought)
                return TradeResult("error", f"Partial failure. Manual intervention required. Buy: {buy_order}, Sell: {sell_order}")
            
            # 3. Log Success
            profit = (sell_order['cost'] - buy_order['cost']) - (sell_order.get('fee',{}).get('cost',0) + buy_order.get('fee',{}).get('cost',0))
            log = {
                "opportunity_id": op['id'], "trading_pair": pair, "buy_exchange": buy_ex_name,
                "sell_exchange": sell_ex_name, "buy_price": buy_order['average'], "sell_price": sell_order['average'],
                "quantity": buy_order['filled'], "profit_usd": profit, "status": "completed", "strategy": "instant"
            }
            trade_id = await self.engine._log_trade_to_base44(log)
            return TradeResult("success", "Instant arbitrage executed successfully.", trade_id, profit)
            
        except Exception as e:
            logger.error(f"Error during instant execution: {e}\n{traceback.format_exc()}")
            return TradeResult("error", str(e))
    
    async def _execute_transfer_arbitrage(self, op: dict, amount_usd: float) -> TradeResult:
        try:
            buy_ex_name, sell_ex_name = op['buy_exchange'], op['sell_exchange']
            buy_exchange = self.engine.exchanges.get(buy_ex_name)
            sell_exchange = self.engine.exchanges.get(sell_ex_name)
            pair = op['trading_pair']
            base, quote = pair.split('/')
            
            if not all([buy_exchange, sell_exchange]):
                return TradeResult("error", "One of the exchanges is not initialized.")

            # --- 1. Balance Check on Buy Exchange ---
            quote_balance = self.account_balances.get(buy_ex_name, {}).get(quote, 0)
            if quote_balance < amount_usd:
                return TradeResult("error", f"Insufficient {quote} on {buy_ex_name} (have: {quote_balance})")

            # --- 1a. Estimate Slippage ---
            slip = await self.engine.calc_slippage(buy_exchange, pair, 'BUY', Decimal(str(amount_base)))
            if not self.engine._slippage_ok(slip):
                return TradeResult("error", f"Buy slippage too high on {buy_ex_name}: {slip:.4%}")
            
            # --- 2. Place Market Buy Order ---
            amount_base = amount_usd / op['buy_price']
            logger.info(f"[TRANSFER ARB] Buying {amount_base:.6f} {base} on {buy_ex_name} at {op['buy_price']}")
            buy_order = await buy_exchange.create_market_buy_order_with_cost(pair, quote_balance)

            if not buy_order or 'status' not in buy_order or buy_order['status'] != 'closed':
                return TradeResult("error", "Buy order failed or not filled.")
            
            amount_base = buy_order['filled']  # Update to actual filled amount

            # --- 3. Fetch Deposit Address from Sell Exchange ---
            logger.info(f"[TRANSFER ARB] Fetching deposit address for {base} on {sell_ex_name}")
            common_chains = await self.get_common_transfer_chains(base, buy_exchange, sell_exchange)

            if not common_chains:
                return TradeResult("error", f"No common transfer chains for {base} between {buy_ex_name} and {sell_ex_name}")
            
            best_chain, fee = await self.select_best_transfer_chain(common_chains, buy_exchange, base)
            if not best_chain:
                return TradeResult("error", f"Could not determine best transfer chain for {base}")
            
            deposit_info = await sell_exchange.fetch_deposit_address(base, {'network': best_chain})
            address = deposit_info['address']
            tag_or_memo = deposit_info.get('tag', None)  # Some exchanges require this (e.g., XRP, XLM)

            # --- 4. Withdraw from Buy Exchange ---
            logger.info(f"[TRANSFER ARB] Withdrawing {amount_base:.6f} {base} from {buy_ex_name} to {sell_ex_name}")
            withdrawal = await buy_exchange.withdraw(
                code=base,
                amount=amount_base,
                address=address,
                tag=tag_or_memo,
                params={'network': best_chain}
            )

            # --- 5. Wait for Deposit on Sell Exchange ---
            logger.info(f"[TRANSFER ARB] Waiting for deposit of {amount_base:.6f} {base} on {sell_ex_name}")
            max_wait = 600  # 10 minutes max
            interval = 10
            waited = 0

            while waited < max_wait:
                balance = await sell_exchange.fetch_balance()
                if balance['total'].get(base, 0) >= (amount_base - fee) * 0.99:  # Allow small margin
                    logger.info(f"[TRANSFER ARB] Deposit received on {sell_ex_name}")
                    amount_received = balance['total'][base]
                    break
                await asyncio.sleep(interval)
                waited += interval
            else:
                return TradeResult("error", "Timeout waiting for deposit on sell exchange.")

            # --- 6a. Estimate Slippage ---
            slip = await self.engine.calc_slippage(sell_exchange, pair, 'SELL', Decimal(str(amount_received)))
            if not self.engine._slippage_ok(slip):
                return TradeResult("error", f"Sell slippage too high on {sell_ex_name}: {slip:.4%}")
            
            # --- 6b. Sell on Sell Exchange ---
            logger.info(f"[TRANSFER ARB] Selling {amount_base:.6f} {base} on {sell_ex_name}")
            sell_order = await sell_exchange.create_market_sell_order(pair, amount_received)

            # --- 7. Calculate Profit ---
            buy_cost = buy_order['cost']
            sell_cost = sell_order['cost']
            buy_fee = buy_order.get('fee', {}).get('cost', 0)
            sell_fee = sell_order.get('fee', {}).get('cost', 0)
            withdraw_fee = (withdrawal.get('fee') or {}).get('cost', fee)

            profit = sell_cost - buy_cost - buy_fee - sell_fee - withdraw_fee

            # --- 8. Log Trade ---
            trade_log = {
                "opportunity_id": op['id'],
                "trading_pair": pair,
                "buy_exchange": buy_ex_name,
                "sell_exchange": sell_ex_name,
                "buy_price": buy_order.get('average'),
                "sell_price": sell_order.get('average'),
                "quantity": amount_base,
                "profit_usd": profit,
                "status": "completed",
                "strategy": "transfer"
            }

            trade_id = await self.engine._log_trade_to_base44(trade_log)

            logger.info(f"[TRANSFER ARB] Transfer arbitrage successful. Profit: ${profit:.2f}")
            return TradeResult("success", "Transfer arbitrage executed successfully.", trade_id, profit)

        except Exception as e:
            logger.error(f"[TRANSFER ARB] Error: {e}")
            logger.error(traceback.format_exc())
            return TradeResult("error", f"Exception occurred: {str(e)}")

    async def get_common_transfer_chains(self, asset: str, buy_exchange, sell_exchange) -> list[str]:
        """Fetch common transfer chains (networks) for an asset between two exchanges."""
        try:
            # Fetch available currency info from both exchanges
            buy_currencies = await buy_exchange.fetch_currencies()
            sell_currencies = await sell_exchange.fetch_currencies()

            buy_chains = set()
            sell_chains = set()

            # Get withdrawable chains from buy exchange
            for key, info in buy_currencies.items():
                if key.upper() == asset.upper() and 'networks' in info:
                    buy_chains.update(info['networks'].keys())

            # Get depositable chains from sell exchange
            for key, info in sell_currencies.items():
                if key.upper() == asset.upper() and 'networks' in info:
                    sell_chains.update(info['networks'].keys())

            common_chains = buy_chains & sell_chains
            return list(common_chains)

        except Exception as e:
            logger.error(f"[CHAIN MATCHING] Error fetching common chains: {e}")
            return []

    async def select_best_transfer_chain(self, common_chains: list, buy_exchange, asset: str) -> str:
        """
        From a list of common chains, select the best one based on lowest withdrawal fee
        and fastest estimated transfer speed.
        """
        try:
            buy_currencies = await buy_exchange.fetch_currencies()
            best_chain = None
            best_score = float('inf')

            for chain in common_chains:
                # Get chain info
                currency_info = buy_currencies.get(asset.upper())
                network_info = currency_info.get('networks', {}).get(chain)

                if not network_info:
                    continue
                
                fee = network_info.get('fee', 0)
                

                # Weighted scoring: prioritize speed and fee
                score = fee 

                if score < best_score:
                    best_score = score
                    best_chain = chain

            return best_chain, best_score

        except Exception as e:
            logger.error(f"[CHAIN SCORING] Error selecting best chain: {e}")
            return None, None
    
    async def _log_pending_transfer(self, op: dict, amount_usd: float) -> TradeResult:
        logger.info(f"Logging 'Transfer & Trade' for opportunity {op['id']}")
        log = {
            "opportunity_id": op['id'], "trading_pair": op['trading_pair'], "buy_exchange": op['buy_exchange'],
            "sell_exchange": op['sell_exchange'], "buy_price": op['buy_price'], "sell_price": op['sell_price'],
            "quantity": amount_usd / op['buy_price'], "profit_usd": op['profit_usd'], "status": "pending", "strategy": "transfer"
        }
        trade_id = await self.engine._log_trade_to_base44(log)
        if trade_id:
            return TradeResult("success", "Trade logged as 'pending'. Manual transfer required.", trade_id)
        return TradeResult("error", "Failed to log the pending trade.")
    
    async def start(self):
        """The main entry point for the CrossExchange engine."""
        logger.info("Initializing Cross-Exchange Arbitrage Engine...")
        self.engine.running = True
        
        # await self.engine.initialize_exchanges()
        
        if not self.engine.exchanges:
            logger.error("No exchanges configured. Stopping cross-exchange engine.")
            return
        
        await self.start_price_streams()
