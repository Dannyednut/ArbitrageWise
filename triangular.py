import asyncio, os
import ccxt.pro as ccxt
from datetime import datetime
from engine import Engine, logger
from models import TradeResult, Opportunity
from typing import Dict, List, Optional
from decimal import Decimal
import traceback

class Triangular:
    def __init__(self, engine: Engine):
        self.engine = engine
        self.paths_by_pair = {} 
        self.order_book_cache = {}
        self.reconnect_delay = 30

    async def start_ops(self):
        """Orchestrates the setup and launch of perpetual scanner tasks for each exchange."""
        logger.info("Starting triangular opportunity scanning...")
        tasks = []
        
        for exchange_name, exchange in self.engine.exchanges.items():
            task = asyncio.create_task(
                self.exchange_scanner_loop(exchange_name, exchange)
            )
            tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            logger.warning("No exchanges available for triangular scanning")

    async def exchange_scanner_loop(self, exchange_name: str, exchange: ccxt.Exchange):
        """The main perpetual loop for a single exchange with improved error handling."""
        logger.info(f"[{exchange_name}] Initializing triangular scanner...")
        
        # Generate paths once at startup
        all_paths = await self._generate_triangular_paths(exchange)
        if not all_paths:
            logger.warning(f"[{exchange_name}] No triangular paths found. Stopping scanner.")
            return

        unique_symbols = self._build_path_lookup_map(exchange_name, all_paths)
        logger.info(f"[{exchange_name}] Generated {len(all_paths)} paths across {len(unique_symbols)} symbols.")

        # IMPROVED: Limit symbols to prevent API limits
        symbols_to_watch = unique_symbols[:20]  # Watch top 20 most liquid pairs
        tasks = []

        try:
            # FIXED: Better WebSocket management with error handling
            for symbol in symbols_to_watch:
                task = asyncio.create_task(self._watch_symbol(exchange_name, exchange, symbol))
                tasks.append(task)

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            else:
                logger.warning("No symbol available to watch for triangular scanning")
                    
        except Exception as e:
            logger.error(f"[{exchange_name}] Major error in scanner: {e}")
            logger.error(traceback.format_exc())
            await asyncio.sleep(self.reconnect_delay)

    async def _watch_symbol(self, exchange_name, exchange, symbol):
        while self.engine.running:
            try:
                ob = await asyncio.wait_for(exchange.watch_order_book(symbol), timeout=None)
                self.order_book_cache[exchange_name][symbol] = ob

                affected_paths = self.paths_by_pair[exchange_name].get(symbol, [])
                for path in affected_paths:
                    asyncio.create_task(self._calculate_path_profit(exchange_name, exchange, path))

            except asyncio.TimeoutError:
                logger.warning(f"[{exchange_name}] Timeout for {symbol}")
            except Exception as e:
                logger.warning(f"[{exchange_name}] Error in symbol watcher for {symbol}: {e}")
                await asyncio.sleep(10)  # backoff before reconnect
                
    # IMPROVED: Better path calculation with validation
    async def _calculate_path_profit(self, exchange_name: str, exchange, path: tuple):
        """Calculates the profit for a single triangular path with improved validation."""
        asset1, asset2, asset3, pair1, pair2, pair3 = path
        
        try:
            cache = self.order_book_cache.get(exchange_name, {})
            ob1, ob2, ob3 = cache.get(pair1), cache.get(pair2), cache.get(pair3)

            if not all([ob1, ob2, ob3]):
                return

            # Validate order books have sufficient depth
            if not (ob1.get('asks') and ob2.get('asks') and ob3.get('bids') and 
                    len(ob1['asks']) > 0 and len(ob2['asks']) > 0 and len(ob3['bids']) > 0):
                return

            initial_usdt = 1000.0
            
            # IMPROVED: More accurate rate calculation
            # Trade 1: Buy asset2 using asset1
            rate1 = self._get_conversion_rate(pair1, asset2, asset1, ob1, is_buy=True)
            if rate1 is None or rate1 == 0:
                return
            amount_asset2 = initial_usdt * 0.999 / rate1

            # Trade 2: Buy asset3 using asset2
            rate2 = self._get_conversion_rate(pair2, asset3, asset2, ob2, is_buy=True)
            if rate2 is None or rate2 == 0:
                return
            amount_asset3 = amount_asset2 * 0.999 / rate2

            # Trade 3: Sell asset3 for asset1
            rate3 = self._get_conversion_rate(pair3, asset3, asset1, ob3, is_buy=False)
            if rate3 is None or rate3 == 0:
                return
            final_amount = amount_asset3 * rate3
            
            # IMPROVED: Account for trading fees (0.1% per trade = 0.3% total)
            final_amount_after_fees = final_amount * 0.999
            profit_percentage = ((final_amount_after_fees - initial_usdt) / initial_usdt) * 100
            
            if profit_percentage > self.engine.min_profit_threshold:
                opp = Opportunity(
                    exchange=exchange_name,
                    trading_path=[pair1, pair2, pair3],
                    assets=[asset1, asset2, asset3],
                    profit_percentage=profit_percentage,
                    initial_amount=initial_usdt,
                    final_amount=final_amount_after_fees,
                    detected_at=datetime.now().isoformat()
                )
                
                logger.info(f"Triangular Opportunity: {exchange_name} | {asset1}=>{asset2}=>{asset3} | {profit_percentage:.3f}%")
                await self.engine.save_opportunity(opp, 'TriangularOpportunity')

        except (KeyError, IndexError, ZeroDivisionError, TypeError) as e:
            logger.debug(f"Data error for path {path}: {e}")
        except Exception as e:
            logger.warning(f"Calculation error for path {path}: {e}")

    # Rest of the methods remain the same but with better error handling...
    def _build_path_lookup_map(self, exchange_name: str, all_paths: list) -> list:
        """Creates a map of {symbol: [paths]} for fast lookups and returns all unique symbols."""
        self.paths_by_pair[exchange_name] = {}
        self.order_book_cache[exchange_name] = {}
        unique_symbols = set()

        for path in all_paths:
            asset1, _, _, pair1, pair2, pair3 = path
            
            for pair in [pair1, pair2, pair3]:
                if pair not in self.paths_by_pair[exchange_name]:
                    self.paths_by_pair[exchange_name][pair] = []
                self.paths_by_pair[exchange_name][pair].append(path)
                unique_symbols.add(pair)
        
        return list(unique_symbols)

    async def _generate_triangular_paths(self, exchange) -> list:
        """Generate triangular arbitrage paths from active spot markets with optional asset filtering."""
        try:
            markets = await exchange.load_markets()
            
            # Step 1: Filter only valid spot markets
            spot_markets = {
                symbol: market for symbol, market in markets.items()
                if market.get('spot') and market.get('active') and '/' in symbol
            }
            
            logger.info(f"[{exchange.id}] Found {len(spot_markets)} active spot markets.")

            # Step 2: Build asset-to-pairs map
            asset_pairs = {}
            for symbol in spot_markets:
                base, quote = symbol.split('/')
                for asset in [base, quote]:
                    asset_pairs.setdefault(asset, []).append(symbol)

            # Step 3: Find all valid triangular paths
            valid_paths = set()
            for pair1 in spot_markets:
                asset1, asset2 = pair1.split('/')
                
                for pair2 in asset_pairs.get(asset2, []):
                    if pair2 == pair1:
                        continue
                    
                    base2, quote2 = pair2.split('/')
                    asset3 = base2 if base2 != asset2 else quote2

                    # Look for closing pair that completes the cycle
                    pair3_options = [f"{asset3}/{asset1}", f"{asset1}/{asset3}"]
                    for pair3 in pair3_options:
                        if pair3 in spot_markets:
                            valid_paths.add((asset1, asset2, asset3, pair1, pair2, pair3))
                            break

            logger.info(f"[{exchange.id}] Generated {len(valid_paths)} total triangular paths.")

            # Step 4 (Optional): Filter paths involving only high-liquidity or priority assets
            priority_assets = {'USDT', 'BTC', 'ETH', 'BNB', 'USDC'}
            filtered_paths = [
                path for path in valid_paths
                if any(asset in priority_assets for asset in path[:3])
            ]

            logger.info(f"[{exchange.id}] Filtered to {len(filtered_paths)} priority paths.")
            await exchange.close()
            return list(filtered_paths)

        except Exception as e:
            logger.info(f"[{exchange.id}] Failed to generate triangular paths: {e}")
            return []

    def _get_conversion_rate(self, pair, base, quote, ob, is_buy):
        """Returns the correct rate based on trade direction."""
        if not ob or not ob.get('asks') or not ob.get('bids'):
            return None
        if pair == f"{base}/{quote}":
            return ob['asks'][0][0] if is_buy else ob['bids'][0][0]
        elif pair == f"{quote}/{base}":
            price = ob['bids'][0][0] if is_buy else ob['asks'][0][0]
            return 1 / price if price != 0 else None
        return None

    async def _execute_triangular_trade(self, op: dict, req: dict) -> TradeResult:
        exchange_name = op['exchange']
        exchange:Optional[ccxt.Exchange] = self.engine.exchanges.get(exchange_name)
        asset1, asset2, asset3 = op['assets']
        pair1, pair2, pair3 = op['trading_path']
        amount1 = float(req.get('amount', op['initial_amount']))

        if not exchange:
            return TradeResult("error", f"Exchange '{exchange_name}' not initialized.")
          
        # 1. Pre-Trade Checks
        if self.account_balances.get(exchange_name, {}).get(asset1, 0) < amount1:
            return TradeResult("error", f"Insufficient {asset1} on {exchange_name}")

        # 2. Execute Sequential Orders
        try:
            # LEG 1: asset1 -> asset2
            logger.info(f"LEG 1: Buying {pair1} with {amount1} {asset1}")
            amount2 = self.try_trade_leg(exchange, pair1, asset1, amount1)
            if isinstance(amount2, TradeResult) and amount2.status == "error":
                return amount2
            
            # LEG 2: asset2 -> asset3
            logger.info(f"LEG 2: Trading {amount2} {asset2} via {pair2}")
            amount3 = self.try_trade_leg(exchange, pair2, asset2, amount2)
            if isinstance(amount3, TradeResult) and amount3.status == "error":
                return amount3

            # LEG 3: asset3 -> asset1
            logger.info(f"LEG 3: Trading {amount3} {asset3} via {pair3}")
            final_amount = self.try_trade_leg(exchange, pair3, asset3, amount3)
            if isinstance(final_amount, TradeResult) and final_amount.status == "error":
                return final_amount

            # 3. Log Success
            profit = final_amount - amount1
            log = {
                "opportunity_id": op['id'], "trading_pair": ' -> '.join(op['assets']), "buy_exchange": exchange_name,
                "sell_exchange": exchange_name, "quantity": amount1,
                "profit_usd": profit, "status": "completed", "strategy": "triangular"
            }
            trade_id = await self.engine._log_trade_to_base44(log)
            return TradeResult("success", "Triangular trade executed successfully.", trade_id, profit)

        except Exception as e:
            logger.error(f"Error during triangular execution: {e}\n{traceback.format_exc()}")
            return TradeResult("error", f"Failed on one of the legs: {e}. Manual intervention required.")
    
    async def try_trade_leg(self, exchange, pair, asset, amount):
        pair_base, pair_quote = pair.split('/')
        side = "BUY" if asset == pair_quote else "SELL"
        asset2 = pair_base if side == "BUY" else pair_quote

        slip = self.engine.calc_slippage(exchange, pair, side, Decimal(amount))
        if not self.engine._slippage_ok(slip):
            return TradeResult("error", f"High slippage on {pair} buy: {slip:.2f}%")
        try:
            if side == "BUY":
                order = await exchange.create_market_buy_order_with_cost(pair, amount)
            else:
                order = await exchange.create_market_sell_order(pair, amount)

            return self.net_amount_received(order, asset2)
        except Exception as e:
            logger.error(f"[TRADE LEG] Failed: {pair}, {side}, {amount}: {e}")
            return TradeResult("error", f"Trade leg failed: {e}")

    def net_amount_received(self, order: dict, received_asset: str) -> float:
        """
        Calculate net amount received of the target asset after deducting fees,
        only if the fee is in that asset.
        """
        fee = order.get("fee", {})
        fee_cost = fee.get("cost", 0) or 0
        fee_currency = fee.get("currency")

        if received_asset == order["symbol"].split("/")[0]:  # base asset
            gross_received = order.get("filled", 0)
        else:  # quote asset
            gross_received = order.get("cost", 0)

        if fee_currency == received_asset:
            return gross_received - fee_cost
        else:
            return gross_received
        
    async def start(self):
        """Start the triangular arbitrage engine"""
        logger.info("Starting Triangular Arbitrage Engine...")
        self.engine.running = True
        
        # await self.engine.initialize_exchanges()
        
        if not self.engine.exchanges:
            logger.error("No exchanges configured for triangular arbitrage")
            return
        
        logger.info(f"Initialized {len(self.engine.exchanges)} exchanges: {list(self.engine.exchanges.keys())}")
        await self.start_ops()