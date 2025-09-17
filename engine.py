import asyncio
import ccxt.pro as ccxt
import json, os
import logging
import time
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional
import aiohttp
from models import Opportunity, asdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('arbitrage.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Engine:
    """Core Arbitrage Engine handling exchange initialization, balance updates, and opportunity logging."""
    account_balances: Dict[str, Dict[str, float]] = {}  # {exchange: {asset: balance}}
    slippage_tolerance = Decimal('0.002')

    def __init__(self, base44_api_url: str, app_token: str, notifier = None):
        self.base44_api_url = base44_api_url
        self.app_token = app_token
        self.notifier = notifier
        # FIXED: Consistent API headers
        self.headers = {
            'api_key': self.app_token,
            'Content-Type': 'application/json'
        }
        self.exchanges: Dict[str, ccxt.Exchange] = {}
        self.min_profit_threshold = 0.3
        self.reconnect_delay = 30
        self.seen_opportunities = set()  # Cache of hashes
        self.cache_ttl = 60  # seconds
        self.loaded_config = None
        self.seen_timestamps = {}  # Optional: for TTL cleanup
        self.running = False
    
    async def fetch_exchanges(self):
        """Load exchange configurations from base44 database"""
        try:
            async with aiohttp.ClientSession() as session:
                # FIXED: Consistent headers
                async with session.get(f'{self.base44_api_url}/entities/Exchange', headers=self.headers) as response:
                    if response.status != 200:
                        logger.error(f"Failed to fetch exchanges: {response.status}")
                        return None
                    exchange_configs = await response.json()
                    return exchange_configs
        except Exception as e:
            logger.error(f"Failed to fetch exchange configurations from base44: {e}")

    async def watch_base44_config(self):
        while True:
            if not self.loaded_config:
                self.loaded_config = await self.fetch_exchanges()

            load_config = await self.fetch_exchanges()
            if not load_config:
                continue

            if not load_config == self.loaded_config:
                logger.info("Detect configuration update. Re-initializing exchanges...")
                await self.stop()
                await asyncio.sleep(2)
                await self.initialize_exchanges(load_config)
            await asyncio.sleep(1800)

    async def initialize_exchanges(self, config=None):
        """Initialize exchanges"""
        try:
            exchange_configs = await self.fetch_exchanges() if not config else config
            if not exchange_configs:
                return
            for config in exchange_configs:
                if not config.get('is_active', False):
                    continue
                    
                exchange_name = config['name'].lower()
                
                try:
                    ccxt_name = self._get_ccxt_name(exchange_name)
                    if not hasattr(ccxt, ccxt_name):
                        logger.warning(f"CCXT does not support {ccxt_name}")
                        continue
                        
                    exchange_class = getattr(ccxt, ccxt_name)
                    
                    exchange = exchange_class({
                        'apiKey': config.get('api_key', ''),
                        'secret': config.get('api_secret', ''),
                        'options': {'defaultType': 'spot'},
                        'enableRateLimit': True,
                        'sandbox': True,  # Set to True for testing
                        'password': config.get('api_passphrase', ('Tunddy20' if not ccxt_name=='okx' else "Tunddy@20")),
                        #'urls': {'api': {'rest': 'https://api-testnet.bitget.com'}} if ccxt_name == 'bitget' else {}
                    })
                    
                    # Test connection with timeout
                    await asyncio.wait_for(exchange.load_markets(), timeout=30.0)
                    self.exchanges[exchange_name] = exchange
                    logger.info(f"Successfully initialized {exchange_name}")
                    
                except asyncio.TimeoutError:
                    logger.error(f"Timeout initializing {exchange_name}")
                    await exchange.close()
                except Exception as e:
                    logger.error(f"Failed to initialize {exchange_name}: {e}")
                    await exchange.close()
            self.loaded_config = exchange_configs       
        except Exception as e:
            logger.error(f"Failed to load exchange configurations: {e}")
    
    def _get_ccxt_name(self, exchange_name: str) -> str:
        """Map exchange names to ccxt identifiers"""
        mapping = {
            'binance': 'binance',
            'coinbase': 'coinbasepro',
            'coinbase pro': 'coinbase',
            'kraken': 'kraken',
            'kucoin': 'kucoin',
            'bybit': 'bybit',
            'mexc': 'mexc',
            'huobi': 'huobi',
            'okx': 'okx',
            'gate.io': 'gate'
        }
        return mapping.get(exchange_name.lower(), exchange_name.lower())
    
    async def calc_slippage(self, exchange, symbol: str, side: str, qty: Decimal) -> Optional[Decimal]:
        """
        Estimates slippage for a market order on the given exchange and symbol.

        Args:
            exchange: ccxt.pro exchange instance
            symbol: e.g. 'BTC/USDT'
            side: 'BUY' or 'SELL'
            qty: quote quantity for BUY (e.g. spend $1000 USDT), base quantity for SELL (e.g. sell 0.1 BTC)

        Returns:
            Decimal representing slippage as a fraction (e.g., 0.002 = 0.2%)
        """
        try:
            orderbook = await exchange.fetch_order_book(symbol)
            if not orderbook or not orderbook.get('bids') or not orderbook.get('asks'):
                return None

            side = side.upper()
            levels = orderbook['asks'] if side == 'BUY' else orderbook['bids']
            filled_base = Decimal('0')
            spent_quote = Decimal('0')

            if side == 'BUY':
                remaining_quote = qty

                for level in levels:
                    if len(level) < 2:
                        continue
                    price_d = Decimal(str(level[0]))
                    amount_d = Decimal(str(level[1]))
                    level_quote = price_d * amount_d

                    if remaining_quote <= level_quote:
                        take_quote = remaining_quote
                        take_base = take_quote / price_d
                    else:
                        take_quote = level_quote
                        take_base = amount_d

                    filled_base += take_base
                    spent_quote += take_quote
                    remaining_quote -= take_quote

                    if remaining_quote <= 0:
                        break

                if remaining_quote > 0:
                    return None  # Not enough liquidity to spend the full quote amount

                vwap = spent_quote / filled_base
                best_price = Decimal(str(orderbook['asks'][0][0]))

                if best_price == 0:
                    return None

                slippage = (vwap - best_price) / best_price

            elif side == 'SELL':
                remaining_base = qty

                for level in levels:
                    price_d = Decimal(str(level[0]))
                    amount_d = Decimal(str(level[1]))
                    take_base = min(amount_d, remaining_base)

                    filled_base += take_base
                    spent_quote += take_base * price_d
                    remaining_base -= take_base

                    if remaining_base <= 0:
                        break

                if remaining_base > 0:
                    return None  # Not enough liquidity to sell the full base amount

                vwap = spent_quote / filled_base
                best_price = Decimal(str(orderbook['bids'][0][0]))

                if best_price == 0:
                    return None

                slippage = (best_price - vwap) / best_price

            else:
                return None  # Invalid side

            return max(slippage, Decimal('0'))

        except Exception as e:
            logger.error(f"[SLIPPAGE] Error estimating slippage for {symbol} on {exchange.id}: {e}")
            return None

    def _slippage_ok(self, slip: Optional[Decimal]) -> bool:
        return slip is not None and slip <= self.slippage_tolerance
    
    async def _log_trade_to_base44(self, entity_data: dict) -> Optional[str]:
        """Logs a completed or pending trade to the 'Trade' entity."""
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.post(f"{self.config.BASE44_API_URL}/entities/Trade", headers=self.headers, json=entity_data)
                if resp.status in [200, 201]:
                    result = await resp.json()
                    logger.info(f"Successfully logged trade to base44. ID: {result.get('id')}")
                    return result.get('id')
                else:
                    logger.error(f"Failed to log trade. Status: {resp.status}, Response: {await resp.text()}")
            return None
        except Exception as e:
            logger.error(f"Exception while logging trade: {e}")
            return None

    async def save_opportunity(self, op: Opportunity, entity_name: str):
        """Saves an opportunity to the base44 database."""
        try:
            # --- Build unique opportunity signature ---
            if entity_name == 'ArbitrageOpportunity':
                signature = f"{op.trading_pair}-{op.buy_exchange}-{op.sell_exchange}-{round(op.profit_percentage, 1)}"
            elif entity_name == 'TriangularOpportunity':
                signature = f"{op.exchange}-{'->'.join(op.trading_path)}-{round(op.profit_percentage, 1)}"
            else:
                signature = f"{op.detected_at}"  # fallback
            
            # --- Skip if already seen recently ---
            now = time.time()
            if signature in self.seen_opportunities:
                return  # Already seen

            # Add to cache
            self.seen_opportunities.add(signature)
            self.seen_timestamps[signature] = now

            # Optional: Clean old cache entries
            self._cleanup_seen_cache(now)

            # --- Proceed to save to DB ---
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f'{self.base44_api_url}/entities/{entity_name}',
                    headers=self.headers,
                    json=asdict(op)
                ) as response:
                    if response.status in [200, 201]:
                        result = await response.json()
                        logger.info(f"Saved {entity_name} opportunity: {result.get('id')}")
                        # Send Telegram notification
                        if self.notifier:
                            await self.notifier.send_opportunity_alert(op, result.get('id'))
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to save {entity_name}: {response.status} - {error_text}")
                        
        except Exception as e:
            logger.error(f"Exception saving {entity_name}: {e}")
    
    async def update_account_balances_periodically(self):
        """Periodically fetches and updates balances for all exchanges."""
        # Wait a bit for exchanges to initialize
        await asyncio.sleep(60) 
        
        while True:
            logger.info("Updating account balances...")
            # Use the exchanges initialized by the cross-exchange engine (or triangular, they should be the same)
            all_balances = {}
            for name, exchange in self.exchanges.items():
                try:
                    balance = await exchange.fetch_balance()
                    # Filter for non-zero balances and store them
                    all_balances[name] = {
                        asset: data 
                        for asset, data in balance['total'].items() if data > 0
                    }
                except Exception as e:
                    logger.error(f"Could not fetch balance for {name}: {e}")
            self.account_balances.clear()
            self.account_balances.update(all_balances)
            logger.info("Account balances updated successfully.")
            await asyncio.sleep(300) # Update every 5 minutes

    def _cleanup_seen_cache(self, now):
        expired_keys = [sig for sig, t in self.seen_timestamps.items() if now - t > self.cache_ttl]
        for sig in expired_keys:
            self.seen_opportunities.discard(sig)
            self.seen_timestamps.pop(sig, None)

    async def reconnect(self, exchange, sleep = None):
        await asyncio.sleep(sleep or self.reconnect_delay)
        await exchange.close()

    async def stop(self):
        """Stop the arbitrage engine and cleanup connections"""
        logger.info("Stopping Arbitrage Engine...")
        self.running = False
        
        # Close all exchange connections properly
        for name, exchange in self.exchanges.items():
            try:
                if hasattr(exchange, 'close'):
                    await exchange.close()
                logger.info(f"Closed {name} connection")
            except Exception as e:
                logger.error(f"Error closing {name}: {e}")


engine = Engine(base44_api_url=os.getenv('BASE44_API_URL'), app_token=os.getenv('APP_TOKEN'))