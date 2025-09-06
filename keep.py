import asyncio
import traceback
from models import TradeResult  # Assuming you use this for responses
from engine import logger       # Your global logger
import asyncio
import ccxt.pro as cxt

async def _execute_transfer_arbitrage(self, op: dict, amount_usd: float) -> TradeResult:
    try:
        buy_ex_name, sell_ex_name = op['buy_exchange'], op['sell_exchange']
        buy_exchange = self.exchanges.get(buy_ex_name)
        sell_exchange = self.exchanges.get(sell_ex_name)
        pair = op['trading_pair']
        base, quote = pair.split('/')
        
        if not buy_exchange or not sell_exchange:
            return TradeResult("error", "One of the exchanges is not initialized.")

        # --- 1. Balance Check on Buy Exchange ---
        quote_balance = self.account_balances.get(buy_ex_name, {}).get(quote, 0)
        if quote_balance < amount_usd:
            return TradeResult("error", f"Insufficient {quote} on {buy_ex_name} (have: {quote_balance})")

        # --- 2. Place Market Buy Order ---
        amount_base = amount_usd / op['buy_price']
        logger.info(f"[TRANSFER ARB] Buying {amount_base:.6f} {base} on {buy_ex_name} at {op['buy_price']}")
        buy_order = await buy_exchange.create_market_buy_order_with_cost(pair, quote_balance)

        if not buy_order or 'status' not in buy_order or buy_order['status'] != 'closed':
            return TradeResult("error", "Buy order failed or not filled.")
        
        amount_base = buy_order['filled']  # Update to actual filled amount

        # --- 3. Fetch Deposit Address from Sell Exchange ---
        logger.info(f"[TRANSFER ARB] Fetching deposit address for {base} on {sell_ex_name}")
        common_chains = await get_common_transfer_chains(base, buy_exchange, sell_exchange)
        if not common_chains:
            return TradeResult("error", f"No common transfer chains for {base} between {buy_ex_name} and {sell_ex_name}")
        
        best_chain, fee = await select_best_transfer_chain(common_chains, buy_exchange, base)
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

        # --- 6. Sell on Sell Exchange ---
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

        trade_id = await self._log_trade_to_base44(trade_log)

        logger.info(f"[TRANSFER ARB] Transfer arbitrage successful. Profit: ${profit:.2f}")
        return TradeResult("success", "Transfer arbitrage executed successfully.", trade_id, profit)

    except Exception as e:
        logger.error(f"[TRANSFER ARB] Error: {e}")
        logger.error(traceback.format_exc())
        return TradeResult("error", f"Exception occurred: {str(e)}")

async def get_common_transfer_chains(asset: str, buy_exchange, sell_exchange) -> list[str]:
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

async def select_best_transfer_chain(common_chains: list, buy_exchange, asset: str) -> str:
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
        return None

async def main():
    exchange1 = cxt.binance({
            'apiKey': 'mb4MZz0XWG6rpNhYAtONez7AFnCjVv1gbq6oDBOb01p2QHQb6W3s6pSiLhNPJ1vl',
            'secret': '1FPwaxy4K52mOtV2y4iJ3nhgXmocccqlojllXvMfXnQ5Q8PGxgt108rSRd67UFds',
            'options': {'defaultType': 'spot'},
            'enableRateLimit': True,
            'sandbox': False,  # Set to True for testing
        })

    exchange2 =  cxt.bybit({
            'apiKey': 'rXK4kRziFR25WtwBgN',
            'secret': 'hTV6liP2e7afHsgWn5uz0v5N3Tk21jCnJbxn',
            'options': {'defaultType': 'spot'},
            'enableRateLimit': True,
            'sandbox': False,  # Set to True for testing
        })

    common = await get_common_transfer_chains('USDT', exchange1, exchange2)
    print("Common Chains:", common)
    best_chain = await select_best_transfer_chain(common, exchange1, 'USDT')
    print("Best Chain:", best_chain)
    await exchange1.close()
    await exchange2.close()
if __name__ == "__main__":
    asyncio.run(main())