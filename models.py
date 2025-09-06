from dataclasses import dataclass, asdict
from typing import Optional, List

@dataclass
class TradeResult:
    status: str
    message: str
    trade_id: Optional[str] = None
    profit_usd: Optional[float] = None

@dataclass
class Opportunity:
    # Common fields
    profit_percentage: float
    detected_at: str
    status: str = "active"
    # Cross-Exchange specific
    trading_pair: Optional[str] = None
    buy_exchange: Optional[str] = None
    sell_exchange: Optional[str] = None
    buy_price: Optional[float] = None
    sell_price: Optional[float] = None
    profit_usd: Optional[float] = None
    volume: Optional[float] = None
    # Triangular specific
    exchange: Optional[str] = None
    trading_path: Optional[List[str]] = None
    assets: Optional[List[str]] = None 
    initial_amount: Optional[float] = None
    final_amount: Optional[float] = None