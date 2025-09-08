import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Base44 Configuration
    BASE44_API_URL = os.getenv("BASE44_API_URL", "https://app.base44.com/api/apps/YOUR_APP_ID")
    APP_TOKEN = os.getenv("BASE44_APP_TOKEN", "your-app-token")
    
    # Trading Configuration
    SYMBOLS = ['FIL/USDT', 'QTUM/USDT', 'DOT/USDT', 'XRP/USDT', 'ADA/USDT']
    MIN_PROFIT_THRESHOLD = float(os.getenv("MIN_PROFIT_THRESHOLD", "0.3"))
    MAX_TRADE_AMOUNT = float(os.getenv("MAX_TRADE_AMOUNT", "1000.0"))

    # Telegram Bot Configuration
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "your_telegram_bot_token")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "your_telegram_chat_id")
    TELEGRAM_API_URL = os.getenv("TELEGRAM_API_URL", "https://api.telegram.org/bot") + TELEGRAM_BOT_TOKEN
    TELEGRAM_ALERTS_ENABLED = os.getenv("TELEGRAM_ALERTS_ENABLED", "True") == "True"
    TELEGRAM_ALERT_THRESHOLD = float(os.getenv("TELEGRAM_ALERT_THRESHOLD", "0.5"))  # in percentage
    TELEGRAM_ALERT_COOLDOWN = int("300")  # in seconds
    
    # WebSocket Configuration
    ORDERBOOK_LIMIT = int(os.getenv("ORDERBOOK_LIMIT", "1"))
    RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", "30"))
    
    # Flask Configuration
    FLASK_HOST = os.getenv("FLASK_HOST", "0.0.0.0")
    FLASK_PORT = int(os.getenv("FLASK_PORT", "5001"))
    EXECUTE_URL = "https://arbitragewise-production.up.railway.app/execute"
    
    # Logging Configuration
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")