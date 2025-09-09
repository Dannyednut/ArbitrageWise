from engine import engine
import asyncio

async def main():
    config = await engine.fetch_exchanges()
    print(config)

asyncio.run(main())