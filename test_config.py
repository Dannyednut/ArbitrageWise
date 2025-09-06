import asyncio
import aiohttp
from config import Config

async def test_base44_connection():
    print(f"Testing Base44 connection...")
    print(f"URL: {Config.BASE44_API_URL}")
    print(f"Token: {Config.APP_TOKEN[:10]}..." if Config.APP_TOKEN else "No token set")
    
    headers = {
        'api_key': Config.APP_TOKEN,
        'Content-Type': 'application/json'
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f'{Config.BASE44_API_URL}/entities/Exchange', headers=headers) as response:
                print(f"Response Status: {response.status}")
                if response.status == 200:
                    exchanges = await response.json()
                    print(f"Found {len(exchanges)} exchange configurations:")
                    for ex in exchanges:
                        print(f"  - {ex.get('name')} (Active: {ex.get('is_active')})")
                else:
                    print(f"Error Response: {await response.text()}")
    except Exception as e:
        print(f"Connection Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_base44_connection())
