import os
import json
import aiohttp
import asyncio

async def fetch_pokemon(session, number):
    url = f"https://pokeapi.co/api/v2/pokemon/{number}"
    print(f"Fetching: {url}")
    async with session.get(url) as response:
        if response.status != 200:
            print(f"Failed to fetch {url}: {response.status}")
            return None
        print(f"Successfully fetched {url}")
        return await response.json()

async def save_pokemon(data, number):
    filename = f"./data/pokemon_{number:04d}.json"
    print(f"Saving data to {filename}")
    loop = asyncio.get_running_loop()
    # Use run_in_executor to avoid blocking the event loop with file I/O
    await loop.run_in_executor(
        None,
        lambda: json.dump(data, open(filename, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    )
    print(f"Saved {filename}")

async def main():
    os.makedirs("./data", exist_ok=True)
    print("Created ./data directory (if not already present)")
    async with aiohttp.ClientSession() as session:
        print("Starting to fetch Pokémon data...")
        tasks = [fetch_pokemon(session, number) for number in range(1025, 1026)]
        results = await asyncio.gather(*tasks)
        pokemons = [(number, data) for number, data in zip(range(1025, 1026), results) if data is not None]
        print(f"Fetched {len(pokemons)} Pokémon successfully. Now saving...")
        save_tasks = [save_pokemon(data, number) for number, data in pokemons]
        await asyncio.gather(*save_tasks)
        print("All Pokémon data saved.")

if __name__ == "__main__":
    print("Starting Pokémon data extraction script...")
    asyncio.run(main())
    print("Script finished.")