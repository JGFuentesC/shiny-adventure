import os
import json
import asyncio
import pandas as pd

async def flatten_pokemon_json_async(json_path):
    """
    Asynchronously flattens a pokemon JSON file into a dict of useful metrics and dimensions for BI dashboarding.
    """
    loop = asyncio.get_running_loop()
    # Read file asynchronously
    with open(json_path, 'r', encoding='utf-8') as f:
        data = await loop.run_in_executor(None, json.load, f)

    flat = {}

    # Dimensions
    flat['id'] = data.get('id')
    flat['name'] = data.get('name')
    flat['species_name'] = data.get('species', {}).get('name')
    flat['is_default'] = data.get('is_default')
    flat['order'] = data.get('order')

    # Types (as comma-separated string)
    flat['types'] = ','.join([t['type']['name'] for t in data.get('types', [])])

    # Abilities (as comma-separated string, with hidden abilities marked)
    abilities = []
    for ab in data.get('abilities', []):
        ab_name = ab['ability']['name']
        if ab.get('is_hidden'):
            ab_name += " (hidden)"
        abilities.append(ab_name)
    flat['abilities'] = ','.join(abilities)

    # Metrics
    flat['base_experience'] = data.get('base_experience')
    flat['height'] = data.get('height')
    flat['weight'] = data.get('weight')

    # Stats (flattened as columns)
    for stat in data.get('stats', []):
        stat_name = stat['stat']['name']
        flat[f'stat_{stat_name}'] = stat['base_stat']
        flat[f'stat_{stat_name}_effort'] = stat['effort']

    # Moves (as count and as comma-separated string of move names)
    moves = [m['move']['name'] for m in data.get('moves', [])]
    flat['move_count'] = len(moves)
    flat['moves'] = ','.join(moves)

    # Game indices (as count and as comma-separated string of version names)
    versions = [gi['version']['name'] for gi in data.get('game_indices', [])]
    flat['game_index_count'] = len(versions)
    flat['game_versions'] = ','.join(versions)

    # Sprite URLs (main ones)
    sprites = data.get('sprites', {})
    flat['sprite_front_default'] = sprites.get('front_default')
    flat['sprite_back_default'] = sprites.get('back_default')
    flat['sprite_front_shiny'] = sprites.get('front_shiny')
    flat['sprite_back_shiny'] = sprites.get('back_shiny')

    return flat

async def process_all_pokemon_jsons_to_parquet(input_folder, output_parquet):
    files = [f for f in os.listdir(input_folder) if f.endswith('.json')]
    tasks = []
    for filename in files:
        input_path = os.path.join(input_folder, filename)
        tasks.append(flatten_pokemon_json_async(input_path))
    results = await asyncio.gather(*tasks)
    df = pd.DataFrame(results)
    df.to_parquet(output_parquet, index=False)
    print(f"Saved {len(df)} records to {output_parquet}")

if __name__ == "__main__":
    # Example: flatten all files in ./data and save to ./pokemon_flattened.parquet
    asyncio.run(process_all_pokemon_jsons_to_parquet('./data', './pokemon_flattened.parquet'))
