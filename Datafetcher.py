# Datafetcher.py - FETCH ALL 10,000 Normies in batches

import requests
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://api.normies.art"

TRAIT_OPTIONS = [
    "Type", "Gender", "Age", "Hair Style",
    "Facial Feature", "Eyes", "Expression", "Accessory"
]

def fetch_all_normies(total=10000, batch_size=100):
    """
    Fetch ALL 10,000 Normies in batches of 100.
    
    Why all 10,000?
    - Normies are mutable (burned/added)
    - Need 100% accuracy for rarity calculations
    - Batch fetching reduces time significantly
    - Takes ~1.7 minutes with batch_size=100
    
    How it works:
    - 10,000 / 100 = 100 batches
    - Each batch fetched in parallel (100 workers)
    - 1 second wait between batches
    - Total: ~100 seconds
    
    Rate limiting:
    - API limit: 60 req/min = 1/sec average
    - We send 100 requests per batch, spaced 1 sec apart
    - Includes retry logic for 429 errors
    """
    save_file = "normies_data.json"
    
    if os.path.exists(save_file):
        print(f"Data already exists in {save_file}!")
        print("Delete the file if you want to re-fetch.")
        return
    
    print("=" * 70)
    print("🎨 NORMIES ANALYZER - BATCH DATA FETCHER")
    print("=" * 70)
    print(f"Fetching ALL {total} Normies...")
    print(f"Batch size: {batch_size} Normies per batch")
    print(f"Batches: {total // batch_size}")
    print(f"Estimated time: {(total // batch_size) * 1} seconds (~{(total // batch_size) / 60:.1f} minutes)")
    print("=" * 70)
    print()
    
    all_data = []
    errors = 0
    
    def fetch_one(token_id):
        """Fetch a single Normie"""
        try:
            url = f"{BASE_URL}/normie/{token_id}/traits"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                row = {"token_id": token_id}
                for attr in data.get('attributes', []):
                    if attr['trait_type'] in TRAIT_OPTIONS:
                        row[attr['trait_type']] = attr['value']
                return row
                
            elif response.status_code == 429:
                # Rate limited — wait and retry once
                time.sleep(65)
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    row = {"token_id": token_id}
                    for attr in data.get('attributes', []):
                        if attr['trait_type'] in TRAIT_OPTIONS:
                            row[attr['trait_type']] = attr['value']
                    return row
            
            return None
            
        except Exception as e:
            return None
    
    # Process in batches
    num_batches = (total + batch_size - 1) // batch_size
    
    for batch_num in range(num_batches):
        start_id = batch_num * batch_size
        end_id = min(start_id + batch_size, total)
        batch_ids = list(range(start_id, end_id))
        
        print(f"Batch {batch_num + 1:3d}/{num_batches}: Fetching Normies #{start_id:5d}-#{end_id-1:5d} ({len(batch_ids):3d} Normies)", end="", flush=True)
        
        # Submit batch in parallel (100 workers)
        completed = 0
        with ThreadPoolExecutor(max_workers=100) as executor:
            futures = {executor.submit(fetch_one, tid): tid for tid in batch_ids}
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        all_data.append(result)
                    else:
                        errors += 1
                    completed += 1
                    
                except Exception as e:
                    errors += 1
                    completed += 1
        
        # Update progress
        percent = int((len(all_data) / total) * 100)
        print(f" ✓ ({percent}% complete, {len(all_data)}/{total} fetched)")
        
        # Wait 1 second before next batch (respect rate limit)
        if batch_num < num_batches - 1:
            time.sleep(1)
    
    # Save to file
    print()
    print(f"Saving {len(all_data)} Normies to {save_file}...")
    with open(save_file, 'w') as f:
        json.dump(all_data, f, indent=2)
    
    print("=" * 70)
    print(f"✅ COMPLETE!")
    print(f"✅ Fetched: {len(all_data)}/{total} Normies")
    print(f"✅ Errors: {errors}")
    print(f"✅ Accuracy: {len(all_data)/total*100:.1f}%")
    print(f"✅ Data saved to: {save_file}")
    print("=" * 70)
    print()
    print("You can now run: python app.py")
    print()

if __name__ == "__main__":
    print()
    fetch_all_normies(total=10000, batch_size=100)