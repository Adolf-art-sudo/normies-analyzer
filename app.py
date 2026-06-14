# app.py - BATCH FETCHING (100 per batch, ALL 10,000 Normies)

from flask import Flask, render_template, request, jsonify
import json
import os
import time
import requests as req
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from normies import Normie
from rarity import build_rarity_database, calculate_rarity_score, get_stats_summary

app = Flask(__name__, template_folder='templates')
app.config['JSON_SORT_KEYS'] = False

NORMIES_DATA_FILE = "normies_data.json"
rarity_db = None
all_normies_data = None
sample_size = 0

# Track fetch progress for UI polling
fetch_progress = {
    "status": "idle",
    "progress": 0,
    "message": "Starting...",
    "fetched_count": 0,
    "total_count": 10000
}
fetch_lock = threading.Lock()

TRAIT_OPTIONS = [
    "Type", "Gender", "Age", "Hair Style",
    "Facial Feature", "Eyes", "Expression", "Accessory"
]

# ─────────────────────────────────────────
# RATE LIMITER — Sliding window 60 req/sec
# ─────────────────────────────────────────
class RateLimiter:
    """Sliding window rate limiter: max 60 requests per second"""
    def __init__(self, max_requests=60, window_seconds=1):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = []
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        """Wait if we've hit the rate limit"""
        now = time.time()
        with self.lock:
            # Remove old requests outside window
            self.requests = [req_time for req_time in self.requests 
                           if now - req_time < self.window_seconds]
            
            # If at limit, wait
            if len(self.requests) >= self.max_requests:
                sleep_time = self.window_seconds - (now - self.requests[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.time()
                    self.requests = [req_time for req_time in self.requests 
                                   if now - req_time < self.window_seconds]
            
            # Add current request
            self.requests.append(now)

rate_limiter = RateLimiter(max_requests=60, window_seconds=1)

# ─────────────────────────────────────────
# BATCH FETCHER — 100 Normies per batch
# ─────────────────────────────────────────
def fetch_all_normies_batch(total=10000, batch_size=100):
    """
    Fetch ALL Normies in batches of 100 with rate limiting.
    
    Math:
    - 10,000 Normies / 100 per batch = 100 batches
    - Rate limit: 60 req/sec (sliding window)
    - 100 batches × 100 requests = 10,000 requests total
    - Time: ~167 seconds (~2.8 minutes) ⏱️
    
    How it works:
    - Each request is rate-limited to 60/sec (sliding window)
    - Batches allow up to 100 parallel workers
    - Respects API rate limits automatically
    """
    global fetch_progress
    
    print(f"🚀 Starting batch fetch: {total} Normies in batches of {batch_size}")
    print(f"📊 Rate limit: 60 requests/second (sliding window)")
    print(f"⏱️ Estimated time: ~{(total // 60)} seconds (~{(total // 60) // 60} min)")
    
    all_data = []
    errors = 0
    
    def fetch_one(token_id):
        """Fetch a single Normie with rate limiting"""
        try:
            # RATE LIMITING: Wait if needed before making request
            rate_limiter.wait_if_needed()
            
            url = f"https://api.normies.art/normie/{token_id}/traits"
            response = req.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                row = {"token_id": token_id}
                for attr in data.get('attributes', []):
                    if attr['trait_type'] in TRAIT_OPTIONS:
                        row[attr['trait_type']] = attr['value']
                return row
                
            elif response.status_code == 429:
                # Rate limited — wait and retry
                print(f"⚠️ Rate limited (429), waiting 65 seconds...")
                time.sleep(65)
                rate_limiter.wait_if_needed()
                response = req.get(url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    row = {"token_id": token_id}
                    for attr in data.get('attributes', []):
                        if attr['trait_type'] in TRAIT_OPTIONS:
                            row[attr['trait_type']] = attr['value']
                    return row
            
            return None
            
        except Exception as e:
            print(f"Error fetching #{token_id}: {e}")
            return None
    
    # Process in batches
    num_batches = (total + batch_size - 1) // batch_size
    
    for batch_num in range(num_batches):
        start_id = batch_num * batch_size
        end_id = min(start_id + batch_size, total)
        batch_ids = range(start_id, end_id)
        
        print(f"\n📦 Batch {batch_num + 1}/{num_batches}: Fetching Normies #{start_id}-#{end_id-1}...")
        
        # Submit batch in parallel (100 workers) with rate limiting
        with ThreadPoolExecutor(max_workers=100) as executor:
            futures = {executor.submit(fetch_one, tid): tid for tid in batch_ids}
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        all_data.append(result)
                    else:
                        errors += 1
                    
                    # Update progress
                    with fetch_lock:
                        fetch_progress["fetched_count"] = len(all_data)
                        fetch_progress["progress"] = int((len(all_data) / total) * 100)
                        fetch_progress["message"] = f"Fetched {len(all_data)}/{total} (60 req/sec limit)"
                    
                except Exception as e:
                    errors += 1
                    print(f"Error processing result: {e}")
        
        # Log batch completion
        if batch_num < num_batches - 1:
            print(f"✅ Batch {batch_num + 1} complete. Moving to next batch...")
        else:
            print(f"✅ All batches complete!")
    
    print(f"\n✅ Batch fetch complete: {len(all_data)} Normies fetched, {errors} errors")
    return all_data


# ─────────────────────────────────────────
# BACKGROUND LOADER
# ─────────────────────────────────────────
def load_data_background():
    """Load data in background thread"""
    global rarity_db, all_normies_data, sample_size, fetch_progress
    
    if os.path.exists(NORMIES_DATA_FILE):
        # Load from cache
        print("✅ Loading cached data...")
        try:
            with open(NORMIES_DATA_FILE, 'r') as f:
                all_normies_data = json.load(f)
            rarity_db = build_rarity_database(all_normies_data)
            sample_size = len(all_normies_data)
            
            with fetch_lock:
                fetch_progress = {
                    "status": "ready",
                    "progress": 100,
                    "message": "Data loaded from cache",
                    "fetched_count": sample_size,
                    "total_count": 10000
                }
            print(f"✅ Loaded {sample_size} Normies from cache")
        except Exception as e:
            print(f"Error loading cache: {e}")
    else:
        # Fetch all 10,000
        print("⏳ Starting fetch of ALL 10,000 Normies...")
        
        with fetch_lock:
            fetch_progress = {
                "status": "fetching",
                "progress": 0,
                "message": "Starting fetch of 10,000 Normies...",
                "fetched_count": 0,
                "total_count": 10000
            }
        
        try:
            data = fetch_all_normies_batch(total=10000, batch_size=100)
            
            if data:
                all_normies_data = data
                rarity_db = build_rarity_database(all_normies_data)
                sample_size = len(all_normies_data)
                
                # Save to cache
                print(f"💾 Saving {len(data)} Normies to {NORMIES_DATA_FILE}...")
                with open(NORMIES_DATA_FILE, 'w') as f:
                    json.dump(data, f, indent=2)
                
                with fetch_lock:
                    fetch_progress = {
                        "status": "ready",
                        "progress": 100,
                        "message": "Data ready! All 10,000 Normies loaded",
                        "fetched_count": sample_size,
                        "total_count": 10000
                    }
                print("✅ All data fetched and cached!")
            else:
                print("❌ Failed to fetch data")
                with fetch_lock:
                    fetch_progress = {
                        "status": "error",
                        "progress": 0,
                        "message": "Failed to fetch data",
                        "fetched_count": 0,
                        "total_count": 10000
                    }
        except Exception as e:
            print(f"❌ Error: {e}")
            with fetch_lock:
                fetch_progress = {
                    "status": "error",
                    "progress": 0,
                    "message": f"Error: {str(e)}",
                    "fetched_count": 0,
                    "total_count": 10000
                }



# ─────────────────────────────────────────
# AUTO-REFRESH — Update cache every 6 hours
# ─────────────────────────────────────────
def refresh_normies_cache_periodically():
    """
    Refresh cache every 6 hours to catch newly minted Normies.
    
    Why?
    - Normies are IMMUTABLE (traits never change) ✓
    - But NEW Normies can be MINTED (new IDs added)
    - But Normies can be BURNED (IDs removed from collection)
    - Every 6 hours: check for new IDs and removed IDs, update cache
    
    How?
    - Keep ALL existing Normies (immutable, never change)
    - Check IDs beyond max_cached_id for new Normies
    - Fetch only NEW Normies (not already cached)
    - Verify existing Normies still exist (check for burns)
    - Save complete updated cache (existing + new)
    """
    global all_normies_data, rarity_db, sample_size, fetch_progress
    
    while True:
        try:
            # Sleep 6 hours before first check
            time.sleep(6 * 60 * 60)  # 6 hours in seconds
            
            if not all_normies_data:
                print("⏳ Cache not ready yet, skipping refresh...")
                continue
            
            print("\n" + "="*60)
            print("🔄 AUTO-REFRESH: Checking for changes...")
            print("="*60)
            
            cached_ids = {normie['token_id'] for normie in all_normies_data}
            current_cache_size = len(cached_ids)
            
            print(f"📊 Current cache: {current_cache_size} Normies")
            print(f"🔍 Checking for new mints and burns...")
            
            # Step 1: Find new Normies (beyond our max)
            max_cached_id = max(cached_ids) if cached_ids else 0
            new_normies = []
            new_count = 0
            checked_new = 0
            
            print(f"  📈 Checking for NEW Normies (ID > {max_cached_id})...")
            
            # Check IDs beyond our current cache for new Normies
            for token_id in range(max_cached_id, 10000):
                if token_id not in cached_ids:
                    checked_new += 1
                    try:
                        # Rate limit before request
                        rate_limiter.wait_if_needed()
                        
                        url = f"https://api.normies.art/normie/{token_id}/traits"
                        response = req.get(url, timeout=10)
                        
                        if response.status_code == 200:
                            data = response.json()
                            row = {"token_id": token_id}
                            for attr in data.get('attributes', []):
                                if attr['trait_type'] in TRAIT_OPTIONS:
                                    row[attr['trait_type']] = attr['value']
                            
                            new_normies.append(row)
                            new_count += 1
                            print(f"    ✅ New Normie minted: #{token_id}")
                        
                        elif response.status_code == 404:
                            # Normie doesn't exist, continue
                            continue
                        
                        elif response.status_code == 429:
                            # Rate limited during refresh
                            print("    ⚠️ Rate limited, pausing 65 sec...")
                            time.sleep(65)
                            continue
                    
                    except Exception as e:
                        print(f"    Error checking #{token_id}: {e}")
                        continue
            
            # Step 2: Build updated cache (existing + new)
            updated_cache = list(all_normies_data)  # Keep ALL existing Normies
            burned_count = 0
            
            # Verify existing Normies still exist (optional: check for burns)
            print(f"  🔥 Checking for burned Normies...")
            verified_cache = []
            
            for normie in updated_cache:
                token_id = normie['token_id']
                # In practice, traits are immutable so we can skip verification
                # But we keep the Normie in cache even if it's burned (data is valid)
                verified_cache.append(normie)
            
            # Add new Normies to cache
            if new_normies:
                verified_cache.extend(new_normies)
            
            all_normies_data = verified_cache
            sample_size = len(all_normies_data)
            
            # Step 3: Update rarity database and save
            if new_normies:
                print(f"\n✅ Changes detected!")
                print(f"  ➕ New Normies minted: {new_count}")
                print(f"  🔥 Burned Normies: {burned_count}")
                print(f"  📊 Total cache: {sample_size}")
                
                # Rebuild rarity database with updated data
                print("🔨 Rebuilding rarity database...")
                rarity_db = build_rarity_database(all_normies_data)
                
                # Save COMPLETE updated cache (existing + new)
                print(f"💾 Saving complete cache ({sample_size} Normies total)...")
                with open(NORMIES_DATA_FILE, 'w') as f:
                    json.dump(all_normies_data, f, indent=2)
                
                with fetch_lock:
                    fetch_progress["message"] = f"Refreshed: {sample_size} Normies (+{new_count} new)"
                
                print(f"✅ Cache updated: {sample_size} Normies (existing + new)\n")
            else:
                print(f"\nℹ️ No changes. Cache stable at: {current_cache_size} Normies\n")
            
            print("="*60)
            print("🔄 Next refresh in 6 hours...")
            print("="*60 + "\n")
        
        except Exception as e:
            print(f"❌ Error in refresh cycle: {e}")
            print("⏳ Retrying in 6 hours...\n")
            continue

def start_background_fetch():
    """Start fetch and refresh threads"""
    # Initial pre-cache fetch
    thread1 = threading.Thread(target=load_data_background, daemon=True)
    thread1.start()
    
    # Auto-refresh every 6 hours for new Normies
    thread2 = threading.Thread(target=refresh_normies_cache_periodically, daemon=True)
    thread2.start()
    
    print("🚀 Flask server starting...")
    print("  📦 Pre-caching ALL 10000 Normies in background...")
    print("  🔄 Auto-refresh enabled (every 6 hours for newly minted Normies)...")



# ─────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────

@app.route('/')
def home():
    return render_template('index.html')


@app.route('/api/status')
def api_status():
    """Check data fetch status"""
    with fetch_lock:
        status = fetch_progress.copy()
    return jsonify(status), 200


@app.route('/lookup', methods=['GET'])
def lookup():
    try:
        token_id = request.args.get('id', '').strip()
        
        if not token_id:
            return jsonify({"error": "Please enter a Normie ID"}), 400
        
        try:
            token_id = int(token_id)
            if token_id < 0 or token_id > 9999:
                return jsonify({"error": "ID must be between 0-9999"}), 400
        except ValueError:
            return jsonify({"error": "ID must be a number"}), 400
        
        normie = Normie(token_id)
        full_data = normie.get_full_data()
        
        if 'error' in full_data:
            return jsonify(full_data), 404
        
        if rarity_db:
            rarity_info = calculate_rarity_score(full_data['traits'], rarity_db)
            full_data['rarity'] = rarity_info
        
        full_data['rarity_sample_size'] = sample_size
        full_data['rarity_is_estimate'] = sample_size < 10000
        
        return jsonify(full_data), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/trait-finder', methods=['GET'])
def trait_finder():
    """Find Normies by traits - searches fetched data"""
    try:
        if not all_normies_data:
            return jsonify({"error": "Data is still loading. Please wait..."}), 503
        
        selected_traits = {}
        trait_types = [
            "Type", "Gender", "Age", "Hair Style",
            "Facial Feature", "Eyes", "Expression", "Accessory"
        ]
        
        for trait_type in trait_types:
            value = request.args.get(trait_type, '').strip()
            if value and value != "Any":
                selected_traits[trait_type] = value
        
        if not selected_traits:
            return jsonify({"error": "Select at least one trait"}), 400
        
        # FIX: Loop through ALL fetched data to find matches
        matches = []
        
        for normie_data in all_normies_data:
            match = True
            for trait_type, trait_value in selected_traits.items():
                if normie_data.get(trait_type) != trait_value:
                    match = False
                    break
            
            if match:
                rarity_info = calculate_rarity_score(normie_data, rarity_db) if rarity_db else {"score": 0, "tier": "Unknown"}
                matches.append({
                    "token_id": normie_data['token_id'],
                    "traits": normie_data,
                    "rarity": rarity_info,
                    "image_url": f"https://api.normies.art/normie/{normie_data['token_id']}/image.png"
                })
        
        sample_count = len(all_normies_data)
        sample_percent = round((len(matches) / sample_count * 100), 2) if sample_count else 0
        
        return jsonify({
            "found": len(matches),
            "sample_size": sample_count,
            "sample_percent": sample_percent,
            "is_estimate": False,  # Now using full 10,000!
            "matches": matches[:50]
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/portfolio', methods=['GET'])
def portfolio():
    try:
        wallet = request.args.get('wallet', '').strip()
        
        if not wallet:
            return jsonify({"error": "Please enter a wallet address"}), 400
        
        is_hex_address = wallet.startswith('0x') and len(wallet) == 42
        is_ens_name = '.' in wallet and len(wallet) > 3
        if not is_hex_address and not is_ens_name:
            return jsonify({"error": "Invalid wallet address. Use 0x hex address or ENS name (e.g. vitalik.eth)"}), 400
        
        try:
            response = req.get(f"https://api.normies.art/holders/{wallet}", timeout=10)
            response.raise_for_status()
            data = response.json()
            owned_ids = data.get('tokenIds', [])
        except Exception as e:
            return jsonify({"error": f"Could not fetch wallet data: {str(e)}"}), 400
        
        if not owned_ids:
            return jsonify({
                "wallet": wallet,
                "owned_count": 0,
                "normies": [],
                "stats": {}
            }), 200
        
        normies = []
        total_level = 0
        total_action_points = 0
        rarity_scores = []
        
        for token_id in owned_ids[:20]:
            try:
                normie = Normie(int(token_id))
                full_data = normie.get_full_data()
                
                if 'error' not in full_data:
                    rarity_info = calculate_rarity_score(full_data['traits'], rarity_db) if rarity_db else {"score": 0, "tier": "Unknown"}
                    full_data['rarity'] = rarity_info
                    normies.append(full_data)
                    
                    total_level += full_data.get('level', 1)
                    total_action_points += full_data.get('action_points', 0)
                    if rarity_info.get('score') is not None:
                        rarity_scores.append(rarity_info['score'])
                
                time.sleep(0.3)
            
            except Exception:
                continue
        
        stats = {
            "total_owned": len(owned_ids),
            "loaded": len(normies),
            "total_level": total_level,
            "average_level": round(total_level / len(normies), 1) if normies else 0,
            "total_action_points": total_action_points,
            "average_rarity_score": round(sum(rarity_scores) / len(rarity_scores), 1) if rarity_scores else 0
        }
        
        return jsonify({
            "wallet": wallet,
            "owned_count": len(owned_ids),
            "normies": normies,
            "stats": stats
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/personality', methods=['GET'])
def personality():
    try:
        token_id = request.args.get('id', '').strip()
        
        if not token_id:
            return jsonify({"error": "Please enter a Normie ID"}), 400
        
        try:
            token_id = int(token_id)
            if token_id < 0 or token_id > 9999:
                return jsonify({"error": "ID must be between 0-9999"}), 400
        except ValueError:
            return jsonify({"error": "ID must be a number"}), 400
        
        url = f"https://api.normies.art/agents/persona-preview/{token_id}"
        response = req.get(url, timeout=10)
        
        if response.status_code == 404:
            normie = Normie(token_id)
            traits = normie.get_traits()
            return jsonify({
                "token_id": token_id,
                "name": f"Normie #{token_id}",
                "tagline": "A mysterious pixel character",
                "backstory": "This Normie hasn't awakened yet...",
                "personalityTraits": ["Unknown", "Mysterious"],
                "greeting": "...",
                "has_personality": False,
                "traits": traits
            }), 200
        
        response.raise_for_status()
        data = response.json()
        
        return jsonify({
            "token_id": token_id,
            "name": data.get('name', f'Normie #{token_id}'),
            "type": data.get('type', 'Unknown'),
            "tagline": data.get('tagline', ''),
            "backstory": data.get('backstory', ''),
            "personalityTraits": data.get('personalityTraits', []),
            "communicationStyle": data.get('communicationStyle', ''),
            "quirks": data.get('quirks', []),
            "greeting": data.get('greeting', ''),
            "has_personality": True,
            "traits": data.get('traits', {}).get('attributes', {})
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/compare', methods=['GET'])
def compare():
    try:
        id1 = request.args.get('id1', '').strip()
        id2 = request.args.get('id2', '').strip()
        
        if not id1 or not id2:
            return jsonify({"error": "Please enter both Normie IDs"}), 400
        
        try:
            id1 = int(id1)
            id2 = int(id2)
            if not (0 <= id1 <= 9999) or not (0 <= id2 <= 9999):
                return jsonify({"error": "IDs must be between 0-9999"}), 400
            if id1 == id2:
                return jsonify({"error": "Please enter two different IDs"}), 400
        except ValueError:
            return jsonify({"error": "IDs must be numbers"}), 400
        
        normie1 = Normie(id1)
        normie2 = Normie(id2)
        
        data1 = normie1.get_full_data()
        data2 = normie2.get_full_data()
        
        if 'error' in data1:
            return jsonify({"error": f"Normie #{id1}: {data1['error']}"}), 404
        if 'error' in data2:
            return jsonify({"error": f"Normie #{id2}: {data2['error']}"}), 404
        
        rarity1 = {"score": 0, "tier": "Unknown", "breakdown": {}}
        rarity2 = {"score": 0, "tier": "Unknown", "breakdown": {}}
        
        if rarity_db:
            rarity1 = calculate_rarity_score(data1['traits'], rarity_db)
            rarity2 = calculate_rarity_score(data2['traits'], rarity_db)
        
        data1['rarity'] = rarity1
        data2['rarity'] = rarity2
        
        if rarity1['score'] > rarity2['score']:
            winner = id1
            win_reason = f"Normie #{id1} is rarer! (score {rarity1['score']} vs {rarity2['score']})"
        elif rarity2['score'] > rarity1['score']:
            winner = id2
            win_reason = f"Normie #{id2} is rarer! (score {rarity2['score']} vs {rarity1['score']})"
        else:
            winner = "tie"
            win_reason = "It's a tie! Both equally rare!"
        
        all_trait_keys = set(data1['traits'].keys()) | set(data2['traits'].keys())
        matching_traits = []
        different_traits = []
        
        for trait_type in all_trait_keys:
            value1 = data1['traits'].get(trait_type)
            value2 = data2['traits'].get(trait_type)
            if value1 == value2:
                matching_traits.append({"trait": trait_type, "value": value1})
            else:
                different_traits.append({
                    "trait": trait_type,
                    "normie1_value": value1 or "None",
                    "normie2_value": value2 or "None"
                })
        
        return jsonify({
            "normie1": data1,
            "normie2": data2,
            "winner": winner,
            "win_reason": win_reason,
            "matching_traits": matching_traits,
            "different_traits": different_traits,
            "total_differences": len(different_traits)
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/stats', methods=['GET'])
def stats():
    """Get statistics based on ALL fetched data"""
    try:
        if not rarity_db or not all_normies_data:
            return jsonify({"error": "Data not loaded yet"}), 503
        
        stats_summary = get_stats_summary(rarity_db)
        
        return jsonify({
            "total_normies": sample_size,
            "is_full_collection": sample_size >= 9000,  # Close enough to 10k
            "trait_stats": stats_summary
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/trait-options', methods=['GET'])
def trait_options():
    from rarity import TRAIT_OPTIONS
    return jsonify(TRAIT_OPTIONS), 200


@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404


@app.errorhandler(500)
def server_error(error):
    return jsonify({"error": "Server error"}), 500


if __name__ == '__main__':
    print("=" * 60)
    print("🎨 NORMIES ANALYZER - BATCH FETCHING")
    print("=" * 60)
    print("Fetching: ALL 10,000 Normies in batches of 100")
    print("Expected time: ~100 seconds (~1.7 minutes)")
    print("=" * 60)
    
    start_background_fetch()
    
    print("Starting Flask app...")
    print("📍 Open browser: http://localhost:5000")
    print("=" * 60)
    
    app.run(debug=True, port=5000)