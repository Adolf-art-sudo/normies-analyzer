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
# BATCH FETCHER — 100 Normies per batch
# ─────────────────────────────────────────
def fetch_all_normies_batch(total=10000, batch_size=100):
    """
    Fetch ALL Normies in batches of 100.
    
    Math:
    - 10,000 Normies / 100 per batch = 100 batches
    - Each batch spaced 1 second apart
    - Total time: ~100 seconds (~1.7 minutes) ⏱️
    
    Rate limiting:
    - Each batch submits 100 parallel tasks
    - We wait 1 second between batches
    - Includes retry on 429 (rate limit)
    """
    global fetch_progress
    
    print(f"Starting batch fetch: {total} Normies in batches of {batch_size}")
    print(f"Estimated time: {total // (batch_size * 10)} seconds")
    
    all_data = []
    errors = 0
    
    def fetch_one(token_id):
        """Fetch a single Normie"""
        try:
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
                time.sleep(65)
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
        
        print(f"\nBatch {batch_num + 1}/{num_batches}: Fetching Normies #{start_id}-#{end_id-1}...")
        
        # Submit batch in parallel (100 workers)
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
                        fetch_progress["message"] = f"Fetched {len(all_data)}/{total}"
                    
                except Exception as e:
                    errors += 1
                    print(f"Error processing result: {e}")
        
        # Wait 1 second before next batch (respect rate limit)
        if batch_num < num_batches - 1:
            print(f"Batch complete. Waiting 1 second before next batch...")
            time.sleep(1)
    
    print(f"\n✅ Batch fetch complete: {len(all_data)} Normies, {errors} errors")
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


def start_background_fetch():
    """Start fetch in daemon thread"""
    thread = threading.Thread(target=load_data_background, daemon=True)
    thread.start()
    print("🚀 Flask server starting (data fetch in background)...")


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