# app.py - UPDATED WITH PARALLEL FETCHING

from flask import Flask, render_template, request, jsonify
import json
import os
import time
import requests as req
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import random
from normies import Normie
from rarity import build_rarity_database, calculate_rarity_score, get_stats_summary

app = Flask(__name__, template_folder='templates')
app.config['JSON_SORT_KEYS'] = False

NORMIES_DATA_FILE = "normies_data.json"
rarity_db = None
all_normies_data = None
sample_size = 0

# FIX: Track fetch progress for UI polling
fetch_progress = {
    "status": "idle",          # idle, fetching, ready
    "progress": 0,             # 0-100%
    "message": "Starting..."
}
fetch_lock = threading.Lock()

# ─────────────────────────────────────────
# TRAIT OPTIONS
# ─────────────────────────────────────────
TRAIT_OPTIONS = [
    "Type", "Gender", "Age", "Hair Style",
    "Facial Feature", "Eyes", "Expression", "Accessory"
]

# ─────────────────────────────────────────
# PARALLEL FETCHER — Fetches 10 at a time
# FIX: Solution 2 — Multiple parallel requests
# ─────────────────────────────────────────
def fetch_sample_normies_parallel(sample_percent=5, max_workers=10):
    """
    Fetch Normies in parallel instead of sequentially.
    
    Original: 500 Normies × 1/sec = 500 sec (~8 min)
    Parallel: 500 Normies ÷ 10 workers = 50 batches × 1 sec = 50 sec (~1 min)
    
    Uses ThreadPoolExecutor to make 10 requests simultaneously.
    """
    global fetch_progress
    
    total = 10000
    sample_count = int(total * sample_percent / 100)  # 500 for 5%
    
    # Random stratified sample (not sequential)
    sample_ids = sorted(random.sample(range(total), sample_count))
    
    print(f"Fetching {sample_count} Normies in parallel ({max_workers} workers)...")
    print(f"Estimated time: ~{sample_count // (max_workers * 60)} minute(s)")
    
    all_data = []
    errors = 0
    
    def fetch_one(token_id):
        """Fetch a single Normie's traits"""
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
                # Rate limited — wait and retry once
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
            print(f"  Error fetching #{token_id}: {e}")
            return None
    
    # FIX: ThreadPoolExecutor for parallel requests
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all fetch tasks
        futures = {executor.submit(fetch_one, tid): tid for tid in sample_ids}
        
        # Process results as they complete
        completed = 0
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    all_data.append(result)
                else:
                    errors += 1
                
                completed += 1
                
                # Update progress for UI polling
                progress = int((completed / sample_count) * 100)
                with fetch_lock:
                    fetch_progress["progress"] = progress
                    fetch_progress["message"] = f"Fetched {completed}/{sample_count}"
                
                # Log every 50 completed
                if completed % 50 == 0:
                    print(f"Progress: {completed}/{sample_count} ({progress}%)")
                
                # Small delay to respect rate limits
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Error processing result: {e}")
                errors += 1
    
    print(f"\n✅ Fetched {len(all_data)} Normies, Errors: {errors}")
    return all_data


# ─────────────────────────────────────────
# BACKGROUND FETCH — Non-blocking startup
# FIX: Solution 3 — Background thread
# ─────────────────────────────────────────
def load_data_background():
    """Load data in background thread so Flask starts immediately"""
    global rarity_db, all_normies_data, sample_size, fetch_progress
    
    if os.path.exists(NORMIES_DATA_FILE):
        # File exists — load from cache (fast)
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
                    "message": "Data loaded from cache"
                }
            print(f"✅ Loaded {sample_size} Normies from cache")
        except Exception as e:
            print(f"Error loading cache: {e}")
    else:
        # File doesn't exist — fetch in background
        print("⏳ Data not found. Starting background fetch...")
        
        with fetch_lock:
            fetch_progress = {
                "status": "fetching",
                "progress": 0,
                "message": "Starting data fetch..."
            }
        
        try:
            # Fetch 5% sample (500 Normies)
            data = fetch_sample_normies_parallel(sample_percent=5, max_workers=10)
            
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
                        "message": "Data ready!"
                    }
                print("✅ Data fetch complete and cached!")
            else:
                print("❌ Failed to fetch any data")
                with fetch_lock:
                    fetch_progress = {
                        "status": "error",
                        "progress": 0,
                        "message": "Failed to fetch data"
                    }
        except Exception as e:
            print(f"❌ Error in background fetch: {e}")
            with fetch_lock:
                fetch_progress = {
                    "status": "error",
                    "progress": 0,
                    "message": f"Error: {str(e)}"
                }


# Start background fetch on app startup (non-blocking)
def start_background_fetch():
    """Start fetch in daemon thread so Flask server starts immediately"""
    thread = threading.Thread(target=load_data_background, daemon=True)
    thread.start()
    print("🚀 Flask server starting (data fetch in background)...")


# ─────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────

@app.route('/')
def home():
    """Home page"""
    return render_template('index.html')


# FIX: New status endpoint for UI polling
@app.route('/api/status')
def api_status():
    """Check data fetch status - called by frontend polling"""
    with fetch_lock:
        status = fetch_progress.copy()
    return jsonify(status), 200


@app.route('/lookup', methods=['GET'])
def lookup():
    """Lookup single Normie by ID"""
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
    """Find Normies by traits - searches ALL 10000 Normies"""
    try:
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
        
        matches = []
        errors = 0
        checked = 0
        
        # Search all 10000 Normies (not just sample)
        for token_id in range(10000):
            try:
                normie = Normie(token_id)
                full_data = normie.get_full_data()
                
                if 'error' not in full_data:
                    checked += 1
                    traits = full_data.get('traits', {})
                    
                    # Check if all selected traits match
                    match = True
                    for trait_type, trait_value in selected_traits.items():
                        if traits.get(trait_type) != trait_value:
                            match = False
                            break
                    
                    if match:
                        rarity_info = calculate_rarity_score(traits, rarity_db) if rarity_db else {"score": 0, "tier": "Unknown"}
                        matches.append({
                            "token_id": token_id,
                            "traits": traits,
                            "rarity": rarity_info,
                            "image_url": f"https://api.normies.art/normie/{token_id}/image.png"
                        })
                
                # Rate limiting
                time.sleep(0.05)
                
            except Exception as e:
                errors += 1
                continue
        
        return jsonify({
            "found": len(matches),
            "checked": checked,
            "errors": errors,
            "searched_total": 10000,
            "is_complete_search": True,
            "matches": matches
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/portfolio', methods=['GET'])
def portfolio():
    """Get wallet portfolio"""
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
    """Get Normie AI personality"""
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
    """Compare two Normies"""
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
    """Get global statistics"""
    try:
        if not rarity_db or not all_normies_data:
            return jsonify({"error": "Data not loaded yet"}), 503
        
        stats_summary = get_stats_summary(rarity_db)
        
        return jsonify({
            "total_normies": sample_size,
            "is_full_collection": sample_size >= 10000,
            "trait_stats": stats_summary
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/trait-options', methods=['GET'])
def trait_options():
    """Get all trait options"""
    from rarity import TRAIT_OPTIONS
    return jsonify(TRAIT_OPTIONS), 200


@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404


@app.errorhandler(500)
def server_error(error):
    return jsonify({"error": "Server error"}), 500


if __name__ == '__main__':
    print("=" * 50)
    print("🎨 NORMIES ANALYZER")
    print("=" * 50)
    
    # Start background data fetch (non-blocking)
    start_background_fetch()
    
    # Railway deployment: use PORT env variable
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV', 'production') == 'development'
    
    print("Starting Flask app...")
    print(f"📍 Server running on port {port}")
    print("Press Ctrl+C to stop")
    print("=" * 50)
    
    app.run(host='0.0.0.0', port=port, debug=debug)