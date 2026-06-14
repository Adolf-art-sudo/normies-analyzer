# rarity.py
# This file calculates HOW RARE a Normie is
# Uses pandas to analyze trait frequencies

import requests   # For API calls
import pandas as pd  # For data analysis
import json       # For saving/loading data
import time       # For rate limiting
import os         # For file operations

# ─────────────────────────────────────────
# TRAIT OPTIONS (from API documentation)
# These are ALL possible trait values
# ─────────────────────────────────────────
TRAIT_OPTIONS = {
    "Type":             ["Human", "Cat", "Alien", "Agent"],
    "Gender":           ["Male", "Female", "Non-Binary"],
    "Age":              ["Young", "Middle-Aged", "Old"],
    "Hair Style":       ["Short Hair", "Long Hair", "Curly Hair", "Braids",
                         "Bun", "Ponytail", "Mohawk", "Spiky Hair",
                         "Wavy Hair", "Afro", "Bald", "Pigtails",
                         "Bowl Cut", "Side Part", "Undercut", "Dreadlocks",
                         "Cornrows", "Fringe", "Shaggy Hair", "Slick Back",
                         "Top Knot"],
    "Facial Feature":   ["Clean Shaven", "Full Beard", "Mustache", "Goatee",
                         "Soul Patch", "Stubble", "Shadow Beard",
                         "Chinstrap", "Mutton Chops", "Handlebar Mustache",
                         "Van Dyke", "Circle Beard", "Pencil Mustache",
                         "Fu Manchu", "Sideburns", "Neck Beard", "5 O Clock Shadow"],
    "Eyes":             ["Classic Shades", "Big Shades", "Round Glasses",
                         "Square Glasses", "Monocle", "Eye Patch",
                         "3D Glasses", "Laser Eyes", "Heart Eyes",
                         "Star Eyes", "Sunglasses", "Visor",
                         "Night Vision", "VR Headset"],
    "Expression":       ["Neutral", "Slight Smile", "Serious",
                         "Happy", "Angry", "Sad", "Surprised"],
    "Accessory":        ["Top Hat", "Fedora", "Cowboy Hat", "Beanie",
                         "Baseball Cap", "Beret", "Crown", "Headphones",
                         "Hoodie", "Tie", "Bow Tie", "Chain",
                         "Earring", "Bandana", "Helmet"]
}


# ─────────────────────────────────────────
# STEP 1: FETCH NORMIE DATA
# Fetches multiple Normies and saves to JSON
# ─────────────────────────────────────────
def fetch_normies_data(start=0, count=200, save_file="normies_data.json"):
    """
    Fetches trait data for multiple Normies
    Saves to a JSON file so we don't need to fetch again
    
    IMPORTANT: Respects 60 req/min rate limit!
    Uses time.sleep(1) between requests
    """
    
    # Check if we already have saved data
    if os.path.exists(save_file):
        print(f"Found existing data in {save_file}!")
        print("Loading from file (no API calls needed)...")
        with open(save_file, 'r') as f:
            return json.load(f)
    
    print(f"Fetching {count} Normies from API...")
    print("This will take a few minutes (rate limiting)...")
    
    all_data = []
    
    for i in range(start, start + count):
        try:
            # Fetch traits for this Normie
            url = f"https://api.normies.art/normie/{i}/traits"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # Build a simple row of data
                row = {"token_id": i}
                for attr in data.get('attributes', []):
                    trait_type = attr['trait_type']
                    value = attr['value']
                    # Only save the 8 main trait types
                    if trait_type in TRAIT_OPTIONS:
                        row[trait_type] = value
                
                all_data.append(row)
                print(f"  Fetched Normie #{i} ✓")
                
            elif response.status_code == 429:
                # Rate limited! Wait and retry
                print(f"  Rate limited! Waiting 60 seconds...")
                time.sleep(60)
                i -= 1  # Retry this one
                
            # Wait 1 second between requests
            # 60 req/min = 1 per second maximum
            time.sleep(1)
            
        except Exception as e:
            print(f"  Error fetching #{i}: {e}")
            continue
    
    # Save to JSON file
    print(f"\nSaving {len(all_data)} Normies to {save_file}...")
    with open(save_file, 'w') as f:
        json.dump(all_data, f, indent=2)
    
    print("Done! Data saved.")
    return all_data


# ─────────────────────────────────────────
# STEP 2: BUILD RARITY DATABASE
# Calculates how rare each trait value is
# ─────────────────────────────────────────
def build_rarity_database(data):
    """
    Analyzes all Normie data and calculates rarity
    Returns a dict of trait frequencies
    
    Example output:
    {
        "Type": {
            "Alien":  {"count": 100, "percent": 10.0},
            "Human":  {"count": 700, "percent": 70.0},
            "Cat":    {"count": 150, "percent": 15.0},
            "Agent":  {"count": 50,  "percent": 5.0}
        },
        ...
    }
    """
    # Convert to DataFrame for easy analysis
    df = pd.DataFrame(data)
    
    total = len(df)
    rarity_db = {}
    
    # Calculate frequency for each trait category
    for trait_name in TRAIT_OPTIONS.keys():
        if trait_name not in df.columns:
            continue
            
        # Count how many Normies have each value
        # value_counts() is a pandas function
        counts = df[trait_name].value_counts()
        
        rarity_db[trait_name] = {}
        
        for value, count in counts.items():
            # Calculate what % of Normies have this trait
            percent = round((count / total) * 100, 2)
            rarity_db[trait_name][value] = {
                "count": int(count),
                "percent": percent,
                # Rarity score: lower % = higher score
                "rarity_score": round(100 - percent, 2)
            }
    
    return rarity_db


# ─────────────────────────────────────────
# STEP 3: CALCULATE RARITY SCORE
# Gives a single rarity score to one Normie
# ─────────────────────────────────────────
def calculate_rarity_score(traits, rarity_db):
    """
    Calculates how rare ONE Normie is
    
    Method: Average rarity score of all traits
    Rarer traits = higher score = more valuable
    
    Returns:
    - score (0-100): higher = rarer
    - breakdown: which traits are rare
    - tier: Common/Uncommon/Rare/Epic/Legendary
    """
    if not rarity_db:
        return {"score": 0, "tier": "Unknown", "breakdown": {}}
    
    scores = []
    breakdown = {}
    
    for trait_name, trait_value in traits.items():
        # Skip non-trait fields like Level, Pixel Count etc
        if trait_name not in rarity_db:
            continue
        
        if trait_value in rarity_db[trait_name]:
            trait_info = rarity_db[trait_name][trait_value]
            rarity_score = trait_info['rarity_score']
            percent = trait_info['percent']
            
            scores.append(rarity_score)
            breakdown[trait_name] = {
                "value": trait_value,
                "percent": percent,
                "rarity_score": rarity_score,
                # Label for how rare this specific trait is
                "label": get_rarity_label(percent)
            }
    
    if not scores:
        return {"score": 0, "tier": "Unknown", "breakdown": {}}
    
    # Overall score = average of all trait scores
    overall_score = round(sum(scores) / len(scores), 1)
    
    return {
        "score": overall_score,
        "tier": get_rarity_tier(overall_score),
        "breakdown": breakdown
    }


# ─────────────────────────────────────────
# HELPER FUNCTIONS
# Small functions used by the main ones
# ─────────────────────────────────────────
def get_rarity_label(percent):
    """Returns a text label based on how common a trait is"""
    if percent <= 2:
        return "Ultra Rare"
    elif percent <= 5:
        return "Very Rare"
    elif percent <= 15:
        return "Rare"
    elif percent <= 30:
        return "Uncommon"
    else:
        return "Common"


def get_rarity_tier(score):
    """Returns overall tier based on rarity score"""
    if score >= 85:
        return "Legendary"
    elif score >= 70:
        return "Epic"
    elif score >= 55:
        return "Rare"
    elif score >= 40:
        return "Uncommon"
    else:
        return "Common"


def get_tier_color(tier):
    """Returns CSS color for each tier (used in HTML)"""
    colors = {
        "Legendary": "#FFD700",   # Gold
        "Epic":      "#9B59B6",   # Purple
        "Rare":      "#3498DB",   # Blue
        "Uncommon":  "#2ECC71",   # Green
        "Common":    "#95A5A6",   # Gray
        "Unknown":   "#95A5A6"    # Gray
    }
    return colors.get(tier, "#95A5A6")


def get_stats_summary(rarity_db):
    """
    Returns interesting stats about the dataset
    Used for the Stats page
    """
    if not rarity_db:
        return {}
    
    stats = {}
    
    for trait_name, values in rarity_db.items():
        # Find rarest value in this category
        rarest = min(values.items(), key=lambda x: x[1]['percent'])
        # Find most common value
        most_common = max(values.items(), key=lambda x: x[1]['percent'])
        
        stats[trait_name] = {
            "rarest": {
                "value": rarest[0],
                "percent": rarest[1]['percent'],
                "count": rarest[1]['count']
            },
            "most_common": {
                "value": most_common[0],
                "percent": most_common[1]['percent'],
                "count": most_common[1]['count']
            },
            "total_values": len(values)
        }
    
    return stats


# ─────────────────────────────────────────
# TEST YOUR CODE
# Run: python rarity.py
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("Testing Rarity Calculator...")
    print("-" * 40)
    
    # Fetch small sample (just 10 for testing)
    print("Fetching 10 Normies for testing...")
    data = []
    for i in range(10):
        url = f"https://api.normies.art/normie/{i}/traits"
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            row = {"token_id": i}
            for attr in r.json().get('attributes', []):
                if attr['trait_type'] in TRAIT_OPTIONS:
                    row[attr['trait_type']] = attr['value']
            data.append(row)
            time.sleep(1)  # Rate limit!
    
    # Build rarity database
    print("\nBuilding rarity database...")
    rarity_db = build_rarity_database(data)
    
    # Test with Normie #0's traits
    test_traits = data[0] if data else {}
    print(f"\nTesting rarity for: {test_traits}")
    
    result = calculate_rarity_score(test_traits, rarity_db)
    print(f"\nRarity Score: {result['score']}")
    print(f"Tier: {result['tier']}")
    print("\nBreakdown:")
    for trait, info in result['breakdown'].items():
        print(f"  {trait}: {info['value']} ({info['percent']}% - {info['label']})")
    
    print("-" * 40)
    print("Test complete!")
