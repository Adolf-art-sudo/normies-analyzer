# normie.py
# This is the BRAIN of your project
# It talks to the Normies API and gets all data

import requests  # Library to make API calls
import time      # Library to add delays (for rate limiting)

class Normie:
    """
    This class represents ONE Normie NFT
    You give it a token ID (0-9999)
    It fetches all data about that Normie
    
    Example usage:
        n = Normie(42)
        traits = n.get_traits()
        image_url = n.get_image_url()
    """
    
    # Base URL for all API calls
    # This is a CLASS variable - shared by all Normie objects
    BASE_URL = "https://api.normies.art"
    
    def __init__(self, token_id):
        """
        This runs when you create a Normie object
        Example: n = Normie(42)
        token_id = the number of the Normie (0 to 9999)
        """
        self.token_id = int(token_id)  # Store the ID
        self.traits_cache = None        # Cache so we don't call API twice
    
    # ─────────────────────────────────────────
    # GET TRAITS
    # ─────────────────────────────────────────
    def get_traits(self):
        """
        Gets all traits of this Normie
        Returns a dict like:
        {
            'Type': 'Alien',
            'Gender': 'Non-Binary',
            'Eyes': 'Big Shades',
            ...
        }
        """
        # If we already fetched traits, use cached version
        # This saves API calls!
        if self.traits_cache:
            return self.traits_cache
        
        try:
            url = f"{self.BASE_URL}/normie/{self.token_id}/traits"
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # Raises error if 404 or 429
            
            data = response.json()
            
            traits = {}
            for attr in data.get('attributes', []):
                traits[attr['trait_type']] = attr['value']
            
            self.traits_cache = traits  # Save to cache
            return traits
            
        except requests.exceptions.Timeout:
            return {"error": "API too slow! Try again."}
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return {"error": f"Normie #{self.token_id} not found!"}
            elif e.response.status_code == 429:
                return {"error": "Rate limit hit! Wait 1 minute."}
            return {"error": str(e)}
        except Exception as e:
            return {"error": str(e)}
    
    # ─────────────────────────────────────────
    # GET IMAGE URL
    # ─────────────────────────────────────────
    def get_image_url(self):
        """
        Returns the direct URL to this Normie's image
        We use this in HTML like: <img src="{{ image_url }}">
        """
        return f"{self.BASE_URL}/normie/{self.token_id}/image.png"
    
    # ─────────────────────────────────────────
    # GET OWNER
    # ─────────────────────────────────────────
    def get_owner(self):
        """
        Returns who owns this Normie
        Returns: wallet address like "0x1234...abcd"
        """
        try:
            url = f"{self.BASE_URL}/normie/{self.token_id}/owner"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return data.get('owner', 'Unknown')
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return "Burned or not minted"
            return "Unknown"
        except Exception:
            return "Unknown"
    
    # ─────────────────────────────────────────
    # GET CANVAS INFO (Level, Action Points)
    # ─────────────────────────────────────────
    def get_canvas_info(self):
        """
        Returns level and action points of this Normie
        Returns: dict with level, actionPoints, customized
        """
        try:
            url = f"{self.BASE_URL}/normie/{self.token_id}/canvas/info"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
            
        except Exception:
            # Return defaults if canvas info not available
            return {
                "level": 1,
                "actionPoints": 0,
                "customized": False
            }
    
    # ─────────────────────────────────────────
    # GET ALL DATA AT ONCE
    # ─────────────────────────────────────────
    def get_full_data(self):
        """
        Gets EVERYTHING about this Normie in one call
        Returns a complete dict with all info
        """
        traits = self.get_traits()
        
        # If there's an error in traits, return early
        if 'error' in traits:
            return traits
        
        canvas = self.get_canvas_info()
        owner = self.get_owner()
        
        return {
            'token_id': self.token_id,
            'image_url': self.get_image_url(),
            'owner': owner,
            'traits': traits,
            'level': canvas.get('level', 1),
            'action_points': canvas.get('actionPoints', 0),
            'customized': canvas.get('customized', False)
        }


# ─────────────────────────────────────────
# TEST YOUR CODE
# Run this file directly to test:
# python normie.py
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("Testing Normie class...")
    print("-" * 40)
    
    # Create a Normie object
    n = Normie(42)
    
    # Get traits
    print("Traits:")
    traits = n.get_traits()
    for key, value in traits.items():
        print(f"  {key}: {value}")
    
    # Get image URL
    print(f"\nImage URL: {n.get_image_url()}")
    
    # Get owner
    print(f"Owner: {n.get_owner()}")
    
    # Get canvas info
    print(f"Canvas: {n.get_canvas_info()}")
    
    print("-" * 40)
    print("Test complete!")
