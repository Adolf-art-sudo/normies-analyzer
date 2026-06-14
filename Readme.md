# 🎨 Normies Analyzer

**NFT rarity analyzer for Normies with parallel data fetching and real-time stats**

## Features

- 🚀 **Parallel Data Fetching** — Fetch 500 Normies in ~1 minute (10 workers)
- 📊 **Rarity Scoring** — Calculate rarity tiers based on trait frequency
- 👤 **Wallet Analysis** — Check your Normies collection stats
- 🔄 **Compare Normies** — Side-by-side trait comparison
- 💾 **Smart Caching** — Automatic data persistence with background loading
- ⚡ **Real-time Progress** — UI polling for fetch status

## Setup

### Local Development

```bash
# Clone repo
git clone https://github.com/Adolf-art-sudo/normies-analyzer.git
cd normies-analyzer

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt

# Run app
python app.py
```

Open **http://localhost:5000** in your browser.

### First Run

The app will automatically:
1. Detect missing `normies_data.json`
2. Start parallel fetching (5% sample = ~500 Normies)
3. Cache data to disk
4. Start Flask server immediately (fetch runs in background)

## Deployment to Railway

### 1. Push to GitHub
```bash
git add .
git commit -m "Add Railway deployment files"
git push origin main
```

### 2. Deploy on Railway
- Go to **railway.app**
- Click **New Project** → **Deploy from GitHub**
- Connect your GitHub account & select `normies-analyzer`
- Railway auto-detects Flask from `Procfile`
- Add environment variables in Railway dashboard:
  ```
  FLASK_ENV=production
  ```

### 3. Access Your App
Railway will give you a URL like: `https://normies-analyzer-prod.up.railway.app`

## Architecture

```
app.py (Flask server)
├── Background Thread: fetch_sample_normies_parallel()
│   ├── ThreadPoolExecutor (10 workers)
│   ├── API requests to https://api.normies.art/normie/{id}/traits
│   └── Saves → normies_data.json
│
├── /rarity endpoint
├── /wallet endpoint
├── /compare endpoint
└── /stats endpoint
```

## File Structure

```
normies-analyzer/
├── app.py                 # Main Flask app
├── rarity.py             # Rarity scoring logic
├── normies.py            # Normie data model
├── templates/            # HTML templates
├── static/               # CSS/JS assets
├── requirements.txt      # Python dependencies
├── Procfile             # Railway deployment config
├── .gitignore           # Git ignore rules
└── README.md            # This file
```

## Technologies

- **Flask** — Web framework
- **ThreadPoolExecutor** — Parallel fetching
- **Requests** — HTTP client
- **JSON** — Data storage

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Home page |
| `/rarity` | GET | Get rarity scores for Normies |
| `/wallet` | GET | Analyze wallet holdings |
| `/compare` | GET | Compare two Normies |
| `/stats` | GET | Global statistics |
| `/personality` | GET | Get AI personality |
| `/progress` | GET | Fetch progress status |

## License

MIT

## Author

Adolf-art-sudo