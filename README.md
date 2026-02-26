# SFP Strategy — Bullish Swing Failure Pattern Bot

A modular Python trading system designed to detect and trade **Bullish Swing Failure Patterns (SFPs)** using exchange price data.  
This project focuses on clean signal generation, explicit logic, and reproducible execution.

---

## 🚀 Features

- **Bullish SFP detection** with explicit, non‑overlapping conditions  
- Modular architecture:
  - `sfp_bot.py` — main execution logic  
  - `sfp_signals.py` — signal generation and pattern detection  
- Logging support for debugging and trade tracking  
- Clean separation of configuration, environment variables, and strategy logic  
- Designed for easy backtesting and live execution

---

Files intentionally **excluded** from the repository:
- `.env` (API keys, secrets)
- `*.log` (runtime logs)
- `trade_log.csv` (local trade history)
- `__pycache__/` and `*.pyc`
- Jupyter notebooks and backup `.txt` files

---

## 🔧 Installation

Clone the repository:

```bash
git clone https://github.com/Ur1k/SFP_Strategy.git
cd SFP_Strategy
```
Create and activate a virtual environment:
python -m venv venv
.\venv\Scripts\activate
Install dependencies:
pip install -r requirements.txt

Environment Variables
Create a .env file in the project root:
API_KEY=your_api_key_here
API_SECRET=your_secret_here
BASE_URL=https://api.exchange.com


⚠️ Never commit your .env file.
It is protected by .gitignore.

📈 How It Works
- sfp_signals.py scans price data for:
- Swing highs/lows
- Liquidity grabs
- Failed breakouts
- Confirmation candles
- sfp_bot.py:
- Loads configuration
- Fetches price data
- Calls the signal engine
- Executes trades or prints alerts
- Logs results for debugging
The logic is fully modular, making it easy to extend with:
- Bearish SFPs
- Swing structure
- Volume filters
- Multi‑timeframe confirmation

▶️ Running the Bot
python sfp_bot.py


You can modify parameters inside the script or through environment variables.

🧪 Backtesting (Optional)
You can integrate this bot with any backtesting engine.
Recommended future improvements:
- Add a backtest.py module
- Add CSV/Parquet data loaders
- Add performance metrics (win rate, drawdown, RR, etc.)

📌 Roadmap
- [ ] Add Bearish SFP detection
- [ ] Add multi‑timeframe filtering
- [ ] Add exchange connector abstraction
- [ ] Add backtesting module
- [ ] Add unit tests
- [ ] Add documentation for each module

🤝 Contributing
Pull requests are welcome.
For major changes, open an issue first to discuss what you’d like to improve.

📜 License
This project is for educational and personal use.
