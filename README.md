README.md

Overview
This repository contains a single-file FastAPI app (main.py) that:
- Scans Bybit USDT-perpetual markets for MACD multi-timeframe signals (root TFs: 1h & 4h).
- Subscribes lightweight public websockets for 5m and 15m klines (auto-detects topic format).
- Optionally connects to Bybit private websocket (authenticated) to receive order updates.
- Creates bracket-style orders (market entry + stop-market stop loss) and persists trades.
- Persists signals, trades, subscriptions and recent raw websocket messages in SQLite.
- Sends Telegram push notifications for signals and order events.
The app is designed to be lightweight and suitable for Render free-tier.

Files
- main.py — application (FastAPI, scanner, websocket managers, sqlite persistence)
- requirements.txt — Python dependencies
- Procfile — start command for Render
- .env.example — example environment variables
- README.md — this file (deployment & usage)

Requirements
- Python 3.10+
- Dependencies (install via pip install -r requirements.txt)

Quick install (local)
1. Clone repo and cd into it.
2. Create a virtualenv and activate it.
3. pip install -r requirements.txt
4. Copy .env.example to .env and fill required values (or set env vars in your environment).
5. Run locally:
   uvicorn main:app --host 0.0.0.0 --port 8000

Environment variables
Minimum for testing (set in .env or in Render environment):
- TELEGRAM_BOT_TOKEN — Telegram bot token
- TELEGRAM_CHAT_ID — Telegram chat id to send messages
Optional / required for trading:
- BYBIT_API_KEY — Bybit API key (testnet for testing)
- BYBIT_API_SECRET — Bybit API secret
- BYBIT_USE_MAINNET — "true" to use mainnet, default false (use testnet)
- TRADING_ENABLED — "true" to place real orders (start with false)
Core settings:
- MAX_OPEN_TRADES — integer, default 5
- SCAN_INTERVAL_SECONDS — polling interval, default 60
- ROOT_SCAN_LOOKBACK — how many root candles to look back, default 3
- DB_PATH — path to sqlite DB, default scanner.db
- LOG_LEVEL — info | debug | none

Important safety notes
- Keep TRADING_ENABLED=false and use Bybit testnet keys while testing.
- Only enable TRADING_ENABLED=true after thorough test of signals & order behavior.
- Do not commit secrets — use Render environment variables.

Endpoints
- GET /health — basic health check
- GET /status — current active signals, trades, subscription lists, ws status
- POST /toggle — JSON { "trading_enabled": bool, "max_open_trades": int } toggles runtime settings
- GET /debug/ws/messages?limit=50 — dump recent raw websocket messages (useful to tune WS auth/topic)
- POST /debug/ws/clear — clear saved raw WS messages

Database (SQLite)
- root_signals — active root flips persisted across restarts
- trades — opened trades with basic metadata and raw API responses
- raw_ws_messages — recent raw websocket messages saved for debugging
- public_subscriptions — public topics persisted
- private_subscriptions — private topics persisted
DB path defaults to scanner.db (configurable via DB_PATH).

How it works (high level)
1. root_scanner_loop runs for 1h and 4h, polling klines and detecting MACD negative→positive flips on candle open.
2. When a root flip is detected, it registers a root signal, persists it, sends Telegram, and subscribes public WS topics (5m & 15m) for that symbol.
3. The public WS manager auto-detects topic template and dispatches kline events to the evaluator.
4. The active signal watcher evaluates smaller TFs and the last flipping TF; when alignment is reached it sends a Telegram signal and optionally places a bracket trade (market + stop-market).
5. Private WS (authenticated) receives order updates and updates the trades table; subscriptions to private topics are persisted and unsubscribed when signals expire.
6. Signals expire at the close of the root timeframe; on expiry the app unsubscribes public & private topics and cleans up persisted entries.

Deploy to Render (short)
1. Create a Render Web Service (Python).
2. Add main.py and requirements.txt to your repo and connect to Render.
3. Set environment variables in Render's dashboard (do not upload .env with secrets).
4. Start command: uvicorn main:app --host 0.0.0.0 --port $PORT (Procfile included).
5. Monitor logs, /health and Telegram messages.

Troubleshooting & tips
- If public WS subscription fails, check /debug/ws/messages for raw responses; the manager auto-detects common topic templates but some environments may require adding templates.
- If private WS auth fails, inspect /debug/ws/messages for the auth reply and share it to adapt prehash/signing sequence.
- Keep TRADING_ENABLED=false while testing; confirm signals & WS messages first.
- For production, monitor DB size and prune raw_ws_messages regularly (there’s a debug clear endpoint).

Extending or customizing
- MACD parameters are configurable via env vars MACD_FAST, MACD_SLOW, MACD_SIGNAL.
- Add take-profit order logic or trailing stops by extending place_market_entry_and_stop.
- Convert to multiple worker processes or split subsystems if you need to scale.

Files to deploy (minimum)
- main.py
- requirements.txt
Optional but recommended:
- Procfile
- .env.example

Contact / support
If you need adjustments (e.g., precise Bybit auth payload changes after viewing raw auth replies), paste the raw private WS auth response and I’ll adapt the signing logic.

License
MIT-style (adapt as needed).
