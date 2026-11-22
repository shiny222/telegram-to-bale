# Telegram → Bale Bridge

Forwards messages, photos and videos from a Telegram channel to a Bale chat/channel in real time.

## Requirements

- Python 3.10 or newer
- A Telegram bot token (from [@BotFather](https://t.me/BotFather))
- A Bale bot token (from the Bale bot creator)
- A Bale chat/channel to forward to

## Installation

Use [Direct Setup for the Bots](https://github.com/shiny222/direct-setup-the-bots)
to install this bot with a local Telegram Bot API. The setup repository includes
a Linux systemd installer and Docker launchers for Linux and Windows.

Choose scenario **1** for Rubika, **2** for Bale, or **3** to run both.

## Setup

### 1. Create your config

```bash
cp config.example.json config.json
```

`config.json` is **gitignored** — never commit it. It holds all your secrets.

### 2. Fill in the values

| Key | What it is |
|---|---|
| `TG_BOT_TOKEN` | Your Telegram bot token from @BotFather (e.g. `123456:ABC...`) |
| `BALE_BOT_TOKEN` | Your Bale bot token |
| `BALE_CHAT_ID` | The Bale chat/channel you want messages sent to |
| `TG_SOURCE_CHAT_ID` | Optional: only forward messages from this Telegram chat; leave empty to forward everything |
| `MY_CHANNEL_TEXT` | Text attached to forwarded messages |
| `MY_CHANNEL_LINK` | Your channel link, e.g. `https://ble.ir/yourchannel` |

#### `LOCAL_API` (optional, recommended for big files)

Uses a [Telegram Local Bot API server](https://core.telegram.org/bots/api#using-a-local-bot-api-server)
instead of the public API so large files (up to 2 GB) can be downloaded:

- `enabled` — `true`/`false`
- `api_id` / `api_hash` — from [my.telegram.org](https://my.telegram.org)
- `executable_path` — path to the `telegram-bot-api` binary
- `host` / `port` — where the local API server listens (default `127.0.0.1:8081`)
- `start_server` — set `true` to let the bridge launch the server itself
- `max_download_mb` — max download size in MB (default `2000`)

#### `SHARED_CACHE`

A download cache **shared with the Rubika bridge**, so files are only downloaded once:

- `dir` — cache folder path
- `keep_hours` — how long files are kept (default `1`)
- `max_gb` — maximum cache size (default `20`)

#### `UPLOAD`

Upload retry behaviour:

- `max_attempts` — retries per file (default `3`)
- `retry_delay_sec` — delay between retries (default `30`)
- `base_timeout_sec` / `timeout_per_mb` — upload timeout calculation
- `pending_queue` — file where failed uploads are queued (default `pending_uploads.jsonl`)

### 3. Install dependencies & run

**Linux:**

```bash
./run_bridge2.sh
```

**Windows:**

```
run_bridge2.bat
```

The launcher creates a virtual environment and installs dependencies
automatically on first run, and restarts the bridge if it ever crashes.

### Update

`./update.sh` (Linux) or `update.bat` (Windows) pulls the latest code from git.

## Useful tools

- `get_chat_id.py` — prints the `getUpdates` URL to find a Bale chat ID
  (reads your token from `config.json`)

## Security

- Never commit `config.json` — it is listed in `.gitignore`.
- If a token ever leaks to a public repo, **revoke it immediately** and regenerate it.
