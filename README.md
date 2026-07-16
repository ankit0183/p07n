# CtbCap Multi-Model Recorder v2.0

A Python-based multi-model livestream recorder for **Chaturbate** and **StripChat**.
Records ALL configured online models concurrently with auto-discovery, auto-restart,
tmux integration, and full CLI management from config.yaml.

## Requirements

- Python 3.8+
- `aiohttp` (`pip install aiohttp`)
- `pyyaml` (`pip install pyyaml`)
- `ffmpeg` installed and in PATH
- `tmux` (optional, for tmux features)

### Install dependencies

```bash
pip install aiohttp pyyaml
```

### Install ffmpeg

**Termux (Android):**
```bash
pkg install ffmpeg
```

**macOS (Homebrew):**
```bash
brew install ffmpeg
```

**Ubuntu / Debian:**
```bash
sudo apt install ffmpeg
```

---

## Quick Start

### 1. Create config.yaml (or edit the existing one)

```yaml
global:
  save_path: "./ctbcap_rec"
  log_path: "./ctbcap_rec/log"
  platform: "stripchat"
  check_interval: 60
  retry_interval: 60
  cut_time: 0
  ffmpeg_codec: "copy"
  auto_restart: true

models:
  - name: "model_username"
    platform: "stripchat"
    enabled: true
    check_interval: 300
```

### 2. Start recording

```bash
python3 ctbcap_multi.py start
```

---

## CLI Commands (All Working)

### `start` — Start Recording All Models

Starts monitoring all enabled models in config.yaml. When a model goes online, it
automatically begins recording. When they go offline, recording stops after a grace period.

```bash
# Start with default config.yaml
python3 ctbcap_multi.py start

# Start with custom config file
python3 ctbcap_multi.py start -c /path/to/myconfig.yaml

# Start in background (daemon mode, acquires Termux wake-lock on Android)
python3 ctbcap_multi.py start -D

# Combine daemon mode with custom config
python3 ctbcap_multi.py start -D -c ./config.yaml
```

**What it does:**
- Reads all models from config.yaml
- Spawns an async monitor for each enabled model
- Checks the platform API at each model's `check_interval`
- Starts FFmpeg recording when model is online
- Stops recording after `offline_grace_seconds` when model goes offline
- Auto-restarts crashed recordings with exponential backoff
- Runs health server on port 8080 (if enabled)

---

### `stop` — Stop Running Daemon

Sends SIGTERM to a running daemon process via the PID file.

```bash
python3 ctbcap_multi.py stop
```

**What it does:**
- Reads the PID from `ctbcap.pid`
- Sends SIGTERM to stop the process cleanly
- Releases Termux wake-lock (on Android)
- Removes the PID file

---

### `add` — Add a Single Model to config.yaml

Adds a new model entry to your config.yaml file from the command line.

```bash
# Add a StripChat model (uses global platform default)
python3 ctbcap_multi.py add "model_username"

# Specify platform explicitly
python3 ctbcap_multi.py add "model_username" --platform stripchat
python3 ctbcap_multi.py add "model_username" -p chaturbate

# Set custom check/retry intervals
python3 ctbcap_multi.py add "model_username" -p stripchat \
  --check-interval 60 \
  --retry-interval 30

# Set cut time (0 = continuous, 3600 = 1 hour segments)
python3 ctbcap_multi.py add "model_username" -p stripchat --cut-time 3600
```

**Options:**
| Flag | Description | Default |
|------|-------------|---------|
| `name` | Model/username (required) | — |
| `--platform, -p` | `chaturbate` or `stripchat` | From global config |
| `--check-interval` | Seconds between online checks | From global config |
| `--retry-interval` | Seconds between retry attempts | From global config |
| `--cut-time` | Segment length in seconds (0=continuous) | From global config |

**What it does:**
- Adds a new model entry to the `models:` list in config.yaml
- Auto-generates `save_path` and `log_path` based on global settings
- If the model already exists, prints a message and does nothing

---

### `add-bulk` — Add Multiple Models at Once

Add many models in a single command, separated by commas.

```bash
# Add multiple StripChat models
python3 ctbcap_multi.py add-bulk "user1,user2,user3" --platform stripchat

# Add multiple Chaturbate models
python3 ctbcap_multi.py add-bulk "user1,user2,user3" -p chaturbate

# With custom intervals
python3 ctbcap_multi.py add-bulk "user1,user2,user3" -p stripchat \
  --check-interval 120 --retry-interval 60
```

**Options:**
| Flag | Description | Default |
|------|-------------|---------|
| `names` | Comma-separated model names (required) | — |
| `--platform, -p` | Platform for all models | `stripchat` |
| `--check-interval` | Check interval for all models | From global config |
| `--retry-interval` | Retry interval for all models | From global config |
| `--cut-time` | Cut time for all models | From global config |

**Example output:**
```
Added 3 new models (0 already existed)
Total models in config: 134
```

---

### `remove` — Remove a Model from config.yaml

Remove a model entry from config.yaml by name.

```bash
python3 ctbcap_multi.py remove "model_username"
```

**What it does:**
- Finds and deletes the model entry matching the name
- If the model was not found, prints a message

**Example output:**
```
Removed model 'model_username' from ./config.yaml
```

---

### `list` — List All Configured Models

Display all models in config.yaml, grouped by platform with status.

```bash
python3 ctbcap_multi.py list
```

**Example output:**
```
============================================================
 CtbCap Models (131 enabled, 0 disabled)
============================================================

  [CHATURBATE] (19 models)
  --------------------------------------------------
    [ON] _magic_kis                     check=60s
    [ON] best_fucks                     check=300s
    [ON] cuty_petite                    check=60s
    ...

  [STRIPCHAT] (112 models)
  --------------------------------------------------
    [ON] 4u4Love                        check=120s
    [ON] Ada-19                         check=300s
    [ON] Meav_                          check=300s
    ...

============================================================
 Total: 131 models across 2 platforms
============================================================
```

---

### `status` — Show Live Recording Status

Connects to the health server (port 8080) and shows current recording stats.

```bash
# Basic status
python3 ctbcap_multi.py status

# Verbose (includes offline model list)
python3 ctbcap_multi.py status --verbose
python3 ctbcap_multi.py status -V
```

**Example output:**
```
======================================================================
 CtbCap Status - 2026-07-16 14:30:00
======================================================================

  ACTIVE RECORDINGS (3)
  ----------------------------------------------------------------------
    model1                           01:23:45     245.3 MB    1.2 MB/s  pid=12345  restarts=0
    model2                           00:45:12     112.7 MB    0.8 MB/s  pid=12346  restarts=1
    model3                           00:12:33      28.4 MB    0.5 MB/s  pid=12347  restarts=0

  MONITORS: 131 total | 5 online | 126 offline | 0 unknown

  ONLINE (5)
    [ON]  model1
    [ON]  model2
    [ON]  model3
    [ON]  model4
    [ON]  model5

======================================================================
```

**Note:** CtbCap must be running for status to work. The health server must be enabled in config.

---

### `discover` — Discover ALL Online Models

Scan the platform API to find every currently online model. This is the key feature
for recording ALL available online models.

```bash
# Discover online models on default platform (from config)
python3 ctbcap_multi.py discover

# Discover on specific platform
python3 ctbcap_multi.py discover --platform stripchat
python3 ctbcap_multi.py discover -p chaturbate

# Scan up to 1000 models (default: 500)
python3 ctbcap_multi.py discover --limit 1000

# Discover AND automatically add all new models to config.yaml
python3 ctbcap_multi.py discover --auto-add

# Discover on specific platform and auto-add
python3 ctbcap_multi.py discover -p stripchat --auto-add

# Combine all flags
python3 ctbcap_multi.py discover -p stripchat --limit 1000 --auto-add
```

**Options:**
| Flag | Description | Default |
|------|-------------|---------|
| `--platform, -p` | Platform to scan | From global config |
| `--limit` | Max models to scan | `500` |
| `--auto-add, -a` | Auto-add new models to config.yaml | `false` |

**Example output (without --auto-add):**
```
Discovering online models on stripchat...

======================================================================
 Found 47 ONLINE models on STRIPCHAT
======================================================================
  Already configured: 12
  New (not in config): 35
    alice_live                           viewers: 234
    bob_streams                          viewers: 189
    ...

To add all new models, run:
  ./ctbcap_multi.py add-bulk "alice_live,bob_streams,..." --platform stripchat

======================================================================
```

**Example output (with --auto-add):**
```
Discovering online models on stripchat...

======================================================================
 Found 47 ONLINE models on STRIPCHAT
======================================================================
  Already configured: 12
  New (not in config): 35

Adding 35 new models to config...
Added 35 models to ./config.yaml

======================================================================
```

**How it works:**
1. Queries the platform's public API endpoint to get lists of online models
2. Paginates through results (100 per batch) until `--limit` is reached
3. Filters models that have active cam status
4. Compares against existing models in config.yaml
5. With `--auto-add`, appends new models directly to config.yaml
6. You can then start/reload to begin recording the new models

---

### `dedup` — Remove Duplicate Models

Find and remove duplicate model entries in config.yaml (same name appearing multiple times).

```bash
python3 ctbcap_multi.py dedup
```

**Example output:**
```
Removed 4 duplicate entries (135 -> 131 models)
```

---

### `validate` — Validate Config File

Check that config.yaml is valid and list all models.

```bash
python3 ctbcap_multi.py --validate
```

**Example output:**
```
Config validation passed
Enabled models (131): ['Ada-19', 'Priyanka011', ...]
```

---

### `version` — Show Version

```bash
python3 ctbcap_multi.py --version
```

**Output:**
```
CtbCap Multi-Model Recorder v2.0.0
```

---

## Tmux Commands

Tmux lets you run CtbCap in a detachable session that survives terminal closes.

### `tmux` — Start in Tmux Session

Creates a new tmux session named `ctbcap` and starts recording inside it.

```bash
python3 ctbcap_multi.py tmux
```

**What it does:**
- Kills any existing `ctbcap` tmux session
- Creates a new session (220x50 characters)
- Runs `start` inside the session
- Prints the attach command

**Output:**
```
tmux session 'ctbcap' started
Attach with: tmux attach -t ctbcap
Or run: ./ctbcap_multi.py tmux-attach
```

---

### `tmux-dashboard` — Start Tmux Dashboard

Creates a tmux session with a live-updating dashboard that shows recording status.

```bash
python3 ctbcap_multi.py tmux-dashboard
```

**What it does:**
- Creates a tmux session (120x40 characters)
- Runs `watch` to poll the health server status endpoint every 5 seconds
- Shows live recording stats in the terminal

---

### `tmux-attach` — Attach to Tmux Session

Re-attach to a running CtbCap tmux session.

```bash
python3 ctbcap_multi.py tmux-attach
```

**Equivalent to:**
```bash
tmux attach -t ctbcap
```

**Detaching (without killing):**
Press `Ctrl+B` then `D` to detach from tmux.

---

### `tmux-stop` — Kill Tmux Session

Stop and destroy the CtbCap tmux session.

```bash
python3 ctbcap_multi.py tmux-stop
```

**Equivalent to:**
```bash
tmux kill-session -t ctbcap
```

---

## config.yaml — Full Reference

### Global Settings

```yaml
global:
  # Where recorded videos are saved
  save_path: "./ctbcap_rec"

  # Where log files are saved
  log_path: "./ctbcap_rec/log"

  # Minimum free disk space (MiB) before refusing to record
  save_space_mib: 512

  # Minimum free disk space for logs (MiB)
  log_space_mib: 16

  # Split recordings into segments of this many seconds (0 = continuous)
  cut_time: 0

  # Split recordings when file reaches this size in MiB (0 = no limit)
  cut_size_mib: 0

  # Default platform: "chaturbate" or "stripchat"
  platform: "stripchat"

  # User agent for API requests
  user_agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

  # Random delay (0-600s) before first check to avoid detection
  edging_mode: false

  # Show debug-level logs
  debug_mode: true

  # Disable proxy for FFmpeg
  ignore_proxy: false

  # Default seconds between online status checks
  check_interval: 60

  # Default seconds between retry attempts
  retry_interval: 60

  # FFmpeg codec: "copy" (no re-encode) or "libx264" etc.
  ffmpeg_codec: "copy"

  # Extra FFmpeg arguments (space-separated string)
  ffmpeg_extra_args: ""

  # Auto-restart crashed recordings
  auto_restart: true

  # Maximum restart attempts before giving up
  max_restart_attempts: 10

  # Restart backoff: starts at base, doubles each time, caps at max (seconds)
  restart_backoff_base: 5.0
  restart_backoff_max: 300.0

  # Download queue limits
  download_queue:
    max_concurrent_downloads: 3      # Max simultaneous FFmpeg recordings
    max_concurrent_fetches: 5        # Max simultaneous API check requests
    offline_grace_seconds: 30        # Wait this long before stopping after offline
    fetch_retry_attempts: 3          # API check retry attempts
    fetch_retry_delay: 5.0           # Seconds between retries
    min_free_space_mib: 1024         # Min free disk space (MiB)

  # Notifications (optional)
  notifications:
    enabled: false
    telegram:
      enabled: false
      bot_token: ""                  # Your Telegram bot token
      chat_id: ""                    # Your Telegram chat ID
    discord:
      enabled: false
      webhook_url: ""                # Discord webhook URL
    ntfy:
      enabled: false
      topic: ""                      # ntfy topic name
      server: "https://ntfy.sh"      # ntfy server URL

  # Metadata event logging (JSONL)
  metadata:
    enabled: true
    log_path: "./ctbcap_rec/log/metadata.jsonl"

  # Health check HTTP server
  health_check:
    enabled: true
    port: 8080
```

### Per-Model Settings

Each model entry in the `models:` list supports these fields:

```yaml
models:
  - name: "model_username"          # Required: username on the platform
    platform: "stripchat"           # "chaturbate" or "stripchat" (default: chaturbate)
    save_path: "./ctbcap_rec/model_username"   # Custom save path (optional)
    log_path: "./ctbcap_rec/log/model_username" # Custom log path (optional)
    enabled: true                   # true = monitor and record, false = skip
    check_interval: 300             # Seconds between online checks (overrides global)
    retry_interval: 180             # Seconds between retries (overrides global)
    cut_time: 0                     # Segment length in seconds (overrides global)
    cut_size_mib: 0                 # Max segment size in MiB (overrides global)
    ffmpeg_codec: "copy"            # Override global codec
    ffmpeg_extra_args: ""           # Override global extra args
    auto_restart: true              # Override global auto_restart
    max_restart_attempts: 10        # Override global max_restart_attempts
```

**Minimal model entry:**
```yaml
models:
  - name: "my_model"
    platform: "stripchat"
```

---

## Health Server API

When `health_check.enabled: true`, CtbCap runs an HTTP server on the configured port (default 8080).

### Endpoints

| Method | URL | Description |
|--------|-----|-------------|
| GET | `/health` | Liveness check (always returns 200) |
| GET | `/status` | Full status: all recordings, monitors, bandwidth |
| GET | `/recordings` | Active recording details with bandwidth stats |
| GET | `/metrics` | Prometheus-format metrics |
| POST | `/control/{model}/stop` | Stop recording a specific model |
| POST | `/control/stop-all` | Stop all recordings |
| POST | `/control/reload-config` | Hot-reload config.yaml |
| POST | `/control/discover?platform=stripchat` | Discover online models via API |

### Example: Check Status

```bash
curl http://localhost:8080/status | python3 -m json.tool
```

### Example: Stop a Model

```bash
curl -X POST http://localhost:8080/control/my_model/stop
```

### Example: Reload Config

```bash
curl -X POST http://localhost:8080/control/reload-config
```

### Example: Discover Online Models

```bash
curl -X POST "http://localhost:8080/control/discover?platform=stripchat"
```

---

## Signals

| Signal | Action |
|--------|--------|
| `SIGTERM` / `SIGINT` | Graceful shutdown (stops all recordings, closes connections) |
| `SIGHUP` | Hot-reload config.yaml (adds new models, updates intervals) |
| `SIGUSR1` | Stop health server |
| `SIGUSR2` | Start health server |

**Send SIGHUP to reload config without restarting:**
```bash
kill -HUP $(cat ctbcap.pid)
```

---

## Typical Workflows

### Workflow 1: Record All Currently Online Models

```bash
# Step 1: Discover all online models
python3 ctbcap_multi.py discover -p stripchat --limit 1000

# Step 2: Add them all to config
python3 ctbcap_multi.py discover -p stripchat --limit 1000 --auto-add

# Step 3: Start recording
python3 ctbcap_multi.py start
```

### Workflow 2: Add a New Model and Start

```bash
# Add the model
python3 ctbcap_multi.py add "new_model_name" -p stripchat

# If already running, send SIGHUP to reload
kill -HUP $(cat ctbcap.pid)

# Or stop and restart
python3 ctbcap_multi.py stop
python3 ctbcap_multi.py start
```

### Workflow 3: Run in Background with Tmux

```bash
# Start in tmux
python3 ctbcap_multi.py tmux

# Later, re-attach to check
python3 ctbcap_multi.py tmux-attach

# Detach without stopping (press Ctrl+B then D)

# Stop everything
python3 ctbcap_multi.py tmux-stop
```

### Workflow 4: Run as Daemon

```bash
# Start in background (Termux wake-lock acquired)
python3 ctbcap_multi.py start -D

# Check status
python3 ctbcap_multi.py status

# Stop
python3 ctbcap_multi.py stop
```

### Workflow 5: Monitor Multiple Platforms

```bash
# Add Chaturbate models
python3 ctbcap_multi.py add "cb_model1" -p chaturbate
python3 ctbcap_multi.py add "cb_model2" -p chaturbate

# Add StripChat models
python3 ctbcap_multi.py add "sc_model1" -p stripchat
python3 ctbcap_multi.py add "sc_model2" -p stripchat

# Start - it records from both platforms simultaneously
python3 ctbcap_multi.py start
```

### Workflow 6: Periodic Auto-Discovery Cron Job

Set up a cron job to periodically discover and add new online models:

```bash
# Run every 6 hours to discover and add new StripChat models
0 */6 * * * cd /path/to/p07n && python3 ctbcap_multi.py discover -p stripchat --auto-add >> /var/log/ctbcap_discover.log 2>&1
```

---

## Output Structure

```
ctbcap_rec/
├── log/
│   ├── ctbcap.log              # Main application log
│   ├── metadata.jsonl          # Structured event log (online/offline/recording)
│   └── <model_name>.log        # Per-model log (if using per-model log paths)
├── <model_name>/
│   ├── <model>-20260716-143000.mp4       # Continuous recording
│   ├── <model>-20260716-143000_001.mp4   # Segmented (if cut_time > 0)
│   ├── <model>-20260716-143000_002.mp4
│   └── ...
└── ...
```

**File naming:** `<model>-<YYYYMMDD>-<HHMMSS>.mp4`

---

## Troubleshooting

### "Config file not found"
```bash
# Make sure you're in the right directory or specify the path
python3 ctbcap_multi.py start -c /full/path/to/config.yaml
```

### "No models configured"
```bash
# Add at least one model
python3 ctbcap_multi.py add "model_name" -p stripchat
```

### "Cannot connect to health server"
```bash
# CtbCap is not running. Start it first:
python3 ctbcap_multi.py start
```

### FFmpeg not found
```bash
# Verify ffmpeg is installed
which ffmpeg

# Install if missing
# Termux:  pkg install ffmpeg
# macOS:   brew install ffmpeg
# Ubuntu:  sudo apt install ffmpeg
```

### Permission errors on Termux
```bash
# Acquire wake-lock to prevent CPU sleep
termux-wake-lock

# Grant storage permission
termux-setup-storage
```

### Duplicate models warning
```bash
# Clean up duplicates
python3 ctbcap_multi.py dedup
```

### Model shows as "already recording" but isn't
```bash
# Stop the stuck recording via health server
curl -X POST http://localhost:8080/control/model_name/stop

# Or stop everything and restart
python3 ctbcap_multi.py stop
python3 ctbcap_multi.py start
```
