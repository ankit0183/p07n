# CtbCap Multi-Model Recorder

A Python-based concurrent stream recorder for **Chaturbate** and **StripChat** supporting multiple models simultaneously. Runs on **macOS**, **Linux**, and **Termux (Android)**.

## Features

- Record multiple models concurrently with FFmpeg
- Automatic online/offline detection and recording start/stop
- Stalled recording detection and auto-restart
- FFmpeg crash recovery with automatic restart
- Grace period before stopping on temporary disconnect
- Health check HTTP server with status, metrics, and control endpoints
- Telegram / Discord / ntfy notifications
- Structured metadata logging (JSONL)
- Log rotation with configurable size and backups
- Hot-reload config without restarting (SIGHUP)
- Daemon mode with Termux wake-lock support
- Termux notifications and vibrate on events
- Battery-aware mode for Termux (reduces check frequency when low)
- Recording stats summary on shutdown

---

## Requirements

```bash
# Python 3.9+
pip install aiohttp pyyaml

# FFmpeg (required)
# macOS (Homebrew)
brew install ffmpeg

# Linux (apt)
sudo apt install ffmpeg

# Termux
pkg install ffmpeg
```

---

## Quick Start

```bash
# 1. Copy example config
cp config.example.yaml config.yaml

# 2. Edit config.yaml with your models
nano config.yaml

# 3. Validate config
python3 ctbcap_multi.py --validate

# 4. List all configured models
python3 ctbcap_multi.py --list

# 5. Run
python3 ctbcap_multi.py
```

---

## CLI Commands

| Command | Description |
|---|---|
| `python3 ctbcap_multi.py` | Run the recorder (auto-daemonizes on Termux) |
| `python3 ctbcap_multi.py -D` | Run as daemon (background, acquires Termux wake-lock) |
| `python3 ctbcap_multi.py -F` | Force foreground mode (even on Termux) |
| `python3 ctbcap_multi.py -S` | Stop the running daemon |
| `python3 ctbcap_multi.py -c /path/to/config.yaml` | Use a custom config file |
| `python3 ctbcap_multi.py --validate` | Validate config and check environment (FFmpeg, platform, models) |
| `python3 ctbcap_multi.py --list` | List all configured models with platform, status, and check interval |
| `python3 ctbcap_multi.py --status` | Check if a daemon is running |
| `python3 ctbcap_multi.py --models Ada-19 Meav_` | Run only specific models (filter) |
| `python3 ctbcap_multi.py -v` | Show version |

### Signal Controls (Unix/Linux/Mac/Termux)

| Signal | Action |
|---|---|
| `SIGTERM` / `SIGINT` | Graceful shutdown (stops all recordings) |
| `SIGHUP` | Hot-reload config (add new models without restart) |
| `SIGUSR1` | Stop health check server |
| `SIGUSR2` | Start health check server |

```bash
# Reload config without restarting
kill -HUP $(cat ctbcap.pid)

# Stop daemon
kill -TERM $(cat ctbcap.pid)
```

---

## Configuration

### Global Settings (`config.yaml`)

```yaml
global:
  save_path: "./ctbcap_rec"              # Base save directory
  log_path: "./ctbcap_rec/log"           # Log directory
  save_space_mib: 512                    # Min free space to start recording
  log_space_mib: 16
  cut_time: 0                            # Segment duration (0 = continuous)
  cut_size_mib: 0                        # Segment by size (0 = disabled)
  platform: "stripchat"                  # Default platform (chaturbate/stripchat)
  user_agent: "Mozilla/5.0 ..."
  edging_mode: false                     # Random delay before first check
  debug_mode: false                      # Verbose FFmpeg and API logging
  ignore_proxy: false
  check_interval: 60                     # Seconds between online checks
  retry_interval: 60                     # Seconds between retries on failure
  ffmpeg_codec: "copy"                   # FFmpeg codec (copy = no re-encode)
  ffmpeg_extra_args: ""                  # Extra FFmpeg arguments

  # Recording reliability
  stall_timeout_seconds: 120             # Restart FFmpeg if file stops growing
  offline_checks_required: 2             # Consecutive failures before offline

  # Log rotation
  max_log_size_mib: 10                   # Max log file size before rotation
  log_backups: 3                         # Number of backup log files

  # Termux (Android)
  termux_notifications: false            # System notifications on events
  termux_vibrate: false                  # Vibrate on events
  battery_aware: false                   # Reduce checks when battery < 20%
```

### Notifications

```yaml
global:
  notifications:
    enabled: false
    telegram:
      enabled: false
      bot_token: "YOUR_BOT_TOKEN"
      chat_id: "YOUR_CHAT_ID"
    discord:
      enabled: false
      webhook_url: "YOUR_WEBHOOK_URL"
    ntfy:
      enabled: false
      topic: "your-topic"
      server: "https://ntfy.sh"
```

### Download Queue

```yaml
global:
  download_queue:
    max_concurrent_downloads: 3           # Max simultaneous FFmpeg processes
    max_concurrent_fetches: 5             # Max simultaneous API calls
    offline_grace_seconds: 30             # Wait before stopping after disconnect
    fetch_retry_attempts: 3               # API retry attempts
    fetch_retry_delay: 5.0                # Seconds between retries
    min_free_space_mib: 1024              # Min free disk space
```

### Health Check Server

```yaml
global:
  health_check:
    enabled: true
    port: 8080
```

### Per-Model Config

```yaml
models:
  - name: "model_name"
    platform: "stripchat"                 # chaturbate or stripchat
    save_path: "./ctbcap_rec/model_name"  # Override save path
    log_path: "./ctbcap_rec/log/model_name"
    enabled: true
    check_interval: 300                   # Override global check interval
    retry_interval: 180
    cut_time: 0                           # Override global cut_time
    cut_size_mib: 0
    ffmpeg_codec: "copy"                  # Override global codec
    ffmpeg_extra_args: ""                 # Override global extra args
```

---

## Health Check API

All endpoints run on `http://localhost:8080` (configurable in `config.yaml` under `health_check.port`).

### Endpoints Reference

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Simple health check |
| `/status` | GET | Full status: all monitors + active recordings |
| `/metrics` | GET | Prometheus-compatible metrics |
| `/control/{model}/stop` | POST | Stop recording for one model |
| `/control/stop-all` | POST | Stop all recordings |
| `/control/reload-config` | POST | Hot-reload config.yaml |
| `/control/add-model` | POST | Add a new model and hot-reload |

---

### Health Check

Check if the service is running and healthy.

```bash
# Basic health check
curl http://localhost:8080/health

# Pretty print
curl -s http://localhost:8080/health | python3 -m json.tool
```

Response:
```json
{
    "status": "healthy",
    "timestamp": "2026-07-15T12:00:00+00:00"
}
```

---

### Full Status

Get the status of all monitors and active recordings.

```bash
# Get full status
curl http://localhost:8080/status

# Pretty print
curl -s http://localhost:8080/status | python3 -m json.tool

# Check only active recordings count
curl -s http://localhost:8080/status | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Active recordings: {d[\"active_count\"]}')"

# Check only a specific model
curl -s http://localhost:8080/status | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['monitors'].get('Ada-19', 'Not found'))"

# List all online models
curl -s http://localhost:8080/status | python3 -c "
import sys, json
d = json.load(sys.stdin)
for name, m in d['monitors'].items():
    status = m['last_status'] or 'unknown'
    print(f'{name}: {status}')
"

# List only currently recording models
curl -s http://localhost:8080/status | python3 -c "
import sys, json
d = json.load(sys.stdin)
for name, s in d['recordings'].items():
    dur = int(s['duration'])
    print(f'{name}: {dur//3600}h {(dur%3600)//60}m {dur%60}s (PID {s[\"pid\"]})')
"

# Count total monitors vs active recordings
curl -s http://localhost:8080/status | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'Total monitors: {d[\"total_monitors\"]}')
print(f'Active recordings: {d[\"active_count\"]}')
"
```

Response:
```json
{
    "recordings": {
        "Ada-19": {
            "model": "Ada-19",
            "platform": "stripchat",
            "recording": true,
            "duration": 3621.5,
            "pid": 12345,
            "output_file": "./ctbcap_rec/Ada-19/Ada-19-20260715-120000.mp4"
        }
    },
    "monitors": {
        "Ada-19": {
            "enabled": true,
            "platform": "stripchat",
            "last_status": "online",
            "running": true,
            "consecutive_offline": 0
        },
        "Meav_": {
            "enabled": true,
            "platform": "stripchat",
            "last_status": "offline",
            "running": true,
            "consecutive_offline": 3
        }
    },
    "active_count": 1,
    "total_monitors": 115,
    "timestamp": "2026-07-15T12:00:00+00:00"
}
```

---

### Metrics (Prometheus)

Get Prometheus-compatible metrics for monitoring.

```bash
# Get all metrics
curl http://localhost:8080/metrics

# Get only active recording count
curl -s http://localhost:8080/metrics | grep ctbcap_recordings_active

# Get only running monitors count
curl -s http://localhost:8080/metrics | grep ctbcap_monitors_running

# Get per-model recording durations
curl -s http://localhost:8080/metrics | grep ctbcap_recording_duration

# Get total stats
curl -s http://localhost:8080/metrics | grep ctbcap_stats
```

Response:
```
ctbcap_recordings_active 1
ctbcap_monitors_total 115
ctbcap_monitors_running 115
ctbcap_stats_total_recordings 42
ctbcap_stats_total_duration 152340
ctbcap_stats_total_bytes 5368709120
ctbcap_recording_duration_seconds{model="Ada-19"} 3621
```

---

### Stop a Single Model

Stop the recording for a specific model by name.

```bash
# Stop recording for Ada-19
curl -X POST http://localhost:8080/control/Ada-19/stop

# Pretty print the response
curl -s -X POST http://localhost:8080/control/Ada-19/stop | python3 -m json.tool

# Stop and check it stopped
curl -s -X POST http://localhost:8080/control/Ada-19/stop | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])"
```

Response:
```json
{
    "status": "stopped",
    "model": "Ada-19"
}
```

---

### Stop All Recordings

Stop every active recording at once.

```bash
# Stop all recordings
curl -X POST http://localhost:8080/control/stop-all

# Pretty print
curl -s -X POST http://localhost:8080/control/stop-all | python3 -m json.tool

# Confirm all stopped
curl -s http://localhost:8080/status | python3 -c "import sys,json; print(f'Active: {json.load(sys.stdin)[\"active_count\"]}')"
```

Response:
```json
{
    "status": "all stopped"
}
```

---

### Hot-Reload Config

Reload `config.yaml` without restarting. Adds new models, removes disabled ones, updates intervals.

```bash
# Reload config
curl -X POST http://localhost:8080/control/reload-config

# Pretty print
curl -s -X POST http://localhost:8080/control/reload-config | python3 -m json.tool

# Reload and verify
curl -s -X POST http://localhost:8080/control/reload-config | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])"
```

Response:
```json
{
    "status": "reloaded"
}
```

---

### Add Model

Add a new model to `config.yaml` and hot-reload the daemon instantly.

```bash
# Add a StripChat model
curl -X POST http://localhost:8080/control/add-model \
  -H "Content-Type: application/json" \
  -d '{"name": "ModelName", "platform": "stripchat"}'

# Add a Chaturbate model
curl -X POST http://localhost:8080/control/add-model \
  -H "Content-Type: application/json" \
  -d '{"name": "ModelName", "platform": "chaturbate"}'

# Pretty print
curl -s -X POST http://localhost:8080/control/add-model \
  -H "Content-Type: application/json" \
  -d '{"name": "ModelName", "platform": "stripchat"}' | python3 -m json.tool

# Add and verify it's running
curl -s -X POST http://localhost:8080/control/add-model \
  -H "Content-Type: application/json" \
  -d '{"name": "ModelName", "platform": "stripchat"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])"
```

Request body (JSON):
```json
{
    "name": "ModelName",
    "platform": "stripchat"
}
```

Response (success):
```json
{
    "status": "added",
    "name": "ModelName",
    "platform": "stripchat"
}
```

Response (duplicate):
```json
{
    "status": "error",
    "message": "Model 'ModelName' already exists"
}
```

---

### Common One-Liners

```bash
# Check if service is alive
curl -sf http://localhost:8080/health > /dev/null && echo "OK" || echo "DOWN"

# Get list of all online models
curl -s http://localhost:8080/status | python3 -c "
import sys, json
online = [n for n, m in json.load(sys.stdin)['monitors'].items() if m['last_status'] == 'online']
print(f'{len(online)} online: {online}')
"

# Get list of all recording models
curl -s http://localhost:8080/status | python3 -c "
import sys, json
rec = list(json.load(sys.stdin)['recordings'].keys())
print(f'{len(rec)} recording: {rec}')
"

# Watch status every 5 seconds
watch -n 5 'curl -s http://localhost:8080/status | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"Active: {d[\"active_count\"]}/{d[\"total_monitors\"]}\")"'

# Get total data recorded
curl -s http://localhost:8080/metrics | grep ctbcap_stats_total_bytes | awk '{printf "%.1f MiB\n", $2/1048576}'

# Stop a model from a script
MODEL="Ada-19"
curl -sf -X POST "http://localhost:8080/control/${MODEL}/stop" && echo "Stopped ${MODEL}" || echo "Failed to stop ${MODEL}"
```

---

### Using with jq

```bash
# Install jq (if not installed)
# macOS: brew install jq
# Linux: sudo apt install jq
# Termux: pkg install jq

# Health check
curl -s http://localhost:8080/health | jq .

# List online models
curl -s http://localhost:8080/status | jq '.monitors | to_entries[] | select(.value.last_status == "online") | .key'

# List recording models with duration
curl -s http://localhost:8080/status | jq '.recordings | to_entries[] | "\(.key): \(.value.duration | int)s"'

# Get active count
curl -s http://localhost:8080/status | jq '.active_count'

# Get metrics as table
curl -s http://localhost:8080/metrics | awk -F' ' '{printf "%-45s %s\n", $1, $2}'
```

---

## Running as Daemon

### macOS / Linux

```bash
# Start
nohup python3 ctbcap_multi.py -D > /dev/null 2>&1 &

# Or with systemd (see below)
python3 ctbcap_multi.py -D

# Check status
python3 ctbcap_multi.py --status

# Stop
python3 ctbcap_multi.py -S
```

### Termux (Android)

```bash
# Install dependencies
pkg install python ffmpeg
pip install aiohttp pyyaml

# Start (auto-daemonizes on Termux for reliability)
python3 ctbcap_multi.py

# Or explicitly start as daemon
python3 ctbcap_multi.py -D

# Force foreground mode (not recommended - may be killed on screen-off)
python3 ctbcap_multi.py -F

# Stop daemon
python3 ctbcap_multi.py -S

# Check if daemon is running
python3 ctbcap_multi.py --status
```

> **Important:** On Termux, the process auto-daemonizes by default because Termux kills foreground processes when the screen turns off or you switch apps. Use `-D` for reliable recording. Use `-F` only for debugging.

### systemd Service (Linux)

```ini
# /etc/systemd/system/ctbcap.service
[Unit]
Description=CtbCap Multi-Model Recorder
After=network.target

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/p07n
ExecStart=/usr/bin/python3 ctbcap_multi.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable ctbcap
sudo systemctl start ctbcap
sudo systemctl status ctbcap

# Reload config
sudo kill -HUP $(cat /path/to/p07n/ctbcap.pid)
```

### launchd (macOS)

```xml
<!-- ~/Library/LaunchAgents/com.ctbcap.plist -->
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.ctbcap</string>
    <key>ProgramArguments</key>
    <array>
        <string>/opt/homebrew/bin/python3</string>
        <string>/path/to/p07n/ctbcap_multi.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/path/to/p07n</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/path/to/p07n/nohup.out</string>
    <key>StandardErrorPath</key>
    <string>/path/to/p07n/nohup.out</string>
</dict>
</plist>
```

```bash
launchctl load ~/Library/LaunchAgents/com.ctbcap.plist
launchctl start com.ctbcap
launchctl stop com.ctbcap
```

---

## Platform Support

| Platform | FFmpeg Path | Notes |
|---|---|---|
| macOS (Apple Silicon) | `/opt/homebrew/bin/ffmpeg` | Homebrew |
| macOS (Intel) | `/usr/local/bin/ffmpeg` | Homebrew |
| Linux | `/usr/bin/ffmpeg` | apt/yum |
| Termux (Android) | `/data/data/com.termux/files/usr/bin/ffmpeg` | pkg install |

---

## Troubleshooting

```bash
# Validate config and environment
python3 ctbcap_multi.py --validate

# List all models
python3 ctbcap_multi.py --list

# Run with debug output
# Set debug_mode: true in config.yaml, then:
python3 ctbcap_multi.py

# Check logs
tail -f ctbcap_rec/log/ctbcap.log

# Check metadata events
tail -f ctbcap_rec/log/metadata.jsonl

# Check health endpoint
curl http://localhost:8080/status

# Check FFmpeg is installed
which ffmpeg
ffmpeg -version
```

---

## File Structure

```
p07n/
  ctbcap_multi.py          # Main recorder script
  config.yaml              # Your configuration
  config.example.yaml      # Example configuration
  ctbcap.pid               # PID file (when running as daemon)
  ctbcap_rec/              # Recording output
    log/
      ctbcap.log           # Application log (with rotation)
      ctbcap.log.1         # Rotated backup
      metadata.jsonl       # Structured event log
    model_name/            # Per-model recording directory
      model_name-20260715-120000.mp4
```
