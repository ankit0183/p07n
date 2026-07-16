#!/usr/bin/env python3
"""
CtbCap Multi-Model Recorder v2.0
A Python-based recorder for Chaturbate and StripChat.
Records ALL available online models concurrently with auto-discovery,
tmux integration, auto-restart, and full CLI management.

Usage:
  ./ctbcap_multi.py start                      Start recording all models
  ./ctbcap_multi.py add <name> --platform ...   Add a model to config.yaml
  ./ctbcap_multi.py remove <name>               Remove a model from config.yaml
  ./ctbcap_multi.py list                        List all configured models
  ./ctbcap_multi.py status                      Show recording status
  ./ctbcap_multi.py discover                    Discover ALL online models on platform
  ./ctbcap_multi.py tmux                        Start in tmux with per-model panes
  ./ctbcap_multi.py tmux-dashboard              Start tmux dashboard view
  ./ctbcap_multi.py dedup                       Remove duplicate models from config
  ./ctbcap_multi.py stop                        Stop running daemon
"""

import argparse
import asyncio
import json
import logging
import os
import platform
import random
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid
import atexit
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Set
from urllib.parse import urlparse

import aiohttp
import yaml

try:
    from aiohttp import web
    HAS_AIOHTTP_WEB = True
except ImportError:
    HAS_AIOHTTP_WEB = False

VERSION = "2.0.0"
PID_FILE = "ctbcap.pid"

# ========================
# Configuration
# ========================

@dataclass
class NotificationConfig:
    enabled: bool = False
    telegram: Dict[str, Any] = field(default_factory=lambda: {"enabled": False, "bot_token": "", "chat_id": ""})
    discord: Dict[str, Any] = field(default_factory=lambda: {"enabled": False, "webhook_url": ""})
    ntfy: Dict[str, Any] = field(default_factory=lambda: {"enabled": False, "topic": "", "server": "https://ntfy.sh"})

@dataclass
class MetadataConfig:
    enabled: bool = True
    log_path: str = "/log/metadata.jsonl"

@dataclass
class HealthCheckConfig:
    enabled: bool = True
    port: int = 8080

@dataclass
class DownloadQueueConfig:
    max_concurrent_downloads: int = 3
    max_concurrent_fetches: int = 5
    offline_grace_seconds: int = 30
    fetch_retry_attempts: int = 3
    fetch_retry_delay: float = 5.0
    min_free_space_mib: int = 1024

@dataclass
class GlobalConfig:
    save_path: str = "./ctbcap_rec"
    log_path: str = "./ctbcap_rec/log"
    save_space_mib: int = 512
    log_space_mib: int = 16
    cut_time: int = 0
    cut_size_mib: int = 0
    platform: str = "stripchat"
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    edging_mode: bool = False
    debug_mode: bool = False
    ignore_proxy: bool = False
    check_interval: int = 60
    retry_interval: int = 60
    ffmpeg_codec: str = "copy"
    ffmpeg_extra_args: str = ""
    notifications: NotificationConfig = field(default_factory=NotificationConfig)
    metadata: MetadataConfig = field(default_factory=MetadataConfig)
    health_check: HealthCheckConfig = field(default_factory=HealthCheckConfig)
    download_queue: DownloadQueueConfig = field(default_factory=DownloadQueueConfig)
    auto_restart: bool = True
    max_restart_attempts: int = 10
    restart_backoff_base: float = 5.0
    restart_backoff_max: float = 300.0

@dataclass
class ModelConfig:
    name: str
    platform: Optional[str] = None
    url: Optional[str] = None
    save_path: Optional[str] = None
    log_path: Optional[str] = None
    cut_time: Optional[int] = None
    cut_size_mib: Optional[int] = None
    check_interval: Optional[int] = None
    retry_interval: Optional[int] = None
    enabled: bool = True
    ffmpeg_codec: Optional[str] = None
    ffmpeg_extra_args: Optional[str] = None
    notifications: Optional[NotificationConfig] = None
    auto_restart: Optional[bool] = None
    max_restart_attempts: Optional[int] = None

    def __post_init__(self):
        if self.platform is None:
            self.platform = "chaturbate"
        if self.url and not self.name:
            parsed = urlparse(self.url)
            self.name = parsed.path.strip('/').split('/')[0].lower()

@dataclass
class Config:
    global_: GlobalConfig = field(default_factory=GlobalConfig)
    models: List[ModelConfig] = field(default_factory=list)

    @classmethod
    def from_yaml(cls, path: str) -> "Config":
        with open(path, 'r') as f:
            data = yaml.safe_load(f) or {}
        global_data = data.get('global', {})
        global_config = GlobalConfig(
            save_path=global_data.get('save_path', './ctbcap_rec'),
            log_path=global_data.get('log_path', './ctbcap_rec/log'),
            save_space_mib=global_data.get('save_space_mib', 512),
            log_space_mib=global_data.get('log_space_mib', 16),
            cut_time=global_data.get('cut_time', 0),
            cut_size_mib=global_data.get('cut_size_mib', 0),
            platform=global_data.get('platform', 'stripchat'),
            user_agent=global_data.get('user_agent', "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"),
            edging_mode=global_data.get('edging_mode', False),
            debug_mode=global_data.get('debug_mode', False),
            ignore_proxy=global_data.get('ignore_proxy', False),
            check_interval=global_data.get('check_interval', 60),
            retry_interval=global_data.get('retry_interval', 60),
            ffmpeg_codec=global_data.get('ffmpeg_codec', 'copy'),
            ffmpeg_extra_args=global_data.get('ffmpeg_extra_args', ''),
            notifications=NotificationConfig(**global_data.get('notifications', {})),
            metadata=MetadataConfig(**global_data.get('metadata', {})),
            health_check=HealthCheckConfig(**global_data.get('health_check', {})),
            download_queue=DownloadQueueConfig(**global_data.get('download_queue', {})),
            auto_restart=global_data.get('auto_restart', True),
            max_restart_attempts=global_data.get('max_restart_attempts', 10),
            restart_backoff_base=global_data.get('restart_backoff_base', 5.0),
            restart_backoff_max=global_data.get('restart_backoff_max', 300.0),
        )
        models = []
        for m in data.get('models', []):
            notif_data = m.get('notifications')
            notifications = NotificationConfig(**notif_data) if notif_data else None
            models.append(ModelConfig(
                name=m.get('name', ''),
                platform=m.get('platform'),
                url=m.get('url'),
                save_path=m.get('save_path'),
                log_path=m.get('log_path'),
                cut_time=m.get('cut_time'),
                cut_size_mib=m.get('cut_size_mib'),
                check_interval=m.get('check_interval'),
                retry_interval=m.get('retry_interval'),
                enabled=m.get('enabled', True),
                ffmpeg_codec=m.get('ffmpeg_codec'),
                ffmpeg_extra_args=m.get('ffmpeg_extra_args'),
                notifications=notifications,
                auto_restart=m.get('auto_restart'),
                max_restart_attempts=m.get('max_restart_attempts'),
            ))
        return cls(global_=global_config, models=models)

# ========================
# YAML Config Manipulation (CLI helpers)
# ========================

class ConfigManager:
    """Direct YAML manipulation for adding/removing models via CLI."""

    def __init__(self, config_path: str):
        self.config_path = config_path
        self._data = self._load()

    def _load(self) -> dict:
        if not os.path.exists(self.config_path):
            return {"global": {}, "models": []}
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f) or {"global": {}, "models": []}

    def _save(self):
        with open(self.config_path, 'w') as f:
            yaml.dump(self._data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    def _ensure_global(self):
        if 'global' not in self._data:
            self._data['global'] = {}
        if 'models' not in self._data:
            self._data['models'] = []

    def add_model(self, name: str, platform: str = None, check_interval: int = None,
                  retry_interval: int = None, cut_time: int = None, enabled: bool = True,
                  save_path: str = None, log_path: str = None) -> bool:
        self._ensure_global()
        for m in self._data['models']:
            if m.get('name') == name:
                return False
        if platform is None:
            platform = self._data['global'].get('platform', 'stripchat')
        if check_interval is None:
            check_interval = self._data['global'].get('check_interval', 300)
        if retry_interval is None:
            retry_interval = self._data['global'].get('retry_interval', 180)
        if cut_time is None:
            cut_time = self._data['global'].get('cut_time', 0)
        save_path_base = self._data['global'].get('save_path', './ctbcap_rec')
        log_path_base = self._data['global'].get('log_path', './ctbcap_rec/log')
        model_entry = {
            'name': name,
            'platform': platform,
            'save_path': save_path or f"{save_path_base}/{name}",
            'log_path': log_path or f"{log_path_base}/{name}",
            'enabled': enabled,
            'check_interval': check_interval,
            'retry_interval': retry_interval,
            'cut_time': cut_time,
        }
        self._data['models'].append(model_entry)
        self._save()
        return True

    def add_models_bulk(self, models_list: List[Dict[str, Any]]) -> int:
        """Add multiple models at once. Returns count of newly added models."""
        self._ensure_global()
        existing = {m.get('name') for m in self._data['models']}
        added = 0
        for m in models_list:
            name = m.get('name', '')
            if not name or name in existing:
                continue
            platform = m.get('platform', self._data['global'].get('platform', 'stripchat'))
            check_interval = m.get('check_interval', self._data['global'].get('check_interval', 300))
            retry_interval = m.get('retry_interval', self._data['global'].get('retry_interval', 180))
            cut_time = m.get('cut_time', self._data['global'].get('cut_time', 0))
            save_path_base = self._data['global'].get('save_path', './ctbcap_rec')
            log_path_base = self._data['global'].get('log_path', './ctbcap_rec/log')
            entry = {
                'name': name,
                'platform': platform,
                'save_path': m.get('save_path', f"{save_path_base}/{name}"),
                'log_path': m.get('log_path', f"{log_path_base}/{name}"),
                'enabled': m.get('enabled', True),
                'check_interval': check_interval,
                'retry_interval': retry_interval,
                'cut_time': cut_time,
            }
            self._data['models'].append(entry)
            existing.add(name)
            added += 1
        if added > 0:
            self._save()
        return added

    def remove_model(self, name: str) -> bool:
        self._ensure_global()
        before = len(self._data['models'])
        self._data['models'] = [m for m in self._data['models'] if m.get('name') != name]
        if len(self._data['models']) < before:
            self._save()
            return True
        return False

    def list_models(self) -> List[dict]:
        self._ensure_global()
        return self._data['models']

    def dedup(self) -> int:
        """Remove duplicate model entries. Returns count of removed duplicates."""
        self._ensure_global()
        seen = set()
        deduped = []
        removed = 0
        for m in self._data['models']:
            name = m.get('name', '')
            if name in seen:
                removed += 1
                continue
            seen.add(name)
            deduped.append(m)
        if removed > 0:
            self._data['models'] = deduped
            self._save()
        return removed

    def disable_model(self, name: str) -> bool:
        self._ensure_global()
        for m in self._data['models']:
            if m.get('name') == name:
                m['enabled'] = False
                self._save()
                return True
        return False

    def enable_model(self, name: str) -> bool:
        self._ensure_global()
        for m in self._data['models']:
            if m.get('name') == name:
                m['enabled'] = True
                self._save()
                return True
        return False

# ========================
# Logging Setup
# ========================

def setup_logging(log_path: str, debug: bool = False, model_name: str = "ctbcap"):
    Path(log_path).mkdir(parents=True, exist_ok=True)
    log_file = Path(log_path) / f"{model_name}.log"
    level = logging.DEBUG if debug else logging.INFO
    handlers = [
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
    logging.basicConfig(
        level=level,
        format='[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
        datefmt='%Y%m%d-%H%M%S',
        handlers=handlers
    )
    return logging.getLogger(model_name)

# ========================
# Metadata Logging
# ========================

class MetadataLogger:
    def __init__(self, log_path: str):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def log_event(self, model: str, event: str, data: Dict[str, Any] = None):
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "model": model,
            "event": event,
            "data": data or {}
        }
        try:
            with open(self.log_path, 'a') as f:
                f.write(json.dumps(record) + '\n')
        except Exception as e:
            logging.getLogger("metadata").error(f"Failed to write metadata: {e}")

# ========================
# Notifications
# ========================

class NotificationManager:
    def __init__(self, config: NotificationConfig, session: aiohttp.ClientSession):
        self.config = config
        self.session = session
        self.logger = logging.getLogger("notifications")

    async def send(self, model: str, event: str, message: str, model_config: Optional[NotificationConfig] = None):
        if not self.config.enabled and not (model_config and model_config.enabled):
            return
        cfg = model_config or self.config
        full_message = f"[{model}] {event}: {message}"
        if cfg.telegram.get('enabled'):
            await self._send_telegram(cfg.telegram, full_message)
        if cfg.discord.get('enabled'):
            await self._send_discord(cfg.discord, full_message)
        if cfg.ntfy.get('enabled'):
            await self._send_ntfy(cfg.ntfy, full_message, model)

    async def _send_telegram(self, cfg: Dict, message: str):
        try:
            url = f"https://api.telegram.org/bot{cfg['bot_token']}/sendMessage"
            await self.session.post(url, json={"chat_id": cfg['chat_id'], "text": message})
        except Exception as e:
            self.logger.error(f"Telegram notification failed: {e}")

    async def _send_discord(self, cfg: Dict, message: str):
        try:
            await self.session.post(cfg['webhook_url'], json={"content": message})
        except Exception as e:
            self.logger.error(f"Discord notification failed: {e}")

    async def _send_ntfy(self, cfg: Dict, message: str, model: str):
        try:
            url = f"{cfg['server'].rstrip('/')}/{cfg['topic']}"
            headers = {"Title": f"CtbCap: {model}", "Tags": "camera,recording"}
            await self.session.post(url, data=message, headers=headers)
        except Exception as e:
            self.logger.error(f"ntfy notification failed: {e}")

# ========================
# Platform API Clients
# ========================

class PlatformClient:
    def __init__(self, session: aiohttp.ClientSession, user_agent: str, debug: bool = False):
        self.session = session
        self.user_agent = user_agent
        self.debug = debug
        self.logger = logging.getLogger("platform")

    async def fetch_stream_url(self, model: str, platform: str, max_retries: int = 3, retry_delay: float = 5.0) -> Optional[str]:
        for attempt in range(max_retries):
            try:
                if platform == "chaturbate":
                    result = await self._fetch_chaturbate(model)
                elif platform == "stripchat":
                    result = await self._fetch_stripchat(model)
                else:
                    return None
                if result:
                    return result
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {model} on {platform}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
        return None

    async def _fetch_chaturbate(self, model: str) -> Optional[str]:
        api_url = f"https://chaturbate.com/api/chatvideocontext/{model}/"
        headers = {"User-Agent": self.user_agent}
        try:
            async with self.session.get(api_url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if self.debug:
                    self.logger.debug(f"Chaturbate API status: {resp.status}")
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('hls_source'):
                        return data['hls_source']
                    room_status = data.get('room_status', 'unknown')
                    self.logger.info(f"[{model}] Room status: {room_status}")
                elif resp.status == 404:
                    async with self.session.get(f"https://chaturbate.com/{model}/", headers=headers) as r:
                        text = await r.text()
                        if 'location:' in text.lower():
                            pass
        except Exception as e:
            self.logger.error(f"Chaturbate fetch error for {model}: {e}")
        return None

    async def _fetch_stripchat(self, model: str) -> Optional[str]:
        api_url = f"https://stripchat.com/api/front/v2/models/username/{model}/cam"
        headers = {"User-Agent": self.user_agent}
        try:
            async with self.session.get(api_url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if self.debug:
                    self.logger.debug(f"Stripchat API status: {resp.status}")
                if resp.status in (200, 302):
                    data = await resp.json()
                    cam = data.get('cam', {})
                    if cam.get('isCamActive') or cam.get('isCamAvailable'):
                        stream_name = cam.get('streamName')
                        if stream_name:
                            return f"https://edge-hls.sacdnssedge.com/hls/{stream_name}/master/{stream_name}_auto.m3u8"
        except Exception as e:
            self.logger.error(f"Stripchat fetch error for {model}: {e}")
        return None

    async def discover_stripchat_online(self, limit: int = 500) -> List[Dict[str, Any]]:
        """Discover ALL online models on StripChat using the public API."""
        online_models = []
        offset = 0
        batch_size = 100
        headers = {"User-Agent": self.user_agent}

        while offset < limit:
            try:
                url = f"https://stripchat.com/api/front/v2/models/offset/{offset}/limit/{batch_size}"
                async with self.session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != 200:
                        self.logger.warning(f"StripChat discovery API returned {resp.status} at offset {offset}")
                        break
                    data = await resp.json()
                    models = data if isinstance(data, list) else data.get('models', data.get('items', []))
                    if not models:
                        break
                    for m in models:
                        username = m.get('username', m.get('name', ''))
                        if not username:
                            continue
                        cam = m if 'cam' not in m else m.get('cam', m)
                        is_active = cam.get('isCamActive', False) or cam.get('isCamAvailable', False)
                        if is_active:
                            online_models.append({
                                'name': username,
                                'platform': 'stripchat',
                                'display_name': m.get('displayName', username),
                                'viewers': m.get('viewers', cam.get('viewers', 0)),
                            })
                    if len(models) < batch_size:
                        break
                    offset += batch_size
                    await asyncio.sleep(0.5)
            except Exception as e:
                self.logger.error(f"StripChat discovery error at offset {offset}: {e}")
                break

        self.logger.info(f"Discovered {len(online_models)} online models on StripChat")
        return online_models

    async def discover_chaturbate_online(self, limit: int = 500) -> List[Dict[str, Any]]:
        """Discover ALL online models on Chaturbate using the public API."""
        online_models = []
        offset = 0
        batch_size = 100
        headers = {"User-Agent": self.user_agent}

        while offset < limit:
            try:
                url = f"https://chaturbate.com/api/chatvideocontext/?offset={offset}&limit={batch_size}"
                async with self.session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != 200:
                        self.logger.warning(f"Chaturbate discovery API returned {resp.status} at offset {offset}")
                        break
                    data = await resp.json()
                    models = data if isinstance(data, list) else data.get('results', [])
                    if not models:
                        break
                    for m in models:
                        username = m.get('username', '')
                        if not username:
                            continue
                        room_status = m.get('room_status', 'unknown')
                        if room_status == 'public':
                            online_models.append({
                                'name': username,
                                'platform': 'chaturbate',
                                'display_name': m.get('display_name', username),
                                'viewers': m.get('num_viewers', 0),
                                'room_status': room_status,
                            })
                    if len(models) < batch_size:
                        break
                    offset += batch_size
                    await asyncio.sleep(0.5)
            except Exception as e:
                self.logger.error(f"Chaturbate discovery error at offset {offset}: {e}")
                break

        self.logger.info(f"Discovered {len(online_models)} online models on Chaturbate")
        return online_models

# ========================
# Download Queue Manager
# ========================

class DownloadQueue:
    def __init__(self, config: DownloadQueueConfig):
        self.config = config
        self.download_semaphore = asyncio.Semaphore(config.max_concurrent_downloads)
        self.fetch_semaphore = asyncio.Semaphore(config.max_concurrent_fetches)
        self.offline_grace_tasks: Dict[str, asyncio.Task] = {}
        self.logger = logging.getLogger("download_queue")

    async def acquire_download_slot(self) -> bool:
        return self.download_semaphore.locked() or await self._try_acquire(self.download_semaphore)

    async def _try_acquire(self, semaphore: asyncio.Semaphore) -> bool:
        try:
            await asyncio.wait_for(semaphore.acquire(), timeout=0.1)
            return True
        except asyncio.TimeoutError:
            return False

    def release_download_slot(self):
        if self.download_semaphore.locked():
            self.download_semaphore.release()

    async def fetch_with_semaphore(self, fetch_func, *args, **kwargs):
        async with self.fetch_semaphore:
            return await fetch_func(*args, **kwargs)

    def schedule_offline_grace(self, model_name: str, callback, grace_seconds: int = None):
        grace = grace_seconds or self.config.offline_grace_seconds
        if model_name in self.offline_grace_tasks:
            self.offline_grace_tasks[model_name].cancel()

        async def grace_period():
            try:
                await asyncio.sleep(grace)
                self.logger.info(f"Grace period ended for {model_name}, executing offline callback")
                await callback()
            except asyncio.CancelledError:
                self.logger.debug(f"Grace period cancelled for {model_name} (model came back online)")
            finally:
                self.offline_grace_tasks.pop(model_name, None)

        self.offline_grace_tasks[model_name] = asyncio.create_task(grace_period())

    def cancel_offline_grace(self, model_name: str) -> bool:
        if model_name in self.offline_grace_tasks:
            self.offline_grace_tasks[model_name].cancel()
            self.offline_grace_tasks.pop(model_name, None)
            return True
        return False

    def is_in_grace_period(self, model_name: str) -> bool:
        return model_name in self.offline_grace_tasks

# ========================
# Disk Space Monitor
# ========================

class DiskSpaceMonitor:
    def __init__(self, min_free_mib: int = 1024):
        self.min_free_mib = min_free_mib
        self.logger = logging.getLogger("disk_space")

    def check_space(self, path: str) -> tuple[bool, int]:
        try:
            stat = shutil.disk_usage(path)
            free_mib = stat.free // (1024 * 1024)
            return free_mib >= self.min_free_mib, free_mib
        except Exception as e:
            self.logger.error(f"Failed to check disk space for {path}: {e}")
            return True, 0

    async def wait_for_space(self, path: str, check_interval: int = 60) -> bool:
        while True:
            has_space, free_mib = self.check_space(path)
            if has_space:
                return True
            self.logger.warning(f"Low disk space on {path}: {free_mib} MiB free, need {self.min_free_mib} MiB. Waiting...")
            await asyncio.sleep(check_interval)

# ========================
# Bandwidth Monitor
# ========================

class BandwidthMonitor:
    """Track recording file sizes to estimate bandwidth usage."""

    def __init__(self):
        self.stats: Dict[str, Dict[str, Any]] = {}
        self.logger = logging.getLogger("bandwidth")

    def register(self, model_name: str, output_file: str):
        self.stats[model_name] = {
            'output_file': output_file,
            'start_time': time.time(),
            'last_check': time.time(),
            'last_size': 0,
            'total_bytes': 0,
            'current_speed': 0.0,
            'peak_speed': 0.0,
        }

    def update(self, model_name: str) -> Optional[Dict[str, Any]]:
        stat = self.stats.get(model_name)
        if not stat:
            return None
        try:
            size = os.path.getsize(stat['output_file'])
            now = time.time()
            elapsed = now - stat['last_check']
            if elapsed > 0:
                speed = (size - stat['last_size']) / elapsed
                stat['current_speed'] = speed
                if speed > stat['peak_speed']:
                    stat['peak_speed'] = speed
            stat['last_size'] = size
            stat['last_check'] = now
            stat['total_bytes'] = size
            return {
                'bytes': size,
                #'speed_bps': stat['current_speed'],
               #'peak_bps': stat['peak_speed'],
                'duration': now - stat['start_time'],
            }
        except OSError:
            return None

    def unregister(self, model_name: str) -> Optional[Dict[str, Any]]:
        stat = self.stats.pop(model_name, None)
        if stat:
            return {
                'total_bytes': stat['total_bytes'],
                'duration': time.time() - stat['start_time'],
                'peak_speed': stat['peak_speed'],
            }
        return None

    def format_speed(self, bps: float) -> str:
        if bps < 1024:
            return f"{bps:.0f} B/s"
        elif bps < 1024 * 1024:
            return f"{bps / 1024:.1f} KB/s"
        else:
            return f"{bps / (1024 * 1024):.2f} MB/s"

    def format_size(self, bytes_val: int) -> str:
        if bytes_val < 1024:
            return f"{bytes_val} B"
        elif bytes_val < 1024 * 1024:
            return f"{bytes_val / 1024:.1f} KB"
        elif bytes_val < 1024 * 1024 * 1024:
            return f"{bytes_val / (1024 * 1024):.1f} MB"
        else:
            return f"{bytes_val / (1024 * 1024 * 1024):.2f} GB"

# ========================
# Recorder
# ========================

@dataclass
class RecordingSession:
    model: str
    platform: str
    stream_url: str
    process: Optional[subprocess.Popen] = None
    start_time: float = field(default_factory=time.time)
    segment: int = 1
    output_file: str = ""
    restart_count: int = 0

class Recorder:
    def __init__(self, global_config: GlobalConfig, metadata_logger: MetadataLogger,
                 notifications: NotificationManager, download_queue: DownloadQueue,
                 disk_monitor: DiskSpaceMonitor, bandwidth_monitor: BandwidthMonitor):
        self.global_config = global_config
        self.metadata_logger = metadata_logger
        self.notifications = notifications
        self.download_queue = download_queue
        self.disk_monitor = disk_monitor
        self.bandwidth_monitor = bandwidth_monitor
        self.logger = logging.getLogger("recorder")
        self.sessions: Dict[str, RecordingSession] = {}
        self.ffmpeg_path = self._find_ffmpeg()

    def _find_ffmpeg(self) -> str:
        termux_paths = [
            '/data/data/com.termux/files/usr/bin/ffmpeg',
            '/data/data/com.termux/files/usr/bin/ffmpeg-static',
        ]
        mac_paths = [
            '/opt/homebrew/bin/ffmpeg',
            '/usr/local/bin/ffmpeg',
            '/opt/homebrew/opt/ffmpeg/bin/ffmpeg',
        ]
        linux_paths = [
            '/usr/bin/ffmpeg',
            '/usr/local/bin/ffmpeg',
            '/snap/bin/ffmpeg',
            '/var/lib/snapd/snap/bin/ffmpeg',
        ]
        found = shutil.which('ffmpeg')
        if found:
            return found
        all_paths = termux_paths + mac_paths + linux_paths
        for path in all_paths:
            if os.path.isfile(path) and os.access(path, os.X_OK):
                self.logger.info(f"Found ffmpeg at: {path}")
                return path
        self.logger.warning("ffmpeg not found in standard locations, using 'ffmpeg' from PATH")
        return 'ffmpeg'

    def _build_ffmpeg_cmd(self, model: ModelConfig, stream_url: str, output_path: str) -> List[str]:
        cut_time = model.cut_time if model.cut_time is not None else self.global_config.cut_time
        cut_size = model.cut_size_mib if model.cut_size_mib is not None else self.global_config.cut_size_mib
        codec = model.ffmpeg_codec or self.global_config.ffmpeg_codec
        extra = model.ffmpeg_extra_args or self.global_config.ffmpeg_extra_args

        self.logger.debug(f"[{model.name}] cut_time={cut_time}, cut_size={cut_size}, codec={codec}")

        referer = "https://chaturbate.com/" if model.platform == "chaturbate" else "https://stripchat.com/"
        origin = referer
        headers = f"Referer: {referer}\r\nOrigin: {origin}\r\n"

        cmd = [
            self.ffmpeg_path,
            '-y', '-loglevel', 'warning' if not self.global_config.debug_mode else 'debug',
            '-nostdin',
            '-reconnect', '1',
            '-reconnect_at_eof', '1',
            '-reconnect_streamed', '1',
            '-reconnect_delay_max', '30',
            '-rw_timeout', '30000000',
            '-timeout', '30000000',
            '-copyts', '-start_at_zero',
            '-copy_unknown',
            '-user_agent', self.global_config.user_agent,
            '-headers', headers,
            '-tls_verify', '0',
            '-fflags', '+genpts+nobuffer+discardcorrupt+igndts',
            '-i', stream_url,
        ]

        if self.global_config.ignore_proxy:
            cmd.extend(['-http_proxy', '0'])

        cmd.extend(['-codec', codec])

        if extra:
            cmd.extend(extra.split())

        if cut_time and cut_time > 0:
            if '%03d' not in output_path:
                output_path = output_path.replace('.mp4', '_%03d.mp4')
            cmd.extend([
                '-f', 'segment',
                '-segment_time', str(cut_time),
                '-segment_start_number', '1',
                '-reset_timestamps', '1',
                '-segment_format_options', 'movflags=+faststart+frag_keyframe+empty_moov',
                '-strftime', '1',
                output_path
            ])
        else:
            cmd.extend([
                '-f', 'mp4',
                '-movflags', 'frag_keyframe+empty_moov+faststart',
                '-avoid_negative_ts', 'make_zero',
                output_path
            ])

        return cmd

    async def start_recording(self, model: ModelConfig, stream_url: str) -> bool:
        save_path = model.save_path or os.path.join(self.global_config.save_path, model.name)
        Path(save_path).mkdir(parents=True, exist_ok=True)

        has_space, free_mib = self.disk_monitor.check_space(save_path)
        if not has_space:
            self.logger.error(f"Insufficient disk space for {model.name}: {free_mib} MiB free, need {self.disk_monitor.min_free_mib} MiB")
            await self.notifications.send(model.name, "DISK_SPACE_LOW",
                f"Insufficient disk space: {free_mib} MiB free, need {self.disk_monitor.min_free_mib} MiB", model.notifications)
            return False

        acquired = await self.download_queue.acquire_download_slot()
        if not acquired:
            self.logger.warning(f"No download slot available for {model.name}, waiting...")
            await asyncio.sleep(1)
            acquired = await self.download_queue.acquire_download_slot()
            if not acquired:
                self.logger.error(f"Could not acquire download slot for {model.name}")
                return False

        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        output_file = os.path.join(save_path, f"{model.name}-{timestamp}.mp4")

        cmd = self._build_ffmpeg_cmd(model, stream_url, output_file)

        self.logger.info(f"Starting recording for {model.name}: {' '.join(cmd)}")

        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True
            )

            session = RecordingSession(
                model=model.name,
                platform=model.platform,
                stream_url=stream_url,
                process=process,
                output_file=output_file
            )
            self.sessions[model.name] = session

            self.bandwidth_monitor.register(model.name, output_file)

            self.metadata_logger.log_event(model.name, "recording_started", {
                "platform": model.platform,
                "stream_url": stream_url,
                "output_file": output_file,
                "pid": process.pid
            })

            await self.notifications.send(model.name, "RECORDING_STARTED",
                f"Started recording {model.name} on {model.platform}", model.notifications)

            asyncio.create_task(self._monitor_process(model, process))

            return True
        except Exception as e:
            self.logger.error(f"Failed to start recording for {model.name}: {e}")
            self.download_queue.release_download_slot()
            return False

    async def _monitor_process(self, model_config: ModelConfig, process: subprocess.Popen):
        model_name = model_config.name
        try:
            stdout, stderr = await asyncio.to_thread(process.communicate)
            return_code = process.returncode

            session = self.sessions.get(model_name)
            if session:
                duration = time.time() - session.start_time
                stderr_text = stderr.decode('utf-8', errors='ignore')[-2000:] if stderr else ""

                bw_stats = self.bandwidth_monitor.unregister(model_name)

                self.metadata_logger.log_event(model_name, "recording_stopped", {
                   
                    "return_code": return_code,
                    "stderr": stderr_text,
                    "total_bytes": bw_stats['total_bytes'] if bw_stats else 0,
                    "peak_speed": bw_stats['peak_speed'] if bw_stats else 0,
                })

                if return_code == 0:
                    self.logger.info(f"[{model_name}] Recording completed normally after {duration:.0f}s")
                elif return_code == -15 or return_code == 143:
                    self.logger.info(f"[{model_name}] Recording stopped by signal")
                elif return_code < 0:
                    self.logger.warning(f"[{model_name}] Recording terminated by signal {-return_code}")
                else:
                    self.logger.warning(f"[{model_name}] Recording exited with code {return_code}: {stderr_text[:500]}")

                await self.notifications.send(model_name, "RECORDING_STOPPED",
                    f"Recording stopped after {duration:.0f}s (code: {return_code})", None)

                del self.sessions[model_name]

            self.download_queue.release_download_slot()

            should_restart = (
                self.global_config.auto_restart
                and model_config.enabled
                and session is not None
                and return_code != 0
                and return_code != -15
                and return_code != 143
            )

            if should_restart:
                max_attempts = model_config.max_restart_attempts or self.global_config.max_restart_attempts
                if session.restart_count < max_attempts:
                    backoff = min(
                        self.global_config.restart_backoff_base * (2 ** session.restart_count),
                        self.global_config.restart_backoff_max
                    )
                    self.logger.info(f"[{model_name}] Auto-restarting in {backoff:.0f}s (attempt {session.restart_count + 1}/{max_attempts})")
                    await asyncio.sleep(backoff)
                    if model_name not in self.sessions:
                        asyncio.create_task(self._restart_recording(model_config, session.restart_count + 1))
                else:
                    self.logger.warning(f"[{model_name}] Max restart attempts ({max_attempts}) reached, not restarting")

        except Exception as e:
            self.logger.error(f"Process monitor error for {model_name}: {e}")
            self.download_queue.release_download_slot()

    async def _restart_recording(self, model_config: ModelConfig, restart_count: int):
        """Attempt to restart a recording by re-fetching the stream URL."""
        model_name = model_config.name
        try:
            stream_url = await self.download_queue.fetch_with_semaphore(
                lambda m, p: asyncio.ensure_future(
                    PlatformClient.__init__ and None or None
                ) if False else self._fetch_stream(model_config)
            )
            if stream_url:
                session = RecordingSession(
                    model=model_name,
                    platform=model_config.platform,
                    stream_url=stream_url,
                    restart_count=restart_count
                )
                self.sessions[model_name] = session
                await self.start_recording(model_config, stream_url)
                # Update restart count on the session
                if model_name in self.sessions:
                    self.sessions[model_name].restart_count = restart_count
        except Exception as e:
            self.logger.error(f"Auto-restart failed for {model_name}: {e}")

    async def _fetch_stream(self, model_config: ModelConfig) -> Optional[str]:
        """Fetch stream URL for restart using a temporary client."""
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        try:
            client = PlatformClient(session, self.global_config.user_agent, self.global_config.debug_mode)
            return await client.fetch_stream_url(
                model_config.name, model_config.platform,
                max_retries=2, retry_delay=3.0
            )
        finally:
            await session.close()

    async def stop_recording(self, model_name: str):
        session = self.sessions.get(model_name)
        if session and session.process:
            self.logger.info(f"Stopping recording for {model_name}")
            session.process.terminate()
            try:
                await asyncio.wait_for(asyncio.to_thread(session.process.wait), timeout=10)
            except asyncio.TimeoutError:
                session.process.kill()
                await asyncio.to_thread(session.process.wait)
            self.bandwidth_monitor.unregister(model_name)
            self.download_queue.release_download_slot()

    async def stop_all(self):
        for model_name in list(self.sessions.keys()):
            await self.stop_recording(model_name)

    def get_recording_stats(self) -> Dict[str, Any]:
        stats = {}
        for name, session in self.sessions.items():
            bw = self.bandwidth_monitor.update(name)
            stats[name] = {
                'platform': session.platform,
                'duration': time.time() - session.start_time,
                'pid': session.process.pid if session.process else None,
                'restart_count': session.restart_count,
                'output_file': session.output_file,
            }
            if bw:
                stats[name]['size_bytes'] = bw['bytes']
                #stats[name]['speed'] = self.bandwidth_monitor.format_speed(bw['speed_bps'])
                #stats[name]['peak_speed'] = self.bandwidth_monitor.format_speed(bw['peak_bps'])
        return stats

# ========================
# Model Monitor
# ========================

class ModelMonitor:
    def __init__(self, model: ModelConfig, global_config: GlobalConfig,
                 platform_client: PlatformClient, recorder: Recorder,
                 metadata_logger: MetadataLogger, notifications: NotificationManager,
                 download_queue: DownloadQueue):
        self.model = model
        self.global_config = global_config
        self.platform_client = platform_client
        self.recorder = recorder
        self.metadata_logger = metadata_logger
        self.notifications = notifications
        self.download_queue = download_queue
        self.logger = logging.getLogger(f"monitor.{model.name}")
        self.running = False
        self.last_status = None
        self.offline_since = None
        self.consecutive_offline = 0

    async def run(self):
        self.running = True

        if self.global_config.edging_mode:
            delay = random.uniform(0, 600)
            self.logger.info(f"Edging mode: waiting {delay:.0f}s before first check")
            await asyncio.sleep(delay)

        while self.running:
            if not self.model.enabled:
                self.logger.info(f"Model {self.model.name} disabled, stopping monitor")
                break

            await self._check_and_record()

            interval = self.model.check_interval or self.global_config.check_interval
            await asyncio.sleep(interval)

    async def _check_and_record(self):
        stream_url = await self.download_queue.fetch_with_semaphore(
            self.platform_client.fetch_stream_url,
            self.model.name, self.model.platform,
            max_retries=self.global_config.download_queue.fetch_retry_attempts,
            retry_delay=self.global_config.download_queue.fetch_retry_delay
        )

        if stream_url:
            was_in_grace = self.download_queue.cancel_offline_grace(self.model.name)
            if was_in_grace:
                self.logger.info(f"[{self.model.name}] Came back online during grace period, continuing recording")

            if self.last_status != "online":
                self.logger.info(f"[{self.model.name}] ONLINE - Stream URL found")
                self.metadata_logger.log_event(self.model.name, "online", {"stream_url": stream_url})
                await self.notifications.send(self.model.name, "ONLINE",
                    f"Model is now online", self.model.notifications)
                self.last_status = "online"
                self.offline_since = None
                self.consecutive_offline = 0

            if self.model.name not in self.recorder.sessions:
                self.logger.info(f"Starting recording for {self.model.name}")
                await self.recorder.start_recording(self.model, stream_url)
            else:
                self.logger.debug(f"Already recording {self.model.name}")
        else:
            if self.last_status != "offline":
                self.logger.info(f"[{self.model.name}] OFFLINE")
                self.metadata_logger.log_event(self.model.name, "offline", {})
                await self.notifications.send(self.model.name, "OFFLINE",
                    f"Model went offline", self.model.notifications)
                self.last_status = "offline"
                self.offline_since = time.time()
                self.consecutive_offline = 1
            else:
                self.consecutive_offline += 1

            if self.model.name in self.recorder.sessions and not self.download_queue.is_in_grace_period(self.model.name):
                self.logger.info(f"Stream ended for {self.model.name}, starting grace period ({self.global_config.download_queue.offline_grace_seconds}s)")
                self.download_queue.schedule_offline_grace(
                    self.model.name,
                    lambda: self._stop_recording_after_grace(self.model.name),
                    self.global_config.download_queue.offline_grace_seconds
                )

    async def _stop_recording_after_grace(self, model_name: str):
        if model_name in self.recorder.sessions:
            self.logger.info(f"Grace period ended for {model_name}, stopping recording")
            await self.recorder.stop_recording(model_name)
        else:
            self.logger.debug(f"Recording already stopped for {model_name} before grace period ended")

    def stop(self):
        self.running = False

# ========================
# Health Check Server
# ========================

class HealthServer:
    def __init__(self, port: int, recorder: Recorder, monitors: List[ModelMonitor], ctbcap_app: 'CtbCap'):
        self.port = port
        self.recorder = recorder
        self.monitors = monitors
        self.ctbcap_app = ctbcap_app
        self.app = None
        self.runner = None

    async def start(self):
        if not HAS_AIOHTTP_WEB:
            logging.getLogger("health").warning("aiohttp.web not available, health check disabled")
            return

        self.app = web.Application()
        self.app.router.add_get('/health', self.health_handler)
        self.app.router.add_get('/status', self.status_handler)
        self.app.router.add_get('/metrics', self.metrics_handler)
        self.app.router.add_get('/recordings', self.recordings_handler)
        self.app.router.add_post('/control/{model}/stop', self.stop_model_handler)
        self.app.router.add_post('/control/stop-all', self.stop_all_handler)
        self.app.router.add_post('/control/reload-config', self.reload_config_handler)
        self.app.router.add_post('/control/discover', self.discover_handler)
        self.app.router.add_post('/control/add', self.add_model_handler)

        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, '0.0.0.0', self.port)
        await site.start()
        logging.getLogger("health").info(f"Health server started on port {self.port}")

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()

    async def health_handler(self, request):
        return web.json_response({"status": "healthy", "version": VERSION, "timestamp": datetime.now(timezone.utc).isoformat()})

    async def status_handler(self, request):
        sessions = {}
        for name, session in self.recorder.sessions.items():
            bw = self.recorder.bandwidth_monitor.update(name)
            entry = {
                "model": session.model,
                "platform": session.platform,
                "recording": True,
                "duration": time.time() - session.start_time,
                "pid": session.process.pid if session.process else None,
                "restart_count": session.restart_count,
            }
            if bw:
                entry['size_bytes'] = bw['bytes']
                #entry['speed'] = self.recorder.bandwidth_monitor.format_speed(bw['speed_bps'])
            sessions[name] = entry

        monitors = {}
        for m in self.monitors:
            monitors[m.model.name] = {
                "enabled": m.model.enabled,
                "platform": m.model.platform,
                "last_status": m.last_status,
                "running": m.running,
                "consecutive_offline": m.consecutive_offline,
            }

        return web.json_response({
            "recordings": sessions,
            "monitors": monitors,
            "total_models": len(self.monitors),
            "active_recordings": len(sessions),
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

    async def recordings_handler(self, request):
        stats = self.recorder.get_recording_stats()
        return web.json_response({"recordings": stats})

    async def stop_model_handler(self, request):
        model_name = request.match_info['model']
        await self.recorder.stop_recording(model_name)
        for m in self.monitors:
            if m.model.name == model_name:
                m.stop()
                break
        return web.json_response({"status": "stopped", "model": model_name})

    async def stop_all_handler(self, request):
        await self.recorder.stop_all()
        for m in self.monitors:
            m.stop()
        return web.json_response({"status": "all stopped"})

    async def reload_config_handler(self, request):
        try:
            await self.ctbcap_app.reload_config()
            return web.json_response({"status": "reloaded"})
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def discover_handler(self, request):
        try:
            platform = request.query.get('platform', self.ctbcap_app.config.global_.platform)
            if platform == 'stripchat':
                online = await self.ctbcap_app.platform_client.discover_stripchat_online()
            elif platform == 'chaturbate':
                online = await self.ctbcap_app.platform_client.discover_chaturbate_online()
            else:
                return web.json_response({"error": f"Unknown platform: {platform}"}, status=400)
            return web.json_response({"platform": platform, "online_count": len(online), "models": online})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def add_model_handler(self, request):
        try:
            data = await request.json()
            name = data.get('name')
            if not name:
                return web.json_response({"error": "name is required"}, status=400)
            platform = data.get('platform', self.ctbcap_app.config.global_.platform)
            if platform not in ('chaturbate', 'stripchat'):
                return web.json_response({"error": f"Unknown platform: {platform}"}, status=400)
            existing_names = {m.model.name for m in self.monitors}
            if name in existing_names:
                return web.json_response({"error": f"Model '{name}' already monitored"}, status=409)
            mgr = ConfigManager(self.ctbcap_app.config_path)
            if not mgr.add_model(name, platform=platform):
                return web.json_response({"error": f"Model '{name}' already in config"}, status=409)
            new_config = Config.from_yaml(self.ctbcap_app.config_path)
            model_cfg = next((m for m in new_config.models if m.name == name), None)
            if not model_cfg:
                return web.json_response({"error": "Failed to load model from config"}, status=500)
            monitor = ModelMonitor(
                model=model_cfg,
                global_config=self.ctbcap_app.config.global_,
                platform_client=self.ctbcap_app.platform_client,
                recorder=self.ctbcap_app.recorder,
                metadata_logger=self.ctbcap_app.metadata_logger,
                notifications=self.ctbcap_app.notifications,
                download_queue=self.ctbcap_app.download_queue,
            )
            self.monitors.append(monitor)
            asyncio.create_task(monitor.run())
            self.ctbcap_app.config = new_config
            return web.json_response({"status": "added", "model": name, "platform": platform, "auto_started": True})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def metrics_handler(self, request):
        lines = [
            f"ctbcap_recordings_active {len(self.recorder.sessions)}",
            f"ctbcap_monitors_total {len(self.monitors)}",
            f"ctbcap_monitors_running {sum(1 for m in self.monitors if m.running)}",
            f"ctbcap_monitors_online {sum(1 for m in self.monitors if m.last_status == 'online')}",
            f"ctbcap_monitors_offline {sum(1 for m in self.monitors if m.last_status == 'offline')}",
        ]
        for name, session in self.recorder.sessions.items():
            elapsed = int(time.time() - session.start_time)
            hours, remainder = divmod(elapsed, 3600)
            minutes, seconds = divmod(remainder, 60)
            duration_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            lines.append(f'ctbcap_recording_duration{{model="{name}"}} "{duration_str}"')
        return web.Response(text='\n'.join(lines) + '\n', content_type='text/plain')

# ========================
# Main Application
# ========================

class CtbCap:
    def __init__(self, config: Config, config_path: str = None):
        self.config = config
        self.config_path = config_path
        self.session: Optional[aiohttp.ClientSession] = None
        self.platform_client: Optional[PlatformClient] = None
        self.metadata_logger: Optional[MetadataLogger] = None
        self.notifications: Optional[NotificationManager] = None
        self.recorder: Optional[Recorder] = None
        self.download_queue: Optional[DownloadQueue] = None
        self.disk_monitor: Optional[DiskSpaceMonitor] = None
        self.bandwidth_monitor: Optional[BandwidthMonitor] = None
        self.monitors: List[ModelMonitor] = []
        self.health_server: Optional[HealthServer] = None
        self.logger = logging.getLogger("ctbcap")
        self.running = False

    async def start(self):
        setup_logging(self.config.global_.log_path, self.config.global_.debug_mode, "ctbcap")

        max_concurrent = max(
            self.config.global_.download_queue.max_concurrent_fetches,
            self.config.global_.download_queue.max_concurrent_downloads
        ) * 2
        connector = aiohttp.TCPConnector(limit=max_concurrent, limit_per_host=30)
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)

        self.platform_client = PlatformClient(
            self.session,
            self.config.global_.user_agent,
            self.config.global_.debug_mode
        )

        self.metadata_logger = MetadataLogger(self.config.global_.metadata.log_path)
        self.notifications = NotificationManager(self.config.global_.notifications, self.session)
        self.download_queue = DownloadQueue(self.config.global_.download_queue)
        self.disk_monitor = DiskSpaceMonitor(self.config.global_.download_queue.min_free_space_mib)
        self.bandwidth_monitor = BandwidthMonitor()

        self.recorder = Recorder(self.config.global_, self.metadata_logger, self.notifications,
                                self.download_queue, self.disk_monitor, self.bandwidth_monitor)

        enabled_count = 0
        for model in self.config.models:
            if not model.enabled:
                self.logger.info(f"Skipping disabled model: {model.name}")
                continue

            monitor = ModelMonitor(
                model=model,
                global_config=self.config.global_,
                platform_client=self.platform_client,
                recorder=self.recorder,
                metadata_logger=self.metadata_logger,
                notifications=self.notifications,
                download_queue=self.download_queue
            )
            self.monitors.append(monitor)
            enabled_count += 1

        self.logger.info(f"Configured {enabled_count} models for monitoring")

        if self.config.global_.health_check.enabled:
            self.health_server = HealthServer(
                self.config.global_.health_check.port,
                self.recorder,
                self.monitors,
                self
            )
            await self.health_server.start()

        self.running = True
        tasks = [monitor.run() for monitor in self.monitors]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def reload_config(self):
        self.logger.info("Reloading config...")
        try:
            new_config = Config.from_yaml(self.config_path)
        except Exception as e:
            self.logger.error(f"Failed to reload config: {e}")
            raise

        existing_names = {m.model.name for m in self.monitors}
        new_models = [m for m in new_config.models if m.name not in existing_names]

        for model in new_models:
            if not model.enabled:
                self.logger.info(f"Skipping disabled model: {model.name}")
                continue

            monitor = ModelMonitor(
                model=model,
                global_config=new_config.global_,
                platform_client=self.platform_client,
                recorder=self.recorder,
                metadata_logger=self.metadata_logger,
                notifications=self.notifications,
                download_queue=self.download_queue
            )
            self.monitors.append(monitor)
            asyncio.create_task(monitor.run())
            self.logger.info(f"Started monitoring new model: {model.name}")

        for monitor in self.monitors:
            for new_model in new_config.models:
                if monitor.model.name == new_model.name:
                    monitor.model.check_interval = new_model.check_interval or new_config.global_.check_interval
                    monitor.model.retry_interval = new_model.retry_interval or new_config.global_.retry_interval
                    monitor.model.enabled = new_model.enabled
                    if not new_model.enabled:
                        monitor.stop()
                        await self.recorder.stop_recording(monitor.model.name)
                    break

        self.config = new_config
        self.logger.info(f"Config reloaded. Total monitors: {len(self.monitors)}")

    async def stop(self):
        self.logger.info("Shutting down...")
        self.running = False

        for monitor in self.monitors:
            monitor.stop()

        await self.recorder.stop_all()

        if self.health_server:
            await self.health_server.stop()

        if self.session:
            await self.session.close()

        self.logger.info("Shutdown complete")

# ========================
# Daemon / Background Helpers
# ========================

def _termux_wake_lock(acquire: bool = True):
    try:
        cmd = 'termux-wake-lock' if acquire else 'termux-wake-unlock'
        subprocess.run([cmd], capture_output=True, timeout=5)
    except FileNotFoundError:
        pass
    except Exception:
        pass

def _write_pid():
    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))

def _remove_pid():
    try:
        os.remove(PID_FILE)
    except FileNotFoundError:
        pass

def _read_pid() -> Optional[int]:
    try:
        with open(PID_FILE, 'r') as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return None

def daemon_start():
    _termux_wake_lock(True)
    _write_pid()
    print(f"Daemon started (PID: {os.getpid()})")

def daemon_stop():
    pid = _read_pid()
    if not pid:
        print("No running daemon found (no PID file)")
        return False
    try:
        os.kill(pid, signal.SIGTERM)
        _termux_wake_lock(False)
        _remove_pid()
        print(f"Daemon (PID: {pid}) stopped")
        return True
    except ProcessLookupError:
        _remove_pid()
        print("Daemon not running (stale PID file removed)")
        return False

# ========================
# Tmux Integration
# ========================

class TmuxManager:
    """Manage tmux sessions for CtbCap multi-model recording."""

    SESSION_NAME = "ctbcap"

    @staticmethod
    def is_tmux_available() -> bool:
        return shutil.which('tmux') is not None

    @staticmethod
    def kill_session():
        subprocess.run(['tmux', 'kill-session', '-t', TmuxManager.SESSION_NAME],
                       capture_output=True, timeout=5)

    @staticmethod
    def start_recording_session(config_path: str, models: List[ModelConfig] = None):
        """Start a tmux session with one pane per model for individual logging."""
        if not TmuxManager.is_tmux_available():
            print("tmux is not installed. Install it with: pkg install tmux (Termux) or apt install tmux")
            return

        TmuxManager.kill_session()

        script_dir = os.path.dirname(os.path.abspath(__file__))
        python_bin = sys.executable

        subprocess.run([
            'tmux', 'new-session', '-d', '-s', TmuxManager.SESSION_NAME,
            '-x', '220', '-y', '50',
            f'{python_bin} {script_dir}/ctbcap_multi.py start -c {config_path}'
        ], timeout=10)

        print(f"tmux session '{TmuxManager.SESSION_NAME}' started")
        print(f"Attach with: tmux attach -t {TmuxManager.SESSION_NAME}")
        print(f"Or run: ./ctbcap_multi.py tmux-attach")

    @staticmethod
    def start_dashboard(config_path: str):
        """Start a tmux session with a status dashboard pane."""
        if not TmuxManager.is_tmux_available():
            print("tmux is not installed.")
            return

        TmuxManager.kill_session()

        subprocess.run([
            'tmux', 'new-session', '-d', '-s', TmuxManager.SESSION_NAME,
            '-x', '120', '-y', '40',
        ], timeout=10)

        config = Config.from_yaml(config_path)
        model_count = len([m for m in config.models if m.enabled])

        subprocess.run([
            'tmux', 'send-keys', '-t', TmuxManager.SESSION_NAME,
            f'watch -n 5 "curl -s http://localhost:{config.global_.health_check.port}/status 2>/dev/null | python3 -m json.tool 2>/dev/null || echo \'Waiting for health server...\'"',
            'C-m'
        ], timeout=5)

        print(f"Dashboard tmux session started")
        print(f"Models configured: {model_count}")
        print(f"Attach with: tmux attach -t {TmuxManager.SESSION_NAME}")

    @staticmethod
    def attach():
        if not TmuxManager.is_tmux_available():
            print("tmux is not installed.")
            return
        subprocess.run(['tmux', 'attach', '-t', TmuxManager.SESSION_NAME])

    @staticmethod
    def is_running() -> bool:
        result = subprocess.run(['tmux', 'has-session', '-t', TmuxManager.SESSION_NAME],
                               capture_output=True, timeout=5)
        return result.returncode == 0

# ========================
# CLI Commands
# ========================

def _try_start_model(config_path: str, name: str, platform: str = None):
    """Try to auto-start a model via the running health server."""
    import urllib.request
    import json as json_mod
    config = Config.from_yaml(config_path)
    port = config.global_.health_check.port
    url = f'http://localhost:{port}/control/add'
    payload = {"name": name}
    if platform:
        payload["platform"] = platform
    try:
        req = urllib.request.Request(
            url,
            data=json_mod.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            result = json_mod.loads(resp.read())
            print(f"  Auto-started recording for '{name}'")
            return True
    except urllib.error.URLError:
        print(f"  Daemon not running - model will be recorded on next 'start'")
        return False
    except Exception as e:
        print(f"  Warning: Could not auto-start '{name}': {e}")
        return False

def cmd_add(args, config_path: str):
    """Add a model to config.yaml, optionally auto-start recording."""
    mgr = ConfigManager(config_path)
    name = args.name
    platform = args.platform
    check_interval = args.check_interval
    retry_interval = args.retry_interval
    cut_time = args.cut_time
    auto_start = getattr(args, 'start', False)

    if mgr.add_model(name, platform=platform, check_interval=check_interval,
                     retry_interval=retry_interval, cut_time=cut_time):
        print(f"Added model '{name}' to {config_path}")
        print(f"  Platform: {platform or '(from global)'}")
        if check_interval:
            print(f"  Check interval: {check_interval}s")
        if retry_interval:
            print(f"  Retry interval: {retry_interval}s")
        if cut_time:
            print(f"  Cut time: {cut_time}s")
        if auto_start:
            _try_start_model(config_path, name, platform)
    else:
        print(f"Model '{name}' already exists in config")
        return 1
    return 0

def cmd_add_bulk(args, config_path: str):
    """Add multiple models at once: ctbcap_multi.py add-bulk name1,name2,name3 --platform stripchat"""
    mgr = ConfigManager(config_path)
    names = [n.strip() for n in args.names.split(',') if n.strip()]
    if not names:
        print("No model names provided")
        return 1

    auto_start = getattr(args, 'start', False)
    models_list = []
    for name in names:
        models_list.append({
            'name': name,
            'platform': args.platform,
            'check_interval': args.check_interval,
            'retry_interval': args.retry_interval,
            'cut_time': args.cut_time,
        })

    added = mgr.add_models_bulk(models_list)
    print(f"Added {added} new models ({len(names) - added} already existed)")
    if added > 0:
        print(f"Total models in config: {len(mgr.list_models())}")
    if auto_start and added > 0:
        for name in names:
            _try_start_model(config_path, name, args.platform)
    return 0

def cmd_remove(args, config_path: str):
    """Remove a model from config.yaml."""
    mgr = ConfigManager(config_path)
    if mgr.remove_model(args.name):
        print(f"Removed model '{args.name}' from {config_path}")
    else:
        print(f"Model '{args.name}' not found in config")
        return 1
    return 0

def cmd_list(args, config_path: str):
    """List all configured models."""
    mgr = ConfigManager(config_path)
    models = mgr.list_models()
    if not models:
        print("No models configured")
        return 0

    enabled = [m for m in models if m.get('enabled', True)]
    disabled = [m for m in models if not m.get('enabled', True)]

    print(f"\n{'='*60}")
    print(f" CtbCap Models ({len(enabled)} enabled, {len(disabled)} disabled)")
    print(f"{'='*60}")

    platforms = {}
    for m in models:
        p = m.get('platform', '?')
        if p not in platforms:
            platforms[p] = []
        platforms[p].append(m)

    for platform, pmodels in sorted(platforms.items()):
        print(f"\n  [{platform.upper()}] ({len(pmodels)} models)")
        print(f"  {'-'*50}")
        for m in sorted(pmodels, key=lambda x: x.get('name', '')):
            status = "ON" if m.get('enabled', True) else "OFF"
            interval = m.get('check_interval', '?')
            print(f"    [{status}] {m.get('name', '?'):<30} check={interval}s")

    print(f"\n{'='*60}")
    print(f" Total: {len(models)} models across {len(platforms)} platforms")
    print(f"{'='*60}\n")
    return 0

def cmd_status(args, config_path: str):
    """Show live recording status from the health server."""
    import urllib.request
    import json as json_mod

    config = Config.from_yaml(config_path)
    port = config.global_.health_check.port

    try:
        url = f"http://localhost:{port}/status"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json_mod.loads(resp.read())
    except Exception as e:
        print(f"Cannot connect to health server on port {port}")
        print(f"Is CtbCap running? Start with: ./ctbcap_multi.py start")
        return 1

    recordings = data.get('recordings', {})
    monitors = data.get('monitors', {})

    print(f"\n{'='*70}")
    print(f" CtbCap Status - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")

    if recordings:
        print(f"\n  ACTIVE RECORDINGS ({len(recordings)})")
        print(f"  {'-'*66}")
        for name, info in sorted(recordings.items()):
            dur = info.get('duration', 0)
            h, rem = divmod(int(dur), 3600)
            m, s = divmod(rem, 60)
            size = info.get('size_bytes', 0)
            speed = info.get('speed', 'N/A')
            restarts = info.get('restart_count', 0)
            pid = info.get('pid', '?')
            size_str = BandwidthMonitor().format_size(size)
            print(f"    {name:<30} {h:02d}:{m:02d}:{s:02d}  {size_str:>10}  {speed:>12}  pid={pid}  restarts={restarts}")
    else:
        print(f"\n  No active recordings")

    online = [n for n, i in monitors.items() if i.get('last_status') == 'online']
    offline = [n for n, i in monitors.items() if i.get('last_status') == 'offline']
    unknown = [n for n, i in monitors.items() if i.get('last_status') is None]

    print(f"\n  MONITORS: {len(monitors)} total | {len(online)} online | {len(offline)} offline | {len(unknown)} unknown")

    if online:
        print(f"\n  ONLINE ({len(online)})")
        for name in sorted(online):
            print(f"    [ON]  {name}")

    if args.verbose and offline:
        print(f"\n  OFFLINE ({len(offline)})")
        for name in sorted(offline):
            info = monitors[name]
            consec = info.get('consecutive_offline', 0)
            print(f"    [OFF] {name} (consecutive: {consec})")

    print(f"\n{'='*70}\n")
    return 0

def cmd_discover(args, config_path: str):
    """Discover ALL online models on a platform and optionally auto-add them."""
    config = Config.from_yaml(config_path)
    platform = args.platform or config.global_.platform

    async def _discover():
        connector = aiohttp.TCPConnector(limit=20)
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            client = PlatformClient(session, config.global_.user_agent, False)
            if platform == 'stripchat':
                return await client.discover_stripchat_online(limit=args.limit)
            elif platform == 'chaturbate':
                return await client.discover_chaturbate_online(limit=args.limit)
            else:
                print(f"Unknown platform: {platform}")
                return []

    print(f"Discovering online models on {platform}...")
    online = asyncio.run(_discover())

    if not online:
        print("No online models found (or API unavailable)")
        return 0

    print(f"\n{'='*70}")
    print(f" Found {len(online)} ONLINE models on {platform.upper()}")
    print(f"{'='*70}")

    existing = {m.get('name') for m in ConfigManager(config_path).list_models()}
    new_models = [m for m in online if m['name'] not in existing]

    print(f"  Already configured: {len(online) - len(new_models)}")
    print(f"  New (not in config): {len(new_models)}")

    for m in sorted(new_models, key=lambda x: x.get('name', '')):
        viewers = m.get('viewers', 0)
        print(f"    {m['name']:<35} viewers: {viewers}")

    if args.auto_add and new_models:
        print(f"\nAdding {len(new_models)} new models to config...")
        mgr = ConfigManager(config_path)
        added = mgr.add_models_bulk([
            {'name': m['name'], 'platform': m['platform']}
            for m in new_models
        ])
        print(f"Added {added} models to {config_path}")
    elif new_models:
        print(f"\nTo add all new models, run:")
        names = ','.join(m['name'] for m in new_models)
        print(f"  ./ctbcap_multi.py add-bulk \"{names}\" --platform {platform}")

    print(f"\n{'='*70}\n")
    return 0

def cmd_dedup(args, config_path: str):
    """Remove duplicate model entries from config.yaml."""
    mgr = ConfigManager(config_path)
    before = len(mgr.list_models())
    removed = mgr.dedup()
    after = len(mgr.list_models())
    if removed > 0:
        print(f"Removed {removed} duplicate entries ({before} -> {after} models)")
    else:
        print(f"No duplicates found ({before} models)")
    return 0

def cmd_tmux(args, config_path: str):
    """Start CtbCap in a tmux session."""
    TmuxManager.start_recording_session(config_path)
    return 0

def cmd_tmux_dashboard(args, config_path: str):
    """Start tmux dashboard view."""
    TmuxManager.start_dashboard(config_path)
    return 0

def cmd_tmux_attach(args, config_path: str):
    """Attach to running tmux session."""
    TmuxManager.attach()
    return 0

def cmd_tmux_stop(args, config_path: str):
    """Stop tmux session."""
    TmuxManager.kill_session()
    print(f"tmux session '{TmuxManager.SESSION_NAME}' killed")
    return 0

# ========================
# CLI Entry Point
# ========================

def parse_args():
    parser = argparse.ArgumentParser(
        description="CtbCap Multi-Model Recorder v2.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s start                          Start recording all configured models
  %(prog)s start -c myconfig.yaml         Start with custom config
  %(prog)s start -D                       Start as daemon (background)
  %(prog)s add mymodel --platform stripchat   Add a model to config
  %(prog)s add mymodel --platform stripchat --start   Add and auto-start recording
  %(prog)s add-bulk "name1,name2,name3" --platform stripchat   Add multiple models
  %(prog)s add-bulk "name1,name2" --start --platform stripchat   Add and auto-start
  %(prog)s remove mymodel                 Remove a model from config
  %(prog)s list                           List all configured models
  %(prog)s status                         Show live recording status
  %(prog)s discover                       Discover ALL online models
  %(prog)s discover --auto-add            Discover and auto-add new models
  %(prog)s dedup                          Remove duplicate model entries
  %(prog)s tmux                           Start in tmux session
  %(prog)s tmux-dashboard                 Start tmux dashboard
  %(prog)s stop                           Stop running daemon
        """)
    parser.add_argument('-c', '--config', default='./config.yaml', help='Config file path')
    parser.add_argument('-v', '--version', action='store_true', help='Show version')
    parser.add_argument('-D', '--daemon', action='store_true', help='Run in background')
    parser.add_argument('--validate', action='store_true', help='Validate config and exit')

    subparsers = parser.add_subparsers(dest='command', help='Commands')

    sub_start = subparsers.add_parser('start', help='Start recording all models')
    sub_start.set_defaults(func=lambda args: None)

    sub_add = subparsers.add_parser('add', help='Add a model to config.yaml')
    sub_add.add_argument('name', help='Model/username to add')
    sub_add.add_argument('--platform', '-p', choices=['chaturbate', 'stripchat'], help='Platform (default: from global config)')
    sub_add.add_argument('--check-interval', type=int, help='Check interval in seconds')
    sub_add.add_argument('--retry-interval', type=int, help='Retry interval in seconds')
    sub_add.add_argument('--cut-time', type=int, help='Cut time in seconds (0=continuous)')
    sub_add.add_argument('--start', '-s', action='store_true', help='Auto-start recording immediately')
    sub_add.set_defaults(func=cmd_add)

    sub_bulk = subparsers.add_parser('add-bulk', help='Add multiple models at once')
    sub_bulk.add_argument('names', help='Comma-separated model names')
    sub_bulk.add_argument('--platform', '-p', choices=['chaturbate', 'stripchat'], default='stripchat')
    sub_bulk.add_argument('--check-interval', type=int, help='Check interval in seconds')
    sub_bulk.add_argument('--retry-interval', type=int, help='Retry interval in seconds')
    sub_bulk.add_argument('--cut-time', type=int, help='Cut time in seconds')
    sub_bulk.add_argument('--start', '-s', action='store_true', help='Auto-start recording immediately')
    sub_bulk.set_defaults(func=cmd_add_bulk)

    sub_remove = subparsers.add_parser('remove', help='Remove a model from config.yaml')
    sub_remove.add_argument('name', help='Model/username to remove')
    sub_remove.set_defaults(func=cmd_remove)

    sub_list = subparsers.add_parser('list', help='List all configured models')
    sub_list.set_defaults(func=cmd_list)

    sub_status = subparsers.add_parser('status', help='Show recording status')
    sub_status.add_argument('--verbose', '-V', action='store_true', help='Show offline models too')
    sub_status.set_defaults(func=cmd_status)

    sub_discover = subparsers.add_parser('discover', help='Discover ALL online models')
    sub_discover.add_argument('--platform', '-p', choices=['chaturbate', 'stripchat'], help='Platform to scan')
    sub_discover.add_argument('--limit', type=int, default=500, help='Max models to scan (default: 500)')
    sub_discover.add_argument('--auto-add', '-a', action='store_true', help='Automatically add new models to config')
    sub_discover.set_defaults(func=cmd_discover)

    sub_dedup = subparsers.add_parser('dedup', help='Remove duplicate models from config')
    sub_dedup.set_defaults(func=cmd_dedup)

    sub_tmux = subparsers.add_parser('tmux', help='Start in tmux session')
    sub_tmux.set_defaults(func=cmd_tmux)

    sub_tmux_dash = subparsers.add_parser('tmux-dashboard', help='Start tmux dashboard')
    sub_tmux_dash.set_defaults(func=cmd_tmux_dashboard)

    sub_tmux_attach = subparsers.add_parser('tmux-attach', help='Attach to tmux session')
    sub_tmux_attach.set_defaults(func=cmd_tmux_attach)

    sub_tmux_stop = subparsers.add_parser('tmux-stop', help='Stop tmux session')
    sub_tmux_stop.set_defaults(func=cmd_tmux_stop)

    sub_stop = subparsers.add_parser('stop', help='Stop running daemon')
    sub_stop.set_defaults(func=lambda args: None)

    return parser.parse_args()

async def main():
    args = parse_args()

    if args.version:
        print(f"CtbCap Multi-Model Recorder v{VERSION}")
        return 0

    config_path = args.config

    if hasattr(args, 'func') and args.func is not None:
        if args.command in ('add', 'add-bulk', 'remove', 'list', 'dedup'):
            if not os.path.exists(config_path):
                print(f"Config file not found: {config_path}")
                return 1
            return args.func(args, config_path)
        elif args.command == 'discover':
            return args.func(args, config_path)
        elif args.command in ('tmux', 'tmux-dashboard', 'tmux-attach', 'tmux-stop'):
            return args.func(args, config_path)
        elif args.command == 'status':
            return args.func(args, config_path)
        elif args.command == 'start':
            pass  # Fall through to start the recorder below
        elif args.command == 'stop':
            daemon_stop()
            return 0

    if not os.path.exists(config_path):
        print(f"Config file not found: {config_path}")
        print("Create config.yaml with your model list first")
        return 1

    if getattr(args, 'stop', False):
        daemon_stop()
        return 0

    try:
        config = Config.from_yaml(config_path)
    except Exception as e:
        print(f"Failed to parse config: {e}")
        return 1

    if args.validate:
        print("Config validation passed")
        enabled = [m.name for m in config.models if m.enabled]
        disabled = [m.name for m in config.models if not m.enabled]
        print(f"Enabled models ({len(enabled)}): {enabled}")
        if disabled:
            print(f"Disabled models ({len(disabled)}): {disabled}")
        return 0

    if not config.models:
        print("No models configured!")
        print("Add models with: ./ctbcap_multi.py add <name> --platform stripchat")
        return 1

    app = CtbCap(config, config_path)

    loop = asyncio.get_running_loop()

    def _signal_handler(sig):
        logging.getLogger("ctbcap").info(f"Received signal {sig.name}, shutting down...")
        asyncio.create_task(app.stop())

    def _reload_handler(sig):
        logging.getLogger("ctbcap").info(f"Received signal {sig.name}, reloading config...")
        asyncio.create_task(app.reload_config())

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, lambda s=sig: _signal_handler(s))
        except (NotImplementedError, ValueError, OSError):
            try:
                signal.signal(sig, lambda s, f: _signal_handler(signal.Signals(s)))
            except (ValueError, OSError):
                pass

    try:
        loop.add_signal_handler(signal.SIGHUP, lambda: _reload_handler(signal.SIGHUP))
    except (NotImplementedError, ValueError, OSError, AttributeError):
        try:
            signal.signal(signal.SIGHUP, lambda s, f: _reload_handler(signal.SIGHUP))
        except (ValueError, OSError, AttributeError):
            pass

    for sig, handler in [(signal.SIGUSR1, lambda: asyncio.create_task(app.health_server.stop() if app.health_server else None)),
                         (signal.SIGUSR2, lambda: asyncio.create_task(app.health_server.start() if app.health_server else None))]:
        try:
            loop.add_signal_handler(sig, handler)
        except (NotImplementedError, ValueError, OSError, AttributeError):
            try:
                signal.signal(sig, lambda s, f: handler())
            except (ValueError, OSError, AttributeError):
                pass

    enabled_models = [m.name for m in config.models if m.enabled]
    print(f"CtbCap v{VERSION} starting with {len(enabled_models)} models")

    try:
        await app.start()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.getLogger("ctbcap").error(f"Fatal error: {e}", exc_info=True)
        return 1

    return 0

if __name__ == '__main__':
    _daemon_mode = '-D' in sys.argv or '--daemon' in sys.argv
    if _daemon_mode:
        daemon_start()
        atexit.register(lambda: (_termux_wake_lock(False), _remove_pid()))
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        pass
