#!/usr/bin/env python3
"""
CtbCap Multi-Model Recorder
A Python-based recorder for Chaturbate and StripChat supporting multiple models concurrently.
"""

import argparse
import asyncio
import json
import logging
import os
import platform
import random
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
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse

import aiohttp
import yaml

# Try to import optional dependencies
try:
    from aiohttp import web
    HAS_AIOHTTP_WEB = True
except ImportError:
    HAS_AIOHTTP_WEB = False

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
    save_path: str = "/save"
    log_path: str = "/log"
    save_space_mib: int = 512
    log_space_mib: int = 16
    cut_time: int = 3600
    cut_size_mib: int = 0
    platform: str = "chaturbate"
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    edging_mode: bool = False
    debug_mode: bool = False
    ignore_proxy: bool = False
    check_interval: int = 600
    retry_interval: int = 300
    ffmpeg_codec: str = "copy"
    ffmpeg_extra_args: str = ""
    notifications: NotificationConfig = field(default_factory=NotificationConfig)
    metadata: MetadataConfig = field(default_factory=MetadataConfig)
    health_check: HealthCheckConfig = field(default_factory=HealthCheckConfig)
    download_queue: DownloadQueueConfig = field(default_factory=DownloadQueueConfig)

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
            save_path=global_data.get('save_path', '/save'),
            log_path=global_data.get('log_path', '/log'),
            save_space_mib=global_data.get('save_space_mib', 512),
            log_space_mib=global_data.get('log_space_mib', 16),
            cut_time=global_data.get('cut_time', 3600),
            cut_size_mib=global_data.get('cut_size_mib', 0),
            platform=global_data.get('platform', 'chaturbate'),
            user_agent=global_data.get('user_agent', "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"),
            edging_mode=global_data.get('edging_mode', False),
            debug_mode=global_data.get('debug_mode', False),
            ignore_proxy=global_data.get('ignore_proxy', False),
            check_interval=global_data.get('check_interval', 600),
            retry_interval=global_data.get('retry_interval', 300),
            ffmpeg_codec=global_data.get('ffmpeg_codec', 'copy'),
            ffmpeg_extra_args=global_data.get('ffmpeg_extra_args', ''),
            notifications=NotificationConfig(**global_data.get('notifications', {})),
            metadata=MetadataConfig(**global_data.get('metadata', {})),
            health_check=HealthCheckConfig(**global_data.get('health_check', {})),
            download_queue=DownloadQueueConfig(**global_data.get('download_queue', {})),
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
            ))
        return cls(global_=global_config, models=models)

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
        
        # Telegram
        if cfg.telegram.get('enabled'):
            await self._send_telegram(cfg.telegram, full_message)
        
        # Discord
        if cfg.discord.get('enabled'):
            await self._send_discord(cfg.discord, full_message)
        
        # ntfy
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

# ========================
# Download Queue Manager
# ========================

class DownloadQueue:
    """Manages concurrent downloads with semaphores and grace period for offline detection."""
    
    def __init__(self, config: DownloadQueueConfig):
        self.config = config
        self.download_semaphore = asyncio.Semaphore(config.max_concurrent_downloads)
        self.fetch_semaphore = asyncio.Semaphore(config.max_concurrent_fetches)
        self.offline_grace_tasks: Dict[str, asyncio.Task] = {}
        self.logger = logging.getLogger("download_queue")
    
    async def acquire_download_slot(self) -> bool:
        """Try to acquire a download slot without blocking."""
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
        """Fetch stream URL with semaphore to limit concurrent API calls."""
        async with self.fetch_semaphore:
            return await fetch_func(*args, **kwargs)
    
    def schedule_offline_grace(self, model_name: str, callback, grace_seconds: int = None):
        """Schedule a grace period before actually stopping recording."""
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
        """Cancel grace period if model comes back online."""
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
        """Check if path has minimum free space. Returns (has_space, free_mib)."""
        try:
            stat = shutil.disk_usage(path)
            free_mib = stat.free // (1024 * 1024)
            return free_mib >= self.min_free_mib, free_mib
        except Exception as e:
            self.logger.error(f"Failed to check disk space for {path}: {e}")
            return True, 0
    
    async def wait_for_space(self, path: str, check_interval: int = 60) -> bool:
        """Wait until enough disk space is available."""
        while True:
            has_space, free_mib = self.check_space(path)
            if has_space:
                return True
            self.logger.warning(f"Low disk space on {path}: {free_mib} MiB free, need {self.min_free_mib} MiB. Waiting...")
            await asyncio.sleep(check_interval)

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

class Recorder:
    def __init__(self, global_config: GlobalConfig, metadata_logger: MetadataLogger, 
                 notifications: NotificationManager, download_queue: DownloadQueue, 
                 disk_monitor: DiskSpaceMonitor):
        self.global_config = global_config
        self.metadata_logger = metadata_logger
        self.notifications = notifications
        self.download_queue = download_queue
        self.disk_monitor = disk_monitor
        self.logger = logging.getLogger("recorder")
        self.sessions: Dict[str, RecordingSession] = {}
        self.ffmpeg_path = self._find_ffmpeg()
    
    def _find_ffmpeg(self) -> str:
        """Find ffmpeg binary with support for Termux, macOS (Homebrew), and standard Linux paths."""
        # Termux paths (Android)
        termux_paths = [
            '/data/data/com.termux/files/usr/bin/ffmpeg',
            '/data/data/com.termux/files/usr/bin/ffmpeg-static',
        ]
        
        # macOS Homebrew paths (Apple Silicon and Intel)
        mac_paths = [
            '/opt/homebrew/bin/ffmpeg',      # Apple Silicon (M1/M2/M3)
            '/usr/local/bin/ffmpeg',         # Intel Mac
            '/opt/homebrew/opt/ffmpeg/bin/ffmpeg',
        ]
        
        # Standard Linux paths
        linux_paths = [
            '/usr/bin/ffmpeg',
            '/usr/local/bin/ffmpeg',
            '/snap/bin/ffmpeg',
            '/var/lib/snapd/snap/bin/ffmpeg',
        ]
        
        # First try shutil.which (searches PATH)
        found = shutil.which('ffmpeg')
        if found:
            return found
        
        # Search in platform-specific paths
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
        
        # Robust input options for handling network interruptions
        cmd = [
            self.ffmpeg_path,
            '-y', '-loglevel', 'warning' if not self.global_config.debug_mode else 'debug',
            '-nostdin',
            # Input reliability options
            '-reconnect', '1',
            '-reconnect_at_eof', '1',
            '-reconnect_streamed', '1',
            '-reconnect_delay_max', '30',
            '-rw_timeout', '30000000',  # 30 second read timeout (microseconds)
            '-timeout', '30000000',      # 30 second connection timeout
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
        
        # Add MP4 flags for playable-while-recording (fragmented MP4)
        if cut_time and cut_time > 0:
            # Segment mode: each segment is independently playable
            if '%03d' not in output_path:
                output_path = output_path.replace('.mp4', '_%03d.mp4')
            cmd.extend([
                '-f', 'segment',
                '-segment_time', str(cut_time),
                '-segment_start_number', '1',
                '-reset_timestamps', '1',
                '-segment_format_options', 'movflags=+faststart+frag_keyframe+empty_moov',
                '-strftime', '1',  # Use strftime for segment naming
                output_path
            ])
        else:
            # Continuous mode: fragmented MP4 for playable while recording
            cmd.extend([
                '-f', 'mp4',
                '-movflags', 'frag_keyframe+empty_moov+faststart',
                '-avoid_negative_ts', 'make_zero',
                output_path
            ])
        
        return cmd
    
    async def start_recording(self, model: ModelConfig, stream_url: str) -> bool:
        # Check disk space before starting
        save_path = model.save_path or os.path.join(self.global_config.save_path, model.name)
        Path(save_path).mkdir(parents=True, exist_ok=True)
        
        has_space, free_mib = self.disk_monitor.check_space(save_path)
        if not has_space:
            self.logger.error(f"Insufficient disk space for {model.name}: {free_mib} MiB free, need {self.disk_monitor.min_free_mib} MiB")
            await self.notifications.send(model.name, "DISK_SPACE_LOW",
                f"Insufficient disk space: {free_mib} MiB free, need {self.disk_monitor.min_free_mib} MiB", model.notifications)
            return False
        
        # Try to acquire download slot
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
            
            self.metadata_logger.log_event(model.name, "recording_started", {
                "platform": model.platform,
                "stream_url": stream_url,
                "output_file": output_file,
                "pid": process.pid
            })
            
            await self.notifications.send(model.name, "RECORDING_STARTED", 
                f"Started recording {model.name} on {model.platform}", model.notifications)
            
            # Monitor process in background
            asyncio.create_task(self._monitor_process(model.name, process))
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to start recording for {model.name}: {e}")
            self.download_queue.release_download_slot()
            return False
    
    async def _monitor_process(self, model_name: str, process: subprocess.Popen):
        try:
            stdout, stderr = await asyncio.to_thread(process.communicate)
            return_code = process.returncode
            
            session = self.sessions.get(model_name)
            if session:
                duration = time.time() - session.start_time
                stderr_text = stderr.decode('utf-8', errors='ignore')[-2000:] if stderr else ""
                self.metadata_logger.log_event(model_name, "recording_stopped", {
                    "duration_seconds": duration,
                    "return_code": return_code,
                    "stderr": stderr_text
                })
                
                # Log the exit reason for debugging
                if return_code == 0:
                    self.logger.info(f"[{model_name}] Recording completed normally after {duration:.0f}s")
                elif return_code == -15 or return_code == 143:  # SIGTERM
                    self.logger.info(f"[{model_name}] Recording stopped by signal")
                elif return_code < 0:
                    self.logger.warning(f"[{model_name}] Recording terminated by signal {-return_code}")
                else:
                    self.logger.warning(f"[{model_name}] Recording exited with code {return_code}: {stderr_text[:500]}")
                
                await self.notifications.send(model_name, "RECORDING_STOPPED",
                    f"Recording stopped after {duration:.0f}s (code: {return_code})", 
                    None)
                
                del self.sessions[model_name]
            
            # Release download slot when recording ends
            self.download_queue.release_download_slot()
        except Exception as e:
            self.logger.error(f"Process monitor error for {model_name}: {e}")
            self.download_queue.release_download_slot()
    
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
            self.download_queue.release_download_slot()
    
    async def stop_all(self):
        for model_name in list(self.sessions.keys()):
            await self.stop_recording(model_name)

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
    
    async def run(self):
        self.running = True
        
        # Initial edging delay
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
        # Use download queue semaphore for fetching stream URL
        stream_url = await self.download_queue.fetch_with_semaphore(
            self.platform_client.fetch_stream_url, 
            self.model.name, self.model.platform,
            max_retries=self.global_config.download_queue.fetch_retry_attempts,
            retry_delay=self.global_config.download_queue.fetch_retry_delay
        )
        
        if stream_url:
            # Model is online - cancel any grace period
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
            
            # Start recording if not already
            if self.model.name not in self.recorder.sessions:
                self.logger.info(f"Starting recording for {self.model.name}")
                await self.recorder.start_recording(self.model, stream_url)
            else:
                self.logger.debug(f"Already recording {self.model.name}")
        else:
            # Model is offline
            if self.last_status != "offline":
                self.logger.info(f"[{self.model.name}] OFFLINE")
                self.metadata_logger.log_event(self.model.name, "offline", {})
                await self.notifications.send(self.model.name, "OFFLINE", 
                    f"Model went offline", self.model.notifications)
                self.last_status = "offline"
                self.offline_since = time.time()
            
            # Schedule grace period before stopping recording
            if self.model.name in self.recorder.sessions and not self.download_queue.is_in_grace_period(self.model.name):
                self.logger.info(f"Stream ended for {self.model.name}, starting grace period ({self.global_config.download_queue.offline_grace_seconds}s)")
                self.download_queue.schedule_offline_grace(
                    self.model.name,
                    lambda: self._stop_recording_after_grace(self.model.name),
                    self.global_config.download_queue.offline_grace_seconds
                )
    
    async def _stop_recording_after_grace(self, model_name: str):
        """Called after grace period expires to actually stop recording."""
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
        self.app.router.add_post('/control/{model}/stop', self.stop_model_handler)
        self.app.router.add_post('/control/stop-all', self.stop_all_handler)
        self.app.router.add_post('/control/reload-config', self.reload_config_handler)
        
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, '0.0.0.0', self.port)
        await site.start()
        logging.getLogger("health").info(f"Health server started on port {self.port}")
    
    async def stop(self):
        if self.runner:
            await self.runner.cleanup()
    
    async def health_handler(self, request):
        return web.json_response({"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()})
    
    async def status_handler(self, request):
        sessions = {}
        for name, session in self.recorder.sessions.items():
            sessions[name] = {
                "model": session.model,
                "platform": session.platform,
                "recording": True,
                "duration": time.time() - session.start_time,
                "pid": session.process.pid if session.process else None
            }
        
        monitors = {}
        for m in self.monitors:
            monitors[m.model.name] = {
                "enabled": m.model.enabled,
                "platform": m.model.platform,
                "last_status": m.last_status,
                "running": m.running
            }
        
        return web.json_response({
            "recordings": sessions,
            "monitors": monitors,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    
    async def stop_model_handler(self, request):
        model_name = request.match_info['model']
        await self.recorder.stop_recording(model_name)
        # Also stop monitor
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
    
    async def metrics_handler(self, request):
        # Prometheus-style metrics
        lines = [
            f"ctbcap_recordings_active {len(self.recorder.sessions)}",
            f"ctbcap_monitors_total {len(self.monitors)}",
            f"ctbcap_monitors_running {sum(1 for m in self.monitors if m.running)}",
        ]
        for name, session in self.recorder.sessions.items():
            lines.append(f'ctbcap_recording_duration_seconds{{model="{name}"}} {time.time() - session.start_time:.0f}')
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
        self.monitors: List[ModelMonitor] = []
        self.health_server: Optional[HealthServer] = None
        self.logger = logging.getLogger("ctbcap")
        self.running = False
    
    async def start(self):
        # Setup logging for main
        setup_logging(self.config.global_.log_path, self.config.global_.debug_mode, "ctbcap")
        
        # Initialize components with higher limits for concurrent operations
        max_concurrent = max(
            self.config.global_.download_queue.max_concurrent_fetches,
            self.config.global_.download_queue.max_concurrent_downloads
        ) * 2  # Buffer for health checks, notifications, etc.
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
        
        # Initialize download queue and disk monitor
        self.download_queue = DownloadQueue(self.config.global_.download_queue)
        self.disk_monitor = DiskSpaceMonitor(self.config.global_.download_queue.min_free_space_mib)
        
        self.recorder = Recorder(self.config.global_, self.metadata_logger, self.notifications, 
                                self.download_queue, self.disk_monitor)
        
        # Create monitors for each model
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
        
        # Start health server
        if self.config.global_.health_check.enabled:
            self.health_server = HealthServer(
                self.config.global_.health_check.port,
                self.recorder,
                self.monitors,
                self
            )
            await self.health_server.start()
        
        # Start all monitors
        self.running = True
        tasks = [monitor.run() for monitor in self.monitors]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def reload_config(self):
        """Hot reload config - add new models without stopping existing recordings"""
        self.logger.info("Reloading config...")
        try:
            new_config = Config.from_yaml(self.config_path)
        except Exception as e:
            self.logger.error(f"Failed to reload config: {e}")
            raise
        
        # Find new models (not already monitored)
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
        
        # Update existing models' configs (check_interval, retry_interval, enabled)
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
        self.config_path = self.config_path  # Keep reference
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

PID_FILE = "ctbcap.pid"

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
    """Acquire Termux wake-lock and write PID file."""
    _termux_wake_lock(True)
    _write_pid()
    print(f"Daemon started (PID: {os.getpid()})")

def daemon_stop():
    """Stop a running daemon by PID file."""
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
# CLI Entry Point
# ========================

def parse_args():
    parser = argparse.ArgumentParser(description="CtbCap Multi-Model Recorder")
    parser.add_argument('-c', '--config', default='/config/config.yaml', help='Config file path')
    parser.add_argument('-v', '--version', action='store_true', help='Show version')
    parser.add_argument('--validate', action='store_true', help='Validate config and exit')
    parser.add_argument('-D', '--daemon', action='store_true', help='Run in background (acquires Termux wake-lock on Android)')
    parser.add_argument('-S', '--stop', action='store_true', help='Stop running daemon')
    return parser.parse_args()

async def main():
    args = parse_args()
    
    if args.version:
        print("CtbCap Multi-Model Recorder v1.0.0")
        return 0
    
    if args.stop:
        daemon_stop()
        return 0
    
    config_path = args.config
    if not os.path.exists(config_path):
        print(f"Config file not found: {config_path}")
        print("Copy config.example.yaml to config.yaml and customize")
        return 1
    
    try:
        config = Config.from_yaml(config_path)
    except Exception as e:
        print(f"Failed to parse config: {e}")
        return 1
    
    if args.validate:
        print("Config validation passed")
        print(f"Models: {[m.name for m in config.models if m.enabled]}")
        return 0
    
    if not config.models:
        print("No models configured!")
        return 1
    
    app = CtbCap(config, config_path)
    
    # Signal handlers (with fallback for Termux/Android/Unix compatibility)
    loop = asyncio.get_running_loop()
    
    def _signal_handler(sig):
        logging.getLogger("ctbcap").info(f"Received signal {sig.name}, shutting down...")
        asyncio.create_task(app.stop())
    
    def _reload_handler(sig):
        logging.getLogger("ctbcap").info(f"Received signal {sig.name}, reloading config...")
        asyncio.create_task(app.reload_config())
    
    # Register signal handlers
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, lambda s=sig: _signal_handler(s))
        except (NotImplementedError, ValueError, OSError):
            # Fallback for platforms without add_signal_handler (Windows, some Termux)
            try:
                signal.signal(sig, lambda s, f: _signal_handler(signal.Signals(s)))
            except (ValueError, OSError):
                pass
    
    # SIGHUP for config reload (Unix/Linux/Mac)
    try:
        loop.add_signal_handler(signal.SIGHUP, lambda: _reload_handler(signal.SIGHUP))
    except (NotImplementedError, ValueError, OSError, AttributeError):
        try:
            signal.signal(signal.SIGHUP, lambda s, f: _reload_handler(signal.SIGHUP))
        except (ValueError, OSError, AttributeError):
            pass
    
    # SIGUSR1/SIGUSR2 for manual control (Unix/Linux/Mac)
    for sig, handler in [(signal.SIGUSR1, lambda: asyncio.create_task(app.health_server.stop() if app.health_server else None)),
                         (signal.SIGUSR2, lambda: asyncio.create_task(app.health_server.start() if app.health_server else None))]:
        try:
            loop.add_signal_handler(sig, handler)
        except (NotImplementedError, ValueError, OSError, AttributeError):
            try:
                signal.signal(sig, lambda s, f: handler())
            except (ValueError, OSError, AttributeError):
                pass
    
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
