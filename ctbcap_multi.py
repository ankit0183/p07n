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
    
    async def fetch_stream_url(self, model: str, platform: str) -> Optional[str]:
        if platform == "chaturbate":
            return await self._fetch_chaturbate(model)
        elif platform == "stripchat":
            return await self._fetch_stripchat(model)
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
                    # Check for name change
                    async with self.session.get(f"https://chaturbate.com/{model}/", headers=headers) as r:
                        text = await r.text()
                        if 'location:' in text.lower():
                            # Parse redirect for new name
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
                    # API returns cam object with isCamActive and streamName
                    cam = data.get('cam', {})
                    if cam.get('isCamActive') or cam.get('isCamAvailable'):
                        stream_name = cam.get('streamName')
                        if stream_name:
                            return f"https://edge-hls.sacdnssedge.com/hls/{stream_name}/master/{stream_name}_auto.m3u8"
        except Exception as e:
            self.logger.error(f"Stripchat fetch error for {model}: {e}")
        return None

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
    def __init__(self, global_config: GlobalConfig, metadata_logger: MetadataLogger, notifications: NotificationManager):
        self.global_config = global_config
        self.metadata_logger = metadata_logger
        self.notifications = notifications
        self.logger = logging.getLogger("recorder")
        self.sessions: Dict[str, RecordingSession] = {}
        self.ffmpeg_path = self._find_ffmpeg()
    
    def _find_ffmpeg(self) -> str:
        termux_paths = [
            '/data/data/com.termux/files/usr/bin/ffmpeg',
        ]
        found = shutil.which('ffmpeg')
        if found:
            return found
        for path in ['/usr/bin/ffmpeg', '/usr/local/bin/ffmpeg', *termux_paths]:
            if os.path.isfile(path) and os.access(path, os.X_OK):
                return path
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
            '-copyts', '-start_at_zero',
            '-copy_unknown',
            '-user_agent', self.global_config.user_agent,
            '-headers', headers,
            '-tls_verify', '0',
            '-fflags', '+genpts+nobuffer',
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
                output_path
            ])
        else:
            # Continuous mode: fragmented MP4 for playable while recording
            cmd.extend([
                '-f', 'mp4',
                '-movflags', 'frag_keyframe+empty_moov+faststart',
                output_path
            ])
        
        return cmd
    
    async def start_recording(self, model: ModelConfig, stream_url: str) -> bool:
        save_path = model.save_path or os.path.join(self.global_config.save_path, model.name)
        Path(save_path).mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        # Always use base name; _build_ffmpeg_cmd adds %03d for segment mode
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
            return False
    
    async def _monitor_process(self, model_name: str, process: subprocess.Popen):
        try:
            stdout, stderr = await asyncio.to_thread(process.communicate)
            return_code = process.returncode
            
            session = self.sessions.get(model_name)
            if session:
                duration = time.time() - session.start_time
                self.metadata_logger.log_event(model_name, "recording_stopped", {
                    "duration_seconds": duration,
                    "return_code": return_code,
                    "stderr": stderr.decode('utf-8', errors='ignore')[-1000:] if stderr else ""
                })
                
                await self.notifications.send(model_name, "RECORDING_STOPPED",
                    f"Recording stopped after {duration:.0f}s (code: {return_code})", 
                    None)  # Will use global config
                
                del self.sessions[model_name]
        except Exception as e:
            self.logger.error(f"Process monitor error for {model_name}: {e}")
    
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
    
    async def stop_all(self):
        for model_name in list(self.sessions.keys()):
            await self.stop_recording(model_name)

# ========================
# Model Monitor
# ========================

class ModelMonitor:
    def __init__(self, model: ModelConfig, global_config: GlobalConfig, 
                 platform_client: PlatformClient, recorder: Recorder,
                 metadata_logger: MetadataLogger, notifications: NotificationManager):
        self.model = model
        self.global_config = global_config
        self.platform_client = platform_client
        self.recorder = recorder
        self.metadata_logger = metadata_logger
        self.notifications = notifications
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
        stream_url = await self.platform_client.fetch_stream_url(self.model.name, self.model.platform)
        
        if stream_url:
            # Model is online
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
            
            # Stop recording if was recording
            if self.model.name in self.recorder.sessions:
                self.logger.info(f"Stream ended for {self.model.name}, stopping recording")
                await self.recorder.stop_recording(self.model.name)
    
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
        self.monitors: List[ModelMonitor] = []
        self.health_server: Optional[HealthServer] = None
        self.logger = logging.getLogger("ctbcap")
        self.running = False
    
    async def start(self):
        # Setup logging for main
        setup_logging(self.config.global_.log_path, self.config.global_.debug_mode, "ctbcap")
        
        # Initialize components
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        
        self.platform_client = PlatformClient(
            self.session, 
            self.config.global_.user_agent, 
            self.config.global_.debug_mode
        )
        
        self.metadata_logger = MetadataLogger(self.config.global_.metadata.log_path)
        
        self.notifications = NotificationManager(self.config.global_.notifications, self.session)
        
        self.recorder = Recorder(self.config.global_, self.metadata_logger, self.notifications)
        
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
                notifications=self.notifications
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
                notifications=self.notifications
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
    
    # Signal handlers (with fallback for Termux/Android compatibility)
    loop = asyncio.get_running_loop()
    def _signal_handler():
        asyncio.create_task(app.stop())
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except (NotImplementedError, ValueError):
            signal.signal(sig, lambda s, f: asyncio.ensure_future(app.stop()))
    
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