# -*- coding: utf-8 -*-
"""
Telegram -> Bale Bridge (Bridge2)
Includes: JSON Config, 5-Min Watchdog, Clean Links (Markdown), Inline Retries, NATIVE ALBUM TEST,
10-Min Old News Filter, OPTIONAL Local Bot API Server (unlimited downloads), SHARED 1-hour download
cache (shared with the Rubika bridge), size-scaled upload timeouts and a persistent pending-upload
retry queue (survives restarts).
"""

import asyncio
import json
import logging
import os
import subprocess
import time
import re
import threading
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import httpx
from telegram import Message, Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters

# =========================
# 1. JSON CONFIG
# =========================
with open("config.json", "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

TG_BOT_TOKEN = CONFIG["TG_BOT_TOKEN"]
BALE_BOT_TOKEN = CONFIG["BALE_BOT_TOKEN"]
BALE_CHAT_ID = CONFIG["BALE_CHAT_ID"]
TG_SOURCE_CHAT_ID = CONFIG.get("TG_SOURCE_CHAT_ID", "")
MY_CHANNEL_TEXT = CONFIG.get("MY_CHANNEL_TEXT", "Join our channel")
MY_CHANNEL_LINK = CONFIG.get("MY_CHANNEL_LINK", "")

ALBUM_DEBOUNCE = 2.0  # ensure all album parts are caught before sending

# ---- Optional Local Bot API server ----
LOCAL_API = CONFIG.get("LOCAL_API", {})
LOCAL_API_ENABLED = bool(LOCAL_API.get("enabled", False))
LOCAL_API_HOST = LOCAL_API.get("host", "127.0.0.1")
LOCAL_API_PORT = int(LOCAL_API.get("port", 8081))
LOCAL_API_START_SERVER = bool(LOCAL_API.get("start_server", False))
LOCAL_API_EXECUTABLE = LOCAL_API.get("executable_path", "")
LOCAL_API_ID = LOCAL_API.get("api_id", "")
LOCAL_API_HASH = LOCAL_API.get("api_hash", "")
LOCAL_MAX_MB = int(LOCAL_API.get("max_download_mb", 2000))

# Download limit: 20MB on the public API, max_download_mb on the local server.
TG_MAX_DOWNLOAD_BYTES = (LOCAL_MAX_MB if LOCAL_API_ENABLED else 20) * 1024 * 1024
TG_MAX_DOWNLOAD_MB = TG_MAX_DOWNLOAD_BYTES // (1024 * 1024)

# PTB appends the token to base_url/base_file_url itself (verified).
LOCAL_TG_BASE = f"http://{LOCAL_API_HOST}:{LOCAL_API_PORT}/bot"
LOCAL_TG_FILE_BASE = f"http://{LOCAL_API_HOST}:{LOCAL_API_PORT}/file/bot"

# ---- Shared download cache (SAME dir for BOTH bridges -> download once) ----
SHARED_CACHE = CONFIG.get("SHARED_CACHE", {})
CACHE_DIR = SHARED_CACHE.get("dir", "bridge_cache")
CACHE_KEEP_SECONDS = float(SHARED_CACHE.get("keep_hours", 1)) * 3600.0
CACHE_MAX_BYTES = int(SHARED_CACHE.get("max_gb", 20)) * 1024 ** 3

# ---- Upload behaviour ----
UPLOAD_CFG = CONFIG.get("UPLOAD", {})
MAX_ATTEMPTS = int(UPLOAD_CFG.get("max_attempts", 3))
UPLOAD_BASE_TIMEOUT = float(UPLOAD_CFG.get("base_timeout_sec", 120))
UPLOAD_TIMEOUT_PER_MB = float(UPLOAD_CFG.get("timeout_per_mb", 5))
PENDING_FILE = UPLOAD_CFG.get("pending_queue", "pending_uploads.jsonl")

LOG_TAG_BALE = "#BRIDGE_LOG_BALE"
LOG_TAG_RUBIKA = "#BRIDGE_LOG_RUBIKA"
LOG_PREFIX = f"🔔 [Bale Bridge] مربوط به برنامه «بله»\n{LOG_TAG_BALE}\n"

BALE_BASE = f"https://tapi.bale.ai/bot{BALE_BOT_TOKEN}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | bridge | %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bridge.log", encoding="utf-8")],
)
logger = logging.getLogger("bridge")

# =========================
# 2. WATCHDOG (5-Minute Anti-Freeze)
# =========================
LAST_TICK = time.time()

async def async_ticker():
    global LAST_TICK
    while True:
        LAST_TICK = time.time()
        await asyncio.sleep(30)

def watchdog_thread():
    while True:
        time.sleep(30)
        if time.time() - LAST_TICK > 300:
            logger.error("Watchdog: Bot is stuck! Forcing crash for .bat to restart...")
            os._exit(1)

# =========================
# JOB MODEL
# =========================
@dataclass
class SendJob:
    kind: str
    tg_chat_id: int
    tg_message_id: int
    created_at: float = field(default_factory=lambda: time.time())
    attempts: int = 0
    message: Optional[Message] = None
    album_messages: Optional[List[Message]] = None
    album_id: Optional[str] = None
    pending: Optional[dict] = None  # recovered pending record

    def short_id(self) -> str:
        return f"{self.tg_chat_id}:{self.tg_message_id}"

# =========================
# BALE CLIENT
# =========================
class BaleFileTooBigError(RuntimeError):
    """Bale rejected the file because it exceeds Bale's upload size limit."""

class BaleClient:
    def __init__(self):
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(45.0, connect=15.0))

    async def close(self):
        await self._client.aclose()

    async def _call(self, method: str, *, data: Dict[str, Any] = None, files: Dict[str, Any] = None,
                    timeout: Optional[float] = None) -> Dict[str, Any]:
        url = f"{BALE_BASE}/{method}"
        try:
            if files:
                r = await self._client.post(url, data=data or {}, files=files,
                                            timeout=httpx.Timeout(timeout or 240.0, connect=15.0))
            else:
                r = await self._client.post(url, json=data or {})
        except Exception as e:
            raise RuntimeError(f"Bale request failed (network): {e}") from e

        try:
            j = r.json()
        except Exception:
            raise RuntimeError(f"Bale non-JSON response (status={r.status_code}): {(r.text or '')[:800]}")

        if r.status_code != 200 or not j.get("ok", False):
            desc = str(j.get("description", ""))
            if re.search(r"file[\s_]+is[\s_]+too[\s_]+big|file_size_exceeded|too[\s_]+large|"
    r"exceeds.{0,20}limit|413|too[\s_]+big|limit.{0,20}exceed|entity[\s_]+too[\s_]+large|"
    r"file[\s_]+too[\s_]+large", desc, re.IGNORECASE):
                raise BaleFileTooBigError(f"Bale API error (status={r.status_code}): {desc}")
            raise RuntimeError(f"Bale API error (status={r.status_code}): {json.dumps(j, ensure_ascii=False)[:1200]}")
        return j

    async def send_message(self, chat_id: str, text: str) -> Dict[str, Any]:
        return await self._call("sendMessage", data={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})

    async def send_document(self, chat_id: str, file_path: str, filename: str, caption: Optional[str] = None,
                            timeout: Optional[float] = None) -> Dict[str, Any]:
        data = {"chat_id": chat_id, "parse_mode": "Markdown"}
        if caption: data["caption"] = caption
        with open(file_path, "rb") as fh:
            return await self._call("sendDocument", data=data, files={"document": (filename, fh)}, timeout=timeout)

    async def send_photo(self, chat_id: str, file_path: str, filename: str, caption: Optional[str] = None,
                         timeout: Optional[float] = None) -> Dict[str, Any]:
        data = {"chat_id": chat_id, "parse_mode": "Markdown"}
        if caption: data["caption"] = caption
        with open(file_path, "rb") as fh:
            return await self._call("sendPhoto", data=data, files={"photo": (filename, fh)}, timeout=timeout)

    async def send_video(self, chat_id: str, file_path: str, filename: str, caption: Optional[str] = None,
                         timeout: Optional[float] = None) -> Dict[str, Any]:
        data = {"chat_id": chat_id, "parse_mode": "Markdown"}
        if caption: data["caption"] = caption
        with open(file_path, "rb") as fh:
            return await self._call("sendVideo", data=data, files={"video": (filename, fh)}, timeout=timeout)

    async def send_audio(self, chat_id: str, file_path: str, filename: str, caption: Optional[str] = None,
                         timeout: Optional[float] = None) -> Dict[str, Any]:
        data = {"chat_id": chat_id, "parse_mode": "Markdown"}
        if caption: data["caption"] = caption
        with open(file_path, "rb") as fh:
            return await self._call("sendAudio", data=data, files={"audio": (filename, fh)}, timeout=timeout)

    async def send_voice(self, chat_id: str, file_path: str, filename: str, caption: Optional[str] = None,
                         timeout: Optional[float] = None) -> Dict[str, Any]:
        data = {"chat_id": chat_id, "parse_mode": "Markdown"}
        if caption: data["caption"] = caption
        with open(file_path, "rb") as fh:
            return await self._call("sendVoice", data=data, files={"voice": (filename, fh)}, timeout=timeout)

    async def send_animation(self, chat_id: str, file_path: str, filename: str, caption: Optional[str] = None,
                             timeout: Optional[float] = None) -> Dict[str, Any]:
        data = {"chat_id": chat_id, "parse_mode": "Markdown"}
        if caption: data["caption"] = caption
        with open(file_path, "rb") as fh:
            return await self._call("sendAnimation", data=data, files={"animation": (filename, fh)}, timeout=timeout)

    async def send_sticker(self, chat_id: str, file_path: str, filename: str,
                           timeout: Optional[float] = None) -> Dict[str, Any]:
        with open(file_path, "rb") as fh:
            return await self._call("sendSticker", data={"chat_id": chat_id}, files={"sticker": (filename, fh)}, timeout=timeout)

    # The Official Media Group (Album) Method
    async def send_media_group(self, chat_id: str, media_json: list, files_dict: dict,
                               timeout: Optional[float] = None) -> Dict[str, Any]:
        data = {"chat_id": chat_id, "media": json.dumps(media_json)}
        return await self._call("sendMediaGroup", data=data, files=files_dict, timeout=timeout)

# =========================
# SHARED DOWNLOAD CACHE
# =========================
def _ensure_cache_dir():
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
    except OSError as e:
        logger.warning("Cannot create cache dir %s: %s", CACHE_DIR, e)

def _cache_path(file_unique_id: str, suffix: str) -> str:
    return os.path.join(CACHE_DIR, f"{file_unique_id}{suffix}")

def _sweep_cache():
    """Delete cache files older than keep_hours; if still over max_gb, delete oldest first."""
    _ensure_cache_dir()
    now = time.time()
    cutoff = now - CACHE_KEEP_SECONDS
    try:
        total = 0
        files = []
        for name in os.listdir(CACHE_DIR):
            p = os.path.join(CACHE_DIR, name)
            try:
                st = os.stat(p)
            except OSError:
                continue
            if not os.path.isfile(p):
                continue
            total += st.st_size
            files.append((st.st_mtime, st.st_size, p))

        # age-based deletion
        for mt, sz, p in files:
            if mt < cutoff:
                try:
                    os.remove(p)
                except OSError:
                    pass

        # size-cap deletion (oldest first) - never touch fresh/active files
        files = [f for f in files if f[0] >= cutoff]
        files.sort()
        for mt, sz, p in files:
            if total <= CACHE_MAX_BYTES:
                break
            if now - mt < 600:  # might be mid-upload
                continue
            if ".part." in os.path.basename(p):  # in-progress/abandoned temp
                continue
            try:
                os.remove(p)
                total -= sz
            except OSError:
                pass
    except OSError as e:
        logger.warning("cache sweep error: %s", e)

async def cache_sweep_loop():
    while True:
        await asyncio.sleep(600)
        _sweep_cache()

async def _download_to_cache(app: Application, file_id: str, file_unique_id: str, suffix: str) -> str:
    """Download a Telegram file into the SHARED cache. Returns the local path.

    If the file is already in the cache (downloaded by this bot or the Rubika
    bridge), it is reused - Telegram is only contacted once per file.
    """
    _ensure_cache_dir()
    final = _cache_path(file_unique_id, suffix)
    if os.path.exists(final) and os.path.getsize(final) > 0:
        return final  # cache hit

    tmp = f"{final}.part.{os.getpid()}"  # unique temp: both bots share CACHE_DIR
    try:
        tg_file = await app.bot.get_file(file_id)
        await tg_file.download_to_drive(custom_path=tmp)
        os.replace(tmp, final)  # atomic
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
    return final

# =========================
# PENDING UPLOAD QUEUE (persists across restarts)
# =========================
def _load_pending() -> Dict[str, dict]:
    if not os.path.exists(PENDING_FILE):
        return {}
    try:
        with open(PENDING_FILE, "r", encoding="utf-8") as f:
            return {json.loads(l)["key"]: json.loads(l) for l in f if l.strip()}
    except Exception as e:
        logger.warning("pending load failed: %s", e)
        return {}

def _save_pending(records: Dict[str, dict]):
    try:
        with open(PENDING_FILE, "w", encoding="utf-8") as f:
            for rec in records.values():
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.error("pending save failed: %s", e)

def _add_pending(rec: dict):
    records = _load_pending()
    records[rec["key"]] = rec
    _save_pending(records)

def _remove_pending(key: str):
    records = _load_pending()
    if key in records:
        records.pop(key)
        _save_pending(records)

# =========================
# UTILS & TEXT CLEANING
# =========================
MD_ESCAPE_RE = re.compile(r'([_*\[\]`\\])')

def esc_md(text: str) -> str:
    """Escape characters that are special in Bale/Telegram legacy Markdown."""
    return MD_ESCAPE_RE.sub(r'\\\1', text)

def clean_text(text: Optional[str]) -> Optional[str]:
    link_md = f'\n\n[{MY_CHANNEL_TEXT}]({MY_CHANNEL_LINK})'
    if not text:
        return link_md.strip()
    cleaned = re.sub(r'https?://\S+|#\S+|@\S+', '', text).strip()
    cleaned = esc_md(cleaned)
    return f"{cleaned}{link_md}"

def _doc_suffix(file_name: Optional[str], default: str = ".bin") -> str:
    if file_name and "." in file_name:
        suf = "." + file_name.split(".")[-1]
        if 1 < len(suf) <= 10:
            return suf
    return default

def _media_size(msg: Message) -> Optional[int]:
    for attr in ("photo", "video", "document", "audio", "voice", "animation", "sticker", "video_note"):
        m = getattr(msg, attr, None)
        # PTB returns an EMPTY TUPLE () (not None) for absent photo/video/etc.
        if not m:
            continue
        if attr == "photo":
            return getattr(m[-1], "file_size", None)
        return getattr(m, "file_size", None)
    return None

def _media_info(msg: Message) -> Optional[Tuple[str, str, str, str, str]]:
    """Return (kind, file_id, file_unique_id, suffix, filename) or None for text-only.

    Suffix conventions are IDENTICAL in the Rubika bridge so both bots compute the
    same cache path for the same Telegram file (file_unique_id is content-based).
    """
    if msg.photo:
        p = msg.photo[-1]
        return ("photo", p.file_id, getattr(p, "file_unique_id", p.file_id), ".jpg", "photo.jpg")
    if msg.video:
        v = msg.video
        return ("video", v.file_id, getattr(v, "file_unique_id", v.file_id), ".mp4", getattr(v, "file_name", None) or "video.mp4")
    if msg.animation:
        a = msg.animation
        return ("animation", a.file_id, getattr(a, "file_unique_id", a.file_id), ".mp4", "animation.mp4")
    if msg.document:
        d = msg.document
        fn = d.file_name or "document.bin"
        return ("document", d.file_id, getattr(d, "file_unique_id", d.file_id), _doc_suffix(fn), fn)
    if msg.audio:
        a = msg.audio
        fn = getattr(a, "file_name", None) or "audio.mp3"
        return ("audio", a.file_id, getattr(a, "file_unique_id", a.file_id), _doc_suffix(fn, default=".mp3"), fn)
    if msg.voice:
        v = msg.voice
        return ("voice", v.file_id, getattr(v, "file_unique_id", v.file_id), ".ogg", "voice.ogg")
    if msg.video_note:
        v = msg.video_note
        return ("video_note", v.file_id, getattr(v, "file_unique_id", v.file_id), ".mp4", "video_note.mp4")
    if msg.sticker:
        s = msg.sticker
        suf = ".webm" if getattr(s, "is_video", False) else ".tgs" if getattr(s, "is_animated", False) else ".webp"
        return ("sticker", s.file_id, getattr(s, "file_unique_id", s.file_id), suf, "sticker" + suf)
    return None

def _upload_timeout(file_size: Optional[int]) -> float:
    """Scale the Bale upload timeout with file size so big videos are not killed mid-upload."""
    if not file_size:
        return 240.0
    return min(3600.0, UPLOAD_BASE_TIMEOUT + (file_size / (1024 * 1024)) * UPLOAD_TIMEOUT_PER_MB)

async def tg_log_reply(app: Application, tg_chat_id: int, tg_message_id: int, text: str):
    try:
        await app.bot.send_message(chat_id=tg_chat_id, text=LOG_PREFIX + text, reply_to_message_id=tg_message_id, disable_web_page_preview=True)
    except Exception as e:
        logger.warning("Telegram log reply failed: %s", e)

# =========================
# TELEGRAM -> BALE SENDING
# =========================
async def _send_media(app: Application, bale: BaleClient, kind: str, file_id: str, file_unique_id: str,
                      suffix: str, filename: str, caption: Optional[str], tg_chat_id: int, tg_message_id: int):
    """Download (from cache if possible) and upload a single media item to Bale."""
    path = await _download_to_cache(app, file_id, file_unique_id, suffix)
    size = os.path.getsize(path)
    if size > TG_MAX_DOWNLOAD_BYTES:
        await tg_log_reply(app, tg_chat_id, tg_message_id,
                           f"⚠️ فایل بزرگتر از حد مجاز {TG_MAX_DOWNLOAD_MB} مگابایت است و ارسال نشد.\nحجم: {size // (1024 * 1024)}MB")
        logger.warning("Skipped oversized file (size=%s)", size)
        return
    timeout = _upload_timeout(size)

    if kind == "photo":
        await bale.send_photo(BALE_CHAT_ID, path, filename, caption=caption, timeout=timeout)
    elif kind == "video":
        await bale.send_video(BALE_CHAT_ID, path, filename, caption=caption, timeout=timeout)
    elif kind == "animation":
        await bale.send_animation(BALE_CHAT_ID, path, filename, caption=caption, timeout=timeout)
    elif kind == "document":
        await bale.send_document(BALE_CHAT_ID, path, filename, caption=caption, timeout=timeout)
    elif kind == "audio":
        await bale.send_audio(BALE_CHAT_ID, path, filename, caption=caption, timeout=timeout)
    elif kind == "voice":
        await bale.send_voice(BALE_CHAT_ID, path, filename, caption=caption, timeout=timeout)
    elif kind == "video_note":
        await bale.send_video(BALE_CHAT_ID, path, filename, caption=caption, timeout=timeout)
    elif kind == "sticker":
        try:
            await bale.send_sticker(BALE_CHAT_ID, path, filename, timeout=timeout)
        except Exception:
            await bale.send_document(BALE_CHAT_ID, path, filename, caption=caption or "sticker", timeout=timeout)
    else:
        raise RuntimeError(f"unknown media kind: {kind}")

async def send_single_to_bale(app: Application, bale: BaleClient, msg: Message, override_caption: Optional[str] = None, is_album_part: bool = False) -> None:
    size = _media_size(msg)
    if size and size > TG_MAX_DOWNLOAD_BYTES:
        await tg_log_reply(app, msg.chat_id, msg.message_id,
                           f"⚠️ فایل بزرگتر از حد مجاز {TG_MAX_DOWNLOAD_MB} مگابایت است و ارسال نشد.\nحجم: {size // (1024 * 1024)}MB")
        logger.warning("Skipped oversized file (size=%s) from %s:%s", size, msg.chat_id, msg.message_id)
        return

    if is_album_part:
        cap = override_caption
    else:
        raw_text = msg.caption if msg.caption else msg.text
        cap = clean_text(raw_text) if raw_text else clean_text(None)

    info = _media_info(msg)
    if info is None:
        # text-only (or unsupported type) -> send as a text message
        await bale.send_message(BALE_CHAT_ID, cap)
        return

    kind, file_id, file_unique_id, suffix, filename = info
    await _send_media(app, bale, kind, file_id, file_unique_id, suffix, filename, cap, msg.chat_id, msg.message_id)

# Official Album (Media Group) Logic
async def send_album_to_bale(app: Application, bale: BaleClient, messages: List[Message]) -> None:
    messages.sort(key=lambda m: m.message_id)

    album_caption = None
    for m in messages:
        if m.caption or m.text:
            album_caption = clean_text(m.caption or m.text)
            break

    media_json = []
    media_meta = []  # (kind, file_id, file_unique_id, suffix, filename, size)
    files_dict = {}
    handles = []
    fallback_messages = []

    try:
        for i, m in enumerate(messages):
            attach_name = f"file_{i}"

            size = _media_size(m)
            if size and size > TG_MAX_DOWNLOAD_BYTES:
                await tg_log_reply(app, m.chat_id, m.message_id,
                                   f"⚠️ فایل بزرگتر از حد مجاز {TG_MAX_DOWNLOAD_MB} مگابایت است و از آلبوم حذف شد.\nحجم: {size // (1024 * 1024)}MB")
                logger.warning("Skipped oversized album item (size=%s)", size)
                continue

            info = _media_info(m)
            if info is None:
                fallback_messages.append(m)
                continue

            kind, file_id, file_unique_id, suffix, filename = info

            # Bale MediaGroups only support Photos and Videos
            if kind in ("photo", "video"):
                path = await _download_to_cache(app, file_id, file_unique_id, suffix)
                fh = open(path, "rb")
                handles.append(fh)
                mtype = "photo" if kind == "photo" else "video"
                media_json.append({"type": mtype, "media": f"attach://{attach_name}", "caption": None, "parse_mode": "Markdown"})
                files_dict[attach_name] = (filename, fh)
                media_meta.append((kind, file_id, file_unique_id, suffix, filename, os.path.getsize(path)))
            else:
                fallback_messages.append(m)

        # Attach the album caption to the LAST item that is actually sent
        if media_json:
            media_json[-1]["caption"] = album_caption
        elif fallback_messages:
            fallback_messages[-1] = (fallback_messages[-1], album_caption)
        fallback_messages = [(m, None) if not isinstance(m, tuple) else m for m in fallback_messages]

        # Send the native album!
        if media_json:
            if len(media_json) == 1:
                # Single-item albums are safer sent as a regular message
                kind, file_id, file_unique_id, suffix, filename, _ = media_meta[0]
                await _send_media(app, bale, kind, file_id, file_unique_id, suffix, filename, album_caption,
                                  messages[0].chat_id, messages[0].message_id)
                await asyncio.sleep(2.0)
            else:
                max_size = max(mm[5] for mm in media_meta)
                logger.info(f"Sending {len(media_json)} items as a media group to Bale...")
                await bale.send_media_group(BALE_CHAT_ID, media_json, files_dict, timeout=_upload_timeout(max_size))

        # Send anything else that couldn't be grouped
        for m, cap in fallback_messages:
            await send_single_to_bale(app, bale, m, override_caption=cap, is_album_part=True)
            await asyncio.sleep(2.0)
    finally:
        for fh in handles:
            try:
                fh.close()
            except Exception:
                pass

# =========================
# PENDING RECOVERY
# =========================
def _make_pending_record(job: SendJob) -> Optional[dict]:
    """Serialize a failed job so it can be retried after a restart."""
    if job.kind == "single" and job.message:
        info = _media_info(job.message)
        if info is None:
            return None  # text-only: not persisted
        kind, file_id, file_unique_id, suffix, filename = info
        raw = job.message.caption if job.message.caption else job.message.text
        cap = clean_text(raw) if raw else clean_text(None)
        return {
            "key": f"{job.tg_chat_id}:{job.tg_message_id}",
            "type": "single",
            "kind": kind, "file_id": file_id, "file_unique_id": file_unique_id,
            "suffix": suffix, "filename": filename, "caption": cap,
            "tg_chat_id": job.tg_chat_id, "tg_message_id": job.tg_message_id,
        }
    if job.kind == "album" and job.album_messages:
        items = []
        album_cap = None
        for m in job.album_messages:
            info = _media_info(m)
            if info:
                kind, file_id, file_unique_id, suffix, filename = info
                items.append({"kind": kind, "file_id": file_id, "file_unique_id": file_unique_id,
                              "suffix": suffix, "filename": filename})
                if not album_cap and (m.caption or m.text):
                    album_cap = clean_text(m.caption or m.text)
        if not items:
            return None
        return {
            "key": f"{job.tg_chat_id}:{job.tg_message_id}",
            "type": "album", "items": items, "caption": album_cap,
            "tg_chat_id": job.tg_chat_id, "tg_message_id": job.tg_message_id,
        }
    return None

async def _send_pending_record(app: Application, bale: BaleClient, rec: dict):
    """Send a recovered pending record (downloads from cache or Telegram if evicted)."""
    if rec["type"] == "single":
        await _send_media(app, bale, rec["kind"], rec["file_id"], rec["file_unique_id"],
                          rec["suffix"], rec["filename"], rec.get("caption"),
                          rec.get("tg_chat_id"), rec.get("tg_message_id"))
    else:
        # album: rebuild media group from stored items
        media_json = []
        media_meta = []
        files_dict = {}
        handles = []
        try:
            for i, it in enumerate(rec.get("items", [])):
                attach_name = f"file_{i}"
                path = await _download_to_cache(app, it["file_id"], it["file_unique_id"], it["suffix"])
                fh = open(path, "rb")
                handles.append(fh)
                mtype = "photo" if it["kind"] == "photo" else "video"
                media_json.append({"type": mtype, "media": f"attach://{attach_name}", "caption": None, "parse_mode": "Markdown"})
                files_dict[attach_name] = (it["filename"], fh)
                media_meta.append((it["kind"], it["file_id"], it["file_unique_id"], it["suffix"], it["filename"], os.path.getsize(path)))

            if media_json:
                media_json[-1]["caption"] = rec.get("caption")

            if len(media_json) == 1:
                kind, file_id, fuid, suffix, filename, _ = media_meta[0]
                await _send_media(app, bale, kind, file_id, fuid, suffix, filename, rec.get("caption"),
                                  rec.get("tg_chat_id"), rec.get("tg_message_id"))
            elif media_json:
                max_size = max(mm[5] for mm in media_meta)
                await bale.send_media_group(BALE_CHAT_ID, media_json, files_dict, timeout=_upload_timeout(max_size))
        finally:
            for fh in handles:
                try:
                    fh.close()
                except Exception:
                    pass

# =========================
# BRIDGE CORE
# =========================
class Bridge:
    def __init__(self, app: Application):
        self.app = app
        self.bale = BaleClient()
        self.queue = asyncio.Queue()
        self.album_buffers = {}
        self.album_lock = asyncio.Lock()
        self._workers = []
        self._album_flusher = None

    async def start(self):
        for i in range(WORKERS): self._workers.append(asyncio.create_task(self.worker_loop(i)))
        self._album_flusher = asyncio.create_task(self.album_flush_loop())

    async def stop(self):
        if self._album_flusher: self._album_flusher.cancel()
        for t in self._workers: t.cancel()
        await self.bale.close()

    async def enqueue_single(self, msg: Message):
        await self.queue.put(SendJob(kind="single", tg_chat_id=msg.chat_id, tg_message_id=msg.message_id, message=msg))

    async def enqueue_album_buffered(self, msg: Message):
        album_id = str(msg.media_group_id)
        async with self.album_lock:
            buf = self.album_buffers.setdefault(album_id, {"last_ts": time.time(), "messages": [], "tg_chat_id": msg.chat_id, "tg_message_id": msg.message_id})
            buf["last_ts"] = time.time()
            buf["messages"].append(msg)

    async def album_flush_loop(self):
        while True:
            await asyncio.sleep(0.25)
            now = time.time()
            to_flush = []
            async with self.album_lock:
                for album_id, buf in list(self.album_buffers.items()):
                    if now - buf["last_ts"] >= ALBUM_DEBOUNCE:
                        to_flush.append((album_id, buf))
                        del self.album_buffers[album_id]

            for album_id, buf in to_flush:
                msgs = sorted(buf["messages"], key=lambda m: m.message_id)
                await self.queue.put(SendJob(kind="album", tg_chat_id=buf["tg_chat_id"], tg_message_id=buf["tg_message_id"], album_messages=msgs, album_id=album_id))

    async def worker_loop(self, idx: int):
        while True:
            job = await self.queue.get()
            success = False
            last_err = None

            for attempt in range(1, MAX_ATTEMPTS + 1):
                try:
                    if job.kind == "pending":
                        await _send_pending_record(self.app, self.bale, job.pending)
                    elif job.kind == "single":
                        await send_single_to_bale(self.app, self.bale, job.message)
                    elif job.kind == "album":
                        await send_album_to_bale(self.app, self.bale, job.album_messages)
                    success = True
                    break
                except BaleFileTooBigError as e:
                    # Bale rejected the file (e.g. too big): retrying can never
                    # succeed, so drop it immediately instead of looping forever.
                    last_err = e
                    await tg_log_reply(
                        self.app, job.tg_chat_id, job.tg_message_id,
                        f"❌ بله این فایل را به دلیل حجم زیاد نپذیرفت و ارسال نشد.\n"
                        f"نوع: {job.kind}\nجزئیات: {type(e).__name__}: {str(e)[:200]}"
                    )
                    logger.warning("DROP job (file too big for Bale) | kind=%s | %s", job.kind, str(e)[:200])
                    break
                except Exception as e:
                    last_err = e
                    delay = min(20.0, 2.0 ** attempt)

                    await tg_log_reply(
                        self.app, job.tg_chat_id, job.tg_message_id,
                        f"⚠️ ارسال به «بله» ناموفق بود. تلاش {attempt}/{MAX_ATTEMPTS}\n"
                        f"نوع: {job.kind}\n"
                        f"تلاش بعدی تا {int(delay)} ثانیه دیگر...\n"
                        f"جزئیات: {type(e).__name__}: {str(e)[:200]}"
                    )
                    logger.warning(f"Bale send failed | attempt={attempt}/{MAX_ATTEMPTS} | {str(e)[:200]}")
                    await asyncio.sleep(delay)

            if success:
                _remove_pending(job.short_id())
            else:
                # Permanently drop the job. A pending record that can never be sent
                # (e.g. file too big for Bale) must not survive, otherwise the restart
                # loop in run_bridge2.sh would retry it forever.
                _remove_pending(job.short_id())
                rec = None
                if not isinstance(last_err, BaleFileTooBigError) and job.kind != "pending":
                    rec = _make_pending_record(job)
                    if rec:
                        _add_pending(rec)
                if not isinstance(last_err, BaleFileTooBigError):
                    await tg_log_reply(
                        self.app, job.tg_chat_id, job.tg_message_id,
                        f"❌ ارسال انجام نشد و بعد از {MAX_ATTEMPTS} تلاش رها شد."
                        f"{' در صف تلاش مجدد ذخیره شد.' if rec else ''}\n"
                        f"نوع: {job.kind}\nجزئیات: {str(last_err)[:300]}"
                    )
                logger.error("DROP job after max attempts | kind=%s | %s", job.kind, str(last_err)[:200])

            self.queue.task_done()

WORKERS = 1

# =========================
# LOCAL BOT API SERVER
# =========================
LOCAL_API_PROC: Optional[subprocess.Popen] = None

def _local_api_healthy() -> bool:
    try:
        r = httpx.get(f"{LOCAL_TG_BASE}{TG_BOT_TOKEN}/getMe", timeout=3.0)
        return r.status_code == 200
    except Exception:
        return False

def start_local_api_server_if_needed():
    """If LOCAL_API is enabled, make sure the local server is reachable; optionally start it."""
    global LOCAL_API_PROC
    if not LOCAL_API_ENABLED:
        return
    if _local_api_healthy():
        logger.info("Local Bot API server already reachable on port %s", LOCAL_API_PORT)
        return

    if not LOCAL_API_START_SERVER:
        logger.error(
            "LOCAL_API is enabled but the server is not reachable and start_server is false. "
            "Start telegram-bot-api manually or via systemd first."
        )
        raise SystemExit(1)

    if not (LOCAL_API_ID and LOCAL_API_HASH and LOCAL_API_EXECUTABLE):
        logger.error("LOCAL_API start_server=true requires api_id, api_hash and executable_path in config.json")
        raise SystemExit(1)

    cmd = [
        LOCAL_API_EXECUTABLE,
        f"--api-id={LOCAL_API_ID}",
        f"--api-hash={LOCAL_API_HASH}",
        f"--http-port={LOCAL_API_PORT}",
        "--local",
    ]
    logger.info("Starting telegram-bot-api: %s", " ".join(cmd))
    LOCAL_API_PROC = subprocess.Popen(cmd)

    for _ in range(30):
        if _local_api_healthy():
            logger.info("Local Bot API server is up.")
            return
        time.sleep(1)

    logger.error("Local Bot API server did not become healthy within 30s.")
    raise SystemExit(1)

def stop_local_api_server():
    global LOCAL_API_PROC
    if LOCAL_API_PROC and LOCAL_API_PROC.poll() is None:
        LOCAL_API_PROC.terminate()
        try:
            LOCAL_API_PROC.wait(timeout=10)
        except Exception:
            LOCAL_API_PROC.kill()

# =========================
# TELEGRAM HANDLER
# =========================
def is_allowed_source(update: Update) -> bool:
    if not TG_SOURCE_CHAT_ID: return True
    try: return str(update.effective_chat.id) == str(TG_SOURCE_CHAT_ID)
    except Exception: return False

def is_bridge_log_message(msg: Message) -> bool:
    t = (msg.text or msg.caption or "")
    return (LOG_TAG_BALE in t) or (LOG_TAG_RUBIKA in t)

async def on_any_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed_source(update): return
    msg = update.effective_message
    if not msg or is_bridge_log_message(msg): return

    now = datetime.now(timezone.utc)
    if (now - msg.date).total_seconds() > 600:
        logger.info(f"Ignored old message (older than 10 mins). Message ID: {msg.message_id}")
        return

    bridge: Bridge = context.application.bot_data["bridge"]
    if msg.media_group_id: await bridge.enqueue_album_buffered(msg)
    else: await bridge.enqueue_single(msg)

# =========================
# MAIN
# =========================
async def post_init(app: Application):
    asyncio.create_task(async_ticker())
    asyncio.create_task(cache_sweep_loop())
    _sweep_cache()
    bridge = Bridge(app)
    app.bot_data["bridge"] = bridge
    await bridge.start()

    # Recover pending uploads from a previous run (survives restarts)
    for rec in _load_pending().values():
        await bridge.queue.put(SendJob(kind="pending", tg_chat_id=rec.get("tg_chat_id", 0),
                                       tg_message_id=rec.get("tg_message_id", 0), pending=rec))

async def post_shutdown(app: Application):
    bridge: Bridge = app.bot_data.get("bridge")
    if bridge: await bridge.stop()
    stop_local_api_server()

def main():
    start_local_api_server_if_needed()
    threading.Thread(target=watchdog_thread, daemon=True).start()

    builder = Application.builder().token(TG_BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown)
    if LOCAL_API_ENABLED:
        builder = builder.base_url(LOCAL_TG_BASE).base_file_url(LOCAL_TG_FILE_BASE)
    app = builder.build()
    app.add_handler(MessageHandler(filters.ALL & ~filters.StatusUpdate.ALL, on_any_message))

    logger.info("Running Telegram polling... (Press Ctrl+C to stop)")
    app.run_polling(drop_pending_updates=False)

if __name__ == "__main__":
    main()
