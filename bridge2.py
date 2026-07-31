# -*- coding: utf-8 -*-
"""
Telegram -> Bale Bridge (Bridge2)
Includes: JSON Config, 5-Min Watchdog, Clean Links (Markdown), Inline Retries, NATIVE ALBUM TEST, and 10-Min Old News Filter.
"""

import asyncio
import json
import logging
import os
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

ALBUM_DEBOUNCE = 2.0  # Increased slightly to ensure all album parts are caught before sending
MAX_ATTEMPTS = 6
WORKERS = 1
TG_MAX_DOWNLOAD_BYTES = 20 * 1024 * 1024  # 20MB — Telegram Bot API download limit

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

    def short_id(self) -> str:
        return f"{self.tg_chat_id}:{self.tg_message_id}"

# =========================
# BALE CLIENT
# =========================
class BaleClient:
    def __init__(self):
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(45.0, connect=15.0))

    async def close(self):
        await self._client.aclose()

    async def _call(self, method: str, *, data: Dict[str, Any] = None, files: Dict[str, Any] = None) -> Dict[str, Any]:
        url = f"{BALE_BASE}/{method}"
        try:
            if files:
                r = await self._client.post(url, data=data or {}, files=files, timeout=httpx.Timeout(240.0, connect=15.0))
            else:
                r = await self._client.post(url, json=data or {})
        except Exception as e:
            raise RuntimeError(f"Bale request failed (network): {e}") from e

        try:
            j = r.json()
        except Exception:
            raise RuntimeError(f"Bale non-JSON response (status={r.status_code}): {(r.text or '')[:800]}")

        if r.status_code != 200 or not j.get("ok", False):
            raise RuntimeError(f"Bale API error (status={r.status_code}): {json.dumps(j, ensure_ascii=False)[:1200]}")
        return j

    async def send_message(self, chat_id: str, text: str) -> Dict[str, Any]:
        return await self._call("sendMessage", data={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})

    async def send_document(self, chat_id: str, file_bytes: bytes, filename: str, caption: Optional[str] = None) -> Dict[str, Any]:
        data = {"chat_id": chat_id, "parse_mode": "Markdown"}
        if caption: data["caption"] = caption
        return await self._call("sendDocument", data=data, files={"document": (filename, file_bytes)})

    async def send_photo(self, chat_id: str, file_bytes: bytes, filename: str, caption: Optional[str] = None) -> Dict[str, Any]:
        data = {"chat_id": chat_id, "parse_mode": "Markdown"}
        if caption: data["caption"] = caption
        return await self._call("sendPhoto", data=data, files={"photo": (filename, file_bytes)})

    async def send_video(self, chat_id: str, file_bytes: bytes, filename: str, caption: Optional[str] = None) -> Dict[str, Any]:
        data = {"chat_id": chat_id, "parse_mode": "Markdown"}
        if caption: data["caption"] = caption
        return await self._call("sendVideo", data=data, files={"video": (filename, file_bytes)})

    async def send_audio(self, chat_id: str, file_bytes: bytes, filename: str, caption: Optional[str] = None) -> Dict[str, Any]:
        data = {"chat_id": chat_id, "parse_mode": "Markdown"}
        if caption: data["caption"] = caption
        return await self._call("sendAudio", data=data, files={"audio": (filename, file_bytes)})

    async def send_voice(self, chat_id: str, file_bytes: bytes, filename: str, caption: Optional[str] = None) -> Dict[str, Any]:
        data = {"chat_id": chat_id, "parse_mode": "Markdown"}
        if caption: data["caption"] = caption
        return await self._call("sendVoice", data=data, files={"voice": (filename, file_bytes)})

    async def send_animation(self, chat_id: str, file_bytes: bytes, filename: str, caption: Optional[str] = None) -> Dict[str, Any]:
        data = {"chat_id": chat_id, "parse_mode": "Markdown"}
        if caption: data["caption"] = caption
        return await self._call("sendAnimation", data=data, files={"animation": (filename, file_bytes)})

    async def send_sticker(self, chat_id: str, file_bytes: bytes, filename: str) -> Dict[str, Any]:
        return await self._call("sendSticker", data={"chat_id": chat_id}, files={"sticker": (filename, file_bytes)})

    # ✨ NEW: The Official Media Group (Album) Method Test
    async def send_media_group(self, chat_id: str, media_json: list, files_dict: dict) -> Dict[str, Any]:
        data = {
            "chat_id": chat_id,
            "media": json.dumps(media_json)
        }
        return await self._call("sendMediaGroup", data=data, files=files_dict)

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

async def tg_download_file_bytes(app: Application, file_id: str) -> Tuple[bytes, str]:
    tg_file = await app.bot.get_file(file_id)
    b = await tg_file.download_as_bytearray()
    filename = os.path.basename(tg_file.file_path or "") or "file"
    return bytes(b), filename

async def tg_log_reply(app: Application, tg_chat_id: int, tg_message_id: int, text: str):
    try:
        await app.bot.send_message(chat_id=tg_chat_id, text=LOG_PREFIX + text, reply_to_message_id=tg_message_id, disable_web_page_preview=True)
    except Exception as e:
        logger.warning("Telegram log reply failed: %s", e)

# =========================
# TELEGRAM -> BALE SENDING
# =========================
def _media_size(msg: Message) -> Optional[int]:
    for attr in ("photo", "video", "document", "audio", "voice", "animation", "sticker", "video_note"):
        m = getattr(msg, attr, None)
        if m is None:
            continue
        if attr == "photo":
            return getattr(m[-1], "file_size", None)
        return getattr(m, "file_size", None)
    return None


async def send_single_to_bale(app: Application, bale: BaleClient, msg: Message, override_caption: Optional[str] = None, is_album_part: bool = False) -> None:
    size = _media_size(msg)
    if size and size > TG_MAX_DOWNLOAD_BYTES:
        await tg_log_reply(app, msg.chat_id, msg.message_id,
                           f"⚠️ فایل بزرگتر از حد مجاز ۲۰ مگابایت است و ارسال نشد.\nحجم: {size // (1024 * 1024)}MB")
        logger.warning("Skipped oversized file (size=%s) from %s:%s", size, msg.chat_id, msg.message_id)
        return

    if is_album_part:
        cap = override_caption
    else:
        raw_text = msg.caption if msg.caption else msg.text
        cap = clean_text(raw_text) if raw_text else clean_text(None)

    if msg.text and not (msg.photo or msg.video or msg.document or msg.audio or msg.voice or msg.animation or msg.sticker or msg.video_note):
        await bale.send_message(BALE_CHAT_ID, cap)
        return

    if msg.photo:
        data, filename = await tg_download_file_bytes(app, msg.photo[-1].file_id)
        if not filename.lower().endswith((".jpg", ".jpeg", ".png", ".webp")): filename += ".jpg"
        await bale.send_photo(BALE_CHAT_ID, data, filename, caption=cap)
        return

    if msg.video:
        data, filename = await tg_download_file_bytes(app, msg.video.file_id)
        if not filename.lower().endswith((".mp4", ".mkv", ".mov", ".webm")): filename += ".mp4"
        await bale.send_video(BALE_CHAT_ID, data, filename, caption=cap)
        return

    if msg.animation:
        data, filename = await tg_download_file_bytes(app, msg.animation.file_id)
        if not filename.lower().endswith((".gif", ".mp4")): filename += ".gif"
        await bale.send_animation(BALE_CHAT_ID, data, filename, caption=cap)
        return

    if msg.document:
        data, filename2 = await tg_download_file_bytes(app, msg.document.file_id)
        filename = msg.document.file_name or filename2 or "document"
        await bale.send_document(BALE_CHAT_ID, data, filename, caption=cap)
        return

    if msg.audio:
        data, filename2 = await tg_download_file_bytes(app, msg.audio.file_id)
        filename = msg.audio.file_name or filename2 or "audio"
        await bale.send_audio(BALE_CHAT_ID, data, filename, caption=cap)
        return

    if msg.voice:
        data, filename = await tg_download_file_bytes(app, msg.voice.file_id)
        if not filename.lower().endswith(".ogg"): filename += ".ogg"
        await bale.send_voice(BALE_CHAT_ID, data, filename, caption=cap)
        return

    if msg.video_note:
        data, filename = await tg_download_file_bytes(app, msg.video_note.file_id)
        if not filename.lower().endswith(".mp4"): filename += ".mp4"
        await bale.send_video(BALE_CHAT_ID, data, filename, caption=cap)
        return

    if msg.sticker:
        data, filename = await tg_download_file_bytes(app, msg.sticker.file_id)
        try:
            await bale.send_sticker(BALE_CHAT_ID, data, filename or "sticker")
        except Exception:
            await bale.send_document(BALE_CHAT_ID, data, filename or "sticker", caption=cap or "sticker")
        return

    if cap: await bale.send_message(BALE_CHAT_ID, cap)

# ✨ NEW: The Official Album Test Logic
async def send_album_to_bale(app: Application, bale: BaleClient, messages: List[Message]) -> None:
    messages.sort(key=lambda m: m.message_id)

    album_caption = None
    for m in messages:
        if m.caption or m.text:
            album_caption = clean_text(m.caption or m.text)
            break

    media_json = []
    media_msgs = []
    files_dict = {}
    fallback_messages = []

    for i, m in enumerate(messages):
        attach_name = f"file_{i}"

        size = _media_size(m)
        if size and size > TG_MAX_DOWNLOAD_BYTES:
            await tg_log_reply(app, m.chat_id, m.message_id,
                               f"⚠️ فایل بزرگتر از حد مجاز ۲۰ مگابایت است و از آلبوم حذف شد.\nحجم: {size // (1024 * 1024)}MB")
            logger.warning("Skipped oversized album item (size=%s)", size)
            continue

        # Bale MediaGroups typically only support Photos and Videos
        if m.photo:
            data, filename = await tg_download_file_bytes(app, m.photo[-1].file_id)
            if not filename.lower().endswith((".jpg", ".jpeg", ".png", ".webp")): filename += ".jpg"
            media_json.append({"type": "photo", "media": f"attach://{attach_name}", "caption": None, "parse_mode": "Markdown"})
            files_dict[attach_name] = (filename, data)
            media_msgs.append(m)
            
        elif m.video:
            data, filename = await tg_download_file_bytes(app, m.video.file_id)
            if not filename.lower().endswith((".mp4", ".mkv", ".mov", ".webm")): filename += ".mp4"
            media_json.append({"type": "video", "media": f"attach://{attach_name}", "caption": None, "parse_mode": "Markdown"})
            files_dict[attach_name] = (filename, data)
            media_msgs.append(m)
            
        else:
            # If the album contains a file/document, we handle it separately just in case
            fallback_messages.append((m, None))

    # Attach the album caption to the LAST item that is actually sent
    if media_json:
        media_json[-1]["caption"] = album_caption
    elif fallback_messages:
        last_m, _ = fallback_messages[-1]
        fallback_messages[-1] = (last_m, album_caption)

    # Send the native album!
    if media_json:
        if len(media_json) == 1:
            # Single-item albums are safer sent as a regular message
            single_msg = media_msgs[0]
            await send_single_to_bale(app, bale, single_msg, override_caption=album_caption, is_album_part=True)
            await asyncio.sleep(2.0)
        else:
            logger.info(f"Sending {len(media_json)} items as a media group to Bale...")
            await bale.send_media_group(BALE_CHAT_ID, media_json, files_dict)

    # Send anything else that couldn't be grouped
    for m, cap in fallback_messages:
        await send_single_to_bale(app, bale, m, override_caption=cap, is_album_part=True)
        await asyncio.sleep(2.0)

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
                    if job.kind == "single": await send_single_to_bale(self.app, self.bale, job.message)
                    elif job.kind == "album": await send_album_to_bale(self.app, self.bale, job.album_messages)
                    success = True
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

            if not success:
                await tg_log_reply(
                    self.app, job.tg_chat_id, job.tg_message_id,
                    f"❌ ارسال انجام نشد و بعد از {MAX_ATTEMPTS} تلاش رها شد.\n"
                    f"نوع: {job.kind}\nجزئیات: {str(last_err)[:300]}"
                )
                logger.error("DROP job after max attempts | kind=%s", job.kind)
                
            self.queue.task_done()

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
    bridge = Bridge(app)
    app.bot_data["bridge"] = bridge
    await bridge.start()

async def post_shutdown(app: Application):
    bridge: Bridge = app.bot_data.get("bridge")
    if bridge: await bridge.stop()

def main():
    threading.Thread(target=watchdog_thread, daemon=True).start()

    app = Application.builder().token(TG_BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()
    app.add_handler(MessageHandler(filters.ALL & ~filters.StatusUpdate.ALL, on_any_message))

    logger.info("Running Telegram polling... (Press Ctrl+C to stop)")
    app.run_polling(drop_pending_updates=False)

if __name__ == "__main__":
    main()