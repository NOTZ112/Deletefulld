import os
import asyncio
import logging
from typing import Dict, Set, Optional

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, RPCError

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from motor.motor_asyncio import AsyncIOMotorClient


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
STRING_SESSION = os.getenv("STRING_SESSION", "").strip()
MONGO_URI = os.getenv("MONGO_URI", "").strip()

ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().lstrip("-").isdigit()
}

LOG_CHAT_ID = int(os.getenv("PIPE_LOG_CHAT_ID", "0"))

DELETE_BATCH = 100
PROGRESS_EVERY = 500
DELETE_PAUSE = 0.05
PROGRESS_EDIT_INTERVAL = 1.5

# Button intervals, in minutes.
PURGE_INTERVALS = {
    "60": 60,       # 1 hour
    "75": 75,       # 1¼ hours
    "90": 90,       # 1½ hours
    "120": 120,     # 2 hours
    "150": 150,     # 2½ hours
    "165": 165,     # 2¾ hours
    "180": 180,     # 3 hours
}

CUSTOM_MIN_MINUTES = 1
CUSTOM_MAX_MINUTES = 7 * 24 * 60


# ============================================================
# VALIDATION
# ============================================================

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if not API_ID:
    raise RuntimeError("API_ID is missing")
if not API_HASH:
    raise RuntimeError("API_HASH is missing")
if not STRING_SESSION:
    raise RuntimeError("STRING_SESSION is missing")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI is missing")
if not LOG_CHAT_ID:
    raise RuntimeError("PIPE_LOG_CHAT_ID is missing")


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger("cleaner-bot")


# ============================================================
# MONGO
# ============================================================

mongo = AsyncIOMotorClient(MONGO_URI)
db = mongo["duplicate_cleaner"]

approved_groups = db["approved_groups"]
dclean_state = db["dclean_state"]
media_fingerprints = db["media_fingerprints"]
purge_settings = db["purge_settings"]

# Stores message IDs received during the current purge cycle.
# _id = chat_id, message_ids = [ids...], cycle_started_at = UTC datetime
purge_cycles = db["purge_cycles"]


# ============================================================
# TELETHON
# ============================================================

tg = TelegramClient(
    StringSession(STRING_SESSION),
    API_ID,
    API_HASH,
)


# ============================================================
# RUNTIME STATE
# ============================================================

dclean_tasks: Dict[int, asyncio.Task] = {}
purge_tasks: Dict[int, asyncio.Task] = {}

# Admins can select a chat and then choose an action.
admin_selected_chat: Dict[int, int] = {}

# Admin custom-time conversation state.
pending_custom_time: Dict[int, int] = {}

# Prevent two purge workers for the same chat.
purge_locks: Dict[int, asyncio.Lock] = {}


# ============================================================
# TELETHON
# ============================================================

async def ensure_tg():
    if not tg.is_connected():
        await tg.connect()

    if not await tg.is_user_authorized():
        raise RuntimeError("STRING_SESSION is not authorized")


async def get_entity(chat_id: int):
    await ensure_tg()
    return await tg.get_entity(chat_id)


# ============================================================
# HELPERS
# ============================================================

def is_bot_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def chat_label(row: dict) -> str:
    title = str(row.get("title") or "Unknown")
    chat_id = int(row["_id"])
    return f"{title} ({chat_id})"


def format_interval(minutes: int) -> str:
    minutes = int(minutes)
    if minutes == 60:
        return "1 Hour"
    if minutes == 75:
        return "1¼ Hours"
    if minutes == 90:
        return "1½ Hours"
    if minutes == 120:
        return "2 Hours"
    if minutes == 150:
        return "2½ Hours"
    if minutes == 165:
        return "2¾ Hours"
    if minutes == 180:
        return "3 Hours"

    hours, mins = divmod(minutes, 60)
    if hours and mins:
        return f"{hours}h {mins}m"
    if hours:
        return f"{hours}h"
    return f"{mins}m"


async def resolve_chat_info(context, chat_id: int):
    title = "Unknown Chat"
    chat_type = "unknown"

    try:
        target_chat = await context.bot.get_chat(chat_id)
        title = (
            getattr(target_chat, "title", None)
            or getattr(target_chat, "username", None)
            or str(chat_id)
        )
        chat_type = getattr(target_chat, "type", "unknown")
        return title, chat_type
    except Exception as e:
        logger.warning("Bot API could not resolve %s: %s", chat_id, e)

    try:
        entity = await get_entity(chat_id)
        title = (
            getattr(entity, "title", None)
            or getattr(entity, "username", None)
            or str(chat_id)
        )
        chat_type = "channel" if getattr(entity, "broadcast", False) else "group"
    except Exception as e:
        logger.warning("Telethon could not resolve %s: %s", chat_id, e)

    return title, chat_type


async def is_dupe_approved(chat_id: int) -> bool:
    return await approved_groups.find_one(
        {"_id": int(chat_id), "approved_dupe": True}
    ) is not None


async def save_dupe_approval(chat_id: int, title: str, admin_id: int):
    await approved_groups.update_one(
        {"_id": int(chat_id)},
        {
            "$set": {
                "title": title,
                "approved_dupe": True,
                "dupe_approved_by": int(admin_id),
            }
        },
        upsert=True,
    )


# ============================================================
# PURGE DB
# ============================================================

async def get_purge_settings(chat_id: int) -> dict:
    row = await purge_settings.find_one({"_id": int(chat_id)})
    return row or {
        "_id": int(chat_id),
        "enabled": False,
        "interval_minutes": 60,
    }


async def set_purge_settings(chat_id: int, enabled: bool, interval_minutes: int):
    await purge_settings.update_one(
        {"_id": int(chat_id)},
        {
            "$set": {
                "enabled": bool(enabled),
                "interval_minutes": int(interval_minutes),
            }
        },
        upsert=True,
    )


async def set_purge_cycle_start(chat_id: int, started_at):
    await purge_cycles.update_one(
        {"_id": int(chat_id)},
        {
            "$set": {
                "cycle_started_at": started_at,
                "message_ids": [],
            }
        },
        upsert=True,
    )


async def get_purge_cycle(chat_id: int) -> dict:
    row = await purge_cycles.find_one({"_id": int(chat_id)})
    return row or {"_id": int(chat_id), "message_ids": []}


async def add_purge_message_id(chat_id: int, message_id: int):
    try:
        await purge_cycles.update_one(
            {"_id": int(chat_id)},
            {
                "$addToSet": {
                    "message_ids": int(message_id)
                }
            },
            upsert=True,
        )
    except Exception as e:
        logger.warning("Could not store purge message %s: %s", message_id, e)


async def get_approved_chats() -> list[dict]:
    rows = []
    cursor = approved_groups.find(
        {"approved_dupe": True},
        {"title": 1, "approved_dupe": 1},
    )
    async for row in cursor:
        rows.append(row)

    # Also include chats with purge enabled, even if dupe was not approved.
    cursor = purge_settings.find(
        {"enabled": True},
        {"_id": 1},
    )
    async for row in cursor:
        if not any(int(x["_id"]) == int(row["_id"]) for x in rows):
            approved = await approved_groups.find_one({"_id": int(row["_id"])})
            if approved:
                rows.append(approved)

    rows.sort(key=lambda x: str(x.get("title") or x["_id"]).lower())
    return rows


# ============================================================
# ADMIN PANEL
# ============================================================

async def send_panel(message, context):
    if not message:
        return

    chats = await get_approved_chats()

    if not chats:
        await message.reply_text(
            "🛠️ <b>ADMIN PANEL</b>\n\n"
            "No approved groups/channels yet.\n\n"
            "Add the bot to a group/channel and approve "
            "♻️ DUPE from the log chat.",
            parse_mode=ParseMode.HTML,
        )
        return

    buttons = []
    for row in chats:
        title = str(row.get("title") or row["_id"])
        buttons.append([
            InlineKeyboardButton(
                f"📌 {title}",
                callback_data=f"chat:{int(row['_id'])}",
            )
        ])

    await message.reply_text(
        "🛠️ <b>CLEANER ADMIN PANEL</b>\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📂 Select a group or channel:\n"
        "━━━━━━━━━━━━━━━━━━",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not is_bot_admin(user.id):
        return
    await send_panel(update.effective_message, context)


async def show_chat_menu(query, chat_id: int):
    row = await approved_groups.find_one({"_id": int(chat_id)})
    if not row:
        row = {"_id": int(chat_id), "title": str(chat_id)}

    title = str(row.get("title") or chat_id)
    settings = await get_purge_settings(chat_id)

    purge_status = "🟢 ON" if settings.get("enabled") else "🔴 OFF"
    interval = format_interval(int(settings.get("interval_minutes", 60)))

    buttons = []

    if await is_dupe_approved(chat_id):
        buttons.append([
            InlineKeyboardButton(
                "♻️ DUPLICATE DELETE",
                callback_data=f"dclean:{chat_id}",
            )
        ])

    buttons.extend([
        [
            InlineKeyboardButton(
                "🧹 PURGE",
                callback_data=f"purge:{chat_id}",
            ),
            InlineKeyboardButton(
                "⚙️ PURGE SETTINGS",
                callback_data=f"psettings:{chat_id}",
            ),
        ],
        [
            InlineKeyboardButton(
                "🔙 BACK",
                callback_data="back:chats",
            )
        ],
    ])

    await query.edit_message_text(
        (
            "📌 <b>CHAT PANEL</b>\n\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"📍 Name: <b>{title}</b>\n"
            f"🆔 <code>{chat_id}</code>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            f"🧹 Purge: <b>{purge_status}</b>\n"
            f"⏱️ Interval: <b>{interval}</b>"
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def show_chats(query):
    chats = await get_approved_chats()

    if not chats:
        await query.edit_message_text(
            "📂 <b>NO CHATS</b>\n\n"
            "No approved chats are available.",
            parse_mode=ParseMode.HTML,
        )
        return

    buttons = []
    for row in chats:
        title = str(row.get("title") or row["_id"])
        buttons.append([
            InlineKeyboardButton(
                f"📌 {title}",
                callback_data=f"chat:{int(row['_id'])}",
            )
        ])

    await query.edit_message_text(
        "🛠️ <b>CLEANER ADMIN PANEL</b>\n\n"
        "📂 Select a group or channel:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


# ============================================================
# APPROVAL COMMAND
# ============================================================

async def approvedupe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user

    if not message or not chat or chat.id != LOG_CHAT_ID:
        return
    if not user or not is_bot_admin(user.id):
        return

    if not context.args:
        await message.reply_text(
            "⚠️ <b>APPROVEDUPE</b>\n\n"
            "Use:\n"
            "<code>/approvedupe -100XXXXXXXXXX</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await message.reply_text("❌ Invalid Chat ID.")
        return

    title, chat_type = await resolve_chat_info(context, target_id)
    await save_dupe_approval(target_id, title, user.id)

    await message.reply_text(
        "♻️ <b>DUPLICATE CLEAN APPROVED</b>\n\n"
        "━━━━━━━━━━━━━━\n"
        f"📌 Name: <b>{title}</b>\n"
        f"🆔 <code>{target_id}</code>\n"
        f"📂 Type: <b>{chat_type}</b>\n"
        "━━━━━━━━━━━━━━\n\n"
        "♻️ DCLEAN: <b>ENABLED</b>\n"
        "🧹 PURGE: <b>OFF</b>\n\n"
        "Use <code>/start</code> to open the button panel.",
        parse_mode=ParseMode.HTML,
    )


# ============================================================
# BOT ADDED
# ============================================================

async def bot_membership_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    change = update.my_chat_member
    if not change:
        return

    chat = change.chat
    if chat.type not in ("group", "supergroup", "channel"):
        return

    old = change.old_chat_member
    new = change.new_chat_member

    was = old.status in ("member", "administrator")
    now = new.status in ("member", "administrator")

    if not now or was:
        return

    title = chat.title or "Unknown Chat"

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "♻️ DUPE",
                callback_data=f"approvedupe:{chat.id}",
            )
        ]
    ])

    try:
        await context.bot.send_message(
            LOG_CHAT_ID,
            (
                "🤖 <b>NEW GROUP / CHANNEL ADDED</b>\n\n"
                "━━━━━━━━━━━━━━\n"
                f"📌 Name: <b>{title}</b>\n"
                f"🆔 <code>{chat.id}</code>\n"
                f"📂 Type: <b>{chat.type}</b>\n"
                "━━━━━━━━━━━━━━\n\n"
                "Approve Duplicate Cleaner:"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.error("Could not send approval message: %s", e)


# ============================================================
# DUPLICATE MEDIA
# ============================================================

def media_key(message):
    document = getattr(message, "document", None)
    if document:
        # Telethon Document uses id/access_hash/file_reference.
        # Use Telegram's document id as the stable fingerprint.
        return "document", str(getattr(document, "id", ""))

    photo = getattr(message, "photo", None)
    if photo:
        return "photo", str(getattr(photo, "id", ""))

    return None


async def get_last_scanned_id(chat_id: int) -> int:
    row = await dclean_state.find_one({"_id": int(chat_id)})
    return int(row.get("last_message_id", 0)) if row else 0


async def save_last_scanned_id(chat_id: int, message_id: int):
    await dclean_state.update_one(
        {"_id": int(chat_id)},
        {"$set": {"last_message_id": int(message_id)}},
        upsert=True,
    )


async def save_fingerprint(
    chat_id: int,
    media_type: str,
    fingerprint: str,
    message_id: int,
):
    try:
        await media_fingerprints.update_one(
            {
                "chat_id": int(chat_id),
                "media_type": str(media_type),
                "fingerprint": str(fingerprint),
            },
            {
                "$setOnInsert": {
                    "chat_id": int(chat_id),
                    "media_type": str(media_type),
                    "fingerprint": str(fingerprint),
                    "message_id": int(message_id),
                }
            },
            upsert=True,
        )
    except Exception as e:
        logger.warning("Fingerprint save failed: %s", e)


# ============================================================
# LOG EDIT
# ============================================================

async def log_edit(message, text: str):
    try:
        await message.edit_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.debug("Log edit failed: %s", e)


# ============================================================
# DCLEAN
# ============================================================

async def dclean_chat(chat_id: int, log_message):
    scanned = 0
    duplicates = 0
    deleted = 0
    failed = 0
    duplicate_ids = []

    last_scanned_id = await get_last_scanned_id(chat_id)

    try:
        entity = await get_entity(chat_id)
        title = getattr(entity, "title", str(chat_id))

        if last_scanned_id == 0:
            await log_edit(
                log_message,
                "🔍 <b>FIRST DCLEAN SCAN</b>\n\n"
                f"📌 Chat: <b>{title}</b>\n"
                f"🆔 <code>{chat_id}</code>\n\n"
                "📦 Scanning complete history...",
            )
        else:
            await log_edit(
                log_message,
                "⚡ <b>INCREMENTAL DCLEAN</b>\n\n"
                f"📌 Chat: <b>{title}</b>\n"
                f"📍 Last scanned ID: <code>{last_scanned_id}</code>\n\n"
                "🔍 Scanning new messages...",
            )

        seen: Set[tuple] = set()

        cursor = media_fingerprints.find(
            {"chat_id": int(chat_id)},
            {"media_type": 1, "fingerprint": 1, "_id": 0},
        )
        async for row in cursor:
            media_type = row.get("media_type")
            fingerprint = row.get("fingerprint")
            if media_type and fingerprint:
                seen.add((str(media_type), str(fingerprint)))

        kwargs = {"entity": entity, "reverse": True}
        if last_scanned_id > 0:
            kwargs["min_id"] = last_scanned_id

        max_message_id = last_scanned_id

        async for message in tg.iter_messages(**kwargs):
            scanned += 1
            max_message_id = max(max_message_id, int(message.id))

            key = media_key(message)
            if key is None:
                continue

            media_type, fingerprint = key
            if not fingerprint:
                continue

            fingerprint_key = (str(media_type), str(fingerprint))

            if fingerprint_key in seen:
                duplicate_ids.append(int(message.id))
                duplicates += 1
            else:
                seen.add(fingerprint_key)
                await save_fingerprint(
                    chat_id,
                    media_type,
                    fingerprint,
                    int(message.id),
                )

            if scanned % PROGRESS_EVERY == 0:
                await log_edit(
                    log_message,
                    "🧹 <b>DCLEAN SCANNING</b>\n\n"
                    f"📌 <b>{title}</b>\n\n"
                    f"🔍 Scanned: <b>{scanned:,}</b>\n"
                    f"♻️ Duplicates: <b>{duplicates:,}</b>\n"
                    f"🆔 Last ID: <code>{max_message_id}</code>",
                )
                await asyncio.sleep(0)

        if max_message_id > last_scanned_id:
            await save_last_scanned_id(chat_id, max_message_id)

        total = len(duplicate_ids)

        if not duplicate_ids:
            await log_edit(
                log_message,
                "✅ <b>DCLEAN COMPLETED</b>\n\n"
                f"📌 <b>{title}</b>\n\n"
                f"📨 Scanned: <b>{scanned:,}</b>\n"
                "♻️ Duplicates: <b>0</b>\n"
                "🗑️ Deleted: <b>0</b>\n\n"
                "✅ No duplicate files found.",
            )
            return

        await log_edit(
            log_message,
            "🗑️ <b>DELETING DUPLICATES</b>\n\n"
            f"📌 <b>{title}</b>\n\n"
            f"🔍 Scanned: <b>{scanned:,}</b>\n"
            f"♻️ Duplicates: <b>{total:,}</b>\n"
            "🗑️ Deleted: <b>0</b>\n"
            f"⏳ Remaining: <b>{total:,}</b>",
        )

        for start in range(0, total, DELETE_BATCH):
            batch = duplicate_ids[start:start + DELETE_BATCH]

            while True:
                try:
                    await tg.delete_messages(
                        entity,
                        batch,
                        revoke=True,
                    )
                    deleted += len(batch)
                    break

                except FloodWaitError as e:
                    await log_edit(
                        log_message,
                        "⏳ <b>TELEGRAM FLOOD WAIT</b>\n\n"
                        f"Waiting <b>{e.seconds}s</b>...\n\n"
                        f"🗑️ Deleted: <b>{deleted:,}</b>\n"
                        f"⏳ Remaining: <b>{max(total-deleted, 0):,}</b>",
                    )
                    await asyncio.sleep(e.seconds)

                except RPCError as e:
                    logger.error("Delete RPC error: %s", e)
                    failed += len(batch)
                    break

                except Exception as e:
                    logger.exception("Delete error: %s", e)
                    failed += len(batch)
                    break

            await log_edit(
                log_message,
                "🧹 <b>DCLEAN PROGRESS</b>\n\n"
                f"📌 <b>{title}</b>\n\n"
                f"🔍 Scanned: <b>{scanned:,}</b>\n"
                f"♻️ Duplicates: <b>{total:,}</b>\n"
                f"🗑️ Deleted: <b>{deleted:,}</b>\n"
                f"❌ Failed: <b>{failed:,}</b>\n"
                f"⏳ Remaining: <b>{max(total-deleted, 0):,}</b>",
            )
            await asyncio.sleep(DELETE_PAUSE)

        await log_edit(
            log_message,
            "✅ <b>DUPLICATE CLEAN COMPLETED</b>\n\n"
            f"📌 <b>{title}</b>\n"
            f"🆔 <code>{chat_id}</code>\n\n"
            f"🔍 Scanned: <b>{scanned:,}</b>\n"
            f"♻️ Duplicates: <b>{total:,}</b>\n"
            f"🗑️ Deleted: <b>{deleted:,}</b>\n"
            f"❌ Failed: <b>{failed:,}</b>\n\n"
            "✅ Duplicate files removed.",
        )

    except Exception as e:
        logger.exception("DCLEAN failed")
        await log_edit(
            log_message,
            "❌ <b>DCLEAN FAILED</b>\n\n"
            f"🆔 <code>{chat_id}</code>\n\n"
            f"<code>{str(e)[:1200]}</code>",
        )


# ============================================================
# START DCLEAN
# ============================================================

async def start_dclean(query, chat_id: int):
    if not await is_dupe_approved(chat_id):
        await query.answer(
            "Duplicate Cleaner is not approved for this chat.",
            show_alert=True,
        )
        return

    old = dclean_tasks.get(chat_id)
    if old and not old.done():
        await query.answer(
            "DCLEAN is already running for this chat.",
            show_alert=True,
        )
        return

    log_message = await query.message.reply_text(
        "🧹 <b>DCLEAN STARTED</b>\n\n"
        f"🆔 <code>{chat_id}</code>\n\n"
        "🔍 Scanning...",
        parse_mode=ParseMode.HTML,
    )

    task = asyncio.create_task(dclean_chat(chat_id, log_message))
    dclean_tasks[chat_id] = task
    await query.answer("DCLEAN started.")


# ============================================================
# PURGE CYCLE
# ============================================================

async def purge_messages(chat_id: int, message_ids: list[int], title: str, log_message):
    unique_ids = list(dict.fromkeys(int(x) for x in message_ids))
    total = len(unique_ids)
    deleted = 0
    failed = 0

    if total == 0:
        await log_edit(
            log_message,
            "ℹ️ <b>PURGE COMPLETED</b>\n\n"
            f"📌 <b>{title}</b>\n\n"
            "📨 Messages in cycle: <b>0</b>\n"
            "🗑️ Deleted: <b>0</b>\n\n"
            "Nothing to delete.",
        )
        return

    entity = await get_entity(chat_id)

    for start in range(0, total, DELETE_BATCH):
        batch = unique_ids[start:start + DELETE_BATCH]

        while True:
            try:
                await tg.delete_messages(
                    entity,
                    batch,
                    revoke=True,
                )
                deleted += len(batch)
                break

            except FloodWaitError as e:
                await log_edit(
                    log_message,
                    "⏳ <b>PURGE FLOOD WAIT</b>\n\n"
                    f"📌 <b>{title}</b>\n"
                    f"⏳ Waiting <b>{e.seconds}s</b>...\n\n"
                    f"📨 Total: <b>{total:,}</b>\n"
                    f"🗑️ Deleted: <b>{deleted:,}</b>\n"
                    f"⏳ Remaining: <b>{max(total-deleted, 0):,}</b>",
                )
                await asyncio.sleep(e.seconds)

            except RPCError as e:
                logger.error("Purge RPC error: %s", e)
                failed += len(batch)
                break

            except Exception as e:
                logger.exception("Purge delete error: %s", e)
                failed += len(batch)
                break

        await log_edit(
            log_message,
            "🧹 <b>PURGE PROCESSING</b>\n\n"
            f"📌 <b>{title}</b>\n\n"
            f"📨 Messages: <b>{total:,}</b>\n"
            f"🗑️ Deleted: <b>{deleted:,}</b>\n"
            f"❌ Failed: <b>{failed:,}</b>\n"
            f"⏳ Remaining: <b>{max(total-deleted, 0):,}</b>",
        )
        await asyncio.sleep(DELETE_PAUSE)

    await log_edit(
        log_message,
        "✅ <b>PURGE COMPLETED</b>\n\n"
        f"📌 <b>{title}</b>\n\n"
        f"📨 Messages in cycle: <b>{total:,}</b>\n"
        f"🗑️ Deleted: <b>{deleted:,}</b>\n"
        f"❌ Failed: <b>{failed:,}</b>\n\n"
        "🔄 Next purge cycle is now active.",
    )


async def purge_worker(chat_id: int):
    lock = purge_locks.setdefault(chat_id, asyncio.Lock())

    async with lock:
        while True:
            settings = await get_purge_settings(chat_id)

            if not settings.get("enabled"):
                return

            interval = int(settings.get("interval_minutes", 60))
            if interval < CUSTOM_MIN_MINUTES:
                interval = 60

            # Each worker starts a new cycle. Messages received after this
            # point belong to the new cycle and are not part of this purge.
            from datetime import datetime, timezone

            cycle_started = datetime.now(timezone.utc)
            await set_purge_cycle_start(chat_id, cycle_started)

            logger.info(
                "Purge cycle started for %s; interval=%s minutes",
                chat_id,
                interval,
            )

            await asyncio.sleep(interval * 60)

            settings = await get_purge_settings(chat_id)
            if not settings.get("enabled"):
                return

            # If the interval was changed while sleeping, restart the cycle
            # using the new interval rather than deleting unexpectedly.
            current_interval = int(settings.get("interval_minutes", 60))
            if current_interval != interval:
                continue

            cycle = await get_purge_cycle(chat_id)
            message_ids = cycle.get("message_ids", [])

            title_row = await approved_groups.find_one({"_id": int(chat_id)})
            title = (
                str(title_row.get("title"))
                if title_row and title_row.get("title")
                else str(chat_id)
            )

            try:
                log_message = await tg_bot.send_message(
                    LOG_CHAT_ID,
                    "🧹 <b>PURGE STARTED</b>\n\n"
                    f"📌 <b>{title}</b>\n"
                    f"🆔 <code>{chat_id}</code>\n\n"
                    f"⏱️ Cycle: <b>{format_interval(interval)}</b>\n"
                    f"📨 Messages: <b>{len(message_ids):,}</b>\n"
                    "🗑️ Deleted: <b>0</b>",
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                logger.error("Could not create purge log: %s", e)
                log_message = None

            # Clear the cycle BEFORE deleting. New messages are now stored in
            # the next cycle while this batch is being deleted.
            await set_purge_cycle_start(
                chat_id,
                __import__("datetime").datetime.now(
                    __import__("datetime").timezone.utc
                ),
            )

            if log_message:
                await purge_messages(
                    chat_id,
                    message_ids,
                    title,
                    log_message,
                )

            # Continue immediately into the next fixed-length cycle.


# ============================================================
# MESSAGE CAPTURE FOR PURGE
# ============================================================

async def purge_message_capture(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message:
        return

    chat = update.effective_chat
    if not chat:
        return

    settings = await get_purge_settings(chat.id)
    if not settings.get("enabled"):
        return

    # Store every message in the current purge cycle.
    # This handler runs for normal group messages and channel posts.
    await add_purge_message_id(chat.id, message.id)


# ============================================================
# PURGE SETTINGS UI
# ============================================================

async def show_purge_settings(query, chat_id: int):
    row = await approved_groups.find_one({"_id": int(chat_id)})
    title = str(row.get("title") if row else chat_id)

    settings = await get_purge_settings(chat_id)
    enabled = bool(settings.get("enabled"))
    interval = int(settings.get("interval_minutes", 60))

    status = "🟢 ON" if enabled else "🔴 OFF"

    keyboard = [
        [
            InlineKeyboardButton("🕐 1 Hour", callback_data=f"ptime:{chat_id}:60"),
            InlineKeyboardButton("🕜 1¼ Hours", callback_data=f"ptime:{chat_id}:75"),
        ],
        [
            InlineKeyboardButton("🕝 1½ Hours", callback_data=f"ptime:{chat_id}:90"),
            InlineKeyboardButton("🕑 2 Hours", callback_data=f"ptime:{chat_id}:120"),
        ],
        [
            InlineKeyboardButton("🕝 2½ Hours", callback_data=f"ptime:{chat_id}:150"),
            InlineKeyboardButton("🕝 2¾ Hours", callback_data=f"ptime:{chat_id}:165"),
        ],
        [
            InlineKeyboardButton("🕒 3 Hours", callback_data=f"ptime:{chat_id}:180"),
        ],
        [
            InlineKeyboardButton("⚙️ CUSTOM TIME", callback_data=f"pcustom:{chat_id}"),
        ],
        [
            InlineKeyboardButton("🟢 ENABLE PURGE", callback_data=f"penable:{chat_id}"),
            InlineKeyboardButton("🔴 DISABLE PURGE", callback_data=f"pdisable:{chat_id}"),
        ],
        [
            InlineKeyboardButton("🔙 BACK", callback_data=f"chat:{chat_id}"),
        ],
    ]

    await query.edit_message_text(
        "🧹 <b>PURGE SETTINGS</b>\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>{title}</b>\n"
        f"🧹 Status: <b>{status}</b>\n"
        f"⏱️ Interval: <b>{format_interval(interval)}</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "Select how often the current cycle is fully cleared.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def set_purge_interval_from_button(query, chat_id: int, minutes: int):
    await set_purge_settings(chat_id, True, minutes)

    old = purge_tasks.get(chat_id)
    if old and not old.done():
        old.cancel()

    purge_tasks[chat_id] = asyncio.create_task(purge_worker(chat_id))

    await query.answer(f"Purge enabled: {format_interval(minutes)}")
    await show_purge_settings(query, chat_id)


async def disable_purge(query, chat_id: int):
    await set_purge_settings(chat_id, False, 60)

    old = purge_tasks.get(chat_id)
    if old and not old.done():
        old.cancel()

    purge_tasks.pop(chat_id, None)

    await query.answer("Purge disabled.")
    await show_purge_settings(query, chat_id)


# ============================================================
# CUSTOM TIME
# ============================================================

async def custom_time_prompt(query, chat_id: int):
    pending_custom_time[query.from_user.id] = chat_id

    await query.answer()
    await query.message.reply_text(
        "⚙️ <b>CUSTOM PURGE TIME</b>\n\n"
        "Send the interval in minutes.\n\n"
        "Examples:\n"
        "<code>90</code> = 1½ Hours\n"
        "<code>150</code> = 2½ Hours\n\n"
        f"Allowed: <b>{CUSTOM_MIN_MINUTES}</b>–"
        f"<b>{CUSTOM_MAX_MINUTES}</b> minutes.",
        parse_mode=ParseMode.HTML,
    )


async def custom_time_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message

    if not user or not message or not is_bot_admin(user.id):
        return

    chat_id = pending_custom_time.get(user.id)
    if not chat_id:
        return

    raw = (message.text or "").strip()

    try:
        minutes = int(raw)
    except ValueError:
        await message.reply_text("❌ Send only the number of minutes, e.g. <code>90</code>.", parse_mode=ParseMode.HTML)
        return

    if not CUSTOM_MIN_MINUTES <= minutes <= CUSTOM_MAX_MINUTES:
        await message.reply_text(
            f"❌ Enter a value between {CUSTOM_MIN_MINUTES} and {CUSTOM_MAX_MINUTES} minutes."
        )
        return

    pending_custom_time.pop(user.id, None)

    await set_purge_settings(chat_id, True, minutes)

    old = purge_tasks.get(chat_id)
    if old and not old.done():
        old.cancel()

    purge_tasks[chat_id] = asyncio.create_task(purge_worker(chat_id))

    await message.reply_text(
        "✅ <b>PURGE ENABLED</b>\n\n"
        f"📌 Chat ID: <code>{chat_id}</code>\n"
        f"⏱️ Interval: <b>{format_interval(minutes)}</b>\n\n"
        "🔄 A new cycle has started.",
        parse_mode=ParseMode.HTML,
    )


# ============================================================
# CALLBACK HANDLER
# ============================================================

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    user = query.from_user
    if not user or not is_bot_admin(user.id):
        await query.answer("❌ Not authorized", show_alert=True)
        return

    data = query.data or ""

    try:
        if data == "back:chats":
            await query.answer()
            await show_chats(query)
            return

        if data.startswith("approvedupe:"):
            chat_id = int(data.split(":", 1)[1])

            title_row = await approved_groups.find_one({"_id": chat_id})
            title = str(title_row.get("title")) if title_row else str(chat_id)

            # If the approval came from the new-group message, refresh title.
            if not title_row:
                title, _ = await resolve_chat_info(context, chat_id)

            await save_dupe_approval(chat_id, title, user.id)
            await query.answer("Duplicate Cleaner approved.")
            await show_chat_menu(query, chat_id)
            return

        if data.startswith("chat:"):
            chat_id = int(data.split(":", 1)[1])
            await query.answer()
            await show_chat_menu(query, chat_id)
            return

        if data.startswith("dclean:"):
            chat_id = int(data.split(":", 1)[1])
            await start_dclean(query, chat_id)
            return

        if data.startswith("psettings:"):
            chat_id = int(data.split(":", 1)[1])
            await query.answer()
            await show_purge_settings(query, chat_id)
            return

        if data.startswith("ptime:"):
            _, chat_id_s, minutes_s = data.split(":")
            await set_purge_interval_from_button(
                query,
                int(chat_id_s),
                int(minutes_s),
            )
            return

        if data.startswith("pcustom:"):
            chat_id = int(data.split(":", 1)[1])
            await custom_time_prompt(query, chat_id)
            return

        if data.startswith("penable:"):
            chat_id = int(data.split(":", 1)[1])
            settings = await get_purge_settings(chat_id)
            minutes = int(settings.get("interval_minutes", 60))
            await set_purge_settings(chat_id, True, minutes)

            old = purge_tasks.get(chat_id)
            if old and not old.done():
                old.cancel()

            purge_tasks[chat_id] = asyncio.create_task(purge_worker(chat_id))

            await query.answer("Purge enabled.")
            await show_purge_settings(query, chat_id)
            return

        if data.startswith("pdisable:"):
            chat_id = int(data.split(":", 1)[1])
            await disable_purge(query, chat_id)
            return

        if data.startswith("purge:"):
            chat_id = int(data.split(":", 1)[1])
            settings = await get_purge_settings(chat_id)

            if not settings.get("enabled"):
                await query.answer(
                    "Purge is OFF. Open Purge Settings to enable it.",
                    show_alert=True,
                )
                return

            await query.answer("Purge is running automatically.")
            await show_purge_settings(query, chat_id)
            return

    except Exception as e:
        logger.exception("Callback failed: %s", e)
        try:
            await query.answer("❌ Something went wrong.", show_alert=True)
        except Exception:
            pass


# ============================================================
# START PURGE WORKERS AFTER RESTART
# ============================================================

async def restore_purge_workers():
    cursor = purge_settings.find({"enabled": True})

    async for row in cursor:
        chat_id = int(row["_id"])
        old = purge_tasks.get(chat_id)

        if old and not old.done():
            continue

        purge_tasks[chat_id] = asyncio.create_task(
            purge_worker(chat_id)
        )

        logger.info(
            "Restored purge worker for %s (%s)",
            chat_id,
            format_interval(int(row.get("interval_minutes", 60))),
        )


# ============================================================
# TELEGRAM BOT OBJECT
# ============================================================

tg_bot = None


# ============================================================
# POST INIT / SHUTDOWN
# ============================================================

async def post_init(application: Application):
    global tg_bot
    tg_bot = application.bot

    await ensure_tg()

    try:
        await dclean_state.create_index(
            [("_id", 1)],
            unique=True,
        )

        await media_fingerprints.create_index(
            [
                ("chat_id", 1),
                ("media_type", 1),
                ("fingerprint", 1),
            ],
            unique=True,
        )

        await purge_settings.create_index(
            [("_id", 1)],
            unique=True,
        )

        await purge_cycles.create_index(
            [("_id", 1)],
            unique=True,
        )

        await purge_cycles.create_index(
            [("cycle_started_at", 1)],
        )

    except Exception as e:
        logger.warning("Mongo index setup warning: %s", e)

    me = await tg.get_me()
    logger.info(
        "String Session connected: %s (%s)",
        me.first_name,
        me.id,
    )

    await restore_purge_workers()


async def post_shutdown(application: Application):
    for task in list(dclean_tasks.values()) + list(purge_tasks.values()):
        if task and not task.done():
            task.cancel()

    try:
        await tg.disconnect()
    except Exception:
        pass

    try:
        mongo.close()
    except Exception:
        pass


# ============================================================
# MAIN
# ============================================================

def main():
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(CommandHandler("start", start))

    # Legacy command retained for initial approval from LOG_CHAT_ID.
    application.add_handler(
        CommandHandler("approvedupe", approvedupe_command)
    )

    application.add_handler(
        ChatMemberHandler(
            bot_membership_update,
            ChatMemberHandler.MY_CHAT_MEMBER,
        )
    )

    application.add_handler(
        CallbackQueryHandler(callback_handler)
    )

    # Purge captures messages in approved/active chats.
    application.add_handler(
        MessageHandler(
            filters.ALL,
            purge_message_capture,
        ),
        group=10,
    )

    # Admin custom purge time input.
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            custom_time_message,
        ),
        group=20,
    )

    logger.info("Bot started.")
    application.run_polling(
        allowed_updates=[
            "message",
            "channel_post",
            "callback_query",
            "my_chat_member",
        ]
    )


if __name__ == "__main__":
    main()
