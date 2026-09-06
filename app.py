import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from html import escape

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, RPCError

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatMemberStatus
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ChatMemberHandler,
    MessageHandler,
    ChannelPostHandler,
    ContextTypes,
    filters,
)

# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
MONGO_URI = os.getenv("MONGO_URI", "").strip()

# EXISTING STRING SESSION
STRING_SESSION = os.getenv("STRING_SESSION", "").strip()

try:
    API_ID = int(os.getenv("API_ID", "0").strip())
except Exception:
    API_ID = 0

API_HASH = os.getenv("API_HASH", "").strip()

try:
    LOG_CHAT_ID = int(os.getenv("PIPE_LOG_CHAT_ID", "0").strip())
except Exception:
    LOG_CHAT_ID = 0

DB_NAME = "duplicate_cleaner"

# DCLEAN
DCLEAN_PROGRESS_EVERY = 500

# PURGE
PURGE_BATCH_SIZE = 100
PURGE_PROGRESS_EVERY = 500
DELETE_DELAY = 0.05

PURGE_PRESETS = {
    "60": 60,      # 1h
    "75": 75,      # 1¼h
    "90": 90,      # 1½h
    "120": 120,    # 2h
    "150": 150,    # 2½h
    "165": 165,    # 2¾h
    "180": 180,    # 3h
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger("DCLEAN-PURGE")


# ============================================================
# ADMIN IDS FROM ENV
# ============================================================

def parse_admin_ids(value):
    result = set()

    for item in value.split(","):
        item = item.strip()

        if not item:
            continue

        try:
            result.add(int(item))
        except ValueError:
            logger.warning("Invalid ADMIN_IDS value: %s", item)

    return result


ENV_ADMIN_IDS = parse_admin_ids(
    os.getenv("ADMIN_IDS", "")
)


# ============================================================
# GLOBALS
# ============================================================

mongo = None
db = None

chats_col = None
admins_col = None
dclean_state_col = None
media_col = None
pending_col = None
purge_settings_col = None

tg = None
telegram_bot = None

purge_tasks = {}
chat_locks = {}

input_states = {}


# ============================================================
# DATABASE
# ============================================================

async def init_database():

    global mongo
    global db
    global chats_col
    global admins_col
    global dclean_state_col
    global media_col
    global pending_col
    global purge_settings_col

    if not MONGO_URI:
        raise RuntimeError("MONGO_URI is missing")

    mongo = AsyncIOMotorClient(MONGO_URI)

    db = mongo[DB_NAME]

    chats_col = db["chats"]
    admins_col = db["admins"]
    dclean_state_col = db["dclean_state"]
    media_col = db["media_fingerprints"]
    pending_col = db["dclean_pending"]
    purge_settings_col = db["purge_settings"]

    await chats_col.create_index(
        [("active", ASCENDING), ("title", ASCENDING)]
    )

    await admins_col.create_index(
        "user_id",
        unique=True
    )

    await dclean_state_col.create_index(
        "chat_id",
        unique=True
    )

    await media_col.create_index(
        [
            ("chat_id", ASCENDING),
            ("media_type", ASCENDING),
            ("fingerprint", ASCENDING),
        ],
        unique=True,
    )

    await pending_col.create_index(
        [
            ("chat_id", ASCENDING),
            ("message_id", ASCENDING),
        ],
        unique=True,
    )

    await purge_settings_col.create_index(
        "chat_id",
        unique=True
    )


async def seed_environment_admins():

    for user_id in ENV_ADMIN_IDS:

        await admins_col.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "user_id": user_id,
                    "source": "ENV",
                    "updated_at": datetime.now(timezone.utc),
                },
                "$setOnInsert": {
                    "created_at": datetime.now(timezone.utc)
                },
            },
            upsert=True,
        )


async def is_admin(user_id):

    if user_id in ENV_ADMIN_IDS:
        return True

    doc = await admins_col.find_one(
        {"user_id": user_id}
    )

    return bool(doc)


async def get_admins():

    docs = await admins_col.find({}).sort(
        "user_id",
        ASCENDING
    ).to_list(length=1000)

    return [
        int(x["user_id"])
        for x in docs
        if x.get("user_id") is not None
    ]


async def add_admin(user_id, added_by):

    await admins_col.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "user_id": user_id,
                "source": "BUTTON",
                "added_by": added_by,
                "updated_at": datetime.now(timezone.utc),
            },
            "$setOnInsert": {
                "created_at": datetime.now(timezone.utc)
            },
        },
        upsert=True,
    )


async def remove_admin(user_id):

    # ADMIN_IDS from Render/Heroku Config Vars are protected.
    if user_id in ENV_ADMIN_IDS:
        return False

    result = await admins_col.delete_one(
        {"user_id": user_id}
    )

    return result.deleted_count > 0


# ============================================================
# CHAT DATABASE
# ============================================================

async def register_chat(chat):

    if not chat:
        return

    if chat.type not in (
        "group",
        "supergroup",
        "channel",
    ):
        return

    chat_id = int(chat.id)

    title = chat.title or str(chat_id)

    await chats_col.update_one(
        {"chat_id": chat_id},
        {
            "$set": {
                "chat_id": chat_id,
                "title": title,
                "chat_type": chat.type,
                "active": True,
                "updated_at": datetime.now(timezone.utc),
            },
            "$setOnInsert": {
                "created_at": datetime.now(timezone.utc)
            },
        },
        upsert=True,
    )

    # Create default purge settings.
    await purge_settings_col.update_one(
        {"chat_id": chat_id},
        {
            "$setOnInsert": {
                "chat_id": chat_id,
                "enabled": False,
                "minutes": 60,
                "duplicate_delete": False,
                "created_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )


async def deactivate_chat(chat_id):

    await chats_col.update_one(
        {"chat_id": chat_id},
        {
            "$set": {
                "active": False,
                "updated_at": datetime.now(timezone.utc),
            }
        }
    )


async def get_chat(chat_id):

    return await chats_col.find_one(
        {
            "chat_id": int(chat_id),
            "active": True,
        }
    )


async def get_all_chats():

    return await chats_col.find(
        {"active": True}
    ).sort(
        "title",
        ASCENDING
    ).to_list(length=1000)


# ============================================================
# CHAT LOCK
# ============================================================

def get_chat_lock(chat_id):

    chat_id = int(chat_id)

    if chat_id not in chat_locks:
        chat_locks[chat_id] = asyncio.Lock()

    return chat_locks[chat_id]


# ============================================================
# TELETHON HELPERS
# ============================================================

async def telethon_call(factory):

    while True:

        try:
            return await factory()

        except FloodWaitError as e:

            logger.warning(
                "FloodWait: sleeping %s seconds",
                e.seconds
            )

            await asyncio.sleep(
                e.seconds + 1
            )


async def get_entity(chat_id):

    return await telethon_call(
        lambda: tg.get_entity(chat_id)
    )


# ============================================================
# DCLEAN MEDIA FINGERPRINT
# ============================================================

def get_media_fingerprint(message):

    photo = getattr(message, "photo", None)

    if photo:

        return (
            "photo",
            str(photo.id)
        )

    document = getattr(
        message,
        "document",
        None
    )

    if document:

        return (
            "document",
            str(document.id)
        )

    return None


async def get_fingerprint(
    chat_id,
    media_type,
    fingerprint
):

    return await media_col.find_one(
        {
            "chat_id": int(chat_id),
            "media_type": media_type,
            "fingerprint": fingerprint,
        }
    )


async def save_fingerprint(
    chat_id,
    media_type,
    fingerprint,
    message_id
):

    await media_col.update_one(
        {
            "chat_id": int(chat_id),
            "media_type": media_type,
            "fingerprint": fingerprint,
        },
        {
            "$setOnInsert": {
                "chat_id": int(chat_id),
                "media_type": media_type,
                "fingerprint": fingerprint,
                "message_id": int(message_id),
                "created_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )


# ============================================================
# DCLEAN STATE
# ============================================================

async def get_last_dclean_id(chat_id):

    doc = await dclean_state_col.find_one(
        {"chat_id": int(chat_id)}
    )

    if not doc:
        return 0

    return int(
        doc.get("last_message_id", 0)
    )


async def save_last_dclean_id(
    chat_id,
    message_id
):

    await dclean_state_col.update_one(
        {"chat_id": int(chat_id)},
        {
            "$set": {
                "chat_id": int(chat_id),
                "last_message_id": int(message_id),
                "updated_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )


# ============================================================
# DCLEAN PENDING
# ============================================================

async def add_pending(
    chat_id,
    message_id
):

    await pending_col.update_one(
        {
            "chat_id": int(chat_id),
            "message_id": int(message_id),
        },
        {
            "$set": {
                "updated_at": datetime.now(timezone.utc)
            },
            "$setOnInsert": {
                "chat_id": int(chat_id),
                "message_id": int(message_id),
                "created_at": datetime.now(timezone.utc),
            },
        },
        upsert=True,
    )


async def remove_pending(
    chat_id,
    message_ids
):

    if not message_ids:
        return

    await pending_col.delete_many(
        {
            "chat_id": int(chat_id),
            "message_id": {
                "$in": [
                    int(x)
                    for x in message_ids
                ]
            },
        }
    )


async def get_pending(chat_id):

    return await pending_col.find(
        {"chat_id": int(chat_id)}
    ).sort(
        "message_id",
        ASCENDING
    ).to_list(length=10000)


# ============================================================
# DELETE MESSAGES
# ============================================================

async def delete_ids(
    entity,
    ids
):

    if not ids:
        return True

    try:

        await telethon_call(
            lambda: tg.delete_messages(
                entity,
                ids,
                revoke=True
            )
        )

        await asyncio.sleep(
            DELETE_DELAY
        )

        return True

    except RPCError:

        logger.exception(
            "Telegram RPC delete error"
        )

        return False

    except Exception:

        logger.exception(
            "Delete error"
        )

        return False


# ============================================================
# DCLEAN
# ============================================================

async def run_dclean(
    chat_id,
    progress_message
):

    lock = get_chat_lock(chat_id)

    if lock.locked():

        await progress_message.edit_text(
            "♻️ <b>DCLEAN</b>\n\n"
            "⚠️ Another DCLEAN/PURGE job is running.",
            parse_mode="HTML",
            reply_markup=chat_keyboard(chat_id),
        )

        return

    async with lock:

        chat = await get_chat(chat_id)

        if not chat:

            await progress_message.edit_text(
                "❌ Chat is not available.",
                reply_markup=main_back_keyboard(),
            )

            return

        entity = await get_entity(chat_id)

        last_id = await get_last_dclean_id(
            chat_id
        )

        full_scan = last_id == 0

        await progress_message.edit_text(
            "♻️ <b>DCLEAN STARTED</b>\n\n"
            f"Mode: <b>{'FULL HISTORY' if full_scan else 'NEW MESSAGES ONLY'}</b>\n"
            f"Last ID: <code>{last_id}</code>\n\n"
            "Scanning...",
            parse_mode="HTML",
        )

        scanned = 0
        duplicate_ids = []
        highest_id = last_id

        # Oldest -> newest on first scan.
        async for message in tg.iter_messages(
            entity,
            min_id=last_id if last_id else 0,
            reverse=True,
        ):

            if not message:
                continue

            message_id = getattr(
                message,
                "id",
                None
            )

            if not message_id:
                continue

            message_id = int(message_id)

            highest_id = max(
                highest_id,
                message_id
            )

            scanned += 1

            fp = get_media_fingerprint(
                message
            )

            if not fp:
                continue

            media_type, fingerprint = fp

            existing = await get_fingerprint(
                chat_id,
                media_type,
                fingerprint
            )

            if existing:

                original_id = int(
                    existing.get(
                        "message_id",
                        0
                    )
                )

                if original_id != message_id:

                    duplicate_ids.append(
                        message_id
                    )

                    await add_pending(
                        chat_id,
                        message_id
                    )

            else:

                await save_fingerprint(
                    chat_id,
                    media_type,
                    fingerprint,
                    message_id
                )

            if scanned % DCLEAN_PROGRESS_EVERY == 0:

                await progress_message.edit_text(
                    "♻️ <b>DCLEAN SCANNING</b>\n\n"
                    f"Scanned: <code>{scanned}</code>\n"
                    f"Duplicates: <code>{len(duplicate_ids)}</code>\n"
                    f"Last ID: <code>{highest_id}</code>",
                    parse_mode="HTML",
                )

        # Retry old failed duplicates.
        pending_docs = await get_pending(
            chat_id
        )

        pending_ids = [
            int(x["message_id"])
            for x in pending_docs
        ]

        all_delete_ids = sorted(
            set(
                duplicate_ids +
                pending_ids
            )
        )

        deleted = 0
        failed = 0

        for start in range(
            0,
            len(all_delete_ids),
            PURGE_BATCH_SIZE
        ):

            batch = all_delete_ids[
                start:
                start + PURGE_BATCH_SIZE
            ]

            success = await delete_ids(
                entity,
                batch
            )

            if success:

                deleted += len(batch)

                await remove_pending(
                    chat_id,
                    batch
                )

            else:

                failed += len(batch)

            await progress_message.edit_text(
                "♻️ <b>DCLEAN DELETE</b>\n\n"
                f"Scanned: <code>{scanned}</code>\n"
                f"Found: <code>{len(all_delete_ids)}</code>\n"
                f"Deleted: <code>{deleted}</code>\n"
                f"Remaining: <code>{len(all_delete_ids) - deleted}</code>",
                parse_mode="HTML",
            )

        # Save the scan position.
        if highest_id > last_id:

            await save_last_dclean_id(
                chat_id,
                highest_id
            )

        await progress_message.edit_text(
            "♻️ <b>DCLEAN COMPLETE</b>\n\n"
            f"Scanned: <code>{scanned}</code>\n"
            f"Duplicates: <code>{len(all_delete_ids)}</code>\n"
            f"Deleted: <code>{deleted}</code>\n"
            f"Failed: <code>{failed}</code>\n"
            f"Last scanned ID: <code>{highest_id}</code>",
            parse_mode="HTML",
            reply_markup=chat_keyboard(chat_id),
        )


# ============================================================
# PURGE SETTINGS
# ============================================================

async def get_purge_settings(chat_id):

    doc = await purge_settings_col.find_one(
        {"chat_id": int(chat_id)}
    )

    if doc:
        return doc

    default = {
        "chat_id": int(chat_id),
        "enabled": False,
        "minutes": 60,
        "duplicate_delete": False,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }

    await purge_settings_col.insert_one(
        default
    )

    return default


async def save_purge_settings(
    chat_id,
    **values
):

    values["updated_at"] = datetime.now(
        timezone.utc
    )

    await purge_settings_col.update_one(
        {"chat_id": int(chat_id)},
        {
            "$set": values,
            "$setOnInsert": {
                "chat_id": int(chat_id)
            },
        },
        upsert=True,
    )


def duration_text(minutes):

    mapping = {
        60: "1h",
        75: "1¼h",
        90: "1½h",
        120: "2h",
        150: "2½h",
        165: "2¾h",
        180: "3h",
    }

    if minutes in mapping:
        return mapping[minutes]

    if minutes >= 60:

        hours = minutes // 60
        rem = minutes % 60

        if rem:
            return f"{hours}h {rem}m"

        return f"{hours}h"

    return f"{minutes}m"


# ============================================================
# PURGE
# ============================================================

async def run_purge(
    chat_id,
    minutes,
    duplicate_delete=False,
    progress_message=None,
):

    lock = get_chat_lock(chat_id)

    if lock.locked():

        if progress_message:

            await progress_message.edit_text(
                "🗑️ <b>PURGE</b>\n\n"
                "⚠️ Another DCLEAN/PURGE job is already running.",
                parse_mode="HTML",
                reply_markup=chat_keyboard(chat_id),
            )

        return

    async with lock:

        chat = await get_chat(chat_id)

        if not chat:
            return

        entity = await get_entity(
            chat_id
        )

        cutoff = (
            datetime.now(timezone.utc)
            - timedelta(minutes=minutes)
        )

        scanned = 0
        deleted = 0
        failed = 0

        old_ids = []

        if progress_message:

            await progress_message.edit_text(
                "🗑️ <b>PURGE STARTED</b>\n\n"
                f"Delete older than: <b>{duration_text(minutes)}</b>\n"
                f"Duplicate Delete: <b>{'ON' if duplicate_delete else 'OFF'}</b>\n\n"
                "Scanning...",
                parse_mode="HTML",
            )

        # ----------------------------------------------------
        # AGE PURGE
        # ----------------------------------------------------

        async for message in tg.iter_messages(
            entity
        ):

            if not message:
                continue

            message_id = getattr(
                message,
                "id",
                None
            )

            if not message_id:
                continue

            scanned += 1

            msg_date = getattr(
                message,
                "date",
                None
            )

            if not msg_date:
                continue

            if msg_date.tzinfo is None:

                msg_date = msg_date.replace(
                    tzinfo=timezone.utc
                )

            if msg_date < cutoff:

                old_ids.append(
                    int(message_id)
                )

            else:
                # Newest -> oldest, so once we cross
                # the cutoff we can stop.
                break

            if (
                scanned %
                PURGE_PROGRESS_EVERY
                == 0
            ):

                if progress_message:

                    await progress_message.edit_text(
                        "🧹 <b>PURGE SCANNING</b>\n\n"
                        f"Scanned: <code>{scanned}</code>\n"
                        f"To delete: <code>{len(old_ids)}</code>\n"
                        f"Deleted: <code>{deleted}</code>",
                        parse_mode="HTML",
                    )

        # ----------------------------------------------------
        # OPTIONAL DUPLICATE DELETE
        # ----------------------------------------------------

        if duplicate_delete:

            seen = set()

            async for message in tg.iter_messages(
                entity
            ):

                fp = get_media_fingerprint(
                    message
                )

                if not fp:
                    continue

                media_type, fingerprint = fp

                key = (
                    media_type,
                    fingerprint
                )

                if key in seen:

                    old_ids.append(
                        int(message.id)
                    )

                else:

                    seen.add(key)

        delete_ids_list = sorted(
            set(old_ids),
            reverse=True
        )

        total = len(delete_ids_list)

        # ----------------------------------------------------
        # DELETE
        # ----------------------------------------------------

        for start in range(
            0,
            total,
            PURGE_BATCH_SIZE
        ):

            batch = delete_ids_list[
                start:
                start + PURGE_BATCH_SIZE
            ]

            success = await delete_ids(
                entity,
                batch
            )

            if success:

                deleted += len(batch)

            else:

                failed += len(batch)

            remaining = max(
                0,
                total - deleted - failed
            )

            if progress_message:

                await progress_message.edit_text(
                    "🗑️ <b>PURGE DELETE</b>\n\n"
                    f"Age: <b>{duration_text(minutes)}</b>\n"
                    f"Scanned: <code>{scanned}</code>\n"
                    f"Deleted: <code>{deleted}</code>\n"
                    f"Failed: <code>{failed}</code>\n"
                    f"Remaining: <code>{remaining}</code>",
                    parse_mode="HTML",
                )

        if progress_message:

            await progress_message.edit_text(
                "🗑️ <b>PURGE COMPLETE</b>\n\n"
                f"Age: <b>{duration_text(minutes)}</b>\n"
                f"Scanned: <code>{scanned}</code>\n"
                f"Deleted: <code>{deleted}</code>\n"
                f"Failed: <code>{failed}</code>\n"
                f"Remaining: <code>{max(0, total - deleted - failed)}</code>",
                parse_mode="HTML",
                reply_markup=chat_keyboard(chat_id),
            )


# ============================================================
# PURGE WORKER
# ============================================================

async def purge_worker(chat_id):

    logger.info(
        "Purge worker started: %s",
        chat_id
    )

    while True:

        try:

            settings = await get_purge_settings(
                chat_id
            )

            if not settings.get(
                "enabled",
                False
            ):
                break

            minutes = int(
                settings.get(
                    "minutes",
                    60
                )
            )

            duplicate_delete = bool(
                settings.get(
                    "duplicate_delete",
                    False
                )
            )

            # Check once per minute.
            await asyncio.sleep(60)

            settings = await get_purge_settings(
                chat_id
            )

            if not settings.get(
                "enabled",
                False
            ):
                break

            await run_purge(
                chat_id,
                minutes,
                duplicate_delete,
                None,
            )

        except asyncio.CancelledError:

            raise

        except Exception:

            logger.exception(
                "Purge worker crashed"
            )

            await asyncio.sleep(
                30
            )

    purge_tasks.pop(
        chat_id,
        None
    )

    logger.info(
        "Purge worker stopped: %s",
        chat_id
    )


def ensure_purge_worker(chat_id):

    task = purge_tasks.get(
        chat_id
    )

    if task and not task.done():
        return

    purge_tasks[chat_id] = asyncio.create_task(
        purge_worker(chat_id)
    )


async def stop_purge_worker(chat_id):

    task = purge_tasks.get(
        chat_id
    )

    if task and not task.done():

        task.cancel()

        try:
            await task

        except asyncio.CancelledError:
            pass

    purge_tasks.pop(
        chat_id,
        None
    )


async def restore_purge_workers():

    docs = await purge_settings_col.find(
        {"enabled": True}
    ).to_list(
        length=10000
    )

    restored = 0

    for doc in docs:

        chat_id = int(
            doc["chat_id"]
        )

        chat = await get_chat(
            chat_id
        )

        if chat:

            ensure_purge_worker(
                chat_id
            )

            restored += 1

    logger.info(
        "Restored purge workers: %s",
        restored
    )


# ============================================================
# KEYBOARDS
# ============================================================

def main_keyboard():

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "👥 GROUPS / CHANNELS",
                    callback_data="chats"
                )
            ],
            [
                InlineKeyboardButton(
                    "👤 ADMIN MANAGEMENT",
                    callback_data="admins"
                )
            ],
        ]
    )


def main_back_keyboard():

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⬅️ MAIN",
                    callback_data="main"
                )
            ]
        ]
    )


async def chats_keyboard():

    chats = await get_all_chats()

    rows = []

    for chat in chats:

        chat_id = int(
            chat["chat_id"]
        )

        title = chat.get(
            "title",
            str(chat_id)
        )

        chat_type = chat.get(
            "chat_type",
            ""
        )

        icon = (
            "📢"
            if chat_type == "channel"
            else "👥"
        )

        rows.append(
            [
                InlineKeyboardButton(
                    f"{icon} {title[:55]}",
                    callback_data=f"chat:{chat_id}"
                )
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(
                "🔄 REFRESH",
                callback_data="chats"
            )
        ]
    )

    rows.append(
        [
            InlineKeyboardButton(
                "⬅️ MAIN",
                callback_data="main"
            )
        ]
    )

    return InlineKeyboardMarkup(
        rows
    )


def chat_keyboard(chat_id):

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "♻️ DCLEAN",
                    callback_data=f"dclean:{chat_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "🗑️ PURGE",
                    callback_data=f"purge:{chat_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "📊 STATUS",
                    callback_data=f"status:{chat_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "🔄 REFRESH",
                    callback_data=f"chat:{chat_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "⬅️ BACK",
                    callback_data="chats"
                )
            ],
        ]
    )


def purge_keyboard(
    chat_id,
    enabled,
    duplicate_delete
):

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "1h",
                    callback_data=f"pm:{chat_id}:60"
                ),
                InlineKeyboardButton(
                    "1¼h",
                    callback_data=f"pm:{chat_id}:75"
                ),
                InlineKeyboardButton(
                    "1½h",
                    callback_data=f"pm:{chat_id}:90"
                ),
            ],
            [
                InlineKeyboardButton(
                    "2h",
                    callback_data=f"pm:{chat_id}:120"
                ),
                InlineKeyboardButton(
                    "2½h",
                    callback_data=f"pm:{chat_id}:150"
                ),
                InlineKeyboardButton(
                    "2¾h",
                    callback_data=f"pm:{chat_id}:165"
                ),
            ],
            [
                InlineKeyboardButton(
                    "3h",
                    callback_data=f"pm:{chat_id}:180"
                )
            ],
            [
                InlineKeyboardButton(
                    "⚙️ CUSTOM MINUTES",
                    callback_data=f"custom:{chat_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    (
                        "♻️ DUPLICATE DELETE: ON"
                        if duplicate_delete
                        else
                        "♻️ DUPLICATE DELETE: OFF"
                    ),
                    callback_data=f"dup:{chat_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    (
                        "⏹️ DISABLE PURGE"
                        if enabled
                        else
                        "▶️ ENABLE PURGE"
                    ),
                    callback_data=f"toggle:{chat_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "▶️ RUN PURGE NOW",
                    callback_data=f"runpurge:{chat_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "⬅️ BACK",
                    callback_data=f"chat:{chat_id}"
                )
            ],
        ]
    )


# ============================================================
# UI HELPERS
# ============================================================

async def show_chat_list(query):

    await query.edit_message_text(
        "👥 <b>GROUPS / CHANNELS</b>\n\n"
        "Select a chat:",
        parse_mode="HTML",
        reply_markup=await chats_keyboard(),
    )


async def show_chat_panel(
    query,
    chat_id
):

    chat = await get_chat(
        chat_id
    )

    if not chat:

        await query.edit_message_text(
            "❌ Chat not found.",
            reply_markup=await chats_keyboard()
        )

        return

    settings = await get_purge_settings(
        chat_id
    )

    title = escape(
        chat.get(
            "title",
            str(chat_id)
        )
    )

    chat_type = escape(
        chat.get(
            "chat_type",
            "unknown"
        )
    )

    enabled = (
        "ON"
        if settings.get("enabled")
        else
        "OFF"
    )

    minutes = int(
        settings.get(
            "minutes",
            60
        )
    )

    duplicate = (
        "ON"
        if settings.get(
            "duplicate_delete"
        )
        else
        "OFF"
    )

    await query.edit_message_text(
        f"📂 <b>{title}</b>\n\n"
        f"Type: <code>{chat_type}</code>\n"
        f"ID: <code>{chat_id}</code>\n\n"
        f"🗑️ Purge: <b>{enabled}</b>\n"
        f"⏱️ Age: <b>{duration_text(minutes)}</b>\n"
        f"♻️ Duplicate Delete: <b>{duplicate}</b>",
        parse_mode="HTML",
        reply_markup=chat_keyboard(
            chat_id
        ),
    )


async def show_purge_menu(
    query,
    chat_id
):

    settings = await get_purge_settings(
        chat_id
    )

    enabled = bool(
        settings.get(
            "enabled",
            False
        )
    )

    duplicate_delete = bool(
        settings.get(
            "duplicate_delete",
            False
        )
    )

    minutes = int(
        settings.get(
            "minutes",
            60
        )
    )

    await query.edit_message_text(
        "🗑️ <b>PURGE SETTINGS</b>\n\n"
        f"Current age: <b>{duration_text(minutes)}</b>\n"
        f"Enabled: <b>{'ON' if enabled else 'OFF'}</b>\n"
        f"Duplicate Delete: <b>{'ON' if duplicate_delete else 'OFF'}</b>\n\n"
        "Select purge age:",
        parse_mode="HTML",
        reply_markup=purge_keyboard(
            chat_id,
            enabled,
            duplicate_delete
        ),
    )


# ============================================================
# ADMIN UI
# ============================================================

async def show_admin_panel(query):

    admins = await get_admins()

    lines = [
        "👤 <b>ADMIN MANAGEMENT</b>",
        "",
    ]

    for user_id in admins:

        source = (
            "ENV"
            if user_id in ENV_ADMIN_IDS
            else
            "MONGO"
        )

        lines.append(
            f"• <code>{user_id}</code> — {source}"
        )

    await query.edit_message_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "➕ ADD ADMIN",
                        callback_data="addadmin"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "❌ REMOVE ADMIN",
                        callback_data="removeadmin"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "🔄 REFRESH",
                        callback_data="admins"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ MAIN",
                        callback_data="main"
                    )
                ],
            ]
        ),
    )


# ============================================================
# /START
# ============================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user = update.effective_user

    if not user:
        return

    if not await is_admin(
        user.id
    ):
        return

    await update.message.reply_text(
        "🤖 <b>DCLEAN + PURGE ADMIN PANEL</b>\n\n"
        "Select an option:",
        parse_mode="HTML",
        reply_markup=main_keyboard(),
    )


async def cancel_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user = update.effective_user

    if not user:
        return

    if not await is_admin(
        user.id
    ):
        return

    input_states.pop(
        user.id,
        None
    )

    await update.message.reply_text(
        "❌ Cancelled.",
        reply_markup=main_keyboard()
    )


# ============================================================
# CALLBACK HANDLER
# ============================================================

async def callback_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    query = update.callback_query

    user = query.from_user

    if not await is_admin(
        user.id
    ):

        await query.answer(
            "Not authorized.",
            show_alert=True
        )

        return

    await query.answer()

    data = query.data or ""

    # MAIN
    if data == "main":

        await query.edit_message_text(
            "🤖 <b>DCLEAN + PURGE ADMIN PANEL</b>\n\n"
            "Select an option:",
            parse_mode="HTML",
            reply_markup=main_keyboard(),
        )

        return

    # CHAT LIST
    if data == "chats":

        await show_chat_list(
            query
        )

        return

    # ADMIN LIST
    if data == "admins":

        await show_admin_panel(
            query
        )

        return

    # ADD ADMIN
    if data == "addadmin":

        input_states[
            user.id
        ] = {
            "action": "add_admin"
        }

        await query.edit_message_text(
            "➕ <b>ADD ADMIN</b>\n\n"
            "Send Telegram numeric User ID.\n\n"
            "Example:\n"
            "<code>123456789</code>\n\n"
            "Use /cancel to cancel.",
            parse_mode="HTML",
        )

        return

    # REMOVE ADMIN
    if data == "removeadmin":

        input_states[
            user.id
        ] = {
            "action": "remove_admin"
        }

        await query.edit_message_text(
            "❌ <b>REMOVE ADMIN</b>\n\n"
            "Send Telegram numeric User ID.\n\n"
            "Use /cancel to cancel.",
            parse_mode="HTML",
        )

        return

    # CHAT PANEL
    if data.startswith("chat:"):

        chat_id = int(
            data.split(":")[1]
        )

        await show_chat_panel(
            query,
            chat_id
        )

        return

    # DCLEAN
    if data.startswith("dclean:"):

        chat_id = int(
            data.split(":")[1]
        )

        await run_dclean(
            chat_id,
            query.message
        )

        return

    # PURGE MENU
    if data.startswith("purge:"):

        chat_id = int(
            data.split(":")[1]
        )

        await show_purge_menu(
            query,
            chat_id
        )

        return

    # PURGE PRESET
    if data.startswith("pm:"):

        _, chat_id_text, minutes_text = data.split(":")

        chat_id = int(
            chat_id_text
        )

        minutes = int(
            minutes_text
        )

        if minutes not in PURGE_PRESETS.values():

            await query.answer(
                "Invalid duration.",
                show_alert=True
            )

            return

        await save_purge_settings(
            chat_id,
            minutes=minutes
        )

        settings = await get_purge_settings(
            chat_id
        )

        await query.edit_message_text(
            "🗑️ <b>PURGE SETTINGS SAVED</b>\n\n"
            f"Age: <b>{duration_text(minutes)}</b>\n"
            f"Enabled: <b>{'ON' if settings.get('enabled') else 'OFF'}</b>\n"
            f"Duplicate Delete: <b>{'ON' if settings.get('duplicate_delete') else 'OFF'}</b>",
            parse_mode="HTML",
            reply_markup=purge_keyboard(
                chat_id,
                bool(settings.get("enabled")),
                bool(settings.get("duplicate_delete")),
            ),
        )

        return

    # CUSTOM MINUTES
    if data.startswith("custom:"):

        chat_id = int(
            data.split(":")[1]
        )

        input_states[
            user.id
        ] = {
            "action": "custom_minutes",
            "chat_id": chat_id,
        }

        await query.edit_message_text(
            "⚙️ <b>CUSTOM PURGE MINUTES</b>\n\n"
            "Send the number of minutes.\n\n"
            "Example:\n"
            "<code>45</code>\n\n"
            "Use /cancel to cancel.",
            parse_mode="HTML",
        )

        return

    # DUPLICATE DELETE TOGGLE
    if data.startswith("dup:"):

        chat_id = int(
            data.split(":")[1]
        )

        settings = await get_purge_settings(
            chat_id
        )

        new_value = not bool(
            settings.get(
                "duplicate_delete",
                False
            )
        )

        await save_purge_settings(
            chat_id,
            duplicate_delete=new_value
        )

        await show_purge_menu(
            query,
            chat_id
        )

        return

    # PURGE ENABLE/DISABLE
    if data.startswith("toggle:"):

        chat_id = int(
            data.split(":")[1]
        )

        settings = await get_purge_settings(
            chat_id
        )

        new_enabled = not bool(
            settings.get(
                "enabled",
                False
            )
        )

        await save_purge_settings(
            chat_id,
            enabled=new_enabled
        )

        if new_enabled:

            ensure_purge_worker(
                chat_id
            )

        else:

            await stop_purge_worker(
                chat_id
            )

        await show_purge_menu(
            query,
            chat_id
        )

        return

    # RUN PURGE NOW
    if data.startswith("runpurge:"):

        chat_id = int(
            data.split(":")[1]
        )

        settings = await get_purge_settings(
            chat_id
        )

        await run_purge(
            chat_id,
            int(
                settings.get(
                    "minutes",
                    60
                )
            ),
            bool(
                settings.get(
                    "duplicate_delete",
                    False
                )
            ),
            query.message
        )

        return

    # STATUS
    if data.startswith("status:"):

        chat_id = int(
            data.split(":")[1]
        )

        settings = await get_purge_settings(
            chat_id
        )

        last_id = await get_last_dclean_id(
            chat_id
        )

        worker = purge_tasks.get(
            chat_id
        )

        worker_running = (
            worker is not None
            and not worker.done()
        )

        await query.edit_message_text(
            "📊 <b>STATUS</b>\n\n"
            f"♻️ DCLEAN Last ID: <code>{last_id}</code>\n"
            f"🗑️ Purge: <b>{'ON' if settings.get('enabled') else 'OFF'}</b>\n"
            f"⏱️ Purge Age: <b>{duration_text(int(settings.get('minutes', 60)))}</b>\n"
            f"♻️ Duplicate Delete: <b>{'ON' if settings.get('duplicate_delete') else 'OFF'}</b>\n"
            f"⚙️ Worker: <b>{'RUNNING' if worker_running else 'STOPPED'}</b>",
            parse_mode="HTML",
            reply_markup=chat_keyboard(
                chat_id
            ),
        )

        return


# ============================================================
# ADMIN TEXT INPUT
# ============================================================

async def text_input_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user = update.effective_user

    if not user:
        return

    if not await is_admin(
        user.id
    ):
        return

    state = input_states.get(
        user.id
    )

    if not state:
        return

    text = (
        update.message.text or ""
    ).strip()

    action = state.get(
        "action"
    )

    # --------------------------------------------------------
    # ADD ADMIN
    # --------------------------------------------------------

    if action == "add_admin":

        try:

            target_id = int(
                text
            )

        except ValueError:

            await update.message.reply_text(
                "❌ Invalid User ID.\n\n"
                "Send numeric Telegram User ID."
            )

            return

        await add_admin(
            target_id,
            user.id
        )

        input_states.pop(
            user.id,
            None
        )

        await update.message.reply_text(
            "✅ <b>ADMIN ADDED</b>\n\n"
            f"User ID: <code>{target_id}</code>",
            parse_mode="HTML",
            reply_markup=main_keyboard(),
        )

        return

    # --------------------------------------------------------
    # REMOVE ADMIN
    # --------------------------------------------------------

    if action == "remove_admin":

        try:

            target_id = int(
                text
            )

        except ValueError:

            await update.message.reply_text(
                "❌ Invalid User ID."
            )

            return

        if target_id in ENV_ADMIN_IDS:

            await update.message.reply_text(
                "❌ This admin is protected by "
                "the ADMIN_IDS Config Var."
            )

            return

        removed = await remove_admin(
            target_id
        )

        input_states.pop(
            user.id,
            None
        )

        if removed:

            text_out = (
                "✅ <b>ADMIN REMOVED</b>\n\n"
                f"User ID: <code>{target_id}</code>"
            )

        else:

            text_out = (
                "⚠️ Admin not found.\n\n"
                f"User ID: <code>{target_id}</code>"
            )

        await update.message.reply_text(
            text_out,
            parse_mode="HTML",
            reply_markup=main_keyboard(),
        )

        return

    # --------------------------------------------------------
    # CUSTOM PURGE
    # --------------------------------------------------------

    if action == "custom_minutes":

        try:

            minutes = int(
                text
            )

        except ValueError:

            await update.message.reply_text(
                "❌ Enter only a number.\n\n"
                "Example: <code>45</code>",
                parse_mode="HTML",
            )

            return

        if minutes < 1:

            await update.message.reply_text(
                "❌ Minutes must be at least 1."
            )

            return

        chat_id = int(
            state["chat_id"]
        )

        await save_purge_settings(
            chat_id,
            minutes=minutes
        )

        input_states.pop(
            user.id,
            None
        )

        await update.message.reply_text(
            "✅ <b>CUSTOM PURGE TIME SAVED</b>\n\n"
            f"Age: <b>{duration_text(minutes)}</b>",
            parse_mode="HTML",
            reply_markup=main_keyboard(),
        )

        return


# ============================================================
# BOT ADDED / REMOVED
# ============================================================

async def my_chat_member_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    member_update = update.my_chat_member

    if not member_update:
        return

    chat = member_update.chat

    if chat.type not in (
        "group",
        "supergroup",
        "channel",
    ):
        return

    new_status = (
        member_update
        .new_chat_member
        .status
    )

    active_statuses = {
        ChatMemberStatus.MEMBER,
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.OWNER,
        ChatMemberStatus.RESTRICTED,
    }

    if new_status in active_statuses:

        await register_chat(
            chat
        )

        logger.info(
            "Registered chat: %s (%s)",
            chat.title,
            chat.id
        )

    else:

        await deactivate_chat(
            chat.id
        )

        await stop_purge_worker(
            chat.id
        )

        logger.info(
            "Removed chat: %s (%s)",
            chat.title,
            chat.id
        )


# ============================================================
# MESSAGE WATCHER
# ============================================================

async def message_watcher(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    chat = update.effective_chat

    if not chat:
        return

    if chat.type not in (
        "group",
        "supergroup",
        "channel",
    ):
        return

    await register_chat(
        chat
    )


async def channel_post_watcher(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    chat = update.effective_chat

    if not chat:
        return

    if chat.type != "channel":
        return

    await register_chat(
        chat
    )


# ============================================================
# STARTUP
# ============================================================

async def post_init(
    application: Application
):

    global tg
    global telegram_bot

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN is missing"
        )

    if not MONGO_URI:
        raise RuntimeError(
            "MONGO_URI is missing"
        )

    if not API_ID:
        raise RuntimeError(
            "API_ID is missing"
        )

    if not API_HASH:
        raise RuntimeError(
            "API_HASH is missing"
        )

    if not STRING_SESSION:
        raise RuntimeError(
            "STRING_SESSION is missing"
        )

    await init_database()

    await seed_environment_admins()

    telegram_bot = application.bot

    # Existing StringSession.
    tg = TelegramClient(
        StringSession(
            STRING_SESSION
        ),
        API_ID,
        API_HASH,
    )

    await tg.connect()

    if not await tg.is_user_authorized():

        raise RuntimeError(
            "STRING_SESSION is not authorized."
        )

    me = await application.bot.get_me()

    logger.info(
        "Bot started: @%s",
        me.username
    )

    logger.info(
        "Telethon StringSession connected."
    )

    # Restore MongoDB-enabled purge workers.
    await restore_purge_workers()


# ============================================================
# SHUTDOWN
# ============================================================

async def post_shutdown(
    application: Application
):

    global tg
    global mongo

    for chat_id in list(
        purge_tasks.keys()
    ):

        try:

            await stop_purge_worker(
                chat_id
            )

        except Exception:

            logger.exception(
                "Worker shutdown error"
            )

    if tg:

        try:

            await tg.disconnect()

        except Exception:

            logger.exception(
                "Telethon disconnect error"
            )

    if mongo:

        mongo.close()


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

    # Commands
    application.add_handler(
        CommandHandler(
            "start",
            start_command
        )
    )

    application.add_handler(
        CommandHandler(
            "cancel",
            cancel_command
        )
    )

    # Bot added/removed from group/channel
    application.add_handler(
        ChatMemberHandler(
            my_chat_member_handler,
            ChatMemberHandler.MY_CHAT_MEMBER,
        )
    )

    # Channel posts
    application.add_handler(
        ChannelPostHandler(
            channel_post_watcher,
            filters.ALL,
        )
    )

    # Button callbacks
    application.add_handler(
        CallbackQueryHandler(
            callback_handler
        )
    )

    # Text input for Add Admin / Remove Admin /
    # Custom Purge Minutes
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            text_input_handler,
        )
    )

    # Register groups/supergroups.
    application.add_handler(
        MessageHandler(
            filters.ALL & ~filters.COMMAND,
            message_watcher,
        ),
        group=10,
    )

    application.run_polling(
        allowed_updates=[
            "message",
            "channel_post",
            "callback_query",
            "my_chat_member",
        ],
        drop_pending_updates=False,
    )


if __name__ == "__main__":
    main()
