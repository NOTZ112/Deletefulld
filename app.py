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
    ContextTypes,
    filters,
)

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger("DCLEAN-PURGE")


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
MONGO_URI = os.getenv("MONGO_URI", "").strip()

API_ID_RAW = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()

STRING_SESSION = os.getenv("STRING_SESSION", "").strip()

PIPE_LOG_CHAT_ID_RAW = os.getenv(
    "PIPE_LOG_CHAT_ID",
    ""
).strip()

DB_NAME = "duplicate_cleaner"

try:
    API_ID = int(API_ID_RAW)
except Exception:
    API_ID = 0

try:
    PIPE_LOG_CHAT_ID = int(PIPE_LOG_CHAT_ID_RAW)
except Exception:
    PIPE_LOG_CHAT_ID = 0


# ============================================================
# ADMIN IDS
# ============================================================

def parse_admin_ids(value: str):

    result = set()

    for item in value.split(","):

        item = item.strip()

        if not item:
            continue

        try:
            result.add(int(item))
        except ValueError:
            logger.warning(
                "Invalid ADMIN_IDS value: %s",
                item
            )

    return result


ENV_ADMIN_IDS = parse_admin_ids(
    os.getenv("ADMIN_IDS", "")
)


# ============================================================
# PURGE SETTINGS
# ============================================================

PURGE_PRESETS = {
    60: "1h",
    75: "1¼h",
    90: "1½h",
    120: "2h",
    150: "2½h",
    165: "2¾h",
    180: "3h",
}

PURGE_BATCH_SIZE = 100
DELETE_DELAY = 0.05
DCLEAN_PROGRESS_EVERY = 500


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

purge_tasks = {}
chat_locks = {}
input_states = {}


# ============================================================
# DATABASE INIT
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
        raise RuntimeError(
            "MONGO_URI is missing"
        )

    mongo = AsyncIOMotorClient(
        MONGO_URI
    )

    db = mongo[DB_NAME]

    chats_col = db["chats"]
    admins_col = db["admins"]
    dclean_state_col = db["dclean_state"]
    media_col = db["media_fingerprints"]
    pending_col = db["dclean_pending"]
    purge_settings_col = db["purge_settings"]

    await chats_col.create_index(
        [
            ("active", ASCENDING),
            ("title", ASCENDING)
        ]
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
        unique=True
    )

    await pending_col.create_index(
        [
            ("chat_id", ASCENDING),
            ("message_id", ASCENDING),
        ],
        unique=True
    )

    await purge_settings_col.create_index(
        "chat_id",
        unique=True
    )


# ============================================================
# ADMIN DATABASE
# ============================================================

async def seed_environment_admins():

    for user_id in ENV_ADMIN_IDS:

        await admins_col.update_one(
            {
                "user_id": user_id
            },
            {
                "$set": {
                    "user_id": user_id,
                    "source": "ENV",
                    "updated_at": datetime.now(
                        timezone.utc
                    ),
                },
                "$setOnInsert": {
                    "created_at": datetime.now(
                        timezone.utc
                    )
                },
            },
            upsert=True
        )


async def is_admin(user_id: int):

    if user_id in ENV_ADMIN_IDS:
        return True

    doc = await admins_col.find_one(
        {
            "user_id": int(user_id)
        }
    )

    return doc is not None


async def get_admins():

    docs = await admins_col.find(
        {}
    ).sort(
        "user_id",
        ASCENDING
    ).to_list(
        length=1000
    )

    return docs


async def add_admin(
    user_id: int,
    added_by: int
):

    await admins_col.update_one(
        {
            "user_id": int(user_id)
        },
        {
            "$set": {
                "user_id": int(user_id),
                "source": "MONGO",
                "added_by": int(added_by),
                "updated_at": datetime.now(
                    timezone.utc
                ),
            },
            "$setOnInsert": {
                "created_at": datetime.now(
                    timezone.utc
                )
            },
        },
        upsert=True
    )


async def remove_admin(user_id: int):

    # ENV admins cannot be removed
    if user_id in ENV_ADMIN_IDS:
        return False

    result = await admins_col.delete_one(
        {
            "user_id": int(user_id)
        }
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

    title = (
        chat.title
        or str(chat_id)
    )

    await chats_col.update_one(
        {
            "chat_id": chat_id
        },
        {
            "$set": {
                "chat_id": chat_id,
                "title": title,
                "chat_type": chat.type,
                "active": True,
                "updated_at": datetime.now(
                    timezone.utc
                ),
            },
            "$setOnInsert": {
                "created_at": datetime.now(
                    timezone.utc
                )
            },
        },
        upsert=True
    )

    await purge_settings_col.update_one(
        {
            "chat_id": chat_id
        },
        {
            "$setOnInsert": {
                "chat_id": chat_id,
                "enabled": False,
                "minutes": 60,
                "duplicate_delete": False,
                "created_at": datetime.now(
                    timezone.utc
                ),
            }
        },
        upsert=True
    )


async def deactivate_chat(chat_id):

    await chats_col.update_one(
        {
            "chat_id": int(chat_id)
        },
        {
            "$set": {
                "active": False,
                "updated_at": datetime.now(
                    timezone.utc
                ),
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
        {
            "active": True
        }
    ).sort(
        "title",
        ASCENDING
    ).to_list(
        length=1000
    )


# ============================================================
# CHAT LOCK
# ============================================================

def get_chat_lock(chat_id):

    chat_id = int(chat_id)

    if chat_id not in chat_locks:
        chat_locks[chat_id] = asyncio.Lock()

    return chat_locks[chat_id]


# ============================================================
# TELETHON
# ============================================================

async def telethon_call(func):

    while True:

        try:
            return await func()

        except FloodWaitError as e:

            logger.warning(
                "FloodWait: %s seconds",
                e.seconds
            )

            await asyncio.sleep(
                e.seconds + 1
            )


async def get_entity(chat_id):

    return await telethon_call(
        lambda: tg.get_entity(
            int(chat_id)
        )
    )


# ============================================================
# LOG
# ============================================================

async def send_log(text):

    if not PIPE_LOG_CHAT_ID:
        return

    try:

        await tg.send_message(
            PIPE_LOG_CHAT_ID,
            text
        )

    except Exception:

        logger.exception(
            "Failed to send log"
        )


# ============================================================
# MEDIA FINGERPRINT
# ============================================================

def get_media_fingerprint(message):

    photo = getattr(
        message,
        "photo",
        None
    )

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


# ============================================================
# DCLEAN STATE
# ============================================================

async def get_last_dclean_id(chat_id):

    doc = await dclean_state_col.find_one(
        {
            "chat_id": int(chat_id)
        }
    )

    if not doc:
        return 0

    return int(
        doc.get(
            "last_message_id",
            0
        )
    )


async def save_last_dclean_id(
    chat_id,
    message_id
):

    await dclean_state_col.update_one(
        {
            "chat_id": int(chat_id)
        },
        {
            "$set": {
                "chat_id": int(chat_id),
                "last_message_id": int(
                    message_id
                ),
                "updated_at": datetime.now(
                    timezone.utc
                ),
            }
        },
        upsert=True
    )


# ============================================================
# PENDING DUPLICATES
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
                "updated_at": datetime.now(
                    timezone.utc
                ),
            },
            "$setOnInsert": {
                "chat_id": int(chat_id),
                "message_id": int(message_id),
                "created_at": datetime.now(
                    timezone.utc
                ),
            },
        },
        upsert=True
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
        {
            "chat_id": int(chat_id)
        }
    ).sort(
        "message_id",
        ASCENDING
    ).to_list(
        length=10000
    )


# ============================================================
# DELETE
# ============================================================

async def delete_message_ids(
    entity,
    message_ids
):

    if not message_ids:
        return True

    try:

        await telethon_call(
            lambda: tg.delete_messages(
                entity,
                message_ids,
                revoke=True
            )
        )

        await asyncio.sleep(
            DELETE_DELAY
        )

        return True

    except RPCError as e:

        logger.error(
            "Telegram delete error: %s",
            e
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

    lock = get_chat_lock(
        chat_id
    )

    if lock.locked():

        await progress_message.edit_text(
            "♻️ <b>DCLEAN</b>\n\n"
            "⚠️ Another DCLEAN/PURGE operation "
            "is already running.",
            parse_mode="HTML",
            reply_markup=chat_keyboard(
                chat_id
            )
        )

        return

    async with lock:

        chat = await get_chat(
            chat_id
        )

        if not chat:

            await progress_message.edit_text(
                "❌ Chat not found.",
                reply_markup=main_back_keyboard()
            )

            return

        entity = await get_entity(
            chat_id
        )

        last_id = await get_last_dclean_id(
            chat_id
        )

        mode = (
            "FULL HISTORY"
            if last_id == 0
            else
            "NEW MESSAGES ONLY"
        )

        await progress_message.edit_text(
            "♻️ <b>DCLEAN STARTED</b>\n\n"
            f"Mode: <b>{mode}</b>\n"
            f"Last scanned ID: <code>{last_id}</code>\n\n"
            "Scanning...",
            parse_mode="HTML"
        )

        scanned = 0
        duplicates = []

        highest_id = last_id

        # ----------------------------------------------------
        # SCAN
        # ----------------------------------------------------

        async for message in tg.iter_messages(
            entity,
            min_id=last_id if last_id else 0,
            reverse=True
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

            message_id = int(
                message_id
            )

            highest_id = max(
                highest_id,
                message_id
            )

            scanned += 1

            fingerprint = get_media_fingerprint(
                message
            )

            if fingerprint:

                media_type, fp = fingerprint

                existing = await media_col.find_one(
                    {
                        "chat_id": int(chat_id),
                        "media_type": media_type,
                        "fingerprint": fp,
                    }
                )

                if existing:

                    original_id = int(
                        existing.get(
                            "message_id",
                            0
                        )
                    )

                    if original_id != message_id:

                        duplicates.append(
                            message_id
                        )

                        await add_pending(
                            chat_id,
                            message_id
                        )

                else:

                    await media_col.update_one(
                        {
                            "chat_id": int(chat_id),
                            "media_type": media_type,
                            "fingerprint": fp,
                        },
                        {
                            "$setOnInsert": {
                                "chat_id": int(chat_id),
                                "media_type": media_type,
                                "fingerprint": fp,
                                "message_id": message_id,
                                "created_at": datetime.now(
                                    timezone.utc
                                ),
                            }
                        },
                        upsert=True
                    )

            if (
                scanned %
                DCLEAN_PROGRESS_EVERY
                == 0
            ):

                await progress_message.edit_text(
                    "♻️ <b>DCLEAN SCANNING</b>\n\n"
                    f"Scanned: <code>{scanned}</code>\n"
                    f"Duplicates: <code>{len(duplicates)}</code>\n"
                    f"Last ID: <code>{highest_id}</code>",
                    parse_mode="HTML"
                )

        # ----------------------------------------------------
        # OLD FAILED DUPLICATES
        # ----------------------------------------------------

        pending = await get_pending(
            chat_id
        )

        pending_ids = [
            int(x["message_id"])
            for x in pending
        ]

        delete_list = sorted(
            set(
                duplicates +
                pending_ids
            )
        )

        total = len(
            delete_list
        )

        deleted = 0
        failed = 0

        # ----------------------------------------------------
        # DELETE DUPLICATES
        # ----------------------------------------------------

        for start in range(
            0,
            total,
            PURGE_BATCH_SIZE
        ):

            batch = delete_list[
                start:
                start + PURGE_BATCH_SIZE
            ]

            success = await delete_message_ids(
                entity,
                batch
            )

            if success:

                deleted += len(
                    batch
                )

                await remove_pending(
                    chat_id,
                    batch
                )

            else:

                failed += len(
                    batch
                )

            remaining = max(
                0,
                total - deleted - failed
            )

            await progress_message.edit_text(
                "♻️ <b>DCLEAN DELETE</b>\n\n"
                f"Scanned: <code>{scanned}</code>\n"
                f"Found: <code>{total}</code>\n"
                f"Deleted: <code>{deleted}</code>\n"
                f"Failed: <code>{failed}</code>\n"
                f"Remaining: <code>{remaining}</code>",
                parse_mode="HTML"
            )

        # ----------------------------------------------------
        # SAVE LAST SCANNED MESSAGE
        # ----------------------------------------------------

        if highest_id > last_id:

            await save_last_dclean_id(
                chat_id,
                highest_id
            )

        await progress_message.edit_text(
            "♻️ <b>DCLEAN COMPLETE</b>\n\n"
            f"Scanned: <code>{scanned}</code>\n"
            f"Duplicates: <code>{total}</code>\n"
            f"Deleted: <code>{deleted}</code>\n"
            f"Failed: <code>{failed}</code>\n"
            f"Last scanned ID: <code>{highest_id}</code>",
            parse_mode="HTML",
            reply_markup=chat_keyboard(
                chat_id
            )
        )


# ============================================================
# PURGE SETTINGS
# ============================================================

async def get_purge_settings(chat_id):

    doc = await purge_settings_col.find_one(
        {
            "chat_id": int(chat_id)
        }
    )

    if doc:
        return doc

    default = {
        "chat_id": int(chat_id),
        "enabled": False,
        "minutes": 60,
        "duplicate_delete": False,
        "created_at": datetime.now(
            timezone.utc
        ),
    }

    await purge_settings_col.insert_one(
        default
    )

    return default


async def save_purge_settings(
    chat_id,
    **values
):

    values[
        "updated_at"
    ] = datetime.now(
        timezone.utc
    )

    await purge_settings_col.update_one(
        {
            "chat_id": int(chat_id)
        },
        {
            "$set": values,
            "$setOnInsert": {
                "chat_id": int(chat_id)
            },
        },
        upsert=True
    )


def duration_text(minutes):

    if minutes in PURGE_PRESETS:
        return PURGE_PRESETS[minutes]

    minutes = int(minutes)

    if minutes >= 60:

        hours = minutes // 60
        remainder = minutes % 60

        if remainder:
            return f"{hours}h {remainder}m"

        return f"{hours}h"

    return f"{minutes}m"


# ============================================================
# PURGE
# ============================================================

async def run_purge(
    chat_id,
    minutes,
    duplicate_delete=False,
    progress_message=None
):

    lock = get_chat_lock(
        chat_id
    )

    if lock.locked():

        if progress_message:

            await progress_message.edit_text(
                "🗑️ <b>PURGE</b>\n\n"
                "⚠️ Another DCLEAN/PURGE operation "
                "is already running.",
                parse_mode="HTML",
                reply_markup=chat_keyboard(
                    chat_id
                )
            )

        return

    async with lock:

        chat = await get_chat(
            chat_id
        )

        if not chat:
            return

        entity = await get_entity(
            chat_id
        )

        cutoff = (
            datetime.now(
                timezone.utc
            )
            -
            timedelta(
                minutes=int(minutes)
            )
        )

        scanned = 0

        age_delete_ids = []

        if progress_message:

            await progress_message.edit_text(
                "🗑️ <b>PURGE STARTED</b>\n\n"
                f"Age: <b>{duration_text(minutes)}</b>\n"
                f"Duplicate Delete: "
                f"<b>{'ON' if duplicate_delete else 'OFF'}</b>\n\n"
                "Scanning...",
                parse_mode="HTML"
            )

        # ----------------------------------------------------
        # AGE SCAN
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

                age_delete_ids.append(
                    int(message_id)
                )

            else:

                # Newest -> oldest.
                # Once cutoff is reached, stop.
                break

            if (
                scanned %
                DCLEAN_PROGRESS_EVERY
                == 0
            ):

                if progress_message:

                    await progress_message.edit_text(
                        "🧹 <b>PURGE SCANNING</b>\n\n"
                        f"Scanned: <code>{scanned}</code>\n"
                        f"Old messages: "
                        f"<code>{len(age_delete_ids)}</code>",
                        parse_mode="HTML"
                    )

        # ----------------------------------------------------
        # DUPLICATE DELETE
        # ----------------------------------------------------

        duplicate_ids = []

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

                    msg_date = getattr(
                        message,
                        "date",
                        None
                    )

                    if msg_date:

                        if msg_date.tzinfo is None:

                            msg_date = msg_date.replace(
                                tzinfo=timezone.utc
                            )

                        # Keep newest copy.
                        # Delete duplicate only when
                        # it is older than selected age.
                        if msg_date < cutoff:

                            duplicate_ids.append(
                                int(message.id)
                            )

                else:

                    seen.add(
                        key
                    )

        delete_list = sorted(
            set(
                age_delete_ids +
                duplicate_ids
            ),
            reverse=True
        )

        total = len(
            delete_list
        )

        deleted = 0
        failed = 0

        # ----------------------------------------------------
        # DELETE
        # ----------------------------------------------------

        for start in range(
            0,
            total,
            PURGE_BATCH_SIZE
        ):

            batch = delete_list[
                start:
                start + PURGE_BATCH_SIZE
            ]

            success = await delete_message_ids(
                entity,
                batch
            )

            if success:

                deleted += len(
                    batch
                )

            else:

                failed += len(
                    batch
                )

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
                    parse_mode="HTML"
                )

        # ----------------------------------------------------
        # COMPLETE
        # ----------------------------------------------------

        if progress_message:

            await progress_message.edit_text(
                "🗑️ <b>PURGE COMPLETE</b>\n\n"
                f"Age: <b>{duration_text(minutes)}</b>\n"
                f"Scanned: <code>{scanned}</code>\n"
                f"Deleted: <code>{deleted}</code>\n"
                f"Failed: <code>{failed}</code>\n"
                f"Remaining: "
                f"<code>{max(0, total - deleted - failed)}</code>",
                parse_mode="HTML",
                reply_markup=chat_keyboard(
                    chat_id
                )
            )


# ============================================================
# PURGE WORKER
# ============================================================

async def purge_worker(chat_id):

    logger.info(
        "Purge worker started: %s",
        chat_id
    )

    try:

        while True:

            settings = await get_purge_settings(
                chat_id
            )

            if not settings.get(
                "enabled",
                False
            ):
                break

            await asyncio.sleep(
                60
            )

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

            try:

                await run_purge(
                    chat_id,
                    minutes,
                    duplicate_delete,
                    None
                )

            except Exception:

                logger.exception(
                    "Purge worker job failed"
                )

    except asyncio.CancelledError:

        raise

    except Exception:

        logger.exception(
            "Purge worker crashed"
        )

    finally:

        purge_tasks.pop(
            int(chat_id),
            None
        )

        logger.info(
            "Purge worker stopped: %s",
            chat_id
        )


def ensure_purge_worker(chat_id):

    chat_id = int(chat_id)

    task = purge_tasks.get(
        chat_id
    )

    if task and not task.done():
        return

    purge_tasks[
        chat_id
    ] = asyncio.create_task(
        purge_worker(
            chat_id
        )
    )


async def stop_purge_worker(chat_id):

    chat_id = int(chat_id)

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
        {
            "enabled": True
        }
    ).to_list(
        length=10000
    )

    count = 0

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

            count += 1

    logger.info(
        "Restored %s purge workers",
        count
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

    if not chats:

        rows.append(
            [
                InlineKeyboardButton(
                    "⚠️ NO CHATS REGISTERED",
                    callback_data="noop"
                )
            ]
        )

    for chat in chats:

        chat_id = int(
            chat["chat_id"]
        )

        title = (
            chat.get(
                "title",
                str(chat_id)
            )
        )

        chat_type = chat.get(
            "chat_type",
            ""
        )

        icon = (
            "📢"
            if chat_type == "channel"
            else
            "👥"
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
# CHAT PANEL
# ============================================================

async def show_chat_list(query):

    await query.edit_message_text(
        "👥 <b>GROUPS / CHANNELS</b>\n\n"
        "Select a group, supergroup or channel:",
        parse_mode="HTML",
        reply_markup=await chats_keyboard()
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

    purge_enabled = bool(
        settings.get(
            "enabled",
            False
        )
    )

    minutes = int(
        settings.get(
            "minutes",
            60
        )
    )

    duplicate = bool(
        settings.get(
            "duplicate_delete",
            False
        )
    )

    await query.edit_message_text(
        f"📂 <b>{title}</b>\n\n"
        f"Type: <code>{chat_type}</code>\n"
        f"ID: <code>{chat_id}</code>\n\n"
        f"🗑️ Purge: "
        f"<b>{'ON' if purge_enabled else 'OFF'}</b>\n"
        f"⏱️ Age: "
        f"<b>{duration_text(minutes)}</b>\n"
        f"♻️ Duplicate Delete: "
        f"<b>{'ON' if duplicate else 'OFF'}</b>",
        parse_mode="HTML",
        reply_markup=chat_keyboard(
            chat_id
        )
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
        f"Current age: "
        f"<b>{duration_text(minutes)}</b>\n"
        f"Enabled: "
        f"<b>{'ON' if enabled else 'OFF'}</b>\n"
        f"Duplicate Delete: "
        f"<b>{'ON' if duplicate_delete else 'OFF'}</b>\n\n"
        "Select purge age:",
        parse_mode="HTML",
        reply_markup=purge_keyboard(
            chat_id,
            enabled,
            duplicate_delete
        )
    )


# ============================================================
# ADMIN PANEL
# ============================================================

async def show_admin_panel(query):

    admins = await get_admins()

    lines = [
        "👤 <b>ADMIN MANAGEMENT</b>",
        "",
    ]

    if not admins:

        lines.append(
            "No admins found."
        )

    else:

        for admin in admins:

            user_id = int(
                admin["user_id"]
            )

            source = admin.get(
                "source",
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
        )
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
        "🤖 <b>DCLEAN + PURGE</b>\n\n"
        "👑 Admin Panel",
        parse_mode="HTML",
        reply_markup=main_keyboard()
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
# CALLBACK
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

    # --------------------------------------------------------
    # NOOP
    # --------------------------------------------------------

    if data == "noop":
        return

    # --------------------------------------------------------
    # MAIN
    # --------------------------------------------------------

    if data == "main":

        await query.edit_message_text(
            "🤖 <b>DCLEAN + PURGE</b>\n\n"
            "👑 Admin Panel",
            parse_mode="HTML",
            reply_markup=main_keyboard()
        )

        return

    # --------------------------------------------------------
    # CHATS
    # --------------------------------------------------------

    if data == "chats":

        await show_chat_list(
            query
        )

        return

    # --------------------------------------------------------
    # ADMINS
    # --------------------------------------------------------

    if data == "admins":

        await show_admin_panel(
            query
        )

        return

    # --------------------------------------------------------
    # ADD ADMIN
    # --------------------------------------------------------

    if data == "addadmin":

        # Only ENV admins can manage admins.
        if user.id not in ENV_ADMIN_IDS:

            await query.answer(
                "Only main ADMIN_IDS admins can manage admins.",
                show_alert=True
            )

            return

        input_states[
            user.id
        ] = {
            "action": "add_admin"
        }

        await query.edit_message_text(
            "➕ <b>ADD ADMIN</b>\n\n"
            "Send the Telegram numeric User ID.\n\n"
            "Example:\n"
            "<code>123456789</code>\n\n"
            "/cancel to cancel.",
            parse_mode="HTML"
        )

        return

    # --------------------------------------------------------
    # REMOVE ADMIN
    # --------------------------------------------------------

    if data == "removeadmin":

        if user.id not in ENV_ADMIN_IDS:

            await query.answer(
                "Only main ADMIN_IDS admins can manage admins.",
                show_alert=True
            )

            return

        input_states[
            user.id
        ] = {
            "action": "remove_admin"
        }

        await query.edit_message_text(
            "❌ <b>REMOVE ADMIN</b>\n\n"
            "Send Telegram numeric User ID.\n\n"
            "/cancel to cancel.",
            parse_mode="HTML"
        )

        return

    # --------------------------------------------------------
    # CHAT
    # --------------------------------------------------------

    if data.startswith("chat:"):

        chat_id = int(
            data.split(
                ":",
                1
            )[1]
        )

        await show_chat_panel(
            query,
            chat_id
        )

        return

    # --------------------------------------------------------
    # DCLEAN
    # --------------------------------------------------------

    if data.startswith("dclean:"):

        chat_id = int(
            data.split(
                ":",
                1
            )[1]
        )

        await run_dclean(
            chat_id,
            query.message
        )

        return

    # --------------------------------------------------------
    # PURGE MENU
    # --------------------------------------------------------

    if data.startswith("purge:"):

        chat_id = int(
            data.split(
                ":",
                1
            )[1]
        )

        await show_purge_menu(
            query,
            chat_id
        )

        return

    # --------------------------------------------------------
    # PURGE PRESET
    # --------------------------------------------------------

    if data.startswith("pm:"):

        parts = data.split(":")

        if len(parts) != 3:
            return

        chat_id = int(
            parts[1]
        )

        minutes = int(
            parts[2]
        )

        if minutes not in PURGE_PRESETS:

            await query.answer(
                "Invalid duration.",
                show_alert=True
            )

            return

        await save_purge_settings(
            chat_id,
            minutes=minutes
        )

        await show_purge_menu(
            query,
            chat_id
        )

        return

    # --------------------------------------------------------
    # CUSTOM MINUTES
    # --------------------------------------------------------

    if data.startswith("custom:"):

        chat_id = int(
            data.split(
                ":",
                1
            )[1]
        )

        input_states[
            user.id
        ] = {
            "action": "custom_minutes",
            "chat_id": chat_id,
        }

        await query.edit_message_text(
            "⚙️ <b>CUSTOM PURGE</b>\n\n"
            "Send minutes.\n\n"
            "Examples:\n"
            "<code>5</code>\n"
            "<code>45</code>\n"
            "<code>200</code>\n\n"
            "/cancel to cancel.",
            parse_mode="HTML"
        )

        return

    # --------------------------------------------------------
    # DUPLICATE TOGGLE
    # --------------------------------------------------------

    if data.startswith("dup:"):

        chat_id = int(
            data.split(
                ":",
                1
            )[1]
        )

        settings = await get_purge_settings(
            chat_id
        )

        current = bool(
            settings.get(
                "duplicate_delete",
                False
            )
        )

        await save_purge_settings(
            chat_id,
            duplicate_delete=not current
        )

        await show_purge_menu(
            query,
            chat_id
        )

        return

    # --------------------------------------------------------
    # PURGE ENABLE/DISABLE
    # --------------------------------------------------------

    if data.startswith("toggle:"):

        chat_id = int(
            data.split(
                ":",
                1
            )[1]
        )

        settings = await get_purge_settings(
            chat_id
        )

        current = bool(
            settings.get(
                "enabled",
                False
            )
        )

        new_value = not current

        await save_purge_settings(
            chat_id,
            enabled=new_value
        )

        if new_value:

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

    # --------------------------------------------------------
    # RUN PURGE NOW
    # --------------------------------------------------------

    if data.startswith("runpurge:"):

        chat_id = int(
            data.split(
                ":",
                1
            )[1]
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

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    if data.startswith("status:"):

        chat_id = int(
            data.split(
                ":",
                1
            )[1]
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
            f"♻️ Last DCLEAN ID: "
            f"<code>{last_id}</code>\n"
            f"🗑️ Purge: "
            f"<b>{'ON' if settings.get('enabled') else 'OFF'}</b>\n"
            f"⏱️ Age: "
            f"<b>{duration_text(int(settings.get('minutes', 60)))}</b>\n"
            f"♻️ Duplicate Delete: "
            f"<b>{'ON' if settings.get('duplicate_delete') else 'OFF'}</b>\n"
            f"⚙️ Worker: "
            f"<b>{'RUNNING' if worker_running else 'STOPPED'}</b>",
            parse_mode="HTML",
            reply_markup=chat_keyboard(
                chat_id
            )
        )

        return


# ============================================================
# TEXT INPUT
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
        update.message.text
        or ""
    ).strip()

    action = state.get(
        "action"
    )

    # --------------------------------------------------------
    # ADD ADMIN
    # --------------------------------------------------------

    if action == "add_admin":

        if user.id not in ENV_ADMIN_IDS:

            input_states.pop(
                user.id,
                None
            )

            await update.message.reply_text(
                "❌ Only main ENV admins can add admins."
            )

            return

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
            reply_markup=main_keyboard()
        )

        return

    # --------------------------------------------------------
    # REMOVE ADMIN
    # --------------------------------------------------------

    if action == "remove_admin":

        if user.id not in ENV_ADMIN_IDS:

            input_states.pop(
                user.id,
                None
            )

            await update.message.reply_text(
                "❌ Only main ENV admins can remove admins."
            )

            return

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
                "ADMIN_IDS Config Var."
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

            response = (
                "✅ <b>ADMIN REMOVED</b>\n\n"
                f"User ID: <code>{target_id}</code>"
            )

        else:

            response = (
                "⚠️ Admin not found.\n\n"
                f"User ID: <code>{target_id}</code>"
            )

        await update.message.reply_text(
            response,
            parse_mode="HTML",
            reply_markup=main_keyboard()
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
                parse_mode="HTML"
            )

            return

        if minutes < 1:

            await update.message.reply_text(
                "❌ Minutes must be greater than 0."
            )

            return

        if minutes > 43200:

            await update.message.reply_text(
                "❌ Maximum allowed is 43200 minutes."
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
            "✅ <b>PURGE TIME SAVED</b>\n\n"
            f"Age: <b>{duration_text(minutes)}</b>",
            parse_mode="HTML",
            reply_markup=main_keyboard()
        )

        return


# ============================================================
# BOT MEMBERSHIP
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
            "Deactivated chat: %s (%s)",
            chat.title,
            chat.id
        )


# ============================================================
# GROUP/SUPERGROUP MESSAGE WATCHER
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
    ):
        return

    await register_chat(
        chat
    )


# ============================================================
# CHANNEL POST WATCHER
#
# IMPORTANT:
# PTB 22.3 has no ChannelPostHandler.
#
# We use MessageHandler with UpdateType filtering.
# ============================================================

async def channel_post_watcher(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not update.channel_post:
        return

    chat = update.channel_post.chat

    if not chat:
        return

    if chat.type != "channel":
        return

    await register_chat(
        chat
    )


# ============================================================
# CHANNEL POST FILTER
# ============================================================

class ChannelPostFilter(filters.MessageFilter):

    def filter(self, message):

        return False


# ============================================================
# STARTUP
# ============================================================

async def post_init(
    application: Application
):

    global tg

    # --------------------------------------------------------
    # CHECK CONFIG
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # DATABASE
    # --------------------------------------------------------

    await init_database()

    await seed_environment_admins()

    # --------------------------------------------------------
    # TELETHON
    # --------------------------------------------------------

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

    me = await tg.get_me()

    logger.info(
        "Telethon connected: %s",
        getattr(
            me,
            "username",
            None
        )
    )

    # --------------------------------------------------------
    # RESTORE PURGE WORKERS
    # --------------------------------------------------------

    await restore_purge_workers()

    logger.info(
        "Bot startup completed."
    )


# ============================================================
# SHUTDOWN
# ============================================================

async def post_shutdown(
    application: Application
):

    global tg
    global mongo

    logger.info(
        "Shutting down..."
    )

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

    logger.info(
        "Shutdown complete."
    )


# ============================================================
# CHANNEL POST UPDATE HANDLER
#
# PTB 22.3 doesn't have ChannelPostHandler.
# TypeHandler is not used globally because that would
# intercept every update.
#
# Instead we use a MessageHandler whose filter checks
# update.channel_post through a custom Update filter.
# ============================================================

from telegram.ext import BaseHandler


class ChannelPostUpdateHandler(BaseHandler):

    def check_update(self, update):

        if isinstance(
            update,
            Update
        ):

            return bool(
                update.channel_post
            )

        return False

    async def handle_update(
        self,
        update,
        application,
        check_result,
        context
    ):

        await channel_post_watcher(
            update,
            context
        )


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

    # --------------------------------------------------------
    # COMMANDS
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # BOT MEMBERSHIP
    # --------------------------------------------------------

    application.add_handler(
        ChatMemberHandler(
            my_chat_member_handler,
            ChatMemberHandler.MY_CHAT_MEMBER
        )
    )

    # --------------------------------------------------------
    # CHANNEL POSTS
    # --------------------------------------------------------

    application.add_handler(
        ChannelPostUpdateHandler(),
        group=1
    )

    # --------------------------------------------------------
    # CALLBACKS
    # --------------------------------------------------------

    application.add_handler(
        CallbackQueryHandler(
            callback_handler
        )
    )

    # --------------------------------------------------------
    # ADMIN TEXT INPUT
    # --------------------------------------------------------

    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            text_input_handler
        ),
        group=0
    )

    # --------------------------------------------------------
    # GROUP / SUPERGROUP REGISTRATION
    # --------------------------------------------------------

    application.add_handler(
        MessageHandler(
            filters.ALL & ~filters.COMMAND,
            message_watcher
        ),
        group=10
    )

    # --------------------------------------------------------
    # RUN
    # --------------------------------------------------------

    application.run_polling(
        allowed_updates=[
            "message",
            "channel_post",
            "callback_query",
            "my_chat_member",
        ],
        drop_pending_updates=False
    )


if __name__ == "__main__":
    main()
