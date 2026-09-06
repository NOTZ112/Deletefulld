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

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ChatMemberStatus
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ChatMemberHandler,
    MessageHandler,
    TypeHandler,
    ContextTypes,
    filters,
)


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
MONGO_URI = os.getenv("MONGO_URI", "")

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
STRING_SESSION = os.getenv("STRING_SESSION", "")

PIPE_LOG_CHAT_ID = os.getenv("PIPE_LOG_CHAT_ID", "")

DB_NAME = "duplicate_cleaner"


def parse_admin_ids(value):
    result = set()

    for item in value.split(","):
        item = item.strip()
        if not item:
            continue

        try:
            result.add(int(item))
        except ValueError:
            pass

    return result


ENV_ADMIN_IDS = parse_admin_ids(os.getenv("ADMIN_IDS", ""))


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger("duplicate-cleaner")


# ============================================================
# GLOBALS
# ============================================================

mongo = None
db = None
telethon_client = None

chats_col = None
admins_col = None
dclean_state_col = None
media_col = None
pending_col = None
purge_settings_col = None

purge_tasks = {}

admin_input_state = {}


PURGE_PRESETS = {
    60: "1h",
    75: "1¼h",
    90: "1½h",
    120: "2h",
    150: "2½h",
    165: "2¾h",
    180: "3h",
}


# ============================================================
# BASIC HELPERS
# ============================================================

def now_utc():
    return datetime.now(timezone.utc)


def is_admin(user_id):
    if user_id in ENV_ADMIN_IDS:
        return True

    return False


async def is_db_admin(user_id):
    if user_id in ENV_ADMIN_IDS:
        return True

    doc = await admins_col.find_one({"user_id": user_id})

    return bool(doc)


async def get_admin_list():
    ids = set(ENV_ADMIN_IDS)

    cursor = admins_col.find({})

    async for doc in cursor:
        uid = doc.get("user_id")

        if isinstance(uid, int):
            ids.add(uid)

    return sorted(ids)


# ============================================================
# SAFE MONGO INDEX MIGRATION
# ============================================================

async def ensure_index_safe(
    collection,
    key,
    name,
    unique=False,
):
    """
    Safely creates an index.

    IMPORTANT:
    If an old index with the same name has a conflicting definition,
    ONLY THE INDEX is dropped.

    Documents are NEVER deleted.
    """

    indexes = await collection.index_information()

    existing = indexes.get(name)

    if existing:
        existing_key = existing.get("key")
        existing_unique = bool(existing.get("unique", False))

        wanted_key = list(key)

        if (
            existing_key == wanted_key
            and existing_unique == unique
        ):
            return

        logger.warning(
            "Migrating conflicting index %s on %s",
            name,
            collection.name,
        )

        try:
            await collection.drop_index(name)
            logger.info(
                "Dropped old index %s. Documents preserved.",
                name,
            )
        except Exception as e:
            logger.warning(
                "Could not drop index %s: %s",
                name,
                e,
            )

    try:
        await collection.create_index(
            key,
            name=name,
            unique=unique,
        )
    except Exception as e:
        logger.warning(
            "Index creation skipped for %s: %s",
            name,
            e,
        )


async def setup_indexes():
    """
    Safe index setup.

    No collection is deleted.
    No documents are deleted.
    """

    await ensure_index_safe(
        chats_col,
        [("chat_id", ASCENDING)],
        "chat_id_1",
        False,
    )

    await ensure_index_safe(
        admins_col,
        [("user_id", ASCENDING)],
        "user_id_1",
        False,
    )

    await ensure_index_safe(
        dclean_state_col,
        [("chat_id", ASCENDING)],
        "chat_id_1",
        False,
    )

    await ensure_index_safe(
        media_col,
        [
            ("chat_id", ASCENDING),
            ("fingerprint", ASCENDING),
        ],
        "chat_fingerprint_1",
        False,
    )

    await ensure_index_safe(
        pending_col,
        [
            ("chat_id", ASCENDING),
            ("message_id", ASCENDING),
        ],
        "chat_message_1",
        False,
    )

    await ensure_index_safe(
        purge_settings_col,
        [("chat_id", ASCENDING)],
        "purge_chat_id_1",
        False,
    )


# ============================================================
# ADMIN MANAGEMENT
# ============================================================

async def add_admin(user_id):
    if user_id in ENV_ADMIN_IDS:
        return False

    await admins_col.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "user_id": user_id,
                "created_at": now_utc(),
            }
        },
        upsert=True,
    )

    return True


async def remove_admin(user_id):
    if user_id in ENV_ADMIN_IDS:
        return False

    result = await admins_col.delete_one(
        {"user_id": user_id}
    )

    return result.deleted_count > 0


# ============================================================
# CHAT DATABASE
# ============================================================

async def register_chat(
    chat_id,
    title,
    chat_type,
):
    await chats_col.update_one(
        {"chat_id": chat_id},
        {
            "$set": {
                "chat_id": chat_id,
                "title": title or str(chat_id),
                "type": chat_type,
                "active": True,
                "updated_at": now_utc(),
            },
            "$setOnInsert": {
                "created_at": now_utc(),
            },
        },
        upsert=True,
    )

    if chat_type in ("group", "supergroup"):
        await purge_settings_col.update_one(
            {"chat_id": chat_id},
            {
                "$setOnInsert": {
                    "chat_id": chat_id,
                    "enabled": False,
                    "minutes": 60,
                    "delete_duplicates": False,
                    "created_at": now_utc(),
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
                "updated_at": now_utc(),
            }
        },
    )


async def get_chat(chat_id):
    return await chats_col.find_one(
        {"chat_id": chat_id}
    )


async def get_all_chats():
    cursor = chats_col.find(
        {"active": True}
    ).sort("title", ASCENDING)

    return await cursor.to_list(length=1000)


# ============================================================
# TELETHON SAFE CALL
# ============================================================

async def tg_call(func, *args, **kwargs):
    while True:
        try:
            return await func(*args, **kwargs)

        except FloodWaitError as e:
            wait_time = int(e.seconds) + 2

            logger.warning(
                "FloodWait: sleeping %s seconds",
                wait_time,
            )

            await asyncio.sleep(wait_time)

        except RPCError as e:
            logger.error(
                "Telegram RPC error: %s",
                e,
            )
            return None

        except Exception as e:
            logger.exception(
                "Telegram call failed: %s",
                e,
            )
            return None


# ============================================================
# LOGGING
# ============================================================

async def send_log(text):
    if not PIPE_LOG_CHAT_ID:
        return

    try:
        chat_id = int(PIPE_LOG_CHAT_ID)

        await tg_call(
            telethon_client.send_message,
            chat_id,
            text,
        )

    except Exception:
        logger.exception("Log sending failed")


# ============================================================
# MEDIA FINGERPRINT
# ============================================================

def get_media_fingerprint(message):
    if not message:
        return None

    try:
        if getattr(message, "photo", None):
            photo = message.photo

            if getattr(photo, "id", None):
                return f"photo:{photo.id}"

        document = getattr(message, "document", None)

        if document:
            if getattr(document, "id", None):
                return f"document:{document.id}"

    except Exception:
        logger.exception(
            "Fingerprint generation failed"
        )

    return None


# ============================================================
# DCLEAN STATE
# ============================================================

async def get_last_dclean_id(chat_id):
    doc = await dclean_state_col.find_one(
        {"chat_id": chat_id}
    )

    if not doc:
        return 0

    return int(doc.get("last_message_id", 0) or 0)


async def save_last_dclean_id(
    chat_id,
    message_id,
):
    await dclean_state_col.update_one(
        {"chat_id": chat_id},
        {
            "$set": {
                "chat_id": chat_id,
                "last_message_id": int(message_id),
                "updated_at": now_utc(),
            }
        },
        upsert=True,
    )


# ============================================================
# PENDING DELETE
# ============================================================

async def add_pending_delete(
    chat_id,
    message_id,
    fingerprint=None,
):
    await pending_col.update_one(
        {
            "chat_id": chat_id,
            "message_id": message_id,
        },
        {
            "$set": {
                "chat_id": chat_id,
                "message_id": message_id,
                "fingerprint": fingerprint,
                "updated_at": now_utc(),
            },
            "$setOnInsert": {
                "created_at": now_utc(),
            },
        },
        upsert=True,
    )


async def remove_pending_delete(
    chat_id,
    message_id,
):
    await pending_col.delete_one(
        {
            "chat_id": chat_id,
            "message_id": message_id,
        }
    )


async def retry_pending_deletes(
    entity,
    chat_id,
):
    cursor = pending_col.find(
        {"chat_id": chat_id}
    )

    pending = await cursor.to_list(length=5000)

    deleted = 0

    for item in pending:
        message_id = item.get("message_id")

        if not message_id:
            continue

        try:
            result = await tg_call(
                telethon_client.delete_messages,
                entity,
                [message_id],
            )

            if result is not None:
                await remove_pending_delete(
                    chat_id,
                    message_id,
                )

                deleted += 1

        except Exception:
            logger.exception(
                "Pending delete failed: %s",
                message_id,
            )

    return deleted


# ============================================================
# DCLEAN
# ============================================================

async def run_dclean(
    chat_id,
    status_message=None,
):
    chat = await get_chat(chat_id)

    if not chat:
        return "❌ Chat not registered."

    last_id = await get_last_dclean_id(chat_id)

    scanned = 0
    duplicates = 0
    deleted = 0
    failed = 0
    highest_id = last_id

    try:
        entity = await telethon_client.get_entity(
            chat_id
        )
    except Exception as e:
        logger.exception(
            "Entity error: %s",
            e,
        )
        return "❌ Could not access this chat with the Telegram session."

    # Retry previous failed deletions first.
    try:
        retry_count = await retry_pending_deletes(
            entity,
            chat_id,
        )

        deleted += retry_count

    except Exception:
        logger.exception(
            "Pending retry failed"
        )

    seen = set()

    # Load previously known fingerprints.
    cursor = media_col.find(
        {"chat_id": chat_id},
        {
            "fingerprint": 1,
            "_id": 0,
        },
    )

    async for doc in cursor:
        fp = doc.get("fingerprint")

        if fp:
            seen.add(fp)

    try:
        async for message in telethon_client.iter_messages(
            entity,
            min_id=last_id,
            reverse=True,
        ):
            scanned += 1

            message_id = getattr(
                message,
                "id",
                0,
            )

            if message_id > highest_id:
                highest_id = message_id

            fingerprint = get_media_fingerprint(
                message
            )

            if fingerprint:
                if fingerprint in seen:
                    duplicates += 1

                    try:
                        result = await tg_call(
                            telethon_client.delete_messages,
                            entity,
                            [message_id],
                        )

                        if result is not None:
                            deleted += 1

                        else:
                            failed += 1

                            await add_pending_delete(
                                chat_id,
                                message_id,
                                fingerprint,
                            )

                    except Exception:
                        failed += 1

                        await add_pending_delete(
                            chat_id,
                            message_id,
                            fingerprint,
                        )

                else:
                    seen.add(fingerprint)

                    await media_col.update_one(
                        {
                            "chat_id": chat_id,
                            "fingerprint": fingerprint,
                        },
                        {
                            "$set": {
                                "chat_id": chat_id,
                                "fingerprint": fingerprint,
                                "message_id": message_id,
                                "updated_at": now_utc(),
                            },
                            "$setOnInsert": {
                                "created_at": now_utc(),
                            },
                        },
                        upsert=True,
                    )

            if scanned % 100 == 0:
                await save_last_dclean_id(
                    chat_id,
                    highest_id,
                )

                if status_message:
                    try:
                        await status_message.edit_text(
                            "♻️ <b>DCLEAN RUNNING</b>\n\n"
                            f"📖 Scanned: <b>{scanned}</b>\n"
                            f"♻️ Duplicates: <b>{duplicates}</b>\n"
                            f"🗑️ Deleted: <b>{deleted}</b>\n"
                            f"⏳ Pending: <b>{failed}</b>\n"
                            f"🆔 Last ID: <b>{highest_id}</b>",
                            parse_mode="HTML",
                        )
                    except Exception:
                        pass

    except Exception as e:
        logger.exception(
            "DCLEAN error: %s",
            e,
        )

    await save_last_dclean_id(
        chat_id,
        highest_id,
    )

    return (
        "♻️ <b>DCLEAN COMPLETE</b>\n\n"
        f"📖 Scanned: <b>{scanned}</b>\n"
        f"♻️ Duplicates: <b>{duplicates}</b>\n"
        f"🗑️ Deleted: <b>{deleted}</b>\n"
        f"⏳ Pending: <b>{failed}</b>\n"
        f"🆔 Last ID: <b>{highest_id}</b>"
    )


# ============================================================
# PURGE SETTINGS
# ============================================================

async def get_purge_settings(chat_id):
    doc = await purge_settings_col.find_one(
        {"chat_id": chat_id}
    )

    if not doc:
        return {
            "chat_id": chat_id,
            "enabled": False,
            "minutes": 60,
            "delete_duplicates": False,
        }

    return {
        "chat_id": chat_id,
        "enabled": bool(doc.get("enabled", False)),
        "minutes": int(doc.get("minutes", 60)),
        "delete_duplicates": bool(
            doc.get("delete_duplicates", False)
        ),
    }


async def save_purge_settings(
    chat_id,
    enabled=None,
    minutes=None,
    delete_duplicates=None,
):
    update = {
        "updated_at": now_utc(),
    }

    if enabled is not None:
        update["enabled"] = bool(enabled)

    if minutes is not None:
        update["minutes"] = int(minutes)

    if delete_duplicates is not None:
        update["delete_duplicates"] = bool(
            delete_duplicates
        )

    await purge_settings_col.update_one(
        {"chat_id": chat_id},
        {
            "$set": update,
            "$setOnInsert": {
                "chat_id": chat_id,
                "created_at": now_utc(),
            },
        },
        upsert=True,
    )


# ============================================================
# PURGE
# ============================================================

async def run_purge_once(
    chat_id,
    status_message=None,
):
    chat = await get_chat(chat_id)

    if not chat:
        return

    # HARD BLOCK CHANNELS
    if chat.get("type") not in (
        "group",
        "supergroup",
    ):
        return

    settings = await get_purge_settings(
        chat_id
    )

    minutes = settings["minutes"]

    cutoff = now_utc() - timedelta(
        minutes=minutes
    )

    try:
        entity = await telethon_client.get_entity(
            chat_id
        )
    except Exception:
        logger.exception(
            "PURGE entity error"
        )
        return

    scanned = 0
    deleted = 0
    duplicate_deleted = 0

    seen = set()

    try:
        async for message in telethon_client.iter_messages(
            entity,
            reverse=False,
        ):
            scanned += 1

            message_date = getattr(
                message,
                "date",
                None,
            )

            if message_date:
                if message_date.tzinfo is None:
                    message_date = message_date.replace(
                        tzinfo=timezone.utc
                    )

                if message_date >= cutoff:
                    continue

            fingerprint = get_media_fingerprint(
                message
            )

            should_delete = True

            if settings["delete_duplicates"]:
                if fingerprint:
                    if fingerprint in seen:
                        duplicate_deleted += 1
                    else:
                        seen.add(fingerprint)

            try:
                result = await tg_call(
                    telethon_client.delete_messages,
                    entity,
                    [message.id],
                )

                if result is not None:
                    deleted += 1

            except Exception:
                await add_pending_delete(
                    chat_id,
                    message.id,
                    fingerprint,
                )

            if scanned % 50 == 0:
                if status_message:
                    try:
                        await status_message.edit_text(
                            "🗑️ <b>PURGE RUNNING</b>\n\n"
                            f"📖 Scanned: <b>{scanned}</b>\n"
                            f"🗑️ Deleted: <b>{deleted}</b>\n"
                            f"♻️ Duplicate: <b>{duplicate_deleted}</b>",
                            parse_mode="HTML",
                        )
                    except Exception:
                        pass

    except Exception:
        logger.exception(
            "PURGE failed"
        )

    return {
        "scanned": scanned,
        "deleted": deleted,
        "duplicates": duplicate_deleted,
    }


# ============================================================
# PURGE WORKER
# ============================================================

async def purge_worker(chat_id):
    logger.info(
        "PURGE worker started: %s",
        chat_id,
    )

    try:
        while True:
            chat = await get_chat(chat_id)

            if not chat:
                break

            # Never run PURGE in channels.
            if chat.get("type") not in (
                "group",
                "supergroup",
            ):
                break

            settings = await get_purge_settings(
                chat_id
            )

            if not settings["enabled"]:
                break

            try:
                result = await run_purge_once(
                    chat_id
                )

                if result:
                    logger.info(
                        "PURGE %s: %s",
                        chat_id,
                        result,
                    )

            except Exception:
                logger.exception(
                    "PURGE worker error"
                )

            await asyncio.sleep(60)

    except asyncio.CancelledError:
        pass

    finally:
        purge_tasks.pop(chat_id, None)

        logger.info(
            "PURGE worker stopped: %s",
            chat_id,
        )


async def start_purge_worker(chat_id):
    existing = purge_tasks.get(chat_id)

    if existing and not existing.done():
        return

    task = asyncio.create_task(
        purge_worker(chat_id)
    )

    purge_tasks[chat_id] = task


async def stop_purge_worker(chat_id):
    task = purge_tasks.pop(
        chat_id,
        None,
    )

    if task:
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass


async def restore_purge_workers():
    cursor = purge_settings_col.find(
        {"enabled": True}
    )

    async for settings in cursor:
        chat_id = settings.get("chat_id")

        if not chat_id:
            continue

        chat = await get_chat(chat_id)

        if not chat:
            continue

        # NEVER restore PURGE for channels.
        if chat.get("type") not in (
            "group",
            "supergroup",
        ):
            continue

        await start_purge_worker(
            chat_id
        )


# ============================================================
# TELEGRAM BOT UI
# ============================================================

def main_keyboard():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📋 CHATS",
                    callback_data="chats",
                ),
            ],
            [
                InlineKeyboardButton(
                    "👑 ADMINS",
                    callback_data="admins",
                ),
            ],
        ]
    )


def admin_keyboard():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "➕ ADD ADMIN",
                    callback_data="admin_add",
                ),
            ],
            [
                InlineKeyboardButton(
                    "👥 ADMIN LIST",
                    callback_data="admin_list",
                ),
            ],
            [
                InlineKeyboardButton(
                    "❌ REMOVE ADMIN",
                    callback_data="admin_remove",
                ),
            ],
            [
                InlineKeyboardButton(
                    "⬅️ BACK",
                    callback_data="home",
                ),
            ],
        ]
    )


def chats_keyboard(chats):
    buttons = []

    for chat in chats:
        chat_id = chat.get("chat_id")
        title = chat.get("title") or str(chat_id)

        if len(title) > 35:
            title = title[:32] + "..."

        buttons.append(
            [
                InlineKeyboardButton(
                    title,
                    callback_data=f"chat:{chat_id}",
                )
            ]
        )

    buttons.append(
        [
            InlineKeyboardButton(
                "⬅️ BACK",
                callback_data="home",
            )
        ]
    )

    return InlineKeyboardMarkup(
        buttons
    )


def chat_keyboard(chat):
    chat_id = chat["chat_id"]
    chat_type = chat.get("type")

    buttons = [
        [
            InlineKeyboardButton(
                "♻️ DCLEAN",
                callback_data=f"dclean:{chat_id}",
            )
        ]
    ]

    # PURGE ONLY GROUP/SUPERGROUP
    if chat_type in (
        "group",
        "supergroup",
    ):
        buttons.append(
            [
                InlineKeyboardButton(
                    "🗑️ PURGE",
                    callback_data=f"purge:{chat_id}",
                )
            ]
        )

    buttons.append(
        [
            InlineKeyboardButton(
                "📊 STATUS",
                callback_data=f"status:{chat_id}",
            )
        ]
    )

    buttons.append(
        [
            InlineKeyboardButton(
                "⬅️ CHATS",
                callback_data="chats",
            )
        ]
    )

    return InlineKeyboardMarkup(
        buttons
    )


def purge_keyboard(chat_id):
    buttons = []

    row = []

    for minutes, label in PURGE_PRESETS.items():
        row.append(
            InlineKeyboardButton(
                label,
                callback_data=f"purgeset:{chat_id}:{minutes}",
            )
        )

        if len(row) == 3:
            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    buttons.append(
        [
            InlineKeyboardButton(
                "✏️ CUSTOM MINUTES",
                callback_data=f"purgecustom:{chat_id}",
            )
        ]
    )

    buttons.append(
        [
            InlineKeyboardButton(
                "♻️ DUPLICATES ON/OFF",
                callback_data=f"purgedup:{chat_id}",
            )
        ]
    )

    buttons.append(
        [
            InlineKeyboardButton(
                "▶️ ENABLE",
                callback_data=f"purgeenable:{chat_id}",
            ),
            InlineKeyboardButton(
                "⏹️ DISABLE",
                callback_data=f"purgedisable:{chat_id}",
            ),
        ]
    )

    buttons.append(
        [
            InlineKeyboardButton(
                "⬅️ BACK",
                callback_data=f"chat:{chat_id}",
            )
        ]
    )

    return InlineKeyboardMarkup(
        buttons
    )


# ============================================================
# /START
# ============================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user = update.effective_user

    if not user:
        return

    if not await is_db_admin(user.id):
        await update.message.reply_text(
            "❌ <b>Admin access required.</b>",
            parse_mode="HTML",
        )
        return

    await update.message.reply_text(
        "🛡️ <b>DUPLICATE CLEANER</b>\n\n"
        "Select an option:",
        parse_mode="HTML",
        reply_markup=main_keyboard(),
    )


async def cancel_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user = update.effective_user

    if user:
        admin_input_state.pop(
            user.id,
            None,
        )

    await update.message.reply_text(
        "❌ Cancelled."
    )


# ============================================================
# CALLBACKS
# ============================================================

async def callback_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    query = update.callback_query

    if not query:
        return

    await query.answer()

    user = query.from_user

    if not await is_db_admin(user.id):
        await query.edit_message_text(
            "❌ Admin access required."
        )
        return

    data = query.data or ""

    # --------------------------------------------------------
    # HOME
    # --------------------------------------------------------

    if data == "home":
        await query.edit_message_text(
            "🛡️ <b>DUPLICATE CLEANER</b>\n\n"
            "Select an option:",
            parse_mode="HTML",
            reply_markup=main_keyboard(),
        )
        return

    # --------------------------------------------------------
    # CHATS
    # --------------------------------------------------------

    if data == "chats":
        chats = await get_all_chats()

        if not chats:
            await query.edit_message_text(
                "📋 <b>CHATS</b>\n\n"
                "No chats registered yet.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ BACK",
                                callback_data="home",
                            )
                        ]
                    ]
                ),
            )
            return

        await query.edit_message_text(
            "📋 <b>SELECT CHAT</b>",
            parse_mode="HTML",
            reply_markup=chats_keyboard(
                chats
            ),
        )
        return

    # --------------------------------------------------------
    # CHAT
    # --------------------------------------------------------

    if data.startswith("chat:"):
        chat_id = int(
            data.split(":", 1)[1]
        )

        chat = await get_chat(chat_id)

        if not chat:
            await query.edit_message_text(
                "❌ Chat not found."
            )
            return

        title = escape(
            chat.get("title")
            or str(chat_id)
        )

        chat_type = chat.get("type")

        await query.edit_message_text(
            f"📂 <b>{title}</b>\n\n"
            f"Type: <b>{chat_type}</b>",
            parse_mode="HTML",
            reply_markup=chat_keyboard(
                chat
            ),
        )
        return

    # --------------------------------------------------------
    # DCLEAN
    # --------------------------------------------------------

    if data.startswith("dclean:"):
        chat_id = int(
            data.split(":", 1)[1]
        )

        await query.edit_message_text(
            "♻️ <b>DCLEAN STARTING...</b>",
            parse_mode="HTML",
        )

        result = await run_dclean(
            chat_id,
            query.message,
        )

        await query.edit_message_text(
            result,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "⬅️ BACK",
                            callback_data=f"chat:{chat_id}",
                        )
                    ]
                ]
            ),
        )
        return

    # --------------------------------------------------------
    # PURGE MENU
    # --------------------------------------------------------

    if data.startswith("purge:"):
        chat_id = int(
            data.split(":", 1)[1]
        )

        chat = await get_chat(chat_id)

        if not chat:
            await query.edit_message_text(
                "❌ Chat not found."
            )
            return

        # HARD BLOCK CHANNEL
        if chat.get("type") not in (
            "group",
            "supergroup",
        ):
            await query.edit_message_text(
                "❌ PURGE is available only for "
                "Groups and Supergroups."
            )
            return

        settings = await get_purge_settings(
            chat_id
        )

        status = (
            "🟢 ENABLED"
            if settings["enabled"]
            else "🔴 DISABLED"
        )

        dup = (
            "🟢 ON"
            if settings["delete_duplicates"]
            else "🔴 OFF"
        )

        await query.edit_message_text(
            "🗑️ <b>PURGE SETTINGS</b>\n\n"
            f"Status: <b>{status}</b>\n"
            f"Delay: <b>{settings['minutes']} minutes</b>\n"
            f"Duplicate Delete: <b>{dup}</b>",
            parse_mode="HTML",
            reply_markup=purge_keyboard(
                chat_id
            ),
        )
        return

    # --------------------------------------------------------
    # PURGE PRESET
    # --------------------------------------------------------

    if data.startswith("purgeset:"):
        parts = data.split(":")

        chat_id = int(parts[1])
        minutes = int(parts[2])

        chat = await get_chat(chat_id)

        if not chat or chat.get("type") not in (
            "group",
            "supergroup",
        ):
            await query.edit_message_text(
                "❌ PURGE is not available for channels."
            )
            return

        settings = await get_purge_settings(
            chat_id
        )

        await save_purge_settings(
            chat_id,
            minutes=minutes,
        )

        await query.edit_message_text(
            "🗑️ <b>PURGE SETTINGS</b>\n\n"
            f"Delay: <b>{minutes} minutes</b>\n"
            f"Status: <b>{'🟢 ENABLED' if settings['enabled'] else '🔴 DISABLED'}</b>\n"
            f"Duplicate Delete: <b>{'🟢 ON' if settings['delete_duplicates'] else '🔴 OFF'}</b>",
            parse_mode="HTML",
            reply_markup=purge_keyboard(
                chat_id
            ),
        )
        return

    # --------------------------------------------------------
    # CUSTOM PURGE
    # --------------------------------------------------------

    if data.startswith("purgecustom:"):
        chat_id = int(
            data.split(":", 1)[1]
        )

        admin_input_state[user.id] = {
            "type": "purge_custom",
            "chat_id": chat_id,
        }

        await query.edit_message_text(
            "✏️ <b>CUSTOM PURGE TIME</b>\n\n"
            "Send the delay in minutes.\n\n"
            "Example:\n"
            "<code>90</code>\n"
            "<code>125</code>\n"
            "<code>300</code>\n\n"
            "Use /cancel to cancel.",
            parse_mode="HTML",
        )
        return

    # --------------------------------------------------------
    # DUPLICATE TOGGLE
    # --------------------------------------------------------

    if data.startswith("purgedup:"):
        chat_id = int(
            data.split(":", 1)[1]
        )

        chat = await get_chat(chat_id)

        if not chat or chat.get("type") not in (
            "group",
            "supergroup",
        ):
            await query.edit_message_text(
                "❌ PURGE is not available for channels."
            )
            return

        settings = await get_purge_settings(
            chat_id
        )

        new_value = not settings[
            "delete_duplicates"
        ]

        await save_purge_settings(
            chat_id,
            delete_duplicates=new_value,
        )

        await query.edit_message_text(
            "🗑️ <b>PURGE SETTINGS</b>\n\n"
            f"Delay: <b>{settings['minutes']} minutes</b>\n"
            f"Duplicate Delete: <b>{'🟢 ON' if new_value else '🔴 OFF'}</b>",
            parse_mode="HTML",
            reply_markup=purge_keyboard(
                chat_id
            ),
        )
        return

    # --------------------------------------------------------
    # ENABLE
    # --------------------------------------------------------

    if data.startswith("purgeenable:"):
        chat_id = int(
            data.split(":", 1)[1]
        )

        chat = await get_chat(chat_id)

        if not chat or chat.get("type") not in (
            "group",
            "supergroup",
        ):
            await query.edit_message_text(
                "❌ PURGE cannot be enabled for channels."
            )
            return

        await save_purge_settings(
            chat_id,
            enabled=True,
        )

        await start_purge_worker(
            chat_id
        )

        await query.edit_message_text(
            "🟢 <b>PURGE ENABLED</b>",
            parse_mode="HTML",
            reply_markup=purge_keyboard(
                chat_id
            ),
        )
        return

    # --------------------------------------------------------
    # DISABLE
    # --------------------------------------------------------

    if data.startswith("purgedisable:"):
        chat_id = int(
            data.split(":", 1)[1]
        )

        await save_purge_settings(
            chat_id,
            enabled=False,
        )

        await stop_purge_worker(
            chat_id
        )

        await query.edit_message_text(
            "🔴 <b>PURGE DISABLED</b>",
            parse_mode="HTML",
            reply_markup=purge_keyboard(
                chat_id
            ),
        )
        return

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    if data.startswith("status:"):
        chat_id = int(
            data.split(":", 1)[1]
        )

        chat = await get_chat(chat_id)

        if not chat:
            await query.edit_message_text(
                "❌ Chat not found."
            )
            return

        last_id = await get_last_dclean_id(
            chat_id
        )

        settings = await get_purge_settings(
            chat_id
        )

        await query.edit_message_text(
            "📊 <b>STATUS</b>\n\n"
            f"Chat: <b>{escape(chat.get('title', str(chat_id)))}</b>\n"
            f"Type: <b>{chat.get('type')}</b>\n\n"
            f"♻️ DCLEAN Last ID: <b>{last_id}</b>\n"
            f"🗑️ PURGE: <b>{'ON' if settings['enabled'] else 'OFF'}</b>\n"
            f"⏱️ Delay: <b>{settings['minutes']} min</b>\n"
            f"♻️ Duplicate Delete: <b>{'ON' if settings['delete_duplicates'] else 'OFF'}</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "⬅️ BACK",
                            callback_data=f"chat:{chat_id}",
                        )
                    ]
                ]
            ),
        )
        return

    # --------------------------------------------------------
    # ADMINS
    # --------------------------------------------------------

    if data == "admins":
        await query.edit_message_text(
            "👑 <b>ADMIN MANAGEMENT</b>",
            parse_mode="HTML",
            reply_markup=admin_keyboard(),
        )
        return

    # --------------------------------------------------------
    # ADMIN LIST
    # --------------------------------------------------------

    if data == "admin_list":
        ids = await get_admin_list()

        text = "👥 <b>ADMIN LIST</b>\n\n"

        for uid in ids:
            protected = (
                " 🔒 ENV"
                if uid in ENV_ADMIN_IDS
                else ""
            )

            text += (
                f"• <code>{uid}</code>"
                f"{protected}\n"
            )

        await query.edit_message_text(
            text,
            parse_mode="HTML",
            reply_markup=admin_keyboard(),
        )
        return

    # --------------------------------------------------------
    # ADD ADMIN
    # --------------------------------------------------------

    if data == "admin_add":
        admin_input_state[user.id] = {
            "type": "add_admin"
        }

        await query.edit_message_text(
            "➕ <b>ADD ADMIN</b>\n\n"
            "Send the Telegram numeric user ID.\n\n"
            "Example:\n"
            "<code>123456789</code>\n\n"
            "Use /cancel to cancel.",
            parse_mode="HTML",
        )
        return

    # --------------------------------------------------------
    # REMOVE ADMIN
    # --------------------------------------------------------

    if data == "admin_remove":
        admin_input_state[user.id] = {
            "type": "remove_admin"
        }

        await query.edit_message_text(
            "❌ <b>REMOVE ADMIN</b>\n\n"
            "Send the Telegram numeric user ID.\n\n"
            "ENV admins are protected.\n\n"
            "Use /cancel to cancel.",
            parse_mode="HTML",
        )
        return


# ============================================================
# TEXT INPUT
# ============================================================

async def private_text_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    message = update.message

    if not message:
        return

    user = update.effective_user

    if not user:
        return

    if not await is_db_admin(user.id):
        return

    state = admin_input_state.get(
        user.id
    )

    if not state:
        return

    text = (message.text or "").strip()

    # --------------------------------------------------------
    # ADD ADMIN
    # --------------------------------------------------------

    if state["type"] == "add_admin":
        try:
            user_id = int(text)
        except ValueError:
            await message.reply_text(
                "❌ Invalid numeric user ID."
            )
            return

        result = await add_admin(
            user_id
        )

        admin_input_state.pop(
            user.id,
            None,
        )

        if result:
            await message.reply_text(
                f"✅ Admin added:\n<code>{user_id}</code>",
                parse_mode="HTML",
                reply_markup=admin_keyboard(),
            )
        else:
            await message.reply_text(
                "🔒 This admin is protected by ENV."
            )

        return

    # --------------------------------------------------------
    # REMOVE ADMIN
    # --------------------------------------------------------

    if state["type"] == "remove_admin":
        try:
            user_id = int(text)
        except ValueError:
            await message.reply_text(
                "❌ Invalid numeric user ID."
            )
            return

        result = await remove_admin(
            user_id
        )

        admin_input_state.pop(
            user.id,
            None,
        )

        if result:
            await message.reply_text(
                f"✅ Admin removed:\n<code>{user_id}</code>",
                parse_mode="HTML",
                reply_markup=admin_keyboard(),
            )
        elif user_id in ENV_ADMIN_IDS:
            await message.reply_text(
                "🔒 ENV admin cannot be removed."
            )
        else:
            await message.reply_text(
                "❌ Admin not found."
            )

        return

    # --------------------------------------------------------
    # CUSTOM PURGE
    # --------------------------------------------------------

    if state["type"] == "purge_custom":
        chat_id = state["chat_id"]

        try:
            minutes = int(text)
        except ValueError:
            await message.reply_text(
                "❌ Send minutes as a number.\n\n"
                "Example: 90"
            )
            return

        if minutes < 1:
            await message.reply_text(
                "❌ Minutes must be greater than 0."
            )
            return

        chat = await get_chat(
            chat_id
        )

        if not chat or chat.get("type") not in (
            "group",
            "supergroup",
        ):
            admin_input_state.pop(
                user.id,
                None,
            )

            await message.reply_text(
                "❌ PURGE is available only for Groups/Supergroups."
            )
            return

        await save_purge_settings(
            chat_id,
            minutes=minutes,
        )

        admin_input_state.pop(
            user.id,
            None,
        )

        settings = await get_purge_settings(
            chat_id
        )

        await message.reply_text(
            "✅ <b>PURGE TIME UPDATED</b>\n\n"
            f"Delay: <b>{minutes} minutes</b>\n"
            f"Status: <b>{'ON' if settings['enabled'] else 'OFF'}</b>",
            parse_mode="HTML",
            reply_markup=purge_keyboard(
                chat_id
            ),
        )


# ============================================================
# CHAT MEMBER WATCHER
# ============================================================

async def my_chat_member_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    member_update = update.my_chat_member

    if not member_update:
        return

    chat = update.effective_chat

    if not chat:
        return

    status = member_update.new_chat_member.status

    chat_type = chat.type

    if chat_type not in (
        "group",
        "supergroup",
        "channel",
    ):
        return

    if status in (
        ChatMemberStatus.MEMBER,
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.OWNER,
    ):
        await register_chat(
            chat.id,
            chat.title,
            chat_type,
        )

    elif status in (
        ChatMemberStatus.LEFT,
        ChatMemberStatus.KICKED,
    ):
        await deactivate_chat(
            chat.id
        )

        if chat_type in (
            "group",
            "supergroup",
        ):
            await stop_purge_worker(
                chat.id
            )


# ============================================================
# GROUP/SUPERGROUP MESSAGE WATCHER
# ============================================================

async def message_watcher(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    message = update.message

    if not message:
        return

    chat = update.effective_chat

    if not chat:
        return

    if chat.type not in (
        "group",
        "supergroup",
    ):
        return

    await register_chat(
        chat.id,
        chat.title,
        chat.type,
    )


# ============================================================
# CHANNEL POST WATCHER
# ============================================================

async def channel_post_watcher(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    post = update.channel_post

    if not post:
        return

    chat = update.effective_chat

    if not chat:
        return

    if chat.type != "channel":
        return

    await register_chat(
        chat.id,
        chat.title,
        "channel",
    )


# ============================================================
# UNIVERSAL UPDATE ROUTER
# PTB 22.3 SAFE CHANNEL HANDLER
# ============================================================

async def update_router(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if update.channel_post:
        await channel_post_watcher(
            update,
            context,
        )


# ============================================================
# POST INIT
# ============================================================

async def post_init(
    application: Application,
):
    global mongo
    global db
    global telethon_client

    global chats_col
    global admins_col
    global dclean_state_col
    global media_col
    global pending_col
    global purge_settings_col

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN is missing."
        )

    if not MONGO_URI:
        raise RuntimeError(
            "MONGO_URI is missing."
        )

    if not API_ID:
        raise RuntimeError(
            "API_ID is missing."
        )

    if not API_HASH:
        raise RuntimeError(
            "API_HASH is missing."
        )

    if not STRING_SESSION:
        raise RuntimeError(
            "STRING_SESSION is missing."
        )

    # Mongo
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

    # SAFE indexes
    await setup_indexes()

    logger.info(
        "MongoDB initialized safely."
    )

    # Telethon
    telethon_client = TelegramClient(
        StringSession(STRING_SESSION),
        API_ID,
        API_HASH,
    )

    await telethon_client.connect()

    if not await telethon_client.is_user_authorized():
        raise RuntimeError(
            "STRING_SESSION is not authorized."
        )

    logger.info(
        "Telethon session connected."
    )

    # Restore enabled PURGE workers.
    await restore_purge_workers()

    logger.info(
        "PURGE workers restored."
    )


# ============================================================
# POST SHUTDOWN
# ============================================================

async def post_shutdown(
    application: Application,
):
    global mongo
    global telethon_client

    # Stop purge workers.
    tasks = list(
        purge_tasks.values()
    )

    for task in tasks:
        task.cancel()

    if tasks:
        await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

    purge_tasks.clear()

    # Disconnect Telethon.
    if telethon_client:
        try:
            await telethon_client.disconnect()
        except Exception:
            logger.exception(
                "Telethon disconnect failed"
            )

    # Close Mongo.
    if mongo:
        try:
            mongo.close()
        except Exception:
            logger.exception(
                "Mongo close failed"
            )

    logger.info(
        "Shutdown complete."
    )


# ============================================================
# MAIN
# ============================================================

async def run_application():
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    # Commands
    application.add_handler(
        CommandHandler("start", start_command)
    )

    application.add_handler(
        CommandHandler("cancel", cancel_command)
    )

    # Bot membership changes
    application.add_handler(
        ChatMemberHandler(
            my_chat_member_handler,
            ChatMemberHandler.MY_CHAT_MEMBER,
        )
    )

    # Buttons
    application.add_handler(
        CallbackQueryHandler(callback_handler)
    )

    # Private text input
    application.add_handler(
        MessageHandler(
            filters.TEXT
            & ~filters.COMMAND
            & filters.ChatType.PRIVATE,
            private_text_handler,
        )
    )

    # Channel posts
    application.add_handler(
        TypeHandler(
            Update,
            update_router,
        ),
        group=1,
    )

    # Group / Supergroup registration
    application.add_handler(
        MessageHandler(
            filters.ALL
            & ~filters.COMMAND
            & (
                filters.ChatType.GROUP
                | filters.ChatType.SUPERGROUP
            ),
            message_watcher,
        ),
        group=10,
    )

    logger.info(
        "Duplicate Cleaner starting..."
    )

    # --------------------------------------------------------
    # Python 3.14 SAFE STARTUP
    # --------------------------------------------------------

    await application.initialize()
    await application.start()

    if application.updater is None:
        raise RuntimeError(
            "Updater is not available."
        )

    await application.updater.start_polling(
        allowed_updates=[
            "message",
            "channel_post",
            "callback_query",
            "my_chat_member",
        ],
        drop_pending_updates=False,
    )

    logger.info(
        "Telegram polling started successfully."
    )

    try:
        await asyncio.Event().wait()

    finally:
        logger.info(
            "Stopping Duplicate Cleaner..."
        )

        try:
            await application.updater.stop()
        except Exception:
            logger.exception(
                "Updater stop failed"
            )

        try:
            await application.stop()
        except Exception:
            logger.exception(
                "Application stop failed"
            )

        try:
            await application.shutdown()
        except Exception:
            logger.exception(
                "Application shutdown failed"
            )


def main():
    asyncio.run(
        run_application()
    )


if __name__ == "__main__":
    main()
