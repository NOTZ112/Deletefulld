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


# ============================================================
# ADMIN IDS
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
            pass

    return result


ENV_ADMIN_IDS = parse_admin_ids(
    os.getenv("ADMIN_IDS", "")
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(
    "duplicate-cleaner"
)


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

# Temporary admin input states
admin_input_state = {}

# Background UI jobs
ui_tasks = set()

# Prevent same DCLEAN chat from running twice
dclean_tasks = {}


# ============================================================
# PURGE PRESETS
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


# ============================================================
# HELPERS
# ============================================================

def now_utc():
    return datetime.now(timezone.utc)


def is_admin(user_id):
    return user_id in ENV_ADMIN_IDS


async def is_db_admin(user_id):
    if user_id in ENV_ADMIN_IDS:
        return True

    if admins_col is None:
        return False

    doc = await admins_col.find_one(
        {"user_id": user_id}
    )

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
# FAST UI HELPERS
# ============================================================

def spawn_ui_task(coro):

    task = asyncio.create_task(coro)

    ui_tasks.add(task)

    def done_callback(done_task):

        ui_tasks.discard(done_task)

        try:
            done_task.exception()
        except (
            asyncio.CancelledError,
            Exception,
        ):
            pass

    task.add_done_callback(
        done_callback
    )

    return task


async def safe_edit(
    query,
    text,
    markup=None,
):

    try:

        await query.edit_message_text(
            text,
            parse_mode="HTML",
            reply_markup=markup,
        )

    except Exception as e:

        if "not modified" not in str(e).lower():

            logger.debug(
                "UI edit failed: %s",
                e,
            )


async def safe_answer(query):

    try:
        await query.answer()
    except Exception:
        pass


# ============================================================
# SAFE INDEX CREATION
# ============================================================

async def ensure_index_safe(
    collection,
    key,
    name,
    unique=False,
):

    indexes = await collection.index_information()

    existing = indexes.get(name)

    if existing:

        existing_key = existing.get("key")
        existing_unique = bool(
            existing.get("unique", False)
        )

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
# ADMIN DATABASE
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

    if chat_type in (
        "group",
        "supergroup",
    ):

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
    ).sort(
        "title",
        ASCENDING,
    )

    return await cursor.to_list(
        length=1000
    )


# ============================================================
# TELETHON SAFE CALL
# ============================================================

async def tg_call(
    func,
    *args,
    **kwargs,
):

    while True:

        try:

            return await func(
                *args,
                **kwargs,
            )

        except FloodWaitError as e:

            wait_time = int(
                e.seconds
            ) + 2

            logger.warning(
                "FloodWait: sleeping %s seconds",
                wait_time,
            )

            await asyncio.sleep(
                wait_time
            )

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

        chat_id = int(
            PIPE_LOG_CHAT_ID
        )

        await tg_call(
            telethon_client.send_message,
            chat_id,
            text,
        )

    except Exception:

        logger.exception(
            "Log sending failed"
        )


# ============================================================
# MEDIA FINGERPRINT
# ============================================================

def get_media_fingerprint(message):

    if not message:
        return None

    try:

        photo = getattr(
            message,
            "photo",
            None,
        )

        if photo:

            photo_id = getattr(
                photo,
                "id",
                None,
            )

            if photo_id:

                return f"photo:{photo_id}"

        document = getattr(
            message,
            "document",
            None,
        )

        if document:

            document_id = getattr(
                document,
                "id",
                None,
            )

            if document_id:

                return f"document:{document_id}"

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

    return int(
        doc.get(
            "last_message_id",
            0,
        )
        or 0
    )


async def save_last_dclean_id(
    chat_id,
    message_id,
):

    await dclean_state_col.update_one(
        {"chat_id": chat_id},
        {
            "$set": {
                "chat_id": chat_id,
                "last_message_id": int(
                    message_id
                ),
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

    pending = await cursor.to_list(
        length=5000
    )

    deleted = 0

    for item in pending:

        message_id = item.get(
            "message_id"
        )

        if not message_id:
            continue

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

    return deleted


# ============================================================
# DCLEAN
# ============================================================

async def run_dclean(
    chat_id,
    status_message=None,
):

    chat = await get_chat(
        chat_id
    )

    if not chat:
        return (
            "❌ Chat not registered."
        )

    last_id = await get_last_dclean_id(
        chat_id
    )

    scanned = 0
    duplicates = 0
    deleted = 0
    failed = 0

    highest_id = last_id

    try:

        entity = await telethon_client.get_entity(
            chat_id
        )

    except Exception:

        logger.exception(
            "Entity error"
        )

        return (
            "❌ Could not access this chat "
            "with the Telegram session."
        )

    # Retry previously failed deletions
    try:

        deleted += await retry_pending_deletes(
            entity,
            chat_id,
        )

    except Exception:

        logger.exception(
            "Pending retry failed"
        )

    # Load already known fingerprints
    seen = set()

    cursor = media_col.find(
        {"chat_id": chat_id},
        {
            "fingerprint": 1,
            "_id": 0,
        },
    )

    async for doc in cursor:

        fp = doc.get(
            "fingerprint"
        )

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

                else:

                    seen.add(
                        fingerprint
                    )

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

            # Save progress every 100 messages
            if scanned % 100 == 0:

                await save_last_dclean_id(
                    chat_id,
                    highest_id,
                )

                if status_message:

                    await safe_edit(
                        status_message,
                        "♻️ <b>DCLEAN RUNNING</b>\n\n"
                        f"📖 Scanned: <b>{scanned}</b>\n"
                        f"♻️ Duplicates: <b>{duplicates}</b>\n"
                        f"🗑️ Deleted: <b>{deleted}</b>\n"
                        f"⏳ Pending: <b>{failed}</b>\n"
                        f"🆔 Last ID: <b>{highest_id}</b>",
                    )

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

async def get_purge_settings(
    chat_id
):

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
        "enabled": bool(
            doc.get(
                "enabled",
                False,
            )
        ),
        "minutes": int(
            doc.get(
                "minutes",
                60,
            )
        ),
        "delete_duplicates": bool(
            doc.get(
                "delete_duplicates",
                False,
            )
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

        update["enabled"] = bool(
            enabled
        )

    if minutes is not None:

        update["minutes"] = int(
            minutes
        )

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

    chat = await get_chat(
        chat_id
    )

    if not chat:
        return None

    # HARD CHANNEL BLOCK
    if chat.get("type") not in (
        "group",
        "supergroup",
    ):
        return None

    settings = await get_purge_settings(
        chat_id
    )

    minutes = settings[
        "minutes"
    ]

    cutoff = (
        now_utc()
        - timedelta(
            minutes=minutes
        )
    )

    try:

        entity = await telethon_client.get_entity(
            chat_id
        )

    except Exception:

        logger.exception(
            "PURGE entity error"
        )

        return None

    scanned = 0
    deleted = 0
    duplicate_deleted = 0

    # Used only when duplicate deletion is enabled
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

            # Recent message -> don't delete
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

            # ------------------------------------------------
            # Duplicate mode
            # ------------------------------------------------

            if settings[
                "delete_duplicates"
            ]:

                if fingerprint:

                    if fingerprint in seen:

                        duplicate_deleted += 1

                        result = await tg_call(
                            telethon_client.delete_messages,
                            entity,
                            [message.id],
                        )

                        if result is not None:
                            deleted += 1

                        else:
                            await add_pending_delete(
                                chat_id,
                                message.id,
                                fingerprint,
                            )

                        continue

                    else:

                        seen.add(
                            fingerprint
                        )

            # ------------------------------------------------
            # Normal old-message deletion
            # ------------------------------------------------

            result = await tg_call(
                telethon_client.delete_messages,
                entity,
                [message.id],
            )

            if result is not None:

                deleted += 1

            else:

                await add_pending_delete(
                    chat_id,
                    message.id,
                    fingerprint,
                )

            if scanned % 50 == 0:

                if status_message:

                    await safe_edit(
                        status_message,
                        "🗑️ <b>PURGE RUNNING</b>\n\n"
                        f"📖 Scanned: <b>{scanned}</b>\n"
                        f"🗑️ Deleted: <b>{deleted}</b>\n"
                        f"♻️ Duplicate: <b>{duplicate_deleted}</b>",
                    )

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

async def purge_worker(
    chat_id
):

    logger.info(
        "PURGE worker started: %s",
        chat_id,
    )

    try:

        while True:

            chat = await get_chat(
                chat_id
            )

            if not chat:
                break

            if chat.get("type") not in (
                "group",
                "supergroup",
            ):
                break

            settings = await get_purge_settings(
                chat_id
            )

            if not settings[
                "enabled"
            ]:
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

            # Check every minute
            await asyncio.sleep(
                60
            )

    except asyncio.CancelledError:

        pass

    finally:

        purge_tasks.pop(
            chat_id,
            None,
        )

        logger.info(
            "PURGE worker stopped: %s",
            chat_id,
        )


async def start_purge_worker(
    chat_id
):

    existing = purge_tasks.get(
        chat_id
    )

    if existing and not existing.done():
        return

    task = asyncio.create_task(
        purge_worker(chat_id)
    )

    purge_tasks[
        chat_id
    ] = task


async def stop_purge_worker(
    chat_id
):

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

        chat_id = settings.get(
            "chat_id"
        )

        if not chat_id:
            continue

        chat = await get_chat(
            chat_id
        )

        if not chat:
            continue

        if chat.get("type") not in (
            "group",
            "supergroup",
        ):
            continue

        await start_purge_worker(
            chat_id
        )


# ============================================================
# KEYBOARDS
# ============================================================

def main_keyboard():

    return InlineKeyboardMarkup([
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
    ])


def admin_keyboard():

    return InlineKeyboardMarkup([
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
    ])


def chats_keyboard(chats):

    buttons = []

    for chat in chats:

        chat_id = chat.get(
            "chat_id"
        )

        title = (
            chat.get("title")
            or str(chat_id)
        )

        if len(title) > 35:

            title = (
                title[:32]
                + "..."
            )

        buttons.append([
            InlineKeyboardButton(
                title,
                callback_data=f"chat:{chat_id}",
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            "⬅️ BACK",
            callback_data="home",
        )
    ])

    return InlineKeyboardMarkup(
        buttons
    )


def chat_keyboard(chat):

    chat_id = chat[
        "chat_id"
    ]

    chat_type = chat.get(
        "type"
    )

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

        buttons.append([
            InlineKeyboardButton(
                "🗑️ PURGE",
                callback_data=f"purge:{chat_id}",
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            "📊 STATUS",
            callback_data=f"status:{chat_id}",
        )
    ])

    buttons.append([
        InlineKeyboardButton(
            "⬅️ CHATS",
            callback_data="chats",
        )
    ])

    return InlineKeyboardMarkup(
        buttons
    )


def purge_keyboard(
    chat_id
):

    buttons = []

    row = []

    for minutes, label in PURGE_PRESETS.items():

        row.append(
            InlineKeyboardButton(
                label,
                callback_data=(
                    f"purgeset:"
                    f"{chat_id}:"
                    f"{minutes}"
                ),
            )
        )

        if len(row) == 3:

            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    buttons.append([
        InlineKeyboardButton(
            "✏️ CUSTOM MINUTES",
            callback_data=(
                f"purgecustom:{chat_id}"
            ),
        )
    ])

    buttons.append([
        InlineKeyboardButton(
            "♻️ DUPLICATES ON/OFF",
            callback_data=(
                f"purgedup:{chat_id}"
            ),
        )
    ])

    buttons.append([
        InlineKeyboardButton(
            "▶️ ENABLE",
            callback_data=(
                f"purgeenable:{chat_id}"
            ),
        ),
        InlineKeyboardButton(
            "⏹️ DISABLE",
            callback_data=(
                f"purgedisable:{chat_id}"
            ),
        ),
    ])

    buttons.append([
        InlineKeyboardButton(
            "⬅️ BACK",
            callback_data=(
                f"chat:{chat_id}"
            ),
        )
    ])

    return InlineKeyboardMarkup(
        buttons
    )


# ============================================================
# START
# ============================================================

async def start_command(
    update,
    context,
):

    user = update.effective_user

    if not user:
        return

    if not await is_db_admin(
        user.id
    ):

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


# ============================================================
# CANCEL
# ============================================================

async def cancel_command(
    update,
    context,
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
# BACKGROUND UI JOBS
# ============================================================

async def load_chats_ui(
    query
):

    try:

        chats = await get_all_chats()

        if not chats:

            await safe_edit(
                query,
                "📋 <b>CHATS</b>\n\n"
                "No chats registered yet.",
                InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(
                            "⬅️ BACK",
                            callback_data="home",
                        )
                    ]
                ]),
            )

            return

        await safe_edit(
            query,
            "📋 <b>SELECT CHAT</b>",
            chats_keyboard(chats),
        )

    except Exception:

        logger.exception(
            "Chat list failed"
        )

        await safe_edit(
            query,
            "❌ Failed to load chats.",
        )


async def load_chat_ui(
    query,
    chat_id,
):

    try:

        chat = await get_chat(
            chat_id
        )

        if not chat:

            await safe_edit(
                query,
                "❌ Chat not found.",
            )

            return

        title = escape(
            chat.get(
                "title"
            )
            or str(chat_id)
        )

        await safe_edit(
            query,
            f"📂 <b>{title}</b>\n\n"
            f"Type: <b>{chat.get('type')}</b>",
            chat_keyboard(chat),
        )

    except Exception:

        logger.exception(
            "Chat UI failed"
        )

        await safe_edit(
            query,
            "❌ Failed to load chat.",
        )


async def load_status_ui(
    query,
    chat_id,
):

    try:

        chat = await get_chat(
            chat_id
        )

        if not chat:

            await safe_edit(
                query,
                "❌ Chat not found.",
            )

            return

        last_id = await get_last_dclean_id(
            chat_id
        )

        settings = await get_purge_settings(
            chat_id
        )

        await safe_edit(
            query,
            "📊 <b>STATUS</b>\n\n"
            f"Chat: <b>{escape(chat.get('title', str(chat_id)))}</b>\n"
            f"Type: <b>{chat.get('type')}</b>\n\n"
            f"♻️ DCLEAN Last ID: <b>{last_id}</b>\n"
            f"🗑️ PURGE: <b>{'ON' if settings['enabled'] else 'OFF'}</b>\n"
            f"⏱️ Delay: <b>{settings['minutes']} min</b>\n"
            f"♻️ Duplicate Delete: <b>{'ON' if settings['delete_duplicates'] else 'OFF'}</b>",
            InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(
                        "⬅️ BACK",
                        callback_data=f"chat:{chat_id}",
                    )
                ]
            ]),
        )

    except Exception:

        logger.exception(
            "Status failed"
        )

        await safe_edit(
            query,
            "❌ Failed to load status.",
        )


async def load_admin_list_ui(
    query
):

    try:

        ids = await get_admin_list()

        text = (
            "👥 <b>ADMIN LIST</b>\n\n"
        )

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

        await safe_edit(
            query,
            text,
            admin_keyboard(),
        )

    except Exception:

        logger.exception(
            "Admin list failed"
        )

        await safe_edit(
            query,
            "❌ Failed to load admins.",
        )


async def load_purge_ui(
    query,
    chat_id,
):

    try:

        chat = await get_chat(
            chat_id
        )

        if not chat:

            await safe_edit(
                query,
                "❌ Chat not found.",
            )

            return

        if chat.get("type") not in (
            "group",
            "supergroup",
        ):

            await safe_edit(
                query,
                "❌ PURGE is available only "
                "for Groups and Supergroups.",
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

        await safe_edit(
            query,
            "🗑️ <b>PURGE SETTINGS</b>\n\n"
            f"Status: <b>{status}</b>\n"
            f"Delay: <b>{settings['minutes']} minutes</b>\n"
            f"Duplicate Delete: <b>{dup}</b>",
            purge_keyboard(chat_id),
        )

    except Exception:

        logger.exception(
            "Purge UI failed"
        )

        await safe_edit(
            query,
            "❌ Failed to load PURGE settings.",
        )


# ============================================================
# CALLBACK HANDLER
# ============================================================

async def callback_handler(
    update,
    context,
):

    query = update.callback_query

    if not query:
        return

    # ========================================================
    # IMPORTANT:
    # FIRST NETWORK OPERATION
    # ========================================================

    await safe_answer(query)

    user = query.from_user

    # ========================================================
    # ADMIN CHECK
    # ========================================================

    try:

        allowed = await is_db_admin(
            user.id
        )

    except Exception:

        allowed = False

    if not allowed:

        await safe_edit(
            query,
            "❌ <b>Admin access required.</b>",
        )

        return

    data = query.data or ""

    # ========================================================
    # HOME
    # ========================================================

    if data == "home":

        await safe_edit(
            query,
            "🛡️ <b>DUPLICATE CLEANER</b>\n\n"
            "Select an option:",
            main_keyboard(),
        )

        return

    # ========================================================
    # CHATS
    # ========================================================

    if data == "chats":

        # Immediate UI response
        await safe_edit(
            query,
            "📋 <b>SELECT CHAT</b>\n\n"
            "⏳ Loading...",
        )

        spawn_ui_task(
            load_chats_ui(query)
        )

        return

    # ========================================================
    # CHAT
    # ========================================================

    if data.startswith("chat:"):

        try:

            chat_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

        except ValueError:

            return

        await safe_edit(
            query,
            "📂 <b>Loading chat...</b>",
        )

        spawn_ui_task(
            load_chat_ui(
                query,
                chat_id,
            )
        )

        return

    # ========================================================
    # DCLEAN
    # ========================================================

    if data.startswith("dclean:"):

        try:

            chat_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

        except ValueError:

            return

        # Prevent duplicate simultaneous scan
        existing = dclean_tasks.get(
            chat_id
        )

        if existing and not existing.done():

            await safe_edit(
                query,
                "♻️ <b>DCLEAN ALREADY RUNNING</b>\n\n"
                "Please wait for the current scan to finish.",
            )

            return

        # Immediate response
        await safe_edit(
            query,
            "♻️ <b>DCLEAN STARTED</b>\n\n"
            "⚡ Scan is running in background...\n\n"
            "📊 Progress will appear here.",
        )

        async def dclean_job():

            try:

                result = await run_dclean(
                    chat_id,
                    query.message,
                )

                await safe_edit(
                    query,
                    result,
                    InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton(
                                "⬅️ BACK",
                                callback_data=(
                                    f"chat:{chat_id}"
                                ),
                            )
                        ]
                    ]),
                )

            except asyncio.CancelledError:

                raise

            except Exception:

                logger.exception(
                    "DCLEAN background error"
                )

                await safe_edit(
                    query,
                    "❌ <b>DCLEAN FAILED</b>",
                    InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton(
                                "⬅️ BACK",
                                callback_data=(
                                    f"chat:{chat_id}"
                                ),
                            )
                        ]
                    ]),
                )

            finally:

                dclean_tasks.pop(
                    chat_id,
                    None,
                )

        task = asyncio.create_task(
            dclean_job()
        )

        dclean_tasks[
            chat_id
        ] = task

        return

    # ========================================================
    # PURGE MENU
    # ========================================================

    if data.startswith("purge:"):

        try:

            chat_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

        except ValueError:

            return

        await safe_edit(
            query,
            "🗑️ <b>PURGE SETTINGS</b>\n\n"
            "⏳ Loading...",
        )

        spawn_ui_task(
            load_purge_ui(
                query,
                chat_id,
            )
        )

        return

    # ========================================================
    # PURGE PRESET
    # ========================================================

    if data.startswith("purgeset:"):

        try:

            parts = data.split(":")

            chat_id = int(
                parts[1]
            )

            minutes = int(
                parts[2]
            )

        except (
            ValueError,
            IndexError,
        ):

            return

        await safe_edit(
            query,
            "🗑️ <b>Updating PURGE time...</b>",
        )

        async def preset_job():

            try:

                chat = await get_chat(
                    chat_id
                )

                if not chat or chat.get(
                    "type"
                ) not in (
                    "group",
                    "supergroup",
                ):

                    await safe_edit(
                        query,
                        "❌ PURGE is not available for channels.",
                    )

                    return

                await save_purge_settings(
                    chat_id,
                    minutes=minutes,
                )

                settings = await get_purge_settings(
                    chat_id
                )

                await safe_edit(
                    query,
                    "🗑️ <b>PURGE SETTINGS</b>\n\n"
                    f"Delay: <b>{minutes} minutes</b>\n"
                    f"Status: <b>{'🟢 ENABLED' if settings['enabled'] else '🔴 DISABLED'}</b>\n"
                    f"Duplicate Delete: <b>{'🟢 ON' if settings['delete_duplicates'] else '🔴 OFF'}</b>",
                    purge_keyboard(
                        chat_id
                    ),
                )

            except Exception:

                logger.exception(
                    "Preset update failed"
                )

                await safe_edit(
                    query,
                    "❌ Failed to update PURGE time.",
                )

        spawn_ui_task(
            preset_job()
        )

        return

    # ========================================================
    # CUSTOM PURGE
    # ========================================================

    if data.startswith(
        "purgecustom:"
    ):

        try:

            chat_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

        except ValueError:

            return

        admin_input_state[
            user.id
        ] = {
            "type": "purge_custom",
            "chat_id": chat_id,
        }

        await safe_edit(
            query,
            "✏️ <b>CUSTOM PURGE TIME</b>\n\n"
            "Send the delay in minutes.\n\n"
            "Example:\n"
            "<code>90</code>\n"
            "<code>125</code>\n"
            "<code>300</code>\n\n"
            "Use /cancel to cancel.",
        )

        return

    # ========================================================
    # DUPLICATE TOGGLE
    # ========================================================

    if data.startswith(
        "purgedup:"
    ):

        try:

            chat_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

        except ValueError:

            return

        await safe_edit(
            query,
            "♻️ <b>Updating...</b>",
        )

        async def duplicate_job():

            try:

                chat = await get_chat(
                    chat_id
                )

                if not chat or chat.get(
                    "type"
                ) not in (
                    "group",
                    "supergroup",
                ):

                    await safe_edit(
                        query,
                        "❌ PURGE is not available for channels.",
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

                await safe_edit(
                    query,
                    "🗑️ <b>PURGE SETTINGS</b>\n\n"
                    f"Delay: <b>{settings['minutes']} minutes</b>\n"
                    f"Duplicate Delete: <b>{'🟢 ON' if new_value else '🔴 OFF'}</b>",
                    purge_keyboard(
                        chat_id
                    ),
                )

            except Exception:

                logger.exception(
                    "Duplicate toggle failed"
                )

                await safe_edit(
                    query,
                    "❌ Failed to update setting.",
                )

        spawn_ui_task(
            duplicate_job()
        )

        return

    # ========================================================
    # ENABLE
    # ========================================================

    if data.startswith(
        "purgeenable:"
    ):

        try:

            chat_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

        except ValueError:

            return

        await safe_edit(
            query,
            "▶️ <b>Enabling PURGE...</b>",
        )

        async def enable_job():

            try:

                chat = await get_chat(
                    chat_id
                )

                if not chat or chat.get(
                    "type"
                ) not in (
                    "group",
                    "supergroup",
                ):

                    await safe_edit(
                        query,
                        "❌ PURGE cannot be enabled for channels.",
                    )

                    return

                await save_purge_settings(
                    chat_id,
                    enabled=True,
                )

                await start_purge_worker(
                    chat_id
                )

                await safe_edit(
                    query,
                    "🟢 <b>PURGE ENABLED</b>",
                    purge_keyboard(
                        chat_id
                    ),
                )

            except Exception:

                logger.exception(
                    "PURGE enable failed"
                )

                await safe_edit(
                    query,
                    "❌ Failed to enable PURGE.",
                )

        spawn_ui_task(
            enable_job()
        )

        return

    # ========================================================
    # DISABLE
    # ========================================================

    if data.startswith(
        "purgedisable:"
    ):

        try:

            chat_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

        except ValueError:

            return

        await safe_edit(
            query,
            "⏹️ <b>Disabling PURGE...</b>",
        )

        async def disable_job():

            try:

                await save_purge_settings(
                    chat_id,
                    enabled=False,
                )

                await stop_purge_worker(
                    chat_id
                )

                await safe_edit(
                    query,
                    "🔴 <b>PURGE DISABLED</b>",
                    purge_keyboard(
                        chat_id
                    ),
                )

            except Exception:

                logger.exception(
                    "PURGE disable failed"
                )

                await safe_edit(
                    query,
                    "❌ Failed to disable PURGE.",
                )

        spawn_ui_task(
            disable_job()
        )

        return

    # ========================================================
    # STATUS
    # ========================================================

    if data.startswith(
        "status:"
    ):

        try:

            chat_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

        except ValueError:

            return

        await safe_edit(
            query,
            "📊 <b>STATUS</b>\n\n"
            "⏳ Loading...",
        )

        spawn_ui_task(
            load_status_ui(
                query,
                chat_id,
            )
        )

        return

    # ========================================================
    # ADMINS
    # ========================================================

    if data == "admins":

        await safe_edit(
            query,
            "👑 <b>ADMIN MANAGEMENT</b>",
            admin_keyboard(),
        )

        return

    # ========================================================
    # ADMIN LIST
    # ========================================================

    if data == "admin_list":

        await safe_edit(
            query,
            "👥 <b>ADMIN LIST</b>\n\n"
            "⏳ Loading...",
        )

        spawn_ui_task(
            load_admin_list_ui(
                query
            )
        )

        return

    # ========================================================
    # ADD ADMIN
    # ========================================================

    if data == "admin_add":

        admin_input_state[
            user.id
        ] = {
            "type": "add_admin"
        }

        await safe_edit(
            query,
            "➕ <b>ADD ADMIN</b>\n\n"
            "Send the Telegram numeric user ID.\n\n"
            "Example:\n"
            "<code>123456789</code>\n\n"
            "Use /cancel to cancel.",
        )

        return

    # ========================================================
    # REMOVE ADMIN
    # ========================================================

    if data == "admin_remove":

        admin_input_state[
            user.id
        ] = {
            "type": "remove_admin"
        }

        await safe_edit(
            query,
            "❌ <b>REMOVE ADMIN</b>\n\n"
            "Send the Telegram numeric user ID.\n\n"
            "ENV admins are protected.\n\n"
            "Use /cancel to cancel.",
        )

        return


# ============================================================
# PRIVATE ADMIN TEXT
# ============================================================

async def private_text_handler(
    update,
    context,
):

    message = update.message

    if not message:
        return

    user = update.effective_user

    if not user:
        return

    if not await is_db_admin(
        user.id
    ):
        return

    state = admin_input_state.get(
        user.id
    )

    if not state:
        return

    text = (
        message.text or ""
    ).strip()

    # ========================================================
    # ADD ADMIN
    # ========================================================

    if state["type"] == "add_admin":

        try:

            user_id = int(
                text
            )

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
                f"✅ Admin added:\n"
                f"<code>{user_id}</code>",
                parse_mode="HTML",
                reply_markup=admin_keyboard(),
            )

        else:

            await message.reply_text(
                "🔒 This admin is protected by ENV."
            )

        return

    # ========================================================
    # REMOVE ADMIN
    # ========================================================

    if state["type"] == "remove_admin":

        try:

            user_id = int(
                text
            )

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
                f"✅ Admin removed:\n"
                f"<code>{user_id}</code>",
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

    # ========================================================
    # CUSTOM PURGE
    # ========================================================

    if state["type"] == "purge_custom":

        chat_id = state[
            "chat_id"
        ]

        try:

            minutes = int(
                text
            )

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

        if (
            not chat
            or chat.get("type")
            not in (
                "group",
                "supergroup",
            )
        ):

            admin_input_state.pop(
                user.id,
                None,
            )

            await message.reply_text(
                "❌ PURGE is available only "
                "for Groups/Supergroups."
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
# BOT ADDED / REMOVED FROM CHAT
# ============================================================

async def my_chat_member_handler(
    update,
    context,
):

    member_update = (
        update.my_chat_member
    )

    if not member_update:
        return

    chat = update.effective_chat

    if not chat:
        return

    status = (
        member_update
        .new_chat_member
        .status
    )

    chat_type = chat.type

    if chat_type not in (
        "group",
        "supergroup",
        "channel",
    ):
        return

    # --------------------------------------------------------
    # BOT ACTIVE
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # BOT LEFT
    # --------------------------------------------------------

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
# GROUP MESSAGE WATCHER
# ============================================================

async def message_watcher(
    update,
    context,
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
    update,
    context,
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


async def update_router(
    update,
    context,
):

    if update.channel_post:

        await channel_post_watcher(
            update,
            context,
        )


# ============================================================
# STARTUP
# ============================================================

async def post_init(
    application
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

    # --------------------------------------------------------
    # ENV CHECK
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # MONGO
    # --------------------------------------------------------

    mongo = AsyncIOMotorClient(
        MONGO_URI
    )

    db = mongo[
        DB_NAME
    ]

    chats_col = db[
        "chats"
    ]

    admins_col = db[
        "admins"
    ]

    dclean_state_col = db[
        "dclean_state"
    ]

    media_col = db[
        "media_fingerprints"
    ]

    pending_col = db[
        "dclean_pending"
    ]

    purge_settings_col = db[
        "purge_settings"
    ]

    await setup_indexes()

    logger.info(
        "MongoDB initialized safely."
    )

    # --------------------------------------------------------
    # TELETHON
    # --------------------------------------------------------

    telethon_client = TelegramClient(
        StringSession(
            STRING_SESSION
        ),
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

    # --------------------------------------------------------
    # RESTORE PURGE
    # --------------------------------------------------------

    await restore_purge_workers()

    logger.info(
        "PURGE workers restored."
    )


# ============================================================
# SHUTDOWN
# ============================================================

async def post_shutdown(
    application
):

    global mongo
    global telethon_client

    # --------------------------------------------------------
    # STOP PURGE
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # STOP UI TASKS
    # --------------------------------------------------------

    ui_job_tasks = list(
        ui_tasks
    )

    for task in ui_job_tasks:

        task.cancel()

    if ui_job_tasks:

        await asyncio.gather(
            *ui_job_tasks,
            return_exceptions=True,
        )

    ui_tasks.clear()

    # --------------------------------------------------------
    # STOP DCLEAN
    # --------------------------------------------------------

    dc_tasks = list(
        dclean_tasks.values()
    )

    for task in dc_tasks:

        task.cancel()

    if dc_tasks:

        await asyncio.gather(
            *dc_tasks,
            return_exceptions=True,
        )

    dclean_tasks.clear()

    # --------------------------------------------------------
    # TELETHON
    # --------------------------------------------------------

    if telethon_client:

        try:

            await telethon_client.disconnect()

        except Exception:

            logger.exception(
                "Telethon disconnect failed"
            )

    # --------------------------------------------------------
    # MONGO
    # --------------------------------------------------------

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
# APPLICATION
# ============================================================

async def run_application():

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .build()
    )

    # --------------------------------------------------------
    # COMMANDS
    # --------------------------------------------------------

    application.add_handler(
        CommandHandler(
            "start",
            start_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "cancel",
            cancel_command,
        )
    )

    # --------------------------------------------------------
    # BOT CHAT MEMBER
    # --------------------------------------------------------

    application.add_handler(
        ChatMemberHandler(
            my_chat_member_handler,
            ChatMemberHandler.MY_CHAT_MEMBER,
        )
    )

    # --------------------------------------------------------
    # BUTTONS
    # --------------------------------------------------------

    application.add_handler(
        CallbackQueryHandler(
            callback_handler
        )
    )

    # --------------------------------------------------------
    # PRIVATE ADMIN TEXT
    # --------------------------------------------------------

    application.add_handler(
        MessageHandler(
            filters.TEXT
            & ~filters.COMMAND
            & filters.ChatType.PRIVATE,
            private_text_handler,
        )
    )

    # --------------------------------------------------------
    # CHANNEL POSTS
    # --------------------------------------------------------

    application.add_handler(
        TypeHandler(
            Update,
            update_router,
        ),
        group=1,
    )

    # --------------------------------------------------------
    # GROUP/SUPERGROUP MESSAGES
    # --------------------------------------------------------

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

    # ========================================================
    # MANUAL PTB LIFECYCLE
    #
    # This avoids Python 3.14's
    # "There is no current event loop"
    # issue with run_polling().
    # ========================================================

    await application.initialize()

    await post_init(
        application
    )

    await application.start()

    if application.updater is None:

        raise RuntimeError(
            "Telegram updater is not available."
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

    except asyncio.CancelledError:

        logger.info(
            "Application cancellation received."
        )

        raise

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

            await post_shutdown(
                application
            )

        except Exception:

            logger.exception(
                "Post shutdown failed"
            )

        try:

            await application.shutdown()

        except Exception:

            logger.exception(
                "Application shutdown failed"
            )


# ============================================================
# MAIN
# ============================================================

def main():

    asyncio.run(
        run_application()
    )


if __name__ == "__main__":

    main()
