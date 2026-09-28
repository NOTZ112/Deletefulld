import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from html import escape

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, UpdateOne

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


# Active workers
purge_tasks = {}
dclean_tasks = {}
ui_tasks = set()

# DELETE ALL workers
delete_all_tasks = {}

# DELETE ALL temporary selections
delete_all_selections = {}

# Bot user ID cache
bot_user_id = None


# Temporary input states
admin_input_state = {}


# Fast in-memory caches
admin_cache = set()
registered_chat_cache = {}
REGISTER_CACHE_TTL = 300


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
# BASIC HELPERS
# ============================================================

def now_utc():
    return datetime.now(timezone.utc)


def is_env_admin(user_id):
    return user_id in ENV_ADMIN_IDS


def is_admin_fast(user_id):
    """
    Fast admin check.
    No MongoDB query here.
    """
    return user_id in admin_cache


async def refresh_admin_cache():
    global admin_cache

    ids = set(ENV_ADMIN_IDS)

    if admins_col is not None:

        cursor = admins_col.find(
            {},
            {
                "user_id": 1,
                "_id": 0,
            },
        )

        async for doc in cursor:

            uid = doc.get("user_id")

            if isinstance(uid, int):
                ids.add(uid)

    admin_cache = ids


async def is_db_admin(user_id):
    """
    Kept for places where an authoritative DB check is useful.
    UI callbacks use is_admin_fast().
    """

    if user_id in ENV_ADMIN_IDS:
        return True

    return user_id in admin_cache


async def get_admin_list():

    await refresh_admin_cache()

    return sorted(admin_cache)


# ============================================================
# FAST BACKGROUND TASK
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


# ============================================================
# TELEGRAM UI HELPERS
# ============================================================

async def safe_answer(query):

    try:
        await query.answer()

    except Exception:
        pass


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


async def safe_edit_message(
    bot,
    chat_id,
    message_id,
    text,
    markup=None,
):

    try:

        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            parse_mode="HTML",
            reply_markup=markup,
        )

    except Exception as e:

        if "not modified" not in str(e).lower():

            logger.debug(
                "Background edit failed: %s",
                e,
            )


# ============================================================
# INDEX MIGRATION
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

            await collection.drop_index(
                name
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
                "updated_at": now_utc(),
            },
            "$setOnInsert": {
                "created_at": now_utc(),
            },
        },
        upsert=True,
    )

    admin_cache.add(user_id)

    return True


async def remove_admin(user_id):

    if user_id in ENV_ADMIN_IDS:
        return False

    result = await admins_col.delete_one(
        {"user_id": user_id}
    )

    if result.deleted_count:
        admin_cache.discard(user_id)

        return True

    return False


# ============================================================
# CHAT DATABASE
# ============================================================

async def register_chat(
    chat_id,
    title,
    chat_type,
    force=False,
):

    cache_key = chat_id
    current_time = asyncio.get_running_loop().time()

    last_update = registered_chat_cache.get(
        cache_key
    )

    # IMPORTANT:
    # Do not write MongoDB on every single message.
    if (
        not force
        and last_update is not None
        and current_time - last_update
        < REGISTER_CACHE_TTL
    ):
        return

    registered_chat_cache[
        cache_key
    ] = current_time

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

    registered_chat_cache.pop(
        chat_id,
        None,
    )

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
# DELETE ALL - BOT ADMIN CHATS
# ============================================================

async def get_bot_admin_chats(application):

    global bot_user_id

    try:

        if bot_user_id is None:

            me = await application.bot.get_me()

            bot_user_id = me.id

    except Exception:

        logger.exception(
            "Could not get bot user"
        )

        return []

    chats = await get_all_chats()

    admin_chats = []

    for chat in chats:

        chat_id = chat.get("chat_id")

        chat_type = chat.get("type")

        if chat_type not in (
            "group",
            "supergroup",
            "channel",
        ):
            continue

        try:

            member = await application.bot.get_chat_member(
                chat_id,
                bot_user_id,
            )

            if member.status in (
                ChatMemberStatus.ADMINISTRATOR,
                ChatMemberStatus.OWNER,
            ):

                admin_chats.append(chat)

        except Exception as e:

            logger.debug(
                "Could not check bot admin status for %s: %s",
                chat_id,
                e,
            )

    return admin_chats


def delete_all_list_keyboard(
    chats,
    selected,
):

    buttons = []

    for chat in chats:

        chat_id = chat.get("chat_id")

        title = (
            chat.get("title")
            or str(chat_id)
        )

        if len(title) > 28:
            title = title[:25] + "..."

        chat_type = chat.get("type")

        icon = {
            "channel": "📢",
            "supergroup": "👥",
            "group": "👤",
        }.get(
            chat_type,
            "💬",
        )

        mark = (
            "✅"
            if chat_id in selected
            else "☐"
        )

        buttons.append([
            InlineKeyboardButton(
                f"{mark} {icon} {title}",
                callback_data=(
                    f"delsel:{chat_id}"
                ),
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            "🗑️ DELETE SELECTED",
            callback_data="delconfirm",
        )
    ])

    buttons.append([
        InlineKeyboardButton(
            "☑️ SELECT ALL",
            callback_data="delselall",
        ),
        InlineKeyboardButton(
            "❌ CLEAR",
            callback_data="delclear",
        ),
    ])

    buttons.append([
        InlineKeyboardButton(
            "⬅️ BACK",
            callback_data="home",
        )
    ])

    return InlineKeyboardMarkup(buttons)


async def load_delete_all_ui(
    query,
    user_id,
    application,
):

    try:

        chats = await get_bot_admin_chats(
            application
        )

        if not chats:

            await safe_edit(
                query,
                "🗑️ <b>DELETE ALL</b>\n\n"
                "❌ No Groups/Channels found "
                "where the bot is an administrator.",
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

        selected = delete_all_selections.setdefault(
            user_id,
            set(),
        )

        # Remove stale selections
        available_ids = {
            chat.get("chat_id")
            for chat in chats
        }

        selected.intersection_update(
            available_ids
        )

        await safe_edit(
            query,
            "🗑️ <b>DELETE ALL</b>\n\n"
            "Select one or more Groups/Channels.\n\n"
            f"Selected: <b>{len(selected)}</b>",
            delete_all_list_keyboard(
                chats,
                selected,
            ),
        )

    except Exception:

        logger.exception(
            "DELETE ALL list failed"
        )

        await safe_edit(
            query,
            "❌ Failed to load Delete All list.",
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
# PIPE LOG
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

                return (
                    f"photo:{photo_id}"
                )

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

                return (
                    f"document:{document_id}"
                )

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
        {"chat_id": chat_id},
        {
            "last_message_id": 1,
            "_id": 0,
        },
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

    if not message_id:
        return

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
        {"chat_id": chat_id},
        {
            "message_id": 1,
            "_id": 0,
        },
    )

    pending = await cursor.to_list(
        length=5000
    )

    if not pending:
        return 0

    deleted = 0

    # Delete in batches
    batch_size = 50

    for start in range(
        0,
        len(pending),
        batch_size,
    ):

        batch = pending[
            start:start + batch_size
        ]

        ids = [
            int(item["message_id"])
            for item in batch
            if item.get("message_id")
        ]

        if not ids:
            continue

        result = await tg_call(
            telethon_client.delete_messages,
            entity,
            ids,
        )

        if result is not None:

            deleted += len(ids)

            await pending_col.delete_many(
                {
                    "chat_id": chat_id,
                    "message_id": {
                        "$in": ids
                    },
                }
            )

    return deleted


# ============================================================
# DELETE BATCH
# ============================================================

async def delete_message_batch(
    entity,
    chat_id,
    items,
):

    if not items:
        return 0, 0

    ids = [
        int(item["message_id"])
        for item in items
        if item.get("message_id")
    ]

    if not ids:
        return 0, 0

    result = await tg_call(
        telethon_client.delete_messages,
        entity,
        ids,
    )

    if result is not None:

        await pending_col.delete_many(
            {
                "chat_id": chat_id,
                "message_id": {
                    "$in": ids
                },
            }
        )

        return len(ids), 0

    # If batch failed, save all as pending
    for item in items:

        await add_pending_delete(
            chat_id,
            item["message_id"],
            item.get("fingerprint"),
        )

    return 0, len(ids)


# ============================================================
# DELETE ALL - FULL CHAT HISTORY
# ============================================================

async def run_delete_all_chat(
    chat_id,
    status_callback=None,
):

    chat = await get_chat(
        chat_id
    )

    if not chat:

        return {
            "chat_id": chat_id,
            "title": str(chat_id),
            "scanned": 0,
            "deleted": 0,
            "failed": 0,
        }

    title = (
        chat.get("title")
        or str(chat_id)
    )

    chat_type = chat.get("type")

    if chat_type not in (
        "group",
        "supergroup",
        "channel",
    ):

        return {
            "chat_id": chat_id,
            "title": title,
            "scanned": 0,
            "deleted": 0,
            "failed": 0,
        }

    try:

        entity = await telethon_client.get_entity(
            chat_id
        )

    except Exception:

        logger.exception(
            "DELETE ALL entity error: %s",
            chat_id,
        )

        return {
            "chat_id": chat_id,
            "title": title,
            "scanned": 0,
            "deleted": 0,
            "failed": 0,
        }

    scanned = 0
    deleted = 0
    failed = 0

    delete_batch = []

    async def flush_batch():

        nonlocal delete_batch
        nonlocal deleted
        nonlocal failed

        if not delete_batch:
            return

        ids = [
            int(item)
            for item in delete_batch
            if item
        ]

        delete_batch = []

        if not ids:
            return

        try:

            result = await tg_call(
                telethon_client.delete_messages,
                entity,
                ids,
            )

            if result is not None:

                deleted += len(ids)

            else:

                failed += len(ids)

        except Exception:

            logger.exception(
                "DELETE ALL batch failed: %s",
                chat_id,
            )

            failed += len(ids)

    try:

        # Scan the complete available history.
        # Oldest -> newest.
        async for message in telethon_client.iter_messages(
            entity,
            reverse=True,
        ):

            message_id = getattr(
                message,
                "id",
                0,
            )

            if not message_id:
                continue

            scanned += 1

            delete_batch.append(
                message_id
            )

            # Batch size kept at 100.
            if len(delete_batch) >= 100:

                await flush_batch()

                if status_callback:

                    try:

                        await status_callback(
                            scanned,
                            deleted,
                            failed,
                        )

                    except Exception:
                        pass

    except Exception:

        logger.exception(
            "DELETE ALL history scan failed: %s",
            chat_id,
        )

    # Final batch
    await flush_batch()

    return {
        "chat_id": chat_id,
        "title": title,
        "scanned": scanned,
        "deleted": deleted,
        "failed": failed,
    }


async def run_delete_all_selected(
    chat_ids,
    status_callback=None,
):

    total_scanned = 0
    total_deleted = 0
    total_failed = 0

    results = []

    for chat_id in chat_ids:

        async def chat_progress(
            scanned,
            deleted,
            failed,
        ):

            if status_callback:

                await status_callback(
                    chat_id,
                    scanned,
                    deleted,
                    failed,
                )

        result = await run_delete_all_chat(
            chat_id,
            chat_progress,
        )

        results.append(
            result
        )

        total_scanned += result[
            "scanned"
        ]

        total_deleted += result[
            "deleted"
        ]

        total_failed += result[
            "failed"
        ]

    return {
        "results": results,
        "total_scanned": total_scanned,
        "total_deleted": total_deleted,
        "total_failed": total_failed,
    }


# ============================================================
# LOAD KNOWN MEDIA
# ============================================================

async def load_known_fingerprints(
    chat_id
):

    try:

        values = await media_col.distinct(
            "fingerprint",
            {
                "chat_id": chat_id,
                "fingerprint": {
                    "$exists": True
                },
            },
        )

        return {
            value
            for value in values
            if value
        }

    except Exception:

        logger.exception(
            "Could not load fingerprints"
        )

        return set()


# ============================================================
# DCLEAN
# ============================================================

async def run_dclean(
    chat_id,
    status_callback=None,
):

    chat = await get_chat(
        chat_id
    )

    if not chat:

        return (
            "❌ <b>Chat not registered.</b>"
        )

    chat_type = chat.get(
        "type"
    )

    # DCLEAN IS ALLOWED FOR ALL THREE
    if chat_type not in (
        "group",
        "supergroup",
        "channel",
    ):

        return (
            "❌ Unsupported chat type."
        )

    last_id = await get_last_dclean_id(
        chat_id
    )

    scanned = 0
    duplicates = 0
    deleted = 0
    failed = 0

    highest_id = last_id

    # --------------------------------------------------------
    # TELETHON ENTITY
    # --------------------------------------------------------

    try:

        entity = await telethon_client.get_entity(
            chat_id
        )

    except Exception as e:

        logger.exception(
            "DCLEAN entity error"
        )

        return (
            "❌ <b>Could not access this chat.</b>\n\n"
            "Make sure the Telethon account has access "
            "to the group/channel and can delete messages."
        )

    # --------------------------------------------------------
    # RETRY PENDING
    # --------------------------------------------------------

    try:

        deleted += await retry_pending_deletes(
            entity,
            chat_id,
        )

    except Exception:

        logger.exception(
            "Pending delete retry failed"
        )

    # --------------------------------------------------------
    # LOAD EXISTING FINGERPRINTS
    # --------------------------------------------------------

    seen = await load_known_fingerprints(
        chat_id
    )

    logger.info(
        "DCLEAN %s started from message ID %s",
        chat_id,
        last_id,
    )

    # --------------------------------------------------------
    # BULK MEDIA DATABASE OPS
    # --------------------------------------------------------

    media_ops = []

    delete_batch = []

    async def flush_media_ops():

        nonlocal media_ops

        if not media_ops:
            return

        try:

            await media_col.bulk_write(
                media_ops,
                ordered=False,
            )

        except Exception:

            logger.exception(
                "Media bulk write failed"
            )

        media_ops = []

    async def flush_delete_batch():

        nonlocal delete_batch
        nonlocal deleted
        nonlocal failed

        if not delete_batch:
            return

        ok, bad = await delete_message_batch(
            entity,
            chat_id,
            delete_batch,
        )

        deleted += ok
        failed += bad

        delete_batch = []

    # --------------------------------------------------------
    # HISTORY SCAN
    # --------------------------------------------------------

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

                # ------------------------------------------------
                # DUPLICATE
                # ------------------------------------------------

                if fingerprint in seen:

                    duplicates += 1

                    delete_batch.append({
                        "message_id": message_id,
                        "fingerprint": fingerprint,
                    })

                    # Delete every 50 duplicates
                    if len(delete_batch) >= 50:

                        await flush_delete_batch()

                # ------------------------------------------------
                # ORIGINAL
                # ------------------------------------------------

                else:

                    seen.add(
                        fingerprint
                    )

                    media_ops.append(
                        UpdateOne(
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
                    )

                    if len(media_ops) >= 100:

                        await flush_media_ops()

            # ------------------------------------------------
            # SAVE PROGRESS
            # ------------------------------------------------

            if scanned % 100 == 0:

                await flush_delete_batch()
                await flush_media_ops()

                await save_last_dclean_id(
                    chat_id,
                    highest_id,
                )

                if status_callback:

                    try:

                        await status_callback(
                            scanned,
                            duplicates,
                            deleted,
                            failed,
                            highest_id,
                        )

                    except Exception:

                        pass

    except Exception:

        logger.exception(
            "DCLEAN history scan failed"
        )

    # --------------------------------------------------------
    # FINAL FLUSH
    # --------------------------------------------------------

    await flush_delete_batch()
    await flush_media_ops()

    await save_last_dclean_id(
        chat_id,
        highest_id,
    )

    return (
        "♻️ <b>DCLEAN COMPLETE</b>\n\n"
        f"📂 Type: <b>{escape(chat_type)}</b>\n"
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
        "minutes": max(
            1,
            int(
                doc.get(
                    "minutes",
                    60,
                )
                or 60
            ),
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

        update["minutes"] = max(
            1,
            int(minutes),
        )

    if delete_duplicates is not None:

        update[
            "delete_duplicates"
        ] = bool(
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

    cutoff = (
        now_utc()
        - timedelta(
            minutes=settings["minutes"]
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
    failed = 0

    duplicate_deleted = 0

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # We only need to inspect messages from newest to oldest.
    # Once we hit the first old message, everything after it
    # is also old.
    # --------------------------------------------------------

    delete_batch = []
    seen_duplicates = set()

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

            if message_date is None:
                continue

            if message_date.tzinfo is None:

                message_date = message_date.replace(
                    tzinfo=timezone.utc
                )

            # ------------------------------------------------
            # New enough -> skip
            # ------------------------------------------------

            if message_date >= cutoff:

                continue

            fingerprint = get_media_fingerprint(
                message
            )

            # ------------------------------------------------
            # Duplicate tracking
            # ------------------------------------------------

            if (
                settings["delete_duplicates"]
                and fingerprint
            ):

                if fingerprint in seen_duplicates:

                    duplicate_deleted += 1

                else:

                    seen_duplicates.add(
                        fingerprint
                    )

            delete_batch.append({
                "message_id": message.id,
                "fingerprint": fingerprint,
            })

            # ------------------------------------------------
            # Batch delete
            # ------------------------------------------------

            if len(delete_batch) >= 50:

                ok, bad = await delete_message_batch(
                    entity,
                    chat_id,
                    delete_batch,
                )

                deleted += ok
                failed += bad

                delete_batch = []

    except Exception:

        logger.exception(
            "PURGE history error"
        )

    # Final delete
    if delete_batch:

        ok, bad = await delete_message_batch(
            entity,
            chat_id,
            delete_batch,
        )

        deleted += ok
        failed += bad

    return {
        "scanned": scanned,
        "deleted": deleted,
        "failed": failed,
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

            # Check again after 60 seconds
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

    if (
        existing
        and not existing.done()
    ):
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
        {
            "enabled": True
        }
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
                "🗑️ DELETE ALL",
                callback_data="deleteall",
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

        chat_type = chat.get(
            "type"
        )

        if len(title) > 32:

            title = (
                title[:29]
                + "..."
            )

        icon = {
            "channel": "📢",
            "supergroup": "👥",
            "group": "👤",
        }.get(
            chat_type,
            "💬",
        )

        buttons.append([
            InlineKeyboardButton(
                f"{icon} {title}",
                callback_data=(
                    f"chat:{chat_id}"
                ),
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

    chat_id = chat["chat_id"]

    chat_type = chat.get(
        "type"
    )

    buttons = [
        [
            InlineKeyboardButton(
                "♻️ DCLEAN",
                callback_data=(
                    f"dclean:{chat_id}"
                ),
            )
        ]
    ]

    # PURGE ONLY GROUP / SUPERGROUP
    if chat_type in (
        "group",
        "supergroup",
    ):

        buttons.append([
            InlineKeyboardButton(
                "🗑️ PURGE",
                callback_data=(
                    f"purge:{chat_id}"
                ),
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            "📊 STATUS",
            callback_data=(
                f"status:{chat_id}"
            ),
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
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    user = update.effective_user

    if not user:
        return

    # FAST MEMORY CHECK
    if not is_admin_fast(
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
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    user = update.effective_user

    if user:

        admin_input_state.pop(
            user.id,
            None,
        )

        delete_all_selections.pop(
            user.id,
            None,
        )

    await update.message.reply_text(
        "❌ Cancelled."
    )


# ============================================================
# UI LOADERS
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

        chat_type = escape(
            str(
                chat.get(
                    "type",
                    "unknown",
                )
            )
        )

        await safe_edit(
            query,
            f"📂 <b>{title}</b>\n\n"
            f"Type: <b>{chat_type}</b>\n\n"
            "♻️ DCLEAN: <b>Available</b>\n"
            + (
                "🗑️ PURGE: <b>Available</b>"
                if chat.get("type")
                in (
                    "group",
                    "supergroup",
                )
                else
                "🗑️ PURGE: <b>Not available in channels</b>"
            ),
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

        chat_type = chat.get(
            "type"
        )

        if chat_type == "channel":

            purge_text = (
                "🗑️ PURGE: <b>Not available</b>"
            )

        else:

            purge_text = (
                f"🗑️ PURGE: <b>"
                f"{'ON' if settings['enabled'] else 'OFF'}"
                f"</b>\n"
                f"⏱️ Delay: <b>{settings['minutes']} min</b>\n"
                f"♻️ Duplicate Delete: <b>"
                f"{'ON' if settings['delete_duplicates'] else 'OFF'}"
                f"</b>"
            )

        await safe_edit(
            query,
            "📊 <b>STATUS</b>\n\n"
            f"Chat: <b>"
            f"{escape(chat.get('title', str(chat_id)))}"
            f"</b>\n"
            f"Type: <b>{escape(str(chat_type))}</b>\n\n"
            f"♻️ DCLEAN Last ID: <b>{last_id}</b>\n"
            f"{purge_text}",
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
# DCLEAN PROGRESS
# ============================================================

async def update_dclean_progress(
    query,
    scanned,
    duplicates,
    deleted,
    failed,
    last_id,
):

    await safe_edit(
        query,
        "♻️ <b>DCLEAN RUNNING</b>\n\n"
        f"📖 Scanned: <b>{scanned}</b>\n"
        f"♻️ Duplicates: <b>{duplicates}</b>\n"
        f"🗑️ Deleted: <b>{deleted}</b>\n"
        f"⏳ Pending: <b>{failed}</b>\n"
        f"🆔 Last ID: <b>{last_id}</b>\n\n"
        "⚡ Running in background...",
    )


# ============================================================
# CALLBACK HANDLER
# ============================================================

async def callback_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    query = update.callback_query

    if not query:
        return

    # ========================================================
    # ABSOLUTE FIRST ACTION
    # ========================================================

    await safe_answer(query)

    user = query.from_user

    if not user:
        return

    # ========================================================
    # FAST ADMIN CHECK
    # NO MONGO QUERY
    # ========================================================

    if not is_admin_fast(
        user.id
    ):

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
    # DELETE ALL MENU
    # ========================================================

    if data == "deleteall":

        delete_all_selections[
            user.id
        ] = set()

        await safe_edit(
            query,
            "🗑️ <b>DELETE ALL</b>\n\n"
            "⚡ Loading your Groups/Channels...",
        )

        spawn_ui_task(
            load_delete_all_ui(
                query,
                user.id,
                context.application,
            )
        )

        return

    # ========================================================
    # DELETE ALL - SELECT CHAT
    # ========================================================

    if data.startswith("delsel:"):

        try:

            chat_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

        except ValueError:

            return

        chats = await get_bot_admin_chats(
            context.application
        )

        available_ids = {
            chat.get("chat_id")
            for chat in chats
        }

        if chat_id not in available_ids:

            await safe_edit(
                query,
                "❌ This chat is no longer "
                "available for DELETE ALL.",
            )

            return

        selected = delete_all_selections.setdefault(
            user.id,
            set(),
        )

        if chat_id in selected:

            selected.remove(
                chat_id
            )

        else:

            selected.add(
                chat_id
            )

        await safe_edit(
            query,
            "🗑️ <b>DELETE ALL</b>\n\n"
            "Select one or more Groups/Channels.\n\n"
            f"Selected: <b>{len(selected)}</b>",
            delete_all_list_keyboard(
                chats,
                selected,
            ),
        )

        return

    # ========================================================
    # DELETE ALL - SELECT ALL
    # ========================================================

    if data == "delselall":

        chats = await get_bot_admin_chats(
            context.application
        )

        selected = delete_all_selections.setdefault(
            user.id,
            set(),
        )

        selected.clear()

        for chat in chats:

            chat_id = chat.get(
                "chat_id"
            )

            if chat_id:
                selected.add(
                    chat_id
                )

        await safe_edit(
            query,
            "🗑️ <b>DELETE ALL</b>\n\n"
            "All available chats selected.\n\n"
            f"Selected: <b>{len(selected)}</b>",
            delete_all_list_keyboard(
                chats,
                selected,
            ),
        )

        return

    # ========================================================
    # DELETE ALL - CLEAR
    # ========================================================

    if data == "delclear":

        delete_all_selections[
            user.id
        ] = set()

        await load_delete_all_ui(
            query,
            user.id,
            context.application,
        )

        return

    # ========================================================
    # DELETE ALL - CONFIRM
    # ========================================================

    if data == "delconfirm":

        selected = delete_all_selections.get(
            user.id,
            set(),
        )

        if not selected:

            await safe_edit(
                query,
                "⚠️ <b>Nothing selected.</b>\n\n"
                "Please select at least one "
                "Group or Channel.",
                InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(
                            "⬅️ BACK",
                            callback_data="deleteall",
                        )
                    ]
                ]),
            )

            return

        chats = await get_bot_admin_chats(
            context.application
        )

        selected_chats = [
            chat
            for chat in chats
            if chat.get("chat_id")
            in selected
        ]

        if not selected_chats:

            delete_all_selections[
                user.id
            ] = set()

            await safe_edit(
                query,
                "❌ No selected chats are currently "
                "available.",
                InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(
                            "⬅️ BACK",
                            callback_data="deleteall",
                        )
                    ]
                ]),
            )

            return

        text = (
            "⚠️ <b>DELETE ALL CONFIRMATION</b>\n\n"
            "The following chats are selected:\n\n"
        )

        for chat in selected_chats:

            title = escape(
                chat.get(
                    "title",
                    str(
                        chat.get(
                            "chat_id"
                        )
                    ),
                )
            )

            icon = (
                "📢"
                if chat.get("type") == "channel"
                else "👥"
            )

            text += (
                f"{icon} <b>{title}</b>\n"
            )

        text += (
            "\n⚠️ <b>ALL AVAILABLE MESSAGES</b> "
            "IN THESE CHATS WILL BE DELETED.\n\n"
            "This action cannot be undone."
        )

        await safe_edit(
            query,
            text,
            InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(
                        "✅ YES, DELETE ALL",
                        callback_data="delstart",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "❌ CANCEL",
                        callback_data="deleteall",
                    )
                ],
            ]),
        )

        return

    # ========================================================
    # DELETE ALL - START
    # ========================================================

    if data == "delstart":

        selected = delete_all_selections.get(
            user.id,
            set(),
        )

        if not selected:

            await safe_edit(
                query,
                "❌ No chats selected.",
                InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(
                            "⬅️ BACK",
                            callback_data="deleteall",
                        )
                    ]
                ]),
            )

            return

        # Re-check current bot-admin chats immediately
        # before destructive operation.
        available_chats = await get_bot_admin_chats(
            context.application
        )

        available_ids = {
            chat.get("chat_id")
            for chat in available_chats
        }

        chat_ids = [
            cid
            for cid in selected
            if cid in available_ids
        ]

        if not chat_ids:

            delete_all_selections[
                user.id
            ] = set()

            await safe_edit(
                query,
                "❌ None of the selected chats are "
                "currently available for DELETE ALL.",
                InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(
                            "⬅️ BACK",
                            callback_data="deleteall",
                        )
                    ]
                ]),
            )

            return

        # Prevent duplicate workers
        running = [
            cid
            for cid in chat_ids
            if (
                cid in delete_all_tasks
                and not delete_all_tasks[
                    cid
                ].done()
            )
        ]

        if running:

            await safe_edit(
                query,
                "⚠️ <b>DELETE ALL ALREADY RUNNING</b>\n\n"
                "Please wait for the current operation.",
            )

            return

        delete_all_selections[
            user.id
        ] = set()

        await safe_edit(
            query,
            "🗑️ <b>DELETE ALL STARTED</b>\n\n"
            f"Chats: <b>{len(chat_ids)}</b>\n\n"
            "⚡ Running in background...",
        )

        async def delete_all_job():

            try:

                current_task = asyncio.current_task()

                for cid in chat_ids:

                    delete_all_tasks[
                        cid
                    ] = current_task

                async def progress(
                    cid,
                    scanned,
                    deleted,
                    failed,
                ):

                    chat = await get_chat(
                        cid
                    )

                    title = (
                        chat.get("title")
                        if chat
                        else str(cid)
                    )

                    await safe_edit(
                        query,
                        "🗑️ <b>DELETE ALL RUNNING</b>\n\n"
                        f"📂 Chat: <b>"
                        f"{escape(title or str(cid))}"
                        f"</b>\n\n"
                        f"📖 Scanned: <b>{scanned}</b>\n"
                        f"🗑️ Deleted: <b>{deleted}</b>\n"
                        f"⏳ Failed: <b>{failed}</b>\n\n"
                        "⚡ Please wait...",
                    )

                result = await run_delete_all_selected(
                    chat_ids,
                    progress,
                )

                text = (
                    "✅ <b>DELETE ALL COMPLETED</b>\n\n"
                    f"📂 Chats: <b>{len(chat_ids)}</b>\n"
                    f"📖 Scanned: <b>"
                    f"{result['total_scanned']}"
                    f"</b>\n"
                    f"🗑️ Deleted: <b>"
                    f"{result['total_deleted']}"
                    f"</b>\n"
                    f"⏳ Failed: <b>"
                    f"{result['total_failed']}"
                    f"</b>\n"
                )

                await safe_edit(
                    query,
                    text,
                    InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton(
                                "🗑️ DELETE AGAIN",
                                callback_data="deleteall",
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                "⬅️ HOME",
                                callback_data="home",
                            )
                        ],
                    ]),
                )

            except asyncio.CancelledError:

                raise

            except Exception:

                logger.exception(
                    "DELETE ALL background error"
                )

                await safe_edit(
                    query,
                    "❌ <b>DELETE ALL FAILED</b>\n\n"
                    "Check Render logs.",
                    InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton(
                                "⬅️ HOME",
                                callback_data="home",
                            )
                        ]
                    ]),
                )

            finally:

                for cid in chat_ids:

                    delete_all_tasks.pop(
                        cid,
                        None,
                    )

        task = asyncio.create_task(
            delete_all_job()
        )

        for cid in chat_ids:

            delete_all_tasks[
                cid
            ] = task

        return

    # ========================================================
    # CHATS
    # ========================================================

    if data == "chats":

        await safe_edit(
            query,
            "📋 <b>SELECT CHAT</b>\n\n"
            "⚡ Loading...",
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

        existing = dclean_tasks.get(
            chat_id
        )

        if (
            existing
            and not existing.done()
        ):

            await safe_edit(
                query,
                "♻️ <b>DCLEAN ALREADY RUNNING</b>\n\n"
                "Please wait for the current scan.",
            )

            return

        # ----------------------------------------------------
        # INSTANT UI
        # ----------------------------------------------------

        await safe_edit(
            query,
            "♻️ <b>DCLEAN STARTED</b>\n\n"
            "⚡ Running in background...\n\n"
            "This window will update with progress.",
        )

        async def dclean_job():

            try:

                async def progress(
                    scanned,
                    duplicates,
                    deleted,
                    failed,
                    last_id,
                ):

                    await update_dclean_progress(
                        query,
                        scanned,
                        duplicates,
                        deleted,
                        failed,
                        last_id,
                    )

                result = await run_dclean(
                    chat_id,
                    progress,
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
                    "❌ <b>DCLEAN FAILED</b>\n\n"
                    "Check Render logs.",
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
            "⚡ Loading...",
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

    if data.startswith(
        "purgeset:"
    ):

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
            "🗑️ <b>Updating...</b>",
        )

        async def preset_job():

            try:

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

                    await safe_edit(
                        query,
                        "❌ PURGE is available only "
                        "for Groups/Supergroups.",
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
                    f"Status: <b>"
                    f"{'🟢 ENABLED' if settings['enabled'] else '🔴 DISABLED'}"
                    f"</b>\n"
                    f"Duplicate Delete: <b>"
                    f"{'🟢 ON' if settings['delete_duplicates'] else '🔴 OFF'}"
                    f"</b>",
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
            "Send delay in minutes.\n\n"
            "Examples:\n"
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

                if (
                    not chat
                    or chat.get("type")
                    not in (
                        "group",
                        "supergroup",
                    )
                ):

                    await safe_edit(
                        query,
                        "❌ PURGE is available only "
                        "for Groups/Supergroups.",
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
                    f"Duplicate Delete: <b>"
                    f"{'🟢 ON' if new_value else '🔴 OFF'}"
                    f"</b>",
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
    # ENABLE PURGE
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

                if (
                    not chat
                    or chat.get("type")
                    not in (
                        "group",
                        "supergroup",
                    )
                ):

                    await safe_edit(
                        query,
                        "❌ PURGE cannot be enabled "
                        "for channels.",
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
    # DISABLE PURGE
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
            "⚡ Loading...",
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
            "⚡ Loading...",
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
            "Send Telegram numeric user ID.\n\n"
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
            "Send Telegram numeric user ID.\n\n"
            "ENV admins are protected.\n\n"
            "Use /cancel to cancel.",
        )

        return


# ============================================================
# PRIVATE ADMIN INPUT
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

    if not is_admin_fast(
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
            f"Status: <b>"
            f"{'ON' if settings['enabled'] else 'OFF'}"
            f"</b>",
            parse_mode="HTML",
            reply_markup=purge_keyboard(
                chat_id
            ),
        )


# ============================================================
# BOT MEMBER UPDATE
# ============================================================

async def my_chat_member_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
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
    # ACTIVE
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
            force=True,
        )

    # --------------------------------------------------------
    # LEFT
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

    # Cached registration:
    # NOT MongoDB on every message.
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
# STARTUP
# ============================================================

async def post_init(
    application,
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
        MONGO_URI,
        serverSelectionTimeoutMS=10000,
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

    # Test connection
    await db.command(
        "ping"
    )

    logger.info(
        "MongoDB initialized."
    )

    # --------------------------------------------------------
    # ADMIN CACHE
    # --------------------------------------------------------

    await refresh_admin_cache()

    logger.info(
        "Admin cache loaded: %s admins",
        len(admin_cache),
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
    application,
):

    global mongo
    global telethon_client

    # --------------------------------------------------------
    # PURGE TASKS
    # --------------------------------------------------------

    purge_job_tasks = list(
        purge_tasks.values()
    )

    for task in purge_job_tasks:

        task.cancel()

    if purge_job_tasks:

        await asyncio.gather(
            *purge_job_tasks,
            return_exceptions=True,
        )

    purge_tasks.clear()

    # --------------------------------------------------------
    # DELETE ALL TASKS
    # --------------------------------------------------------

    delete_all_job_tasks = list(
        set(
            delete_all_tasks.values()
        )
    )

    for task in delete_all_job_tasks:

        task.cancel()

    if delete_all_job_tasks:

        await asyncio.gather(
            *delete_all_job_tasks,
            return_exceptions=True,
        )

    delete_all_tasks.clear()
    delete_all_selections.clear()

    # --------------------------------------------------------
    # DCLEAN TASKS
    # --------------------------------------------------------

    dc_job_tasks = list(
        dclean_tasks.values()
    )

    for task in dc_job_tasks:

        task.cancel()

    if dc_job_tasks:

        await asyncio.gather(
            *dc_job_tasks,
            return_exceptions=True,
        )

    dclean_tasks.clear()

    # --------------------------------------------------------
    # UI TASKS
    # --------------------------------------------------------

    background_ui_tasks = list(
        ui_tasks
    )

    for task in background_ui_tasks:

        task.cancel()

    if background_ui_tasks:

        await asyncio.gather(
            *background_ui_tasks,
            return_exceptions=True,
        )

    ui_tasks.clear()

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
    # MY CHAT MEMBER
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
    # PRIVATE ADMIN INPUT
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
    # GROUP / SUPERGROUP
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
    # IMPORTANT FOR PYTHON 3.14
    # Do NOT use application.run_polling()
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
