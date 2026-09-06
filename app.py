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

ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


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

telethon_client = None

purge_tasks = {}
purge_locks = {}

admin_input_state = {}


# ============================================================
# DATABASE INIT
# ============================================================

async def init_db():
    global mongo
    global db
    global chats_col
    global admins_col
    global dclean_state_col
    global media_col
    global pending_col
    global purge_settings_col

    mongo = AsyncIOMotorClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000,
    )

    # Force connection test
    await mongo.admin.command("ping")

    db = mongo[DB_NAME]

    chats_col = db["chats"]
    admins_col = db["admins"]
    dclean_state_col = db["dclean_state"]
    media_col = db["media_fingerprints"]
    pending_col = db["dclean_pending"]
    purge_settings_col = db["purge_settings"]

    # ========================================================
    # IMPORTANT:
    #
    # DO NOT DELETE OLD DATA.
    #
    # Old dclean_state may contain:
    #   chat_id: null
    #   missing chat_id
    #
    # Therefore we DO NOT create a unique index here.
    # This preserves all old records safely.
    # ========================================================

    await chats_col.create_index(
        [("chat_id", ASCENDING)]
    )

    await admins_col.create_index(
        [("user_id", ASCENDING)]
    )

    await dclean_state_col.create_index(
        [("chat_id", ASCENDING)]
    )

    await media_col.create_index(
        [
            ("chat_id", ASCENDING),
            ("fingerprint", ASCENDING),
        ]
    )

    await pending_col.create_index(
        [
            ("chat_id", ASCENDING),
            ("message_id", ASCENDING),
        ]
    )

    await purge_settings_col.create_index(
        [("chat_id", ASCENDING)]
    )

    # ========================================================
    # SEED ENV ADMINS
    # ========================================================

    for admin_id in ADMIN_IDS:
        await admins_col.update_one(
            {"user_id": admin_id},
            {
                "$set": {
                    "user_id": admin_id,
                    "source": "ENV",
                    "updated_at": datetime.now(
                        timezone.utc
                    ),
                }
            },
            upsert=True,
        )

    logger.info(
        "MongoDB initialized safely"
    )


# ============================================================
# ADMIN SYSTEM
# ============================================================

async def is_admin(user_id: int) -> bool:

    if user_id in ADMIN_IDS:
        return True

    doc = await admins_col.find_one(
        {"user_id": user_id}
    )

    return bool(doc)


async def get_admin_list():

    cursor = admins_col.find({}).sort(
        "user_id",
        ASCENDING,
    )

    return await cursor.to_list(
        length=500
    )


async def add_admin(user_id: int):

    await admins_col.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "user_id": user_id,
                "source": "MONGO",
                "updated_at": datetime.now(
                    timezone.utc
                ),
            }
        },
        upsert=True,
    )


async def remove_admin(user_id: int):

    # ENV admins are protected
    if user_id in ADMIN_IDS:
        return False

    result = await admins_col.delete_one(
        {"user_id": user_id}
    )

    return result.deleted_count > 0


# ============================================================
# CHAT REGISTRATION
# ============================================================

async def register_chat(
    chat_id: int,
    title: str,
    chat_type: str,
):

    now = datetime.now(
        timezone.utc
    )

    await chats_col.update_one(
        {"chat_id": chat_id},
        {
            "$set": {
                "chat_id": chat_id,
                "title": title or str(chat_id),
                "type": chat_type,
                "active": True,
                "updated_at": now,
            },
            "$setOnInsert": {
                "created_at": now,
            },
        },
        upsert=True,
    )

    # PURGE SETTINGS ONLY FOR GROUPS
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
                    "updated_at": now,
                }
            },
            upsert=True,
        )


async def deactivate_chat(
    chat_id: int,
):

    await chats_col.update_one(
        {"chat_id": chat_id},
        {
            "$set": {
                "active": False,
                "updated_at": datetime.now(
                    timezone.utc
                ),
            }
        },
    )


async def get_chat(
    chat_id: int,
):

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

async def telethon_call(
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

            logger.warning(
                "Telegram FloodWait: %s seconds",
                e.seconds,
            )

            await asyncio.sleep(
                e.seconds + 2
            )

        except RPCError:
            raise


# ============================================================
# LOG
# ============================================================

async def send_log(
    application,
    text: str,
):

    if not PIPE_LOG_CHAT_ID:
        return

    try:

        await application.bot.send_message(
            chat_id=int(
                PIPE_LOG_CHAT_ID
            ),
            text=text,
            disable_web_page_preview=True,
        )

    except Exception as e:

        logger.warning(
            "PIPE log failed: %s",
            e,
        )


# ============================================================
# MEDIA FINGERPRINT
# ============================================================

def get_media_fingerprint(message):

    """
    Telethon does not provide Bot API file_unique_id.

    We use Telegram media object IDs.

    Same Telegram media object:
        photo:<id>
        document:<id>

    This detects repeated copies/forwards of
    the same Telegram media object.
    """

    if getattr(
        message,
        "photo",
        None,
    ):

        return (
            f"photo:{message.photo.id}"
        )

    if getattr(
        message,
        "document",
        None,
    ):

        return (
            f"document:{message.document.id}"
        )

    return None


# ============================================================
# DCLEAN STATE
# ============================================================

async def get_last_dclean_id(
    chat_id: int,
) -> int:

    doc = await dclean_state_col.find_one(
        {
            "chat_id": chat_id
        },
        sort=[
            (
                "updated_at",
                -1,
            )
        ],
    )

    if not doc:
        return 0

    try:
        return int(
            doc.get(
                "last_message_id",
                0,
            )
        )
    except Exception:
        return 0


async def save_last_dclean_id(
    chat_id: int,
    message_id: int,
):

    # IMPORTANT:
    #
    # No unique index is required.
    # This update preserves old records and
    # updates the state belonging to this chat.

    await dclean_state_col.update_one(
        {
            "chat_id": chat_id
        },
        {
            "$set": {
                "chat_id": chat_id,
                "last_message_id": message_id,
                "updated_at": datetime.now(
                    timezone.utc
                ),
            }
        },
        upsert=True,
    )


# ============================================================
# DCLEAN
# ============================================================

async def run_dclean(
    chat_id: int,
    status_message=None,
):

    scanned = 0
    duplicates = 0
    deleted = 0

    last_id = await get_last_dclean_id(
        chat_id
    )

    # ========================================================
    # RETRY PENDING DELETE QUEUE
    # ========================================================

    pending = await pending_col.find(
        {
            "chat_id": chat_id
        }
    ).to_list(
        length=5000
    )

    if pending:

        for item in pending:

            msg_id = item.get(
                "message_id"
            )

            if not msg_id:
                continue

            try:

                await telethon_call(
                    telethon_client.delete_messages,
                    chat_id,
                    [msg_id],
                )

                await pending_col.delete_one(
                    {
                        "_id": item["_id"]
                    }
                )

                deleted += 1

            except Exception as e:

                logger.warning(
                    "Pending delete failed %s: %s",
                    msg_id,
                    e,
                )

    # ========================================================
    # GET ENTITY
    # ========================================================

    entity = await telethon_call(
        telethon_client.get_entity,
        chat_id,
    )

    highest_id = last_id

    # ========================================================
    # INCREMENTAL SCAN
    #
    # First run:
    #   last_id = 0
    #   entire history
    #
    # Later:
    #   only newer messages
    # ========================================================

    async for message in telethon_client.iter_messages(
        entity,
        min_id=last_id,
        reverse=True,
    ):

        scanned += 1

        if message.id > highest_id:
            highest_id = message.id

        fingerprint = get_media_fingerprint(
            message
        )

        if fingerprint:

            existing = await media_col.find_one(
                {
                    "chat_id": chat_id,
                    "fingerprint": fingerprint,
                },
                sort=[
                    (
                        "message_id",
                        ASCENDING,
                    )
                ],
            )

            # ------------------------------------------------
            # DUPLICATE
            # ------------------------------------------------

            if existing:

                original_id = existing.get(
                    "message_id"
                )

                if original_id != message.id:

                    duplicates += 1

                    try:

                        await telethon_call(
                            telethon_client.delete_messages,
                            chat_id,
                            [message.id],
                        )

                        deleted += 1

                    except Exception as e:

                        logger.warning(
                            "DCLEAN delete failed "
                            "%s: %s",
                            message.id,
                            e,
                        )

                        await pending_col.update_one(
                            {
                                "chat_id": chat_id,
                                "message_id": message.id,
                            },
                            {
                                "$set": {
                                    "chat_id": chat_id,
                                    "message_id": message.id,
                                    "fingerprint": fingerprint,
                                    "error": str(e),
                                    "updated_at": datetime.now(
                                        timezone.utc
                                    ),
                                }
                            },
                            upsert=True,
                        )

            # ------------------------------------------------
            # FIRST SEEN MEDIA
            # ------------------------------------------------

            else:

                # DO NOT use unique index.
                #
                # Existing databases may already contain
                # duplicate fingerprint records.
                #
                # This insert is protected against race errors.

                try:

                    await media_col.insert_one(
                        {
                            "chat_id": chat_id,
                            "fingerprint": fingerprint,
                            "message_id": message.id,
                            "created_at": datetime.now(
                                timezone.utc
                            ),
                        }
                    )

                except Exception as e:

                    logger.debug(
                        "Fingerprint insert skipped: %s",
                        e,
                    )

        # ====================================================
        # LIVE PROGRESS
        # ====================================================

        if (
            scanned % 100 == 0
            and status_message
        ):

            remaining = max(
                0,
                duplicates - deleted,
            )

            try:

                await status_message.edit_text(
                    "♻️ <b>DCLEAN RUNNING</b>\n\n"
                    f"📥 Scanned: "
                    f"<code>{scanned}</code>\n"
                    f"♻️ Duplicates: "
                    f"<code>{duplicates}</code>\n"
                    f"🗑️ Deleted: "
                    f"<code>{deleted}</code>\n"
                    f"📌 Remaining: "
                    f"<code>{remaining}</code>\n"
                    f"🔢 Last ID: "
                    f"<code>{highest_id}</code>",
                    parse_mode="HTML",
                )

            except Exception:
                pass

    # ========================================================
    # SAVE STATE
    # ========================================================

    if highest_id > last_id:

        await save_last_dclean_id(
            chat_id,
            highest_id,
        )

    remaining = max(
        0,
        duplicates - deleted,
    )

    return {
        "scanned": scanned,
        "duplicates": duplicates,
        "deleted": deleted,
        "remaining": remaining,
        "last_id": highest_id,
    }


# ============================================================
# PURGE SETTINGS
# ============================================================

async def get_purge_settings(
    chat_id: int,
):

    doc = await purge_settings_col.find_one(
        {
            "chat_id": chat_id
        }
    )

    if not doc:

        doc = {
            "chat_id": chat_id,
            "enabled": False,
            "minutes": 60,
            "delete_duplicates": False,
        }

        await purge_settings_col.update_one(
            {
                "chat_id": chat_id
            },
            {
                "$set": doc
            },
            upsert=True,
        )

    return doc


async def save_purge_settings(
    chat_id: int,
    **values,
):

    values["updated_at"] = datetime.now(
        timezone.utc
    )

    await purge_settings_col.update_one(
        {
            "chat_id": chat_id
        },
        {
            "$set": values
        },
        upsert=True,
    )


# ============================================================
# PURGE
# ============================================================

async def run_purge(
    chat_id: int,
    status_message=None,
):

    chat = await get_chat(
        chat_id
    )

    if not chat:
        return None

    # ========================================================
    # HARD CHANNEL PROTECTION
    # ========================================================

    if chat["type"] not in (
        "group",
        "supergroup",
    ):
        return None

    settings = await get_purge_settings(
        chat_id
    )

    minutes = int(
        settings.get(
            "minutes",
            60,
        )
    )

    delete_duplicates = bool(
        settings.get(
            "delete_duplicates",
            False,
        )
    )

    cutoff = (
        datetime.now(
            timezone.utc
        )
        - timedelta(
            minutes=minutes
        )
    )

    deleted = 0
    checked = 0
    old_found = 0

    lock = purge_locks.setdefault(
        chat_id,
        asyncio.Lock(),
    )

    if lock.locked():
        return None

    async with lock:

        entity = await telethon_call(
            telethon_client.get_entity,
            chat_id,
        )

        # ====================================================
        # NORMAL PURGE
        # ====================================================

        old_ids = []

        async for message in telethon_client.iter_messages(
            entity
        ):

            checked += 1

            msg_date = message.date

            if msg_date.tzinfo is None:

                msg_date = msg_date.replace(
                    tzinfo=timezone.utc
                )

            if msg_date < cutoff:

                old_ids.append(
                    message.id
                )

                old_found += 1

            # ------------------------------------------------
            # Delete in batches
            # ------------------------------------------------

            if len(old_ids) >= 100:

                try:

                    await telethon_call(
                        telethon_client.delete_messages,
                        chat_id,
                        old_ids,
                    )

                    deleted += len(
                        old_ids
                    )

                except Exception as e:

                    logger.warning(
                        "Purge delete failed: %s",
                        e,
                    )

                old_ids.clear()

                if status_message:

                    try:

                        await status_message.edit_text(
                            "🗑️ <b>PURGE RUNNING</b>\n\n"
                            f"🔎 Checked: "
                            f"<code>{checked}</code>\n"
                            f"📦 Old found: "
                            f"<code>{old_found}</code>\n"
                            f"🗑️ Deleted: "
                            f"<code>{deleted}</code>",
                            parse_mode="HTML",
                        )

                    except Exception:
                        pass

        # ====================================================
        # FINAL BATCH
        # ====================================================

        if old_ids:

            try:

                await telethon_call(
                    telethon_client.delete_messages,
                    chat_id,
                    old_ids,
                )

                deleted += len(
                    old_ids
                )

            except Exception as e:

                logger.warning(
                    "Final purge failed: %s",
                    e,
                )

        # ====================================================
        # OPTIONAL DUPLICATE PURGE
        # ====================================================

        duplicate_deleted = 0

        if delete_duplicates:

            seen = set()

            async for message in telethon_client.iter_messages(
                entity
            ):

                fingerprint = get_media_fingerprint(
                    message
                )

                if not fingerprint:
                    continue

                if fingerprint in seen:

                    msg_date = message.date

                    if msg_date.tzinfo is None:

                        msg_date = msg_date.replace(
                            tzinfo=timezone.utc
                        )

                    if msg_date < cutoff:

                        try:

                            await telethon_call(
                                telethon_client.delete_messages,
                                chat_id,
                                [message.id],
                            )

                            duplicate_deleted += 1
                            deleted += 1

                        except Exception:
                            pass

                else:

                    seen.add(
                        fingerprint
                    )

    return {
        "checked": checked,
        "old_found": old_found,
        "deleted": deleted,
        "duplicate_deleted": duplicate_deleted,
        "minutes": minutes,
    }


# ============================================================
# PURGE WORKER
# ============================================================

async def purge_worker(
    application,
    chat_id: int,
):

    logger.info(
        "PURGE worker started: %s",
        chat_id,
    )

    while True:

        try:

            settings = await get_purge_settings(
                chat_id
            )

            chat = await get_chat(
                chat_id
            )

            # =================================================
            # CHANNEL PROTECTION
            # =================================================

            if (
                not chat
                or chat["type"]
                not in (
                    "group",
                    "supergroup",
                )
            ):
                break

            if not settings.get(
                "enabled",
                False,
            ):
                break

            result = await run_purge(
                chat_id
            )

            if result:

                logger.info(
                    "PURGE %s deleted=%s",
                    chat_id,
                    result["deleted"],
                )

            await asyncio.sleep(
                60
            )

        except asyncio.CancelledError:
            break

        except Exception as e:

            logger.exception(
                "PURGE worker error %s",
                chat_id,
            )

            await asyncio.sleep(
                60
            )

    purge_tasks.pop(
        chat_id,
        None,
    )

    logger.info(
        "PURGE worker stopped: %s",
        chat_id,
    )


def start_purge_worker(
    application,
    chat_id: int,
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
        purge_worker(
            application,
            chat_id,
        )
    )

    purge_tasks[
        chat_id
    ] = task


def stop_purge_worker(
    chat_id: int,
):

    task = purge_tasks.get(
        chat_id
    )

    if (
        task
        and not task.done()
    ):
        task.cancel()

    purge_tasks.pop(
        chat_id,
        None,
    )


async def restore_purge_workers(
    application,
):

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

        # ONLY GROUP/SUPERGROUP
        if (
            chat
            and chat["type"]
            in (
                "group",
                "supergroup",
            )
        ):

            start_purge_worker(
                application,
                chat_id,
            )


# ============================================================
# MAIN KEYBOARD
# ============================================================

def main_keyboard():

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "👥 GROUPS / CHANNELS",
                callback_data="chat_list",
            )
        ],
        [
            InlineKeyboardButton(
                "👤 ADMIN MANAGEMENT",
                callback_data="admin_menu",
            )
        ],
    ])


# ============================================================
# START
# ============================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not update.effective_user:
        return

    user_id = update.effective_user.id

    if not await is_admin(
        user_id
    ):

        await update.message.reply_text(
            "⛔ Access Denied."
        )

        return

    await update.message.reply_text(
        "♻️ <b>Duplicate Cleaner</b>\n\n"
        "Select an option:",
        parse_mode="HTML",
        reply_markup=main_keyboard(),
    )


# ============================================================
# CHAT LIST
# ============================================================

async def show_chat_list(
    query,
):

    chats = await get_all_chats()

    buttons = []

    for chat in chats:

        chat_type = chat.get(
            "type",
            ""
        )

        if chat_type == "channel":
            icon = "📢"

        elif chat_type == "supergroup":
            icon = "👥"

        else:
            icon = "👤"

        title = chat.get(
            "title",
            str(chat.get("chat_id")),
        )

        buttons.append([
            InlineKeyboardButton(
                f"{icon} {title[:45]}",
                callback_data=(
                    f"chat:{chat['chat_id']}"
                ),
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            "🔄 Refresh",
            callback_data="chat_list",
        ),
        InlineKeyboardButton(
            "🔙 Back",
            callback_data="main_menu",
        ),
    ])

    if not chats:

        text = (
            "👥 <b>No chats registered.</b>\n\n"
            "Add the bot to a group/channel first."
        )

    else:

        text = (
            "👥 <b>SELECT CHAT</b>\n\n"
            "📢 Channel → DCLEAN only\n"
            "👥 Group/Supergroup → DCLEAN + PURGE"
        )

    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            buttons
        ),
    )


# ============================================================
# CHAT PANEL
# ============================================================

async def show_chat_panel(
    query,
    chat_id: int,
):

    chat = await get_chat(
        chat_id
    )

    if not chat:

        await query.answer(
            "Chat not found",
            show_alert=True,
        )

        return

    title = chat.get(
        "title",
        str(chat_id),
    )

    chat_type = chat.get(
        "type",
        "",
    )

    # ========================================================
    # DCLEAN ALWAYS
    # ========================================================

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

    # ========================================================
    # PURGE ONLY GROUP/SUPERGROUP
    # ========================================================

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

    buttons.extend([
        [
            InlineKeyboardButton(
                "📊 STATUS",
                callback_data=(
                    f"status:{chat_id}"
                ),
            )
        ],
        [
            InlineKeyboardButton(
                "🔙 Chat List",
                callback_data="chat_list",
            )
        ],
    ])

    if chat_type == "channel":

        type_name = "📢 Channel"

    elif chat_type == "supergroup":

        type_name = "👥 Supergroup"

    else:

        type_name = "👤 Group"

    await query.edit_message_text(
        f"<b>{escape(title)}</b>\n\n"
        f"{type_name}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            buttons
        ),
    )


# ============================================================
# PURGE MENU
# ============================================================

async def show_purge_menu(
    query,
    chat_id: int,
):

    chat = await get_chat(
        chat_id
    )

    # HARD PROTECTION
    if (
        not chat
        or chat["type"]
        not in (
            "group",
            "supergroup",
        )
    ):

        await query.answer(
            "PURGE is available for groups only.",
            show_alert=True,
        )

        return

    settings = await get_purge_settings(
        chat_id
    )

    minutes = int(
        settings.get(
            "minutes",
            60,
        )
    )

    enabled = bool(
        settings.get(
            "enabled",
            False,
        )
    )

    duplicate = bool(
        settings.get(
            "delete_duplicates",
            False,
        )
    )

    buttons = []

    preset_row = []

    for value, label in PURGE_PRESETS.items():

        preset_row.append(
            InlineKeyboardButton(
                label,
                callback_data=(
                    f"pminutes:{chat_id}:{value}"
                ),
            )
        )

        if len(preset_row) == 3:

            buttons.append(
                preset_row
            )

            preset_row = []

    if preset_row:

        buttons.append(
            preset_row
        )

    buttons.append([
        InlineKeyboardButton(
            "⚙️ Custom Minutes",
            callback_data=(
                f"custompurge:{chat_id}"
            ),
        )
    ])

    buttons.append([
        InlineKeyboardButton(
            (
                "♻️ Duplicate Delete: ON"
                if duplicate
                else
                "♻️ Duplicate Delete: OFF"
            ),
            callback_data=(
                f"pduplicate:{chat_id}"
            ),
        )
    ])

    buttons.append([
        InlineKeyboardButton(
            (
                "🟢 PURGE ENABLED"
                if enabled
                else
                "🔴 PURGE DISABLED"
            ),
            callback_data=(
                f"ptoggle:{chat_id}"
            ),
        )
    ])

    buttons.append([
        InlineKeyboardButton(
            "🗑️ RUN NOW",
            callback_data=(
                f"prun:{chat_id}"
            ),
        )
    ])

    buttons.append([
        InlineKeyboardButton(
            "🔙 Back",
            callback_data=(
                f"chat:{chat_id}"
            ),
        )
    ])

    await query.edit_message_text(
        "🗑️ <b>PURGE SETTINGS</b>\n\n"
        f"⏱️ Current: "
        f"<code>{minutes}</code> minutes\n"
        f"📌 Status: "
        f"<code>{'ON' if enabled else 'OFF'}</code>\n"
        f"♻️ Duplicate Delete: "
        f"<code>{'ON' if duplicate else 'OFF'}</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            buttons
        ),
    )


# ============================================================
# ADMIN MENU
# ============================================================

async def show_admin_menu(
    query,
):

    admins = await get_admin_list()

    lines = [
        "👤 <b>ADMIN MANAGEMENT</b>",
        "",
        "Current admins:",
        "",
    ]

    for admin in admins:

        uid = admin.get(
            "user_id"
        )

        if uid in ADMIN_IDS:

            source = "ENV"

        else:

            source = "MongoDB"

        lines.append(
            f"• <code>{uid}</code> ({source})"
        )

    buttons = [
        [
            InlineKeyboardButton(
                "➕ ADD ADMIN",
                callback_data="add_admin",
            )
        ],
        [
            InlineKeyboardButton(
                "❌ REMOVE ADMIN",
                callback_data="remove_admin",
            )
        ],
        [
            InlineKeyboardButton(
                "👥 ADMIN LIST",
                callback_data="admin_menu",
            )
        ],
        [
            InlineKeyboardButton(
                "🔙 Back",
                callback_data="main_menu",
            )
        ],
    ]

    await query.edit_message_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            buttons
        ),
    )


# ============================================================
# STATUS
# ============================================================

async def show_status(
    query,
    chat_id: int,
):

    chat = await get_chat(
        chat_id
    )

    if not chat:

        await query.answer(
            "Chat not found",
            show_alert=True,
        )

        return

    last_id = await get_last_dclean_id(
        chat_id
    )

    media_count = await media_col.count_documents(
        {
            "chat_id": chat_id
        }
    )

    pending_count = await pending_col.count_documents(
        {
            "chat_id": chat_id
        }
    )

    text = (
        "📊 <b>STATUS</b>\n\n"
        f"👥 {escape(chat.get('title', str(chat_id)))}\n\n"
        f"🔢 Last DCLEAN ID: "
        f"<code>{last_id}</code>\n"
        f"💾 Media fingerprints: "
        f"<code>{media_count}</code>\n"
        f"⏳ Pending deletes: "
        f"<code>{pending_count}</code>"
    )

    if chat["type"] in (
        "group",
        "supergroup",
    ):

        settings = await get_purge_settings(
            chat_id
        )

        text += (
            "\n\n🗑️ <b>PURGE</b>\n"
            f"⏱️ <code>"
            f"{settings.get('minutes', 60)}"
            f"</code> min\n"
            f"📌 Enabled: "
            f"<code>"
            f"{'YES' if settings.get('enabled') else 'NO'}"
            f"</code>"
        )

    else:

        text += (
            "\n\n📢 <b>CHANNEL</b>\n"
            "🗑️ PURGE: <b>NOT AVAILABLE</b>"
        )

    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "🔙 Back",
                    callback_data=(
                        f"chat:{chat_id}"
                    ),
                )
            ]
        ]),
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

    user_id = query.from_user.id

    if not await is_admin(
        user_id
    ):

        await query.answer(
            "⛔ Access Denied",
            show_alert=True,
        )

        return

    await query.answer()

    data = query.data or ""

    # ========================================================
    # MAIN MENU
    # ========================================================

    if data == "main_menu":

        await query.edit_message_text(
            "♻️ <b>Duplicate Cleaner</b>\n\n"
            "Select an option:",
            parse_mode="HTML",
            reply_markup=main_keyboard(),
        )

        return

    # ========================================================
    # CHAT LIST
    # ========================================================

    if data == "chat_list":

        await show_chat_list(
            query
        )

        return

    # ========================================================
    # CHAT
    # ========================================================

    if data.startswith("chat:"):

        chat_id = int(
            data.split(
                ":",
                1,
            )[1]
        )

        await show_chat_panel(
            query,
            chat_id,
        )

        return

    # ========================================================
    # DCLEAN
    # ========================================================

    if data.startswith("dclean:"):

        chat_id = int(
            data.split(
                ":",
                1,
            )[1]
        )

        chat = await get_chat(
            chat_id
        )

        if not chat:

            await query.edit_message_text(
                "❌ Chat not found."
            )

            return

        await query.edit_message_text(
            "♻️ <b>DCLEAN STARTED</b>\n\n"
            "Scanning...",
            parse_mode="HTML",
        )

        try:

            result = await run_dclean(
                chat_id,
                query.message,
            )

            await query.edit_message_text(
                "♻️ <b>DCLEAN COMPLETE</b>\n\n"
                f"📥 Scanned: "
                f"<code>{result['scanned']}</code>\n"
                f"♻️ Duplicates: "
                f"<code>{result['duplicates']}</code>\n"
                f"🗑️ Deleted: "
                f"<code>{result['deleted']}</code>\n"
                f"📌 Remaining: "
                f"<code>{result['remaining']}</code>\n"
                f"🔢 Last ID: "
                f"<code>{result['last_id']}</code>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(
                            "🔙 Back",
                            callback_data=(
                                f"chat:{chat_id}"
                            ),
                        )
                    ]
                ]),
            )

        except Exception as e:

            logger.exception(
                "DCLEAN error"
            )

            await query.edit_message_text(
                "❌ <b>DCLEAN ERROR</b>\n\n"
                f"<code>{escape(str(e))}</code>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(
                            "🔙 Back",
                            callback_data=(
                                f"chat:{chat_id}"
                            ),
                        )
                    ]
                ]),
            )

        return

    # ========================================================
    # STATUS
    # ========================================================

    if data.startswith("status:"):

        chat_id = int(
            data.split(
                ":",
                1,
            )[1]
        )

        await show_status(
            query,
            chat_id,
        )

        return

    # ========================================================
    # PURGE MENU
    # ========================================================

    if data.startswith("purge:"):

        chat_id = int(
            data.split(
                ":",
                1,
            )[1]
        )

        await show_purge_menu(
            query,
            chat_id,
        )

        return

    # ========================================================
    # PURGE MINUTES
    # ========================================================

    if data.startswith("pminutes:"):

        _, chat_id, minutes = (
            data.split(":")
        )

        chat_id = int(
            chat_id
        )

        minutes = int(
            minutes
        )

        chat = await get_chat(
            chat_id
        )

        if (
            not chat
            or chat["type"]
            not in (
                "group",
                "supergroup",
            )
        ):

            await query.answer(
                "PURGE is only for groups.",
                show_alert=True,
            )

            return

        await save_purge_settings(
            chat_id,
            minutes=minutes,
        )

        await show_purge_menu(
            query,
            chat_id,
        )

        return

    # ========================================================
    # DUPLICATE DELETE TOGGLE
    # ========================================================

    if data.startswith(
        "pduplicate:"
    ):

        chat_id = int(
            data.split(
                ":",
                1,
            )[1]
        )

        chat = await get_chat(
            chat_id
        )

        if (
            not chat
            or chat["type"]
            not in (
                "group",
                "supergroup",
            )
        ):

            await query.answer(
                "PURGE is only for groups.",
                show_alert=True,
            )

            return

        settings = await get_purge_settings(
            chat_id
        )

        current = bool(
            settings.get(
                "delete_duplicates",
                False,
            )
        )

        await save_purge_settings(
            chat_id,
            delete_duplicates=not current,
        )

        await show_purge_menu(
            query,
            chat_id,
        )

        return

    # ========================================================
    # PURGE ENABLE/DISABLE
    # ========================================================

    if data.startswith(
        "ptoggle:"
    ):

        chat_id = int(
            data.split(
                ":",
                1,
            )[1]
        )

        chat = await get_chat(
            chat_id
        )

        if (
            not chat
            or chat["type"]
            not in (
                "group",
                "supergroup",
            )
        ):

            await query.answer(
                "PURGE is only for groups.",
                show_alert=True,
            )

            return

        settings = await get_purge_settings(
            chat_id
        )

        current = bool(
            settings.get(
                "enabled",
                False,
            )
        )

        new_value = not current

        await save_purge_settings(
            chat_id,
            enabled=new_value,
        )

        if new_value:

            start_purge_worker(
                context.application,
                chat_id,
            )

        else:

            stop_purge_worker(
                chat_id
            )

        await show_purge_menu(
            query,
            chat_id,
        )

        return

    # ========================================================
    # RUN PURGE NOW
    # ========================================================

    if data.startswith(
        "prun:"
    ):

        chat_id = int(
            data.split(
                ":",
                1,
            )[1]
        )

        chat = await get_chat(
            chat_id
        )

        if (
            not chat
            or chat["type"]
            not in (
                "group",
                "supergroup",
            )
        ):

            await query.answer(
                "PURGE is only for groups.",
                show_alert=True,
            )

            return

        await query.edit_message_text(
            "🗑️ <b>PURGE STARTED</b>\n\n"
            "Scanning...",
            parse_mode="HTML",
        )

        try:

            result = await run_purge(
                chat_id,
                query.message,
            )

            if result is None:

                await query.edit_message_text(
                    "⚠️ PURGE is already running."
                )

                return

            await query.edit_message_text(
                "🗑️ <b>PURGE COMPLETE</b>\n\n"
                f"🔎 Checked: "
                f"<code>{result['checked']}</code>\n"
                f"📦 Old found: "
                f"<code>{result['old_found']}</code>\n"
                f"🗑️ Deleted: "
                f"<code>{result['deleted']}</code>\n"
                f"♻️ Duplicate deleted: "
                f"<code>{result['duplicate_deleted']}</code>\n"
                f"⏱️ Age: "
                f"<code>{result['minutes']} min</code>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(
                            "🔙 Back",
                            callback_data=(
                                f"purge:{chat_id}"
                            ),
                        )
                    ]
                ]),
            )

        except Exception as e:

            logger.exception(
                "PURGE error"
            )

            await query.edit_message_text(
                "❌ <b>PURGE ERROR</b>\n\n"
                f"<code>{escape(str(e))}</code>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(
                            "🔙 Back",
                            callback_data=(
                                f"purge:{chat_id}"
                            ),
                        )
                    ]
                ]),
            )

        return

    # ========================================================
    # CUSTOM PURGE
    # ========================================================

    if data.startswith(
        "custompurge:"
    ):

        chat_id = int(
            data.split(
                ":",
                1,
            )[1]
        )

        chat = await get_chat(
            chat_id
        )

        if (
            not chat
            or chat["type"]
            not in (
                "group",
                "supergroup",
            )
        ):

            await query.answer(
                "PURGE is only for groups.",
                show_alert=True,
            )

            return

        admin_input_state[
            user_id
        ] = {
            "action": "custom_purge",
            "chat_id": chat_id,
        }

        await query.edit_message_text(
            "⚙️ <b>CUSTOM PURGE</b>\n\n"
            "Send minutes.\n\n"
            "Example:\n"
            "<code>45</code>",
            parse_mode="HTML",
        )

        return

    # ========================================================
    # ADMIN MENU
    # ========================================================

    if data == "admin_menu":

        await show_admin_menu(
            query
        )

        return

    # ========================================================
    # ADD ADMIN
    # ========================================================

    if data == "add_admin":

        admin_input_state[
            user_id
        ] = {
            "action": "add_admin"
        }

        await query.edit_message_text(
            "➕ <b>ADD ADMIN</b>\n\n"
            "Send Telegram numeric user ID.",
            parse_mode="HTML",
        )

        return

    # ========================================================
    # REMOVE ADMIN
    # ========================================================

    if data == "remove_admin":

        admin_input_state[
            user_id
        ] = {
            "action": "remove_admin"
        }

        await query.edit_message_text(
            "❌ <b>REMOVE ADMIN</b>\n\n"
            "Send Telegram numeric user ID.",
            parse_mode="HTML",
        )

        return


# ============================================================
# TEXT INPUT
# ============================================================

async def text_input_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not update.effective_user:
        return

    user_id = update.effective_user.id

    if not await is_admin(
        user_id
    ):
        return

    state = admin_input_state.get(
        user_id
    )

    if not state:
        return

    text = (
        update.message.text or ""
    ).strip()

    action = state.get(
        "action"
    )

    # ========================================================
    # ADD ADMIN
    # ========================================================

    if action == "add_admin":

        try:

            target_id = int(
                text
            )

        except ValueError:

            await update.message.reply_text(
                "❌ Invalid user ID."
            )

            return

        await add_admin(
            target_id
        )

        admin_input_state.pop(
            user_id,
            None,
        )

        await update.message.reply_text(
            "✅ Admin added:\n"
            f"<code>{target_id}</code>",
            parse_mode="HTML",
            reply_markup=main_keyboard(),
        )

        return

    # ========================================================
    # REMOVE ADMIN
    # ========================================================

    if action == "remove_admin":

        try:

            target_id = int(
                text
            )

        except ValueError:

            await update.message.reply_text(
                "❌ Invalid user ID."
            )

            return

        if target_id in ADMIN_IDS:

            await update.message.reply_text(
                "⛔ ENV ADMIN cannot be removed."
            )

            return

        removed = await remove_admin(
            target_id
        )

        admin_input_state.pop(
            user_id,
            None,
        )

        if removed:

            result = (
                "✅ Admin removed."
            )

        else:

            result = (
                "⚠️ Admin not found."
            )

        await update.message.reply_text(
            result,
            reply_markup=main_keyboard(),
        )

        return

    # ========================================================
    # CUSTOM PURGE
    # ========================================================

    if action == "custom_purge":

        try:

            minutes = int(
                text
            )

            if minutes < 1:
                raise ValueError

        except ValueError:

            await update.message.reply_text(
                "❌ Enter valid minutes."
            )

            return

        chat_id = state[
            "chat_id"
        ]

        chat = await get_chat(
            chat_id
        )

        if (
            not chat
            or chat["type"]
            not in (
                "group",
                "supergroup",
            )
        ):

            admin_input_state.pop(
                user_id,
                None,
            )

            await update.message.reply_text(
                "⛔ PURGE is only for groups."
            )

            return

        await save_purge_settings(
            chat_id,
            minutes=minutes,
        )

        admin_input_state.pop(
            user_id,
            None,
        )

        await update.message.reply_text(
            "✅ PURGE time saved:\n"
            f"<code>{minutes}</code> minutes",
            parse_mode="HTML",
            reply_markup=main_keyboard(),
        )

        return


# ============================================================
# CANCEL
# ============================================================

async def cancel_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    user_id = update.effective_user.id

    admin_input_state.pop(
        user_id,
        None,
    )

    await update.message.reply_text(
        "❌ Cancelled."
    )


# ============================================================
# MY CHAT MEMBER
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

    chat = member_update.chat

    new_status = (
        member_update
        .new_chat_member
        .status
    )

    if chat.type not in (
        "group",
        "supergroup",
        "channel",
    ):
        return

    # ========================================================
    # BOT ADDED / ACTIVE
    # ========================================================

    if new_status in (
        ChatMemberStatus.MEMBER,
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.OWNER,
        ChatMemberStatus.RESTRICTED,
    ):

        await register_chat(
            chat.id,
            chat.title or str(chat.id),
            chat.type,
        )

        logger.info(
            "Registered chat: %s (%s) type=%s",
            chat.title,
            chat.id,
            chat.type,
        )

    # ========================================================
    # BOT LEFT / KICKED
    # ========================================================

    elif new_status in (
        ChatMemberStatus.LEFT,
        ChatMemberStatus.KICKED,
    ):

        await deactivate_chat(
            chat.id
        )

        stop_purge_worker(
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

    chat = message.chat

    if chat.type not in (
        "group",
        "supergroup",
    ):
        return

    await register_chat(
        chat.id,
        chat.title or str(chat.id),
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

    chat = post.chat

    if chat.type != "channel":
        return

    # ========================================================
    # CHANNEL REGISTER ONLY
    #
    # NO PURGE SETTINGS ARE ENABLED FOR CHANNEL.
    # ========================================================

    await register_chat(
        chat.id,
        chat.title or str(chat.id),
        "channel",
    )

    logger.info(
        "Registered channel post: %s (%s)",
        chat.title,
        chat.id,
    )


# ============================================================
# TYPE HANDLER ROUTER
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

    global telethon_client

    # ========================================================
    # CONFIG CHECK
    # ========================================================

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN missing"
        )

    if not MONGO_URI:
        raise RuntimeError(
            "MONGO_URI missing"
        )

    if not API_ID:
        raise RuntimeError(
            "API_ID missing"
        )

    if not API_HASH:
        raise RuntimeError(
            "API_HASH missing"
        )

    if not STRING_SESSION:
        raise RuntimeError(
            "STRING_SESSION missing"
        )

    # ========================================================
    # DATABASE
    # ========================================================

    await init_db()

    # ========================================================
    # TELETHON
    # ========================================================

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
            "STRING_SESSION is not authorized"
        )

    logger.info(
        "Telethon connected"
    )

    # ========================================================
    # RESTORE PURGE
    #
    # GROUP/SUPERGROUP ONLY
    # ========================================================

    await restore_purge_workers(
        application
    )

    logger.info(
        "Startup complete"
    )


# ============================================================
# POST SHUTDOWN
# ============================================================

async def post_shutdown(
    application: Application,
):

    global mongo
    global telethon_client

    # Cancel workers
    for task in list(
        purge_tasks.values()
    ):

        if (
            task
            and not task.done()
        ):
            task.cancel()

    purge_tasks.clear()

    # Disconnect Telethon
    if telethon_client:

        try:
            await telethon_client.disconnect()
        except Exception:
            pass

    # Close Mongo
    if mongo:

        try:
            mongo.close()
        except Exception:
            pass

    logger.info(
        "Shutdown complete"
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

    # ========================================================
    # COMMANDS
    # ========================================================

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

    # ========================================================
    # BOT CHAT MEMBER
    # ========================================================

    application.add_handler(
        ChatMemberHandler(
            my_chat_member_handler,
            ChatMemberHandler.MY_CHAT_MEMBER,
        )
    )

    # ========================================================
    # CALLBACKS
    # ========================================================

    application.add_handler(
        CallbackQueryHandler(
            callback_handler
        )
    )

    # ========================================================
    # PRIVATE ADMIN TEXT INPUT
    # ========================================================

    application.add_handler(
        MessageHandler(
            filters.TEXT
            & ~filters.COMMAND
            & filters.ChatType.PRIVATE,
            text_input_handler,
        ),
        group=0,
    )

    # ========================================================
    # CHANNEL POSTS
    #
    # Correct PTB 22.x method.
    # NO ChannelPostHandler.
    # NO custom BaseHandler.
    # ========================================================

    application.add_handler(
        TypeHandler(
            Update,
            update_router,
        ),
        group=1,
    )

    # ========================================================
    # GROUP / SUPERGROUP WATCHER
    # ========================================================

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

    # ========================================================
    # POLLING
    # ========================================================

    application.run_polling(
        allowed_updates=[
            "message",
            "channel_post",
            "callback_query",
            "my_chat_member",
        ],
        drop_pending_updates=False,
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
