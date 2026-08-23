import os
import asyncio
import logging
from typing import Dict, List, Optional, Tuple

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, RPCError

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger("duplicate-cleaner")


# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_ID_RAW = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()
STRING_SESSION = os.getenv("STRING_SESSION", "").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()

# PIPE LOG GROUP
PIPE_LOG_CHAT_ID_RAW = os.getenv(
    "PIPE_LOG_CHAT_ID",
    ""
).strip()


if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")

if not API_ID_RAW:
    raise RuntimeError("API_ID is missing")

if not API_HASH:
    raise RuntimeError("API_HASH is missing")

if not STRING_SESSION:
    raise RuntimeError("STRING_SESSION is missing")

if not PIPE_LOG_CHAT_ID_RAW:
    raise RuntimeError("PIPE_LOG_CHAT_ID is missing")


API_ID = int(API_ID_RAW)
PIPE_LOG_CHAT_ID = int(PIPE_LOG_CHAT_ID_RAW)


# ============================================================
# ADMIN IDS
# ============================================================

def parse_admin_ids(value: str) -> set:

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


ADMIN_IDS = parse_admin_ids(ADMIN_IDS_RAW)


# ============================================================
# TELETHON
# ============================================================

telethon_client = TelegramClient(
    StringSession(STRING_SESSION),
    API_ID,
    API_HASH,
)


# ============================================================
# SETTINGS
# ============================================================

CHAT_PAGE_SIZE = 8

DELETE_BATCH_SIZE = 100

PROGRESS_SCAN_EVERY = 500

PROGRESS_DELETE_EVERY = 100

# Small pause between duplicate delete batches.
DELETE_PAUSE = 0.20


# ============================================================
# PIPE SETTINGS
# ============================================================

PIPE_KEEP_MESSAGES = 20

PIPE_DELETE_BATCH_SIZE = 100

PIPE_DELETE_PAUSE = 0.20


# ============================================================
# GLOBAL STATE
# ============================================================

selected_chats: Dict[int, int] = {}

chat_pages: Dict[int, int] = {}

chat_cache: Dict[
    int,
    List[Tuple[int, str, str]]
] = {}

running_tasks: Dict[int, asyncio.Task] = {}

stop_events: Dict[int, asyncio.Event] = {}

progress_messages: Dict[int, object] = {}

states: Dict[int, dict] = {}

# PIPE TASKS
pipe_tasks: Dict[int, asyncio.Task] = {}


# ============================================================
# STATE
# ============================================================

def get_state(user_id: int) -> dict:

    if user_id not in states:

        states[user_id] = {
            "running": False,
            "status": "Idle",
            "chat_name": "None",
            "scanned": 0,
            "duplicates": 0,
            "deleted": 0,
            "remaining": 0,
        }

    return states[user_id]


def reset_state(user_id: int):

    states[user_id] = {
        "running": False,
        "status": "Idle",
        "chat_name": "None",
        "scanned": 0,
        "duplicates": 0,
        "deleted": 0,
        "remaining": 0,
    }


# ============================================================
# ADMIN CHECK
# ============================================================

def is_admin(user_id: int) -> bool:

    return user_id in ADMIN_IDS


async def require_admin(
    update: Update,
) -> bool:

    user = update.effective_user

    if not user:
        return False

    if is_admin(user.id):
        return True

    if update.callback_query:

        try:
            await update.callback_query.answer(
                "❌ Not authorized.",
                show_alert=True,
            )
        except Exception:
            pass

    elif update.effective_message:

        await update.effective_message.reply_text(
            "❌ You are not authorized to use this bot."
        )

    return False


# ============================================================
# TELETHON CONNECT
# ============================================================

async def ensure_telethon_connected():

    if not telethon_client.is_connected():

        await telethon_client.connect()

    if not await telethon_client.is_user_authorized():

        raise RuntimeError(
            "STRING_SESSION is not authorized."
        )


# ============================================================
# GET ADMIN CHATS
# ============================================================

async def get_admin_chats():

    await ensure_telethon_connected()

    me = await telethon_client.get_me()

    result = []

    async for dialog in telethon_client.iter_dialogs():

        entity = dialog.entity

        # Only groups/channels
        if not (
            getattr(dialog, "is_group", False)
            or getattr(dialog, "is_channel", False)
        ):
            continue

        try:

            permissions = await telethon_client.get_permissions(
                entity,
                me,
            )

            is_creator = bool(
                getattr(permissions, "is_creator", False)
            )

            is_admin = bool(
                getattr(permissions, "is_admin", False)
            )

            if not (is_creator or is_admin):
                continue

        except Exception as e:

            logger.warning(
                "Could not check permissions for %s: %s",
                getattr(entity, "title", "Unknown"),
                e,
            )

            continue

        chat_id = int(entity.id)

        # Telethon entity IDs need -100 prefix for channels.
        if getattr(entity, "broadcast", False) or getattr(
            entity,
            "megagroup",
            False,
        ):

            chat_id = int(
                f"-100{entity.id}"
            )

        title = (
            getattr(entity, "title", None)
            or dialog.name
            or "Unknown Chat"
        )

        if getattr(entity, "broadcast", False):

            chat_type = "📢 Channel"

        elif getattr(entity, "megagroup", False):

            chat_type = "👥 Supergroup"

        else:

            chat_type = "👥 Group"

        result.append(
            (
                chat_id,
                title,
                chat_type,
            )
        )

    result.sort(
        key=lambda x: x[1].lower()
    )

    return result


# ============================================================
# CHAT ENTITY
# ============================================================

async def get_entity_from_chat_id(chat_id: int):

    await ensure_telethon_connected()

    return await telethon_client.get_entity(
        chat_id
    )


# ============================================================
# MAIN MENU
# ============================================================

def main_menu():

    keyboard = [
        [
            InlineKeyboardButton(
                "🔗 Connect Chat",
                callback_data="connect"
            )
        ],
        [
            InlineKeyboardButton(
                "📊 Status",
                callback_data="status"
            )
        ],
        [
            InlineKeyboardButton(
                "❌ Stop",
                callback_data="stop"
            )
        ],
    ]

    return InlineKeyboardMarkup(keyboard)


# ============================================================
# CHAT LIST
# ============================================================

def build_chat_keyboard(
    user_id: int,
    chats: List[Tuple[int, str, str]],
    page: int,
):

    start = page * CHAT_PAGE_SIZE

    current = chats[
        start:start + CHAT_PAGE_SIZE
    ]

    keyboard = []

    for index, (
        chat_id,
        title,
        chat_type,
    ) in enumerate(current):

        absolute_index = start + index

        # callback data contains local index
        keyboard.append(
            [
                InlineKeyboardButton(
                    f"{chat_type} {title[:35]}",
                    callback_data=(
                        f"select:{absolute_index}"
                    ),
                )
            ]
        )

    navigation = []

    if page > 0:

        navigation.append(
            InlineKeyboardButton(
                "⬅️ Previous",
                callback_data=f"page:{page - 1}",
            )
        )

    if start + CHAT_PAGE_SIZE < len(chats):

        navigation.append(
            InlineKeyboardButton(
                "Next ➡️",
                callback_data=f"page:{page + 1}",
            )
        )

    if navigation:

        keyboard.append(navigation)

    keyboard.append(
        [
            InlineKeyboardButton(
                "🔄 Refresh",
                callback_data="refresh",
            ),
            InlineKeyboardButton(
                "🏠 Home",
                callback_data="home",
            ),
        ]
    )

    return InlineKeyboardMarkup(keyboard)


# ============================================================
# CHAT LIST MESSAGE
# ============================================================

async def show_chat_list(
    query,
    user_id: int,
    page: int = 0,
):

    try:

        if (
            user_id not in chat_cache
            or page == 0
        ):

            chats = await get_admin_chats()

            chat_cache[user_id] = chats

        else:

            chats = chat_cache[user_id]

        chat_pages[user_id] = page

        if not chats:

            await query.edit_message_text(
                "📂 <b>Admin Chats</b>\n\n"
                "❌ No groups/channels found "
                "where the String Session account "
                "is an administrator.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "🔄 Refresh",
                                callback_data="refresh",
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                "🏠 Home",
                                callback_data="home",
                            )
                        ],
                    ]
                ),
            )

            return

        total_pages = (
            len(chats) + CHAT_PAGE_SIZE - 1
        ) // CHAT_PAGE_SIZE

        text = (
            "📂 <b>ADMIN CHATS</b>\n\n"
            f"Total: <b>{len(chats)}</b>\n"
            f"Page: <b>{page + 1}/{total_pages}</b>\n\n"
            "Select a group or channel:"
        )

        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=build_chat_keyboard(
                user_id,
                chats,
                page,
            ),
        )

    except Exception as e:

        logger.exception(
            "Could not load chat list"
        )

        await query.edit_message_text(
            "❌ Failed to load chats.\n\n"
            f"<code>{str(e)[:800]}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🔄 Try Again",
                            callback_data="refresh",
                        )
                    ]
                ]
            ),
        )


# ============================================================
# SELECTED CHAT MENU
# ============================================================

def selected_chat_menu():

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🔍 Scan Duplicates",
                    callback_data="scan",
                )
            ],
            [
                InlineKeyboardButton(
                    "🔗 Change Chat",
                    callback_data="connect",
                )
            ],
            [
                InlineKeyboardButton(
                    "🏠 Home",
                    callback_data="home",
                )
            ],
        ]
    )


async def show_selected_chat(
    query,
    user_id: int,
):

    chat_id = selected_chats.get(user_id)

    if not chat_id:

        await query.edit_message_text(
            "❌ No chat connected.",
            reply_markup=main_menu(),
        )

        return

    state = get_state(user_id)

    await query.edit_message_text(
        "✅ <b>CHAT CONNECTED</b>\n\n"
        f"📌 Chat: <b>{state['chat_name']}</b>\n"
        f"🆔 ID: <code>{chat_id}</code>\n\n"
        "Ready to scan old messages.",
        parse_mode=ParseMode.HTML,
        reply_markup=selected_chat_menu(),
    )


# ============================================================
# PROGRESS TEXT
# ============================================================

def state_text(user_id: int):

    state = get_state(user_id)

    return (
        "🧹 <b>DUPLICATE CLEANER</b>\n\n"
        f"📌 Chat: <b>{state['chat_name']}</b>\n\n"
        f"🔍 Scanned: <b>{state['scanned']:,}</b>\n"
        f"♻️ Duplicates: <b>{state['duplicates']:,}</b>\n"
        f"🗑️ Deleted: <b>{state['deleted']:,}</b>\n"
        f"⏳ Remaining: <b>{state['remaining']:,}</b>\n\n"
        f"📌 Status: <b>{state['status']}</b>"
    )


# ============================================================
# MEDIA KEY
# ============================================================

def get_media_key(message):

    """
    Fast duplicate detection.

    This compares Telegram media/document IDs.

    It does NOT download files.
    """

    if not message.media:
        return None

    # Documents include videos, files, audio, etc.
    if getattr(message, "document", None):

        document = message.document

        return (
            "document",
            int(document.id),
        )

    # Photos
    if getattr(message, "photo", None):

        photo = message.photo

        return (
            "photo",
            int(photo.id),
        )

    return None


# ============================================================
# PROGRESS EDIT
# ============================================================

async def edit_progress(
    message,
    user_id: int,
):

    try:

        await message.edit_text(
            state_text(user_id),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🛑 Stop",
                            callback_data="stop",
                        )
                    ]
                ]
            ),
        )

    except Exception as e:

        logger.debug(
            "Progress edit failed: %s",
            e,
        )


# ============================================================
# CLEANUP TASK
# ============================================================

async def cleanup_chat(
    user_id: int,
    progress_message,
):

    state = get_state(user_id)

    chat_id = selected_chats.get(user_id)

    if not chat_id:

        state["status"] = "No chat selected"
        state["running"] = False

        return

    state["running"] = True
    state["status"] = "Scanning"
    state["scanned"] = 0
    state["duplicates"] = 0
    state["deleted"] = 0
    state["remaining"] = 0

    stop_event = asyncio.Event()

    stop_events[user_id] = stop_event

    seen = set()

    duplicate_ids = []

    try:

        entity = await get_entity_from_chat_id(
            chat_id
        )

        logger.info(
            "Starting scan: %s",
            state["chat_name"]
        )

        last_progress = 0

        async for message in telethon_client.iter_messages(
            entity,
            reverse=True,
        ):

            if stop_event.is_set():

                state["status"] = "Stopped"
                break

            state["scanned"] += 1

            key = get_media_key(message)

            if key is None:
                continue

            if key in seen:

                duplicate_ids.append(
                    message.id
                )

                state["duplicates"] += 1

            else:

                seen.add(key)

            state["remaining"] = (
                len(duplicate_ids)
                - state["deleted"]
            )

            if (
                state["scanned"]
                - last_progress
                >= PROGRESS_SCAN_EVERY
            ):

                last_progress = (
                    state["scanned"]
                )

                await edit_progress(
                    progress_message,
                    user_id,
                )

                await asyncio.sleep(0)

        if stop_event.is_set():

            state["status"] = "Stopped"

            await edit_progress(
                progress_message,
                user_id,
            )

            return

        if not duplicate_ids:

            state["status"] = "Completed"
            state["remaining"] = 0

            await edit_progress(
                progress_message,
                user_id,
            )

            return

        # ====================================================
        # DELETE
        # ====================================================

        state["status"] = "Deleting"

        await edit_progress(
            progress_message,
            user_id,
        )

        total_duplicates = len(
            duplicate_ids
        )

        for start in range(
            0,
            total_duplicates,
            DELETE_BATCH_SIZE,
        ):

            if stop_event.is_set():

                state["status"] = "Stopped"
                break

            batch = duplicate_ids[
                start:start + DELETE_BATCH_SIZE
            ]

            success = False

            while not success:

                try:

                    await telethon_client.delete_messages(
                        entity,
                        batch,
                        revoke=True,
                    )

                    success = True

                    state["deleted"] += len(
                        batch
                    )

                    state["remaining"] = (
                        total_duplicates
                        - state["deleted"]
                    )

                except FloodWaitError as e:

                    state["status"] = (
                        f"Waiting {e.seconds}s"
                    )

                    await edit_progress(
                        progress_message,
                        user_id,
                    )

                    logger.warning(
                        "FloodWait: %s seconds",
                        e.seconds,
                    )

                    await asyncio.sleep(
                        e.seconds
                    )

                    state["status"] = "Deleting"

                except RPCError as e:

                    logger.error(
                        "Telegram RPC error: %s",
                        e,
                    )

                    state["status"] = (
                        "Delete error"
                    )

                    break

                except Exception as e:

                    logger.exception(
                        "Delete error"
                    )

                    state["status"] = (
                        "Delete error"
                    )

                    break

            await edit_progress(
                progress_message,
                user_id,
            )

            await asyncio.sleep(
                DELETE_PAUSE
            )

        if stop_event.is_set():

            state["status"] = "Stopped"

        elif state["remaining"] == 0:

            state["status"] = "Completed"

        else:

            state["status"] = (
                "Completed with errors"
            )

        await edit_progress(
            progress_message,
            user_id,
        )

    except Exception as e:

        logger.exception(
            "Cleanup failed"
        )

        state["status"] = "Error"

        try:

            await progress_message.edit_text(
                state_text(user_id)
                + "\n\n"
                + "❌ <b>Error:</b>\n"
                + f"<code>{str(e)[:1000]}</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=main_menu(),
            )

        except Exception:
            pass

    finally:

        state["running"] = False

        running_tasks.pop(
            user_id,
            None,
        )

        stop_events.pop(
            user_id,
            None,
        )


# ============================================================
# PIPE TASK
# ============================================================

async def pipe_chat(
    user_id: int,
    log_message,
):

    state = get_state(user_id)

    chat_id = selected_chats.get(user_id)

    if not chat_id:
        return

    deleted = 0
    failed = 0
    total_seen = 0
    batch = []

    try:

        entity = await get_entity_from_chat_id(
            chat_id
        )

        chat_name = state["chat_name"]

        await log_message.edit_text(
            "🧹 <b>PIPE STARTED</b>\n\n"
            f"📌 Chat: <b>{chat_name}</b>\n"
            f"🛡️ Keeping: <b>{PIPE_KEEP_MESSAGES}</b>\n\n"
            "⏳ Finding old messages...",
            parse_mode=ParseMode.HTML,
        )

        # ====================================================
        # IMPORTANT:
        # reverse=False would start from oldest.
        #
        # We use reverse=True so the newest messages
        # come first.
        #
        # First 20 = KEEP
        # Everything after that = DELETE
        # ====================================================

        async for message in telethon_client.iter_messages(
            entity,
            reverse=False,
        ):

            total_seen += 1

            # Keep newest 20 messages.
            if total_seen <= PIPE_KEEP_MESSAGES:
                continue

            batch.append(message.id)

            if len(batch) < PIPE_DELETE_BATCH_SIZE:
                continue

            # ------------------------------------------------
            # DELETE BATCH
            # ------------------------------------------------

            while batch:

                try:

                    await telethon_client.delete_messages(
                        entity,
                        batch,
                        revoke=True,
                    )

                    deleted += len(batch)

                    batch.clear()

                except FloodWaitError as e:

                    await log_message.edit_text(
                        "⏳ <b>PIPE WAITING</b>\n\n"
                        f"📌 Chat: <b>{chat_name}</b>\n"
                        f"🗑️ Deleted: <b>{deleted:,}</b>\n"
                        f"⏱️ Telegram wait: "
                        f"<b>{e.seconds}s</b>\n\n"
                        "Will continue automatically.",
                        parse_mode=ParseMode.HTML,
                    )

                    logger.warning(
                        "PIPE FloodWait: %s seconds",
                        e.seconds,
                    )

                    await asyncio.sleep(
                        e.seconds
                    )

                except RPCError as e:

                    failed += len(batch)

                    logger.error(
                        "PIPE RPC error: %s",
                        e,
                    )

                    batch.clear()

                except Exception as e:

                    failed += len(batch)

                    logger.exception(
                        "PIPE delete error"
                    )

                    batch.clear()

            await asyncio.sleep(
                PIPE_DELETE_PAUSE
            )

            # ------------------------------------------------
            # UPDATE LOG
            # ------------------------------------------------

            try:

                await log_message.edit_text(
                    "🧹 <b>PIPE RUNNING</b>\n\n"
                    f"📌 Chat: <b>{chat_name}</b>\n"
                    f"📨 Processed: <b>{total_seen:,}</b>\n"
                    f"🗑️ Deleted: <b>{deleted:,}</b>\n"
                    f"❌ Failed: <b>{failed:,}</b>\n"
                    f"🛡️ Keeping: <b>{PIPE_KEEP_MESSAGES}</b>\n\n"
                    "⏳ Continuing...",
                    parse_mode=ParseMode.HTML,
                )

            except Exception:
                pass

        # ====================================================
        # FINAL BATCH
        # ====================================================

        if batch:

            while batch:

                try:

                    await telethon_client.delete_messages(
                        entity,
                        batch,
                        revoke=True,
                    )

                    deleted += len(batch)

                    batch.clear()

                except FloodWaitError as e:

                    await log_message.edit_text(
                        "⏳ <b>PIPE WAITING</b>\n\n"
                        f"📌 Chat: <b>{chat_name}</b>\n"
                        f"🗑️ Deleted: <b>{deleted:,}</b>\n"
                        f"⏱️ Waiting: <b>{e.seconds}s</b>",
                        parse_mode=ParseMode.HTML,
                    )

                    await asyncio.sleep(
                        e.seconds
                    )

                except RPCError as e:

                    failed += len(batch)

                    logger.error(
                        "PIPE final RPC error: %s",
                        e,
                    )

                    batch.clear()

                except Exception as e:

                    failed += len(batch)

                    logger.exception(
                        "PIPE final delete error"
                    )

                    batch.clear()

        # ====================================================
        # FINAL REPORT
        # ====================================================

        kept = min(
            total_seen,
            PIPE_KEEP_MESSAGES,
        )

        await log_message.edit_text(
            "✅ <b>PIPE COMPLETED</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"📌 Chat: <b>{chat_name}</b>\n"
            f"📨 Found: <b>{total_seen:,}</b>\n"
            f"🗑️ Deleted: <b>{deleted:,}</b>\n"
            f"❌ Failed: <b>{failed:,}</b>\n"
            f"🛡️ Kept: <b>{kept}</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "✅ Target chat cleaned.\n"
            "📌 Last 20 messages were kept.",
            parse_mode=ParseMode.HTML,
        )

    except Exception as e:

        logger.exception(
            "PIPE failed"
        )

        try:

            await log_message.edit_text(
                "❌ <b>PIPE FAILED</b>\n\n"
                f"📌 Chat: <b>{state['chat_name']}</b>\n"
                f"📨 Processed: <b>{total_seen:,}</b>\n"
                f"🗑️ Deleted: <b>{deleted:,}</b>\n"
                f"❌ Failed: <b>{failed:,}</b>\n\n"
                f"<code>{str(e)[:1000]}</code>",
                parse_mode=ParseMode.HTML,
            )

        except Exception:
            pass

    finally:

        pipe_tasks.pop(
            user_id,
            None,
        )


# ============================================================
# /START
# ============================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await require_admin(update):
        return

    user_id = update.effective_user.id

    state = get_state(user_id)

    selected = selected_chats.get(
        user_id
    )

    if selected:

        text = (
            "🧹 <b>DUPLICATE CLEANER</b>\n\n"
            f"📌 Connected: "
            f"<b>{state['chat_name']}</b>\n\n"
            "Choose an option:"
        )

    else:

        text = (
            "🧹 <b>DUPLICATE CLEANER</b>\n\n"
            "Clean old duplicate media from "
            "your admin groups/channels.\n\n"
            "👇 Connect a chat to begin."
        )

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=(
            selected_chat_menu()
            if selected
            else main_menu()
        ),
    )


# ============================================================
# /STATUS
# ============================================================

async def status_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await require_admin(update):
        return

    user_id = update.effective_user.id

    await update.message.reply_text(
        state_text(user_id),
        parse_mode=ParseMode.HTML,
    )


# ============================================================
# /STOP
# ============================================================

async def stop_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await require_admin(update):
        return

    user_id = update.effective_user.id

    event = stop_events.get(
        user_id
    )

    state = get_state(user_id)

    if not state["running"] or not event:

        await update.message.reply_text(
            "ℹ️ Nothing is running."
        )

        return

    event.set()

    state["status"] = "Stopping..."

    await update.message.reply_text(
        "🛑 Stop requested.\n\n"
        "The current operation will stop safely."
    )


# ============================================================
# /PIPE
# ============================================================

async def pipe_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await require_admin(update):
        return

    user_id = update.effective_user.id

    # --------------------------------------------------------
    # CHAT MUST BE CONNECTED
    # --------------------------------------------------------

    if user_id not in selected_chats:

        await update.message.reply_text(
            "❌ Please connect a chat first."
        )

        return

    # --------------------------------------------------------
    # PREVENT TWO PIPE TASKS
    # --------------------------------------------------------

    if user_id in pipe_tasks:

        await update.message.reply_text(
            "⚠️ PIPE is already running."
        )

        return

    state = get_state(user_id)

    # --------------------------------------------------------
    # LOG ONLY IN LOG GROUP
    # --------------------------------------------------------

    try:

        log_message = await context.bot.send_message(
            chat_id=PIPE_LOG_CHAT_ID,
            text=(
                "🧹 <b>PIPE STARTED</b>\n\n"
                "━━━━━━━━━━━━━━\n"
                f"📌 Chat: <b>{state['chat_name']}</b>\n"
                f"🛡️ Keep: <b>{PIPE_KEEP_MESSAGES}</b>\n"
                "━━━━━━━━━━━━━━\n\n"
                "⏳ Processing..."
            ),
            parse_mode=ParseMode.HTML,
        )

    except Exception as e:

        logger.exception(
            "Could not send PIPE log message"
        )

        await update.message.reply_text(
            "❌ Could not start PIPE.\n\n"
            "Please check PIPE_LOG_CHAT_ID "
            "and make sure the bot is inside "
            "the log group."
        )

        return

    # --------------------------------------------------------
    # START BACKGROUND TASK
    # --------------------------------------------------------

    task = asyncio.create_task(
        pipe_chat(
            user_id,
            log_message,
        )
    )

    pipe_tasks[user_id] = task

    # --------------------------------------------------------
    # IMPORTANT:
    # NO REPORT IS SENT TO TARGET CHAT.
    # --------------------------------------------------------


# ============================================================
# CALLBACK HANDLER
# ============================================================

async def callback_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    query = update.callback_query

    await query.answer()

    user = query.from_user

    if not is_admin(user.id):

        await query.answer(
            "❌ Not authorized.",
            show_alert=True,
        )

        return

    user_id = user.id

    data = query.data

    # --------------------------------------------------------
    # HOME
    # --------------------------------------------------------

    if data == "home":

        state = get_state(user_id)

        if user_id in selected_chats:

            await query.edit_message_text(
                "🧹 <b>DUPLICATE CLEANER</b>\n\n"
                f"📌 Connected: "
                f"<b>{state['chat_name']}</b>\n\n"
                "Choose an option:",
                parse_mode=ParseMode.HTML,
                reply_markup=selected_chat_menu(),
            )

        else:

            await query.edit_message_text(
                "🧹 <b>DUPLICATE CLEANER</b>\n\n"
                "Choose an option:",
                parse_mode=ParseMode.HTML,
                reply_markup=main_menu(),
            )

        return

    # --------------------------------------------------------
    # CONNECT
    # --------------------------------------------------------

    if data == "connect":

        await show_chat_list(
            query,
            user_id,
            0,
        )

        return

    # --------------------------------------------------------
    # REFRESH
    # --------------------------------------------------------

    if data == "refresh":

        chat_cache.pop(
            user_id,
            None,
        )

        await show_chat_list(
            query,
            user_id,
            0,
        )

        return

    # --------------------------------------------------------
    # PAGE
    # --------------------------------------------------------

    if data.startswith("page:"):

        try:

            page = int(
                data.split(":", 1)[1]
            )

        except ValueError:

            page = 0

        await show_chat_list(
            query,
            user_id,
            page,
        )

        return

    # --------------------------------------------------------
    # SELECT CHAT
    # --------------------------------------------------------

    if data.startswith("select:"):

        try:

            index = int(
                data.split(":", 1)[1]
            )

        except ValueError:

            await query.edit_message_text(
                "❌ Invalid selection.",
                reply_markup=main_menu(),
            )

            return

        chats = chat_cache.get(
            user_id,
            [],
        )

        if (
            index < 0
            or index >= len(chats)
        ):

            await query.edit_message_text(
                "❌ Chat list expired.\n"
                "Please refresh.",
                reply_markup=main_menu(),
            )

            return

        chat_id, title, chat_type = (
            chats[index]
        )

        selected_chats[user_id] = (
            chat_id
        )

        state = get_state(user_id)

        state["chat_name"] = title
        state["status"] = "Connected"

        await query.edit_message_text(
            "✅ <b>CHAT CONNECTED</b>\n\n"
            f"📌 <b>{title}</b>\n"
            f"📂 {chat_type}\n\n"
            "You can now scan the old history.",
            parse_mode=ParseMode.HTML,
            reply_markup=selected_chat_menu(),
        )

        return

    # --------------------------------------------------------
    # SCAN
    # --------------------------------------------------------

    if data == "scan":

        if user_id not in selected_chats:

            await query.edit_message_text(
                "❌ Please connect a chat first.",
                reply_markup=main_menu(),
            )

            return

        state = get_state(user_id)

        if state["running"]:

            await query.answer(
                "A scan is already running.",
                show_alert=True,
            )

            return

        state["status"] = "Starting"

        progress_message = query.message

        task = asyncio.create_task(
            cleanup_chat(
                user_id,
                progress_message,
            )
        )

        running_tasks[user_id] = task

        await query.edit_message_text(
            state_text(user_id),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🛑 Stop",
                            callback_data="stop",
                        )
                    ]
                ]
            ),
        )

        return

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    if data == "status":

        await query.edit_message_text(
            state_text(user_id),
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu(),
        )

        return

    # --------------------------------------------------------
    # STOP
    # --------------------------------------------------------

    if data == "stop":

        event = stop_events.get(
            user_id
        )

        state = get_state(user_id)

        if (
            not state["running"]
            or not event
        ):

            await query.edit_message_text(
                "ℹ️ Nothing is running.",
                reply_markup=main_menu(),
            )

            return

        event.set()

        state["status"] = "Stopping..."

        await query.edit_message_text(
            state_text(user_id),
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu(),
        )

        return


# ============================================================
# STARTUP
# ============================================================

async def post_init(
    application: Application,
):

    logger.info(
        "Connecting Telethon..."
    )

    await ensure_telethon_connected()

    me = await telethon_client.get_me()

    logger.info(
        "Telethon account: %s (%s)",
        me.first_name,
        me.id,
    )


# ============================================================
# SHUTDOWN
# ============================================================

async def post_shutdown(
    application: Application,
):

    logger.info(
        "Disconnecting Telethon..."
    )

    try:

        await telethon_client.disconnect()

    except Exception as e:

        logger.warning(
            "Disconnect error: %s",
            e,
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

    application.add_handler(
        CommandHandler(
            "start",
            start_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "status",
            status_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "stop",
            stop_command,
        )
    )

    # ========================================================
    # PIPE COMMAND
    # ========================================================

    application.add_handler(
        CommandHandler(
            "pipe",
            pipe_command,
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            callback_handler
        )
    )

    logger.info(
        "Duplicate Cleaner started."
    )

    application.run_polling(
        allowed_updates=[
            "message",
            "callback_query",
        ]
    )


if __name__ == "__main__":

    main()
