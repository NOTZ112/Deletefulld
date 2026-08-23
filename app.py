import os
import asyncio
import logging
from typing import Dict, List, Tuple

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, RPCError

from motor.motor_asyncio import AsyncIOMotorClient

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
    ChatMemberHandler,
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

# LOG GROUP / CHAT
LOG_CHAT_ID_RAW = os.getenv(
    "LOG_CHAT_ID",
    os.getenv("PIPE_LOG_CHAT_ID", ""),
).strip()

MONGO_URI = os.getenv(
    "MONGO_URI",
    "",
).strip()


# ============================================================
# REQUIRED VAR CHECK
# ============================================================

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")

if not API_ID_RAW:
    raise RuntimeError("API_ID is missing")

if not API_HASH:
    raise RuntimeError("API_HASH is missing")

if not STRING_SESSION:
    raise RuntimeError("STRING_SESSION is missing")

if not LOG_CHAT_ID_RAW:
    raise RuntimeError("LOG_CHAT_ID is missing")

if not MONGO_URI:
    raise RuntimeError("MONGO_URI is missing")


API_ID = int(API_ID_RAW)
LOG_CHAT_ID = int(LOG_CHAT_ID_RAW)


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
                item,
            )

    return result


ADMIN_IDS = parse_admin_ids(ADMIN_IDS_RAW)


# ============================================================
# MONGODB
# ============================================================

mongo_client = AsyncIOMotorClient(
    MONGO_URI
)

db = mongo_client[
    "duplicate_cleaner"
]

approved_groups = db[
    "approved_groups"
]


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

DELETE_PAUSE = 0.20


# ============================================================
# WIPE SETTINGS
# ============================================================

# Newest N messages will be kept.
WIPE_KEEP_MESSAGES = 20

WIPE_DELETE_BATCH_SIZE = 100

WIPE_DELETE_PAUSE = 0.20


# ============================================================
# GLOBAL STATE
# ============================================================

selected_chats: Dict[int, int] = {}

chat_pages: Dict[int, int] = {}

chat_cache: Dict[
    int,
    List[Tuple[int, str, str]]
] = {}

running_tasks: Dict[
    int,
    asyncio.Task
] = {}

stop_events: Dict[
    int,
    asyncio.Event
] = {}

states: Dict[int, dict] = {}

wipe_tasks: Dict[
    int,
    asyncio.Task
] = {}


# ============================================================
# APPROVAL STATE
# ============================================================

pending_approvals: Dict[int, dict] = {}


# ============================================================
# STATE
# ============================================================

def get_state(
    user_id: int,
) -> dict:

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


def reset_state(
    user_id: int,
):

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

def is_admin(
    user_id: int,
) -> bool:

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
# APPROVED GROUP FUNCTIONS
# ============================================================

async def is_group_approved(
    chat_id: int,
) -> bool:

    document = await approved_groups.find_one(
        {
            "_id": int(chat_id),
            "approved": True,
        }
    )

    return document is not None


async def approve_group(
    chat_id: int,
    title: str,
    approved_by: int,
):

    await approved_groups.update_one(
        {
            "_id": int(chat_id),
        },
        {
            "$set": {
                "title": title,
                "approved": True,
                "approved_by": int(approved_by),
            }
        },
        upsert=True,
    )


async def reject_group(
    chat_id: int,
):

    await approved_groups.update_one(
        {
            "_id": int(chat_id),
        },
        {
            "$set": {
                "approved": False,
            }
        },
        upsert=True,
    )


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
                getattr(
                    permissions,
                    "is_creator",
                    False,
                )
            )

            is_admin = bool(
                getattr(
                    permissions,
                    "is_admin",
                    False,
                )
            )

            if not (
                is_creator
                or is_admin
            ):
                continue

        except Exception as e:

            logger.warning(
                "Could not check permissions for %s: %s",
                getattr(
                    entity,
                    "title",
                    "Unknown",
                ),
                e,
            )

            continue

        chat_id = int(entity.id)

        if (
            getattr(
                entity,
                "broadcast",
                False,
            )
            or getattr(
                entity,
                "megagroup",
                False,
            )
        ):

            chat_id = int(
                f"-100{entity.id}"
            )

        title = (
            getattr(
                entity,
                "title",
                None,
            )
            or dialog.name
            or "Unknown Chat"
        )

        if getattr(
            entity,
            "broadcast",
            False,
        ):

            chat_type = "📢 Channel"

        elif getattr(
            entity,
            "megagroup",
            False,
        ):

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

async def get_entity_from_chat_id(
    chat_id: int,
):

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
                callback_data="connect",
            )
        ],
        [
            InlineKeyboardButton(
                "📊 Status",
                callback_data="status",
            )
        ],
        [
            InlineKeyboardButton(
                "❌ Stop",
                callback_data="stop",
            )
        ],
    ]

    return InlineKeyboardMarkup(
        keyboard
    )


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

        absolute_index = (
            start + index
        )

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
                callback_data=(
                    f"page:{page - 1}"
                ),
            )
        )

    if (
        start + CHAT_PAGE_SIZE
        < len(chats)
    ):

        navigation.append(
            InlineKeyboardButton(
                "Next ➡️",
                callback_data=(
                    f"page:{page + 1}"
                ),
            )
        )

    if navigation:

        keyboard.append(
            navigation
        )

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

    return InlineKeyboardMarkup(
        keyboard
    )


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

            chats = chat_cache[
                user_id
            ]

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
            len(chats)
            + CHAT_PAGE_SIZE
            - 1
        ) // CHAT_PAGE_SIZE

        text = (
            "📂 <b>ADMIN CHATS</b>\n\n"
            f"Total: <b>{len(chats)}</b>\n"
            f"Page: <b>{page + 1}/"
            f"{total_pages}</b>\n\n"
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


# ============================================================
# SELECTED CHAT
# ============================================================

async def show_selected_chat(
    query,
    user_id: int,
):

    chat_id = selected_chats.get(
        user_id
    )

    if not chat_id:

        await query.edit_message_text(
            "❌ No chat connected.",
            reply_markup=main_menu(),
        )

        return

    state = get_state(
        user_id
    )

    await query.edit_message_text(
        "✅ <b>CHAT CONNECTED</b>\n\n"
        f"📌 Chat: <b>{state['chat_name']}</b>\n"
        f"🆔 ID: <code>{chat_id}</code>\n\n"
        "Ready to scan old messages.",
        parse_mode=ParseMode.HTML,
        reply_markup=selected_chat_menu(),
    )


# ============================================================
# STATE TEXT
# ============================================================

def state_text(
    user_id: int,
):

    state = get_state(
        user_id
    )

    return (
        "🧹 <b>DUPLICATE CLEANER</b>\n\n"
        f"📌 Chat: <b>{state['chat_name']}</b>\n\n"
        f"🔍 Scanned: "
        f"<b>{state['scanned']:,}</b>\n"
        f"♻️ Duplicates: "
        f"<b>{state['duplicates']:,}</b>\n"
        f"🗑️ Deleted: "
        f"<b>{state['deleted']:,}</b>\n"
        f"⏳ Remaining: "
        f"<b>{state['remaining']:,}</b>\n\n"
        f"📌 Status: "
        f"<b>{state['status']}</b>"
    )


# ============================================================
# MEDIA KEY
# ============================================================

def get_media_key(
    message,
):

    if not message.media:
        return None

    if getattr(
        message,
        "document",
        None,
    ):

        document = message.document

        return (
            "document",
            int(document.id),
        )

    if getattr(
        message,
        "photo",
        None,
    ):

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
# DUPLICATE CLEANER
# ============================================================

async def cleanup_chat(
    user_id: int,
    progress_message,
):

    state = get_state(
        user_id
    )

    chat_id = selected_chats.get(
        user_id
    )

    if not chat_id:

        state["status"] = (
            "No chat selected"
        )

        state["running"] = False

        return

    state["running"] = True
    state["status"] = "Scanning"
    state["scanned"] = 0
    state["duplicates"] = 0
    state["deleted"] = 0
    state["remaining"] = 0

    stop_event = asyncio.Event()

    stop_events[user_id] = (
        stop_event
    )

    seen = set()

    duplicate_ids = []

    try:

        entity = await get_entity_from_chat_id(
            chat_id
        )

        last_progress = 0

        async for message in (
            telethon_client.iter_messages(
                entity,
                reverse=True,
            )
        ):

            if stop_event.is_set():

                state["status"] = "Stopped"

                break

            state["scanned"] += 1

            key = get_media_key(
                message
            )

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
                start:
                start + DELETE_BATCH_SIZE
            ]

            try:

                await (
                    telethon_client
                    .delete_messages(
                        entity,
                        batch,
                        revoke=True,
                    )
                )

                state["deleted"] += (
                    len(batch)
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

                await asyncio.sleep(
                    e.seconds
                )

                continue

            except Exception as e:

                logger.exception(
                    "Delete error: %s",
                    e,
                )

                state["status"] = (
                    "Delete error"
                )

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
                + (
                    f"<code>{str(e)[:1000]}"
                    "</code>"
                ),
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
# WIPE LOG MESSAGE
# ============================================================

async def update_wipe_log(
    message,
    chat_name: str,
    chat_id: int,
    total_seen: int,
    deleted: int,
    failed: int,
    status: str,
):

    try:

        await message.edit_text(
            "🧹 <b>WIPE</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"📌 Chat: <b>{chat_name}</b>\n"
            f"🆔 <code>{chat_id}</code>\n"
            f"📨 Processed: "
            f"<b>{total_seen:,}</b>\n"
            f"🗑️ Deleted: "
            f"<b>{deleted:,}</b>\n"
            f"❌ Failed: "
            f"<b>{failed:,}</b>\n"
            f"🛡️ Kept: "
            f"<b>{WIPE_KEEP_MESSAGES}</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            f"📌 Status: <b>{status}</b>",
            parse_mode=ParseMode.HTML,
        )

    except Exception as e:

        logger.debug(
            "WIPE log update failed: %s",
            e,
        )


# ============================================================
# WIPE TASK
# ============================================================

async def wipe_chat(
    chat_id: int,
    chat_name: str,
    log_message,
):

    deleted = 0
    failed = 0
    total_seen = 0

    batch = []

    try:

        entity = await get_entity_from_chat_id(
            chat_id
        )

        await update_wipe_log(
            log_message,
            chat_name,
            chat_id,
            0,
            0,
            0,
            "Starting...",
        )

        # ----------------------------------------------------
        # NEWEST -> OLDEST
        # ----------------------------------------------------

        async for message in (
            telethon_client.iter_messages(
                entity,
                reverse=False,
            )
        ):

            total_seen += 1

            # Keep newest N messages
            if total_seen <= (
                WIPE_KEEP_MESSAGES
            ):

                continue

            batch.append(
                message.id
            )

            if len(batch) < (
                WIPE_DELETE_BATCH_SIZE
            ):

                continue

            while batch:

                try:

                    await (
                        telethon_client
                        .delete_messages(
                            entity,
                            batch,
                            revoke=True,
                        )
                    )

                    deleted += len(
                        batch
                    )

                    batch.clear()

                except FloodWaitError as e:

                    await update_wipe_log(
                        log_message,
                        chat_name,
                        chat_id,
                        total_seen,
                        deleted,
                        failed,
                        f"Waiting {e.seconds}s...",
                    )

                    await asyncio.sleep(
                        e.seconds
                    )

                except RPCError as e:

                    failed += len(
                        batch
                    )

                    logger.error(
                        "WIPE RPC error: %s",
                        e,
                    )

                    batch.clear()

                except Exception as e:

                    failed += len(
                        batch
                    )

                    logger.exception(
                        "WIPE delete error"
                    )

                    batch.clear()

            await update_wipe_log(
                log_message,
                chat_name,
                chat_id,
                total_seen,
                deleted,
                failed,
                "Deleting...",
            )

            await asyncio.sleep(
                WIPE_DELETE_PAUSE
            )

        # ----------------------------------------------------
        # FINAL BATCH
        # ----------------------------------------------------

        if batch:

            while batch:

                try:

                    await (
                        telethon_client
                        .delete_messages(
                            entity,
                            batch,
                            revoke=True,
                        )
                    )

                    deleted += len(
                        batch
                    )

                    batch.clear()

                except FloodWaitError as e:

                    await update_wipe_log(
                        log_message,
                        chat_name,
                        chat_id,
                        total_seen,
                        deleted,
                        failed,
                        f"Waiting {e.seconds}s...",
                    )

                    await asyncio.sleep(
                        e.seconds
                    )

                except Exception as e:

                    failed += len(
                        batch
                    )

                    logger.exception(
                        "WIPE final delete error"
                    )

                    batch.clear()

        kept = min(
            total_seen,
            WIPE_KEEP_MESSAGES,
        )

        await update_wipe_log(
            log_message,
            chat_name,
            chat_id,
            total_seen,
            deleted,
            failed,
            "Completed",
        )

        try:

            await log_message.edit_text(
                "✅ <b>WIPE COMPLETED</b>\n\n"
                "━━━━━━━━━━━━━━\n"
                f"📌 Chat: <b>{chat_name}</b>\n"
                f"🆔 <code>{chat_id}</code>\n"
                f"📨 Found: "
                f"<b>{total_seen:,}</b>\n"
                f"🗑️ Deleted: "
                f"<b>{deleted:,}</b>\n"
                f"❌ Failed: "
                f"<b>{failed:,}</b>\n"
                f"🛡️ Kept: "
                f"<b>{kept}</b>\n"
                "━━━━━━━━━━━━━━\n\n"
                "✅ Target chat cleaned.",
                parse_mode=ParseMode.HTML,
            )

        except Exception:
            pass

    except Exception as e:

        logger.exception(
            "WIPE failed"
        )

        try:

            await log_message.edit_text(
                "❌ <b>WIPE FAILED</b>\n\n"
                f"📌 Chat: "
                f"<b>{chat_name}</b>\n"
                f"🆔 <code>{chat_id}</code>\n"
                f"📨 Processed: "
                f"<b>{total_seen:,}</b>\n"
                f"🗑️ Deleted: "
                f"<b>{deleted:,}</b>\n"
                f"❌ Failed: "
                f"<b>{failed:,}</b>\n\n"
                f"<code>{str(e)[:1000]}"
                "</code>",
                parse_mode=ParseMode.HTML,
            )

        except Exception:
            pass


# ============================================================
# /WIPE
# ============================================================

async def wipe_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user

    if not message or not chat or not user:
        return

    # --------------------------------------------------------
    # ONLY LOG CHAT
    # --------------------------------------------------------

    if chat.id != LOG_CHAT_ID:

        return

    # --------------------------------------------------------
    # ONLY BOT ADMINS
    # --------------------------------------------------------

    if not is_admin(user.id):

        await message.reply_text(
            "❌ Not authorized."
        )

        return

    # --------------------------------------------------------
    # CHECK ARGUMENT
    # --------------------------------------------------------

    if not context.args:

        await message.reply_text(
            "❌ <b>Usage:</b>\n\n"
            "<code>/wipe -1003719625887</code>\n\n"
            "Send the target Chat ID.",
            parse_mode=ParseMode.HTML,
        )

        return

    try:

        target_chat_id = int(
            context.args[0]
        )

    except ValueError:

        await message.reply_text(
            "❌ Invalid Chat ID.\n\n"
            "Example:\n"
            "<code>/wipe -1003719625887</code>",
            parse_mode=ParseMode.HTML,
        )

        return

    # --------------------------------------------------------
    # APPROVAL CHECK
    # --------------------------------------------------------

    approved = await is_group_approved(
        target_chat_id
    )

    if not approved:

        await message.reply_text(
            "❌ <b>CHAT NOT APPROVED</b>\n\n"
            f"🆔 <code>{target_chat_id}</code>\n\n"
            "This group must be approved first.",
            parse_mode=ParseMode.HTML,
        )

        return

    # --------------------------------------------------------
    # GET TARGET ENTITY
    # --------------------------------------------------------

    try:

        entity = await get_entity_from_chat_id(
            target_chat_id
        )

        chat_name = (
            getattr(
                entity,
                "title",
                None,
            )
            or "Unknown Chat"
        )

    except Exception as e:

        await message.reply_text(
            "❌ <b>Cannot access target chat.</b>\n\n"
            f"🆔 <code>{target_chat_id}</code>\n\n"
            f"<code>{str(e)[:800]}</code>",
            parse_mode=ParseMode.HTML,
        )

        return

    # --------------------------------------------------------
    # CHECK RUNNING
    # --------------------------------------------------------

    existing_task = wipe_tasks.get(
        target_chat_id
    )

    if (
        existing_task
        and not existing_task.done()
    ):

        await message.reply_text(
            "⚠️ A WIPE is already running for:\n\n"
            f"📌 <b>{chat_name}</b>\n"
            f"🆔 <code>{target_chat_id}</code>",
            parse_mode=ParseMode.HTML,
        )

        return

    # --------------------------------------------------------
    # START LOG
    # --------------------------------------------------------

    try:

        log_message = await (
            context.bot.send_message(
                chat_id=LOG_CHAT_ID,
                text=(
                    "🧹 <b>WIPE STARTED</b>\n\n"
                    "━━━━━━━━━━━━━━\n"
                    f"📌 Chat: "
                    f"<b>{chat_name}</b>\n"
                    f"🆔 <code>{target_chat_id}</code>\n"
                    f"🛡️ Keep: "
                    f"<b>{WIPE_KEEP_MESSAGES}</b>\n"
                    "━━━━━━━━━━━━━━\n\n"
                    "⏳ Processing..."
                ),
                parse_mode=ParseMode.HTML,
            )
        )

    except Exception as e:

        logger.exception(
            "Could not create WIPE log"
        )

        return

    # --------------------------------------------------------
    # CREATE TASK
    # --------------------------------------------------------

    task = asyncio.create_task(
        wipe_chat(
            target_chat_id,
            chat_name,
            log_message,
        )
    )

    wipe_tasks[
        target_chat_id
    ] = task

    await message.reply_text(
        "✅ <b>WIPE STARTED</b>\n\n"
        f"📌 Chat: <b>{chat_name}</b>\n"
        f"🆔 <code>{target_chat_id}</code>\n\n"
        "Progress will be shown here.",
        parse_mode=ParseMode.HTML,
    )


# ============================================================
# BOT ADDED / GROUP APPROVAL
# ============================================================

async def bot_membership_update(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    chat_member_update = (
        update.my_chat_member
    )

    if not chat_member_update:
        return

    new_member = (
        chat_member_update.new_chat_member
    )

    old_member = (
        chat_member_update.old_chat_member
    )

    chat = chat_member_update.chat

    if chat.type not in (
        "group",
        "supergroup",
    ):

        return

    bot_is_member_now = (
        new_member.status
        in (
            "member",
            "administrator",
        )
    )

    bot_was_member = (
        old_member.status
        in (
            "member",
            "administrator",
        )
    )

    if (
        bot_is_member_now
        and not bot_was_member
    ):

        if await is_group_approved(
            chat.id
        ):

            return

        title = (
            chat.title
            or "Unknown Group"
        )

        added_by = (
            chat_member_update.from_user
        )

        added_name = (
            added_by.full_name
            if added_by
            else "Unknown"
        )

        pending_approvals[
            chat.id
        ] = {
            "title": title,
            "added_by": (
                added_by.id
                if added_by
                else None
            ),
        }

        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "✅ APPROVE",
                        callback_data=(
                            f"approve_group:"
                            f"{chat.id}"
                        ),
                    ),
                    InlineKeyboardButton(
                        "❌ REJECT",
                        callback_data=(
                            f"reject_group:"
                            f"{chat.id}"
                        ),
                    ),
                ]
            ]
        )

        try:

            await context.bot.send_message(
                chat_id=LOG_CHAT_ID,
                text=(
                    "🤖 <b>NEW GROUP ADDED</b>\n\n"
                    "━━━━━━━━━━━━━━\n"
                    f"📌 Group: "
                    f"<b>{title}</b>\n"
                    f"🆔 ID: "
                    f"<code>{chat.id}</code>\n"
                    f"👤 Added by: "
                    f"<b>{added_name}</b>\n"
                    "━━━━━━━━━━━━━━\n\n"
                    "⚠️ This group is NOT approved yet.\n\n"
                    "Approve this group?"
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )

        except Exception as e:

            logger.exception(
                "Failed to send approval request: %s",
                e,
            )


# ============================================================
# CALLBACK HANDLER
# ============================================================

async def callback_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    query = update.callback_query

    user = query.from_user

    if not is_admin(
        user.id
    ):

        await query.answer(
            "❌ Not authorized.",
            show_alert=True,
        )

        return

    await query.answer()

    user_id = user.id

    data = query.data

    # ========================================================
    # APPROVE
    # ========================================================

    if data.startswith(
        "approve_group:"
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

        pending = pending_approvals.get(
            chat_id
        )

        if not pending:

            await query.edit_message_text(
                "⚠️ Approval request expired."
            )

            return

        title = pending[
            "title"
        ]

        await approve_group(
            chat_id,
            title,
            user.id,
        )

        pending_approvals.pop(
            chat_id,
            None,
        )

        await query.edit_message_text(
            "✅ <b>GROUP APPROVED</b>\n\n"
            f"📌 Group: <b>{title}</b>\n"
            f"🆔 <code>{chat_id}</code>\n\n"
            "🟢 This group can now be used "
            "with:\n\n"
            f"<code>/wipe {chat_id}</code>",
            parse_mode=ParseMode.HTML,
        )

        return

    # ========================================================
    # REJECT
    # ========================================================

    if data.startswith(
        "reject_group:"
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

        pending = pending_approvals.get(
            chat_id
        )

        title = (
            pending["title"]
            if pending
            else "Unknown Group"
        )

        await reject_group(
            chat_id
        )

        pending_approvals.pop(
            chat_id,
            None,
        )

        await query.edit_message_text(
            "❌ <b>GROUP REJECTED</b>\n\n"
            f"📌 Group: <b>{title}</b>\n"
            f"🆔 <code>{chat_id}</code>",
            parse_mode=ParseMode.HTML,
        )

        return

    # ========================================================
    # HOME
    # ========================================================

    if data == "home":

        state = get_state(
            user_id
        )

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

    # ========================================================
    # CONNECT
    # ========================================================

    if data == "connect":

        await show_chat_list(
            query,
            user_id,
            0,
        )

        return

    # ========================================================
    # REFRESH
    # ========================================================

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

    # ========================================================
    # PAGE
    # ========================================================

    if data.startswith(
        "page:"
    ):

        try:

            page = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

        except ValueError:

            page = 0

        await show_chat_list(
            query,
            user_id,
            page,
        )

        return

    # ========================================================
    # SELECT CHAT
    # ========================================================

    if data.startswith(
        "select:"
    ):

        try:

            index = int(
                data.split(
                    ":",
                    1,
                )[1]
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

        selected_chats[
            user_id
        ] = chat_id

        state = get_state(
            user_id
        )

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

    # ========================================================
    # SCAN
    # ========================================================

    if data == "scan":

        if user_id not in selected_chats:

            await query.edit_message_text(
                "❌ Please connect a chat first.",
                reply_markup=main_menu(),
            )

            return

        state = get_state(
            user_id
        )

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

        running_tasks[
            user_id
        ] = task

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

    # ========================================================
    # STATUS
    # ========================================================

    if data == "status":

        await query.edit_message_text(
            state_text(user_id),
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu(),
        )

        return

    # ========================================================
    # STOP
    # ========================================================

    if data == "stop":

        event = stop_events.get(
            user_id
        )

        state = get_state(
            user_id
        )

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

        state["status"] = (
            "Stopping..."
        )

        await query.edit_message_text(
            state_text(user_id),
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu(),
        )

        return


# ============================================================
# /START
# ============================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await require_admin(
        update
    ):

        return

    user_id = (
        update.effective_user.id
    )

    state = get_state(
        user_id
    )

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

    if not await require_admin(
        update
    ):

        return

    user_id = (
        update.effective_user.id
    )

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

    if not await require_admin(
        update
    ):

        return

    user_id = (
        update.effective_user.id
    )

    event = stop_events.get(
        user_id
    )

    state = get_state(
        user_id
    )

    if (
        not state["running"]
        or not event
    ):

        await update.message.reply_text(
            "ℹ️ Nothing is running."
        )

        return

    event.set()

    state["status"] = (
        "Stopping..."
    )

    await update.message.reply_text(
        "🛑 Stop requested.\n\n"
        "The current operation will stop safely."
    )


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

    try:

        mongo_client.close()

    except Exception as e:

        logger.warning(
            "Mongo disconnect error: %s",
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

    # NEW:
    # /wipe -1001234567890
    application.add_handler(
        CommandHandler(
            "wipe",
            wipe_command,
        )
    )

    # --------------------------------------------------------
    # BOT ADDED TO GROUP
    # --------------------------------------------------------

    application.add_handler(
        ChatMemberHandler(
            bot_membership_update,
            ChatMemberHandler.MY_CHAT_MEMBER,
        )
    )

    # --------------------------------------------------------
    # CALLBACKS
    # --------------------------------------------------------

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
            "my_chat_member",
        ]
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    main()
