import os
import asyncio
import logging
from typing import Optional

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, RPCError

from motor.motor_asyncio import AsyncIOMotorClient

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ChatMemberStatus, ParseMode
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

logger = logging.getLogger("wipe-bot")


# ============================================================
# ENV
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

API_ID_RAW = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()
STRING_SESSION = os.getenv("STRING_SESSION", "").strip()

ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()

PIPE_LOG_CHAT_ID_RAW = os.getenv(
    "PIPE_LOG_CHAT_ID",
    "",
).strip()

MONGO_URI = os.getenv(
    "MONGO_URI",
    "",
).strip()


# ============================================================
# REQUIRED ENV CHECK
# ============================================================

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

if not MONGO_URI:
    raise RuntimeError("MONGO_URI is missing")


API_ID = int(API_ID_RAW)

LOG_CHAT_ID = int(
    PIPE_LOG_CHAT_ID_RAW
)


# ============================================================
# ADMIN IDS
# ============================================================

def parse_admin_ids(value: str) -> set[int]:

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


ADMIN_IDS = parse_admin_ids(
    ADMIN_IDS_RAW
)


# ============================================================
# WIPE SETTINGS
# ============================================================

# Automatic WIPE threshold
WIPE_THRESHOLD = 1500

# Latest messages to keep
WIPE_KEEP_MESSAGES = 50

# Delete batch
WIPE_BATCH_SIZE = 100

# Small pause between batches
WIPE_PAUSE = 0.05

# Progress update interval
WIPE_PROGRESS_EVERY = 500


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
# GLOBAL STATE
# ============================================================

wipe_tasks: dict[int, asyncio.Task] = {}

wipe_locks: dict[int, asyncio.Lock] = {}

pending_approvals: dict[int, dict] = {}


# ============================================================
# LOCK
# ============================================================

def get_wipe_lock(
    chat_id: int,
) -> asyncio.Lock:

    if chat_id not in wipe_locks:
        wipe_locks[chat_id] = asyncio.Lock()

    return wipe_locks[chat_id]


# ============================================================
# ADMIN CHECK
# ============================================================

def is_admin(
    user_id: int,
) -> bool:

    return user_id in ADMIN_IDS


async def require_bot_admin(
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
            "❌ You are not authorized."
        )

    return False


# ============================================================
# APPROVED GROUP
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
                "approved_by": int(
                    approved_by
                ),
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
# GET ENTITY
# ============================================================

async def get_entity(
    chat_id: int,
):

    await ensure_telethon_connected()

    return await telethon_client.get_entity(
        int(chat_id)
    )


# ============================================================
# COUNT MESSAGES
# ============================================================

async def count_messages(
    entity,
) -> int:

    await ensure_telethon_connected()

    try:

        result = await telethon_client(
            __import__(
                "telethon"
            ).functions.messages.GetHistoryRequest(
                peer=entity,
                offset_id=0,
                offset_date=None,
                add_offset=0,
                limit=0,
                max_id=0,
                min_id=0,
                hash=0,
            )
        )

        return int(
            getattr(
                result,
                "count",
                0,
            )
        )

    except Exception as e:

        logger.warning(
            "Count request failed: %s",
            e,
        )

        # Fallback
        count = 0

        async for _ in telethon_client.iter_messages(
            entity,
            limit=None,
        ):

            count += 1

        return count


# ============================================================
# FAST MESSAGE COUNT
# ============================================================

async def get_message_count(
    entity,
) -> int:

    try:

        from telethon import functions

        result = await telethon_client(
            functions.messages.GetHistoryRequest(
                peer=entity,
                offset_id=0,
                offset_date=None,
                add_offset=0,
                limit=1,
                max_id=0,
                min_id=0,
                hash=0,
            )
        )

        return int(
            getattr(
                result,
                "count",
                0,
            )
        )

    except Exception as e:

        logger.warning(
            "Fast count failed: %s",
            e,
        )

        return 0


# ============================================================
# LOG MESSAGE
# ============================================================

async def send_log(
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    reply_markup=None,
):

    try:

        return await context.bot.send_message(
            chat_id=LOG_CHAT_ID,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
        )

    except Exception as e:

        logger.exception(
            "Could not send log message: %s",
            e,
        )

        return None


# ============================================================
# WIPE FUNCTION
# ============================================================

async def wipe_chat(
    chat_id: int,
    chat_name: str,
    context: ContextTypes.DEFAULT_TYPE,
    trigger: str = "Manual",
):

    lock = get_wipe_lock(
        chat_id
    )

    if lock.locked():

        logger.info(
            "WIPE already running: %s",
            chat_id,
        )

        return

    async with lock:

        entity = None

        found = 0
        deleted = 0
        failed = 0

        progress_message = None

        try:

            # ------------------------------------------------
            # ENTITY
            # ------------------------------------------------

            entity = await get_entity(
                chat_id
            )

            # ------------------------------------------------
            # START LOG
            # ------------------------------------------------

            progress_message = await send_log(
                context,
                (
                    "✅ <b>WIPE STARTED</b>\n\n"
                    "━━━━━━━━━━━━━━\n"
                    f"📌 Chat: "
                    f"<b>{chat_name}</b>\n"
                    f"🆔 <code>{chat_id}</code>\n"
                    f"🎯 Trigger: "
                    f"<b>{trigger}</b>\n"
                    f"🛡️ Keep: "
                    f"<b>{WIPE_KEEP_MESSAGES}</b>\n"
                    "━━━━━━━━━━━━━━\n\n"
                    "⏳ Processing..."
                ),
            )

            # ------------------------------------------------
            # GET TOTAL
            # ------------------------------------------------

            total = await get_message_count(
                entity
            )

            logger.info(
                "WIPE %s total messages: %s",
                chat_id,
                total,
            )

            # ------------------------------------------------
            # NOTHING TO DELETE
            # ------------------------------------------------

            if total <= WIPE_KEEP_MESSAGES:

                text = (
                    "ℹ️ <b>WIPE NOT REQUIRED</b>\n\n"
                    "━━━━━━━━━━━━━━\n"
                    f"📌 Chat: "
                    f"<b>{chat_name}</b>\n"
                    f"🆔 <code>{chat_id}</code>\n"
                    f"📨 Messages: "
                    f"<b>{total:,}</b>\n"
                    f"🛡️ Kept: "
                    f"<b>{WIPE_KEEP_MESSAGES}</b>\n"
                    "━━━━━━━━━━━━━━\n\n"
                    "Nothing to delete."
                )

                if progress_message:

                    await progress_message.edit_text(
                        text,
                        parse_mode=ParseMode.HTML,
                    )

                return

            # ------------------------------------------------
            # OLD MESSAGE IDS
            # ------------------------------------------------

            delete_ids = []

            seen = 0

            # newest -> oldest
            async for message in telethon_client.iter_messages(
                entity,
                reverse=False,
            ):

                seen += 1

                # Keep newest 50
                if seen <= WIPE_KEEP_MESSAGES:

                    continue

                delete_ids.append(
                    message.id
                )

                # ------------------------------------------------
                # DELETE BATCH
                # ------------------------------------------------

                if len(delete_ids) >= WIPE_BATCH_SIZE:

                    batch = list(
                        delete_ids
                    )

                    delete_ids.clear()

                    while batch:

                        try:

                            await telethon_client.delete_messages(
                                entity,
                                batch,
                                revoke=True,
                            )

                            deleted += len(
                                batch
                            )

                            batch.clear()

                        except FloodWaitError as e:

                            logger.warning(
                                "FloodWait %s seconds",
                                e.seconds,
                            )

                            if progress_message:

                                try:

                                    await progress_message.edit_text(
                                        (
                                            "⏳ <b>WIPE WAITING</b>\n\n"
                                            f"📌 Chat: "
                                            f"<b>{chat_name}</b>\n"
                                            f"🗑️ Deleted: "
                                            f"<b>{deleted:,}</b>\n"
                                            f"⏱️ Telegram wait: "
                                            f"<b>{e.seconds}s</b>\n\n"
                                            "Will continue automatically."
                                        ),
                                        parse_mode=ParseMode.HTML,
                                    )

                                except Exception:
                                    pass

                            await asyncio.sleep(
                                e.seconds
                            )

                        except RPCError as e:

                            logger.error(
                                "WIPE RPC error: %s",
                                e,
                            )

                            failed += len(
                                batch
                            )

                            batch.clear()

                        except Exception as e:

                            logger.exception(
                                "WIPE delete error"
                            )

                            failed += len(
                                batch
                            )

                            batch.clear()

                    # ------------------------------------------------
                    # PROGRESS
                    # ------------------------------------------------

                    if progress_message:

                        if (
                            deleted
                            % WIPE_PROGRESS_EVERY
                            == 0
                            or deleted == 0
                        ):

                            try:

                                await progress_message.edit_text(
                                    (
                                        "🧹 <b>WIPE IN PROGRESS</b>\n\n"
                                        "━━━━━━━━━━━━━━\n"
                                        f"📌 Chat: "
                                        f"<b>{chat_name}</b>\n"
                                        f"📨 Found: "
                                        f"<b>{seen:,}</b>\n"
                                        f"🗑️ Deleted: "
                                        f"<b>{deleted:,}</b>\n"
                                        f"❌ Failed: "
                                        f"<b>{failed:,}</b>\n"
                                        f"🛡️ Keep: "
                                        f"<b>{WIPE_KEEP_MESSAGES}</b>\n"
                                        "━━━━━━━━━━━━━━"
                                    ),
                                    parse_mode=ParseMode.HTML,
                                )

                            except Exception:
                                pass

                    await asyncio.sleep(
                        WIPE_PAUSE
                    )

            # ------------------------------------------------
            # FINAL BATCH
            # ------------------------------------------------

            if delete_ids:

                batch = list(
                    delete_ids
                )

                while batch:

                    try:

                        await telethon_client.delete_messages(
                            entity,
                            batch,
                            revoke=True,
                        )

                        deleted += len(
                            batch
                        )

                        batch.clear()

                    except FloodWaitError as e:

                        logger.warning(
                            "Final FloodWait: %s",
                            e.seconds,
                        )

                        await asyncio.sleep(
                            e.seconds
                        )

                    except RPCError as e:

                        logger.error(
                            "Final RPC error: %s",
                            e,
                        )

                        failed += len(
                            batch
                        )

                        batch.clear()

                    except Exception as e:

                        logger.exception(
                            "Final delete error"
                        )

                        failed += len(
                            batch
                        )

                        batch.clear()

            # ------------------------------------------------
            # COMPLETED
            # ------------------------------------------------

            kept = min(
                total,
                WIPE_KEEP_MESSAGES,
            )

            result_text = (
                "✅ <b>WIPE COMPLETED</b>\n\n"
                "━━━━━━━━━━━━━━\n"
                f"📌 Chat: "
                f"<b>{chat_name}</b>\n"
                f"🆔 <code>{chat_id}</code>\n"
                f"📨 Found: "
                f"<b>{total:,}</b>\n"
                f"🗑️ Deleted: "
                f"<b>{deleted:,}</b>\n"
                f"❌ Failed: "
                f"<b>{failed:,}</b>\n"
                f"🛡️ Kept: "
                f"<b>{kept}</b>\n"
                "━━━━━━━━━━━━━━\n\n"
                "✅ Target chat cleaned."
            )

            if progress_message:

                try:

                    await progress_message.edit_text(
                        result_text,
                        parse_mode=ParseMode.HTML,
                    )

                except Exception:

                    await send_log(
                        context,
                        result_text,
                    )

        except Exception as e:

            logger.exception(
                "WIPE failed"
            )

            error_text = (
                "❌ <b>WIPE FAILED</b>\n\n"
                "━━━━━━━━━━━━━━\n"
                f"📌 Chat: "
                f"<b>{chat_name}</b>\n"
                f"🆔 <code>{chat_id}</code>\n"
                f"📨 Processed: "
                f"<b>{found:,}</b>\n"
                f"🗑️ Deleted: "
                f"<b>{deleted:,}</b>\n"
                f"❌ Failed: "
                f"<b>{failed:,}</b>\n"
                "━━━━━━━━━━━━━━\n\n"
                f"<code>{str(e)[:1000]}</code>"
            )

            if progress_message:

                try:

                    await progress_message.edit_text(
                        error_text,
                        parse_mode=ParseMode.HTML,
                    )

                except Exception:
                    pass

            else:

                await send_log(
                    context,
                    error_text,
                )

        finally:

            wipe_tasks.pop(
                chat_id,
                None,
            )


# ============================================================
# START
# ============================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await require_bot_admin(
        update
    ):
        return

    await update.message.reply_text(
        "🧹 <b>WIPE BOT</b>\n\n"
        "━━━━━━━━━━━━━━\n"
        f"🛡️ Keep: "
        f"<b>{WIPE_KEEP_MESSAGES}</b> messages\n"
        f"⚡ Auto WIPE: "
        f"<b>{WIPE_THRESHOLD}</b> messages\n"
        "━━━━━━━━━━━━━━\n\n"
        "Manual:\n"
        "<code>/wipe -100XXXXXXXXXX</code>",
        parse_mode=ParseMode.HTML,
    )


# ============================================================
# /WIPE
# ============================================================

async def wipe_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    message = update.effective_message

    if not message:
        return

    # --------------------------------------------------------
    # ONLY LOG GROUP
    # --------------------------------------------------------

    if (
        update.effective_chat
        and update.effective_chat.id
        != LOG_CHAT_ID
    ):

        await message.reply_text(
            "❌ Use /wipe from the Log group."
        )

        return

    # --------------------------------------------------------
    # ADMIN
    # --------------------------------------------------------

    if not await require_bot_admin(
        update
    ):
        return

    # --------------------------------------------------------
    # ARGUMENT
    # --------------------------------------------------------

    if not context.args:

        await message.reply_text(
            "❌ Chat ID missing.\n\n"
            "Example:\n"
            "<code>/wipe -1003719625887</code>",
            parse_mode=ParseMode.HTML,
        )

        return

    try:

        chat_id = int(
            context.args[0]
        )

    except ValueError:

        await message.reply_text(
            "❌ Invalid Chat ID."
        )

        return

    # --------------------------------------------------------
    # CHECK RUNNING
    # --------------------------------------------------------

    existing = wipe_tasks.get(
        chat_id
    )

    if (
        existing
        and not existing.done()
    ):

        await message.reply_text(
            "⏳ WIPE is already running "
            "for this chat."
        )

        return

    # --------------------------------------------------------
    # GET CHAT
    # --------------------------------------------------------

    try:

        chat = await context.bot.get_chat(
            chat_id
        )

        chat_name = (
            chat.title
            or str(chat_id)
        )

    except Exception as e:

        await message.reply_text(
            "❌ Could not access target chat.\n\n"
            f"<code>{str(e)[:700]}</code>",
            parse_mode=ParseMode.HTML,
        )

        return

    # --------------------------------------------------------
    # START
    # --------------------------------------------------------

    task = asyncio.create_task(
        wipe_chat(
            chat_id,
            chat_name,
            context,
            "Manual /wipe",
        )
    )

    wipe_tasks[
        chat_id
    ] = task

    await message.reply_text(
        "✅ WIPE command accepted.\n\n"
        f"📌 Chat: <b>{chat_name}</b>\n"
        f"🆔 <code>{chat_id}</code>\n\n"
        "Progress will be shown here.",
        parse_mode=ParseMode.HTML,
    )


# ============================================================
# GROUP ADMIN CHECK
# ============================================================

async def is_group_admin(
    update: Update,
) -> bool:

    message = update.effective_message
    chat = update.effective_chat

    if not message or not chat:
        return False

    if chat.type not in (
        "group",
        "supergroup",
    ):
        return False

    # --------------------------------------------------------
    # NORMAL ADMIN
    # --------------------------------------------------------

    user = update.effective_user

    if user:

        try:

            member = await update.get_bot().get_chat_member(
                chat.id,
                user.id,
            )

            if member.status in (
                ChatMemberStatus.ADMINISTRATOR,
                ChatMemberStatus.OWNER,
            ):

                return True

        except Exception as e:

            logger.warning(
                "Group admin check failed: %s",
                e,
            )

    # --------------------------------------------------------
    # ANONYMOUS ADMIN
    # --------------------------------------------------------

    sender_chat = message.sender_chat

    if (
        sender_chat
        and sender_chat.id == chat.id
    ):

        return True

    return False


# ============================================================
# AUTO WIPE CHECK
# ============================================================

async def automatic_wipe_check(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    message = update.effective_message
    chat = update.effective_chat

    if not message or not chat:
        return

    if chat.type not in (
        "group",
        "supergroup",
    ):
        return

    # --------------------------------------------------------
    # APPROVAL
    # --------------------------------------------------------

    if not await is_group_approved(
        chat.id
    ):
        return

    # --------------------------------------------------------
    # ONLY MEDIA / NORMAL MESSAGES
    # --------------------------------------------------------

    try:

        total = await get_message_count(
            await get_entity(chat.id)
        )

    except Exception as e:

        logger.warning(
            "Auto count failed: %s",
            e,
        )

        return

    # --------------------------------------------------------
    # THRESHOLD
    # --------------------------------------------------------

    if total < WIPE_THRESHOLD:
        return

    # --------------------------------------------------------
    # ALREADY RUNNING
    # --------------------------------------------------------

    existing = wipe_tasks.get(
        chat.id
    )

    if (
        existing
        and not existing.done()
    ):
        return

    chat_name = (
        chat.title
        or "Unknown Group"
    )

    logger.info(
        "AUTO WIPE TRIGGERED: %s (%s messages)",
        chat_name,
        total,
    )

    task = asyncio.create_task(
        wipe_chat(
            chat.id,
            chat_name,
            context,
            f"Automatic ({total} messages)",
        )
    )

    wipe_tasks[
        chat.id
    ] = task


# ============================================================
# BOT ADDED
# ============================================================

async def bot_membership_update(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    membership = update.my_chat_member

    if not membership:
        return

    chat = membership.chat

    if chat.type not in (
        "group",
        "supergroup",
    ):
        return

    old_member = (
        membership.old_chat_member
    )

    new_member = (
        membership.new_chat_member
    )

    bot_was_member = (
        old_member.status
        in (
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
        )
    )

    bot_is_member = (
        new_member.status
        in (
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
        )
    )

    # --------------------------------------------------------
    # NEW GROUP
    # --------------------------------------------------------

    if (
        bot_is_member
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

        added_by = membership.from_user

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

        await send_log(
            context,
            (
                "🤖 <b>NEW GROUP ADDED</b>\n\n"
                "━━━━━━━━━━━━━━\n"
                f"📌 Group: "
                f"<b>{title}</b>\n"
                f"🆔 <code>{chat.id}</code>\n"
                f"👤 Added by: "
                f"<b>{added_name}</b>\n"
                "━━━━━━━━━━━━━━\n\n"
                "⚠️ This group is NOT approved yet.\n\n"
                "Approve this group?"
            ),
            keyboard,
        )


# ============================================================
# CALLBACK
# ============================================================

async def callback_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    query = update.callback_query

    if not query:
        return

    user = query.from_user

    if not is_admin(
        user.id
    ):

        await query.answer(
            "❌ Not authorized.",
            show_alert=True,
        )

        return

    data = query.data or ""

    await query.answer()

    # --------------------------------------------------------
    # APPROVE
    # --------------------------------------------------------

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

        await approve_group(
            chat_id,
            pending["title"],
            user.id,
        )

        pending_approvals.pop(
            chat_id,
            None,
        )

        await query.edit_message_text(
            "✅ <b>GROUP APPROVED</b>\n\n"
            f"📌 Group: "
            f"<b>{pending['title']}</b>\n"
            f"🆔 <code>{chat_id}</code>\n\n"
            "🟢 Automatic WIPE enabled.\n"
            f"⚡ Threshold: "
            f"<b>{WIPE_THRESHOLD}</b>\n"
            f"🛡️ Keep: "
            f"<b>{WIPE_KEEP_MESSAGES}</b>",
            parse_mode=ParseMode.HTML,
        )

        return

    # --------------------------------------------------------
    # REJECT
    # --------------------------------------------------------

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
            else "Unknown"
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
            f"📌 Group: "
            f"<b>{title}</b>\n"
            f"🆔 <code>{chat_id}</code>",
            parse_mode=ParseMode.HTML,
        )

        return


# ============================================================
# POST INIT
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
# POST SHUTDOWN
# ============================================================

async def post_shutdown(
    application: Application,
):

    logger.info(
        "Shutting down..."
    )

    try:

        await telethon_client.disconnect()

    except Exception:
        pass

    try:

        mongo_client.close()

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
            "wipe",
            wipe_command,
        )
    )

    # --------------------------------------------------------
    # BOT ADDED
    # --------------------------------------------------------

    application.add_handler(
        ChatMemberHandler(
            bot_membership_update,
            ChatMemberHandler.MY_CHAT_MEMBER,
        )
    )

    # --------------------------------------------------------
    # CALLBACK
    # --------------------------------------------------------

    application.add_handler(
        CallbackQueryHandler(
            callback_handler
        )
    )

    # --------------------------------------------------------
    # AUTOMATIC WIPE
    #
    # Every new message checks the count.
    # When count >= 1500, WIPE starts.
    # --------------------------------------------------------

    application.add_handler(
        __import__(
            "telegram.ext"
        ).MessageHandler(
            __import__(
                "telegram.ext"
            ).filters.ALL,
            automatic_wipe_check,
        ),
        group=10,
    )

    logger.info(
        "WIPE BOT STARTED"
    )

    logger.info(
        "Threshold: %s",
        WIPE_THRESHOLD,
    )

    logger.info(
        "Keep: %s",
        WIPE_KEEP_MESSAGES,
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
