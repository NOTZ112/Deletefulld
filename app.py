import os
import asyncio
import logging
from typing import Dict, Set

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, RPCError

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatMemberStatus, ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ChatMemberHandler,
    ContextTypes,
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

AUTO_WIPE_LIMIT = 1500
WIPE_KEEP = 50

DELETE_BATCH = 100
PROGRESS_EVERY = 500
DELETE_PAUSE = 0.05


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

logger = logging.getLogger("wipe-duplicate-bot")


# ============================================================
# MONGO
# ============================================================

mongo = AsyncIOMotorClient(MONGO_URI)

db = mongo["duplicate_cleaner"]

approved_groups = db["approved_groups"]


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
wipe_tasks: Dict[int, asyncio.Task] = {}

auto_wipe_locks: Dict[int, asyncio.Lock] = {}

pending_approvals: Dict[int, dict] = {}


# ============================================================
# TELETHON CONNECT
# ============================================================

async def ensure_tg():

    if not tg.is_connected():
        await tg.connect()

    if not await tg.is_user_authorized():
        raise RuntimeError(
            "STRING_SESSION is not authorized"
        )


# ============================================================
# PERMISSION CHECKS
# ============================================================

async def is_dupe_approved(chat_id: int) -> bool:

    row = await approved_groups.find_one(
        {
            "_id": int(chat_id),
            "approved_dupe": True,
        }
    )

    return row is not None


async def is_wipe_approved(chat_id: int) -> bool:

    row = await approved_groups.find_one(
        {
            "_id": int(chat_id),
            "approved_wipe": True,
        }
    )

    return row is not None


# ============================================================
# SAVE DUPE APPROVAL
# ============================================================

async def save_dupe_approval(
    chat_id: int,
    title: str,
    admin_id: int,
):

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
# SAVE WIPE APPROVAL
# ============================================================

async def save_wipe_approval(
    chat_id: int,
    title: str,
    admin_id: int,
):

    await approved_groups.update_one(
        {"_id": int(chat_id)},
        {
            "$set": {
                "title": title,
                "approved_wipe": True,
                "wipe_approved_by": int(admin_id),
            }
        },
        upsert=True,
    )


# ============================================================
# BOT ADMIN
# ============================================================

def is_bot_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


# ============================================================
# GET CHAT INFO
# ============================================================

async def resolve_chat_info(
    context,
    chat_id: int,
):

    title = "Unknown Chat"
    chat_type = "unknown"

    try:

        target_chat = await context.bot.get_chat(
            chat_id
        )

        title = (
            getattr(target_chat, "title", None)
            or getattr(target_chat, "username", None)
            or str(chat_id)
        )

        chat_type = getattr(
            target_chat,
            "type",
            "unknown",
        )

        return title, chat_type

    except Exception as e:

        logger.warning(
            "Bot API could not resolve %s: %s",
            chat_id,
            e,
        )

    try:

        entity = await get_entity(chat_id)

        title = (
            getattr(entity, "title", None)
            or getattr(entity, "username", None)
            or str(chat_id)
        )

        if getattr(entity, "broadcast", False):
            chat_type = "channel"
        else:
            chat_type = "group"

    except Exception as e:

        logger.warning(
            "Telethon could not resolve %s: %s",
            chat_id,
            e,
        )

    return title, chat_type


# ============================================================
# /APPROVEDUPE
# ============================================================

async def approvedupe_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user

    if not message or not chat:
        return

    if chat.id != LOG_CHAT_ID:
        return

    if not user or not is_bot_admin(user.id):
        return

    if not context.args:

        await message.reply_text(
            (
                "⚠️ <b>APPROVEDUPE</b>\n\n"
                "Use:\n"
                "<code>/approvedupe -100XXXXXXXXXX</code>"
            ),
            parse_mode=ParseMode.HTML,
        )

        return

    try:

        target_id = int(context.args[0])

    except ValueError:

        await message.reply_text(
            "❌ Invalid Chat ID."
        )

        return

    title, chat_type = await resolve_chat_info(
        context,
        target_id,
    )

    await save_dupe_approval(
        target_id,
        title,
        user.id,
    )

    await message.reply_text(
        (
            "♻️ <b>DUPLICATE CLEAN APPROVED</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"📌 Name: <b>{title}</b>\n"
            f"🆔 <code>{target_id}</code>\n"
            f"📂 Type: <b>{chat_type}</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "♻️ DCLEAN: <b>ENABLED</b>\n"
            "🧹 WIPE: <b>OFF</b>\n"
            "⚡ AUTO WIPE: <b>OFF</b>\n\n"
            "ℹ️ Use <code>/approvewipe CHAT_ID</code> "
            "to enable WIPE."
        ),
        parse_mode=ParseMode.HTML,
    )

    logger.info(
        "DUPE approved: %s by %s",
        target_id,
        user.id,
    )


# ============================================================
# /APPROVEWIPE
# ============================================================

async def approvewipe_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user

    if not message or not chat:
        return

    if chat.id != LOG_CHAT_ID:
        return

    if not user or not is_bot_admin(user.id):
        return

    if not context.args:

        await message.reply_text(
            (
                "⚠️ <b>APPROVEWIPE</b>\n\n"
                "Use:\n"
                "<code>/approvewipe -100XXXXXXXXXX</code>"
            ),
            parse_mode=ParseMode.HTML,
        )

        return

    try:

        target_id = int(context.args[0])

    except ValueError:

        await message.reply_text(
            "❌ Invalid Chat ID."
        )

        return

    title, chat_type = await resolve_chat_info(
        context,
        target_id,
    )

    await save_wipe_approval(
        target_id,
        title,
        user.id,
    )

    await message.reply_text(
        (
            "🧹 <b>WIPE APPROVED</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"📌 Name: <b>{title}</b>\n"
            f"🆔 <code>{target_id}</code>\n"
            f"📂 Type: <b>{chat_type}</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "🧹 WIPE: <b>ENABLED</b>\n"
            "⚡ AUTO WIPE: <b>ENABLED</b>\n"
            f"🛡️ Keep: <b>{WIPE_KEEP}</b>\n"
            f"⚡ Limit: <b>{AUTO_WIPE_LIMIT}</b>"
        ),
        parse_mode=ParseMode.HTML,
    )

    logger.info(
        "WIPE approved: %s by %s",
        target_id,
        user.id,
    )


# ============================================================
# LOG
# ============================================================

async def log_send(
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    reply_markup=None,
):

    return await context.bot.send_message(
        chat_id=LOG_CHAT_ID,
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup,
    )


async def log_edit(
    message,
    text: str,
):

    try:

        await message.edit_text(
            text,
            parse_mode=ParseMode.HTML,
        )

    except Exception as e:

        logger.debug(
            "Log edit failed: %s",
            e,
        )


# ============================================================
# GET TARGET ENTITY
# ============================================================

async def get_entity(chat_id: int):

    await ensure_tg()

    return await tg.get_entity(chat_id)


# ============================================================
# MEDIA UNIQUE KEY
# ============================================================

def media_key(message):

    document = getattr(
        message,
        "document",
        None,
    )

    if document:

        file_unique_id = getattr(
            document,
            "file_unique_id",
            None,
        )

        if file_unique_id:

            return (
                "document",
                str(file_unique_id),
            )

        return (
            "document",
            int(document.id),
        )

    photo = getattr(
        message,
        "photo",
        None,
    )

    if photo:

        return (
            "photo",
            int(photo.id),
        )

    return None


# ============================================================
# D CLEAN
# ============================================================

async def dclean_chat(
    chat_id: int,
    log_message,
):

    scanned = 0
    duplicates = 0
    deleted = 0
    failed = 0

    duplicate_ids = []

    seen: Set[tuple] = set()

    try:

        entity = await get_entity(
            chat_id
        )

        title = getattr(
            entity,
            "title",
            str(chat_id),
        )

        await log_edit(
            log_message,
            (
                "🧹 <b>DCLEAN STARTED</b>\n\n"
                "━━━━━━━━━━━━━━\n"
                f"📌 Chat: <b>{title}</b>\n"
                f"🆔 <code>{chat_id}</code>\n"
                "━━━━━━━━━━━━━━\n\n"
                "🔍 Scanning history..."
            ),
        )

        last_progress = 0

        async for message in tg.iter_messages(
            entity,
            reverse=True,
        ):

            scanned += 1

            key = media_key(message)

            if key is None:
                continue

            if key in seen:

                duplicate_ids.append(
                    message.id
                )

                duplicates += 1

            else:

                seen.add(key)

            if (
                scanned - last_progress
                >= PROGRESS_EVERY
            ):

                last_progress = scanned

                await log_edit(
                    log_message,
                    (
                        "🧹 <b>DCLEAN</b>\n\n"
                        "━━━━━━━━━━━━━━\n"
                        f"📌 Chat: <b>{title}</b>\n"
                        f"🆔 <code>{chat_id}</code>\n"
                        "━━━━━━━━━━━━━━\n\n"
                        f"🔍 Scanned: "
                        f"<b>{scanned:,}</b>\n"
                        f"♻️ Duplicates: "
                        f"<b>{duplicates:,}</b>\n"
                        f"🗑️ Deleted: "
                        f"<b>{deleted:,}</b>\n"
                        f"⏳ Remaining: "
                        f"<b>{len(duplicate_ids):,}</b>"
                    ),
                )

                await asyncio.sleep(0)

        if not duplicate_ids:

            await log_edit(
                log_message,
                (
                    "✅ <b>DCLEAN COMPLETED</b>\n\n"
                    "━━━━━━━━━━━━━━\n"
                    f"📌 Chat: <b>{title}</b>\n"
                    f"🆔 <code>{chat_id}</code>\n"
                    "━━━━━━━━━━━━━━\n\n"
                    f"📨 Scanned: <b>{scanned:,}</b>\n"
                    "♻️ Duplicates: <b>0</b>\n"
                    "🗑️ Deleted: <b>0</b>\n\n"
                    "✅ No duplicate files found."
                ),
            )

            return

        total = len(duplicate_ids)

        await log_edit(
            log_message,
            (
                "🗑️ <b>DELETING DUPLICATES</b>\n\n"
                "━━━━━━━━━━━━━━\n"
                f"📌 Chat: <b>{title}</b>\n"
                "━━━━━━━━━━━━━━\n\n"
                f"🔍 Scanned: <b>{scanned:,}</b>\n"
                f"♻️ Duplicates: <b>{total:,}</b>\n"
                "🗑️ Deleted: <b>0</b>\n"
                f"⏳ Remaining: <b>{total:,}</b>"
            ),
        )

        for start in range(
            0,
            total,
            DELETE_BATCH,
        ):

            batch = duplicate_ids[
                start:start + DELETE_BATCH
            ]

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
                        (
                            "⏳ <b>TELEGRAM FLOOD WAIT</b>\n\n"
                            f"Waiting <b>{e.seconds}s</b>...\n\n"
                            f"🗑️ Deleted: <b>{deleted:,}</b>\n"
                            f"⏳ Remaining: <b>{total-deleted:,}</b>"
                        ),
                    )

                    await asyncio.sleep(e.seconds)

                except RPCError as e:

                    logger.error(
                        "Delete RPC error: %s",
                        e,
                    )

                    failed += len(batch)

                    break

                except Exception as e:

                    logger.exception(
                        "Delete error"
                    )

                    failed += len(batch)

                    break

            if (
                deleted % 500 == 0
                or deleted == total
            ):

                await log_edit(
                    log_message,
                    (
                        "🧹 <b>DCLEAN PROGRESS</b>\n\n"
                        "━━━━━━━━━━━━━━\n"
                        f"📌 Chat: <b>{title}</b>\n"
                        "━━━━━━━━━━━━━━\n\n"
                        f"🔍 Scanned: <b>{scanned:,}</b>\n"
                        f"♻️ Duplicates: <b>{total:,}</b>\n"
                        f"🗑️ Deleted: <b>{deleted:,}</b>\n"
                        f"❌ Failed: <b>{failed:,}</b>\n"
                        f"⏳ Remaining: <b>{max(total-deleted,0):,}</b>"
                    ),
                )

            await asyncio.sleep(DELETE_PAUSE)

        await log_edit(
            log_message,
            (
                "✅ <b>DCLEAN COMPLETED</b>\n\n"
                "━━━━━━━━━━━━━━\n"
                f"📌 Chat: <b>{title}</b>\n"
                f"🆔 <code>{chat_id}</code>\n"
                "━━━━━━━━━━━━━━\n\n"
                f"📨 Scanned: <b>{scanned:,}</b>\n"
                f"♻️ Duplicates: <b>{total:,}</b>\n"
                f"🗑️ Deleted: <b>{deleted:,}</b>\n"
                f"❌ Failed: <b>{failed:,}</b>\n\n"
                "✅ Duplicate files removed."
            ),
        )

    except Exception as e:

        logger.exception("DCLEAN failed")

        await log_edit(
            log_message,
            (
                "❌ <b>DCLEAN FAILED</b>\n\n"
                f"🆔 <code>{chat_id}</code>\n\n"
                f"<code>{str(e)[:1200]}</code>"
            ),
        )


# ============================================================
# /DCLEAN
# ============================================================

async def dclean_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user

    if not message or not chat:
        return

    if chat.id != LOG_CHAT_ID:
        return

    if not user or not is_bot_admin(user.id):
        return

    if not context.args:

        await message.reply_text(
            "Usage:\n\n"
            "<code>/dclean -100XXXXXXXXXX</code>",
            parse_mode=ParseMode.HTML,
        )

        return

    try:

        target_id = int(context.args[0])

    except ValueError:

        await message.reply_text(
            "❌ Invalid Chat ID."
        )

        return

    if not await is_dupe_approved(target_id):

        await message.reply_text(
            "❌ Duplicate cleaning is not approved for this chat."
        )

        return

    old = dclean_tasks.get(target_id)

    if old and not old.done():

        await message.reply_text(
            "⚠️ DCLEAN is already running for this chat."
        )

        return

    log_message = await message.reply_text(
        "🧹 <b>DCLEAN STARTED</b>\n\n"
        f"🆔 <code>{target_id}</code>\n\n"
        "🔍 Scanning...",
        parse_mode=ParseMode.HTML,
    )

    task = asyncio.create_task(
        dclean_chat(
            target_id,
            log_message,
        )
    )

    dclean_tasks[target_id] = task


# ============================================================
# WIPE
# ============================================================

async def wipe_chat(
    chat_id: int,
    log_message,
):

    deleted = 0
    total = 0

    try:

        entity = await get_entity(chat_id)

        title = getattr(
            entity,
            "title",
            str(chat_id),
        )

        await log_edit(
            log_message,
            (
                "🧹 <b>WIPE STARTED</b>\n\n"
                "━━━━━━━━━━━━━━\n"
                f"📌 Chat: <b>{title}</b>\n"
                f"🆔 <code>{chat_id}</code>\n"
                f"🛡️ Keep: <b>{WIPE_KEEP}</b>\n"
                "━━━━━━━━━━━━━━\n\n"
                "🔍 Reading messages..."
            ),
        )

        ids = []

        async for msg in tg.iter_messages(
            entity,
            reverse=False,
        ):

            total += 1

            if total <= WIPE_KEEP:
                continue

            ids.append(msg.id)

            if len(ids) >= DELETE_BATCH:

                while ids:

                    try:

                        await tg.delete_messages(
                            entity,
                            ids,
                            revoke=True,
                        )

                        deleted += len(ids)
                        ids.clear()

                    except FloodWaitError as e:

                        await asyncio.sleep(e.seconds)

                    except Exception:

                        ids.clear()

                await log_edit(
                    log_message,
                    (
                        "🧹 <b>WIPE PROGRESS</b>\n\n"
                        f"📌 <b>{title}</b>\n\n"
                        f"📨 Found: <b>{total:,}</b>\n"
                        f"🗑️ Deleted: <b>{deleted:,}</b>\n"
                        f"🛡️ Keep: <b>{WIPE_KEEP}</b>"
                    ),
                )

        if ids:

            try:

                await tg.delete_messages(
                    entity,
                    ids,
                    revoke=True,
                )

                deleted += len(ids)

            except FloodWaitError as e:

                await asyncio.sleep(e.seconds)

                try:

                    await tg.delete_messages(
                        entity,
                        ids,
                        revoke=True,
                    )

                    deleted += len(ids)

                except Exception:
                    pass

            except Exception:
                pass

        await log_edit(
            log_message,
            (
                "✅ <b>WIPE COMPLETED</b>\n\n"
                "━━━━━━━━━━━━━━\n"
                f"📌 Chat: <b>{title}</b>\n"
                f"🆔 <code>{chat_id}</code>\n"
                "━━━━━━━━━━━━━━\n\n"
                f"📨 Found: <b>{total:,}</b>\n"
                f"🗑️ Deleted: <b>{deleted:,}</b>\n"
                f"🛡️ Kept: <b>{min(total, WIPE_KEEP)}</b>\n"
                "━━━━━━━━━━━━━━"
            ),
        )

    except Exception as e:

        logger.exception("WIPE failed")

        await log_edit(
            log_message,
            (
                "❌ <b>WIPE FAILED</b>\n\n"
                f"🆔 <code>{chat_id}</code>\n\n"
                f"<code>{str(e)[:1200]}</code>"
            ),
        )


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

    if not message or not chat:
        return

    if chat.id != LOG_CHAT_ID:
        return

    if not user or not is_bot_admin(user.id):
        return

    if not context.args:

        await message.reply_text(
            "Usage:\n\n"
            "<code>/wipe -100XXXXXXXXXX</code>",
            parse_mode=ParseMode.HTML,
        )

        return

    try:

        target_id = int(context.args[0])

    except ValueError:

        await message.reply_text(
            "❌ Invalid Chat ID."
        )

        return

    if not await is_wipe_approved(target_id):

        await message.reply_text(
            "❌ WIPE is not approved for this chat."
        )

        return

    old = wipe_tasks.get(target_id)

    if old and not old.done():

        await message.reply_text(
            "⚠️ WIPE already running."
        )

        return

    log_message = await message.reply_text(
        "🧹 <b>WIPE STARTED</b>\n\n"
        f"🆔 <code>{target_id}</code>\n"
        f"🛡️ Keep: <b>{WIPE_KEEP}</b>",
        parse_mode=ParseMode.HTML,
    )

    task = asyncio.create_task(
        wipe_chat(
            target_id,
            log_message,
        )
    )

    wipe_tasks[target_id] = task


# ============================================================
# AUTO WIPE
# ============================================================

async def auto_wipe_check(
    chat_id: int,
):

    # --------------------------------------------------------
    # IMPORTANT:
    # AUTO WIPE ONLY IF WIPE APPROVED
    # --------------------------------------------------------

    if not await is_wipe_approved(chat_id):
        return

    lock = auto_wipe_locks.setdefault(
        chat_id,
        asyncio.Lock(),
    )

    if lock.locked():
        return

    async with lock:

        try:

            entity = await get_entity(chat_id)

            count = 0

            async for _ in tg.iter_messages(
                entity,
                limit=AUTO_WIPE_LIMIT,
            ):

                count += 1

                if count >= AUTO_WIPE_LIMIT:
                    break

            if count < AUTO_WIPE_LIMIT:
                return

            try:

                log_message = await tg_bot.send_message(
                    LOG_CHAT_ID,
                    (
                        "⚡ <b>AUTO WIPE STARTED</b>\n\n"
                        f"🆔 <code>{chat_id}</code>\n"
                        f"📨 Threshold: <b>{AUTO_WIPE_LIMIT}</b>\n"
                        f"🛡️ Keep: <b>{WIPE_KEEP}</b>"
                    ),
                    parse_mode=ParseMode.HTML,
                )

            except Exception:
                return

            old = wipe_tasks.get(chat_id)

            if old and not old.done():
                return

            wipe_tasks[chat_id] = asyncio.create_task(
                wipe_chat(
                    chat_id,
                    log_message,
                )
            )

        except Exception as e:

            logger.error(
                "Auto wipe check failed: %s",
                e,
            )


# ============================================================
# TELEGRAM BOT OBJECT
# ============================================================

tg_bot = None


# ============================================================
# MESSAGE WATCHER
# ============================================================

async def message_watcher(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    message = update.effective_message

    if not message:
        return

    chat = update.effective_chat

    if not chat:
        return

    if chat.type not in (
        "group",
        "supergroup",
        "channel",
    ):
        return

    # --------------------------------------------------------
    # ONLY WIPE-APPROVED CHATS CAN AUTO WIPE
    # --------------------------------------------------------

    if not await is_wipe_approved(chat.id):
        return

    await auto_wipe_check(chat.id)


# ============================================================
# BOT ADDED
# ============================================================

async def bot_membership_update(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    change = update.my_chat_member

    if not change:
        return

    chat = change.chat

    if chat.type not in (
        "group",
        "supergroup",
    ):
        return

    old = change.old_chat_member
    new = change.new_chat_member

    was = old.status in (
        ChatMemberStatus.MEMBER,
        ChatMemberStatus.ADMINISTRATOR,
    )

    now = new.status in (
        ChatMemberStatus.MEMBER,
        ChatMemberStatus.ADMINISTRATOR,
    )

    if not now or was:
        return

    if (
        await is_wipe_approved(chat.id)
        or await is_dupe_approved(chat.id)
    ):
        return

    title = (
        chat.title
        or "Unknown Group"
    )

    added_by = change.from_user

    pending_approvals[chat.id] = {
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
                    "♻️ DUPE",
                    callback_data=(
                        f"approvedupe:{chat.id}"
                    ),
                ),
                InlineKeyboardButton(
                    "🧹 WIPE",
                    callback_data=(
                        f"approvewipe:{chat.id}"
                    ),
                ),
            ]
        ]
    )

    await context.bot.send_message(
        LOG_CHAT_ID,
        (
            "🤖 <b>NEW GROUP ADDED</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"📌 Group: <b>{title}</b>\n"
            f"🆔 <code>{chat.id}</code>\n"
            "━━━━━━━━━━━━━━\n\n"
            "Choose approval type:"
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
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

    if not user or not is_bot_admin(user.id):

        await query.answer(
            "❌ Not authorized",
            show_alert=True,
        )

        return

    await query.answer()

    data = query.data

    # --------------------------------------------------------
    # DUPE APPROVE
    # --------------------------------------------------------

    if data.startswith("approvedupe:"):

        chat_id = int(
            data.split(":", 1)[1]
        )

        pending = pending_approvals.get(chat_id)

        title = (
            pending["title"]
            if pending
            else "Unknown"
        )

        await save_dupe_approval(
            chat_id,
            title,
            user.id,
        )

        pending_approvals.pop(
            chat_id,
            None,
        )

        await query.edit_message_text(
            (
                "♻️ <b>DUPLICATE CLEAN APPROVED</b>\n\n"
                f"📌 <b>{title}</b>\n"
                f"🆔 <code>{chat_id}</code>\n\n"
                "♻️ DCLEAN: <b>ON</b>\n"
                "🧹 WIPE: <b>OFF</b>\n"
                "⚡ AUTO WIPE: <b>OFF</b>"
            ),
            parse_mode=ParseMode.HTML,
        )

        return

    # --------------------------------------------------------
    # WIPE APPROVE
    # --------------------------------------------------------

    if data.startswith("approvewipe:"):

        chat_id = int(
            data.split(":", 1)[1]
        )

        pending = pending_approvals.get(chat_id)

        title = (
            pending["title"]
            if pending
            else "Unknown"
        )

        await save_wipe_approval(
            chat_id,
            title,
            user.id,
        )

        pending_approvals.pop(
            chat_id,
            None,
        )

        await query.edit_message_text(
            (
                "🧹 <b>WIPE APPROVED</b>\n\n"
                f"📌 <b>{title}</b>\n"
                f"🆔 <code>{chat_id}</code>\n\n"
                "🧹 WIPE: <b>ON</b>\n"
                "⚡ AUTO WIPE: <b>ON</b>\n"
                "♻️ DCLEAN: <b>OFF</b>"
            ),
            parse_mode=ParseMode.HTML,
        )

        return


# ============================================================
# START
# ============================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    user = update.effective_user

    if not user or not is_bot_admin(user.id):
        return

    await update.effective_message.reply_text(
        (
            "🧹 <b>WIPE + DUPLICATE CLEANER</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            "♻️ Duplicate approval:\n"
            "<code>/approvedupe CHAT_ID</code>\n\n"
            "🧹 Wipe approval:\n"
            "<code>/approvewipe CHAT_ID</code>\n\n"
            "♻️ Duplicate Clean:\n"
            "<code>/dclean CHAT_ID</code>\n\n"
            "⚡ Wipe:\n"
            "<code>/wipe CHAT_ID</code>\n"
            "━━━━━━━━━━━━━━\n\n"
            f"🛡️ Wipe Keep: <b>{WIPE_KEEP}</b>\n"
            f"⚡ Auto Wipe: <b>{AUTO_WIPE_LIMIT}</b> messages"
        ),
        parse_mode=ParseMode.HTML,
    )


# ============================================================
# POST INIT
# ============================================================

async def post_init(
    application: Application,
):

    global tg_bot

    tg_bot = application.bot

    await ensure_tg()

    me = await tg.get_me()

    logger.info(
        "String Session connected: %s (%s)",
        me.first_name,
        me.id,
    )


# ============================================================
# SHUTDOWN
# ============================================================

async def post_shutdown(
    application: Application,
):

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

    # START
    application.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )

    # APPROVE DUPE
    application.add_handler(
        CommandHandler(
            "approvedupe",
            approvedupe_command,
        )
    )

    # APPROVE WIPE
    application.add_handler(
        CommandHandler(
            "approvewipe",
            approvewipe_command,
        )
    )

    # DCLEAN
    application.add_handler(
        CommandHandler(
            "dclean",
            dclean_command,
        )
    )

    # WIPE
    application.add_handler(
        CommandHandler(
            "wipe",
            wipe_command,
        )
    )

    # BOT MEMBERSHIP
    application.add_handler(
        ChatMemberHandler(
            bot_membership_update,
            ChatMemberHandler.MY_CHAT_MEMBER,
        )
    )

    # CALLBACK
    application.add_handler(
        CallbackQueryHandler(
            callback_handler
        )
    )

    # MESSAGE WATCHER
    from telegram.ext import MessageHandler, filters

    application.add_handler(
        MessageHandler(
            filters.ALL,
            message_watcher,
        ),
        group=10,
    )

    logger.info(
        "Bot started."
    )

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
