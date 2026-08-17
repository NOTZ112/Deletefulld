import os
import asyncio
import logging
from typing import Optional

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, RPCError

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

API_ID_RAW = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()

STRING_SESSION = os.getenv("STRING_SESSION", "").strip()

CHAT_ID_RAW = os.getenv("CHAT_ID", "").strip()

ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()


# ============================================================
# VALIDATION
# ============================================================

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")

if not API_ID_RAW:
    raise RuntimeError("API_ID is missing")

if not API_HASH:
    raise RuntimeError("API_HASH is missing")

if not STRING_SESSION:
    raise RuntimeError("STRING_SESSION is missing")

if not CHAT_ID_RAW:
    raise RuntimeError("CHAT_ID is missing")


API_ID = int(API_ID_RAW)
CHAT_ID = int(CHAT_ID_RAW)


def parse_admin_ids():
    result = set()

    for item in ADMIN_IDS_RAW.split(","):
        item = item.strip()

        if not item:
            continue

        try:
            result.add(int(item))
        except ValueError:
            pass

    return result


ADMIN_IDS = parse_admin_ids()


# ============================================================
# TELETHON CLIENT
# ============================================================

telethon_client = TelegramClient(
    StringSession(STRING_SESSION),
    API_ID,
    API_HASH,
)


# ============================================================
# GLOBAL STATE
# ============================================================

scan_task: Optional[asyncio.Task] = None

stop_event = asyncio.Event()

state = {
    "running": False,
    "scanned": 0,
    "duplicates": 0,
    "deleted": 0,
    "remaining": 0,
    "status": "Idle",
}


# ============================================================
# ADMIN CHECK
# ============================================================

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def admin_only(update: Update) -> bool:

    if not update.effective_user:
        return False

    user_id = update.effective_user.id

    if not is_admin(user_id):

        if update.effective_message:
            await update.effective_message.reply_text(
                "❌ You are not authorized to use this bot."
            )

        return False

    return True


# ============================================================
# MEDIA KEY
# ============================================================

def get_media_key(message):

    """
    Fast duplicate detection.

    For documents/videos/audio/files:
        Telegram document ID

    For photos:
        Telegram photo ID

    This is fast because we do NOT download the media.
    """

    if not message.media:
        return None

    # Document / Video / Audio / File
    if getattr(message, "document", None):

        document = message.document

        return (
            "document",
            int(document.id),
        )

    # Photo
    if getattr(message, "photo", None):

        photo = message.photo

        return (
            "photo",
            int(photo.id),
        )

    return None


# ============================================================
# PROGRESS TEXT
# ============================================================

def progress_text():

    return (
        "🧹 <b>Duplicate Cleaner</b>\n\n"
        f"🔍 Scanned: <b>{state['scanned']:,}</b>\n"
        f"♻️ Duplicates: <b>{state['duplicates']:,}</b>\n"
        f"🗑️ Deleted: <b>{state['deleted']:,}</b>\n"
        f"⏳ Remaining: <b>{state['remaining']:,}</b>\n\n"
        f"📌 Status: <b>{state['status']}</b>"
    )


# ============================================================
# SAFE MESSAGE EDIT
# ============================================================

async def update_progress_message(message):

    try:

        await message.edit_text(
            progress_text(),
            parse_mode="HTML",
        )

    except Exception as e:

        logger.warning(
            "Progress message update failed: %s",
            e,
        )


# ============================================================
# SCANNER
# ============================================================

async def perform_scan(progress_message):

    global scan_task

    state["running"] = True
    state["status"] = "Scanning history"

    state["scanned"] = 0
    state["duplicates"] = 0
    state["deleted"] = 0
    state["remaining"] = 0

    stop_event.clear()

    # Stores media keys already seen.
    seen = set()

    # Duplicate Telegram message IDs.
    duplicate_message_ids = []

    last_update = 0

    try:

        logger.info("Starting scan for chat %s", CHAT_ID)

        await telethon_client.start()

        # Make sure the account can access the chat.
        entity = await telethon_client.get_entity(CHAT_ID)

        logger.info("Scanning chat: %s", entity)

        async for message in telethon_client.iter_messages(
            entity,
            reverse=True,
        ):

            if stop_event.is_set():

                state["status"] = "Stopped"

                break

            state["scanned"] += 1

            media_key = get_media_key(message)

            if media_key is None:
                continue

            if media_key in seen:

                state["duplicates"] += 1

                duplicate_message_ids.append(
                    message.id
                )

            else:

                seen.add(media_key)

            state["remaining"] = (
                len(duplicate_message_ids)
                - state["deleted"]
            )

            # Update Telegram message periodically.
            if (
                state["scanned"] - last_update
                >= 500
            ):

                last_update = state["scanned"]

                await update_progress_message(
                    progress_message
                )

                await asyncio.sleep(0)

        # Final scan update.
        await update_progress_message(
            progress_message
        )

        if stop_event.is_set():

            state["status"] = "Stopped during scan"

            await update_progress_message(
                progress_message
            )

            return

        # Nothing found.
        if not duplicate_message_ids:

            state["status"] = "Completed"

            state["remaining"] = 0

            await update_progress_message(
                progress_message
            )

            return

        # ====================================================
        # DELETE
        # ====================================================

        state["status"] = "Deleting duplicates"

        await update_progress_message(
            progress_message
        )

        batch_size = 100

        total_duplicates = len(
            duplicate_message_ids
        )

        for start in range(
            0,
            total_duplicates,
            batch_size,
        ):

            if stop_event.is_set():

                state["status"] = "Stopped"

                break

            batch = duplicate_message_ids[
                start:start + batch_size
            ]

            try:

                await telethon_client.delete_messages(
                    entity,
                    batch,
                    revoke=True,
                )

                state["deleted"] += len(batch)

                state["remaining"] = (
                    total_duplicates
                    - state["deleted"]
                )

            except FloodWaitError as e:

                state["status"] = (
                    f"FloodWait {e.seconds}s"
                )

                await update_progress_message(
                    progress_message
                )

                logger.warning(
                    "FloodWait: sleeping %s seconds",
                    e.seconds,
                )

                await asyncio.sleep(
                    e.seconds
                )

                # Retry
                try:

                    await telethon_client.delete_messages(
                        entity,
                        batch,
                        revoke=True,
                    )

                    state["deleted"] += len(
                        batch
                    )

                    state["remaining"] = (
                        total_duplicates
                        - state["deleted"]
                    )

                except Exception as retry_error:

                    logger.exception(
                        "Retry delete failed: %s",
                        retry_error,
                    )

            except RPCError as e:

                logger.error(
                    "Telegram RPC error: %s",
                    e,
                )

            except Exception as e:

                logger.exception(
                    "Delete error: %s",
                    e,
                )

            # Update every batch.
            await update_progress_message(
                progress_message
            )

            await asyncio.sleep(0.15)

        if stop_event.is_set():

            state["status"] = "Stopped"

        elif state["remaining"] == 0:

            state["status"] = "Completed"

        else:

            state["status"] = "Completed with errors"

        await update_progress_message(
            progress_message
        )

    except Exception as e:

        logger.exception(
            "Scanner error: %s",
            e,
        )

        state["status"] = "Error"

        try:

            await progress_message.edit_text(
                progress_text()
                + f"\n\n❌ <b>Error:</b> {str(e)[:1000]}",
                parse_mode="HTML",
            )

        except Exception:
            pass

    finally:

        state["running"] = False

        scan_task = None


# ============================================================
# /start
# ============================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await admin_only(update):
        return

    text = (
        "🧹 <b>Duplicate Cleaner</b>\n\n"
        "Commands:\n\n"
        "/scan - Scan and delete duplicates\n"
        "/status - Show current status\n"
        "/stop - Stop current scan\n"
        "/help - Show help"
    )

    await update.message.reply_text(
        text,
        parse_mode="HTML",
    )


# ============================================================
# /help
# ============================================================

async def help_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await admin_only(update):
        return

    text = (
        "🧹 <b>Duplicate Cleaner Help</b>\n\n"
        "Use:\n"
        "/scan\n\n"
        "The cleaner will:\n"
        "🔍 Scan old messages\n"
        "♻️ Detect duplicate media\n"
        "🗑️ Delete duplicates in batches\n"
        "📊 Show live progress\n\n"
        "/status - Current status\n"
        "/stop - Stop operation"
    )

    await update.message.reply_text(
        text,
        parse_mode="HTML",
    )


# ============================================================
# /status
# ============================================================

async def status_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await admin_only(update):
        return

    await update.message.reply_text(
        progress_text(),
        parse_mode="HTML",
    )


# ============================================================
# /stop
# ============================================================

async def stop_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await admin_only(update):
        return

    if not state["running"]:

        await update.message.reply_text(
            "ℹ️ No cleanup is currently running."
        )

        return

    stop_event.set()

    await update.message.reply_text(
        "🛑 Stop requested.\n\n"
        "The current operation will stop safely."
    )


# ============================================================
# /scan
# ============================================================

async def scan_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    global scan_task

    if not await admin_only(update):
        return

    if state["running"]:

        await update.message.reply_text(
            "⚠️ A cleanup is already running.\n\n"
            "Use /status to see progress."
        )

        return

    progress_message = await update.message.reply_text(
        "🧹 <b>Duplicate Cleaner</b>\n\n"
        "🚀 Starting...",
        parse_mode="HTML",
    )

    stop_event.clear()

    scan_task = asyncio.create_task(
        perform_scan(
            progress_message
        )
    )

    await asyncio.sleep(0.1)


# ============================================================
# BOT STARTUP
# ============================================================

async def post_init(application):

    logger.info(
        "Starting Telethon client..."
    )

    await telethon_client.start()

    me = await telethon_client.get_me()

    logger.info(
        "Telethon logged in as: %s (%s)",
        me.first_name,
        me.id,
    )


# ============================================================
# BOT SHUTDOWN
# ============================================================

async def post_shutdown(application):

    try:

        await telethon_client.disconnect()

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

    application.add_handler(
        CommandHandler(
            "start",
            start_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "help",
            help_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "scan",
            scan_command,
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

    logger.info(
        "Duplicate Cleaner Bot started."
    )

    application.run_polling(
        allowed_updates=[
            "message",
        ]
    )


if __name__ == "__main__":
    main()
