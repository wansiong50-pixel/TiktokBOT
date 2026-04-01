import logging
import os
import asyncio
import time
from collections import defaultdict

import libsql_client
from aiohttp import web
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

# --- CONFIGURATION & ENVIRONMENT VARIABLES ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_TELEGRAM_TOKEN")
RENDER_APP_URL = os.getenv("RENDER_APP_URL", "https://your-app.onrender.com")
TURSO_URL = os.getenv("TURSO_URL", "libsql://your-db.turso.io")
TURSO_TOKEN = os.getenv("TURSO_TOKEN", "your-turso-token")
DONATION_LINK = os.getenv("DONATION_LINK", "https://ko-fi.com/yourname")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

WEBHOOK_PATH = "/webhook"

# --- PERFORMANCE TUNING CONSTANTS ---
RATE_LIMIT_MESSAGES = 5       # Max messages allowed in the time window
RATE_LIMIT_WINDOW = 3         # Seconds for rate limit window
IDLE_TIMEOUT = 300            # Auto-disconnect after 5 minutes of inactivity
IDLE_CHECK_INTERVAL = 60      # Check for idle users every 60 seconds

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# =============================================================================
# FIX #1: PERSISTENT DATABASE CLIENT (Connection Pooling)
# =============================================================================
# Instead of opening/closing a connection per query, we keep one client alive.
db_client = None


async def init_db():
    """Create the persistent DB client and ensure tables exist."""
    global db_client
    db_client = libsql_client.create_client(TURSO_URL, auth_token=TURSO_TOKEN)

    # Create / migrate tables
    await db_client.batch([
        # Main users table with all new columns
        """CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            status TEXT DEFAULT 'idle',
            partner_id INTEGER,
            block_count INTEGER DEFAULT 0,
            trust_tier TEXT DEFAULT 'normal',
            interest TEXT DEFAULT NULL,
            gender TEXT DEFAULT NULL,
            prefer_gender TEXT DEFAULT NULL,
            chats_completed INTEGER DEFAULT 0,
            positive_ratings INTEGER DEFAULT 0,
            negative_ratings INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        )""",
    ])

    # Safe ALTER TABLE migrations — these silently fail if columns already exist
    migrations = [
        "ALTER TABLE users ADD COLUMN interest TEXT DEFAULT NULL",
        "ALTER TABLE users ADD COLUMN gender TEXT DEFAULT NULL",
        "ALTER TABLE users ADD COLUMN prefer_gender TEXT DEFAULT NULL",
        "ALTER TABLE users ADD COLUMN chats_completed INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN positive_ratings INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN negative_ratings INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN created_at TEXT DEFAULT NULL",
    ]
    for migration in migrations:
        try:
            await db_client.execute(migration)
        except Exception:
            pass  # Column already exists, safe to ignore


async def execute_db(query: str, params: list = []):
    """Run a single query on the persistent client."""
    return await db_client.execute(query, params)


async def batch_db(statements: list):
    """Run multiple queries atomically."""
    return await db_client.batch(statements)


# =============================================================================
# FIX #2: IN-MEMORY CACHE FOR ACTIVE SESSIONS
# =============================================================================
# Read from memory (fast), write-through to DB (durable).
active_pairs = {}       # {user_id: partner_id}  — bidirectional
user_status_cache = {}  # {user_id: 'idle' | 'searching' | 'chatting'}

# FIX #3: RACE CONDITION LOCK
search_lock = asyncio.Lock()

# FIX #8: RATE LIMITING
message_timestamps = defaultdict(list)

# FIX #11: IDLE CHAT TRACKING
last_activity = {}  # {user_id: timestamp}


# =============================================================================
# CACHE HELPERS
# =============================================================================
def cache_pair(user_id: int, partner_id: int):
    """Cache a chat pair in memory."""
    active_pairs[user_id] = partner_id
    active_pairs[partner_id] = user_id
    user_status_cache[user_id] = 'chatting'
    user_status_cache[partner_id] = 'chatting'
    now = time.time()
    last_activity[user_id] = now
    last_activity[partner_id] = now


def cache_unpair(user_id: int):
    """Remove a chat pair from memory."""
    partner_id = active_pairs.pop(user_id, None)
    if partner_id is not None:
        active_pairs.pop(partner_id, None)
        user_status_cache[partner_id] = 'idle'
        last_activity.pop(partner_id, None)
    user_status_cache[user_id] = 'idle'
    last_activity.pop(user_id, None)
    return partner_id


async def load_cache_from_db():
    """On startup, reload active pairs from DB into memory."""
    result = await execute_db("SELECT user_id, status, partner_id FROM users WHERE status IN ('chatting', 'searching')")
    for row in result.rows:
        uid, status, pid = row[0], row[1], row[2]
        user_status_cache[uid] = status
        if status == 'chatting' and pid:
            active_pairs[uid] = pid
            last_activity[uid] = time.time()


# =============================================================================
# BOT COMMANDS
# =============================================================================

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id

    await execute_db(
        "INSERT OR IGNORE INTO users (user_id, status, partner_id, block_count, trust_tier, created_at) "
        "VALUES (?, 'idle', NULL, 0, 'normal', datetime('now'))",
        [user_id]
    )
    user_status_cache[user_id] = 'idle'

    welcome_text = (
        "🚀 <b>Welcome to the Anonymous Chat Bot!</b>\n\n"
        "Find a partner instantly and chat anonymously.\n\n"
        "📌 <b>Commands:</b>\n"
        "• /search — Find a random partner\n"
        "• /search <i>topic</i> — Find someone interested in the same topic\n"
        "• /next — Skip to a new partner\n"
        "• /end — End the current chat\n"
        "• /block — Report a spammer\n"
        "• /profile — View your stats\n"
        "• /setgender <i>male/female</i> — Set your gender\n"
        "• /prefer <i>male/female/any</i> — Set partner preference\n\n"
        "⚠️ <i>Rules:</i> No images allowed. Only tiktok.com links will go through.\n\n"
        f"☕ <i>Support the Bot:</i> {DONATION_LINK}"
    )
    await message.answer(welcome_text, parse_mode="HTML")


# =============================================================================
# FIX #14: GENDER / PREFERENCE
# =============================================================================

@dp.message(Command("setgender"))
async def cmd_setgender(message: types.Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or parts[1].strip().lower() not in ("male", "female"):
        await message.answer("⚠️ Usage: /setgender <b>male</b> or <b>female</b>", parse_mode="HTML")
        return

    gender = parts[1].strip().lower()
    await execute_db("UPDATE users SET gender = ? WHERE user_id = ?", [gender, message.from_user.id])
    emoji = "👨" if gender == "male" else "👩"
    await message.answer(f"{emoji} Gender set to <b>{gender}</b>.", parse_mode="HTML")


@dp.message(Command("prefer"))
async def cmd_prefer(message: types.Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or parts[1].strip().lower() not in ("male", "female", "any"):
        await message.answer("⚠️ Usage: /prefer <b>male</b>, <b>female</b>, or <b>any</b>", parse_mode="HTML")
        return

    pref = parts[1].strip().lower()
    value = None if pref == "any" else pref
    await execute_db("UPDATE users SET prefer_gender = ? WHERE user_id = ?", [value, message.from_user.id])
    await message.answer(f"✅ Partner preference set to <b>{pref}</b>.", parse_mode="HTML")


# =============================================================================
# FIX #7: INTEREST-BASED MATCHING + FIX #3: RACE CONDITION LOCK
# FIX #10: ONLINE COUNT & QUEUE POSITION
# =============================================================================

@dp.message(Command("search"))
async def cmd_search(message: types.Message):
    user_id = message.from_user.id

    # Quick check from cache
    if user_status_cache.get(user_id) == 'chatting':
        await message.answer("You are already in a chat! Type /next to skip them or /end to stop.")
        return

    # Parse optional interest
    parts = message.text.split(maxsplit=1)
    interest = parts[1].strip().lower() if len(parts) > 1 else None

    # Save interest to DB
    await execute_db("UPDATE users SET interest = ? WHERE user_id = ?", [interest, user_id])

    # Get user's own trust tier, gender, preference
    user_data = await execute_db(
        "SELECT trust_tier, gender, prefer_gender FROM users WHERE user_id = ?", [user_id]
    )
    if not user_data.rows:
        return

    trust_tier = user_data.rows[0][0]
    user_gender = user_data.rows[0][1]
    user_pref = user_data.rows[0][2]

    # FIX #3: Lock to prevent race conditions in matching
    async with search_lock:
        # Build the matching query dynamically
        # Same trust tier, searching status, not self
        conditions = ["status = 'searching'", "trust_tier = ?", "user_id != ?"]
        params = [trust_tier, user_id]

        # Interest matching: if user set an interest, prefer same interest
        if interest:
            conditions.append("interest = ?")
            params.append(interest)

        # Gender preference matching
        # If user prefers a gender, filter for partners of that gender
        if user_pref:
            conditions.append("gender = ?")
            params.append(user_pref)

        # If user has a gender, exclude partners who prefer a different gender
        if user_gender:
            conditions.append("(prefer_gender IS NULL OR prefer_gender = ?)")
            params.append(user_gender)

        where_clause = " AND ".join(conditions)
        partner_search = await execute_db(
            f"SELECT user_id FROM users WHERE {where_clause} LIMIT 1",
            params
        )

        # If no exact match found and interest was set, fall back to any interest
        if not partner_search.rows and interest:
            conditions_fallback = ["status = 'searching'", "trust_tier = ?", "user_id != ?"]
            params_fallback = [trust_tier, user_id]
            if user_pref:
                conditions_fallback.append("gender = ?")
                params_fallback.append(user_pref)
            if user_gender:
                conditions_fallback.append("(prefer_gender IS NULL OR prefer_gender = ?)")
                params_fallback.append(user_gender)
            where_fallback = " AND ".join(conditions_fallback)
            partner_search = await execute_db(
                f"SELECT user_id FROM users WHERE {where_fallback} LIMIT 1",
                params_fallback
            )

        if partner_search.rows:
            partner_id = partner_search.rows[0][0]

            # FIX #5: Batch DB operations — update both users atomically
            await batch_db([
                ("UPDATE users SET status = 'chatting', partner_id = ? WHERE user_id = ?", [partner_id, user_id]),
                ("UPDATE users SET status = 'chatting', partner_id = ? WHERE user_id = ?", [user_id, partner_id]),
            ])

            # FIX #2: Update in-memory cache
            cache_pair(user_id, partner_id)

            topic_msg = f" (Topic: <i>{interest}</i>)" if interest else ""
            await message.answer(
                f"🤝 <b>Partner found!</b>{topic_msg} Say hi and drop your TikTok link.",
                parse_mode="HTML"
            )
            await bot.send_message(
                partner_id,
                f"🤝 <b>Partner found!</b>{topic_msg} Say hi and drop your TikTok link.",
                parse_mode="HTML"
            )
        else:
            # No partner found — enter the queue
            await execute_db("UPDATE users SET status = 'searching' WHERE user_id = ?", [user_id])
            user_status_cache[user_id] = 'searching'

            # FIX #10: Show queue position and online count
            queue_data = await execute_db(
                "SELECT COUNT(*) FROM users WHERE status = 'searching' AND trust_tier = ?",
                [trust_tier]
            )
            online_data = await execute_db(
                "SELECT COUNT(*) FROM users WHERE status IN ('searching', 'chatting')"
            )
            queue_pos = queue_data.rows[0][0]
            online_count = online_data.rows[0][0]

            topic_note = f" for <i>{interest}</i>" if interest else ""
            await message.answer(
                f"🔍 Searching{topic_note}...\n"
                f"📊 You are <b>#{queue_pos}</b> in queue · <b>{online_count}</b> users online",
                parse_mode="HTML"
            )


# =============================================================================
# /end WITH RATING SYSTEM (FIX #9)
# =============================================================================

@dp.message(Command("end"))
async def cmd_end(message: types.Message):
    user_id = message.from_user.id

    partner_id = active_pairs.get(user_id)

    if partner_id:
        # Unpair in cache and DB
        cache_unpair(user_id)
        await batch_db([
            ("UPDATE users SET status = 'idle', partner_id = NULL, interest = NULL, chats_completed = chats_completed + 1 WHERE user_id = ?", [user_id]),
            ("UPDATE users SET status = 'idle', partner_id = NULL, interest = NULL, chats_completed = chats_completed + 1 WHERE user_id = ?", [partner_id]),
        ])

        # FIX #9: Send rating keyboard to both users
        rating_kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="👍 Good", callback_data=f"rate_good_{partner_id}"),
                InlineKeyboardButton(text="👎 Bad", callback_data=f"rate_bad_{partner_id}"),
            ]
        ])
        partner_rating_kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="👍 Good", callback_data=f"rate_good_{user_id}"),
                InlineKeyboardButton(text="👎 Bad", callback_data=f"rate_bad_{user_id}"),
            ]
        ])

        await message.answer(
            "Chat ended. How was your partner?",
            reply_markup=rating_kb
        )
        await bot.send_message(
            partner_id,
            "Your partner left the chat. How was your experience?",
            reply_markup=partner_rating_kb
        )
    else:
        # User was searching or idle
        await execute_db("UPDATE users SET status = 'idle', interest = NULL WHERE user_id = ?", [user_id])
        user_status_cache[user_id] = 'idle'
        await message.answer("You have left the queue.")


# FIX #9: Rating callback handler
@dp.callback_query(F.data.startswith("rate_"))
async def handle_rating(callback: CallbackQuery):
    data = callback.data  # e.g., "rate_good_123456789"
    parts = data.split("_", 2)
    rating_type = parts[1]   # 'good' or 'bad'
    target_id = int(parts[2])

    if rating_type == "good":
        await execute_db(
            "UPDATE users SET positive_ratings = positive_ratings + 1 WHERE user_id = ?",
            [target_id]
        )
    else:
        await execute_db(
            "UPDATE users SET negative_ratings = negative_ratings + 1 WHERE user_id = ?",
            [target_id]
        )

    emoji = "👍" if rating_type == "good" else "👎"
    await callback.message.edit_text(f"{emoji} Thanks for your feedback! Type /search to find a new partner.")
    await callback.answer()


# =============================================================================
# /next — FIX #4: No more sleep()
# =============================================================================

@dp.message(Command("next"))
async def cmd_next(message: types.Message):
    user_id = message.from_user.id
    partner_id = active_pairs.get(user_id)

    if partner_id:
        # Unpair silently
        cache_unpair(user_id)
        await batch_db([
            ("UPDATE users SET status = 'idle', partner_id = NULL, interest = NULL WHERE user_id = ?", [user_id]),
            ("UPDATE users SET status = 'idle', partner_id = NULL, interest = NULL WHERE user_id = ?", [partner_id]),
        ])
        await bot.send_message(partner_id, "Your partner skipped. Type /search to find a new one.")
    else:
        await execute_db("UPDATE users SET status = 'idle', interest = NULL WHERE user_id = ?", [user_id])
        user_status_cache[user_id] = 'idle'

    # Immediately search again — no sleep needed because cache is already updated
    await cmd_search(message)


# =============================================================================
# /block
# =============================================================================

@dp.message(Command("block"))
async def cmd_block(message: types.Message):
    user_id = message.from_user.id
    partner_id = active_pairs.get(user_id)

    if partner_id:
        cache_unpair(user_id)

        # Give partner a strike + disconnect
        await batch_db([
            ("UPDATE users SET block_count = block_count + 1 WHERE user_id = ?", [partner_id]),
            ("UPDATE users SET status = 'idle', partner_id = NULL WHERE user_id IN (?, ?)", [user_id, partner_id]),
        ])

        # Check if they crossed the troll threshold
        partner_stats = await execute_db("SELECT block_count FROM users WHERE user_id = ?", [partner_id])
        if partner_stats.rows and partner_stats.rows[0][0] >= 5:
            await execute_db("UPDATE users SET trust_tier = 'low_trust' WHERE user_id = ?", [partner_id])

        await message.answer("🚫 User blocked. They have been reported. Type /search to find a new partner.")
        await bot.send_message(partner_id, "Your partner disconnected. Type /search to find a new one.")
    else:
        await message.answer("You aren't in a chat right now.")


# =============================================================================
# FIX #12: /profile COMMAND
# =============================================================================

@dp.message(Command("profile"))
async def cmd_profile(message: types.Message):
    user_id = message.from_user.id
    data = await execute_db(
        "SELECT trust_tier, chats_completed, positive_ratings, negative_ratings, "
        "block_count, gender, prefer_gender, created_at FROM users WHERE user_id = ?",
        [user_id]
    )

    if not data.rows:
        await message.answer("You haven't started yet! Type /start first.")
        return

    row = data.rows[0]
    trust_tier = row[0]
    chats = row[1] or 0
    pos_ratings = row[2] or 0
    neg_ratings = row[3] or 0
    blocks = row[4] or 0
    gender = row[5] or "Not set"
    pref = row[6] or "Any"
    created_at = row[7] or "Unknown"

    # Trust tier emoji
    tier_display = {
        'normal': '⭐ Normal',
        'low_trust': '⚠️ Low Trust (Troll Pool)',
    }.get(trust_tier, trust_tier)

    # Rating score
    total_ratings = pos_ratings + neg_ratings
    if total_ratings > 0:
        score = round((pos_ratings / total_ratings) * 100)
        rating_display = f"{score}% positive ({pos_ratings}👍 / {neg_ratings}👎)"
    else:
        rating_display = "No ratings yet"

    await message.answer(
        f"📊 <b>Your Profile</b>\n\n"
        f"🛡️ Trust: {tier_display}\n"
        f"💬 Chats completed: {chats}\n"
        f"⭐ Rating: {rating_display}\n"
        f"🚫 Times reported: {blocks}\n"
        f"👤 Gender: {gender.capitalize()}\n"
        f"💕 Preference: {pref.capitalize()}\n"
        f"📅 Member since: {created_at}",
        parse_mode="HTML"
    )


# =============================================================================
# ADMIN COMMANDS
# =============================================================================

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return

    broadcast_text = message.text.replace("/broadcast", "").strip()
    if not broadcast_text:
        await message.answer("⚠️ Usage: /broadcast <your message here>")
        return

    await message.answer("🔄 Starting broadcast...")
    user_data = await execute_db("SELECT user_id FROM users")

    if not user_data.rows:
        await message.answer("No users in the database yet.")
        return

    success_count = 0
    fail_count = 0

    for row in user_data.rows:
        try:
            await bot.send_message(row[0], f"📢 <b>Admin Announcement:</b>\n\n{broadcast_text}", parse_mode="HTML")
            success_count += 1
            await asyncio.sleep(0.05)
        except Exception:
            fail_count += 1

    await message.answer(
        f"✅ <b>Broadcast Complete!</b>\n• Sent: {success_count}\n• Failed: {fail_count}",
        parse_mode="HTML"
    )


@dp.message(Command("restore"))
async def cmd_restore(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return

    try:
        target_id = int(message.text.split()[1])
    except (IndexError, ValueError):
        await message.answer("⚠️ Usage: /restore <user_id>")
        return

    user_data = await execute_db("SELECT trust_tier FROM users WHERE user_id = ?", [target_id])

    if not user_data.rows:
        await message.answer(f"❌ User {target_id} not found.")
        return

    await execute_db("UPDATE users SET trust_tier = 'normal', block_count = 0 WHERE user_id = ?", [target_id])
    await message.answer(f"✅ User {target_id} restored to 'normal'. Blocks reset to 0.")

    try:
        await bot.send_message(target_id, "🛡️ <b>Admin Update:</b> Your account has been restored to normal standing.", parse_mode="HTML")
    except Exception:
        pass


@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return

    total = await execute_db("SELECT COUNT(*) FROM users")
    trolls = await execute_db("SELECT COUNT(*) FROM users WHERE trust_tier = 'low_trust'")
    chatting = await execute_db("SELECT COUNT(*) FROM users WHERE status = 'chatting'")
    searching = await execute_db("SELECT COUNT(*) FROM users WHERE status = 'searching'")

    await message.answer(
        f"📊 <b>Bot Statistics:</b>\n\n"
        f"👥 Total Users: {total.rows[0][0]}\n"
        f"🗣️ Active Chats: {chatting.rows[0][0] // 2}\n"
        f"🔍 Searching: {searching.rows[0][0]}\n"
        f"🗑️ Troll Pool: {trolls.rows[0][0]}",
        parse_mode="HTML"
    )


# =============================================================================
# THE MEDIA BOUNCER
# =============================================================================

@dp.message(F.photo | F.video | F.document | F.sticker | F.animation | F.voice | F.audio)
async def block_media(message: types.Message):
    await message.answer(
        "🚫 <b>Images/Media are disabled for safety.</b>\n\n"
        "To verify a swap, just check your TikTok app to see if the price was cut. "
        "Only text and TikTok links are allowed here!",
        parse_mode="HTML"
    )


# =============================================================================
# THE TEXT RELAY — WITH IN-MEMORY CACHE + RATE LIMITING
# =============================================================================

@dp.message(F.text)
async def relay_text(message: types.Message):
    user_id = message.from_user.id
    text = message.text

    # FIX #2: Check cache first — no DB call
    if user_id not in active_pairs:
        await message.answer("You aren't connected to anyone. Type /search to find a partner!")
        return

    partner_id = active_pairs[user_id]

    # FIX #8: Rate limiting
    now = time.time()
    # Clean old timestamps outside the window
    message_timestamps[user_id] = [
        t for t in message_timestamps[user_id] if now - t < RATE_LIMIT_WINDOW
    ]
    if len(message_timestamps[user_id]) >= RATE_LIMIT_MESSAGES:
        await message.answer("⚠️ Slow down! You're sending messages too fast.")
        return
    message_timestamps[user_id].append(now)

    # FIX #11: Update activity timestamp
    last_activity[user_id] = now

    # Link Filter (keep TikTok-only filter)
    if "http" in text or "www." in text or ".com" in text:
        if "tiktok.com" not in text.lower():
            await message.answer("⚠️ Oops! You can only share TikTok links in this chat.")
            return

    try:
        await bot.send_message(chat_id=partner_id, text=f"💬 Partner:\n{text}")
    except Exception:
        # Partner unreachable — disconnect
        cache_unpair(user_id)
        await batch_db([
            ("UPDATE users SET status = 'idle', partner_id = NULL WHERE user_id = ?", [user_id]),
            ("UPDATE users SET status = 'idle', partner_id = NULL WHERE user_id = ?", [partner_id]),
        ])
        await message.answer("Your partner seems to have disconnected. Type /search to find a new one.")


# =============================================================================
# FIX #11: BACKGROUND TASK — AUTO-DISCONNECT IDLE CHATS
# =============================================================================

async def cleanup_idle_chats():
    """Background task: disconnect users who've been idle for too long."""
    while True:
        await asyncio.sleep(IDLE_CHECK_INTERVAL)
        now = time.time()
        idle_users = []

        for uid, last_time in list(last_activity.items()):
            if now - last_time > IDLE_TIMEOUT and uid in active_pairs:
                idle_users.append(uid)

        for uid in idle_users:
            # Only process if still paired (partner may have been cleaned already)
            if uid not in active_pairs:
                continue

            partner_id = cache_unpair(uid)
            try:
                await batch_db([
                    ("UPDATE users SET status = 'idle', partner_id = NULL WHERE user_id = ?", [uid]),
                    ("UPDATE users SET status = 'idle', partner_id = NULL WHERE user_id = ?", [partner_id]),
                ])
                await bot.send_message(uid, "⏰ Chat ended due to inactivity. Type /search to start again.")
                if partner_id:
                    await bot.send_message(partner_id, "⏰ Chat ended — your partner was inactive. Type /search to find a new one.")
            except Exception as e:
                logging.warning(f"Idle cleanup error for {uid}: {e}")


# =============================================================================
# RENDER WEBHOOK & SERVER SETUP
# =============================================================================

async def on_startup(**kwargs):
    # Initialize DB and cache
    await init_db()
    await load_cache_from_db()

    # Start background idle-cleanup task
    asyncio.create_task(cleanup_idle_chats())

    # Set webhook
    webhook_url = f"{RENDER_APP_URL}{WEBHOOK_PATH}"
    await bot.set_webhook(webhook_url)
    logging.info(f"Webhook set to {webhook_url}")


async def ping_handler(request: web.Request):
    return web.Response(text="Bot is awake and swapping!")


def main():
    dp.startup.register(on_startup)
    app = web.Application()

    webhook_requests_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_requests_handler.register(app, path=WEBHOOK_PATH)

    app.router.add_get("/ping", ping_handler)
    setup_application(app, dp, bot=bot)

    port = int(os.environ.get("PORT", 8080))
    logging.info(f"Starting web server on port {port}...")
    web.run_app(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()