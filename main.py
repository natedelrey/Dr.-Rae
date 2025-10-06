# main.py  (Medical Department bot)

import discord
from discord.ext import commands, tasks
import os
from dotenv import load_dotenv
import datetime
import aiohttp
import asyncpg
from aiohttp import web
from discord import app_commands
import asyncio
from urllib.parse import urlparse

# === Configuration ===
load_dotenv()

def getenv_int(name: str, default: int | None = None) -> int | None:
    val = os.getenv(name)
    try:
        return int(val) if val not in (None, "") else default
    except ValueError:
        return default

BOT_TOKEN = os.getenv("BOT_TOKEN")

# Channels / roles
ANNOUNCEMENT_CHANNEL_ID      = getenv_int("ANNOUNCEMENT_CHANNEL_ID")
LOG_CHANNEL_ID               = getenv_int("LOG_CHANNEL_ID")
ANNOUNCEMENT_ROLE_ID         = getenv_int("ANNOUNCEMENT_ROLE_ID")
MANAGEMENT_ROLE_ID           = getenv_int("MANAGEMENT_ROLE_ID")
AA_CHANNEL_ID                = getenv_int("AA_CHANNEL_ID")
ANOMALY_ACTORS_ROLE_ID       = getenv_int("ANOMALY_ACTORS_ROLE_ID")
DEPARTMENT_ROLE_ID           = getenv_int("DEPARTMENT_ROLE_ID")
MEDICAL_STUDENT_ROLE_ID      = getenv_int("MEDICAL_STUDENT_ROLE_ID")
ORIENTATION_ALERT_CHANNEL_ID = getenv_int("ORIENTATION_ALERT_CHANNEL_ID")
COMMAND_LOG_CHANNEL_ID       = getenv_int("COMMAND_LOG_CHANNEL_ID", 1416965696230789150)
ACTIVITY_LOG_CHANNEL_ID      = getenv_int("ACTIVITY_LOG_CHANNEL_ID", 1409646416829354095)

# DB / API
DATABASE_URL   = os.getenv("DATABASE_URL")
API_SECRET_KEY = os.getenv("API_SECRET_KEY")  # for /roblox webhook auth

def _normalize_base(url: str | None) -> str | None:
    if not url:
        return None
    u = url.strip()
    if u.startswith(("http://", "https://")):
        return u.rstrip("/")
    return ("https://" + u).rstrip("/")

ROBLOX_SERVICE_BASE = _normalize_base(os.getenv("ROBLOX_SERVICE_BASE") or None)
ROBLOX_REMOVE_URL   = os.getenv("ROBLOX_REMOVE_URL") or None
if ROBLOX_REMOVE_URL and not ROBLOX_REMOVE_URL.startswith("http"):
    ROBLOX_REMOVE_URL = "https://" + ROBLOX_REMOVE_URL
ROBLOX_REMOVE_SECRET = os.getenv("ROBLOX_REMOVE_SECRET") or None
ROBLOX_GROUP_ID      = os.getenv("ROBLOX_GROUP_ID") or None  # optional, forwarded if present

# Rank manager role (can run /rank)
RANK_MANAGER_ROLE_ID = getenv_int("RANK_MANAGER_ROLE_ID", 1405979816120942702)

# Weekly configs
WEEKLY_REQUIREMENT      = int(os.getenv("WEEKLY_REQUIREMENT", "3"))
WEEKLY_TIME_REQUIREMENT = int(os.getenv("WEEKLY_TIME_REQUIREMENT", "45"))  # minutes

# === Bot Setup ===
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True

TASK_TYPES = [
    "Interview",
    "Post-Op Interview",
    "Checkup",
    "Anomaly Checkup",
    "Medical Department Recruitment",
]

TASK_PLURALS = {
    "Checkup": "Checkups",
    "Interview": "Interviews",
    "Post-Op Interview": "Post-Op Interviews",
    "Anomaly Checkup": "Anomaly Checkups",
    "Medical Department Recruitment": "Medical Department Recruitments",
}

def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)

def human_remaining(delta: datetime.timedelta) -> str:
    if delta.total_seconds() <= 0:
        return "0d"
    days = delta.days
    hours = (delta.seconds // 3600)
    mins = (delta.seconds % 3600) // 60
    parts = []
    if days: parts.append(f"{days}d")
    if hours: parts.append(f"{hours}h")
    if mins and not days: parts.append(f"{mins}m")
    return " ".join(parts) if parts else "under 1m"

def week_key(dt: datetime.datetime | None = None) -> str:
    d = dt or utcnow()
    iso = d.isocalendar()  # (year, week, weekday)
    return f"{iso.year}-W{iso.week:02d}"

# === Helpers ===
def smart_chunk(text, size=4000):
    chunks = []
    while len(text) > size:
        split_index = text.rfind('\n', 0, size)
        if split_index == -1:
            split_index = text.rfind(' ', 0, size)
        if split_index == -1:
            split_index = size
        chunks.append(text[:split_index])
        text = text[split_index:].lstrip()
    chunks.append(text)
    return chunks

async def send_long_embed(target, title, description, color, footer_text, author_name=None, author_icon_url=None, image_url=None):
    chunks = smart_chunk(description)
    embed = discord.Embed(title=title, description=chunks[0], color=color, timestamp=utcnow())
    if footer_text: embed.set_footer(text=footer_text)
    if author_name: embed.set_author(name=author_name, icon_url=author_icon_url)
    if image_url: embed.set_image(url=image_url)
    await target.send(embed=embed)
    for i, chunk in enumerate(chunks[1:], start=2):
        follow_up = discord.Embed(description=chunk, color=color)
        follow_up.set_footer(text=f"Part {i}/{len(chunks)}")
        await target.send(embed=follow_up)

def channel_or_fallback():
    ch = bot.get_channel(ACTIVITY_LOG_CHANNEL_ID) if ACTIVITY_LOG_CHANNEL_ID else None
    if not ch:
        ch = bot.get_channel(COMMAND_LOG_CHANNEL_ID) if COMMAND_LOG_CHANNEL_ID else None
    return ch

async def send_activity_embed(title: str, desc: str, color: discord.Color):
    ch = channel_or_fallback()
    if not ch:
        return
    embed = discord.Embed(title=title, description=desc, color=color, timestamp=utcnow())
    await ch.send(embed=embed)

async def log_action(title: str, description: str):
    if not COMMAND_LOG_CHANNEL_ID:
        return
    ch = bot.get_channel(COMMAND_LOG_CHANNEL_ID)
    if not ch:
        return
    embed = discord.Embed(title=title, description=description, color=discord.Color.dark_gray(), timestamp=utcnow())
    await ch.send(embed=embed)

def find_member(discord_id: int) -> discord.Member | None:
    for g in bot.guilds:
        m = g.get_member(discord_id)
        if m:
            return m
    return None

# === Roblox service helpers ===
async def _retry(coro_factory, attempts=3, delay=0.8):
    last_exc = None
    for i in range(attempts):
        try:
            return await coro_factory()
        except Exception as e:
            last_exc = e
            if i < attempts - 1:
                await asyncio.sleep(delay)
    raise last_exc

async def try_remove_from_roblox(discord_id: int) -> bool:
    if not ROBLOX_REMOVE_URL or not ROBLOX_REMOVE_SECRET:
        return False
    try:
        async with bot.db_pool.acquire() as conn:
            roblox_id = await conn.fetchval("SELECT roblox_id FROM roblox_verification WHERE discord_id = $1", discord_id)
        if not roblox_id:
            print(f"try_remove_from_roblox: no roblox_id for {discord_id}")
            return False

        async def do_post():
            async with aiohttp.ClientSession() as session:
                headers = {"X-Secret-Key": ROBLOX_REMOVE_SECRET, "Content-Type": "application/json"}
                payload = {"robloxId": int(roblox_id)}
                if ROBLOX_GROUP_ID:
                    try:
                        payload["groupId"] = int(ROBLOX_GROUP_ID)
                    except:
                        pass
                async with session.post(ROBLOX_REMOVE_URL, headers=headers, json=payload, timeout=20) as resp:
                    if not (200 <= resp.status < 300):
                        text = await resp.text()
                        raise RuntimeError(f"Roblox removal failed {resp.status}: {text}")
                    return True
        return await _retry(do_post)
    except Exception as e:
        print(f"Roblox removal call failed: {e}")
        return False

async def fetch_group_ranks():
    if not ROBLOX_SERVICE_BASE or not ROBLOX_REMOVE_SECRET:
        return []
    url = ROBLOX_SERVICE_BASE.rstrip('/') + '/ranks'
    try:
        async def do_get():
            async with aiohttp.ClientSession() as session:
                headers = {"X-Secret-Key": ROBLOX_REMOVE_SECRET}
                async with session.get(url, headers=headers, timeout=20) as resp:
                    if not (200 <= resp.status < 300):
                        text = await resp.text()
                        raise RuntimeError(f"/ranks HTTP {resp.status}: {text}")
                    data = await resp.json()
                    return data.get('roles', [])
        return await _retry(do_get)
    except Exception as e:
        print(f"fetch_group_ranks error: {e}")
        return []

async def set_group_rank(roblox_id: int, role_id: int = None, rank_number: int = None) -> bool:
    if not ROBLOX_SERVICE_BASE or not ROBLOX_REMOVE_SECRET:
        return False
    url = ROBLOX_SERVICE_BASE.rstrip('/') + '/set-rank'
    body = {"robloxId": int(roblox_id)}
    if role_id is not None:
        body["roleId"] = int(role_id)
    if rank_number is not None:
        body["rankNumber"] = int(rank_number)
    if ROBLOX_GROUP_ID:
        try:
            body["groupId"] = int(ROBLOX_GROUP_ID)
        except:
            pass
    try:
        async def do_post():
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=body, headers={"X-Secret-Key": ROBLOX_REMOVE_SECRET, "Content-Type": "application/json"}, timeout=20) as resp:
                    if not (200 <= resp.status < 300):
                        text = await resp.text()
                        raise RuntimeError(f"/set-rank HTTP {resp.status}: {text}")
                    return True
        return await _retry(do_post)
    except Exception as e:
        print(f"set_group_rank error: {e}")
        return False

# === Bot class ===
class MD_BOT(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix='!', intents=intents)
        self.db_pool = None

    async def setup_hook(self):
        # DB pool
        try:
            self.db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
            async with self.db_pool.acquire() as c:
                await c.execute('SELECT 1')
            print("[DB] Connected.")
        except Exception as e:
            print(f"[DB] FAILED: {e}")
            return

        # Schema (create/ensure)
        async with self.db_pool.acquire() as connection:
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS weekly_tasks (
                    member_id BIGINT PRIMARY KEY,
                    tasks_completed INT DEFAULT 0
                );
            ''')
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS task_logs (
                    log_id SERIAL PRIMARY KEY,
                    member_id BIGINT,
                    task TEXT,
                    task_type TEXT,
                    proof_url TEXT,
                    comments TEXT,
                    timestamp TIMESTAMPTZ
                );
            ''')
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS weekly_task_logs (
                    log_id SERIAL PRIMARY KEY,
                    member_id BIGINT,
                    task TEXT,
                    task_type TEXT,
                    proof_url TEXT,
                    comments TEXT,
                    timestamp TIMESTAMPTZ
                );
            ''')
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS roblox_verification (
                    discord_id BIGINT PRIMARY KEY,
                    roblox_id BIGINT UNIQUE
                );
            ''')
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS roblox_time (
                    member_id BIGINT PRIMARY KEY,
                    time_spent INT DEFAULT 0
                );
            ''')
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS roblox_sessions (
                    roblox_id BIGINT PRIMARY KEY,
                    start_time TIMESTAMPTZ
                );
            ''')
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS orientations (
                    discord_id BIGINT PRIMARY KEY,
                    assigned_at TIMESTAMPTZ,
                    deadline TIMESTAMPTZ,
                    passed BOOLEAN DEFAULT FALSE,
                    passed_at TIMESTAMPTZ,
                    warned_5d BOOLEAN DEFAULT FALSE,
                    expired_handled BOOLEAN DEFAULT FALSE
                );
            ''')
            # Strike system (create BEFORE altering)
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS strikes (
                    strike_id SERIAL PRIMARY KEY,
                    member_id BIGINT NOT NULL,
                    reason TEXT,
                    issued_at TIMESTAMPTZ NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL,
                    set_by BIGINT,
                    auto BOOLEAN DEFAULT FALSE
                );
            ''')

            # --- Safe ALTERs for older DBs (fixes UndefinedColumnError) ---
            await connection.execute("ALTER TABLE weekly_task_logs ADD COLUMN IF NOT EXISTS task TEXT;")
            await connection.execute("ALTER TABLE task_logs ADD COLUMN IF NOT EXISTS task TEXT;")
            await connection.execute("UPDATE weekly_task_logs SET task = COALESCE(task, task_type) WHERE task IS NULL;")
            await connection.execute("UPDATE task_logs SET task = COALESCE(task, task_type) WHERE task IS NULL;")

            # Ensure safety columns exist
            await connection.execute("ALTER TABLE orientations ADD COLUMN IF NOT EXISTS passed_at TIMESTAMPTZ;")
            await connection.execute("ALTER TABLE orientations ADD COLUMN IF NOT EXISTS warned_5d BOOLEAN DEFAULT FALSE;")
            await connection.execute("ALTER TABLE orientations ADD COLUMN IF NOT EXISTS expired_handled BOOLEAN DEFAULT FALSE;")
            await connection.execute("ALTER TABLE strikes ADD COLUMN IF NOT EXISTS set_by BIGINT;")
            await connection.execute("ALTER TABLE strikes ADD COLUMN IF NOT EXISTS auto BOOLEAN DEFAULT FALSE;")

            await connection.execute('''
                CREATE TABLE IF NOT EXISTS member_ranks (
                    discord_id BIGINT PRIMARY KEY,
                    rank TEXT,
                    set_by BIGINT,
                    set_at TIMESTAMPTZ
                );
            ''')
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS activity_excuses (
                    week_key TEXT PRIMARY KEY,
                    reason TEXT,
                    set_by BIGINT,
                    set_at TIMESTAMPTZ
                );
            ''')

        print("[DB] Tables ready.")

        # Sync slash commands
        try:
            synced = await self.tree.sync()
            print(f"[Slash] Synced {len(synced)} command(s)")
        except Exception as e:
            print(f"[Slash] Sync failed: {e}")

        # Web server for Roblox integration
        app = web.Application()
        app.router.add_get('/health', lambda _: web.Response(text='ok', status=200))
        app.router.add_post('/roblox', self.roblox_handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()
        print("[Web] Server up on :8080 (GET /health, POST /roblox).")

    # --- Roblox webhook with activity embeds ---
    async def roblox_handler(self, request):
        print("[/roblox] hit")
        if request.headers.get("X-Secret-Key") != API_SECRET_KEY:
            print("[/roblox] 401 bad secret")
            return web.Response(status=401)
        data = await request.json()
        roblox_id = data.get("robloxId")
        status = data.get("status")
        print(f"[/roblox] body: {data}")

        async with self.db_pool.acquire() as connection:
            discord_id = await connection.fetchval(
                "SELECT discord_id FROM roblox_verification WHERE roblox_id = $1", roblox_id
            )

        if discord_id:
            if status == "joined":
                async with self.db_pool.acquire() as connection:
                    await connection.execute(
                        "INSERT INTO roblox_sessions (roblox_id, start_time) VALUES ($1, $2) "
                        "ON CONFLICT (roblox_id) DO UPDATE SET start_time = $2",
                        roblox_id, utcnow()
                    )
                member = find_member(int(discord_id))
                name = member.display_name if member else f"User {discord_id}"
                await send_activity_embed(
                    "🟢 Joined Site",
                    f"**{name}** started a session.",
                    discord.Color.green()
                )

            elif status == "left":
                session_start = None
                async with self.db_pool.acquire() as connection:
                    session_start = await connection.fetchval(
                        "SELECT start_time FROM roblox_sessions WHERE roblox_id = $1", roblox_id
                    )
                    if session_start:
                        await connection.execute("DELETE FROM roblox_sessions WHERE roblox_id = $1", roblox_id)
                        duration = (utcnow() - session_start).total_seconds()
                        await connection.execute(
                            "INSERT INTO roblox_time (member_id, time_spent) VALUES ($1, $2) "
                            "ON CONFLICT (member_id) DO UPDATE SET time_spent = roblox_time.time_spent + $2",
                            discord_id, int(duration)
                        )

                mins = int((utcnow() - session_start).total_seconds() // 60) if session_start else 0
                async with self.db_pool.acquire() as connection:
                    total_seconds = await connection.fetchval(
                        "SELECT time_spent FROM roblox_time WHERE member_id=$1", discord_id
                    ) or 0
                weekly_minutes = total_seconds // 60
                member = find_member(int(discord_id))
                name = member.display_name if member else f"User {discord_id}"
                await send_activity_embed(
                    "🔴 Left Site",
                    f"**{name}** ended their session. Time this session: **{mins} min**.\nThis week: **{weekly_minutes}/{WEEKLY_TIME_REQUIREMENT} min**",
                    discord.Color.red()
                )

        return web.Response(status=200)

bot = MD_BOT()

# === Events ===
@bot.event
async def on_ready():
    print(f'[READY] Logged in as {bot.user.name}')
    print("Activity channel:", bot.get_channel(ACTIVITY_LOG_CHANNEL_ID))
    print("Command log channel:", bot.get_channel(COMMAND_LOG_CHANNEL_ID))
    check_weekly_tasks.start()
    orientation_reminder_loop.start()

@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    before_roles = {r.id for r in before.roles}
    after_roles  = {r.id for r in after.roles}
    if MEDICAL_STUDENT_ROLE_ID and (MEDICAL_STUDENT_ROLE_ID not in before_roles) and (MEDICAL_STUDENT_ROLE_ID in after_roles):
        assigned = utcnow()
        deadline = assigned + datetime.timedelta(days=14)
        async with bot.db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO orientations (discord_id, assigned_at, deadline, passed, warned_5d, expired_handled) "
                "VALUES ($1, $2, $3, FALSE, FALSE, FALSE) "
                "ON CONFLICT (discord_id) DO NOTHING",
                after.id, assigned, deadline
            )
        await log_action("Orientation Assigned", f"Member: {after.mention} • Deadline: {deadline.strftime('%Y-%m-%d %H:%M UTC')}")

# Global slash error
@bot.tree.error
async def global_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    try:
        await log_action("Slash Command Error", f"Command: **/{getattr(interaction.command, 'name', 'unknown')}**\nError: `{error}`")
    finally:
        if not interaction.response.is_done():
            try:
                await interaction.response.send_message("Sorry, something went wrong running that command.", ephemeral=True)
            except:
                pass
# === Slash Commands ===

# /verify
@bot.tree.command(name="verify", description="Link your Roblox account to the bot.")
async def verify(interaction: discord.Interaction, roblox_username: str):
    payload = {"usernames": [roblox_username], "excludeBannedUsers": True}
    async with aiohttp.ClientSession() as session:
        async with session.post("https://users.roblox.com/v1/usernames/users", json=payload) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data["data"]:
                    user_data = data["data"][0]
                    roblox_id = user_data["id"]
                    roblox_name = user_data["name"]
                    async with bot.db_pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO roblox_verification (discord_id, roblox_id) VALUES ($1, $2) "
                            "ON CONFLICT (discord_id) DO UPDATE SET roblox_id = EXCLUDED.roblox_id",
                            interaction.user.id, roblox_id
                        )
                    await log_action("Verification Linked", f"User: {interaction.user.mention}\nRoblox: **{roblox_name}** (`{roblox_id}`)")
                    await interaction.response.send_message(f"Successfully verified as {roblox_name}!", ephemeral=True)
                else:
                    await interaction.response.send_message("Could not find that Roblox user.", ephemeral=True)
            else:
                await interaction.response.send_message("There was an error looking up the Roblox user.", ephemeral=True)

# /announce (modal)
class AnnouncementForm(discord.ui.Modal, title='Send Announcement'):
    def __init__(self, color_obj: discord.Color):
        super().__init__()
        self.color_obj = color_obj

    ann_title   = discord.ui.TextInput(label='Title',   placeholder='Announcement title', style=discord.TextStyle.short, required=True, max_length=200)
    ann_message = discord.ui.TextInput(label='Message', placeholder='Write your announcement here…', style=discord.TextStyle.paragraph, required=True, max_length=4000)

    async def on_submit(self, interaction: discord.Interaction):
        announcement_channel = bot.get_channel(ANNOUNCEMENT_CHANNEL_ID)
        if not announcement_channel:
            await interaction.response.send_message("Announcement channel not found.", ephemeral=True)
            return
        await send_long_embed(
            target=announcement_channel,
            title=f"📢 {self.ann_title.value}",
            description=self.ann_message.value,
            color=self.color_obj,
            footer_text=f"Announcement by {interaction.user.display_name}"
        )
        await log_action("Announcement Sent", f"User: {interaction.user.mention}\nTitle: **{self.ann_title.value}**")
        await interaction.response.send_message("Announcement sent successfully!", ephemeral=True)

@bot.tree.command(name="announce", description="Open a form to send an announcement.")
@app_commands.checks.has_role(ANNOUNCEMENT_ROLE_ID)
@app_commands.choices(color=[
    app_commands.Choice(name="Blue", value="blue"),
    app_commands.Choice(name="Green", value="green"),
    app_commands.Choice(name="Red", value="red"),
    app_commands.Choice(name="Yellow", value="yellow"),
    app_commands.Choice(name="Purple", value="purple"),
    app_commands.Choice(name="Orange", value="orange"),
    app_commands.Choice(name="Gold", value="gold"),
])
async def announce(interaction: discord.Interaction, color: str = "blue"):
    color_obj = getattr(discord.Color, color, discord.Color.blue)()
    await interaction.response.send_modal(AnnouncementForm(color_obj=color_obj))

# /log (modal)
class LogTaskForm(discord.ui.Modal, title='Add Comments (optional)'):
    def __init__(self, proof: discord.Attachment, task_type: str):
        super().__init__()
        self.proof = proof
        self.task_type = task_type

    comments = discord.ui.TextInput(label='Comments', placeholder='Any additional comments?', style=discord.TextStyle.paragraph, required=False, max_length=1000)

    async def on_submit(self, interaction: discord.Interaction):
        log_channel = bot.get_channel(LOG_CHANNEL_ID)
        if not log_channel:
            await interaction.response.send_message("Log channel not found.", ephemeral=True)
            return

        member_id = interaction.user.id
        comments_str = self.comments.value or "No comments"

        async with bot.db_pool.acquire() as conn:
            # permanent + weekly + counter
            await conn.execute(
                "INSERT INTO task_logs (member_id, task, task_type, proof_url, comments, timestamp) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                member_id, self.task_type, self.task_type, self.proof.url, comments_str, utcnow()
            )
            await conn.execute(
                "INSERT INTO weekly_task_logs (member_id, task, task_type, proof_url, comments, timestamp) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                member_id, self.task_type, self.task_type, self.proof.url, comments_str, utcnow()
            )
            await conn.execute(
                "INSERT INTO weekly_tasks (member_id, tasks_completed) VALUES ($1, 1) "
                "ON CONFLICT (member_id) DO UPDATE SET tasks_completed = weekly_tasks.tasks_completed + 1",
                member_id
            )
            tasks_completed = await conn.fetchval("SELECT tasks_completed FROM weekly_tasks WHERE member_id = $1", member_id)

        full_description = f"**Task Type:** {self.task_type}\n\n**Comments:**\n{comments_str}"
        await send_long_embed(
            target=log_channel,
            title="✅ Task Logged",
            description=full_description,
            color=discord.Color.green(),
            footer_text=f"Member ID: {member_id}",
            author_name=interaction.user.display_name,
            author_icon_url=interaction.user.avatar.url if interaction.user.avatar else None,
            image_url=self.proof.url
        )
        await log_action("Task Logged", f"User: {interaction.user.mention}\nType: **{self.task_type}**")
        await interaction.response.send_message(
            f"Your task has been logged! You have completed {tasks_completed} task(s) this week.",
            ephemeral=True
        )

@bot.tree.command(name="log", description="Log a completed task with proof and type.")
@app_commands.choices(task_type=[app_commands.Choice(name=t, value=t) for t in TASK_TYPES])
async def log(interaction: discord.Interaction, task_type: str, proof: discord.Attachment):
    await interaction.response.send_modal(LogTaskForm(proof=proof, task_type=task_type))

# /mytasks
@bot.tree.command(name="mytasks", description="Check your weekly tasks and time.")
async def mytasks(interaction: discord.Interaction):
    member_id = interaction.user.id
    async with bot.db_pool.acquire() as conn:
        tasks_completed    = await conn.fetchval("SELECT tasks_completed FROM weekly_tasks WHERE member_id = $1", member_id) or 0
        time_spent_seconds = await conn.fetchval("SELECT time_spent FROM roblox_time WHERE member_id = $1", member_id) or 0
        active_strikes     = await conn.fetchval("SELECT COUNT(*) FROM strikes WHERE member_id=$1 AND expires_at > $2", member_id, utcnow())
    time_spent_minutes = time_spent_seconds // 60
    await interaction.response.send_message(
        f"You have **{tasks_completed}/{WEEKLY_REQUIREMENT}** tasks and **{time_spent_minutes}/{WEEKLY_TIME_REQUIREMENT}** mins. "
        f"Active strikes: **{active_strikes}/3**.",
        ephemeral=True
    )

# /viewtasks
@bot.tree.command(name="viewtasks", description="Show a member's task totals by type (all-time).")
async def viewtasks(interaction: discord.Interaction, member: discord.Member | None = None):
    target = member or interaction.user
    async with bot.db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT COALESCE(NULLIF(task_type, ''), task) AS ttype, COUNT(*) AS cnt "
            "FROM task_logs WHERE member_id = $1 GROUP BY ttype ORDER BY cnt DESC, ttype ASC",
            target.id,
        )
        total = await conn.fetchval("SELECT COUNT(*) FROM task_logs WHERE member_id = $1", target.id)
    if not rows:
        await interaction.response.send_message(f"No tasks found for {target.display_name}.", ephemeral=True)
        return
    lines = []
    for r in rows:
        base = r['ttype'] or "Uncategorized"
        label = TASK_PLURALS.get(base, base + ("s" if not base.endswith("s") else ""))
        lines.append(f"**{label}** — {r['cnt']}")
    embed = discord.Embed(
        title=f"🗂️ Task Totals for {target.display_name}",
        description="\n".join(lines),
        color=discord.Color.blurple(),
        timestamp=utcnow()
    )
    embed.set_footer(text=f"Total tasks: {total}")
    await log_action("Viewed Tasks", f"Requester: {interaction.user.mention}\nTarget: {target.mention if target != interaction.user else 'self'}")
    await interaction.response.send_message(embed=embed, ephemeral=True)

# /addtask (mgmt)
@bot.tree.command(name="addtask", description="(Mgmt) Add tasks to a member's history and weekly totals.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
@app_commands.choices(task_type=[app_commands.Choice(name=t, value=t) for t in TASK_TYPES])
async def addtask(
    interaction: discord.Interaction,
    member: discord.Member,
    task_type: str,
    count: app_commands.Range[int, 1, 100] = 1,
    comments: str | None = None,
    proof: discord.Attachment | None = None,
):
    now = utcnow()
    proof_url    = proof.url if proof else None
    comments_val = comments or "Added by management"

    async with bot.db_pool.acquire() as conn:
        async with conn.transaction():
            batch_rows = [(member.id, task_type, task_type, proof_url, comments_val, now)] * count
            await conn.executemany(
                "INSERT INTO task_logs (member_id, task, task_type, proof_url, comments, timestamp) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                batch_rows
            )
            await conn.executemany(
                "INSERT INTO weekly_task_logs (member_id, task, task_type, proof_url, comments, timestamp) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                batch_rows
            )
            await conn.execute(
                "INSERT INTO weekly_tasks (member_id, tasks_completed) VALUES ($1, $2) "
                "ON CONFLICT (member_id) DO UPDATE SET tasks_completed = weekly_tasks.tasks_completed + $2",
                member.id, count
            )

        rows = await conn.fetch(
            "SELECT COALESCE(NULLIF(task_type, ''), task) AS ttype, COUNT(*) AS cnt "
            "FROM task_logs WHERE member_id = $1 GROUP BY ttype ORDER BY cnt DESC, ttype ASC",
            member.id,
        )

    lines = []
    for r in rows:
        base = r['ttype'] or "Uncategorized"
        label = TASK_PLURALS.get(base, base + ("s" if not base.endswith("s") else ""))
        lines.append(f"{label} — {r['cnt']}")

    desc = f"Added **{count}× {task_type}** to {member.mention}.\n\n**Now totals:**\n" + "\n".join(lines)
    embed = discord.Embed(title="✅ Tasks Added", description=desc, color=discord.Color.green(), timestamp=utcnow())
    if proof_url:
        embed.set_image(url=proof_url)

    await log_action("Tasks Added", f"By: {interaction.user.mention}\nMember: {member.mention}\nType: **{task_type}** × {count}")
    await interaction.response.send_message(embed=embed, ephemeral=True)

# /leaderboard
@bot.tree.command(name="leaderboard", description="Displays the weekly leaderboard (tasks + on-site minutes).")
async def leaderboard(interaction: discord.Interaction):
    async with bot.db_pool.acquire() as conn:
        task_rows = await conn.fetch("SELECT member_id, tasks_completed FROM weekly_tasks")
        time_rows = await conn.fetch("SELECT member_id, time_spent FROM roblox_time")

    task_map = {r['member_id']: r['tasks_completed'] for r in task_rows}
    time_map = {r['member_id']: r['time_spent'] for r in time_rows}

    member_ids = set(task_map.keys()) | set(time_map.keys())
    if not member_ids:
        await interaction.response.send_message("No activity logged this week.", ephemeral=True)
        return

    records = []
    for mid in member_ids:
        member = interaction.guild.get_member(mid)
        name = member.display_name if member else f"Unknown ({mid})"
        tasks_done = task_map.get(mid, 0)
        minutes_done = (time_map.get(mid, 0) // 60)
        records.append((name, tasks_done, minutes_done, mid))

    records.sort(key=lambda x: (-x[1], -x[2], x[0].lower()))

    embed = discord.Embed(title="🏆 Weekly Leaderboard", color=discord.Color.gold(), timestamp=utcnow())
    lines = []
    rank_emoji = ["🥇", "🥈", "🥉"]
    for i, (name, tasks_done, minutes_done, _) in enumerate(records[:10]):
        prefix = rank_emoji[i] if i < 3 else f"**{i+1}.**"
        lines.append(f"{prefix} **{name}** — {tasks_done} tasks, {minutes_done} mins")
    embed.description = "\n".join(lines)
    await log_action("Viewed Leaderboard", f"Requester: {interaction.user.mention}")
    await interaction.response.send_message(embed=embed)

# /removelastlog (mgmt)
@bot.tree.command(name="removelastlog", description="Removes the last logged task for a member.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def removelastlog(interaction: discord.Interaction, member: discord.Member):
    member_id = member.id
    async with bot.db_pool.acquire() as conn:
        async with conn.transaction():
            last_log = await conn.fetchrow(
                "SELECT log_id, task FROM weekly_task_logs WHERE member_id = $1 ORDER BY timestamp DESC LIMIT 1",
                member_id
            )
            if not last_log:
                await interaction.response.send_message(f"{member.display_name} has no weekly tasks logged.", ephemeral=True)
                return
            await conn.execute("DELETE FROM weekly_task_logs WHERE log_id = $1", last_log['log_id'])
            await conn.execute(
                "UPDATE weekly_tasks SET tasks_completed = GREATEST(tasks_completed - 1, 0) WHERE member_id = $1",
                member_id
            )
            new_count = await conn.fetchval("SELECT tasks_completed FROM weekly_tasks WHERE member_id = $1", member_id)
    await log_action("Removed Last Weekly Task", f"By: {interaction.user.mention}\nMember: {member.mention}\nRemoved: **{last_log['task']}**")
    await interaction.response.send_message(
        f"Removed last weekly task for {member.mention}: '{last_log['task']}'. They now have {new_count} tasks.",
        ephemeral=True
    )
# /welcome
@bot.tree.command(name="welcome", description="Sends the official welcome message.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def welcome(interaction: discord.Interaction):
    msg = (
        "Hello, congratulations on your acceptance to the **Medical Department**!\n\n"
        ":one: Before you jump into anything, be sure that you familiarize yourself with our central "
        "[MD Trello](https://trello.com/b/j2jvme4Z/md-information-hub) and also it's subsidiary divisions like the "
        "[Pathology Hub](https://trello.com/b/QPD3QshW/md-pathology-hub) and the "
        "[Psychology Hub](https://trello.com/b/B6eHAvEN/md-psychology-hub).\n"
        ">  :information_source:  Even if you aren't apart of these specializations yet, it's good to know what they do "
        "because each focus on a critical component in your MD gameplay.\n\n"
        ":two: After you've reviewed our guidelines, focus on getting your **Medical Student Orientation** completed. "
        ":calendar_spiral: These are 20 minute sessions that **have to be completed** within your first 2 weeks of entry "
        "and can be booked with any member of management.\n\n"
        ":three: If you are interested in receiving **commission** for your medical duty :money_with_wings:, we offer a "
        "[Medical Outreach Program](https://www.roblox.com/communities/451852407/SCPF-Outreach-Program#!/about) that conducts payouts.\n"
        "> :information_source: If are applying to MD to receive the recent **sign-on bonus** advertisement, this is a critical "
        "step to ensure you receive your payout.\n\n"
        ":four: Familiarize yourself with myself—Dr. Rae! I will serve as your medical AI assistant throughout our journey, "
        "and you'll have to learn a few of my important commands if you want to succeed. :checkered_flag: The first step we'll "
        "take together is my **/verify** command with your ROBLOX username—this is to ensure your on-site activity is *always* "
        "accurately tracked.\n\n"
        "That's all for now, if you have any questions at all just message any management member or even your peers! "
        "We're happy to have you here :sparkling_heart:"
    )

    embed = discord.Embed(title="Welcome to the Team!", description=msg, color=discord.Color.green())
    embed.set_footer(text="Best,\nThe Medical Department Management Team")

    await interaction.channel.send(embed=embed)
    await log_action("Welcome Sent", f"By: {interaction.user.mention} • Channel: {interaction.channel.mention}")
    await interaction.response.send_message("Welcome message sent!", ephemeral=True)

# /dm (mgmt)
@bot.tree.command(name="dm", description="Sends a direct message to a member.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def dm(interaction: discord.Interaction, member: discord.Member, title: str, message: str):
    if member.bot:
        await interaction.response.send_message("You can't send messages to bots!", ephemeral=True)
        return
    description = message.replace('\\n', '\n')
    try:
        await send_long_embed(
            target=member,
            title=f"💌 {title}",
            description=description,
            color=discord.Color.magenta(),
            footer_text=f"A special message from {interaction.guild.name}"
        )
        await log_action("DM Sent", f"From: {interaction.user.mention}\nTo: {member.mention}\nTitle: **{title}**")
        await interaction.response.send_message(f"Your message has been sent to {member.mention}!", ephemeral=True)
    except discord.Forbidden:
        await interaction.response.send_message(f"I couldn't message {member.mention}. They might have DMs disabled.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message("An unexpected error occurred.", ephemeral=True)
        print(f"DM command error: {e}")

# /aa (mgmt, cooldown)
@bot.tree.command(name="aa", description="Ping Anomaly Actors to get on-site for a checkup.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
@app_commands.checks.cooldown(1, 300.0, key=lambda i: i.user.id)
async def aa(interaction: discord.Interaction, note: str | None = None):
    target_channel = bot.get_channel(AA_CHANNEL_ID)
    if not target_channel:
        await interaction.response.send_message("Could not find the AA announcement channel.", ephemeral=True)
        return

    role = interaction.guild.get_role(ANOMALY_ACTORS_ROLE_ID)
    if not role:
        await interaction.response.send_message("Could not find the Anomaly Actors role.", ephemeral=True)
        return

    title = "🧪 Anomaly Actors Checkup Call"
    body = [
        f"{role.mention}, please get on-site for a quick **Anomaly Actors checkup**.",
        "Check the radio for further instructions."
    ]
    if note:
        body.append(f"\n**Note:** {note}")

    embed = discord.Embed(title=title, description="\n".join(body), color=discord.Color.purple(), timestamp=utcnow())
    embed.set_footer(text=f"Requested by {interaction.user.display_name}")

    await target_channel.send(content=f"{role.mention}", embed=embed, allowed_mentions=discord.AllowedMentions(roles=True))
    await log_action("AA Ping Sent", f"By: {interaction.user.mention}\nChannel: {target_channel.mention}")
    await interaction.response.send_message("Anomaly Actors have been pinged for a checkup.", ephemeral=True)

# --- Orientation helpers/commands ---

async def ensure_orientation_record(member: discord.Member):
    """Ensure orientation window exists for members with MEDICAL_STUDENT_ROLE_ID."""
    async with bot.db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT discord_id FROM orientations WHERE discord_id = $1", member.id)
        if row:
            return
        if MEDICAL_STUDENT_ROLE_ID and any(r.id == MEDICAL_STUDENT_ROLE_ID for r in member.roles):
            assigned = utcnow()
            deadline = assigned + datetime.timedelta(days=14)
            await conn.execute(
                "INSERT INTO orientations (discord_id, assigned_at, deadline, passed, warned_5d, expired_handled) "
                "VALUES ($1, $2, $3, FALSE, FALSE, FALSE)",
                member.id, assigned, deadline
            )

@bot.tree.command(name="passedorientation", description="(Mgmt) Mark a member as having passed orientation.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def passedorientation(interaction: discord.Interaction, member: discord.Member):
    assigned = utcnow()
    deadline = assigned + datetime.timedelta(days=14)
    passed_at = assigned
    async with bot.db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO orientations (discord_id, assigned_at, deadline, passed, passed_at, warned_5d, expired_handled)
            VALUES ($1, $2, $3, TRUE, $4, TRUE, TRUE)
            ON CONFLICT (discord_id)
            DO UPDATE SET
                passed = TRUE,
                passed_at = EXCLUDED.passed_at,
                warned_5d = TRUE,
                expired_handled = TRUE
            """,
            member.id, assigned, deadline, passed_at
        )
    await log_action("Orientation Passed", f"Member: {member.mention}\nBy: {interaction.user.mention}")
    await interaction.response.send_message(f"Marked {member.mention} as **passed orientation**.", ephemeral=True)

@bot.tree.command(name="orientationview", description="View a member's orientation status.")
async def orientationview(interaction: discord.Interaction, member: discord.Member | None = None):
    target = member or interaction.user
    await ensure_orientation_record(target)
    async with bot.db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT assigned_at, deadline, passed, passed_at FROM orientations WHERE discord_id = $1",
            target.id
        )
    if not row:
        await interaction.response.send_message(f"No orientation record for {target.display_name}.", ephemeral=True)
        return
    if row["passed"]:
        when = row["passed_at"].strftime("%Y-%m-%d %H:%M UTC") if row["passed_at"] else "unknown time"
        msg = f"**{target.display_name}**: ✅ Passed orientation (at {when})."
    else:
        remaining = row["deadline"] - utcnow()
        pretty = human_remaining(remaining)
        msg = (
            f"**{target.display_name}**: ❌ Not passed.\n"
            f"Deadline: **{row['deadline'].strftime('%Y-%m-%d %H:%M UTC')}** "
            f"(**{pretty}** remaining)"
        )
    await log_action("Orientation Viewed", f"Requester: {interaction.user.mention}\nTarget: {target.mention if target != interaction.user else 'self'}")
    await interaction.response.send_message(msg, ephemeral=True)

@bot.tree.command(name="extendorientation", description="(Mgmt) Extend a member's orientation deadline by N days.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def extendorientation(interaction: discord.Interaction, member: discord.Member, days: app_commands.Range[int, 1, 60], reason: str | None = None):
    await ensure_orientation_record(member)
    async with bot.db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT deadline, passed FROM orientations WHERE discord_id = $1", member.id)
        if not row:
            await interaction.response.send_message(f"No orientation record for {member.display_name} and they are not a Medical Student.", ephemeral=True)
            return
        if row["passed"]:
            await interaction.response.send_message(f"{member.display_name} already passed orientation.", ephemeral=True)
            return
        new_deadline = (row["deadline"] or utcnow()) + datetime.timedelta(days=days)
        await conn.execute("UPDATE orientations SET deadline = $1 WHERE discord_id = $2", new_deadline, member.id)

    await log_action("Orientation Deadline Extended",
                     f"Member: {member.mention}\nAdded: **{days}** day(s)\nNew deadline: **{new_deadline.strftime('%Y-%m-%d %H:%M UTC')}**\nReason: {reason or '—'}")
    await interaction.response.send_message(
        f"Extended {member.mention}'s orientation by **{days}** day(s). New deadline: **{new_deadline.strftime('%Y-%m-%d %H:%M UTC')}**.",
        ephemeral=True
    )

# --- Strike helpers/commands ---

async def issue_strike(member: discord.Member, reason: str, *, set_by: int | None, auto: bool) -> int:
    """Create a strike, DM the user, return active strike count after this new strike."""
    now = utcnow()
    expires = now + datetime.timedelta(days=90)
    async with bot.db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO strikes (member_id, reason, issued_at, expires_at, set_by, auto) "
            "VALUES ($1, $2, $3, $4, $5, $6)",
            member.id, reason, now, expires, set_by, auto
        )
        active = await conn.fetchval("SELECT COUNT(*) FROM strikes WHERE member_id=$1 AND expires_at > $2", member.id, now)

    # DM
    try:
        await member.send(
            f"You've received a strike for failing to complete your weekly quota. "
            f"This will expire on **{expires.strftime('%Y-%m-%d')}**. "
            f"(**{active}/3 strikes**)"
        )
    except:
        pass

    await log_action("Strike Issued", f"Member: {member.mention}\nReason: {reason}\nAuto: {auto}\nActive now: **{active}/3**")
    return active

async def enforce_three_strikes(member: discord.Member):
    """Kick from Roblox (if connected) and Discord, DM, and log."""
    try:
        await member.send("You've been automatically removed from the Medical Department for reaching **3/3 strikes**.")
    except:
        pass

    roblox_removed = await try_remove_from_roblox(member.id)

    kicked = False
    try:
        await member.kick(reason="Reached 3/3 strikes — automatic removal.")
        kicked = True
    except Exception as e:
        print(f"Kick failed for {member.id}: {e}")

    await log_action("Three-Strike Removal",
                     f"Member: {member.mention}\nRoblox removal: {'✅' if roblox_removed else '❌/N/A'}\nDiscord kick: {'✅' if kicked else '❌'}")

@bot.tree.command(name="strikes_add", description="(Mgmt) Add a strike to a member.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def strikes_add(interaction: discord.Interaction, member: discord.Member, reason: str):
    active_after = await issue_strike(member, reason, set_by=interaction.user.id, auto=False)
    if active_after >= 3:
        await enforce_three_strikes(member)
    await interaction.response.send_message(f"Strike added to {member.mention}. Active strikes: **{active_after}/3**.", ephemeral=True)

@bot.tree.command(name="strikes_remove", description="(Mgmt) Remove N active strikes from a member (earliest expiring first).")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def strikes_remove(interaction: discord.Interaction, member: discord.Member, count: app_commands.Range[int, 1, 10] = 1):
    now = utcnow()
    async with bot.db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT strike_id FROM strikes WHERE member_id=$1 AND expires_at > $2 ORDER BY expires_at ASC LIMIT $3",
            member.id, now, count
        )
        if not rows:
            await interaction.response.send_message(f"{member.display_name} has no active strikes.", ephemeral=True)
            return
        ids = [r['strike_id'] for r in rows]
        await conn.execute("DELETE FROM strikes WHERE strike_id = ANY($1::int[])", ids)
        remaining = await conn.fetchval("SELECT COUNT(*) FROM strikes WHERE member_id=$1 AND expires_at > $2", member.id, now)
    await log_action("Strikes Removed", f"Member: {member.mention}\nRemoved: **{len(ids)}**\nActive remaining: **{remaining}/3**")
    await interaction.response.send_message(f"Removed **{len(ids)}** strike(s) from {member.mention}. Active remaining: **{remaining}/3**.", ephemeral=True)

@bot.tree.command(name="strikes_view", description="View a member's active and total strikes.")
async def strikes_view(interaction: discord.Interaction, member: discord.Member | None = None):
    target = member or interaction.user
    now = utcnow()
    async with bot.db_pool.acquire() as conn:
        active_rows = await conn.fetch("SELECT reason, expires_at, issued_at, auto FROM strikes WHERE member_id=$1 AND expires_at > $2 ORDER BY expires_at ASC", target.id, now)
        total = await conn.fetchval("SELECT COUNT(*) FROM strikes WHERE member_id=$1", target.id)
    if not active_rows:
        desc = f"**Active strikes:** 0/3\n**Total strikes ever:** {total}"
    else:
        lines = [f"• {r['reason']} — expires **{r['expires_at'].strftime('%Y-%m-%d')}** ({'auto' if r['auto'] else 'manual'})" for r in active_rows]
        desc = f"**Active strikes:** {len(active_rows)}/3\n" + "\n".join(lines) + f"\n\n**Total strikes ever:** {total}"
    embed = discord.Embed(title=f"Strikes for {target.display_name}", description=desc, color=discord.Color.orange(), timestamp=utcnow())
    await interaction.response.send_message(embed=embed, ephemeral=True)
# === Activity Excuse commands ===
@bot.tree.command(name="activityexcuse", description="(Mgmt) Set or clear a weekly activity excuse (no strikes for that week).")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
@app_commands.describe(action="set or clear", week="ISO week like 2025-W39; default = current week", reason="Required when action=set")
async def activityexcuse(
    interaction: discord.Interaction,
    action: str = "set",
    week: str | None = None,
    reason: str | None = None
):
    wk = (week or week_key()).upper()
    if action not in ("set", "clear"):
        await interaction.response.send_message("Action must be `set` or `clear`.", ephemeral=True)
        return

    async with bot.db_pool.acquire() as conn:
        if action == "set":
            if not reason:
                await interaction.response.send_message("Please include a reason when setting an excuse.", ephemeral=True)
                return
            await conn.execute(
                "INSERT INTO activity_excuses (week_key, reason, set_by, set_at) VALUES ($1, $2, $3, $4) "
                "ON CONFLICT (week_key) DO UPDATE SET reason=EXCLUDED.reason, set_by=EXCLUDED.set_by, set_at=EXCLUDED.set_at",
                wk, reason, interaction.user.id, utcnow()
            )
            await log_action("Activity Excuse Set", f"Week: **{wk}**\nBy: {interaction.user.mention}\nReason: {reason}")
            await interaction.response.send_message(f"Activity excuse **set** for week **{wk}**.", ephemeral=True)
        else:
            await conn.execute("DELETE FROM activity_excuses WHERE week_key=$1", wk)
            await log_action("Activity Excuse Cleared", f"Week: **{wk}**\nBy: {interaction.user.mention}")
            await interaction.response.send_message(f"Activity excuse **cleared** for week **{wk}**.", ephemeral=True)

# === Weekly task summary + strikes + reset ===
@tasks.loop(time=datetime.time(hour=4, minute=0, tzinfo=datetime.timezone.utc))
async def check_weekly_tasks():
    # Only fire on Sunday UTC
    if utcnow().weekday() != 6:
        return

    wk = week_key()
    # If excused week, we still post the report but we **do not issue strikes**
    async with bot.db_pool.acquire() as conn:
        is_excused_row = await conn.fetchrow("SELECT week_key, reason FROM activity_excuses WHERE week_key=$1", wk)
    excused_reason = is_excused_row["reason"] if is_excused_row else None

    announcement_channel = bot.get_channel(ANNOUNCEMENT_CHANNEL_ID)
    if not announcement_channel:
        print("Weekly check failed: Announcement channel not found.")
        return

    guild = announcement_channel.guild
    dept_role = guild.get_role(DEPARTMENT_ROLE_ID)
    if not dept_role:
        print("Weekly check failed: Department role not found.")
        return

    dept_member_ids = {m.id for m in dept_role.members if not m.bot}

    async with bot.db_pool.acquire() as conn:
        all_tasks = await conn.fetch("SELECT member_id, tasks_completed FROM weekly_tasks")
        all_time = await conn.fetch("SELECT member_id, time_spent FROM roblox_time")
        # Pull active strikes counts for all dept members
        strike_counts = {
            r['member_id']: r['cnt'] for r in await conn.fetch(
                "SELECT member_id, COUNT(*) as cnt FROM strikes WHERE expires_at > $1 GROUP BY member_id",
                utcnow()
            )
        }

    tasks_map = {r['member_id']: r['tasks_completed'] for r in all_tasks if r['member_id'] in dept_member_ids}
    time_map = {r['member_id']: r['time_spent'] for r in all_time if r['member_id'] in dept_member_ids}

    met, not_met, zero = [], [], []
    considered_ids = set(tasks_map.keys()) | set(time_map.keys())

    for member_id in considered_ids:
        member = guild.get_member(member_id)
        if not member:
            continue
        tasks_done = tasks_map.get(member_id, 0)
        time_done_minutes = (time_map.get(member_id, 0)) // 60
        sc = strike_counts.get(member_id, 0)
        if tasks_done >= WEEKLY_REQUIREMENT and time_done_minutes >= WEEKLY_TIME_REQUIREMENT:
            met.append((member, sc))
        else:
            not_met.append((member, tasks_done, time_done_minutes, sc))

    zero_ids = dept_member_ids - considered_ids
    for mid in zero_ids:
        member = guild.get_member(mid)
        if member:
            sc = strike_counts.get(mid, 0)
            zero.append((member, sc))

    # Post report
    def fmt_met(lst):
        return ", ".join(f"{m.mention} (strikes: {sc})" for m, sc in lst) if lst else "—"

    def fmt_not_met(lst):
        return "\n".join(f"{m.mention} — {t}/{WEEKLY_REQUIREMENT} tasks, {mins}/{WEEKLY_TIME_REQUIREMENT} mins (strikes: {sc})" for m, t, mins, sc in lst) if lst else "—"

    def fmt_zero(lst):
        return ", ".join(f"{m.mention} (strikes: {sc})" for m, sc in lst) if lst else "—"

    summary = f"--- Weekly Task Report (**{wk}**){' — EXCUSED' if excused_reason else ''} ---\n\n"
    if excused_reason:
        summary += f"**Excuse Reason:** {excused_reason}\n\n"
    summary += f"**✅ Met Requirement ({len(met)}):**\n{fmt_met(met)}\n\n"
    summary += f"**❌ Below Quota ({len(not_met)}):**\n{fmt_not_met(not_met)}\n\n"
    summary += f"**🚫 0 Activity ({len(zero)}):**\n{fmt_zero(zero)}\n\n"
    summary += "Weekly counts will now be reset."

    await send_long_embed(
        target=announcement_channel,
        title="Weekly Task Summary",
        description=summary,
        color=discord.Color.gold(),
        footer_text=None
    )

    # Issue strikes for not-met (if NOT excused)
    if not excused_reason:
        for m, t, mins, _sc in not_met + [(m, 0, 0, sc) for m, sc in zero]:
            try:
                if not m:
                    continue
                active_after = await issue_strike(m, "Missed weekly quota", set_by=None, auto=True)
                if active_after >= 3:
                    await enforce_three_strikes(m)
            except Exception as e:
                print(f"Strike flow error for {getattr(m, 'id', 'unknown')}: {e}")

    # Reset weekly tables
    async with bot.db_pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE weekly_tasks, weekly_task_logs, roblox_time, roblox_sessions")
    print("Weekly tasks and time checked and reset.")

# Orientation 5-day warning + overdue enforcement
@tasks.loop(minutes=30)
async def orientation_reminder_loop():
    try:
        alert_channel = bot.get_channel(ORIENTATION_ALERT_CHANNEL_ID)
        async with bot.db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT discord_id, deadline, warned_5d, passed, expired_handled "
                "FROM orientations WHERE passed = FALSE"
            )
        if not rows:
            return

        now = utcnow()
        for r in rows:
            discord_id = r["discord_id"]
            deadline = r["deadline"]
            warned = r["warned_5d"]
            expired_handled = r["expired_handled"]
            if not deadline:
                continue

            remaining = deadline - now

            # 5-day warning
            if (not warned) and datetime.timedelta(days=4, hours=23) <= remaining <= datetime.timedelta(days=5, hours=1):
                if alert_channel:
                    member = find_member(discord_id)
                    if member:
                        pretty = human_remaining(remaining)
                        await alert_channel.send(
                            f"{member.mention} hasn't completed their orientation yet and has **{pretty}** to complete it, please check in with them."
                        )
                        async with bot.db_pool.acquire() as conn2:
                            await conn2.execute("UPDATE orientations SET warned_5d = TRUE WHERE discord_id = $1", discord_id)

            # Overdue enforcement (only once)
            if remaining <= datetime.timedelta(seconds=0) and not expired_handled:
                member = find_member(discord_id)
                if member:
                    try:
                        await member.send(
                            "Hi — this is an automatic notice from the Medical Department.\n\n"
                            "Your **2-week orientation deadline** has passed and you have been **removed** due to not completing orientation in time.\n"
                            "If this is a mistake, please contact MD Management."
                        )
                    except:
                        pass

                    roblox_removed = await try_remove_from_roblox(discord_id)

                    try:
                        await member.kick(reason="Orientation deadline expired — automatic removal.")
                        kicked = True
                    except Exception as e:
                        print(f"Kick failed for {member.id}: {e}")
                        kicked = False

                    await log_action(
                        "Orientation Expiry Enforced",
                        f"Member: <@{discord_id}>\nRoblox removal: {'✅' if roblox_removed else 'Skipped/Failed ❌'}\nDiscord kick: {'✅' if kicked else '❌'}"
                    )

                    async with bot.db_pool.acquire() as conn3:
                        await conn3.execute("UPDATE orientations SET expired_handled = TRUE WHERE discord_id = $1", discord_id)
    except Exception as e:
        print(f"orientation_reminder_loop error: {e}")

@orientation_reminder_loop.before_loop
async def before_orientation_loop():
    await bot.wait_until_ready()

# === /rank with autocomplete ===
async def group_role_autocomplete(interaction: discord.Interaction, current: str):
    current_lower = (current or "").lower()
    roles = await fetch_group_ranks()
    if not roles:
        return []
    out = []
    for r in roles:
        name = r.get('name', '')
        if not current_lower or name.lower().startswith(current_lower):
            out.append(app_commands.Choice(name=name, value=name))
        if len(out) >= 25:
            break
    return out

@bot.tree.command(name="rank", description="(Rank Manager) Set a member's Roblox/Discord rank to a group role.")
@app_commands.checks.has_role(RANK_MANAGER_ROLE_ID)
@app_commands.autocomplete(group_role=group_role_autocomplete)
async def rank(interaction: discord.Interaction, member: discord.Member, group_role: str):
    # Resolve roblox_id
    async with bot.db_pool.acquire() as conn:
        roblox_id = await conn.fetchval("SELECT roblox_id FROM roblox_verification WHERE discord_id = $1", member.id)
    if not roblox_id:
        await interaction.response.send_message(f"{member.display_name} hasn’t linked a Roblox account with `/verify` yet.", ephemeral=True)
        return

    # Fetch ranks from the service
    ranks = await fetch_group_ranks()
    if not ranks:
        await interaction.response.send_message("Couldn’t fetch Roblox group ranks. Check ROBLOX_SERVICE_BASE & secret.", ephemeral=True)
        return

    # Find by name (case-insensitive)
    target = next((r for r in ranks if r.get('name','').lower() == group_role.lower()), None)
    if not target:
        await interaction.response.send_message("That rank wasn’t found. Try typing to see suggestions.", ephemeral=True)
        return

    # Remove previous Discord role that matches stored rank, then set new
    try:
        prev_rank = None
        async with bot.db_pool.acquire() as conn:
            prev_rank = await conn.fetchval("SELECT rank FROM member_ranks WHERE discord_id=$1", member.id)
        if prev_rank:
            for role in interaction.guild.roles:
                if role.name.lower() == prev_rank.lower():
                    await member.remove_roles(role, reason=f"Replacing rank via /rank by {interaction.user}")
                    break
    except Exception as e:
        print(f"/rank remove old role error: {e}")

    # Set Roblox rank via service
    ok = await set_group_rank(int(roblox_id), role_id=int(target['id']))
    if not ok:
        await interaction.response.send_message("Failed to set Roblox rank (service error).", ephemeral=True)
        return

    # Store in DB
    async with bot.db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO member_ranks (discord_id, rank, set_by, set_at) VALUES ($1, $2, $3, $4) "
            "ON CONFLICT (discord_id) DO UPDATE SET rank = EXCLUDED.rank, set_by = EXCLUDED.set_by, set_at = EXCLUDED.set_at",
            member.id, target['name'], interaction.user.id, utcnow()
        )

    # Assign matching Discord role if present
    assigned_role = None
    try:
        for role in interaction.guild.roles:
            if role.name.lower() == target['name'].lower():
                await member.add_roles(role, reason=f"Rank set via /rank by {interaction.user}")
                assigned_role = role
                break
    except Exception as e:
        print(f"/rank role assign error: {e}")

    msg = f"Set **Roblox rank** for {member.mention} to **{target['name']}**."
    if assigned_role:
        msg += f" Also assigned Discord role **{assigned_role.name}**."
    await log_action("Rank Set", f"By: {interaction.user.mention}\nMember: {member.mention}\nNew Rank: **{target['name']}**")
    await interaction.response.send_message(msg, ephemeral=True)

# === Run ===
if __name__ == "__main__":
    if ROBLOX_SERVICE_BASE:
        try:
            parsed = urlparse(ROBLOX_SERVICE_BASE)
            if not parsed.scheme or not parsed.netloc:
                print(f"[WARN] ROBLOX_SERVICE_BASE looks odd: {ROBLOX_SERVICE_BASE}")
        except Exception:
            print(f"[WARN] Could not parse ROBLOX_SERVICE_BASE: {ROBLOX_SERVICE_BASE}")
    else:
        print("[INFO] ROBLOX_SERVICE_BASE not set; /rank autocomplete + set-rank will be unavailable.")

    bot.run(BOT_TOKEN)
