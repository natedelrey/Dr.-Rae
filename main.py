import discord
from discord.ext import commands, tasks
import os
from dotenv import load_dotenv
import datetime
import random
import aiohttp
import asyncpg
from aiohttp import web
from discord import app_commands

# --- Configuration ---
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

ANNOUNCEMENT_CHANNEL_ID = int(os.getenv("ANNOUNCEMENT_CHANNEL_ID"))
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID"))
ANNOUNCEMENT_ROLE_ID = int(os.getenv("ANNOUNCEMENT_ROLE_ID"))
MANAGEMENT_ROLE_ID = int(os.getenv("MANAGEMENT_ROLE_ID"))

# Anomaly Actors (for /aa command)
AA_CHANNEL_ID = int(os.getenv("AA_CHANNEL_ID"))
ANOMALY_ACTORS_ROLE_ID = int(os.getenv("ANOMALY_ACTORS_ROLE_ID"))

# Department & Orientation
DEPARTMENT_ROLE_ID = int(os.getenv("DEPARTMENT_ROLE_ID"))                # 1405988543230382091
MEDICAL_STUDENT_ROLE_ID = int(os.getenv("MEDICAL_STUDENT_ROLE_ID"))      # 1405977418254127205
ORIENTATION_ALERT_CHANNEL_ID = int(os.getenv("ORIENTATION_ALERT_CHANNEL_ID"))  # 1405985030823743600

# DB / API
DATABASE_URL = os.getenv("DATABASE_URL")
API_SECRET_KEY = os.getenv("API_SECRET_KEY")

# Misc
ACTIVITY_LOG_CHANNEL_ID = 1409646416829354095  # move to env if you prefer
WEEKLY_REQUIREMENT = 3
WEEKLY_TIME_REQUIREMENT = 45  # minutes

# --- Bot Setup ---
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.guilds = True

TASK_TYPES = [
    "Interview",
    "Post-Op Interview",
    "Checkup",
    "Anomaly Checkup",
    "Medical Department Recruitment",
]

def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)

def human_remaining(delta: datetime.timedelta) -> str:
    if delta.total_seconds() <= 0:
        return "0 days"
    days = delta.days
    hours = (delta.seconds // 3600)
    mins = (delta.seconds % 3600) // 60
    parts = []
    if days: parts.append(f"{days}d")
    if hours: parts.append(f"{hours}h")
    if mins and not days: parts.append(f"{mins}m")  # keep short
    return " ".join(parts) if parts else "under 1m"

class MD_BOT(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix='!', intents=intents)
        self.db_pool = None

    async def setup_hook(self):
        # DB pool
        try:
            self.db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
            print("Successfully connected to the database.")
        except Exception as e:
            print(f"Failed to connect to the database: {e}")
            return

        # Schema (create/alter)
        async with self.db_pool.acquire() as connection:
            # existing tables
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

            # NEW: orientation tracking
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS orientations (
                    discord_id BIGINT PRIMARY KEY,
                    assigned_at TIMESTAMPTZ,      -- when Med Student role detected
                    deadline TIMESTAMPTZ,         -- assigned_at + 14 days
                    passed BOOLEAN DEFAULT FALSE,
                    passed_at TIMESTAMPTZ,
                    warned_5d BOOLEAN DEFAULT FALSE
                );
            ''')

            # NEW: add task_type to task_logs
            await connection.execute('''
                ALTER TABLE task_logs
                ADD COLUMN IF NOT EXISTS task_type TEXT;
            ''')

        print("Database tables are ready.")

        # Sync commands
        try:
            synced = await self.tree.sync()
            print(f"Synced {len(synced)} command(s)")
        except Exception as e:
            print(f"Failed to sync commands: {e}")

        # Web server for Roblox integration
        app = web.Application()
        app.router.add_post('/roblox', self.roblox_handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()
        print("Web server for Roblox integration is running.")

    async def roblox_handler(self, request):
        if request.headers.get("X-Secret-Key") != API_SECRET_KEY:
            return web.Response(status=401)

        data = await request.json()
        roblox_id = data.get("robloxId")
        status = data.get("status")

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
            elif status == "left":
                async with self.db_pool.acquire() as connection:
                    session_start = await connection.fetchval(
                        "SELECT start_time FROM roblox_sessions WHERE roblox_id = $1", roblox_id
                    )
                    if session_start:
                        await connection.execute(
                            "DELETE FROM roblox_sessions WHERE roblox_id = $1", roblox_id
                        )
                        duration = (utcnow() - session_start).total_seconds()
                        await connection.execute(
                            "INSERT INTO roblox_time (member_id, time_spent) VALUES ($1, $2) "
                            "ON CONFLICT (member_id) DO UPDATE SET time_spent = roblox_time.time_spent + $2",
                            discord_id, int(duration)
                        )
                        new_total_time = await connection.fetchval(
                            "SELECT time_spent FROM roblox_time WHERE member_id = $1", discord_id
                        )
                        try:
                            activity_log_channel = await self.fetch_channel(ACTIVITY_LOG_CHANNEL_ID)
                            if activity_log_channel:
                                member = await activity_log_channel.guild.fetch_member(discord_id)
                                if member:
                                    embed = discord.Embed(
                                        title="Roblox Activity Logged",
                                        description=f"**{member.display_name}** was on-site for **{int(duration // 60)} minutes**.",
                                        color=discord.Color.blue(),
                                        timestamp=utcnow()
                                    )
                                    embed.set_footer(text=f"Total on-site time this week: {int(new_total_time // 60)} minutes")
                                    await activity_log_channel.send(embed=embed)
                        except Exception as e:
                            print(f"Error sending activity log: {e}")

        return web.Response(status=200)

bot = MD_BOT()

# --- Helpers ---
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
    embed = discord.Embed(
        title=title,
        description=chunks[0],
        color=color,
        timestamp=utcnow()
    )
    if footer_text:
        embed.set_footer(text=footer_text)
    if author_name:
        embed.set_author(name=author_name, icon_url=author_icon_url)
    if image_url:
        embed.set_image(url=image_url)
    await target.send(embed=embed)
    for i, chunk in enumerate(chunks[1:], start=2):
        follow_up = discord.Embed(description=chunk, color=color)
        follow_up.set_footer(text=f"Part {i}/{len(chunks)}")
        await target.send(embed=follow_up)

# --- Modals ---
class AnnouncementForm(discord.ui.Modal, title='Send Announcement'):
    def __init__(self, color_obj: discord.Color):
        super().__init__()
        self.color_obj = color_obj

    ann_title = discord.ui.TextInput(
        label='Title',
        placeholder='Announcement title',
        style=discord.TextStyle.short,
        required=True,
        max_length=200
    )
    ann_message = discord.ui.TextInput(
        label='Message',
        placeholder='Write your announcement here… (auto-formats)',
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=4000
    )

    async def on_submit(self, interaction: discord.Interaction):
        announcement_channel = bot.get_channel(ANNOUNCEMENT_CHANNEL_ID)
        if not announcement_channel:
            await interaction.response.send_message("Announcement channel not found.", ephemeral=True)
            return

        await send_long_embed(
            target=announcement_channel,
            title=f"📢 {self.ann_title.value}",
            description=self.ann_message.value,  # already multi-line
            color=self.color_obj,
            footer_text=f"Announcement by {interaction.user.display_name}"
        )
        await interaction.response.send_message("Announcement sent successfully!", ephemeral=True)

class LogTaskForm(discord.ui.Modal, title='Add Comments (optional)'):
    def __init__(self, proof: discord.Attachment, task_type: str):
        super().__init__()
        self.proof = proof
        self.task_type = task_type

    comments = discord.ui.TextInput(
        label='Comments',
        placeholder='Any additional comments?',
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=1000
    )

    async def on_submit(self, interaction: discord.Interaction):
        log_channel = bot.get_channel(LOG_CHANNEL_ID)
        if not log_channel:
            await interaction.response.send_message("Log channel not found.", ephemeral=True)
            return

        member_id = interaction.user.id
        comments_str = self.comments.value or "No comments"

        async with bot.db_pool.acquire() as connection:
            await connection.execute(
                "INSERT INTO task_logs (member_id, task, task_type, proof_url, comments, timestamp) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                member_id, self.task_type, self.task_type, self.proof.url, comments_str, utcnow()
            )
            await connection.execute(
                "INSERT INTO weekly_tasks (member_id, tasks_completed) VALUES ($1, 1) "
                "ON CONFLICT (member_id) DO UPDATE SET tasks_completed = weekly_tasks.tasks_completed + 1",
                member_id
            )
            tasks_completed = await connection.fetchval(
                "SELECT tasks_completed FROM weekly_tasks WHERE member_id = $1", member_id
            )

        full_description = f"**Task Type:** {self.task_type}\n\n**Comments:**\n{comments_str}"
        await send_long_embed(
            target=log_channel,
            title="✅ Task Logged",
            description=full_description,
            color=discord.Color.from_rgb(0, 255, 127),
            footer_text=f"Member ID: {member_id}",
            author_name=interaction.user.display_name,
            author_icon_url=interaction.user.avatar.url if interaction.user.avatar else None,
            image_url=self.proof.url
        )
        await interaction.response.send_message(
            f"Your task has been logged! You have completed {tasks_completed} task(s) this week.",
            ephemeral=True
        )

# --- Events ---
@bot.event
async def on_ready():
    print(f'Logged in as {bot.user.name}')
    print(f'discord.py version: {discord.__version__}')
    check_weekly_tasks.start()
    orientation_reminder_loop.start()

@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    """Detect Medical Student role being added -> create orientation record with 14-day deadline."""
    try:
        before_roles = {r.id for r in before.roles}
        after_roles = {r.id for r in after.roles}
        if MEDICAL_STUDENT_ROLE_ID not in before_roles and MEDICAL_STUDENT_ROLE_ID in after_roles:
            # New Med Student — set orientation record if not exists
            assigned = utcnow()
            deadline = assigned + datetime.timedelta(days=14)
            async with bot.db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO orientations (discord_id, assigned_at, deadline, passed, warned_5d) "
                    "VALUES ($1, $2, $3, FALSE, FALSE) "
                    "ON CONFLICT (discord_id) DO NOTHING",
                    after.id, assigned, deadline
                )
            print(f"Orientation timer started for {after.id} (deadline {deadline.isoformat()}).")
    except Exception as e:
        print(f"on_member_update error: {e}")

# --- Commands ---

# VERIFY (unchanged)
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
                    async with bot.db_pool.acquire() as connection:
                        await connection.execute(
                            "INSERT INTO roblox_verification (discord_id, roblox_id) VALUES ($1, $2) "
                            "ON CONFLICT (discord_id) DO UPDATE SET roblox_id = $2",
                            interaction.user.id, roblox_id
                        )
                    await interaction.response.send_message(f"Successfully verified as {roblox_name}!", ephemeral=True)
                else:
                    await interaction.response.send_message("Could not find a Roblox user with that name.", ephemeral=True)
            else:
                await interaction.response.send_message("There was an error looking up the Roblox user.", ephemeral=True)

# ANNOUNCE -> opens modal (keeps color choice as slash option)
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

@announce.error
async def announce_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingRole):
        await interaction.response.send_message("Sorry, you don't have the required role.", ephemeral=True)
    else:
        await interaction.response.send_message("An error occurred.", ephemeral=True)
        print(error)

# LOG with selectable task type + proof + optional comments modal
@bot.tree.command(name="log", description="Log a completed task with proof and selected task type.")
@app_commands.choices(task_type=[app_commands.Choice(name=t, value=t) for t in TASK_TYPES])
async def log(interaction: discord.Interaction, task_type: str, proof: discord.Attachment):
    # open a tiny modal for optional comments
    await interaction.response.send_modal(LogTaskForm(proof=proof, task_type=task_type))

# MYTASKS (unchanged)
@bot.tree.command(name="mytasks", description="Check how many tasks you have completed this week.")
async def mytasks(interaction: discord.Interaction):
    member_id = interaction.user.id
    async with bot.db_pool.acquire() as connection:
        tasks_completed = await connection.fetchval(
            "SELECT tasks_completed FROM weekly_tasks WHERE member_id = $1", member_id
        ) or 0
        time_spent_seconds = await connection.fetchval(
            "SELECT time_spent FROM roblox_time WHERE member_id = $1", member_id
        ) or 0
    time_spent_minutes = time_spent_seconds // 60
    await interaction.response.send_message(
        f"You have completed **{tasks_completed}** of **{WEEKLY_REQUIREMENT}** tasks and have "
        f"**{time_spent_minutes}** of **{WEEKLY_TIME_REQUIREMENT}** minutes on-site.",
        ephemeral=True
    )

# LEADERBOARD (unchanged)
@bot.tree.command(name="leaderboard", description="Displays the weekly task leaderboard.")
async def leaderboard(interaction: discord.Interaction):
    async with bot.db_pool.acquire() as connection:
        rows = await connection.fetch(
            "SELECT member_id, tasks_completed FROM weekly_tasks ORDER BY tasks_completed DESC LIMIT 10"
        )
    if not rows:
        await interaction.response.send_message("No tasks logged this week.", ephemeral=True)
        return
    embed = discord.Embed(
        title="🏆 Weekly Task Leaderboard",
        color=discord.Color.gold(),
        timestamp=utcnow()
    )
    description = ""
    rank_emoji = ["🥇", "🥈", "🥉"]
    for i, record in enumerate(rows):
        member = interaction.guild.get_member(record['member_id'])
        name = member.display_name if member else f"Unknown User ({record['member_id']})"
        if i < 3:
            description += f"{rank_emoji[i]} **{name}** - {record['tasks_completed']} tasks\n"
        else:
            description += f"**{i+1}.** {name} - {record['tasks_completed']} tasks\n"
    embed.description = description
    await interaction.response.send_message(embed=embed)

# REMOVE LAST LOG (unchanged)
@bot.tree.command(name="removelastlog", description="Removes the last logged task for a member.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def removelastlog(interaction: discord.Interaction, member: discord.Member):
    member_id = member.id
    async with bot.db_pool.acquire() as connection:
        async with connection.transaction():
            last_log = await connection.fetchrow(
                "SELECT log_id, task FROM task_logs WHERE member_id = $1 ORDER BY timestamp DESC LIMIT 1",
                member_id
            )
            if not last_log:
                await interaction.response.send_message(f"{member.display_name} has no tasks logged.", ephemeral=True)
                return
            await connection.execute("DELETE FROM task_logs WHERE log_id = $1", last_log['log_id'])
            await connection.execute(
                "UPDATE weekly_tasks SET tasks_completed = GREATEST(tasks_completed - 1, 0) WHERE member_id = $1", member_id
            )
            new_count = await connection.fetchval(
                "SELECT tasks_completed FROM weekly_tasks WHERE member_id = $1", member_id
            )
    await interaction.response.send_message(
        f"Removed last task for {member.mention}: '{last_log['task']}'. They now have {new_count} tasks.",
        ephemeral=True
    )

@removelastlog.error
async def removelastlog_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingRole):
        await interaction.response.send_message("You do not have the required role.", ephemeral=True)
    else:
        await interaction.response.send_message("An error occurred.", ephemeral=True)
        print(error)

# WELCOME (unchanged)
@bot.tree.command(name="welcome", description="Sends the official welcome message.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def welcome(interaction: discord.Interaction):
    msg = (
        "Welcome to the Medical Department! We're super excited to have you join us. 😊\n\n"
        "First things first, you have get your student orientation done within your first two weeks. 🗓️ "
        "Just message any of the management team and they'll get you scheduled. ✔️\n\n"
        "👉 Before you jump in, read our MD Info Hub on Trello:\n"
        "https://trello.com/b/j2jvme4Z/md-information-hub\n\n"
        "We're happy to have you here! If you have any questions, just ask. 🚀"
    )
    embed = discord.Embed(title="Welcome to the Team!", description=msg, color=discord.Color.green())
    embed.set_footer(text="Best,\nThe Medical Department Management Team")
    await interaction.channel.send(embed=embed)
    await interaction.response.send_message("Welcome message sent!", ephemeral=True)

@welcome.error
async def welcome_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingRole):
        await interaction.response.send_message("You do not have the required role.", ephemeral=True)
    else:
        await interaction.response.send_message("An error occurred.", ephemeral=True)
        print(error)

# DM (unchanged)
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
        await interaction.response.send_message(f"Your message has been sent to {member.mention}!", ephemeral=True)
    except discord.Forbidden:
        await interaction.response.send_message(f"I couldn't message {member.mention}. They might have DMs disabled.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message("An unexpected error occurred.", ephemeral=True)
        print(f"DM command error: {e}")

# AA ping (unchanged)
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

    embed = discord.Embed(
        title=title,
        description="\n".join(body),
        color=discord.Color.purple(),
        timestamp=utcnow()
    )
    embed.set_footer(text=f"Requested by {interaction.user.display_name}")

    await target_channel.send(
        content=f"{role.mention}",
        embed=embed,
        allowed_mentions=discord.AllowedMentions(roles=True)
    )
    await interaction.response.send_message("Anomaly Actors have been pinged for a checkup.", ephemeral=True)

@aa.error
async def aa_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingRole):
        await interaction.response.send_message("You don’t have permission to use this command.", ephemeral=True)
    elif isinstance(error, app_commands.CommandOnCooldown):
        retry_in = int(error.retry_after)
        minutes, seconds = divmod(retry_in, 60)
        pretty = f"{minutes}m {seconds}s" if minutes else f"{seconds}s"
        await interaction.response.send_message(f"⏳ You can use this again in **{pretty}**.", ephemeral=True)
    else:
        await interaction.response.send_message("An unexpected error occurred.", ephemeral=True)
        print(f"/aa error: {error}")

# --- Orientation Commands ---
@bot.tree.command(name="passedorientation", description="Mark a member as having passed orientation.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def passedorientation(interaction: discord.Interaction, member: discord.Member):
    async with bot.db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO orientations (discord_id, assigned_at, deadline, passed, warned_5d) "
            "VALUES ($1, $2, $2 + interval '14 days', TRUE, TRUE) "
            "ON CONFLICT (discord_id) DO UPDATE SET passed = TRUE, passed_at = $3",
            member.id, utcnow(), utcnow()
        )
    await interaction.response.send_message(f"Marked {member.mention} as **passed orientation**.", ephemeral=True)

@bot.tree.command(name="orientationview", description="View a member's orientation status.")
async def orientationview(interaction: discord.Interaction, member: discord.Member):
    async with bot.db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT assigned_at, deadline, passed, passed_at FROM orientations WHERE discord_id = $1",
            member.id
        )
    if not row:
        await interaction.response.send_message(
            f"No orientation record for {member.mention}.",
            ephemeral=True
        )
        return

    if row["passed"]:
        when = row["passed_at"].strftime("%Y-%m-%d %H:%M UTC") if row["passed_at"] else "unknown time"
        msg = f"**{member.display_name}**: ✅ Passed orientation (at {when})."
    else:
        remaining = row["deadline"] - utcnow()
        msg = (
            f"**{member.display_name}**: ❌ Not passed.\n"
            f"Deadline: {row['deadline'].strftime('%Y-%m-%d %H:%M UTC')} "
            f"(**{human_remaining(remaining)}** remaining)"
        )
    await interaction.response.send_message(msg, ephemeral=True)

# --- View Tasks ---
@bot.tree.command(name="viewtasks", description="List a member's logged tasks (type + date).")
async def viewtasks(interaction: discord.Interaction, member: discord.Member):
    async with bot.db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT task_type, task, timestamp, proof_url FROM task_logs "
            "WHERE member_id = $1 ORDER BY timestamp DESC LIMIT 25",
            member.id
        )
    if not rows:
        await interaction.response.send_message(f"No tasks found for {member.display_name}.", ephemeral=True)
        return
    lines = []
    for r in rows:
        t = r["timestamp"].strftime("%Y-%m-%d")
        ttype = r["task_type"] or r["task"]
        lines.append(f"• **{t}** — {ttype}")
    embed = discord.Embed(
        title=f"🗂️ Tasks for {member.display_name}",
        description="\n".join(lines),
        color=discord.Color.blurple(),
        timestamp=utcnow()
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

# --- Weekly Task Checking (FILTERED TO DEPARTMENT ROLE) ---
@tasks.loop(time=datetime.time(hour=4, minute=0, tzinfo=datetime.timezone.utc))
async def check_weekly_tasks():
    # Run on Sundays (weekday=6) only
    if utcnow().weekday() != 6:
        return

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

    async with bot.db_pool.acquire() as connection:
        all_tasks = await connection.fetch("SELECT member_id, tasks_completed FROM weekly_tasks")
        all_time = await connection.fetch("SELECT member_id, time_spent FROM roblox_time")

    tasks_map = {r['member_id']: r['tasks_completed'] for r in all_tasks if r['member_id'] in dept_member_ids}
    time_map = {r['member_id']: r['time_spent'] for r in all_time if r['member_id'] in dept_member_ids}

    # Consider only department members for reporting
    met, not_met, zero = [], [], []

    considered_ids = set(tasks_map.keys()) | set(time_map.keys())
    for member_id in considered_ids:
        member = guild.get_member(member_id)
        if not member:
            continue
        tasks_done = tasks_map.get(member_id, 0)
        time_done_minutes = (time_map.get(member_id, 0)) // 60
        if tasks_done >= WEEKLY_REQUIREMENT and time_done_minutes >= WEEKLY_TIME_REQUIREMENT:
            met.append(member.mention)
        else:
            not_met.append(f"{member.mention} ({tasks_done}/{WEEKLY_REQUIREMENT} tasks, {time_done_minutes}/{WEEKLY_TIME_REQUIREMENT} mins)")

    # Department members with no records at all -> zero
    zero_ids = dept_member_ids - considered_ids
    for mid in zero_ids:
        member = guild.get_member(mid)
        if member:
            zero.append(member.mention)

    summary = "--- Weekly Task Report ---\n\n"
    if met:
        summary += f"**✅ Met Requirement ({len(met)}):**\n" + ", ".join(met) + "\n\n"
    if not_met:
        summary += f"**❌ Below Quota ({len(not_met)}):**\n" + "\n".join(not_met) + "\n\n"
    if zero:
        summary += f"**🚫 0 Activity ({len(zero)}):**\n" + ", ".join(zero) + "\n\n"
    if not (met or not_met or zero):
        summary += "**No department activity was logged this week.**\n\n"
    summary += "Task and time counts have now been reset for the new week."

    await send_long_embed(
        target=announcement_channel,
        title="Weekly Task Summary",
        description=summary,
        color=discord.Color.gold(),
        footer_text=None
    )

    # Reset weekly counters
    async with bot.db_pool.acquire() as connection:
        await connection.execute("TRUNCATE TABLE weekly_tasks, task_logs, roblox_time, roblox_sessions")
    print("Weekly tasks and time checked and reset.")

@check_weekly_tasks.before_loop
async def before_check():
    await bot.wait_until_ready()

# --- Orientation reminder loop (5 days remaining) ---
@tasks.loop(minutes=30)
async def orientation_reminder_loop():
    try:
        guilds = bot.guilds
        alert_channel = bot.get_channel(ORIENTATION_ALERT_CHANNEL_ID)
        if not alert_channel:
            return

        async with bot.db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT discord_id, deadline, warned_5d, passed FROM orientations "
                "WHERE passed = FALSE"
            )
        if not rows:
            return

        now = utcnow()
        for r in rows:
            deadline = r["deadline"]
            if not deadline:
                continue
            remaining = deadline - now
            # Trigger when crossing 5 days remaining, only once
            if datetime.timedelta(days=4, hours=23, minutes=30) <= remaining <= datetime.timedelta(days=5, hours=0, minutes=30) and not r["warned_5d"]:
                # Best-effort resolve member for mention
                for g in guilds:
                    member = g.get_member(r["discord_id"])
                    if member:
                        pretty = human_remaining(remaining)
                        msg = f"{member.mention} hasn't completed their orientation yet and has **{pretty}** to complete it, please check in with them."
                        await alert_channel.send(msg)
                        async with bot.db_pool.acquire() as conn:
                            await conn.execute("UPDATE orientations SET warned_5d = TRUE WHERE discord_id = $1", r["discord_id"])
                        break
    except Exception as e:
        print(f"orientation_reminder_loop error: {e}")

@orientation_reminder_loop.before_loop
async def before_orientation_loop():
    await bot.wait_until_ready()

# --- Run ---
if __name__ == "__main__":
    bot.run(BOT_TOKEN)
