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
from discord.ext.commands import BucketType  # <-- correct place for BucketType

# --- Configuration ---
# Load environment variables from a .env file
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ANNOUNCEMENT_CHANNEL_ID = int(os.getenv("ANNOUNCEMENT_CHANNEL_ID"))
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID"))
ACTIVITY_LOG_CHANNEL_ID = 1409646416829354095  # The new channel for Roblox activity logs
WEEKLY_REQUIREMENT = 3
WEEKLY_TIME_REQUIREMENT = 45  # in minutes
ANNOUNCEMENT_ROLE_ID = int(os.getenv("ANNOUNCEMENT_ROLE_ID"))
MANAGEMENT_ROLE_ID = int(os.getenv("MANAGEMENT_ROLE_ID"))
DATABASE_URL = os.getenv("DATABASE_URL")
API_SECRET_KEY = os.getenv("API_SECRET_KEY")  # Add this to your .env file

# Fixed IDs for /aa command
AA_CHANNEL_ID = 1414791179941314580
ANOMALY_ACTORS_ROLE_ID = 1414796930172715028

# --- Bot Setup ---
# Define the intents your bot needs.
intents = discord.Intents.default()
intents.members = True
intents.message_content = True  # Added privileged intent

class MD_BOT(commands.Bot):
    def __init__(self):
        # Using a prefix that won't conflict with slash commands
        super().__init__(command_prefix='!', intents=intents)
        self.db_pool = None

    async def setup_hook(self):
        # Create a database connection pool
        try:
            # Added min_size and max_size to make the pool more resilient
            self.db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
            print("Successfully connected to the database.")
        except Exception as e:
            print(f"Failed to connect to the database: {e}")
            return

        # Create tables if they don't exist
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
                    time_spent INT DEFAULT 0 -- in seconds
                );
            ''')
            # New table for persistent sessions
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS roblox_sessions (
                    roblox_id BIGINT PRIMARY KEY,
                    start_time TIMESTAMPTZ
                );
            ''')
        print("Database tables are ready.")
        # Synchronize the slash commands with Discord
        try:
            synced = await self.tree.sync()
            print(f"Synced {len(synced)} command(s)")
        except Exception as e:
            print(f"Failed to sync commands: {e}")
        
        # Start the web server for Roblox integration
        app = web.Application()
        app.router.add_post('/roblox', self.roblox_handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 8080)  # Railway will use port 8080
        await site.start()
        print("Web server for Roblox integration is running.")

    async def roblox_handler(self, request):
        if request.headers.get("X-Secret-Key") != API_SECRET_KEY:
            return web.Response(status=401)  # Unauthorized

        data = await request.json()
        roblox_id = data.get("robloxId")
        status = data.get("status")

        async with self.db_pool.acquire() as connection:
            discord_id = await connection.fetchval("SELECT discord_id FROM roblox_verification WHERE roblox_id = $1", roblox_id)

        if discord_id:
            if status == "joined":
                async with self.db_pool.acquire() as connection:
                    await connection.execute(
                        "INSERT INTO roblox_sessions (roblox_id, start_time) VALUES ($1, $2) ON CONFLICT (roblox_id) DO UPDATE SET start_time = $2",
                        roblox_id, datetime.datetime.now(datetime.timezone.utc)
                    )
            elif status == "left":
                async with self.db_pool.acquire() as connection:
                    session_start = await connection.fetchval("SELECT start_time FROM roblox_sessions WHERE roblox_id = $1", roblox_id)
                    if session_start:
                        await connection.execute("DELETE FROM roblox_sessions WHERE roblox_id = $1", roblox_id)
                        duration = (datetime.datetime.now(datetime.timezone.utc) - session_start).total_seconds()
                        
                        await connection.execute(
                            "INSERT INTO roblox_time (member_id, time_spent) VALUES ($1, $2) ON CONFLICT (member_id) DO UPDATE SET time_spent = roblox_time.time_spent + $2",
                            discord_id, int(duration)
                        )
                        new_total_time = await connection.fetchval("SELECT time_spent FROM roblox_time WHERE member_id = $1", discord_id)
                        
                        try:
                            activity_log_channel = await self.fetch_channel(ACTIVITY_LOG_CHANNEL_ID)
                            if activity_log_channel:
                                member = await activity_log_channel.guild.fetch_member(discord_id)
                                if member:
                                    embed = discord.Embed(
                                        title="Roblox Activity Logged",
                                        description=f"**{member.display_name}** was on-site for **{int(duration // 60)} minutes**.",
                                        color=discord.Color.blue(),
                                        timestamp=datetime.datetime.now(datetime.timezone.utc)
                                    )
                                    embed.set_footer(text=f"Total on-site time this week: {int(new_total_time // 60)} minutes")
                                    await activity_log_channel.send(embed=embed)
                        except Exception as e:
                            print(f"Error sending activity log: {e}")
        return web.Response(status=200)


bot = MD_BOT()

# --- Helper Function for Long Messages ---
def smart_chunk(text, size=4000):
    """Splits text into chunks of a given size without breaking words or lines."""
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
    """A helper function to send long messages by splitting them into multiple embeds."""
    chunks = smart_chunk(description)
    embed = discord.Embed(
        title=title,
        description=chunks[0],
        color=color,
        timestamp=datetime.datetime.now(datetime.timezone.utc)
    )
    if footer_text:
        embed.set_footer(text=footer_text)
    if author_name:
        embed.set_author(name=author_name, icon_url=author_icon_url)
    if image_url:
        embed.set_image(url=image_url)
    await target.send(embed=embed)
    if len(chunks) > 1:
        for i, chunk in enumerate(chunks[1:]):
            follow_up_embed = discord.Embed(
                description=chunk,
                color=color
            ).set_footer(text=f"Part {i+2}/{len(chunks)}")
            await target.send(embed=follow_up_embed)


# --- Modal for Task Logging ---
class LogTaskForm(discord.ui.Modal, title='Log a New Task'):
    def __init__(self, proof: discord.Attachment):
        super().__init__()
        self.proof = proof
    task_name = discord.ui.TextInput(label='Task Name', placeholder='What task did you complete?', style=discord.TextStyle.short, required=True)
    comments = discord.ui.TextInput(label='Comments', placeholder='Any additional comments?', style=discord.TextStyle.long, required=False)
    async def on_submit(self, interaction: discord.Interaction):
        log_channel = bot.get_channel(LOG_CHANNEL_ID)
        if not log_channel:
            await interaction.response.send_message("Log channel not found.", ephemeral=True)
            return
        member_id = interaction.user.id
        task_str = self.task_name.value
        comments_str = self.comments.value or "No comments"
        async with bot.db_pool.acquire() as connection:
            await connection.execute("INSERT INTO task_logs (member_id, task, proof_url, comments, timestamp) VALUES ($1, $2, $3, $4, $5)", member_id, task_str, self.proof.url, comments_str, datetime.datetime.now(datetime.timezone.utc))
            await connection.execute("INSERT INTO weekly_tasks (member_id, tasks_completed) VALUES ($1, 1) ON CONFLICT (member_id) DO UPDATE SET tasks_completed = weekly_tasks.tasks_completed + 1", member_id)
            tasks_completed = await connection.fetchval("SELECT tasks_completed FROM weekly_tasks WHERE member_id = $1", member_id)
        full_description = f"**Task:** {task_str}\n\n**Comments:**\n{comments_str}"
        await send_long_embed(target=log_channel, title="✅ Task Logged", description=full_description, color=discord.Color.from_rgb(0, 255, 127), footer_text=f"Member ID: {member_id}", author_name=interaction.user.display_name, author_icon_url=interaction.user.avatar.url, image_url=self.proof.url)
        await interaction.response.send_message(f"Your task has been logged! You have completed {tasks_completed} task(s) this week.", ephemeral=True)
    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await interaction.response.send_message("Oops! Something went wrong.", ephemeral=True)
        print(error)


# --- Bot Events ---
@bot.event
async def on_ready():
    """Event that runs when the bot is connected and ready."""
    print(f'Logged in as {bot.user.name}')
    print(f'Discord.py Version: {discord.__version__}')
    check_weekly_tasks.start()

# --- Commands ---

# Verify Command
@bot.tree.command(name="verify", description="Link your Roblox account to the bot.")
async def verify(interaction: discord.Interaction, roblox_username: str):
    """Links a user's Roblox account to their Discord account."""
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
                            "INSERT INTO roblox_verification (discord_id, roblox_id) VALUES ($1, $2) ON CONFLICT (discord_id) DO UPDATE SET roblox_id = $2",
                            interaction.user.id, roblox_id
                        )
                    await interaction.response.send_message(f"Successfully verified as {roblox_name}!", ephemeral=True)
                else:
                    await interaction.response.send_message("Could not find a Roblox user with that name.", ephemeral=True)
            else:
                await interaction.response.send_message("There was an error looking up the Roblox user.", ephemeral=True)

# Announce Command
@bot.tree.command(name="announce", description="Make an announcement in the designated channel.")
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
async def announce(interaction: discord.Interaction, title: str, message: str, color: str = "blue"):
    announcement_channel = bot.get_channel(ANNOUNCEMENT_CHANNEL_ID)
    if not announcement_channel:
        await interaction.response.send_message("Announcement channel not found.", ephemeral=True)
        return
    color_obj = getattr(discord.Color, color, discord.Color.blue)()
    description = message.replace('\\n', '\n')
    await send_long_embed(target=announcement_channel, title=f"📢 {title}", description=description, color=color_obj, footer_text=f"Announcement by {interaction.user.display_name}")
    await interaction.response.send_message("Announcement sent successfully!", ephemeral=True)

@announce.error
async def announce_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingRole):
        await interaction.response.send_message(f"Sorry, you don't have the required role.", ephemeral=True)
    else:
        await interaction.response.send_message("An error occurred.", ephemeral=True)
        print(error)

# Log Command
@bot.tree.command(name="log", description="Log a completed task with proof.")
async def log(interaction: discord.Interaction, proof: discord.Attachment):
    await interaction.response.send_modal(LogTaskForm(proof=proof))

# MyTasks Command
@bot.tree.command(name="mytasks", description="Check how many tasks you have completed this week.")
async def mytasks(interaction: discord.Interaction):
    member_id = interaction.user.id
    async with bot.db_pool.acquire() as connection:
        tasks_completed = await connection.fetchval("SELECT tasks_completed FROM weekly_tasks WHERE member_id = $1", member_id) or 0
        time_spent_seconds = await connection.fetchval("SELECT time_spent FROM roblox_time WHERE member_id = $1", member_id) or 0
    time_spent_minutes = time_spent_seconds // 60
    await interaction.response.send_message(f"You have completed **{tasks_completed}** of **{WEEKLY_REQUIREMENT}** tasks and have **{time_spent_minutes}** of **{WEEKLY_TIME_REQUIREMENT}** minutes on-site.", ephemeral=True)

# Leaderboard Command
@bot.tree.command(name="leaderboard", description="Displays the weekly task leaderboard.")
async def leaderboard(interaction: discord.Interaction):
    async with bot.db_pool.acquire() as connection:
        sorted_users = await connection.fetch("SELECT member_id, tasks_completed FROM weekly_tasks ORDER BY tasks_completed DESC LIMIT 10")
    if not sorted_users:
        await interaction.response.send_message("No tasks logged this week.", ephemeral=True)
        return
    embed = discord.Embed(title="🏆 Weekly Task Leaderboard", color=discord.Color.gold(), timestamp=datetime.datetime.now(datetime.timezone.utc))
    description = ""
    for i, record in enumerate(sorted_users):
        member = interaction.guild.get_member(record['member_id'])
        member_name = member.display_name if member else f"Unknown User ({record['member_id']})"
        rank_emoji = ["🥇", "🥈", "🥉"]
        if i < 3:
            description += f"{rank_emoji[i]} **{member_name}** - {record['tasks_completed']} tasks\n"
        else:
            description += f"**{i+1}.** {member_name} - {record['tasks_completed']} tasks\n"
    embed.description = description
    await interaction.response.send_message(embed=embed)

# Remove Last Log Command
@bot.tree.command(name="removelastlog", description="Removes the last logged task for a member.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def removelastlog(interaction: discord.Interaction, member: discord.Member):
    member_id = member.id
    async with bot.db_pool.acquire() as connection:
        async with connection.transaction():
            last_log = await connection.fetchrow("SELECT log_id, task FROM task_logs WHERE member_id = $1 ORDER BY timestamp DESC LIMIT 1", member_id)
            if not last_log:
                await interaction.response.send_message(f"{member.display_name} has no tasks logged.", ephemeral=True)
                return
            await connection.execute("DELETE FROM task_logs WHERE log_id = $1", last_log['log_id'])
            await connection.execute("UPDATE weekly_tasks SET tasks_completed = tasks_completed - 1 WHERE member_id = $1", member_id)
            new_count = await connection.fetchval("SELECT tasks_completed FROM weekly_tasks WHERE member_id = $1", member_id)
    await interaction.response.send_message(f"Removed last task for {member.mention}: '{last_log['task']}'. They now have {new_count} tasks.", ephemeral=True)

@removelastlog.error
async def removelastlog_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingRole):
        await interaction.response.send_message("You do not have the required role.", ephemeral=True)
    else:
        await interaction.response.send_message("An error occurred.", ephemeral=True)
        print(error)

# Welcome Command
@bot.tree.command(name="welcome", description="Sends the official welcome message.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def welcome(interaction: discord.Interaction):
    welcome_message = (
        "Welcome to the Medical Department! We're super excited to have you join us. 😊 We know you're gonna be a great addition to the department! 🩺\n\n"
        "First things first, you have get your student orientation done within your first two weeks. 🗓️ No stress, it's easy! Just message any of the management team and they'll get you scheduled for one. ✔️\n\n"
        "👉 Before you jump in, make sure you read through our MD Info Hub on Trello. 🧠 It's got all the important stuff about what you can and can't do.\n\n"
        "Trello Link: https://trello.com/b/j2jvme4Z/md-information-hub\n\n"
        "We're happy to have you here! If you have any questions, just ask. Welcome aboard! 🚀"
    )
    embed = discord.Embed(title="Welcome to the Team!", description=welcome_message, color=discord.Color.green())
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

# DM Command
@bot.tree.command(name="dm", description="Sends a direct message to a member.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def dm(interaction: discord.Interaction, member: discord.Member, title: str, message: str):
    if member.bot:
        await interaction.response.send_message("You can't send messages to bots!", ephemeral=True)
        return
    description = message.replace('\\n', '\n')
    try:
        await send_long_embed(target=member, title=f"💌 {title}", description=description, color=discord.Color.magenta(), footer_text=f"A special message from {interaction.guild.name}")
        await interaction.response.send_message(f"Your message has been sent to {member.mention}!", ephemeral=True)
    except discord.Forbidden:
        await interaction.response.send_message(f"I couldn't message {member.mention}. They might have DMs disabled.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message("An unexpected error occurred.", ephemeral=True)
        print(f"DM command error: {e}")

@dm.error
async def dm_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingRole):
        await interaction.response.send_message("You do not have the required role.", ephemeral=True)
    else:
        await interaction.response.send_message("An error occurred.", ephemeral=True)
        print(error)

# Meme Command
@bot.tree.command(name="meme", description="Fetches a random meme.")
async def meme(interaction: discord.Interaction):
    await interaction.response.defer()
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("https://meme-api.com/gimme") as response:
                if response.status == 200:
                    data = await response.json()
                    embed = discord.Embed(title=data['title'], url=data['postLink'], color=discord.Color.random())
                    embed.set_image(url=data['url'])
                    embed.set_footer(text=f"From r/{data['subreddit']}")
                    await interaction.followup.send(embed=embed)
                else:
                    await interaction.followup.send("Could not fetch a meme.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send("An error occurred while fetching a meme.", ephemeral=True)
            print(f"Meme command error: {e}")

# --- NEW: /aa command (Anomaly Actors ping) ---
@bot.tree.command(name="aa", description="Ping Anomaly Actors to get on-site for a checkup.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)  # Only management can use
@app_commands.checks.cooldown(1, 300.0, key=BucketType.user)  # 5 min per-user cooldown
async def aa(interaction: discord.Interaction, note: str | None = None):
    """
    Pings the Anomaly Actors role in the specified channel to get on-site for a checkup.
    Optional 'note' lets the caller add a short extra message.
    """
    target_channel = bot.get_channel(AA_CHANNEL_ID)
    if not target_channel:
        await interaction.response.send_message("Could not find the AA announcement channel.", ephemeral=True)
        return

    role = interaction.guild.get_role(ANOMALY_ACTORS_ROLE_ID)
    if not role:
        await interaction.response.send_message("Could not find the Anomaly Actors role.", ephemeral=True)
        return

    title = "🧪 Anomaly Actors Checkup Call"
    body_lines = [
        f"{role.mention}, please get on-site for a quick **Anomaly Actors checkup**.",
        "Check the radio for further instructions."
    ]
    if note:
        body_lines.append(f"\n**Note:** {note}")

    embed = discord.Embed(
        title=title,
        description="\n".join(body_lines),
        color=discord.Color.purple(),
        timestamp=datetime.datetime.now(datetime.timezone.utc)
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
        await interaction.response.send_message(
            f"⏳ You can use this command again in **{pretty}**.",
            ephemeral=True
        )
    else:
        await interaction.response.send_message("An unexpected error occurred.", ephemeral=True)
        print(f"/aa error: {error}")

# --- Weekly Task Checking ---
@tasks.loop(time=datetime.time(hour=4, minute=0, tzinfo=datetime.timezone.utc))
async def check_weekly_tasks():
    if datetime.datetime.now(datetime.timezone.utc).weekday() == 6:
        announcement_channel = bot.get_channel(ANNOUNCEMENT_CHANNEL_ID)
        if not announcement_channel:
            print("Weekly check failed: Announcement channel not found.")
            return
        async with bot.db_pool.acquire() as connection:
            all_tasks = await connection.fetch("SELECT member_id, tasks_completed FROM weekly_tasks")
            all_time = await connection.fetch("SELECT member_id, time_spent FROM roblox_time")
            
            tasks_map = {record['member_id']: record['tasks_completed'] for record in all_tasks}
            time_map = {record['member_id']: record['time_spent'] for record in all_time}

            all_member_ids = set(tasks_map.keys()) | set(time_map.keys())
            
            members_met_req, members_not_met_req, members_with_zero_activity = [], [], []

            for member_id in all_member_ids:
                member = announcement_channel.guild.get_member(member_id)
                if member:
                    tasks_done = tasks_map.get(member_id, 0)
                    time_done_seconds = time_map.get(member_id, 0)
                    time_done_minutes = time_done_seconds // 60

                    if tasks_done >= WEEKLY_REQUIREMENT and time_done_minutes >= WEEKLY_TIME_REQUIREMENT:
                        members_met_req.append(member.mention)
                    else:
                        members_not_met_req.append(f"{member.mention} ({tasks_done}/{WEEKLY_REQUIREMENT} tasks, {time_done_minutes}/{WEEKLY_TIME_REQUIREMENT} mins)")

            all_guild_members = announcement_channel.guild.members
            for member in all_guild_members:
                if not member.bot and member.id not in all_member_ids:
                    members_with_zero_activity.append(member.mention)

            summary_message = "--- Weekly Task Report ---\n\n"
            if members_met_req:
                summary_message += f"**✅ Met Requirement ({len(members_met_req)}):**\n" + ", ".join(members_met_req) + "\n\n"
            if members_not_met_req:
                summary_message += f"**❌ Below Quota ({len(members_not_met_req)}):**\n" + "\n".join(members_not_met_req) + "\n\n"
            if members_with_zero_activity:
                summary_message += f"**🚫 0 Activity ({len(members_with_zero_activity)}):**\n" + ", ".join(members_with_zero_activity) + "\n\n"
            if not all_member_ids:
                 summary_message += "**No activity was logged this week.**\n\n"
            summary_message += "Task and time counts have now been reset for the new week."
            
            await send_long_embed(target=announcement_channel, title="Weekly Task Summary", description=summary_message, color=discord.Color.gold(), footer_text=None)
            await connection.execute("TRUNCATE TABLE weekly_tasks, task_logs, roblox_time, roblox_sessions")
            print("Weekly tasks and time checked and reset.")

@check_weekly_tasks.before_loop
async def before_check():
    await bot.wait_until_ready()

# --- Running the Bot ---
if __name__ == "__main__":
    bot.run(BOT_TOKEN)
