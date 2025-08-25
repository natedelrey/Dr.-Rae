import discord
from discord.ext import commands, tasks
import os
from dotenv import load_dotenv
import datetime
import random
import aiohttp
import asyncpg

# --- Configuration ---
# Load environment variables from a .env file
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ANNOUNCEMENT_CHANNEL_ID = int(os.getenv("ANNOUNCEMENT_CHANNEL_ID"))
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID"))
WEEKLY_REQUIREMENT = 3
ANNOUNCEMENT_ROLE_ID = int(os.getenv("ANNOUNCEMENT_ROLE_ID"))
MANAGEMENT_ROLE_ID = int(os.getenv("MANAGEMENT_ROLE_ID"))
DATABASE_URL = os.getenv("DATABASE_URL")


# --- Bot Setup ---
# Define the intents your bot needs.
intents = discord.Intents.default()
intents.members = True
intents.message_content = True # Added privileged intent

class MD_BOT(commands.Bot):
    def __init__(self):
        # Using a prefix that won't conflict with slash commands
        super().__init__(command_prefix='!', intents=intents)
        self.db_pool = None

    async def setup_hook(self):
        # Create a database connection pool
        try:
            self.db_pool = await asyncpg.create_pool(DATABASE_URL)
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
        print("Database tables are ready.")
        # Synchronize the slash commands with Discord
        try:
            synced = await self.tree.sync()
            print(f"Synced {len(synced)} command(s)")
        except Exception as e:
            print(f"Failed to sync commands: {e}")


bot = MD_BOT()

# --- Helper Function for Long Messages ---
async def send_long_embed(target, title, description, color, footer_text):
    """A helper function to send long messages by splitting them into multiple embeds."""
    chunks = [description[i:i + 4000] for i in range(0, len(description), 4000)]
    
    # First embed with title and footer
    embed = discord.Embed(
        title=title,
        description=chunks[0],
        color=color,
        timestamp=datetime.datetime.now(datetime.timezone.utc)
    )
    if footer_text:
        embed.set_footer(text=footer_text)
    
    await target.send(embed=embed)

    # Subsequent embeds for the rest of the message
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

    task_name = discord.ui.TextInput(
        label='Task Name',
        placeholder='What task did you complete?',
        style=discord.TextStyle.short,
        required=True
    )

    comments = discord.ui.TextInput(
        label='Comments',
        placeholder='Any additional comments?',
        style=discord.TextStyle.long,
        required=False
    )

    async def on_submit(self, interaction: discord.Interaction):
        log_channel = bot.get_channel(LOG_CHANNEL_ID)
        if not log_channel:
            await interaction.response.send_message("Log channel not found.", ephemeral=True)
            return

        member_id = interaction.user.id
        task_str = self.task_name.value
        comments_str = self.comments.value or "No comments"

        if len(comments_str) > 1024:
            comments_str = comments_str[:1021] + "..."

        async with bot.db_pool.acquire() as connection:
            await connection.execute(
                "INSERT INTO task_logs (member_id, task, proof_url, comments, timestamp) VALUES ($1, $2, $3, $4, $5)",
                member_id, task_str, self.proof.url, comments_str, datetime.datetime.now(datetime.timezone.utc)
            )
            await connection.execute(
                "INSERT INTO weekly_tasks (member_id, tasks_completed) VALUES ($1, 1) ON CONFLICT (member_id) DO UPDATE SET tasks_completed = weekly_tasks.tasks_completed + 1",
                member_id
            )
            tasks_completed = await connection.fetchval("SELECT tasks_completed FROM weekly_tasks WHERE member_id = $1", member_id)

        embed = discord.Embed(
            title="✅ Task Logged",
            description=f"**Task:** {task_str}",
            color=discord.Color.from_rgb(0, 255, 127),
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.avatar.url)
        embed.add_field(name="Comments", value=comments_str, inline=False)
        embed.set_image(url=self.proof.url)
        embed.set_footer(text=f"Member ID: {member_id}")

        await log_channel.send(embed=embed)
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

# Announce Command
@bot.tree.command(name="announce", description="Make an announcement in the designated channel.")
@discord.app_commands.checks.has_role(ANNOUNCEMENT_ROLE_ID)
@discord.app_commands.choices(color=[
    discord.app_commands.Choice(name="Blue", value="blue"),
    discord.app_commands.Choice(name="Green", value="green"),
    discord.app_commands.Choice(name="Red", value="red"),
    discord.app_commands.Choice(name="Yellow", value="yellow"),
    discord.app_commands.Choice(name="Purple", value="purple"),
    discord.app_commands.Choice(name="Orange", value="orange"),
    discord.app_commands.Choice(name="Gold", value="gold"),
])
async def announce(interaction: discord.Interaction, title: str, message: str, color: str = "blue"):
    announcement_channel = bot.get_channel(ANNOUNCEMENT_CHANNEL_ID)
    if not announcement_channel:
        await interaction.response.send_message("Announcement channel not found.", ephemeral=True)
        return

    color_obj = getattr(discord.Color, color, discord.Color.blue)()
    description = message.replace('\\n', '\n')
    
    await send_long_embed(
        target=announcement_channel,
        title=f"📢 {title}",
        description=description,
        color=color_obj,
        footer_text=f"Announcement by {interaction.user.display_name}"
    )

    await interaction.response.send_message("Announcement sent successfully!", ephemeral=True)

@announce.error
async def announce_error(interaction: discord.Interaction, error: discord.app_commands.AppCommandError):
    if isinstance(error, discord.app_commands.MissingRole):
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
    await interaction.response.send_message(f"You have completed **{tasks_completed}** of **{WEEKLY_REQUIREMENT}** tasks.", ephemeral=True)

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
@discord.app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
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
async def removelastlog_error(interaction: discord.Interaction, error: discord.app_commands.AppCommandError):
    if isinstance(error, discord.app_commands.MissingRole):
        await interaction.response.send_message("You do not have the required role.", ephemeral=True)
    else:
        await interaction.response.send_message("An error occurred.", ephemeral=True)
        print(error)

# Welcome Command
@bot.tree.command(name="welcome", description="Sends the official welcome message.")
@discord.app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
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
async def welcome_error(interaction: discord.Interaction, error: discord.app_commands.AppCommandError):
    if isinstance(error, discord.app_commands.MissingRole):
        await interaction.response.send_message("You do not have the required role.", ephemeral=True)
    else:
        await interaction.response.send_message("An error occurred.", ephemeral=True)
        print(error)

# DM Command
@bot.tree.command(name="dm", description="Sends a direct message to a member.")
@discord.app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
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

@dm.error
async def dm_error(interaction: discord.Interaction, error: discord.app_commands.AppCommandError):
    if isinstance(error, discord.app_commands.MissingRole):
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
            await interaction.followup.send("An error occurred.", ephemeral=True)
            print(f"Meme command error: {e}")

# --- Weekly Task Checking ---
@tasks.loop(time=datetime.time(hour=4, minute=0, tzinfo=datetime.timezone.utc))
async def check_weekly_tasks():
    if datetime.datetime.now(datetime.timezone.utc).weekday() == 6:
        announcement_channel = bot.get_channel(ANNOUNCEMENT_CHANNEL_ID)
        if not announcement_channel:
            print("Weekly check failed: Announcement channel not found.")
            return

        async with bot.db_pool.acquire() as connection:
            all_tasks_records = await connection.fetch("SELECT member_id, tasks_completed FROM weekly_tasks")
            
            members_met_req, members_not_met_req, logged_member_ids = [], [], set()
            for record in all_tasks_records:
                logged_member_ids.add(record['member_id'])
                member = announcement_channel.guild.get_member(record['member_id'])
                if member:
                    if record['tasks_completed'] >= WEEKLY_REQUIREMENT:
                        members_met_req.append(member.mention)
                    else:
                        members_not_met_req.append(f"{member.mention} ({record['tasks_completed']}/{WEEKLY_REQUIREMENT})")

            members_with_zero_tasks = [m.mention for m in announcement_channel.guild.members if not m.bot and m.id not in logged_member_ids]

            summary_message = "--- Weekly Task Report ---\n\n"
            if members_met_req:
                summary_message += f"**✅ Met Requirement ({len(members_met_req)}):**\n" + ", ".join(members_met_req) + "\n\n"
            if members_not_met_req:
                summary_message += f"**❌ Below Quota ({len(members_not_met_req)}):**\n" + "\n".join(members_not_met_req) + "\n\n"
            if members_with_zero_tasks:
                summary_message += f"**🚫 0 Tasks Logged ({len(members_with_zero_tasks)}):**\n" + ", ".join(members_with_zero_tasks) + "\n\n"
            if not all_tasks_records:
                 summary_message += "**No tasks were logged this week.**\n\n"
            summary_message += "Task counts have now been reset for the new week."
            
            await send_long_embed(
                target=announcement_channel,
                title="Weekly Task Summary",
                description=summary_message,
                color=discord.Color.gold(),
                footer_text=None
            )

            await connection.execute("TRUNCATE TABLE weekly_tasks, task_logs")
            print("Weekly tasks checked and reset.")

@check_weekly_tasks.before_loop
async def before_check():
    await bot.wait_until_ready()

# --- Running the Bot ---
if __name__ == "__main__":
    bot.run(BOT_TOKEN)
