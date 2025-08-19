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

class MD_BOT(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix='/', intents=intents)
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

# --- Bot Events ---
@bot.event
async def on_ready():
    """Event that runs when the bot is connected and ready."""
    print(f'Logged in as {bot.user.name}')
    print(f'Discord.py Version: {discord.__version__}')
    # Start the weekly task check loop.
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
    """
    Creates and sends an announcement embed.
    Only members with the ANNOUNCEMENT_ROLE_ID can use this.
    """
    announcement_channel = bot.get_channel(ANNOUNCEMENT_CHANNEL_ID)
    if not announcement_channel:
        await interaction.response.send_message("Announcement channel not found. Please check the `ANNOUNCEMENT_CHANNEL_ID`.", ephemeral=True)
        return

    color_obj = getattr(discord.Color, color, discord.Color.blue)()

    embed = discord.Embed(
        title=f"📢 {title}",
        description=message,
        color=color_obj,
        timestamp=datetime.datetime.now(datetime.timezone.utc)
    )
    embed.set_footer(text=f"Announcement by {interaction.user.display_name}")

    await announcement_channel.send(embed=embed)
    await interaction.response.send_message("Announcement sent successfully!", ephemeral=True)

@announce.error
async def announce_error(interaction: discord.Interaction, error: discord.app_commands.AppCommandError):
    if isinstance(error, discord.app_commands.MissingRole):
        await interaction.response.send_message(f"Sorry, you don't have the required role to use this command.", ephemeral=True)
    else:
        await interaction.response.send_message("An error occurred while trying to send the announcement.", ephemeral=True)
        print(error)

# Log Command
@bot.tree.command(name="log", description="Log a completed task with proof and comments.")
async def log(interaction: discord.Interaction, task: str, proof: discord.Attachment, comments: str = "No comments"):
    log_channel = bot.get_channel(LOG_CHANNEL_ID)
    if not log_channel:
        await interaction.response.send_message("Log channel not found.", ephemeral=True)
        return

    member_id = interaction.user.id
    async with bot.db_pool.acquire() as connection:
        # Add the log entry
        await connection.execute(
            "INSERT INTO task_logs (member_id, task, proof_url, comments, timestamp) VALUES ($1, $2, $3, $4, $5)",
            member_id, task, proof.url, comments, datetime.datetime.now(datetime.timezone.utc)
        )
        # Update the weekly task count
        await connection.execute(
            "INSERT INTO weekly_tasks (member_id, tasks_completed) VALUES ($1, 1) ON CONFLICT (member_id) DO UPDATE SET tasks_completed = weekly_tasks.tasks_completed + 1",
            member_id
        )
        tasks_completed = await connection.fetchval("SELECT tasks_completed FROM weekly_tasks WHERE member_id = $1", member_id)

    embed = discord.Embed(
        title="✅ Task Logged",
        description=f"**Task:** {task}",
        color=discord.Color.from_rgb(0, 255, 127),
        timestamp=datetime.datetime.now(datetime.timezone.utc)
    )
    embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.avatar.url)
    embed.add_field(name="Comments", value=comments, inline=False)
    embed.set_image(url=proof.url)
    embed.set_footer(text=f"Member ID: {member_id}")

    await log_channel.send(embed=embed)
    await interaction.response.send_message(f"Your task has been logged! You have completed {tasks_completed} task(s) this week.", ephemeral=True)

# MyTasks Command
@bot.tree.command(name="mytasks", description="Check how many tasks you have completed this week.")
async def mytasks(interaction: discord.Interaction):
    member_id = interaction.user.id
    async with bot.db_pool.acquire() as connection:
        tasks_completed = await connection.fetchval("SELECT tasks_completed FROM weekly_tasks WHERE member_id = $1", member_id) or 0
    await interaction.response.send_message(f"You have completed **{tasks_completed}** out of **{WEEKLY_REQUIREMENT}** required tasks this week.", ephemeral=True)

# Leaderboard Command
@bot.tree.command(name="leaderboard", description="Displays the weekly task leaderboard.")
async def leaderboard(interaction: discord.Interaction):
    async with bot.db_pool.acquire() as connection:
        sorted_users = await connection.fetch("SELECT member_id, tasks_completed FROM weekly_tasks ORDER BY tasks_completed DESC LIMIT 10")

    if not sorted_users:
        await interaction.response.send_message("No tasks have been logged this week.", ephemeral=True)
        return

    embed = discord.Embed(title="🏆 Weekly Task Leaderboard", color=discord.Color.gold(), timestamp=datetime.datetime.now(datetime.timezone.utc))
    description = ""
    for i, record in enumerate(sorted_users):
        member_id = record['member_id']
        tasks_completed = record['tasks_completed']
        member = interaction.guild.get_member(member_id)
        member_name = member.display_name if member else f"Unknown User ({member_id})"
        
        rank_emoji = ["🥇", "🥈", "🥉"]
        if i < 3:
            description += f"{rank_emoji[i]} **{member_name}** - {tasks_completed} tasks\n"
        else:
            description += f"**{i+1}.** {member_name} - {tasks_completed} tasks\n"

    embed.description = description
    await interaction.response.send_message(embed=embed)

# Remove Last Log Command
@bot.tree.command(name="removelastlog", description="Removes the last logged task for a member.")
@discord.app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def removelastlog(interaction: discord.Interaction, member: discord.Member):
    member_id = member.id
    async with bot.db_pool.acquire() as connection:
        async with connection.transaction():
            # Find the last log for the member
            last_log = await connection.fetchrow("SELECT log_id, task FROM task_logs WHERE member_id = $1 ORDER BY timestamp DESC LIMIT 1", member_id)
            if not last_log:
                await interaction.response.send_message(f"{member.display_name} has no tasks logged.", ephemeral=True)
                return

            # Delete the log
            await connection.execute("DELETE FROM task_logs WHERE log_id = $1", last_log['log_id'])
            # Decrement the task count
            await connection.execute("UPDATE weekly_tasks SET tasks_completed = tasks_completed - 1 WHERE member_id = $1", member_id)
            
            new_count = await connection.fetchval("SELECT tasks_completed FROM weekly_tasks WHERE member_id = $1", member_id)

    await interaction.response.send_message(f"Successfully removed the last task for {member.mention}: '{last_log['task']}'. They now have {new_count} task(s) logged.", ephemeral=True)

@removelastlog.error
async def removelastlog_error(interaction: discord.Interaction, error: discord.app_commands.AppCommandError):
    if isinstance(error, discord.app_commands.MissingRole):
        await interaction.response.send_message("You do not have the required role for this command.", ephemeral=True)
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

# --- Weekly Task Checking ---
@tasks.loop(hours=24)
async def check_weekly_tasks():
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    if now_utc.weekday() == 5 and now_utc.hour >= 23:
        announcement_channel = bot.get_channel(ANNOUNCEMENT_CHANNEL_ID)
        if not announcement_channel:
            print("Weekly check failed: Announcement channel not found.")
            return

        async with bot.db_pool.acquire() as connection:
            all_tasks = await connection.fetch("SELECT member_id, tasks_completed FROM weekly_tasks")
            
            members_met_req = []
            members_not_met_req = []

            for record in all_tasks:
                member = announcement_channel.guild.get_member(record['member_id'])
                if member:
                    if record['tasks_completed'] >= WEEKLY_REQUIREMENT:
                        members_met_req.append(member.mention)
                    else:
                        members_not_met_req.append(f"{member.mention} ({record['tasks_completed']}/{WEEKLY_REQUIREMENT})")

            summary_message = "--- Weekly Task Report ---\n\n"
            if members_met_req:
                summary_message += f"**✅ Members who met the requirement ({len(members_met_req)}):**\n" + ", ".join(members_met_req) + "\n\n"
            if members_not_met_req:
                summary_message += f"**❌ Members who did not meet the requirement ({len(members_not_met_req)}):**\n" + "\n".join(members_not_met_req) + "\n\n"
            
            if not members_met_req and not members_not_met_req:
                 summary_message += "**No tasks were logged this week.**\n\n"

            summary_message += "Task counts have now been reset for the new week."
            report_embed = discord.Embed(title="Weekly Task Summary", description=summary_message, color=discord.Color.gold(), timestamp=now_utc)
            await announcement_channel.send(embed=report_embed)

            # Reset tasks for the new week
            await connection.execute("TRUNCATE TABLE weekly_tasks, task_logs")
            print("Weekly tasks checked and reset.")

@check_weekly_tasks.before_loop
async def before_check():
    await bot.wait_until_ready()

# --- Running the Bot ---
if __name__ == "__main__":
    bot.run(BOT_TOKEN)
