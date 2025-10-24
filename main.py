# main.py  (Medical Department bot) — PART 1/3
# NOTE: Paste this as the top of your main.py. I will send Parts 2 and 3 after you reply "next".

import discord
import discord.abc
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
import json
import re
from typing import Optional, Any
from discord.utils import escape_markdown

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
DEPARTMENT_ROLE_ID           = getenv_int("DEPARTMENT_ROLE_ID")
MEDICAL_STUDENT_ROLE_ID      = getenv_int("MEDICAL_STUDENT_ROLE_ID")
ORIENTATION_ALERT_CHANNEL_ID = getenv_int("ORIENTATION_ALERT_CHANNEL_ID")
COMMAND_LOG_CHANNEL_ID       = getenv_int("COMMAND_LOG_CHANNEL_ID", 1416965696230789150)
ACTIVITY_LOG_CHANNEL_ID      = getenv_int("ACTIVITY_LOG_CHANNEL_ID", 1409646416829354095)
COMMS_CHANNEL_ID             = getenv_int("COMMS_CHANNEL_ID")
APPLICATION_MANAGEMENT_CHANNEL_ID = 1405988167982649436
GUIDELINES_CHANNEL_ID        = getenv_int("GUIDELINES_CHANNEL_ID")

# Extra roles to grant on successful application
APPLICATION_EXTRA_ROLE_IDS = [
    1405988543230382091,
    1405981117235990640,
]

# DB / API
DATABASE_URL   = os.getenv("DATABASE_URL")
API_SECRET_KEY = os.getenv("API_SECRET_KEY")  # for /roblox webhook auth

# AI (application review)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # use OpenAI-compatible endpoint
AI_MODEL       = os.getenv("AI_MODEL", "gpt-4o-mini")
AI_BASE_URL    = os.getenv("AI_BASE_URL", "https://api.openai.com/v1")
GUIDELINES_FILE = os.getenv("GUIDELINES_FILE", "resources/guidelines.json")

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
ROBLOX_MEDICAL_DIVISION_URL = "https://www.roblox.com/communities/695368604/SCPF-Medical-Division#!/about"

APPLICATION_PENDING_WARNING = (
    "⚠️ **Reminder:** Ensure you have a pending join request for the "
    f"[SCPF Medical Division Roblox group]({ROBLOX_MEDICAL_DIVISION_URL})."
)

# Roblox rank configuration for automatic onboarding
AUTO_ACCEPT_GROUP_ROLE_NAME   = os.getenv("AUTO_ACCEPT_GROUP_ROLE_NAME") or "Medical Student"

# Rank manager role (can run /rank)
RANK_MANAGER_ROLE_ID = getenv_int("RANK_MANAGER_ROLE_ID", 1405979816120942702)

# Staff role (can view/override application queue)
STAFF_ROLE_ID = getenv_int("STAFF_ROLE_ID", (MANAGEMENT_ROLE_ID or 0))

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

# === Application System Config ===
APPLICATION_AUTO_ACCEPT_THRESHOLD = float(os.getenv("APPLICATION_AUTO_ACCEPT_THRESHOLD", "55"))
APPLICATION_BORDERLINE_MIN        = float(os.getenv("APPLICATION_BORDERLINE_MIN", "30"))
APPLICATION_HARD_REJECT_THRESHOLD = float(os.getenv("APPLICATION_HARD_REJECT_THRESHOLD", "20"))
APPLICATION_TIMEOUT_MINUTES       = int(os.getenv("APPLICATION_TIMEOUT_MINUTES", "20"))  # idle per step
APPLICATION_COOLDOWN_HOURS        = int(os.getenv("APPLICATION_COOLDOWN_HOURS", "24"))   # after decision

# The core questions for the MD application (order matters)
APPLICATION_QUESTIONS: list[dict[str, Any]] = [
    {
        "code": "roblox_username",
        "prompt": "What is your exact Roblox username? (Case-sensitive, please double-check.)",
        "type": "short",
        "required": True,
        "min_len": 3,
        "max_len": 32
    },
    {
        "code": "availability",
        "prompt": "How many hours a week can you actively participate with the Medical Department? Be honest.",
        "type": "short",
        "required": True,
        "min_len": 1,
        "max_len": 200
    },
    {
        "code": "experience",
        "prompt": "List relevant experience: groups, roles, medical RP, or responsibilities you’ve handled.",
        "type": "long",
        "required": True,
        "min_len": 50,
        "max_len": 1200
    },
    {
        "code": "communication",
        "prompt": "Describe your communication style and how you handle conflicts.",
        "type": "long",
        "required": True,
        "min_len": 50,
        "max_len": 1200
    },
    {
        "code": "policy",
        "prompt": "Pick one MD guideline you find crucial and explain why it matters in practice.",
        "type": "long",
        "required": True,
        "min_len": 50,
        "max_len": 1200
    }
]

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


QUESTION_KEYWORDS = {
    "activity",
    "quota",
    "requirement",
    "requirements",
    "loa",
    "leave",
    "absence",
    "orientation",
    "checkup",
    "check-up",
    "clinic",
    "patient",
    "procedure",
    "surgery",
    "training",
    "rank",
    "medical",
    "department",
    "md",
    "duty",
    "shift",
    "log",
    "report",
    "guideline",
    "conduct",
    "evaluation",
    "promotion",
    "demotion",
    "weekly",
    "expectation",
    "expectations",
}

TROLL_KEYWORDS = {
    "kys",
    "kill yourself",
    "suicide",
    "sex",
    "porn",
    "nsfw",
    "idiot",
    "dumb",
    "stupid",
    "troll",
}


def looks_like_question(text: str) -> bool:
    lower = text.lower()
    if "?" in text:
        return True
    question_words = {
        "who",
        "what",
        "when",
        "where",
        "why",
        "how",
        "can",
        "does",
        "do",
        "should",
        "is",
        "are",
        "will",
        "could",
        "would",
        "may",
    }
    tokens = set(re.findall(r"[a-zA-Z]+", lower))
    return bool(tokens & question_words)


def is_probably_troll(text: str) -> bool:
    lower = text.lower()
    if len(lower) < 8:
        return True
    if sum(ch.isalpha() for ch in lower) < 4:
        return True
    return any(keyword in lower for keyword in TROLL_KEYWORDS)


class GuidelineStore:
    def __init__(self, path: str):
        self.path = path
        self.raw_text: str = ""
        self.sections: list[dict[str, str]] = []
        self.section_tokens: list[set[str]] = []
        self.default_context: str = ""
        self.loaded: bool = False
        self._load()

    def _load(self) -> None:
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                data = handle.read()
        except FileNotFoundError:
            print(f"[WARN] Guidelines file missing: {self.path}")
            return

        self.raw_text = data
        parsed_sections: list[dict[str, str]] = []
        try:
            loaded = json.loads(data)
        except json.JSONDecodeError:
            loaded = None

        if isinstance(loaded, dict):
            for key, value in loaded.items():
                text = ""
                if isinstance(value, str):
                    text = value.strip()
                elif isinstance(value, (list, dict)):
                    text = json.dumps(value, ensure_ascii=False, indent=2)
                if text:
                    parsed_sections.append({"title": str(key), "text": text})
        elif isinstance(loaded, list):
            for entry in loaded:
                if isinstance(entry, dict):
                    title = str(entry.get("title") or entry.get("name") or "Section")
                    text = entry.get("text") or entry.get("content")
                    if isinstance(text, str) and text.strip():
                        parsed_sections.append({"title": title, "text": text.strip()})

        if not parsed_sections:
            # Treat as raw plaintext/markdown; build sections around headings so
            # the bullets that follow stay grouped with their parent header.
            def is_heading(line: str) -> bool:
                stripped = line.strip()
                if not stripped or stripped.startswith("---"):
                    return False
                if stripped.startswith("PART "):
                    return True
                if stripped.startswith("§"):
                    return True
                if stripped.startswith("LEVEL "):
                    return True
                # Lines that are all caps (ignoring punctuation) are often
                # headings in the handbook.
                alpha = re.sub(r"[^A-Z]", "", stripped.upper())
                return bool(alpha) and stripped == stripped.upper()

            sections: list[dict[str, str]] = []
            current_title: str | None = None
            current_lines: list[str] = []

            def flush_section() -> None:
                nonlocal current_title, current_lines
                if not current_lines:
                    current_title = None
                    return
                text = "\n".join(line.rstrip() for line in current_lines).strip()
                if not text:
                    current_title = None
                    current_lines = []
                    return
                title = current_title or text.splitlines()[0].strip()
                sections.append({"title": title, "text": text})
                current_title = None
                current_lines = []

            for raw_line in self.raw_text.splitlines():
                if raw_line.strip().startswith("---"):
                    flush_section()
                    continue
                if is_heading(raw_line):
                    flush_section()
                    current_title = raw_line.strip()
                    current_lines = [raw_line]
                    continue
                if not raw_line.strip():
                    if current_lines and current_lines[-1] != "":
                        current_lines.append("")
                    continue
                if not current_lines:
                    current_title = raw_line.strip()
                    current_lines = [raw_line]
                else:
                    current_lines.append(raw_line)

            flush_section()
            parsed_sections = sections

        self.sections = parsed_sections
        self.section_tokens = []
        for section in self.sections:
            tokens = set(re.findall(r"[a-zA-Z]{3,}", f"{section['title']}\n{section['text']}".lower()))
            self.section_tokens.append(tokens)

        default_parts: list[str] = []
        total = 0
        for section in self.sections[:5]:
            text = section["text"].strip()
            if not text:
                continue
            default_parts.append(text)
            total += len(text)
            if total > 2500:
                break
        self.default_context = "\n\n".join(default_parts)[:3000]
        self.loaded = bool(self.sections)

    def build_context(self, question: str, max_sections: int = 4, limit_chars: int = 2800) -> str:
        if not self.loaded:
            return ""
        tokens = set(re.findall(r"[a-zA-Z]{3,}", question.lower()))
        scored: list[tuple[int, int]] = []
        for idx, section_tokens in enumerate(self.section_tokens):
            score = len(tokens & section_tokens)
            if score:
                scored.append((score, idx))
        scored.sort(key=lambda item: (-item[0], item[1]))

        parts: list[str] = []
        total_chars = 0
        for _, idx in scored[:max_sections]:
            text = self.sections[idx]["text"].strip()
            if not text:
                continue
            if parts and total_chars + len(text) > limit_chars:
                continue
            parts.append(text)
            total_chars += len(text)

        if not parts:
            return self.default_context
        combined = "\n\n".join(parts)
        return combined[:limit_chars]


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

def find_member(discord_id: int) -> Optional[discord.Member]:
    for g in bot.guilds:
        m = g.get_member(discord_id)
        if m:
            return m
    return None


def build_welcome_embed() -> discord.Embed:
    """Create the standard welcome embed used for new members."""
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
        "> :information_source: If are applying to MD to receive the recent **sign-on bonus** advertisement, this is a critical"
        " step to ensure you receive your payout.\n\n"
        ":four: Familiarize yourself with myself—Dr. Rae! I will serve as your medical AI assistant throughout our journey, "
        "and you'll have to learn a few of my important commands if you want to succeed. :checkered_flag: The first step we'll "
        "take together is my **/verify** command with your ROBLOX username—this is to ensure your on-site activity is *always* "
        "accurately tracked.\n\n"
        "That's all for now, if you have any questions at all just message any management member or even your peers! "
        "We're happy to have you here :sparkling_heart:"
    )

    embed = discord.Embed(title="Welcome to the Team!", description=msg, color=discord.Color.green())
    embed.set_footer(text="Best,\nThe Medical Department Management Team")
    return embed

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
                async with session.post(
                    url,
                    json=body,
                    headers={"X-Secret-Key": ROBLOX_REMOVE_SECRET, "Content-Type": "application/json"},
                    timeout=20,
                ) as resp:
                    text = await resp.text()
                    if _is_idempotent_ok(resp.status, text):
                        return True
                    raise RuntimeError(f"/set-rank HTTP {resp.status}: {text}")
        return await _retry(do_post)
    except Exception as e:
        print(f"set_group_rank error: {e}")
        return False

# Helper to map Roblox rank names from the service
async def find_group_role_by_name(name: str) -> dict | None:
    if not name:
        return None
    roles = await fetch_group_ranks()
    if not roles:
        return None
    name_lower = name.lower()
    for role in roles:
        role_name = str(role.get("name", ""))
        if role_name.lower() == name_lower:
            return role
    return None

# >>> NEW: accept join + ensure member+rank helpers <<<
def _is_idempotent_ok(status: int, body_text: str) -> bool:
    if 200 <= status < 300:
        return True
    if status != 400:
        return False
    text_lower = body_text.lower()
    idempotent_markers = [
        "you cannot change the user's role to the same role",
        "group join request is invalid",
        "user is already a member",
    ]
    return any(marker in text_lower for marker in idempotent_markers)

async def accept_group_join(roblox_id: int) -> bool:
    """Call service /accept-join to approve a pending request for this user."""
    if not ROBLOX_SERVICE_BASE or not ROBLOX_REMOVE_SECRET:
        return False
    url = ROBLOX_SERVICE_BASE.rstrip('/') + '/accept-join'
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json={"robloxId": int(roblox_id)},
                headers={"X-Secret-Key": ROBLOX_REMOVE_SECRET, "Content-Type": "application/json"},
                timeout=20
            ) as resp:
                text = await resp.text()
                if _is_idempotent_ok(resp.status, text):
                    return True
                print(f"accept_group_join failed {resp.status}: {text}")
                return False
    except Exception as e:
        print(f"accept_group_join error: {e}")
        return False

async def ensure_member_and_rank(roblox_id: int, *, rank_number: int = None, role_id: int = None) -> bool:
    """Optional: do acceptance + ranking in one server call."""
    if not ROBLOX_SERVICE_BASE or not ROBLOX_REMOVE_SECRET:
        return False
    url = ROBLOX_SERVICE_BASE.rstrip('/') + '/ensure-member-and-rank'
    payload = {"robloxId": int(roblox_id)}
    if rank_number is not None: payload["rankNumber"] = int(rank_number)
    if role_id is not None: payload["roleId"] = int(role_id)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json=payload,
                headers={"X-Secret-Key": ROBLOX_REMOVE_SECRET, "Content-Type": "application/json"},
                timeout=30
            ) as resp:
                text = await resp.text()
                if _is_idempotent_ok(resp.status, text):
                    return True
                print(f"ensure_member_and_rank failed {resp.status}: {text}")
                return False
    except Exception as e:
        print(f"ensure_member_and_rank error: {e}")
        return False

# === Minimal OpenAI client for rubric-based review ===
class SimpleOpenAI:
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    async def answer_guidelines(self, question: str, role_name: str, context: str) -> str:
        if not self.api_key:
            raise RuntimeError("Missing OPENAI_API_KEY for guidelines support.")
        system_prompt = (
            "You are Dr. Rae, a friendly yet professional assistant for the SCPF Medical Department. "
            "Rely exclusively on the provided guideline excerpts and any saved member background; treat them as your full "
            "knowledge base. "
            "If the excerpts do not contain the requested information, state that you are unsure and invite the member to "
            "check the handbook or provide more details via the `/guidelines context` command. "
            "Do not invent or reference real-world medical practices, Roblox platform rules, or anything outside the "
            "handbook. "
            "Do not quote large passages verbatim; instead, paraphrase and give clear action steps. "
            "Always remind members to follow official procedures if unsure and keep responses respectful. "
            f"Whenever a question touches on quota or activity expectations, spell out the standard requirement of "
            f"{WEEKLY_REQUIREMENT} logged services and {WEEKLY_TIME_REQUIREMENT} minutes on-site, and note that missing "
            "quota without an approved LoA/IN can lead to strikes. "
            "When explaining how to complete a task—such as running a checkup—lay out the process in clear, ordered steps "
            "so the member knows exactly what to do from preparation through logging."
        )
        user_message = (
            f"Member rank: {role_name or 'Unknown'}\n"
            "Relevant guideline excerpts:\n"
            f"{context or 'No context available.'}\n\n"
            f"Question: {question}\n\n"
            "Reply with a concise, encouraging explanation and include any key reminders the member should know. "
            "State the exact numbers, deadlines, or channels/forms involved instead of saying 'standard' or 'usual'. "
            "If an answer would require information that is not in the excerpts, say you are not sure and ask for more "
            "details or suggest reviewing the handbook section directly. "
            "If the member is asking how to carry out something, outline the steps in order so they can follow them."
        )
        payload = {
            "model": AI_MODEL,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            "temperature": 0.5,
            "max_tokens": 450,
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        url = f"{self.base_url}/chat/completions"
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload, timeout=60) as resp:
                txt = await resp.text()
                if resp.status // 100 != 2:
                    raise RuntimeError(f"AI guidelines answer failed {resp.status}: {txt}")
                data = json.loads(txt)
                return data["choices"][0]["message"]["content"].strip()

    async def score_application(self, answers: dict[str, str]) -> dict:
        """
        Call Chat Completions with a strict JSON schema for:
        overall_score (0-100), verdict, dimension scores, rationale, flags[].
        """
        system = (
            "You are a supportive reviewer for Medical Department applications. "
            "Default to accepting applicants unless their answers clearly show trolling, rule-breaking, or an inability to participate. "
            "Output only valid JSON."
        )
        user_content = {
            "instructions": (
                "Score this applicant using the rubric with a generous lens. "
                "Only recommend rejection when responses are extremely poor, off-topic, or violate guidelines. "
                "Return strict JSON (no prose)."
            ),
            "rubric": {
                "dimensions": {
                    "commitment": "Evidence of availability/consistency",
                    "clarity": "Clear writing and coherent reasoning",
                    "experience": "Relevant past roles/responsibility fit",
                    "professionalism": "Tone, maturity, no toxicity",
                    "policy": "Understands and respects guidelines"
                },
                "weights": {"commitment": 0.25, "clarity": 0.20, "experience": 0.25, "professionalism": 0.15, "policy": 0.15},
                "output_schema": {
                    "overall_score": "number 0..100",
                    "verdict": "accept|borderline|reject",
                    "dimension_scores": {"commitment": 0, "clarity": 0, "experience": 0, "professionalism": 0, "policy": 0},
                    "rationale": "string",
                    "flags": ["optional string flags like 'toxicity' or 'plagiarism_suspected'"]
                }
            },
            "answers": answers
        }
        payload = {
            "model": AI_MODEL,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(user_content)}
            ],
            "temperature": 0.2
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        url = f"{self.base_url}/chat/completions"
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload, timeout=60) as resp:
                txt = await resp.text()
                if resp.status // 100 != 2:
                    raise RuntimeError(f"AI review failed {resp.status}: {txt}")
                data = json.loads(txt)
                content = data["choices"][0]["message"]["content"]
                try:
                    return json.loads(content)
                except Exception:
                    cleaned = content.strip().strip("`")
                    return json.loads(cleaned)

# === Bot class ===
class MD_BOT(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix='!', intents=intents)
        self.db_pool: Optional[asyncpg.Pool] = None
        self.ai = SimpleOpenAI(OPENAI_API_KEY or "", AI_BASE_URL)
        self.guidelines = GuidelineStore(GUIDELINES_FILE)
        self._bootstrap_lock = asyncio.Lock()
        self._bootstrap_complete = False
        self.web_runner: web.AppRunner | None = None
        self.web_site: web.TCPSite | None = None

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

    async def ensure_bootstrap(self) -> None:
        if self._bootstrap_complete or not self.db_pool:
            return

        async with self._bootstrap_lock:
            if self._bootstrap_complete or not self.db_pool:
                return

            # Schema (create/ensure)
            async with self.db_pool.acquire() as connection:
                # Existing tables
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

                # Safety ALTERs for legacy DBs
                await connection.execute("ALTER TABLE weekly_task_logs ADD COLUMN IF NOT EXISTS task TEXT;")
                await connection.execute("ALTER TABLE task_logs ADD COLUMN IF NOT EXISTS task TEXT;")
                await connection.execute("UPDATE weekly_task_logs SET task = COALESCE(task, task_type) WHERE task IS NULL;")
                await connection.execute("UPDATE task_logs SET task = COALESCE(task, task_type) WHERE task IS NULL;")
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
                await connection.execute('''
                    CREATE TABLE IF NOT EXISTS member_activity_excuses (
                        member_id BIGINT PRIMARY KEY,
                        reason TEXT,
                        set_by BIGINT,
                        set_at TIMESTAMPTZ,
                        expires_at TIMESTAMPTZ
                    );
                ''')
                await connection.execute('''
                    CREATE TABLE IF NOT EXISTS guideline_context (
                        discord_id BIGINT PRIMARY KEY,
                        details TEXT NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                ''')

                # === New: Application system tables ===
                await connection.execute('''
                    CREATE TABLE IF NOT EXISTS applicants (
                        id BIGSERIAL PRIMARY KEY,
                        discord_id BIGINT UNIQUE NOT NULL,
                        roblox_username TEXT,
                        roblox_user_id BIGINT,
                        status TEXT NOT NULL DEFAULT 'in_progress', -- in_progress|submitted|accepted|rejected
                        created_at TIMESTAMPTZ DEFAULT now(),
                        updated_at TIMESTAMPTZ DEFAULT now(),
                        last_active TIMESTAMPTZ DEFAULT now(),
                        cooldown_until TIMESTAMPTZ
                    );
                ''')
                await connection.execute('''
                    CREATE TABLE IF NOT EXISTS application_runs (
                        id BIGSERIAL PRIMARY KEY,
                        applicant_id BIGINT REFERENCES applicants(id) ON DELETE CASCADE,
                        started_at TIMESTAMPTZ DEFAULT now(),
                        submitted_at TIMESTAMPTZ
                    );
                ''')
                await connection.execute('''
                    CREATE TABLE IF NOT EXISTS questions (
                        id SERIAL PRIMARY KEY,
                        code TEXT UNIQUE NOT NULL,
                        prompt TEXT NOT NULL,
                        type TEXT NOT NULL,
                        order_index INT NOT NULL
                    );
                ''')
                await connection.execute('''
                    CREATE TABLE IF NOT EXISTS answers (
                        id BIGSERIAL PRIMARY KEY,
                        run_id BIGINT REFERENCES application_runs(id) ON DELETE CASCADE,
                        question_code TEXT NOT NULL,
                        answer_text TEXT,
                        created_at TIMESTAMPTZ DEFAULT now()
                    );
                ''')
                await connection.execute('''
                    CREATE TABLE IF NOT EXISTS ai_reviews (
                        id BIGSERIAL PRIMARY KEY,
                        run_id BIGINT REFERENCES application_runs(id) ON DELETE CASCADE,
                        model TEXT NOT NULL,
                        score NUMERIC(5,2),
                        verdict TEXT,
                        rationale TEXT,
                        tokens_in INT,
                        tokens_out INT,
                        created_at TIMESTAMPTZ DEFAULT now()
                    );
                ''')
                await connection.execute('''
                    CREATE TABLE IF NOT EXISTS decisions (
                        id BIGSERIAL PRIMARY KEY,
                        run_id BIGINT REFERENCES application_runs(id) ON DELETE CASCADE,
                        decided_by TEXT NOT NULL,  -- 'ai' or 'staff:<discord_id>'
                        decision TEXT NOT NULL,    -- accept|reject
                        reason TEXT,
                        created_at TIMESTAMPTZ DEFAULT now()
                    );
                ''')

                # Seed/refresh "questions" ordering to match APPLICATION_QUESTIONS
                for idx, q in enumerate(APPLICATION_QUESTIONS):
                    await connection.execute(
                        """
                        INSERT INTO questions (code, prompt, type, order_index)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (code) DO UPDATE SET prompt = EXCLUDED.prompt, type = EXCLUDED.type, order_index = EXCLUDED.order_index
                        """,
                        q["code"], q["prompt"], q["type"], idx
                    )

            print("[DB] Tables ready.")

            if not self.web_runner:
                app = web.Application()
                app.router.add_get('/health', lambda _: web.Response(text='ok', status=200))
                app.router.add_post('/roblox', self.roblox_handler)
                self.web_runner = web.AppRunner(app)
                await self.web_runner.setup()
                self.web_site = web.TCPSite(self.web_runner, '0.0.0.0', 8080)
                await self.web_site.start()
                print("[Web] Server up on :8080 (GET /health, POST /roblox).")

            # Sync slash commands once
            try:
                synced = await self.tree.sync()
                print(f"[Slash] Synced {len(synced)} command(s)")
            except Exception as e:
                print(f"[Slash] Sync failed: {e}")

            self._bootstrap_complete = True

    async def resolve_member_rank(self, member: discord.Member) -> str:
        stored_rank: str | None = None
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    stored_rank = await conn.fetchval("SELECT rank FROM member_ranks WHERE discord_id=$1", member.id)
            except Exception as e:
                print(f"[WARN] Failed to fetch stored rank for {member.id}: {e}")
        if stored_rank:
            return stored_rank
        roles = [role for role in member.roles if not getattr(role, "is_default", lambda: role.id == member.guild.id)()]
        if not roles:
            roles = [role for role in member.roles if role.id != member.guild.id]
        if roles:
            roles.sort(key=lambda r: r.position, reverse=True)
            return roles[0].name
        return "Member"

    async def get_guideline_context(self, discord_id: int) -> str | None:
        if not self.db_pool:
            return None
        try:
            async with self.db_pool.acquire() as conn:
                return await conn.fetchval(
                    "SELECT details FROM guideline_context WHERE discord_id=$1",
                    discord_id,
                )
        except Exception as e:
            print(f"[WARN] Failed to load guideline context for {discord_id}: {e}")
            return None

    async def set_guideline_context(self, discord_id: int, details: str) -> None:
        if not self.db_pool:
            return
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO guideline_context (discord_id, details, updated_at) VALUES ($1, $2, $3) "
                    "ON CONFLICT (discord_id) DO UPDATE SET details = EXCLUDED.details, updated_at = EXCLUDED.updated_at",
                    discord_id,
                    details,
                    utcnow(),
                )
        except Exception as e:
            print(f"[WARN] Failed to store guideline context for {discord_id}: {e}")

    async def clear_guideline_context(self, discord_id: int) -> None:
        if not self.db_pool:
            return
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM guideline_context WHERE discord_id=$1",
                    discord_id,
                )
        except Exception as e:
            print(f"[WARN] Failed to clear guideline context for {discord_id}: {e}")

    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return

        await self.ensure_bootstrap()

        channel_id: int | None = None
        if isinstance(message.channel, discord.Thread):
            channel_id = message.channel.parent_id
        elif isinstance(message.channel, discord.abc.GuildChannel):
            channel_id = message.channel.id

        if GUIDELINES_CHANNEL_ID and channel_id == GUIDELINES_CHANNEL_ID:
            content = message.content.strip()
            lower = content.lower()
            if content and looks_like_question(content) and not is_probably_troll(content):
                if any(keyword in lower for keyword in QUESTION_KEYWORDS):
                    if self.guidelines.loaded:
                        rank_name = await self.resolve_member_rank(message.author)
                        context = self.guidelines.build_context(content)
                        extra_context = await self.get_guideline_context(message.author.id)
                        combined_context = context or ""
                        if extra_context:
                            combined_context = (combined_context + "\n\nMember-provided context:\n" + extra_context).strip()
                        if not combined_context.strip():
                            unsure = (
                                "I want to help, but I’m not sure I have the right details yet. "
                                "Please share more background with `/guidelines context` so I can give an accurate answer."
                            )
                            try:
                                await message.channel.send(unsure, reference=message)
                            except Exception as send_exc:
                                print(f"[WARN] Could not send guidelines unsure reply: {send_exc}")
                        else:
                            try:
                                async with message.channel.typing():
                                    reply = await self.ai.answer_guidelines(content, rank_name, combined_context)
                            except Exception as exc:
                                print(f"[WARN] Failed to answer guidelines question: {exc}")
                                reply = (
                                    "I’m having trouble accessing the guidelines right now. "
                                    "Please double-check the handbook or reach out to MD management for help."
                                )
                            if reply:
                                try:
                                    await message.channel.send(reply, reference=message)
                                    await log_action(
                                        "Guidelines Q&A",
                                        f"Question by {message.author.mention}:\n{escape_markdown(content)}",
                                    )
                                except Exception as send_exc:
                                    print(f"[WARN] Could not send guidelines reply: {send_exc}")
                    else:
                        print("[WARN] Guidelines requested but store is not loaded.")
                else:
                    unsure = (
                        "I’m not completely sure how to answer that. "
                        "If you can include more details or use `/guidelines context` to share background, I can give a better reply."
                    )
                    try:
                        await message.channel.send(unsure, reference=message)
                    except Exception as send_exc:
                        print(f"[WARN] Could not send unsure guidelines prompt: {send_exc}")

        await super().on_message(message)

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

# Pre-create command groups (kept from your original)
tasks_group = app_commands.Group(name="tasks", description="Commands for tracking Medical Department tasks.")
orientation_group = app_commands.Group(name="orientation", description="Manage member orientation progress.")
strikes_group = app_commands.Group(name="strikes", description="Manage member strikes.")
excuses_group = app_commands.Group(name="excuses", description="Manage activity excuses.")
guidelines_group = app_commands.Group(name="guidelines", description="Help Dr. Rae answer guideline questions accurately.")

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

# Global slash error (kept)
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

@guidelines_group.command(name="context", description="Save extra background so Dr. Rae can tailor answers to you.")
@app_commands.describe(details="Key responsibilities, roles, or expectations you want Dr. Rae to remember (max 1000 characters).")
async def guidelines_context(interaction: discord.Interaction, details: str):
    await bot.ensure_bootstrap()
    trimmed = details.strip()
    if not trimmed:
        await interaction.response.send_message("Please include a little information for me to remember.", ephemeral=True)
        return
    if len(trimmed) > 1000:
        await interaction.response.send_message("Please keep the saved context under 1000 characters.", ephemeral=True)
        return
    await bot.set_guideline_context(interaction.user.id, trimmed)
    await log_action(
        "Guidelines Context Updated",
        f"User: {interaction.user.mention}\nDetails: {escape_markdown(trimmed)}",
    )
    await interaction.response.send_message(
        "Got it! I’ll factor that in when answering your future guideline questions.",
        ephemeral=True,
    )

@guidelines_group.command(name="show_context", description="See what extra background Dr. Rae currently remembers about you.")
async def guidelines_show_context(interaction: discord.Interaction):
    await bot.ensure_bootstrap()
    details = await bot.get_guideline_context(interaction.user.id)
    if details:
        await interaction.response.send_message(
            f"Here’s what I have saved right now:\n\n{details}",
            ephemeral=True,
        )
    else:
        await interaction.response.send_message(
            "I don’t have any extra context saved for you yet. Use `/guidelines context` to add some!",
            ephemeral=True,
        )

@guidelines_group.command(name="clear_context", description="Remove any extra background saved for guideline answers.")
async def guidelines_clear_context(interaction: discord.Interaction):
    await bot.ensure_bootstrap()
    existing = await bot.get_guideline_context(interaction.user.id)
    if not existing:
        await interaction.response.send_message("There isn’t any saved context to clear.", ephemeral=True)
        return
    await bot.clear_guideline_context(interaction.user.id)
    await log_action(
        "Guidelines Context Cleared",
        f"User: {interaction.user.mention}",
    )
    await interaction.response.send_message("All set. I’ve cleared your saved context.", ephemeral=True)

# === PART 1/3 END ===
# Reply "next" and I'll send PART 2/3 with: /verify, the /apply wizard, AI review, and auto-accept (including accept-join → rank → welcome).
# main.py (Medical Department bot) — PART 2/3
# Continues directly from Part 1 — /verify, /apply wizard, AI review, and auto-accept flow.

# /verify
@bot.tree.command(name="verify", description="Link your Roblox account to the bot.")
async def verify(interaction: discord.Interaction, roblox_username: str):
    payload = {"usernames": [roblox_username], "excludeBannedUsers": True}
    async with aiohttp.ClientSession() as session:
        async with session.post("https://users.roblox.com/v1/usernames/users", json=payload) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("data"):
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

# --- Application flow ---

class ApplicationModal(discord.ui.Modal):
    """Modal for long-answer questions."""
    def __init__(self, question_code: str, question_text: str, min_len: int, max_len: int):
        modal_title = question_text if len(question_text) <= 45 else question_text[:42] + "..."
        super().__init__(title=modal_title)
        self.q_code = question_code
        self.min_len = min_len
        self.max_len = max_len
        self.answer = None
        placeholder = question_text if len(question_text) <= 100 else question_text[:97] + "..."
        self.q_field = discord.ui.TextInput(
            label="Your Answer",
            style=discord.TextStyle.paragraph,
            min_length=min_len if 0 < min_len < max_len else None,
            max_length=max_len,
            required=True,
            placeholder=placeholder
        )
        self.add_item(self.q_field)

    async def on_submit(self, interaction: discord.Interaction):
        self.answer = self.q_field.value.strip()
        await interaction.response.defer()

class ApplyView(discord.ui.View):
    """Dynamic per-user application wizard view."""
    def __init__(self, user_id: int):
        super().__init__(timeout=None)
        self.user_id = user_id
        self.current_index = 0
        self.answers: dict[str, str] = {}
        self.stage: str = "intro"
        self.review_message_sent = False
        self.total_questions = len(APPLICATION_QUESTIONS)
        # Button adjustments happen post-init once components exist
        self.next.label = "Start Application"
        self.next.style = discord.ButtonStyle.green

    def _progress_bar(self) -> str:
        if not self.total_questions:
            return "[██████████] (0/0)"
        ratio = self.current_index / self.total_questions
        filled = max(0, min(10, int(round(ratio * 10))))
        bar = "█" * filled + "░" * (10 - filled)
        return f"[{bar}] ({self.current_index}/{self.total_questions})"

    def _base_message(self) -> str:
        header = "**Medical Department Application**"
        progress = f"Progress: {self._progress_bar()}"
        if self.stage == "review":
            body = (
                "Review your answers in the summary below, then press **Submit Application** when you're ready."
            )
        elif self.stage == "submitting":
            body = "Submitting your responses for review… please wait."
        elif self.stage == "completed":
            body = "Your application has been submitted. You may close this window."
        elif self.current_index == 0:
            body = (
                "Press **Start Application** to answer the first question.\n\n"
                f"{APPLICATION_PENDING_WARNING}"
            )
        else:
            body = "Click **Next Question** to continue."
        return f"{header}\n{progress}\n\n{body}"

    def _truncate(self, text: str, limit: int = 200) -> str:
        clean = text.strip()
        if not clean:
            return "*No response provided*"
        if len(clean) <= limit:
            return clean
        return clean[: limit - 3] + "..."

    async def _refresh_message(self, interaction: discord.Interaction):
        if self.stage == "review":
            self.next.label = "Submit Application"
            self.next.style = discord.ButtonStyle.green
        elif self.stage == "completed":
            self.next.label = "Submitted"
            self.next.disabled = True
            self.next.style = discord.ButtonStyle.gray
        elif self.current_index == 0:
            self.next.label = "Start Application"
            self.next.style = discord.ButtonStyle.green
        else:
            self.next.label = "Next Question"
            self.next.style = discord.ButtonStyle.blurple

        if interaction.response.is_done():
            await interaction.edit_original_response(content=self._base_message(), view=self)
        else:
            await interaction.response.edit_message(content=self._base_message(), view=self)

    def _build_review_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="Application Review",
            description="Please look over your responses before submitting.",
            color=discord.Color.blurple(),
            timestamp=utcnow()
        )
        for question in APPLICATION_QUESTIONS:
            answer = self.answers.get(question["code"], "")
            snippet = self._truncate(answer)
            embed.add_field(name=question["prompt"][:256], value=escape_markdown(snippet), inline=False)
        return embed

    async def _ensure_applicant_row(self):
        async with bot.db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO applicants (discord_id, status) VALUES ($1,'in_progress') "
                "ON CONFLICT (discord_id) DO UPDATE SET last_active = now(), updated_at = now()",
                self.user_id
            )

    async def _after_answer(self, interaction: discord.Interaction):
        self.current_index = min(self.current_index + 1, self.total_questions)
        self.stage = "questions"
        await self._refresh_message(interaction)
        if self.current_index >= self.total_questions:
            await self.show_review(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.blurple)
    async def next(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This isn’t your application.", ephemeral=True)
            return

        if self.stage == "completed":
            await interaction.response.send_message("Your application has already been submitted.", ephemeral=True)
            return

        if self.stage == "review":
            await self.submit_application(interaction)
            return

        if self.current_index >= self.total_questions:
            await self.show_review(interaction)
            return

        await self.present_question(interaction)

    async def present_question(self, interaction: discord.Interaction):
        await self._ensure_applicant_row()

        question = APPLICATION_QUESTIONS[self.current_index]
        prompt = question["prompt"]
        q_code = question["code"]

        if question["type"] == "long":
            modal = ApplicationModal(q_code, prompt, question.get("min_len", 0), question.get("max_len", 1000))
            await interaction.response.send_modal(modal)
            await modal.wait()
            if not modal.answer:
                return
            self.answers[q_code] = modal.answer
            await self._after_answer(interaction)
            return

        await interaction.response.send_message(prompt, ephemeral=True)
        try:
            msg = await bot.wait_for(
                "message",
                check=lambda m: m.author.id == interaction.user.id and m.channel == interaction.channel,
                timeout=APPLICATION_TIMEOUT_MINUTES * 60
            )
        except asyncio.TimeoutError:
            await interaction.followup.send("⏰ Application timed out. Please restart with `/apply`.", ephemeral=True)
            return

        ans = (msg.content or "").strip()
        if question.get("min_len") and len(ans) < question["min_len"]:
            await interaction.followup.send(
                f"Please provide at least **{question['min_len']}** characters.",
                ephemeral=True
            )
            try:
                await msg.delete()
            except Exception:
                pass
            return

        if question.get("max_len"):
            ans = ans[: question["max_len"]]
        self.answers[q_code] = ans
        try:
            await msg.delete()
        except Exception:
            pass

        await self._after_answer(interaction)

    async def show_review(self, interaction: discord.Interaction):
        self.stage = "review"
        self.current_index = self.total_questions
        await self._refresh_message(interaction)
        if not self.review_message_sent:
            embed = self._build_review_embed()
            await interaction.followup.send(
                "Here is a summary of your responses. If you need to make major changes, you can restart `/apply` before submitting.",
                embed=embed,
                ephemeral=True
            )
            self.review_message_sent = True

    async def submit_application(self, interaction: discord.Interaction):
        if len(self.answers) < self.total_questions:
            await interaction.response.send_message(
                "Please answer every question before submitting.",
                ephemeral=True
            )
            return

        self.stage = "submitting"
        self.next.disabled = True
        self.next.label = "Submitting..."
        self.next.style = discord.ButtonStyle.gray

        await interaction.response.defer(ephemeral=True, thinking=True)
        await interaction.edit_original_response(content=self._base_message(), view=self)

        roblox_username = (self.answers.get("roblox_username") or "").strip()

        try:
            async with bot.db_pool.acquire() as conn:
                applicant_id = await conn.fetchval(
                    "INSERT INTO applicants (discord_id, roblox_username, status, updated_at) "
                    "VALUES ($1, $2, 'submitted', now()) "
                    "ON CONFLICT (discord_id) DO UPDATE SET roblox_username = EXCLUDED.roblox_username, status='submitted', updated_at=now() "
                    "RETURNING id",
                    self.user_id, roblox_username or None
                )
                run_id = await conn.fetchval(
                    "INSERT INTO application_runs (applicant_id, started_at, submitted_at) VALUES ($1, now(), now()) RETURNING id",
                    applicant_id
                )
                for code, text in self.answers.items():
                    await conn.execute(
                        "INSERT INTO answers (run_id, question_code, answer_text, created_at) VALUES ($1, $2, $3, now())",
                        run_id, code, text
                    )

            await interaction.followup.send(
                "✅ Application submitted! Evaluating your responses...\n\n"
                f"{APPLICATION_PENDING_WARNING}",
                ephemeral=True,
            )

            try:
                result = await bot.ai.score_application(self.answers)
            except Exception as e:
                self.stage = "review"
                self.next.disabled = False
                self.next.label = "Submit Application"
                self.next.style = discord.ButtonStyle.green
                await interaction.edit_original_response(content=self._base_message(), view=self)
                await interaction.followup.send(f"AI review failed: {e}", ephemeral=True)
                await log_action("Application AI Error", f"User: <@{self.user_id}>\nError: {e}")
                return

            try:
                score = float(result.get("overall_score", 0))
            except Exception:
                score = 0.0
            verdict = (result.get("verdict") or "reject").lower()
            rationale = (result.get("rationale") or "")[:1500]
            flags_raw = result.get("flags") or []
            if isinstance(flags_raw, str):
                flags_raw = [flags_raw]
            flags = [str(flag).lower() for flag in flags_raw if isinstance(flag, str)]

            await log_action(
                "Application Scored",
                f"User: <@{self.user_id}>\nScore: **{score:.1f}**\nVerdict: `{verdict}`\nFlags: {', '.join(flags) or 'none'}\nRationale: {rationale[:300]}..."
            )

            async with bot.db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO ai_reviews (run_id, model, score, verdict, rationale) VALUES ($1,$2,$3,$4,$5)",
                    run_id, AI_MODEL, score, verdict, rationale
                )

            severe_terms = {"toxicity", "harassment", "hate", "plagiarism_suspected", "troll", "spam"}
            has_severe_flag = any(flag in severe_terms for flag in flags)

            decision_made = False
            if not has_severe_flag and (verdict == "accept" or score >= APPLICATION_AUTO_ACCEPT_THRESHOLD):
                await handle_accept(interaction, self.user_id, self.answers, run_id)
                decision_made = True
            elif not has_severe_flag and (score >= APPLICATION_BORDERLINE_MIN or verdict == "borderline"):
                await handle_borderline(interaction, self.user_id, self.answers, score, run_id)
                decision_made = True
            elif not has_severe_flag and score >= APPLICATION_HARD_REJECT_THRESHOLD:
                await handle_borderline(interaction, self.user_id, self.answers, score, run_id)
                decision_made = True
            else:
                await handle_reject(interaction, self.user_id, score, rationale, run_id)
                decision_made = True

            if decision_made:
                self.stage = "completed"
                self.next.label = "Submitted"
                self.next.disabled = True
                self.next.style = discord.ButtonStyle.gray
                await interaction.edit_original_response(content=self._base_message(), view=self)

        except Exception as exc:
            self.stage = "review"
            self.next.disabled = False
            self.next.label = "Submit Application"
            self.next.style = discord.ButtonStyle.green
            await interaction.edit_original_response(content=self._base_message(), view=self)
            await interaction.followup.send(f"There was an error submitting your application: {exc}", ephemeral=True)
            await log_action("Application Submission Error", f"User: <@{self.user_id}>\nError: {exc}")

async def handle_accept(interaction: discord.Interaction, discord_id: int, answers: dict, run_id: int):
    """Accept, verify Roblox, accept join request if pending, rank, welcome, and set cooldown."""
    member = find_member(discord_id)
    roblox_name = (answers.get("roblox_username") or "").strip()
    if not member:
        await log_action("Application Accepted (but member missing)", f"User ID: {discord_id}")
        return

    # Auto verify Roblox
    roblox_id = None
    if roblox_name:
        payload = {"usernames": [roblox_name], "excludeBannedUsers": True}
        async with aiohttp.ClientSession() as session:
            async with session.post("https://users.roblox.com/v1/usernames/users", json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("data"):
                        user_info = data["data"][0]
                        roblox_id = user_info["id"]
                        roblox_name = str(user_info.get("name") or roblox_name)
                        async with bot.db_pool.acquire() as conn:
                            await conn.execute(
                                "INSERT INTO roblox_verification (discord_id, roblox_id) VALUES ($1,$2) "
                                "ON CONFLICT (discord_id) DO UPDATE SET roblox_id=EXCLUDED.roblox_id",
                                discord_id, roblox_id
                            )
                        await log_action("Auto Verified", f"User: <@{discord_id}> | Roblox: `{roblox_name}` ({roblox_id})")

    # Accept join request if pending, then rank to the configured Roblox group role
    if roblox_id:
        ensure_kwargs: dict[str, int] = {}
        target_role_name: str | None = None
        target_role = await find_group_role_by_name(AUTO_ACCEPT_GROUP_ROLE_NAME)
        if not target_role:
            print(
                f"[WARN] Auto-accept Roblox role '{AUTO_ACCEPT_GROUP_ROLE_NAME}' not found in group ranks."
            )
        else:
            role_name = str(target_role.get("name") or "").strip()
            target_role_name = role_name or AUTO_ACCEPT_GROUP_ROLE_NAME
            role_id = target_role.get("id")
            if role_id is not None:
                try:
                    ensure_kwargs["role_id"] = int(role_id)
                except Exception:
                    pass
            if not ensure_kwargs:
                rank_value = target_role.get("rank")
                if rank_value is None:
                    rank_value = target_role.get("rankNumber")
                if rank_value is not None:
                    try:
                        ensure_kwargs["rank_number"] = int(rank_value)
                    except Exception:
                        pass
        if not ensure_kwargs:
            print(
                f"[WARN] Unable to determine Roblox role ID or rank number for '{AUTO_ACCEPT_GROUP_ROLE_NAME}'."
            )
            roblox_rank_success = False
        else:
            roblox_rank_success = await ensure_member_and_rank(int(roblox_id), **ensure_kwargs)
            if not roblox_rank_success:
                # fallback: accept then rank separately
                await accept_group_join(int(roblox_id))
                roblox_rank_success = await set_group_rank(int(roblox_id), **ensure_kwargs)

        if roblox_rank_success and not target_role_name:
            # Attempt to resolve the role name by the rank number if available
            rank_number = ensure_kwargs.get("rank_number")
            if rank_number is not None:
                roles = await fetch_group_ranks()
                for role in roles:
                    role_rank = role.get("rank") if role.get("rank") is not None else role.get("rankNumber")
                    if role_rank == rank_number:
                        candidate_name = str(role.get("name") or "").strip()
                        if candidate_name:
                            target_role_name = candidate_name
                        break

        if roblox_rank_success and target_role_name:
            try:
                async with bot.db_pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO member_ranks (discord_id, rank, set_by, set_at) VALUES ($1, $2, $3, $4) "
                        "ON CONFLICT (discord_id) DO UPDATE SET rank = EXCLUDED.rank, set_by = EXCLUDED.set_by, set_at = EXCLUDED.set_at",
                        member.id,
                        target_role_name,
                        bot.user.id if bot.user else None,
                        utcnow(),
                    )
            except Exception as e:
                print(f"[WARN] Failed to record auto rank for {member.id}: {e}")

            try:
                matching_role = next(
                    (role for role in interaction.guild.roles if role.name.lower() == target_role_name.lower()),
                    None,
                )
                if matching_role and matching_role not in member.roles:
                    await member.add_roles(matching_role, reason="Auto-accepted application rank sync")
            except Exception as e:
                print(f"[WARN] Failed to assign Discord rank role to {member.id}: {e}")

    # Sync Discord nickname with Roblox username
    cleaned_nick = (roblox_name or "").strip()
    if cleaned_nick:
        trimmed_nick = cleaned_nick[:32]
        if member.nick != trimmed_nick:
            try:
                await member.edit(nick=trimmed_nick, reason="Auto-accepted application Roblox sync")
            except discord.Forbidden:
                print(f"[WARN] Missing permissions to change nickname for {member.id}")
            except Exception as e:
                print(f"[WARN] Failed to update nickname for {member.id}: {e}")

    # Assign Discord roles
    role_ids = [rid for rid in [MEDICAL_STUDENT_ROLE_ID, *APPLICATION_EXTRA_ROLE_IDS] if rid]
    roles_to_add = [interaction.guild.get_role(rid) for rid in role_ids]
    roles_to_add = [role for role in roles_to_add if role and role not in member.roles]
    if roles_to_add:
        try:
            await member.add_roles(*roles_to_add, reason="Auto-accepted application")
        except Exception as e:
            print(f"Failed to add roles {[r.id for r in roles_to_add]}: {e}")

    # Welcome in comms with standard embed
    comms = bot.get_channel(COMMS_CHANNEL_ID) if COMMS_CHANNEL_ID else None
    if comms:
        try:
            await comms.send(content=f"🎉 Please welcome {member.mention} to the **Medical Department**!", embed=build_welcome_embed())
        except Exception as e:
            print(f"Failed to send welcome: {e}")

    # Management channel notification
    management_channel = bot.get_channel(APPLICATION_MANAGEMENT_CHANNEL_ID)
    if management_channel:
        try:
            roblox_display = roblox_name or "unknown"
            await management_channel.send(
                f"✅ Application accepted for {member.mention} (`{roblox_display}`) — roles assigned and onboarding message posted."
            )
        except Exception as e:
            print(f"Failed to send management acceptance notice: {e}")

    # Store decision + cooldown
    async with bot.db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO decisions (run_id, decided_by, decision, reason) VALUES ($1,$2,$3,$4)",
            run_id, "ai", "accept", "Auto-accepted by AI threshold"
        )
        await conn.execute(
            "UPDATE applicants SET status='accepted', cooldown_until = (now() + ($1 * interval '1 hour')), updated_at=now() WHERE discord_id=$2",
            APPLICATION_COOLDOWN_HOURS, discord_id
        )

    await log_action("Application Accepted", f"Auto-accepted: <@{discord_id}>")
    await interaction.followup.send(f"✅ Application accepted for {member.mention}!", ephemeral=True)

async def handle_borderline(interaction, discord_id, answers, score, run_id):
    """Queue for manual review."""
    member = find_member(discord_id)
    management_channel = bot.get_channel(APPLICATION_MANAGEMENT_CHANNEL_ID)
    await log_action("Application Borderline", f"<@{discord_id}> — Score: {score:.1f}")
    if management_channel:
        await management_channel.send(
            f"🟡 Application borderline — needs manual review.\nUser: {member.mention if member else discord_id}\nScore: **{score:.1f}**"
        )
    async with bot.db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO decisions (run_id, decided_by, decision, reason) VALUES ($1,$2,$3,$4)",
            run_id, "ai", "borderline", "Below auto-accept threshold but above minimum"
        )
        await conn.execute(
            "UPDATE applicants SET status='submitted', cooldown_until=NULL, updated_at=now() WHERE discord_id=$1",
            discord_id
        )
    await interaction.followup.send("⚠️ Application under manual review.", ephemeral=True)

async def handle_reject(interaction, discord_id, score, rationale, run_id):
    """Reject application."""
    member = find_member(discord_id)
    if member:
        try:
            await member.send(
                f"Hello — thank you for applying to the **Medical Department**, but unfortunately your application has not been accepted.\n\n"
                f"**Score:** {score:.1f}\nReasoning:\n> {rationale[:500]}"
            )
        except:
            pass
    async with bot.db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO decisions (run_id, decided_by, decision, reason) VALUES ($1,$2,$3,$4)",
            run_id, "ai", "reject", rationale[:500]
        )
        await conn.execute(
            "UPDATE applicants SET status='rejected', cooldown_until = (now() + ($1 * interval '1 hour')), updated_at=now() WHERE discord_id=$2",
            APPLICATION_COOLDOWN_HOURS, discord_id
        )
    await log_action("Application Rejected", f"User: <@{discord_id}> | Score: {score:.1f}")
    await interaction.followup.send("❌ Application rejected.", ephemeral=True)

# /apply command
@bot.tree.command(name="apply", description="Begin your Medical Department application.")
async def apply(interaction: discord.Interaction):
    # Cooldown check
    async with bot.db_pool.acquire() as conn:
        cooldown = await conn.fetchval("SELECT cooldown_until FROM applicants WHERE discord_id=$1", interaction.user.id)
        if cooldown and cooldown > utcnow():
            remain = human_remaining(cooldown - utcnow())
            await interaction.response.send_message(
                f"You must wait **{remain}** before applying again.", ephemeral=True
            )
            return
        # bootstrap row
        await conn.execute(
            "INSERT INTO applicants (discord_id, status) VALUES ($1,'in_progress') "
            "ON CONFLICT (discord_id) DO UPDATE SET status='in_progress', updated_at=now(), last_active=now()",
            interaction.user.id
        )

    view = ApplyView(interaction.user.id)
    await interaction.response.send_message(
        view._base_message(),
        view=view,
        ephemeral=True
    )

# === PART 2/3 END ===
# Reply "next" to receive PART 3/3 — remaining commands (tasks/orientation/strikes/excuses), loops, /rank, and bot.run().
# main.py (Medical Department bot) — PART 3/3
# Continues directly from Part 2 — announcements, tasks, orientation/strikes/excuses, loops, /rank, and bot.run().

# ---------- Announcements ----------
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

# ---------- Tasks ----------
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

@tasks_group.command(name="log", description="Log a completed task with proof and type.")
@app_commands.choices(task_type=[app_commands.Choice(name=t, value=t) for t in TASK_TYPES])
async def tasks_log(interaction: discord.Interaction, task_type: str, proof: discord.Attachment):
    await interaction.response.send_modal(LogTaskForm(proof=proof, task_type=task_type))

@tasks_group.command(name="my", description="Check your weekly tasks and time.")
async def tasks_my(interaction: discord.Interaction):
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

@tasks_group.command(name="member", description="Show a member's task totals by type (all-time).")
async def tasks_member(interaction: discord.Interaction, member: discord.Member | None = None):
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

@tasks_group.command(name="add", description="(Mgmt) Add tasks to a member's history and weekly totals.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
@app_commands.choices(task_type=[app_commands.Choice(name=t, value=t) for t in TASK_TYPES])
async def tasks_add(
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

@tasks_group.command(name="leaderboard", description="Displays the weekly leaderboard (tasks + on-site minutes).")
async def tasks_leaderboard(interaction: discord.Interaction):
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

@tasks_group.command(name="undo", description="Removes the last logged task for a member.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def tasks_undo(interaction: discord.Interaction, member: discord.Member):
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

# ---------- Welcome + DM ----------
@bot.tree.command(name="welcome", description="Sends the official welcome message.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def welcome(interaction: discord.Interaction):
    await interaction.channel.send(embed=build_welcome_embed())
    await log_action("Welcome Sent", f"By: {interaction.user.mention} • Channel: {interaction.channel.mention}")
    await interaction.response.send_message("Welcome message sent!", ephemeral=True)

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

# ---------- Verification audit ----------
@bot.tree.command(
    name="verification_audit",
    description="(Mgmt) List members missing /verify and optionally ping them in comms.",
)
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def verification_audit(interaction: discord.Interaction, ping: bool = False):
    await interaction.response.defer(ephemeral=True, thinking=True)

    guild = interaction.guild
    if not guild:
        await interaction.followup.send("This command can only be used inside the server.", ephemeral=True)
        return

    roles_to_check: list[discord.Role] = []
    for role_id in (DEPARTMENT_ROLE_ID, MEDICAL_STUDENT_ROLE_ID):
        if not role_id:
            continue
        role = guild.get_role(role_id)
        if role:
            roles_to_check.append(role)

    if not roles_to_check:
        await interaction.followup.send(
            "No department or medical student roles are configured, so I can't audit verification.",
            ephemeral=True,
        )
        return

    members_to_check: dict[int, discord.Member] = {}
    for role in roles_to_check:
        for member in role.members:
            if member.bot:
                continue
            members_to_check[member.id] = member

    if not members_to_check:
        await interaction.followup.send(
            "I couldn't find any members in the configured roles to audit.",
            ephemeral=True,
        )
        return

    async with bot.db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT discord_id FROM roblox_verification")
    verified_ids = {int(row["discord_id"]) for row in rows}

    missing_members = [member for member in members_to_check.values() if member.id not in verified_ids]
    missing_members.sort(key=lambda m: (m.display_name.lower(), m.id))

    ping_result: str | None = None
    ping_sent = False

    if ping:
        if not missing_members:
            ping_result = "Everyone is already verified, so no ping was sent."
        else:
            comms_channel = bot.get_channel(COMMS_CHANNEL_ID) if COMMS_CHANNEL_ID else None
            if not comms_channel:
                ping_result = "The comms channel isn't configured."
            else:
                allowed_mentions = discord.AllowedMentions(users=True)
                reminder = "Please run `/verify` with your Roblox username so your activity can be logged accurately."

                def mention_chunks(max_len: int = 1800):
                    chunk: list[str] = []
                    length = 0
                    for member in missing_members:
                        mention = member.mention
                        additional = len(mention) + (1 if chunk else 0)
                        if length + additional > max_len:
                            if chunk:
                                yield " ".join(chunk)
                            chunk = [mention]
                            length = len(mention)
                        else:
                            if chunk:
                                length += 1
                            chunk.append(mention)
                            length += len(mention)
                    if chunk:
                        yield " ".join(chunk)

                for chunk in mention_chunks():
                    await comms_channel.send(
                        content=f"{chunk}\n\n{reminder}",
                        allowed_mentions=allowed_mentions,
                    )
                ping_result = f"Ping sent in {comms_channel.mention}."
                ping_sent = True

    messages: list[str] = []
    if missing_members:
        header = f"**{len(missing_members)}** member(s) still need to complete `/verify`."
        if ping_result:
            header += f"\n{ping_result}"
        lines = [f"- {member.mention} ({member.display_name})" for member in missing_members]
        chunks = smart_chunk("\n".join(lines), size=1800)
        if chunks:
            messages.append(f"{header}\n{chunks[0]}" if chunks[0] else header)
            for chunk in chunks[1:]:
                messages.append(chunk)
        else:
            messages.append(header)
    else:
        text = "Everyone in the configured roles has completed `/verify`. ✅"
        if ping_result:
            text += f"\n{ping_result}"
        messages.append(text)

    for msg in messages:
        await interaction.followup.send(msg, ephemeral=True)

    log_lines = [
        f"By: {interaction.user.mention}",
        f"Missing members: {len(missing_members)}",
        f"Ping requested: {'✅' if ping else '❌'}",
        f"Ping sent: {'✅' if ping_sent else '❌'}",
    ]
    if ping_result:
        log_lines.append(f"Note: {ping_result}")

    await log_action("Verification Audit", "\n".join(log_lines))

# ---------- Orientation ----------
async def ensure_orientation_record(member: discord.Member):
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

@orientation_group.command(name="complete", description="(Mgmt) Mark a member as having passed orientation.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def orientation_complete(interaction: discord.Interaction, member: discord.Member):
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

@orientation_group.command(name="view", description="View a member's orientation status.")
async def orientation_view(interaction: discord.Interaction, member: discord.Member | None = None):
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

@orientation_group.command(name="extend", description="(Mgmt) Extend a member's orientation deadline by N days.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def orientation_extend(interaction: discord.Interaction, member: discord.Member, days: app_commands.Range[int, 1, 60], reason: str | None = None):
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

# ---------- Strikes ----------
async def issue_strike(member: discord.Member, reason: str, *, set_by: int | None, auto: bool) -> int:
    now = utcnow()
    expires = now + datetime.timedelta(days=90)
    async with bot.db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO strikes (member_id, reason, issued_at, expires_at, set_by, auto) "
            "VALUES ($1, $2, $3, $4, $5, $6)",
            member.id, reason, now, expires, set_by, auto
        )
        active = await conn.fetchval("SELECT COUNT(*) FROM strikes WHERE member_id=$1 AND expires_at > $2", member.id, now)

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

@strikes_group.command(name="add", description="(Mgmt) Add a strike to a member.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def strikes_add(interaction: discord.Interaction, member: discord.Member, reason: str):
    active_after = await issue_strike(member, reason, set_by=interaction.user.id, auto=False)
    if active_after >= 3:
        await enforce_three_strikes(member)
    await interaction.response.send_message(f"Strike added to {member.mention}. Active strikes: **{active_after}/3**.", ephemeral=True)

@strikes_group.command(name="remove", description="(Mgmt) Remove N active strikes from a member (earliest expiring first).")
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

@strikes_group.command(name="view", description="View a member's active and total strikes.")
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

# ---------- Excuses ----------
@excuses_group.command(name="week", description="(Mgmt) Set or clear a weekly activity excuse (no strikes for that week).")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
@app_commands.describe(action="set or clear", week="ISO week like 2025-W39; default = current week", reason="Required when action=set")
async def excuses_week(
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

@excuses_group.command(name="member", description="(Mgmt) Excuse a member from weekly activity requirements.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
@app_commands.describe(
    member="Member to excuse",
    days="Number of days to excuse them (0 removes the excuse)",
    reason="Reason for the excuse (required when days > 0)",
)
async def excuses_member(
    interaction: discord.Interaction,
    member: discord.Member,
    days: app_commands.Range[int, 0, 365],
    reason: str | None = None,
):
    now = utcnow()
    async with bot.db_pool.acquire() as conn:
        if days == 0:
            await conn.execute("DELETE FROM member_activity_excuses WHERE member_id=$1", member.id)
            await log_action(
                "Member Excuse Cleared",
                f"Member: {member.mention}\nBy: {interaction.user.mention}",
            )
            await interaction.response.send_message(
                f"Removed activity excuse for {member.mention}.",
                ephemeral=True,
            )
            return

        if not reason:
            await interaction.response.send_message(
                "Please include a reason when setting an excuse.",
                ephemeral=True,
            )
            return

        expires_at = now + datetime.timedelta(days=days)
        await conn.execute(
            "INSERT INTO member_activity_excuses (member_id, reason, set_by, set_at, expires_at) "
            "VALUES ($1, $2, $3, $4, $5) "
            "ON CONFLICT (member_id) DO UPDATE SET reason=EXCLUDED.reason, set_by=EXCLUDED.set_by, set_at=EXCLUDED.set_at, expires_at=EXCLUDED.expires_at",
            member.id,
            reason,
            interaction.user.id,
            now,
            expires_at,
        )
        await log_action(
            "Member Excused",
            f"Member: {member.mention}\nBy: {interaction.user.mention}\nDays: {days}\nUntil: {expires_at.strftime('%Y-%m-%d %H:%M UTC')}\nReason: {reason}",
        )
        await interaction.response.send_message(
            f"{member.mention} is excused from activity requirements until **{expires_at.strftime('%Y-%m-%d %H:%M UTC')}**.",
            ephemeral=True,
        )

# ---------- Weekly task summary + strikes + reset ----------
@tasks.loop(time=datetime.time(hour=4, minute=0, tzinfo=datetime.timezone.utc))
async def check_weekly_tasks():
    # Only fire on Sunday UTC
    if utcnow().weekday() != 6:
        return

    wk = week_key()
    # If excused week, post the report but **do not issue strikes**
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

    now = utcnow()

    async with bot.db_pool.acquire() as conn:
        all_tasks = await conn.fetch("SELECT member_id, tasks_completed FROM weekly_tasks")
        all_time = await conn.fetch("SELECT member_id, time_spent FROM roblox_time")
        # Pull active strikes counts for all dept members
        strike_counts = {
            r['member_id']: r['cnt'] for r in await conn.fetch(
                "SELECT member_id, COUNT(*) as cnt FROM strikes WHERE expires_at > $1 GROUP BY member_id",
                now
            )
        }
        await conn.execute("DELETE FROM member_activity_excuses WHERE expires_at <= $1", now)
        excused_rows = await conn.fetch(
            "SELECT member_id, reason, expires_at FROM member_activity_excuses WHERE expires_at > $1",
            now,
        )

    tasks_map = {r['member_id']: r['tasks_completed'] for r in all_tasks if r['member_id'] in dept_member_ids}
    time_map = {r['member_id']: r['time_spent'] for r in all_time if r['member_id'] in dept_member_ids}
    excused_map = {
        r['member_id']: (r['reason'], r['expires_at'])
        for r in excused_rows
        if r['member_id'] in dept_member_ids
    }

    met, not_met, zero = [], [], []
    considered_ids = set(tasks_map.keys()) | set(time_map.keys())
    excused_members: list[tuple[discord.Member, str, datetime.datetime]] = []
    handled_excused_ids: set[int] = set()

    for member_id in considered_ids:
        member = guild.get_member(member_id)
        if not member:
            continue
        if member_id in excused_map:
            if member_id not in handled_excused_ids:
                reason, expires_at = excused_map[member_id]
                excused_members.append((member, reason, expires_at))
                handled_excused_ids.add(member_id)
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
            if mid in excused_map and mid not in handled_excused_ids:
                reason, expires_at = excused_map[mid]
                excused_members.append((member, reason, expires_at))
                handled_excused_ids.add(mid)
                continue
            sc = strike_counts.get(mid, 0)
            zero.append((member, sc))

    # Post report
    def fmt_met(lst):
        return ", ".join(f"{m.mention} (strikes: {sc})" for m, sc in lst) if lst else "—"

    def fmt_not_met(lst):
        return "\n".join(f"{m.mention} — {t}/{WEEKLY_REQUIREMENT} tasks, {mins}/{WEEKLY_TIME_REQUIREMENT} mins (strikes: {sc})" for m, t, mins, sc in lst) if lst else "—"

    def fmt_zero(lst):
        return ", ".join(f"{m.mention} (strikes: {sc})" for m, sc in lst) if lst else "—"

    def fmt_excused(lst):
        return "\n".join(
            f"{m.mention} — excused until {expires.strftime('%Y-%m-%d %H:%M UTC')} (Reason: {reason})"
            for m, reason, expires in lst
        ) if lst else "—"

    summary = f"--- Weekly Task Report (**{wk}**){' — EXCUSED' if excused_reason else ''} ---\n\n"
    if excused_reason:
        summary += f"**Excuse Reason:** {excused_reason}\n\n"
    summary += f"**✅ Met Requirement ({len(met)}):**\n{fmt_met(met)}\n\n"
    summary += f"**❌ Below Quota ({len(not_met)}):**\n{fmt_not_met(not_met)}\n\n"
    summary += f"**🚫 0 Activity ({len(zero)}):**\n{fmt_zero(zero)}\n\n"
    summary += f"**🟦 Excused ({len(excused_members)}):**\n{fmt_excused(excused_members)}\n\n"
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

# ---------- Orientation reminder loop ----------
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

# ---------- /rank with autocomplete ----------
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

# ---------- Register groups ----------
bot.tree.add_command(tasks_group)
bot.tree.add_command(orientation_group)
bot.tree.add_command(strikes_group)
bot.tree.add_command(excuses_group)
bot.tree.add_command(guidelines_group)

# ---------- Run ----------
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
