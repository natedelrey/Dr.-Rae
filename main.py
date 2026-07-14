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
from pathlib import Path
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
WEEKLY_QUOTA_CHANNEL_ID      = getenv_int("WEEKLY_QUOTA_CHANNEL_ID", 1474906766847246478)
LOG_CHANNEL_ID               = getenv_int("LOG_CHANNEL_ID")
ANNOUNCEMENT_ROLE_ID         = getenv_int("ANNOUNCEMENT_ROLE_ID")
MANAGEMENT_ROLE_ID           = getenv_int("MANAGEMENT_ROLE_ID")
DEPARTMENT_ROLE_ID           = getenv_int("DEPARTMENT_ROLE_ID")
MEDICAL_STUDENT_ROLE_ID      = getenv_int("MEDICAL_STUDENT_ROLE_ID")
ORIENTATION_ALERT_CHANNEL_ID = getenv_int("ORIENTATION_ALERT_CHANNEL_ID")
COMMAND_LOG_CHANNEL_ID       = getenv_int("COMMAND_LOG_CHANNEL_ID", 1416965696230789150)
ACTIVITY_LOG_CHANNEL_ID      = getenv_int("ACTIVITY_LOG_CHANNEL_ID", 1409646416829354095)
ROBLOX_AUDIT_LOG_CHANNEL_ID  = getenv_int("ROBLOX_AUDIT_LOG_CHANNEL_ID", COMMAND_LOG_CHANNEL_ID)
COMMS_CHANNEL_ID             = getenv_int("COMMS_CHANNEL_ID")
PROMOTION_ALERT_CHANNEL_ID   = getenv_int("PROMOTION_ALERT_CHANNEL_ID")
# DB / API
DATABASE_URL   = os.getenv("DATABASE_URL")
API_SECRET_KEY = os.getenv("API_SECRET_KEY")  # for /roblox webhook auth

# AI (application review)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # use OpenAI-compatible endpoint
AI_MODEL       = os.getenv("AI_MODEL", "gpt-4o-mini")
AI_BASE_URL    = os.getenv("AI_BASE_URL", "https://api.openai.com/v1")
GUIDELINES_FILE = os.getenv("GUIDELINES_FILE", "resources/guidelines.json")

# Tokens that should be expanded with extra context-specific synonyms when
# members use shorthand in their questions.
GUIDELINE_TOKEN_HINTS: dict[str, set[str]] = {
    # Common shorthand -> related handbook terms
    "cart": {"cure", "curecart", "cure-cart"},
    "cure cart": {"cure", "cart"},
    "curecart": {"cure", "cart"},
    "loa": {"leave", "absence"},
}

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
ROBLOX_GROUP_ID      = os.getenv("ROBLOX_GROUP_ID") or "745163328"  # optional, forwarded if present
# Rank manager role (can run /rank)
RANK_MANAGER_ROLE_ID = getenv_int("RANK_MANAGER_ROLE_ID", 1405979816120942702)

# Weekly configs
WEEKLY_TEST_REQUIREMENT = int(os.getenv("WEEKLY_TEST_REQUIREMENT", "1"))
WEEKLY_MISC_REQUIREMENT = int(os.getenv("WEEKLY_MISC_REQUIREMENT", "2"))
WEEKLY_TIME_REQUIREMENT = int(os.getenv("WEEKLY_TIME_REQUIREMENT", "20"))  # minutes

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
    "Anomaly Test",
    "Pharmacy",
    "Department of Medical Sciences Recruitment",
]

TASK_PLURALS = {
    "Checkup": "Checkups",
    "Interview": "Interviews",
    "Post-Op Interview": "Post-Op Interviews",
    "Anomaly Checkup": "Anomaly Checkups",
    "Department of Medical Sciences Recruitment": "Department of Medical Sciences Recruitments",
}

TASK_ROBUX_PAYOUTS = {
    "Interview": 20,
    "Post-Op Interview": 20,
    "Checkup": 35,
    "Anomaly Checkup": 100,
    "Anomaly Test": 100,
    "Pharmacy": 35,
    "Department of Medical Sciences Recruitment": 5,  # +200 bonus is manually verified by management
}

PROMOTION_TASK_ALIASES = {
    "associate_checkup": {
        "checkup",
        "check up",
        "check-up",
        "anomaly checkup",
        "anomaly check up",
        "anomaly check-up",
        "supervised checkup",
        "supervised check up",
        "supervised check-up",
    },
    "anomaly_test": {"anomaly test"},
    "interview": {"interview"},
    "pharmacy_counter_duty": {"pharmacy", "pharmacy counter duty"},
    "anomaly_checkup": {"anomaly checkup"},
    "specimen_testing": {"specimen testing"},
    "surgery": {"surgery"},
}

PROMOTION_RANK_ORDER = ["Associate", "Assistant Researcher", "Researcher", "Practitioner", "Research Advisor"]

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

def pretty_date(dt: datetime.datetime) -> str:
    day = dt.day
    if 10 <= day % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return dt.strftime(f"%B {day}{suffix}, %Y")

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
        base_path = Path(path)
        if not base_path.is_absolute():
            base_path = Path(__file__).resolve().parent / base_path
        self.path = str(base_path)
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
        question_lower = question.lower()
        tokens = set(re.findall(r"[a-zA-Z]{3,}", question_lower))
        expanded_tokens = set(tokens)
        for key, extras in GUIDELINE_TOKEN_HINTS.items():
            if key in tokens or key in question_lower:
                for extra in extras:
                    expanded_tokens.update(re.findall(r"[a-zA-Z]{3,}", extra.lower()))
        tokens = expanded_tokens
        scored: list[tuple[int, int]] = []
        for idx, section_tokens in enumerate(self.section_tokens):
            score = len(tokens & section_tokens)
            if score:
                scored.append((score, idx))
        scored.sort(key=lambda item: (-item[0], item[1]))

        parts: list[str] = []
        total_chars = 0

        def add_text(raw: str) -> None:
            nonlocal total_chars
            text = raw.strip()
            if not text or total_chars >= limit_chars:
                return
            remaining = limit_chars - total_chars
            snippet = text if len(text) <= remaining else text[:remaining]
            if not snippet:
                return
            parts.append(snippet)
            total_chars += len(snippet)

        # Always include a portion of the default handbook context so answers stay grounded.
        if self.default_context:
            base_limit = min(len(self.default_context), max(1, limit_chars // 2))
            add_text(self.default_context[:base_limit])

        seen: set[int] = set()
        for _, idx in scored[:max_sections]:
            if idx in seen:
                continue
            seen.add(idx)
            add_text(self.sections[idx]["text"])

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


def parse_embed_color(raw: str | None) -> discord.Color | None:
    """Parse a user-supplied color string into a discord.Color."""
    if not raw:
        return None

    text = raw.strip().lower()
    if not text:
        return None

    try:
        return discord.Color(int(text.strip("#"), 16))
    except Exception:
        pass

    if hasattr(discord.Color, text):
        try:
            return getattr(discord.Color, text)()
        except Exception:
            pass
    return None

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
        "Hello, congratulations on your acceptance to the **Department of Medical Sciences!**\n\n"
        ":one: Before you jump into anything, be sure that you familiarize yourself with the entirety of our "
        "[MD Trello](https://trello.com/b/j2jvme4Z/dms-information-hub) ! This contains critical information relevant "
        "to your gameplay, and your journey as whole here in the DMS.\n\n"
        ":two: After you've reviewed our guidelines, focus on getting your **Research Student Orientation** completed. "
        ":calendar_spiral: These are 15 minute sessions that have to be completed within your first 2 weeks of entry "
        "and can be booked with any member of management, or you can complete one instantly through our "
        "[Automatic Training Center](https://www.roblox.com/games/135840468925158/Medical-Student-Orientation-Center).\n\n"
        ":three: If you are interested in receiving **commission** for your scientific duty :money_with_wings:, "
        "we offer an [Outreach Program](https://www.roblox.com/communities/451852407/SCPF-Outreach-Program#!/about) "
        "that conducts payouts.\n\n"
        ":four: Familiarize yourself with myself—Dr. Rae! I will serve as your AI assistant throughout our journey, "
        "and you'll have to learn a few of my important commands if you want to succeed. :checkered_flag: The first "
        "step we'll take together is my **/verify command** with your ROBLOX username—this is to ensure your on-site "
        "activity is *always* accurately tracked.\n\n"
        "That's all for now, if you have any questions at all just message any management member or even your peers! "
        "We're happy to have you here :sparkling_heart:"
    )

    embed = discord.Embed(title="Welcome to the Team!", description=msg, color=discord.Color(0x9CBADD))
    embed.set_footer(text="Best,\nThe Department of Medical Sciences Management Team")
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
            "You are Dr. Rae, a friendly yet professional assistant for the Department of Medical Sciences. "
            "Rely exclusively on the provided guideline excerpts and any saved member background; treat them as your full "
            "knowledge base. "
            "If the excerpts do not contain the requested information, state that you are unsure and invite the member to "
            "check the handbook or provide more details via the `/guidelines context` command. "
            "Do not invent or reference real-world medical practices, Roblox platform rules, or anything outside the "
            "handbook. "
            "Do not quote large passages verbatim; instead, paraphrase and give clear action steps. "
            "Interpret shorthand, acronyms, or vague phrasing using the closest matching concept in the provided excerpts—"
            "for example, if a member mentions 'the cart', treat it as the Cure Cart whenever that appears in the handbook. "
            "Always remind members to follow official procedures if unsure and keep responses respectful. "
            f"Whenever a question touches on quota or activity expectations, spell out the standard expectation of "
            f"{WEEKLY_TIME_REQUIREMENT} minutes on-site, {WEEKLY_TEST_REQUIREMENT} test, and {WEEKLY_MISC_REQUIREMENT} miscellaneous tasks. "
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
            "Use only the facts that appear in the excerpts or saved context, even when interpreting shorthand. "
            "If an answer would require information that is not in the excerpts, say you are not sure and ask for more "
            "details or suggest reviewing the handbook section directly. "
            "Be very direct: lead with the core answer or required action before expanding with supporting details. "
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
            # Even if the DB is down, still attempt to register slash commands so the
            # tree stays current. The commands that rely on the DB will surface
            # their own errors when invoked.
            try:
                synced = await self.tree.sync()
                print(f"[Slash] Synced {len(synced)} command(s) (DB unavailable)")
            except Exception as sync_err:
                print(f"[Slash] Sync failed without DB: {sync_err}")
            return

        # Run bootstrap (schema, web server, slash sync)
        await self.ensure_bootstrap()

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
                    CREATE TABLE IF NOT EXISTS task_types (
                        task_type TEXT PRIMARY KEY,
                        enabled BOOLEAN NOT NULL DEFAULT TRUE,
                        robux_value INT NOT NULL DEFAULT 0,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
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
                await connection.execute("ALTER TABLE task_types ADD COLUMN IF NOT EXISTS robux_value INT NOT NULL DEFAULT 0;")
                await connection.execute("UPDATE weekly_task_logs SET task = COALESCE(task, task_type) WHERE task IS NULL;")
                await connection.execute("UPDATE task_logs SET task = COALESCE(task, task_type) WHERE task IS NULL;")
                await connection.executemany(
                    '''
                    INSERT INTO task_types (task_type, enabled, robux_value)
                    VALUES ($1, TRUE, $2)
                    ON CONFLICT (task_type) DO UPDATE
                    SET enabled = EXCLUDED.enabled,
                        robux_value = EXCLUDED.robux_value,
                        updated_at = now()
                    ''',
                    [(task_type, TASK_ROBUX_PAYOUTS.get(task_type, 0)) for task_type in TASK_TYPES]
                )
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
                    CREATE TABLE IF NOT EXISTS guideline_context (
                        discord_id BIGINT PRIMARY KEY,
                        details TEXT NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                ''')


            print("[DB] Tables ready.")

            if not self.web_runner:
                app = web.Application()
                app.router.add_get('/health', lambda _: web.Response(text='ok', status=200))
                app.router.add_post('/roblox', self.roblox_handler)
                app.router.add_post('/roblox/audit', self.roblox_audit_handler)
                self.web_runner = web.AppRunner(app)
                await self.web_runner.setup()
                self.web_site = web.TCPSite(self.web_runner, '0.0.0.0', 8080)
                await self.web_site.start()
                print("[Web] Server up on :8080 (GET /health, POST /roblox, POST /roblox/audit).")

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

    async def get_roblox_id(self, discord_id: int) -> int | None:
        if not self.db_pool:
            return None
        try:
            async with self.db_pool.acquire() as conn:
                return await conn.fetchval(
                    "SELECT roblox_id FROM roblox_verification WHERE discord_id = $1",
                    discord_id,
                )
        except Exception as e:
            print(f"[WARN] Failed to fetch Roblox ID for {discord_id}: {e}")
            return None

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

    async def roblox_audit_handler(self, request):
        print("[/roblox/audit] hit")
        if request.headers.get("X-Secret-Key") != API_SECRET_KEY:
            print("[/roblox/audit] 401 bad secret")
            return web.Response(status=401)

        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid_json"}, status=400)

        event = str(data.get("event") or data.get("action") or data.get("eventType") or "Audit Event")
        actor = str(data.get("actor") or data.get("author") or data.get("username") or "Unknown")
        target = str(data.get("target") or data.get("recipient") or data.get("targetUser") or "Unknown")

        amount = data.get("amount")
        amount_text = f"{amount:,} R$" if isinstance(amount, int) else str(amount or "Unknown")
        group_id = data.get("groupId") or ROBLOX_GROUP_ID or "Unknown"
        reason = str(data.get("reason") or data.get("description") or "No reason provided")
        occurred_at = str(data.get("timestamp") or data.get("createdAt") or utcnow().isoformat())

        ch = bot.get_channel(ROBLOX_AUDIT_LOG_CHANNEL_ID) if ROBLOX_AUDIT_LOG_CHANNEL_ID else None
        if not ch:
            print("[/roblox/audit] no channel configured")
            return web.json_response({"ok": False, "error": "channel_not_found"}, status=500)

        embed = discord.Embed(
            title="💸 Roblox Group Payout Logged",
            color=discord.Color.gold(),
            timestamp=utcnow(),
        )
        embed.add_field(name="Event", value=event[:1024], inline=True)
        embed.add_field(name="Amount", value=amount_text[:1024], inline=True)
        embed.add_field(name="Group", value=str(group_id)[:1024], inline=True)
        embed.add_field(name="By", value=actor[:1024], inline=True)
        embed.add_field(name="To", value=target[:1024], inline=True)
        embed.add_field(name="Occurred", value=occurred_at[:1024], inline=False)
        embed.add_field(name="Reason", value=reason[:1024], inline=False)

        raw_payload = json.dumps(data, ensure_ascii=False)
        if len(raw_payload) > 1024:
            raw_payload = raw_payload[:1021] + "..."
        embed.add_field(name="Payload", value=f"```json\n{raw_payload}\n```", inline=False)

        await ch.send(embed=embed)
        return web.json_response({"ok": True})


bot = MD_BOT()

# Pre-create command groups (kept from your original)
tasks_group = app_commands.Group(name="tasks", description="Commands for tracking Department of Medical Sciences tasks.")
orientation_group = app_commands.Group(name="orientation", description="Manage member orientation progress.")
strikes_group = app_commands.Group(name="strikes", description="Manage member strikes.")
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

# === PART 1/3 END ===
# Reply "next" and I'll send PART 2/3 with: /verify.
# main.py (Medical Department bot) — PART 2/3
# Continues directly from Part 1 — /verify.

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

# Application commands have been removed; onboarding is handled outside Dr. Rae.

# === PART 2/3 END ===
# Reply "next" to receive PART 3/3 — remaining commands (tasks/orientation/strikes/excuses), loops, /rank, and bot.run().
# main.py (Medical Department bot) — PART 3/3
# Continues directly from Part 2 — announcements, tasks, orientation/strikes/excuses, loops, /rank, and bot.run().

# ---------- Custom Embed Builder (Management-only) ----------
class EmbedContentModal(discord.ui.Modal, title="Edit Embed"):
    def __init__(self, builder_view: "EmbedBuilderView"):
        super().__init__(timeout=300)
        self.builder_view = builder_view

        self.title_input = discord.ui.TextInput(
            label="Title",
            default=self.builder_view.embed_data["title"],
            max_length=256,
            required=False,
        )
        self.description_input = discord.ui.TextInput(
            label="Description",
            style=discord.TextStyle.paragraph,
            default=self.builder_view.embed_data["description"],
            max_length=4000,
            required=False,
        )
        self.color_input = discord.ui.TextInput(
            label="Color (hex or Discord color name)",
            placeholder="#2b2d31 or blue",
            default=self.builder_view.embed_data.get("color_text", ""),
            required=False,
            max_length=20,
        )
        self.footer_input = discord.ui.TextInput(
            label="Footer",
            default=self.builder_view.embed_data["footer"],
            max_length=2048,
            required=False,
        )
        self.author_input = discord.ui.TextInput(
            label="Author name",
            default=self.builder_view.embed_data["author"],
            max_length=256,
            required=False,
        )
        self.author_icon_input = discord.ui.TextInput(
            label="Author icon URL",
            default=self.builder_view.embed_data.get("author_icon", ""),
            required=False,
            max_length=400,
        )
        self.thumbnail_input = discord.ui.TextInput(
            label="Thumbnail URL",
            default=self.builder_view.embed_data.get("thumbnail", ""),
            required=False,
            max_length=400,
        )
        self.image_input = discord.ui.TextInput(
            label="Image URL",
            default=self.builder_view.embed_data.get("image", ""),
            required=False,
            max_length=400,
        )

        for item in (
            self.title_input,
            self.description_input,
            self.color_input,
            self.footer_input,
            self.author_input,
            self.author_icon_input,
            self.thumbnail_input,
            self.image_input,
        ):
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction):
        color_obj = None
        color_text = self.color_input.value.strip()
        if color_text:
            color_obj = parse_embed_color(color_text)
            if not color_obj:
                await interaction.response.send_message(
                    "Color must be a hex code (e.g., #ffcc00) or a Discord color name (e.g., blue).",
                    ephemeral=True,
                )
                return

        self.builder_view.embed_data.update(
            {
                "title": self.title_input.value,
                "description": self.description_input.value,
                "footer": self.footer_input.value,
                "author": self.author_input.value,
                "author_icon": self.author_icon_input.value,
                "thumbnail": self.thumbnail_input.value,
                "image": self.image_input.value,
            }
        )
        if color_obj:
            self.builder_view.embed_data["color"] = color_obj
            self.builder_view.embed_data["color_text"] = color_text
        await self.builder_view.refresh(interaction)


class EmbedFieldModal(discord.ui.Modal, title="Add Field"):
    def __init__(self, builder_view: "EmbedBuilderView"):
        super().__init__(timeout=300)
        self.builder_view = builder_view

        self.name_input = discord.ui.TextInput(label="Field name", max_length=256, required=True)
        self.value_input = discord.ui.TextInput(
            label="Field value",
            style=discord.TextStyle.paragraph,
            max_length=1024,
            required=True,
        )
        self.inline_input = discord.ui.TextInput(
            label="Inline? (yes/no)",
            placeholder="yes",
            required=False,
            max_length=5,
        )

        for item in (self.name_input, self.value_input, self.inline_input):
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction):
        if len(self.builder_view.embed_data["fields"]) >= 25:
            await interaction.response.send_message("Embeds can only contain up to 25 fields.", ephemeral=True)
            return

        inline_raw = (self.inline_input.value or "no").strip().lower()
        inline = inline_raw in ("yes", "y", "true", "1", "inline")
        self.builder_view.embed_data["fields"].append(
            {"name": self.name_input.value, "value": self.value_input.value, "inline": inline}
        )
        await self.builder_view.refresh(interaction)


class EmbedBuilderView(discord.ui.View):
    def __init__(self, target_channel: discord.TextChannel, author: discord.Member):
        super().__init__(timeout=900)
        self.target_channel = target_channel
        self.author = author
        self.message: discord.Message | None = None
        self.embed_data: dict[str, Any] = {
            "title": "",
            "description": "",
            "footer": "",
            "author": author.display_name,
            "author_icon": getattr(author.display_avatar, "url", ""),
            "thumbnail": "",
            "image": "",
            "color": discord.Color.blurple(),
            "color_text": "blurple",
            "fields": [],
        }

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title=self.embed_data["title"] or None,
            description=self.embed_data["description"] or "Use the buttons below to customize this embed.",
            color=self.embed_data.get("color", discord.Color.blurple()),
            timestamp=utcnow(),
        )
        if self.embed_data["footer"]:
            embed.set_footer(text=self.embed_data["footer"])
        if self.embed_data["author"]:
            embed.set_author(name=self.embed_data["author"], icon_url=self.embed_data.get("author_icon") or discord.Embed.Empty)
        if self.embed_data.get("thumbnail"):
            embed.set_thumbnail(url=self.embed_data["thumbnail"])
        if self.embed_data.get("image"):
            embed.set_image(url=self.embed_data["image"])
        for field in self.embed_data["fields"]:
            embed.add_field(name=field["name"], value=field["value"], inline=field["inline"])
        return embed

    async def refresh(self, interaction: discord.Interaction):
        embed = self.build_embed()
        await interaction.response.edit_message(embed=embed, view=self)

    async def on_timeout(self):
        self.disable_all_items()
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass

    @discord.ui.button(label="Edit content", style=discord.ButtonStyle.primary)
    async def edit_content(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.send_modal(EmbedContentModal(self))

    @discord.ui.button(label="Add field", style=discord.ButtonStyle.secondary)
    async def add_field(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.send_modal(EmbedFieldModal(self))

    @discord.ui.button(label="Clear fields", style=discord.ButtonStyle.secondary)
    async def clear_fields(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not self.embed_data["fields"]:
            await interaction.response.send_message("There are no fields to clear.", ephemeral=True)
            return
        self.embed_data["fields"].clear()
        await self.refresh(interaction)

    @discord.ui.button(label="Send embed", style=discord.ButtonStyle.success)
    async def send_embed(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        embed = self.build_embed()
        try:
            await self.target_channel.send(embed=embed)
            await log_action(
                "Custom Embed Sent",
                f"Channel: {self.target_channel.mention}\nBy: {interaction.user.mention}\nTitle: {embed.title or 'Untitled'}",
            )
            await interaction.followup.send(
                f"Embed sent to {self.target_channel.mention}.", ephemeral=True
            )
        except Exception:
            await interaction.followup.send(
                "Failed to send embed. Please check my permissions or the channel.",
                ephemeral=True,
            )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button):
        self.disable_all_items()
        await interaction.response.edit_message(content="Embed builder closed.", embed=None, view=self)


@bot.tree.command(name="embedbuilder", description="(Mgmt) Build and send a customized embed.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
@app_commands.describe(channel="Channel where the embed will be sent (defaults to current channel)")
async def embedbuilder(interaction: discord.Interaction, channel: discord.TextChannel | None = None):
    target_channel = channel or interaction.channel
    if not isinstance(target_channel, discord.TextChannel):
        await interaction.response.send_message(
            "Please run this in a text channel or specify a valid text channel.",
            ephemeral=True,
        )
        return

    view = EmbedBuilderView(target_channel=target_channel, author=interaction.user)
    await interaction.response.send_message(
        content=f"Building an embed for {target_channel.mention}. Use the controls below.",
        embed=view.build_embed(),
        view=view,
        ephemeral=True,
    )
    view.message = await interaction.original_response()


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
async def fetch_enabled_task_types() -> list[str]:
    """Return enabled task types ordered for command autocompletes."""
    if not bot.db_pool:
        return list(TASK_TYPES)
    async with bot.db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT task_type FROM task_types WHERE enabled = TRUE ORDER BY task_type ASC"
        )
    return [r["task_type"] for r in rows]


async def get_task_type_robux_map(enabled_only: bool = False) -> dict[str, int]:
    query = "SELECT task_type, robux_value FROM task_types"
    if enabled_only:
        query += " WHERE enabled = TRUE"
    if not bot.db_pool:
        return {k.casefold(): v for k, v in TASK_ROBUX_PAYOUTS.items()}
    async with bot.db_pool.acquire() as conn:
        rows = await conn.fetch(query)
    return {(r["task_type"] or "").casefold(): int(r["robux_value"] or 0) for r in rows}


async def sync_weekly_task_counter(conn: asyncpg.Connection, member_id: int) -> int:
    """Keep the legacy weekly_tasks counter aligned with weekly_task_logs."""
    count = await conn.fetchval(
        "SELECT COUNT(*) FROM weekly_task_logs WHERE member_id = $1",
        member_id,
    )
    count = int(count or 0)
    if count:
        await conn.execute(
            "INSERT INTO weekly_tasks (member_id, tasks_completed) VALUES ($1, $2) "
            "ON CONFLICT (member_id) DO UPDATE SET tasks_completed = EXCLUDED.tasks_completed",
            member_id, count,
        )
    else:
        await conn.execute("DELETE FROM weekly_tasks WHERE member_id = $1", member_id)
    return count


async def task_type_autocomplete(
    interaction: discord.Interaction,
    current: str
) -> list[app_commands.Choice[str]]:
    del interaction
    try:
        task_types = await fetch_enabled_task_types()
    except Exception:
        task_types = list(TASK_TYPES)
    current_lower = current.lower().strip()
    filtered = [t for t in task_types if current_lower in t.lower()] if current_lower else task_types
    return [app_commands.Choice(name=t, value=t) for t in filtered[:25]]


async def is_valid_task_type(task_type: str) -> bool:
    task_types = await fetch_enabled_task_types()
    normalized = task_type.strip().casefold()
    return any(t.casefold() == normalized for t in task_types)


def is_test_task_type(task_type: str | None) -> bool:
    """Return whether a task type counts toward the weekly test requirement."""
    return "test" in _normalize_label(task_type)


def quota_status(test_count: int, misc_count: int, time_minutes: int) -> tuple[bool, str]:
    """Evaluate the active weekly quota and return a short progress string."""
    met = (
        time_minutes >= WEEKLY_TIME_REQUIREMENT
        and test_count >= WEEKLY_TEST_REQUIREMENT
        and misc_count >= WEEKLY_MISC_REQUIREMENT
    )
    progress = (
        f"{time_minutes}/{WEEKLY_TIME_REQUIREMENT} mins on-site, "
        f"{test_count}/{WEEKLY_TEST_REQUIREMENT} test, "
        f"{misc_count}/{WEEKLY_MISC_REQUIREMENT} miscellaneous tasks"
    )
    return met, progress


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
        max_length=4000,
    )

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
            tasks_completed = await sync_weekly_task_counter(conn, member_id)

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
        await maybe_send_promotion_alert(interaction.user)

@tasks_group.command(name="log", description="Log a completed task with proof and type.")
@app_commands.autocomplete(task_type=task_type_autocomplete)
async def tasks_log(interaction: discord.Interaction, task_type: str, proof: discord.Attachment):
    if not await is_valid_task_type(task_type):
        await interaction.response.send_message(
            "That task type is not enabled. Ask management to add it first.",
            ephemeral=True
        )
        return
    await interaction.response.send_modal(LogTaskForm(proof=proof, task_type=task_type))

@tasks_group.command(name="my", description="Check your weekly tasks and time.")
async def tasks_my(interaction: discord.Interaction):
    member_id = interaction.user.id
    async with bot.db_pool.acquire() as conn:
        weekly_rows = await conn.fetch(
            "SELECT COALESCE(NULLIF(task_type, ''), task) AS ttype "
            "FROM weekly_task_logs WHERE member_id = $1",
            member_id,
        )
        time_spent_seconds = await conn.fetchval("SELECT time_spent FROM roblox_time WHERE member_id = $1", member_id) or 0
        active_strikes     = await conn.fetchval("SELECT COUNT(*) FROM strikes WHERE member_id=$1 AND expires_at > $2", member_id, utcnow())
    time_spent_minutes = time_spent_seconds // 60
    test_count = sum(1 for row in weekly_rows if is_test_task_type(row["ttype"]))
    misc_count = len(weekly_rows) - test_count
    met_quota, progress = quota_status(test_count, misc_count, time_spent_minutes)
    status = "✅ Met" if met_quota else "❌ Below"
    await interaction.response.send_message(
        f"Weekly quota: **{status}** — {progress}. "
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
    robux_map = await get_task_type_robux_map(enabled_only=False)
    if not rows:
        await interaction.response.send_message(f"No tasks found for {target.display_name}.", ephemeral=True)
        return
    lines = []
    total_robux = 0
    for r in rows:
        base = r['ttype'] or "Uncategorized"
        label = TASK_PLURALS.get(base, base + ("s" if not base.endswith("s") else ""))
        robux_each = robux_map.get(base.casefold(), 0)
        subtotal = robux_each * int(r['cnt'])
        total_robux += subtotal
        lines.append(f"**{label}** — {r['cnt']} *(R${robux_each} each · R${subtotal} total)*")
    embed = discord.Embed(
        title=f"🗂️ Task Totals for {target.display_name}",
        description="\n".join(lines),
        color=discord.Color.blurple(),
        timestamp=utcnow()
    )
    embed.set_footer(text=f"Total tasks: {total} • Estimated Robux: R${total_robux}")
    await log_action("Viewed Tasks", f"Requester: {interaction.user.mention}\nTarget: {target.mention if target != interaction.user else 'self'}")
    await interaction.response.send_message(embed=embed, ephemeral=True)

@tasks_group.command(name="add", description="(Mgmt) Add tasks to a member's history and weekly totals.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
@app_commands.autocomplete(task_type=task_type_autocomplete)
async def tasks_add(
    interaction: discord.Interaction,
    member: discord.Member,
    task_type: str,
    count: app_commands.Range[int, 1, 100] = 1,
    comments: app_commands.Range[str, 0, 4000] | None = None,
    proof: discord.Attachment | None = None,
):
    if not await is_valid_task_type(task_type):
        await interaction.response.send_message(
            "That task type is not enabled. Use `/tasks type_add` first.",
            ephemeral=True
        )
        return

    now = utcnow()
    proof_url    = proof.url if proof else None
    comments_val = comments or "Added by management"
    robux_map = await get_task_type_robux_map(enabled_only=False)
    robux_each = robux_map.get(task_type.casefold(), TASK_ROBUX_PAYOUTS.get(task_type, 0))

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
            await sync_weekly_task_counter(conn, member.id)

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

    added_robux = int(robux_each) * int(count)
    desc = (
        f"Added **{count}× {task_type}** to {member.mention}.\n"
        f"Estimated payout added: **R${added_robux}** *(R${robux_each} each)*.\n\n**Now totals:**\n"
        + "\n".join(lines)
    )
    embed = discord.Embed(title="✅ Tasks Added", description=desc, color=discord.Color.green(), timestamp=utcnow())
    if proof_url:
        embed.set_image(url=proof_url)

    await log_action("Tasks Added", f"By: {interaction.user.mention}\nMember: {member.mention}\nType: **{task_type}** × {count}")
    await interaction.response.send_message(embed=embed, ephemeral=True)
    await maybe_send_promotion_alert(member)

@tasks_group.command(name="type_add", description="(Mgmt) Add a task type that members can log.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def tasks_type_add(
    interaction: discord.Interaction,
    task_type: app_commands.Range[str, 1, 80],
    robux_value: app_commands.Range[int, 0, 100000] = 0
):
    cleaned = " ".join(task_type.split()).strip()
    if not cleaned:
        await interaction.response.send_message("Please provide a valid task type name.", ephemeral=True)
        return

    async with bot.db_pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM task_types WHERE lower(task_type) = lower($1)",
            cleaned
        )
        if exists:
            await conn.execute(
                "UPDATE task_types SET enabled = TRUE, robux_value = $2, updated_at = now() WHERE lower(task_type) = lower($1)",
                cleaned, int(robux_value)
            )
            message = f"Enabled existing task type: **{cleaned}** with payout **R${robux_value}**."
        else:
            await conn.execute(
                "INSERT INTO task_types (task_type, enabled, robux_value) VALUES ($1, TRUE, $2)",
                cleaned, int(robux_value)
            )
            message = f"Added new task type: **{cleaned}** with payout **R${robux_value}**."

    await log_action("Task Type Added", f"By: {interaction.user.mention}\nTask Type: **{cleaned}**\nRobux: **R${robux_value}**")
    await interaction.response.send_message(message, ephemeral=True)

@tasks_group.command(name="type_remove", description="(Mgmt) Remove/disable a loggable task type.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
@app_commands.autocomplete(task_type=task_type_autocomplete)
async def tasks_type_remove(interaction: discord.Interaction, task_type: str):
    if task_type.casefold() == "medical department recruitment":
        await interaction.response.send_message(
            "You can't disable Department of Medical Sciences Recruitment because weekly payout logic depends on it.",
            ephemeral=True
        )
        return

    async with bot.db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT task_type, enabled FROM task_types WHERE lower(task_type) = lower($1)",
            task_type
        )
        if not row:
            await interaction.response.send_message("That task type does not exist.", ephemeral=True)
            return
        if not row["enabled"]:
            await interaction.response.send_message("That task type is already disabled.", ephemeral=True)
            return
        await conn.execute(
            "UPDATE task_types SET enabled = FALSE, updated_at = now() WHERE task_type = $1",
            row["task_type"]
        )

    await log_action("Task Type Removed", f"By: {interaction.user.mention}\nTask Type: **{row['task_type']}**")
    await interaction.response.send_message(f"Disabled task type: **{row['task_type']}**.", ephemeral=True)

@tasks_group.command(name="type_list", description="List currently enabled task types.")
async def tasks_type_list(interaction: discord.Interaction):
    if not bot.db_pool:
        task_rows = [(t, TASK_ROBUX_PAYOUTS.get(t, 0)) for t in TASK_TYPES]
    else:
        async with bot.db_pool.acquire() as conn:
            task_rows = await conn.fetch(
                "SELECT task_type, robux_value FROM task_types WHERE enabled = TRUE ORDER BY task_type ASC"
            )
    if not task_rows:
        await interaction.response.send_message("No task types are currently enabled.", ephemeral=True)
        return
    lines = "\n".join(
        f"• {row[0] if isinstance(row, tuple) else row['task_type']} — **R${row[1] if isinstance(row, tuple) else int(row['robux_value'] or 0)}**"
        for row in task_rows
    )
    embed = discord.Embed(
        title="🧾 Enabled Task Types",
        description=lines,
        color=discord.Color.blurple(),
        timestamp=utcnow()
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

@tasks_group.command(name="leaderboard", description="Displays the weekly leaderboard (tasks + on-site minutes).")
async def tasks_leaderboard(interaction: discord.Interaction):
    async with bot.db_pool.acquire() as conn:
        task_rows = await conn.fetch("SELECT member_id, COUNT(*) AS tasks_completed FROM weekly_task_logs GROUP BY member_id")
        time_rows = await conn.fetch("SELECT member_id, time_spent FROM roblox_time")

    task_map = {r['member_id']: int(r['tasks_completed'] or 0) for r in task_rows}
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
            new_count = await sync_weekly_task_counter(conn, member_id)
    await log_action("Removed Last Weekly Task", f"By: {interaction.user.mention}\nMember: {member.mention}\nRemoved: **{last_log['task']}**")
    await interaction.response.send_message(
        f"Removed last weekly task for {member.mention}: '{last_log['task']}'. They now have {new_count} tasks.",
        ephemeral=True
    )

@tasks_group.command(name="weekly_preview", description="(Mgmt+) Preview the weekly activity summary style without resetting data.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def tasks_weekly_preview(interaction: discord.Interaction):
    wk = week_key()
    now = utcnow()
    week_end = now + datetime.timedelta(days=(6 - now.weekday()))
    week_start = week_end - datetime.timedelta(days=6)

    preview = (
        f"--- Weekly Task Report (**{wk}**) ---\n"
        f"Week of **{pretty_date(week_start)}** to **{pretty_date(week_end)}**\n\n"
        "**✅ Met Requirement (0):**\n—\n\n"
        "**❌ Below Quota (0):**\n—\n\n"
        "**🚫 0 Activity (0):**\n—\n\n"
        "**🟦 Excused (0):**\n—\n\n"
        "*Robux totals and task counts are calculated from weekly log entries. Recruitment +200 promotion bonuses must be manually verified by management.*\n\n"
        "This is only a preview command for style testing; no data was reset."
    )

    embed = discord.Embed(
        title="Weekly Task Summary (Preview)",
        description=preview,
        color=discord.Color.from_str("#5aa9ff"),
        timestamp=utcnow(),
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

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

@orientation_group.command(name="pending", description="(Mgmt) View Medical Students who have not passed orientation.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def orientation_pending(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    if not MEDICAL_STUDENT_ROLE_ID:
        await interaction.response.send_message("MEDICAL_STUDENT_ROLE_ID is not configured.", ephemeral=True)
        return

    student_role = interaction.guild.get_role(MEDICAL_STUDENT_ROLE_ID)
    if not student_role:
        await interaction.response.send_message("Could not find the configured Medical Student role in this server.", ephemeral=True)
        return

    students = list(student_role.members)
    if not students:
        await interaction.response.send_message("There are no members with the Medical Student role.", ephemeral=True)
        return

    ids = [m.id for m in students]
    async with bot.db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT discord_id, deadline, passed FROM orientations WHERE discord_id = ANY($1::bigint[])",
            ids,
        )

    by_id = {int(row["discord_id"]): row for row in rows}
    pending_members: list[tuple[discord.Member, datetime.datetime | None]] = []

    for member in students:
        row = by_id.get(member.id)
        if not row or not row["passed"]:
            pending_members.append((member, row["deadline"] if row else None))

    if not pending_members:
        await interaction.response.send_message("All Medical Students have passed orientation. ✅", ephemeral=True)
        await log_action("Orientation Pending Viewed", f"By: {interaction.user.mention}\nPending count: 0")
        return

    pending_members.sort(key=lambda item: item[1] or datetime.datetime.max.replace(tzinfo=datetime.timezone.utc))
    lines = []
    for member, deadline in pending_members:
        if deadline:
            remaining = human_remaining(deadline - utcnow())
            deadline_text = f"{deadline.strftime('%Y-%m-%d %H:%M UTC')} ({remaining} remaining)"
        else:
            deadline_text = "No orientation record yet"
        lines.append(f"- {member.mention} ({member.display_name}) — {deadline_text}")

    header = f"**Pending orientation:** {len(pending_members)} member(s)"
    chunks = smart_chunk("\n".join(lines), size=1700)
    await interaction.response.send_message(f"{header}\n{chunks[0]}", ephemeral=True)
    for chunk in chunks[1:]:
        await interaction.followup.send(chunk, ephemeral=True)

    await log_action("Orientation Pending Viewed", f"By: {interaction.user.mention}\nPending count: {len(pending_members)}")

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
            f"You've received a strike. Reason: **{reason}**. "
            f"This will expire on **{expires.strftime('%Y-%m-%d')}**. "
            f"(**{active}/3 strikes**)"
        )
    except:
        pass

    await log_action("Strike Issued", f"Member: {member.mention}\nReason: {reason}\nAuto: {auto}\nActive now: **{active}/3**")
    return active

async def enforce_three_strikes(member: discord.Member):
    try:
        await member.send("You've been automatically removed from the Department of Medical Sciences for reaching **3/3 strikes**.")
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

# ---------- Weekly task summary + strikes + reset ----------
@tasks.loop(time=datetime.time(hour=4, minute=0, tzinfo=datetime.timezone.utc))
async def check_weekly_tasks():
    # Only fire on Sunday UTC
    if utcnow().weekday() != 6:
        return

    wk = week_key()
    now = utcnow()
    week_end = now
    week_start = week_end - datetime.timedelta(days=6)
    announcement_channel = bot.get_channel(WEEKLY_QUOTA_CHANNEL_ID)
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
        all_tasks = await conn.fetch(
            "SELECT member_id, COUNT(*) AS tasks_completed "
            "FROM weekly_task_logs GROUP BY member_id"
        )
        all_time = await conn.fetch("SELECT member_id, time_spent FROM roblox_time")
        payout_rows = await conn.fetch(
            "SELECT member_id, COALESCE(NULLIF(task_type, ''), task) AS ttype, COUNT(*) AS cnt "
            "FROM weekly_task_logs GROUP BY member_id, ttype"
        )
        db_robux_rows = await conn.fetch("SELECT task_type, robux_value FROM task_types")

    tasks_map = {r['member_id']: int(r['tasks_completed'] or 0) for r in all_tasks if r['member_id'] in dept_member_ids}
    time_map = {r['member_id']: r['time_spent'] for r in all_time if r['member_id'] in dept_member_ids}
    payout_lookup = {
        **{task_type.casefold(): value for task_type, value in TASK_ROBUX_PAYOUTS.items()},
        **{(r['task_type'] or '').casefold(): int(r['robux_value'] or 0) for r in db_robux_rows},
    }
    robux_map: dict[int, int] = {}
    task_breakdown_map: dict[int, list[tuple[str, int]]] = {}
    test_count_map: dict[int, int] = {}
    misc_count_map: dict[int, int] = {}
    for row in payout_rows:
        member_id = row['member_id']
        if member_id not in dept_member_ids:
            continue
        task_type = row['ttype'] or "Uncategorized"
        count = int(row['cnt'] or 0)
        task_breakdown_map.setdefault(member_id, []).append((task_type, count))
        if is_test_task_type(task_type):
            test_count_map[member_id] = test_count_map.get(member_id, 0) + count
        else:
            misc_count_map[member_id] = misc_count_map.get(member_id, 0) + count
        payout = payout_lookup.get(task_type.casefold(), 0)
        if payout:
            robux_map[member_id] = robux_map.get(member_id, 0) + (payout * count)
    for breakdown in task_breakdown_map.values():
        breakdown.sort(key=lambda item: (-item[1], item[0].casefold()))
    met, not_met, zero = [], [], []
    considered_ids = set(tasks_map.keys()) | set(time_map.keys())

    for member_id in considered_ids:
        member = guild.get_member(member_id)
        if not member:
            continue
        tasks_done = tasks_map.get(member_id, 0)
        time_done_minutes = (time_map.get(member_id, 0)) // 60
        robux_total = robux_map.get(member_id, 0)
        task_breakdown = task_breakdown_map.get(member_id, [])
        test_count = test_count_map.get(member_id, 0)
        misc_count = misc_count_map.get(member_id, 0)
        quota_met, progress = quota_status(test_count, misc_count, time_done_minutes)
        if quota_met:
            met.append((member, tasks_done, time_done_minutes, robux_total, task_breakdown, progress))
        else:
            not_met.append((member, tasks_done, time_done_minutes, robux_total, task_breakdown, progress))

    zero_ids = dept_member_ids - considered_ids
    for mid in zero_ids:
        member = guild.get_member(mid)
        if member:
            _, progress = quota_status(0, 0, 0)
            zero.append((member, robux_map.get(mid, 0), progress))

    # Post report
    def fmt_breakdown(task_breakdown: list[tuple[str, int]]) -> str:
        if not task_breakdown:
            return "no logged tasks"
        return ", ".join(f"{count}× {task_type}" for task_type, count in task_breakdown)

    def fmt_met(lst):
        return "\n".join(
            f"• {m.mention} | {robux}R$ — {progress} ({fmt_breakdown(breakdown)})"
            for m, t, mins, robux, breakdown, progress in lst
        ) if lst else "—"

    def fmt_not_met(lst):
        return "\n".join(
            f"• {m.mention} | {robux}R$ — {progress} ({fmt_breakdown(breakdown)})"
            for m, t, mins, robux, breakdown, progress in lst
        ) if lst else "—"

    def fmt_zero(lst):
        return "\n".join(f"• {m.mention} | {robux}R$ — {progress}" for m, robux, progress in lst) if lst else "—"

    summary = (
        f"--- Weekly Task Report (**{wk}**) ---\n"
        f"Week of **{pretty_date(week_start)}** to **{pretty_date(week_end)}**\n\n"
    )
    summary += f"**✅ Met Requirement ({len(met)}):**\n{fmt_met(met)}\n\n"
    summary += f"**❌ Below Quota ({len(not_met)}):**\n{fmt_not_met(not_met)}\n\n"
    summary += f"**🚫 0 Activity ({len(zero)}):**\n{fmt_zero(zero)}\n\n"
    summary += "*Robux totals and task counts are calculated from weekly log entries. Recruitment +200 promotion bonuses must be manually verified by management.*\n\n"
    summary += "Weekly counts will now be reset."

    await send_long_embed(
        target=announcement_channel,
        title="Weekly Task Summary",
        description=summary,
        color=discord.Color.from_str("#5aa9ff"),
        footer_text=None
    )

    for member, _tasks, _mins, _robux, _breakdown, progress in not_met:
        active_after = await issue_strike(member, f"Failed weekly quota ({progress})", set_by=None, auto=True)
        if active_after >= 3:
            await enforce_three_strikes(member)
    for member, _robux, progress in zero:
        active_after = await issue_strike(member, f"Failed weekly quota ({progress})", set_by=None, auto=True)
        if active_after >= 3:
            await enforce_three_strikes(member)

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
                            "Hi — this is an automatic notice from the Department of Medical Sciences.\n\n"
                            "Your **2-week orientation deadline** has passed and you have been **removed** due to not completing orientation in time.\n"
                            "If this is a mistake, please contact DMS Management."
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
def _normalize_label(value: str | None) -> str:
    return " ".join((value or "").replace("-", " ").split()).strip().casefold()


async def _count_matching_tasks(conn: asyncpg.Connection, member_id: int, labels: set[str]) -> int:
    rows = await conn.fetch("SELECT COALESCE(NULLIF(task_type, ''), task) AS ttype FROM task_logs WHERE member_id = $1", member_id)
    return sum(1 for r in rows if _normalize_label(r["ttype"]) in labels)


async def evaluate_promotion(member: discord.Member) -> tuple[str | None, str]:
    now = utcnow()
    async with bot.db_pool.acquire() as conn:
        orientation_row = await conn.fetchrow(
            "SELECT assigned_at, passed, passed_at FROM orientations WHERE discord_id=$1",
            member.id
        )
        assigned_at = orientation_row["assigned_at"] if orientation_row else None
        passed_orientation = (orientation_row["passed"] if orientation_row else False) or False
        passed_at = orientation_row["passed_at"] if orientation_row else None
        total_tasks = await conn.fetchval("SELECT COUNT(*) FROM task_logs WHERE member_id=$1", member.id) or 0
        strikes_30 = await conn.fetchval("SELECT COUNT(*) FROM strikes WHERE member_id=$1 AND issued_at >= $2", member.id, now - datetime.timedelta(days=30)) or 0
        strikes_60 = await conn.fetchval("SELECT COUNT(*) FROM strikes WHERE member_id=$1 AND issued_at >= $2", member.id, now - datetime.timedelta(days=60)) or 0
        week_rows = await conn.fetch("SELECT DISTINCT to_char(timestamp, 'IYYY-IW') AS week FROM task_logs WHERE member_id=$1", member.id)
        weeks_active = len([r["week"] for r in week_rows if r["week"]])
        assoc_req_count = await _count_matching_tasks(conn, member.id, PROMOTION_TASK_ALIASES["associate_checkup"])
        anomaly_test_count = await _count_matching_tasks(conn, member.id, PROMOTION_TASK_ALIASES["anomaly_test"])
        interview_count = await _count_matching_tasks(conn, member.id, PROMOTION_TASK_ALIASES["interview"])
        pharmacy_count = await _count_matching_tasks(conn, member.id, PROMOTION_TASK_ALIASES["pharmacy_counter_duty"])
        anomaly_checkup_count = await _count_matching_tasks(conn, member.id, PROMOTION_TASK_ALIASES["anomaly_checkup"])
        specimen_count = await _count_matching_tasks(conn, member.id, PROMOTION_TASK_ALIASES["specimen_testing"])
        surgery_count = await _count_matching_tasks(conn, member.id, PROMOTION_TASK_ALIASES["surgery"])
    start_dt = assigned_at or member.joined_at or passed_at
    days_in_dept = (now - start_dt).days if start_dt else 0
    if weeks_active >= 16 and days_in_dept >= 120 and strikes_60 == 0:
        return "Research Advisor", "16+ active weeks, 120+ days in department, and no disciplinary action in the last 60 days."
    if specimen_count >= 1 and surgery_count >= 1 and total_tasks >= 25 and days_in_dept >= 42 and strikes_30 == 0:
        return "Practitioner", "Specimen Testing + Surgery complete, 25+ total tasks, 42+ days in department, and no strikes in the last 30 days."
    if anomaly_test_count >= 1 and interview_count >= 1 and pharmacy_count >= 1 and anomaly_checkup_count >= 1 and weeks_active >= 2 and days_in_dept >= 14:
        return "Researcher", "Completed Anomaly Test, Interview, Pharmacy Counter Duty, and Anomaly Check-Up with 2+ active weeks and 14+ days in department."
    if passed_orientation and assoc_req_count >= 1:
        return "Assistant Researcher", "Passed orientation and completed at least one supervised/check-up task."
    return None, ""


class PromotionAlertView(discord.ui.View):
    def __init__(self, member_id: int, target_rank: str):
        super().__init__(timeout=None)
        self.member_id = member_id
        self.target_rank = target_rank

    @discord.ui.button(label="Auto-Rank", style=discord.ButtonStyle.success, emoji="⬆️")
    async def auto_rank(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        if MANAGEMENT_ROLE_ID and not any(r.id == MANAGEMENT_ROLE_ID for r in interaction.user.roles):
            await interaction.response.send_message("You need management permissions to auto-rank.", ephemeral=True)
            return
        member = interaction.guild.get_member(self.member_id) if interaction.guild else None
        if not member:
            await interaction.response.send_message("Member is no longer in this server.", ephemeral=True)
            return
        roblox_id = await bot.get_roblox_id(member.id)
        if not roblox_id:
            await interaction.response.send_message("Member is not Roblox-verified.", ephemeral=True)
            return
        ranks = await fetch_group_ranks()
        target = next((r for r in ranks if _normalize_label(r.get("name")) == _normalize_label(self.target_rank)), None)
        if not target:
            await interaction.response.send_message(f"Could not find Roblox rank '{self.target_rank}'.", ephemeral=True)
            return
        ok = await set_group_rank(int(roblox_id), role_id=int(target["id"]))
        if not ok:
            await interaction.response.send_message("Failed to update Roblox rank.", ephemeral=True)
            return
        async with bot.db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO member_ranks (discord_id, rank, set_by, set_at) VALUES ($1, $2, $3, $4) "
                "ON CONFLICT (discord_id) DO UPDATE SET rank = EXCLUDED.rank, set_by = EXCLUDED.set_by, set_at = EXCLUDED.set_at",
                member.id, target["name"], interaction.user.id, utcnow()
            )
        for role in member.guild.roles:
            if _normalize_label(role.name) == _normalize_label(target["name"]):
                await member.add_roles(role, reason=f"Auto-ranked via promotion alert by {interaction.user}")
                break
        await interaction.response.send_message(f"✅ Ranked {member.mention} to **{target['name']}**.", ephemeral=True)


async def maybe_send_promotion_alert(member: discord.Member):
    if not PROMOTION_ALERT_CHANNEL_ID or not bot.db_pool:
        return
    target_rank, requirement_text = await evaluate_promotion(member)
    if not target_rank:
        return
    current_rank = await bot.resolve_member_rank(member)
    cur_idx = PROMOTION_RANK_ORDER.index(current_rank) if current_rank in PROMOTION_RANK_ORDER else -1
    target_idx = PROMOTION_RANK_ORDER.index(target_rank)
    if target_idx <= cur_idx:
        return
    channel = bot.get_channel(PROMOTION_ALERT_CHANNEL_ID)
    if not channel:
        try:
            channel = await bot.fetch_channel(PROMOTION_ALERT_CHANNEL_ID)
        except Exception as e:
            print(f"[promotion-alert] Failed to fetch channel {PROMOTION_ALERT_CHANNEL_ID}: {e}")
            return
    if not isinstance(channel, discord.abc.Messageable):
        print(f"[promotion-alert] Channel {PROMOTION_ALERT_CHANNEL_ID} is not messageable.")
        return
    embed = discord.Embed(
        title="🌸 Promotion Requirement Met",
        description=f"{member.mention} has reach the promotion requirements for **{target_rank}**!\n\nPlease review the requirements of ({requirement_text}).",
        color=discord.Color.pink(),
        timestamp=utcnow(),
    )
    try:
        await channel.send(embed=embed, view=PromotionAlertView(member.id, target_rank))
    except Exception as e:
        print(f"[promotion-alert] Failed to send alert for member {member.id}: {e}")

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
