# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════╗
║         🤖  ربات مدیریت حرفه‌ای گروه‌های تلگرام  🤖           ║
║                    Group Manager Bot                         ║
║         Telethon + PostgreSQL + Flask + asyncio              ║
║  ✨ ایموجی پرمیوم + واسطه + مدیریت گروه‌ها + سیستم پیروی ✨  ║
║           🚀 آماده استقرار روی Render (Web Service)          ║
╚══════════════════════════════════════════════════════════════╝
"""

import os
import re
import asyncio
import logging
import threading
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque

import socks
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from flask import Flask
from telethon import TelegramClient, events, Button
from telethon.tl.functions.channels import (
    EditBannedRequest,
    GetParticipantRequest,
    GetParticipantsRequest,
)
from telethon.tl.types import (
    ChatBannedRights,
    ChannelParticipantCreator,
    ChannelParticipantAdmin,
    ChannelParticipantsAdmins,
    User,
)
from telethon.errors import (
    ChatAdminRequiredError,
    UserAdminInvalidError,
    MessageNotModifiedError,
)

# ═════════════════════════════════════════════
# ۱) بارگذاری متغیرهای محیطی
# ═════════════════════════════════════════════
load_dotenv()

API_ID = int(os.getenv("API_ID", "6") or 6)
API_HASH = os.getenv("API_HASH", "eb06d4abfb49dc3eeb1aeb98ae0f581e").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)
SECOND_ADMIN_ID = int(os.getenv("SECOND_ADMIN_ID", "0") or 0)
THIRD_ADMIN_ID = int(os.getenv("THIRD_ADMIN_ID", "0") or 0)
SESSION_NAME = os.getenv("SESSION_NAME", "group_manager_bot")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

_log_channel = os.getenv("LOG_CHANNEL_ID", "").strip()
LOG_CHANNEL_ID = int(_log_channel) if _log_channel.lstrip("-").isdigit() else None

PROXY_HOST = os.getenv("PROXY_HOST", "").strip()
PROXY_PORT = int(os.getenv("PROXY_PORT", "0") or 0)

IRAN_TZ = timezone(timedelta(hours=3, minutes=30))
MAX_MUTE_SECONDS = 366 * 86400
DEFAULT_MAX_WARNINGS = 3

SUPER_ADMINS = [uid for uid in (OWNER_ID, SECOND_ADMIN_ID, THIRD_ADMIN_ID) if uid]

# ═════════════════════════════════════════════
# تنظیمات قابلیت واسطه
# ═════════════════════════════════════════════
MEDIATOR_CHAT_ID = 1004337969779
MEDIATOR_KEYWORD = "واسطه"
MEDIATOR_USERNAMES = ["@mAMmA2222", "@mamad_slayer"]
MEDIATOR_COOLDOWN_SECONDS = 30

_MEDIATOR_COOLDOWN = {}
_MEDIATOR_REQUESTS = {}


# ═════════════════════════════════════════════
# ۲) تنظیمات logging
# ═════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("GroupManagerBot")
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.WARNING)


# ═════════════════════════════════════════════
# ۳) ساخت کلاینت تلگرام
# ═════════════════════════════════════════════
if PROXY_HOST and PROXY_PORT:
    logger.info(f"🌐 اتصال از طریق پروکسی SOCKS5: {PROXY_HOST}:{PROXY_PORT}")
    client = TelegramClient(
        SESSION_NAME, API_ID, API_HASH,
        proxy=(socks.SOCKS5, PROXY_HOST, PROXY_PORT, True, None, None),
    )
else:
    logger.info("🌐 اتصال مستقیم (بدون پروکسی) - مناسب برای Render")
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)


# ═════════════════════════════════════════════
# ۴) کلاس مدیریت دیتابیس PostgreSQL
# ═════════════════════════════════════════════
class Database:
    def __init__(self, db_url: str):
        if not db_url:
            raise ValueError("❌ DATABASE_URL تنظیم نشده است.")
        self.db_url = db_url
        self.conn = psycopg2.connect(db_url)
        self.conn.autocommit = True
        self._create_tables()

    def _cursor(self):
        return self.conn.cursor(cursor_factory=RealDictCursor)

    def _create_tables(self):
        with self._cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS admins (
                    user_id  BIGINT NOT NULL,
                    group_id BIGINT NOT NULL,
                    added_by BIGINT,
                    added_at TEXT,
                    PRIMARY KEY (user_id, group_id)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bans (
                    user_id   BIGINT NOT NULL,
                    group_id  BIGINT NOT NULL,
                    reason    TEXT,
                    banned_by BIGINT,
                    banned_at TEXT,
                    is_active INTEGER DEFAULT 1,
                    PRIMARY KEY (user_id, group_id)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mutes (
                    user_id   BIGINT NOT NULL,
                    group_id  BIGINT NOT NULL,
                    reason    TEXT,
                    muted_by  BIGINT,
                    muted_at  TEXT,
                    until     TEXT,
                    is_active INTEGER DEFAULT 1,
                    PRIMARY KEY (user_id, group_id)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS warnings (
                    user_id        BIGINT NOT NULL,
                    group_id       BIGINT NOT NULL,
                    count          INTEGER DEFAULT 0,
                    last_warned_by BIGINT,
                    last_warned_at TEXT,
                    PRIMARY KEY (user_id, group_id)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS logs (
                    id        SERIAL PRIMARY KEY,
                    action    TEXT NOT NULL,
                    target_id BIGINT,
                    admin_id  BIGINT,
                    group_id  BIGINT,
                    reason    TEXT,
                    timestamp TEXT
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bot_groups (
                    group_id  BIGINT PRIMARY KEY,
                    title     TEXT,
                    username  TEXT,
                    added_at  TEXT,
                    last_seen TEXT
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS followed_users (
                    user_id     BIGINT NOT NULL,
                    group_id    BIGINT NOT NULL,
                    user_name   TEXT,
                    username    TEXT,
                    is_followed INTEGER DEFAULT 1,
                    added_at    TEXT,
                    PRIMARY KEY (user_id, group_id)
                );
            """)

    # ───────────── ادمین‌ها ─────────────
    def add_admin(self, user_id, group_id, added_by):
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO admins (user_id, group_id, added_by, added_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id, group_id) DO UPDATE
                SET added_by = EXCLUDED.added_by, added_at = EXCLUDED.added_at
            """, (user_id, group_id, added_by, datetime.now(IRAN_TZ).isoformat()))

    def remove_admin(self, user_id, group_id):
        with self._cursor() as cur:
            cur.execute("DELETE FROM admins WHERE user_id=%s AND group_id=%s", (user_id, group_id))
            return cur.rowcount > 0

    def is_admin(self, user_id, group_id):
        with self._cursor() as cur:
            cur.execute(
                "SELECT 1 FROM admins WHERE user_id=%s AND (group_id=%s OR group_id=0)",
                (user_id, group_id),
            )
            return cur.fetchone() is not None

    def get_admins(self, group_id=None):
        with self._cursor() as cur:
            if group_id is not None:
                cur.execute("SELECT * FROM admins WHERE group_id=%s ORDER BY added_at DESC", (group_id,))
            else:
                cur.execute("SELECT * FROM admins ORDER BY added_at DESC")
            return [dict(r) for r in cur.fetchall()]

    def count_admins(self):
        with self._cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM admins")
            return cur.fetchone()["c"]

    # ───────────── بن ─────────────
    def add_ban(self, user_id, group_id, reason, banned_by):
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO bans (user_id, group_id, reason, banned_by, banned_at, is_active)
                VALUES (%s, %s, %s, %s, %s, 1)
                ON CONFLICT (user_id, group_id) DO UPDATE
                SET reason = EXCLUDED.reason, banned_by = EXCLUDED.banned_by,
                    banned_at = EXCLUDED.banned_at, is_active = 1
            """, (user_id, group_id, reason, banned_by, datetime.now(IRAN_TZ).isoformat()))

    def remove_ban(self, user_id, group_id):
        with self._cursor() as cur:
            cur.execute("UPDATE bans SET is_active=0 WHERE user_id=%s AND group_id=%s", (user_id, group_id))

    def get_bans(self, group_id=None):
        with self._cursor() as cur:
            if group_id is not None:
                cur.execute("SELECT * FROM bans WHERE group_id=%s AND is_active=1 ORDER BY banned_at DESC", (group_id,))
            else:
                cur.execute("SELECT * FROM bans WHERE is_active=1 ORDER BY banned_at DESC")
            return [dict(r) for r in cur.fetchall()]

    def count_bans(self):
        with self._cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM bans WHERE is_active=1")
            return cur.fetchone()["c"]

    # ───────────── میوت ─────────────
    def add_mute(self, user_id, group_id, reason, muted_by, until_iso):
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO mutes (user_id, group_id, reason, muted_by, muted_at, until, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, 1)
                ON CONFLICT (user_id, group_id) DO UPDATE
                SET reason = EXCLUDED.reason, muted_by = EXCLUDED.muted_by,
                    muted_at = EXCLUDED.muted_at, until = EXCLUDED.until, is_active = 1
            """, (user_id, group_id, reason, muted_by, datetime.now(IRAN_TZ).isoformat(), until_iso))

    def remove_mute(self, user_id, group_id):
        with self._cursor() as cur:
            cur.execute("UPDATE mutes SET is_active=0 WHERE user_id=%s AND group_id=%s", (user_id, group_id))

    def get_mutes(self, group_id=None):
        with self._cursor() as cur:
            if group_id is not None:
                cur.execute("SELECT * FROM mutes WHERE group_id=%s AND is_active=1 ORDER BY muted_at DESC", (group_id,))
            else:
                cur.execute("SELECT * FROM mutes WHERE is_active=1 ORDER BY muted_at DESC")
            return [dict(r) for r in cur.fetchall()]

    def count_mutes(self):
        with self._cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM mutes WHERE is_active=1")
            return cur.fetchone()["c"]

    def cleanup_expired_mutes(self):
        now = datetime.now(IRAN_TZ).isoformat()
        with self._cursor() as cur:
            cur.execute(
                "UPDATE mutes SET is_active=0 WHERE is_active=1 AND until IS NOT NULL AND until < %s",
                (now,),
            )

    # ───────────── اخطار ─────────────
    def add_warning(self, user_id, group_id, warned_by):
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO warnings (user_id, group_id, count, last_warned_by, last_warned_at)
                VALUES (%s, %s, 1, %s, %s)
                ON CONFLICT (user_id, group_id) DO UPDATE
                SET count = warnings.count + 1,
                    last_warned_by = EXCLUDED.last_warned_by,
                    last_warned_at = EXCLUDED.last_warned_at
            """, (user_id, group_id, warned_by, datetime.now(IRAN_TZ).isoformat()))
            cur.execute("SELECT count FROM warnings WHERE user_id=%s AND group_id=%s", (user_id, group_id))
            row = cur.fetchone()
            return row["count"] if row else 0

    def remove_warning(self, user_id, group_id):
        with self._cursor() as cur:
            cur.execute("DELETE FROM warnings WHERE user_id=%s AND group_id=%s", (user_id, group_id))

    def get_warning_count(self, user_id, group_id):
        with self._cursor() as cur:
            cur.execute("SELECT count FROM warnings WHERE user_id=%s AND group_id=%s", (user_id, group_id))
            row = cur.fetchone()
            return row["count"] if row else 0

    def get_warnings(self, group_id=None):
        with self._cursor() as cur:
            if group_id is not None:
                cur.execute("SELECT * FROM warnings WHERE group_id=%s AND count>0 ORDER BY last_warned_at DESC", (group_id,))
            else:
                cur.execute("SELECT * FROM warnings WHERE count>0 ORDER BY last_warned_at DESC")
            return [dict(r) for r in cur.fetchall()]

    def count_warnings(self):
        with self._cursor() as cur:
            cur.execute("SELECT COALESCE(SUM(count), 0) AS c FROM warnings WHERE count>0")
            return cur.fetchone()["c"]

    # ───────────── لاگ ─────────────
    def add_log(self, action, target_id, admin_id, group_id, reason):
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO logs (action, target_id, admin_id, group_id, reason, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (action, target_id, admin_id, group_id, reason, datetime.now(IRAN_TZ).isoformat()))

    def count_logs(self):
        with self._cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM logs")
            return cur.fetchone()["c"]

    # ───────────── تنظیمات ─────────────
    def set_setting(self, key, value):
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO settings (key, value) VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """, (key, str(value)))

    def get_setting(self, key, default=None):
        with self._cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key=%s", (key,))
            row = cur.fetchone()
            return row["value"] if row else default

    def get_bool_setting(self, key, default=False):
        v = self.get_setting(key)
        if v is None:
            return default
        return str(v).lower() in ("1", "true", "yes", "on", "روشن")

    # ───────────── گروه‌های ربات ─────────────
    def save_group(self, group_id, title, username=None):
        now = datetime.now(IRAN_TZ).isoformat()
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO bot_groups (group_id, title, username, added_at, last_seen)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (group_id) DO UPDATE
                SET title = EXCLUDED.title,
                    username = EXCLUDED.username,
                    last_seen = EXCLUDED.last_seen
            """, (group_id, title, username, now, now))

    def get_bot_groups(self):
        with self._cursor() as cur:
            cur.execute("SELECT * FROM bot_groups ORDER BY last_seen DESC")
            return [dict(r) for r in cur.fetchall()]

    def get_bot_group(self, group_id):
        with self._cursor() as cur:
            cur.execute("SELECT * FROM bot_groups WHERE group_id=%s", (group_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    # ───────────── افراد مورد پیروی ─────────────
    def add_followed_user(self, user_id, group_id, user_name, username=None, is_followed=1):
        now = datetime.now(IRAN_TZ).isoformat()
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO followed_users (user_id, group_id, user_name, username, is_followed, added_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, group_id) DO UPDATE
                SET user_name = EXCLUDED.user_name,
                    username = EXCLUDED.username
            """, (user_id, group_id, user_name, username, is_followed, now))

    def get_followed_users(self, group_id):
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM followed_users WHERE group_id=%s ORDER BY is_followed DESC, user_name ASC",
                (group_id,),
            )
            return [dict(r) for r in cur.fetchall()]

    def get_follow_state(self, user_id, group_id):
        with self._cursor() as cur:
            cur.execute(
                "SELECT is_followed FROM followed_users WHERE user_id=%s AND group_id=%s",
                (user_id, group_id),
            )
            row = cur.fetchone()
            return row["is_followed"] if row else None

    def toggle_follow(self, user_id, group_id):
        with self._cursor() as cur:
            cur.execute(
                "SELECT is_followed FROM followed_users WHERE user_id=%s AND group_id=%s",
                (user_id, group_id),
            )
            row = cur.fetchone()
            if row is None:
                return None
            new_state = 0 if row["is_followed"] == 1 else 1
            cur.execute(
                "UPDATE followed_users SET is_followed=%s WHERE user_id=%s AND group_id=%s",
                (new_state, user_id, group_id),
            )
            return new_state


db = Database(DATABASE_URL)


# ═════════════════════════════════════════════
# ۵) ایموجی‌های پرمیوم
# ═════════════════════════════════════════════
PREMIUM_EMOJI = {
    "ban": "5260293700088511294",
    "unban": "5206607081334906820",
    "mute": "5224736245665511429",
    "unmute": "5337080053119336309",
    "admin": "5397782960512444700",
    "stats": "5231200819986047254",
    "settings": "6337048821603763745",
    "crown": "5458603043203327669",
    "user": "5443038326535759644",
    "id": "5397782960512444700",
    "group": "5447410659077661506",
    "reason": "5436113877181941026",
    "time": "5458603043203327669",
    "link": "5271604874419647061",
    "warning": "5447644880824181073",
    "info": "5323442290708985472",
    "cross": "5210952531676504517",
    "check": "5206607081334906820",
    "fire": "5424972470023104089",
    "star": "6337048821603763745",
    "list": "5447410659077661506",
    "shield": "5397782960512444700",
    "clock": "5458603043203327669",
    "handshake": "5447410659077661506",
    "message": "5443038326535759644",
    "alert": "5447644880824181073",
    "hourglass": "5224736245665511429",
    "green": "5206607081334906820",
    "red": "5210952531676504517",
    "target": "5397782960512444700",
    "tag": "5436113877181941026",
    "diamond": "5424972470023104089",
    "announce": "5231200819986047254",
    "sparkles": "6337048821603763745",
    "wave": "5447410659077661506",
    "rocket": "5424972470023104089",
    "building": "5447410659077661506",
    "eye": "5397782960512444700",
    "thumbs_up": "5337080053119336309",
    "thumbs_down": "5210952531676504517",
}


def prem(key: str, fallback: str) -> str:
    emoji_id = PREMIUM_EMOJI.get(key)
    if emoji_id:
        return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'
    return fallback


# ═════════════════════════════════════════════
# ۶) توابع کمکی
# ═════════════════════════════════════════════
COMMAND_KEYWORDS = {
    "unwarn": ["unwarn", "removewarn", "حذف اخطار", "حذف هشدار", "رفع اخطار"],
    "unmute": ["unmute", "آنمیوت", "آن میوت", "انمیوت", "ان میوت"],
    "unban": ["unban", "آنبن", "آن بن", "انبن", "ان بن"],
    "warn": ["warn", "اخطار", "هشدار"],
    "mute": ["mute", "silence", "سکوت", "میوت"],
    "ban": ["ban", "مسدود", "بن", "سیک", "صیک", "سیکش", "صیکش"],
}


def parse_command(text):
    if not text:
        return None
    raw = text.strip()
    lower = raw.lower()
    for cmd in ("unwarn", "unmute", "unban", "warn", "mute", "ban"):
        for kw in COMMAND_KEYWORDS[cmd]:
            k = kw.lower()
            if lower.startswith(k):
                if len(raw) > len(k) and not raw[len(k)].isspace():
                    continue
                return cmd, raw[len(k):].strip()
    return None


def parse_duration(text):
    text = (text or "").strip()
    if not text:
        return 3600, ""
    parts = text.split(None, 1)
    first = parts[0]
    rest = parts[1].strip() if len(parts) > 1 else ""
    m = re.match(r"^(\d+)([smhdSMHD])?$", first)
    if m:
        num = int(m.group(1))
        unit = (m.group(2) or "m").lower()
        mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
        seconds = num * mult
        if seconds > MAX_MUTE_SECONDS:
            seconds = MAX_MUTE_SECONDS
        if seconds <= 0:
            seconds = 60
        return seconds, rest
    return 3600, text


def humanize_duration(seconds):
    if seconds < 60:
        return f"{seconds} ثانیه"
    if seconds < 3600:
        return f"{seconds // 60} دقیقه"
    if seconds < 86400:
        hrs = seconds // 3600
        mins = (seconds % 3600) // 60
        return f"{hrs} ساعت" + (f" و {mins} دقیقه" if mins else "")
    days = seconds // 86400
    hrs = (seconds % 86400) // 3600
    return f"{days} روز" + (f" و {hrs} ساعت" if hrs else "")


def h(text):
    if text is None:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def now_iran_str():
    return datetime.now(IRAN_TZ).strftime("%Y/%m/%d - %H:%M:%S")


def user_display(user):
    if user is None:
        return "ناشناس"
    name = getattr(user, "first_name", None) or ""
    last = getattr(user, "last_name", None) or ""
    full = (name + " " + last).strip()
    return full or (getattr(user, "username", None) or str(getattr(user, "id", "?")))


def build_message_link(chat_id, message_id):
    if message_id is None:
        return None
    s = str(chat_id)
    if s.startswith("-100"):
        return f"https://t.me/c/{s[4:]}/{message_id}"
    return None


def format_report(action_title, action_emoji_key, target, admin_user, chat,
                  reason, link, target_message=None):
    target_name = user_display(target)
    target_username = getattr(target, "username", None)
    target_id_link = f'<a href="tg://user?id={target.id}">{target.id}</a>'
    target_username_txt = (
        f'<a href="https://t.me/{target_username}">@{target_username}</a>'
        if target_username else "—"
    )
    admin_name = user_display(admin_user)
    admin_username = getattr(admin_user, "username", None)
    admin_id_link = f'<a href="tg://user?id={admin_user.id}">{admin_user.id}</a>'
    admin_username_txt = (
        f'<a href="https://t.me/{admin_username}">@{admin_username}</a>'
        if admin_username else "—"
    )
    chat_title = getattr(chat, "title", "—") or "—"
    chat_username = getattr(chat, "username", None)
    chat_title_txt = (
        f'<a href="https://t.me/{chat_username}">{h(chat_title)}</a>'
        if chat_username else h(chat_title)
    )
    reason_txt = h(reason) if reason else "—"
    link_txt = f'<a href="{link}">اینجا کلیک کنید</a>' if link else "—"

    msg_section = ""
    if target_message:
        preview = target_message.strip()
        if len(preview) > 400:
            preview = preview[:400] + "..."
        msg_section = (
            "\n┏━━━ " + prem('message', '📝') + " <b>پیام کاربر هدف</b> ━━━┓\n"
            f"┃ <blockquote>{h(preview)}</blockquote>\n"
            "┗━━━━━━━━━━━━━━━━━━━━┛\n"
        )

    return (
        "╔══════════════════════════════════╗\n"
        f"   {prem(action_emoji_key, '📢')} <b>گزارش عملیات جدید</b> {prem(action_emoji_key, '📢')}\n"
        "╚══════════════════════════════════╝\n\n"
        f"◆ {prem('fire', '🔥')} <b>نوع:</b> {action_title}\n"
        f"◆ {prem('time', '⏱')} <b>زمان:</b> <code>{now_iran_str()}</code>\n\n"
        "┏━━━ " + prem('user', '👤') + " <b>کاربر هدف</b> ━━━┓\n"
        f"┃ {prem('tag', '🏷️')} <b>نام:</b> {h(target_name)}\n"
        f"┃ {prem('id', '🆔')} <b>آیدی:</b> {target_id_link}\n"
        f"┃ {prem('link', '🔗')} <b>یوزرنیم:</b> {target_username_txt}\n"
        "┗━━━━━━━━━━━━━━━━━━━━┛\n\n"
        "┏━━━ " + prem('admin', '🛡') + " <b>توسط ادمین</b> ━━━┓\n"
        f"┃ {prem('tag', '🏷️')} <b>نام:</b> {h(admin_name)}\n"
        f"┃ {prem('id', '🆔')} <b>آیدی:</b> {admin_id_link}\n"
        f"┃ {prem('link', '🔗')} <b>یوزرنیم:</b> {admin_username_txt}\n"
        "┗━━━━━━━━━━━━━━━━━━━━┛\n\n"
        "┏━━━ " + prem('group', '📌') + " <b>گروه</b> ━━━┓\n"
        f"┃ {prem('tag', '🏷️')} <b>نام:</b> {chat_title_txt}\n"
        f"┃ {prem('id', '🆔')} <b>آیدی:</b> <code>{chat.id}</code>\n"
        f"┃ {prem('reason', '💬')} <b>دلیل:</b> {reason_txt}\n"
        f"┃ {prem('link', '🔗')} <b>پیام:</b> {link_txt}\n"
        "┗━━━━━━━━━━━━━━━━━━━━┛\n"
        f"{msg_section}"
        "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <i>Group Manager Bot</i>"
    )


async def is_telegram_admin(chat_id, user_id):
    try:
        p = await client(GetParticipantRequest(chat_id, user_id))
        return isinstance(p.participant, (ChannelParticipantCreator, ChannelParticipantAdmin))
    except Exception:
        return False


async def is_admin_or_owner(chat_id, user_id):
    if user_id in SUPER_ADMINS:
        return True
    if db.is_admin(user_id, chat_id):
        return True
    return await is_telegram_admin(chat_id, user_id)


async def can_execute(chat_id, user_id, user_obj=None):
    """
    آیا کاربر مجاز به اجرای دستورات است؟
    - ادمین‌های ارشد همیشه مجاز
    - اگه کاربر توی followed_users هست، وضعیت پیروی چک می‌شه
    - اگه ثبت نشده، چک می‌شه آیا ادمین تلگرامه. اگه بود، خودکار اضافه می‌شه (followed=1)
    """
    if user_id in SUPER_ADMINS:
        return True

    state = db.get_follow_state(user_id, chat_id)
    if state is not None:
        return state == 1

    # چک ادمین تلگرام
    if await is_telegram_admin(chat_id, user_id):
        name = user_display(user_obj) if user_obj else str(user_id)
        uname = getattr(user_obj, "username", None) if user_obj else None
        db.add_followed_user(user_id, chat_id, name, uname, is_followed=1)
        return True

    # چک ادمین ثبت‌شده قدیمی
    if db.is_admin(user_id, chat_id):
        return True

    return False


async def get_reply_target(event):
    if not event.is_reply:
        return None
    try:
        reply = await event.get_reply_message()
        if not reply:
            return None
        sender = await reply.get_sender()
        if not isinstance(sender, User):
            return None
        return sender
    except Exception:
        return None


async def get_replied_message_text(event):
    if not event.is_reply:
        return None
    try:
        reply = await event.get_reply_message()
        if not reply:
            return None
        txt = reply.text or reply.message or ""
        if not txt and reply.media:
            return "📎 [محتوای مدیا]"
        return txt.strip() or None
    except Exception:
        return None


def is_mediator_group(chat_id, raw_chat_id=None):
    candidates = set()

    def add_candidates(val):
        try:
            s = str(abs(int(val)))
            candidates.add(int(s))
            if s.startswith("100") and len(s) > 10:
                candidates.add(int(s[3:]))
            else:
                candidates.add(int("100" + s))
        except Exception:
            pass

    add_candidates(chat_id)
    if raw_chat_id is not None:
        add_candidates(raw_chat_id)

    return MEDIATOR_CHAT_ID in candidates


def is_mediator_request(raw_text):
    return (raw_text or "").strip() == MEDIATOR_KEYWORD


# ═════════════════════════════════════════════
# ۷) ارسال گزارش
# ═════════════════════════════════════════════
async def send_report(text, buttons=None):
    for admin_id in SUPER_ADMINS:
        try:
            await client.send_message(
                admin_id, text, parse_mode="html",
                link_preview=False, buttons=buttons,
            )
        except Exception as ex:
            logger.error(f"خطا در ارسال گزارش به {admin_id}: {ex}")

    if LOG_CHANNEL_ID:
        try:
            await client.send_message(
                LOG_CHANNEL_ID, text, parse_mode="html", link_preview=False
            )
        except Exception as ex:
            logger.error(f"خطا در ارسال گزارش به کانال: {ex}")


# ═════════════════════════════════════════════
# ۸) سیستم واسطه خودکار
# ═════════════════════════════════════════════
async def handle_mediator_request(event, chat, sender, replied_target=None):
    now_ts = datetime.now(timezone.utc).timestamp()
    uid = sender.id if sender else event.sender_id

    last_ts = _MEDIATOR_COOLDOWN.get(uid)
    if last_ts and (now_ts - last_ts) < MEDIATOR_COOLDOWN_SECONDS:
        remaining = int(MEDIATOR_COOLDOWN_SECONDS - (now_ts - last_ts))
        try:
            await event.reply(
                f"{prem('hourglass', '⏳')} <b>لطفاً {remaining} ثانیه دیگر دوباره تلاش کنید.</b>",
                parse_mode="html",
            )
        except Exception:
            pass
        return
    _MEDIATOR_COOLDOWN[uid] = now_ts

    sender_name = user_display(sender) if sender else "ناشناس"
    sender_username = getattr(sender, "username", None) if sender else None
    sender_id_link = f'<a href="tg://user?id={uid}">{uid}</a>'
    sender_username_link = (
        f'<a href="https://t.me/{sender_username}">@{sender_username}</a>'
        if sender_username else "—"
    )

    target_section_group = ""
    target_section_admin = ""
    if replied_target is not None:
        tgt_name = user_display(replied_target)
        tgt_username = getattr(replied_target, "username", None)
        tgt_id_link = f'<a href="tg://user?id={replied_target.id}">{replied_target.id}</a>'
        tgt_username_link = (
            f'<a href="https://t.me/{tgt_username}">@{tgt_username}</a>'
            if tgt_username else "—"
        )
        target_section_group = (
            "\n┏━━━ " + prem('target', '🎯') + " <b>طرف مقابل</b> ━━━┓\n"
            f"┃ {prem('tag', '🏷️')} <b>نام:</b> {h(tgt_name)}\n"
            f"┃ {prem('id', '🆔')} <b>آیدی:</b> {tgt_id_link}\n"
            f"┃ {prem('link', '🔗')} <b>یوزرنیم:</b> {tgt_username_link}\n"
            "┗━━━━━━━━━━━━━━━━━━━━┛\n"
        )
        target_section_admin = target_section_group

    group_reply = (
        "╔══════════════════════════════════╗\n"
        f"   {prem('handshake', '🤝')} <b>درخواست واسطه ثبت شد</b> {prem('handshake', '🤝')}\n"
        "╚══════════════════════════════════╝\n\n"
        "┏━━━ " + prem('user', '👤') + " <b>درخواست‌دهنده</b> ━━━┓\n"
        f"┃ {prem('tag', '🏷️')} <b>نام:</b> {h(sender_name)}\n"
        f"┃ {prem('id', '🆔')} <b>آیدی:</b> {sender_id_link}\n"
        f"┃ {prem('link', '🔗')} <b>یوزرنیم:</b> {sender_username_link}\n"
        "┗━━━━━━━━━━━━━━━━━━━━┛"
        f"{target_section_group}"
        "\n"
        f"{prem('star', '⭐')} <b>واسطه‌های رسمی:</b>\n"
        f"┃ {prem('user', '👤')} {MEDIATOR_USERNAMES[0]}\n"
        f"┃ {prem('user', '👤')} {MEDIATOR_USERNAMES[1]}\n\n"
        f"{prem('clock', '⏱')} <b>لطفاً صبر کنید...</b>\n"
        f"به‌زودی یکی از واسطه‌ها با شما تماس می‌گیرد. {prem('check', '✅')}"
    )
    try:
        await event.reply(group_reply, parse_mode="html")
    except Exception as ex:
        logger.error(f"خطا در ارسال پیام واسطه در گروه: {ex}")

    chat_title = getattr(chat, "title", "—") or "—"
    chat_username = getattr(chat, "username", None)
    chat_title_txt = (
        f'<a href="https://t.me/{chat_username}">{h(chat_title)}</a>'
        if chat_username else h(chat_title)
    )
    link = build_message_link(chat.id, event.id)

    admin_msg = (
        "╔══════════════════════════════════╗\n"
        f"   {prem('alert', '🚨')} <b>درخواست واسطه جدید</b> {prem('alert', '🚨')}\n"
        "╚══════════════════════════════════╝\n\n"
        f"◆ {prem('fire', '🔥')} <b>نوع:</b> درخواست واسطه\n"
        f"◆ {prem('time', '⏱')} <b>زمان:</b> <code>{now_iran_str()}</code>\n\n"
        "┏━━━ " + prem('user', '👤') + " <b>درخواست‌دهنده</b> ━━━┓\n"
        f"┃ {prem('tag', '🏷️')} <b>نام:</b> {h(sender_name)}\n"
        f"┃ {prem('id', '🆔')} <b>آیدی:</b> {sender_id_link}\n"
        f"┃ {prem('link', '🔗')} <b>یوزرنیم:</b> {sender_username_link}\n"
        "┗━━━━━━━━━━━━━━━━━━━━┛"
        f"{target_section_admin}"
        "\n┏━━━ " + prem('group', '📌') + " <b>گروه</b> ━━━┓\n"
        f"┃ {prem('tag', '🏷️')} <b>نام:</b> {chat_title_txt}\n"
        f"┃ {prem('id', '🆔')} <b>آیدی:</b> <code>{chat.id}</code>\n"
        f"┃ {prem('link', '🔗')} <b>لینک پیام:</b> "
        f"{f'<a href=\"{link}\">اینجا کلیک کنید</a>' if link else '—'}\n"
        "┗━━━━━━━━━━━━━━━━━━━━┛\n\n"
        f"{prem('warning', '⚠️')} <b>از دکمه‌های زیر برای تایید یا رد استفاده کنید:</b>"
    )

    target_uid = replied_target.id if replied_target else 0
    request_id = f"{uid}_{int(now_ts)}"

    buttons = [[
        Button.inline("✅ تایید", data=f"med_a:{request_id}".encode()),
        Button.inline("❌ رد", data=f"med_r:{request_id}".encode()),
    ]]

    sent_messages = []
    for admin_id in SUPER_ADMINS:
        try:
            msg = await client.send_message(
                admin_id, admin_msg, parse_mode="html",
                link_preview=False, buttons=buttons,
            )
            sent_messages.append((admin_id, msg.id))
        except Exception as ex:
            logger.error(f"خطا در ارسال گزارش واسطه به {admin_id}: {ex}")

    if LOG_CHANNEL_ID:
        try:
            msg = await client.send_message(
                LOG_CHANNEL_ID, admin_msg, parse_mode="html",
                link_preview=False, buttons=buttons,
            )
            sent_messages.append((LOG_CHANNEL_ID, msg.id))
        except Exception as ex:
            logger.error(f"خطا در ارسال گزارش واسطه به کانال: {ex}")

    _MEDIATOR_REQUESTS[request_id] = {
        "messages": sent_messages,
        "requester": uid,
        "requester_name": sender_name,
        "target": target_uid,
        "target_name": user_display(replied_target) if replied_target else None,
        "status": "pending",
        "admin_name": None,
        "admin_id": None,
        "chat_title": chat_title,
        "chat_id": chat.id,
        "timestamp": now_iran_str(),
    }

    db.add_log("mediator-request", uid, 0, chat.id, "درخواست واسطه")


# ═════════════════════════════════════════════
# ۹) ضد اسپم و ضد لینک
# ═════════════════════════════════════════════
_SPAM_TRACKER = defaultdict(lambda: deque(maxlen=20))
SPAM_THRESHOLD = 6
SPAM_WINDOW_SECONDS = 8
LINK_REGEX = re.compile(
    r"(https?://\S+|t\.me/\S+|telegram\.me/\S+|www\.\S+)", re.IGNORECASE
)


async def check_antispam(event, chat, sender):
    if not db.get_bool_setting("anti_spam", False):
        return
    if sender is None or getattr(sender, "bot", False):
        return
    if await is_admin_or_owner(chat.id, sender.id):
        return
    now = datetime.now(timezone.utc).timestamp()
    key = (chat.id, sender.id)
    dq = _SPAM_TRACKER[key]
    while dq and now - dq[0] > SPAM_WINDOW_SECONDS:
        dq.popleft()
    dq.append(now)

    if len(dq) >= SPAM_THRESHOLD:
        dq.clear()
        try:
            until = datetime.now(timezone.utc) + timedelta(minutes=5)
            rights = ChatBannedRights(
                until_date=until, send_messages=True, send_media=True,
                send_stickers=True, send_gifs=True, send_games=True,
                send_inline=True, embed_links=True,
            )
            await client(EditBannedRequest(chat.id, sender.id, rights))
            await event.reply(
                f"{prem('alert', '🚨')} <b>کاربر {h(user_display(sender))} به دلیل ارسال پیام‌های پیاپی، "
                f"به مدت ۵ دقیقه میوت شد.</b>",
                parse_mode="html",
            )
            db.add_log("antispam-mute", sender.id, 0, chat.id, "اسپم خودکار")
        except Exception as ex:
            logger.warning(f"ضد اسپم: میوت ناموفق - {ex}")


async def check_antilink(event, chat, sender):
    if not db.get_bool_setting("anti_link", False):
        return
    if not event.raw_text:
        return
    if await is_admin_or_owner(chat.id, sender.id):
        return
    if LINK_REGEX.search(event.raw_text):
        try:
            await event.delete()
            await event.respond(
                f"{prem('link', '🔗')} <b>پیام حاوی لینک کاربر {h(user_display(sender))} حذف شد.</b>",
                parse_mode="html",
            )
        except Exception as ex:
            logger.warning(f"ضد لینک: حذف ناموفق - {ex}")


# ═════════════════════════════════════════════
# ۱۰) اجرای عملیات
# ═════════════════════════════════════════════
def _full_ban_rights():
    return ChatBannedRights(
        until_date=None, view_messages=True, send_messages=True, send_media=True,
        send_stickers=True, send_gifs=True, send_games=True, send_inline=True,
        embed_links=True,
    )


def _mute_rights(until_dt):
    return ChatBannedRights(
        until_date=until_dt, send_messages=True, send_media=True,
        send_stickers=True, send_gifs=True, send_games=True, send_inline=True,
        embed_links=True,
    )


def _free_rights():
    return ChatBannedRights(until_date=None)


async def do_ban(event, chat, target, reason, admin_user, target_msg=None):
    try:
        await client(EditBannedRequest(chat.id, target.id, _full_ban_rights()))
    except (ChatAdminRequiredError, UserAdminInvalidError):
        await event.reply(
            f"{prem('warning', '⚠️')} <b>ربات دسترسی کافی برای بن کردن کاربر را ندارد.</b>",
            parse_mode="html",
        )
        return
    except Exception as ex:
        logger.exception(f"خطا در بن کردن: {ex}")
        await event.reply(
            f"{prem('warning', '⚠️')} <b>خطا در بن کردن کاربر:</b>\n<code>{h(ex)}</code>",
            parse_mode="html",
        )
        return

    db.add_ban(target.id, chat.id, reason or "", admin_user.id)
    db.add_log("ban", target.id, admin_user.id, chat.id, reason or "")

    reason_txt = f"\n{prem('reason', '💬')} <b>دلیل:</b> {h(reason)}" if reason else ""
    await event.reply(
        f"{prem('ban', '🚫')} <b>کاربر {h(user_display(target))} با موفقیت بن شد.</b>{reason_txt}",
        parse_mode="html",
    )

    link = build_message_link(chat.id, event.reply_to_msg_id or event.id)
    report = format_report("بن کردن کاربر", "ban", target, admin_user, chat,
                           reason, link, target_message=target_msg)
    await send_report(report)


async def do_unban(event, chat, target, reason, admin_user):
    try:
        await client(EditBannedRequest(chat.id, target.id, _free_rights()))
    except (ChatAdminRequiredError, UserAdminInvalidError):
        await event.reply(
            f"{prem('warning', '⚠️')} <b>ربات دسترسی کافی برای آنبن کردن کاربر را ندارد.</b>",
            parse_mode="html",
        )
        return
    except Exception as ex:
        logger.exception(f"خطا در آنبن کردن: {ex}")
        await event.reply(
            f"{prem('warning', '⚠️')} <b>خطا در آنبن کردن کاربر:</b>\n<code>{h(ex)}</code>",
            parse_mode="html",
        )
        return

    db.remove_ban(target.id, chat.id)
    db.remove_mute(target.id, chat.id)
    db.remove_warning(target.id, chat.id)
    db.add_log("unban", target.id, admin_user.id, chat.id, reason or "")

    reason_txt = f"\n{prem('reason', '💬')} <b>دلیل:</b> {h(reason)}" if reason else ""
    await event.reply(
        f"{prem('unban', '✅')} <b>کاربر {h(user_display(target))} آنبن شد و اخطارهایش پاک شد.</b>{reason_txt}",
        parse_mode="html",
    )

    link = build_message_link(chat.id, event.reply_to_msg_id or event.id)
    report = format_report("آنبن کردن کاربر", "unban", target, admin_user, chat, reason, link)
    await send_report(report)


async def do_mute(event, chat, target, reason, admin_user, seconds, target_msg=None):
    if seconds > MAX_MUTE_SECONDS:
        seconds = MAX_MUTE_SECONDS
    until_dt = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    try:
        await client(EditBannedRequest(chat.id, target.id, _mute_rights(until_dt)))
    except (ChatAdminRequiredError, UserAdminInvalidError):
        await event.reply(
            f"{prem('warning', '⚠️')} <b>ربات دسترسی کافی برای میوت کردن کاربر را ندارد.</b>",
            parse_mode="html",
        )
        return
    except Exception as ex:
        logger.exception(f"خطا در میوت کردن: {ex}")
        await event.reply(
            f"{prem('warning', '⚠️')} <b>خطا در میوت کردن کاربر:</b>\n<code>{h(ex)}</code>",
            parse_mode="html",
        )
        return

    until_iran = (datetime.now(IRAN_TZ) + timedelta(seconds=seconds)).isoformat()
    db.add_mute(target.id, chat.id, reason or "", admin_user.id, until_iran)
    db.add_log("mute", target.id, admin_user.id, chat.id, reason or "")

    duration_txt = humanize_duration(seconds)
    reason_txt = f"\n{prem('reason', '💬')} <b>دلیل:</b> {h(reason)}" if reason else ""
    await event.reply(
        f"{prem('mute', '🔇')} <b>کاربر {h(user_display(target))} به مدت {duration_txt} میوت شد.</b>"
        f"{reason_txt}",
        parse_mode="html",
    )

    link = build_message_link(chat.id, event.reply_to_msg_id or event.id)
    full_reason = f"{duration_txt}" + (f" - {reason}" if reason else "")
    report = format_report(
        f"میوت کردن کاربر ({duration_txt})", "mute", target, admin_user,
        chat, full_reason, link, target_message=target_msg,
    )
    await send_report(report)


async def do_unmute(event, chat, target, reason, admin_user):
    try:
        await client(EditBannedRequest(chat.id, target.id, _free_rights()))
    except (ChatAdminRequiredError, UserAdminInvalidError):
        await event.reply(
            f"{prem('warning', '⚠️')} <b>ربات دسترسی کافی برای آن‌میوت کردن کاربر را ندارد.</b>",
            parse_mode="html",
        )
        return
    except Exception as ex:
        logger.exception(f"خطا در آن‌میوت کردن: {ex}")
        await event.reply(
            f"{prem('warning', '⚠️')} <b>خطا در آن‌میوت کردن کاربر:</b>\n<code>{h(ex)}</code>",
            parse_mode="html",
        )
        return

    db.remove_mute(target.id, chat.id)
    db.add_log("unmute", target.id, admin_user.id, chat.id, reason or "")

    reason_txt = f"\n{prem('reason', '💬')} <b>دلیل:</b> {h(reason)}" if reason else ""
    await event.reply(
        f"{prem('unmute', '🔊')} <b>کاربر {h(user_display(target))} آن‌میوت شد.</b>{reason_txt}",
        parse_mode="html",
    )

    link = build_message_link(chat.id, event.reply_to_msg_id or event.id)
    report = format_report("آن‌میوت کردن کاربر", "unmute", target, admin_user, chat, reason, link)
    await send_report(report)


async def do_warn(event, chat, target, reason, admin_user, target_msg=None):
    try:
        max_warns = int(db.get_setting("max_warnings", str(DEFAULT_MAX_WARNINGS)) or DEFAULT_MAX_WARNINGS)
    except Exception:
        max_warns = DEFAULT_MAX_WARNINGS

    new_count = db.add_warning(target.id, chat.id, admin_user.id)
    db.add_log("warn", target.id, admin_user.id, chat.id, reason or "")

    if new_count >= max_warns:
        ban_success = False
        try:
            await client(EditBannedRequest(chat.id, target.id, _full_ban_rights()))
            ban_success = True
        except (ChatAdminRequiredError, UserAdminInvalidError):
            ban_success = False
        except Exception as ex:
            logger.exception(f"خطا در بن خودکار: {ex}")
            ban_success = False

        if ban_success:
            db.remove_warning(target.id, chat.id)
            db.add_ban(target.id, chat.id, f"بن خودکار پس از {max_warns} اخطار" + (f" - {reason}" if reason else ""), admin_user.id)
            db.add_log("auto-ban", target.id, admin_user.id, chat.id, f"پس از {max_warns} اخطار")

            await event.reply(
                f"{prem('ban', '🚫')} <b>کاربر {h(user_display(target))} به دلیل رسیدن به "
                f"{max_warns} اخطار، به صورت خودکار بن شد!</b>",
                parse_mode="html",
            )

            link = build_message_link(chat.id, event.reply_to_msg_id or event.id)
            full_reason = f"بن خودکار پس از {max_warns} اخطار" + (f" - {reason}" if reason else "")
            report = format_report(
                f"اخطار {max_warns}ام و بن خودکار", "ban", target, admin_user,
                chat, full_reason, link, target_message=target_msg,
            )
            await send_report(report)
        else:
            await event.reply(
                f"{prem('warning', '⚠️')} <b>کاربر {h(user_display(target))} به {max_warns} اخطار رسید، "
                f"اما ربات نتوانست او را بن کند (دسترسی کافی ندارد).</b>",
                parse_mode="html",
            )
    else:
        remaining = max_warns - new_count
        reason_txt = f"\n┃ {prem('reason', '💬')} <b>دلیل:</b> {h(reason)}" if reason else ""
        await event.reply(
            "┏━━━ " + prem('warning', '⚠️') + " <b>اخطار جدید</b> ━━━┓\n"
            f"┃ {prem('user', '👤')} <b>کاربر:</b> {h(user_display(target))}\n"
            f"┃ 📊 <b>اخطارها:</b> <code>{new_count}/{max_warns}</code>\n"
            f"┃ {prem('hourglass', '⏳')} <b>تا بن خودکار:</b> <code>{remaining}</code> اخطار دیگر"
            f"{reason_txt}\n"
            "┗━━━━━━━━━━━━━━━━━━━━┛",
            parse_mode="html",
        )

        link = build_message_link(chat.id, event.reply_to_msg_id or event.id)
        full_reason = f"اخطار {new_count}/{max_warns}" + (f" - {reason}" if reason else "")
        report = format_report(
            f"اخطار دادن به کاربر ({new_count}/{max_warns})",
            "warning", target, admin_user, chat, full_reason, link,
            target_message=target_msg,
        )
        await send_report(report)


async def do_unwarn(event, chat, target, reason, admin_user):
    prev_count = db.get_warning_count(target.id, chat.id)
    db.remove_warning(target.id, chat.id)
    db.add_log("unwarn", target.id, admin_user.id, chat.id, reason or "")

    reason_txt = f"\n{prem('reason', '💬')} <b>دلیل:</b> {h(reason)}" if reason else ""
    await event.reply(
        f"{prem('check', '✅')} <b>اخطارهای کاربر {h(user_display(target))} پاک شد.</b>\n"
        f"┃ 📊 <b>اخطارهای پاک شده:</b> <code>{prev_count}</code>{reason_txt}",
        parse_mode="html",
    )

    link = build_message_link(chat.id, event.reply_to_msg_id or event.id)
    report = format_report(
        f"حذف اخطار کاربر (قبلاً {prev_count} اخطار)",
        "check", target, admin_user, chat, reason, link,
    )
    await send_report(report)


# ═════════════════════════════════════════════
# ۱۱) سینک ادمین‌های گروه از تلگرام
# ═════════════════════════════════════════════
async def sync_group_admins(group_id):
    """همه ادمین‌های گروه رو از تلگرام می‌گیره و توی دیتابیس ثبت می‌کنه"""
    try:
        result = await client(GetParticipantsRequest(
            channel=group_id,
            filter=ChannelParticipantsAdmins(),
            offset=0,
            limit=200,
            hash=0,
        ))
        added = 0
        updated = 0
        for participant in result.participants:
            try:
                user = getattr(participant, "user", None)
                if user is None:
                    uid = getattr(participant, "user_id", None)
                    if uid is None:
                        continue
                    try:
                        user = await client.get_entity(uid)
                    except Exception:
                        continue
                if not isinstance(user, User):
                    continue
                uid = user.id
                name = user_display(user)
                uname = getattr(user, "username", None)

                existing = db.get_follow_state(uid, group_id)
                if existing is None:
                    db.add_followed_user(uid, group_id, name, uname, is_followed=1)
                    added += 1
                else:
                    db.add_followed_user(uid, group_id, name, uname, is_followed=existing)
                    updated += 1
            except Exception as ex:
                logger.debug(f"خطا در پردازش ادمین: {ex}")
        logger.info(f"✅ sync admins {group_id}: +{added} new, ~{updated} updated")
        return added
    except ChatAdminRequiredError:
        logger.warning(f"⛔ ربات ادمین گروه {group_id} نیست - نمی‌تونه لیست ادمین‌ها رو بگیره")
        return 0
    except Exception as ex:
        logger.warning(f"خطا در گرفتن ادمین‌های گروه {group_id}: {ex}")
        return 0


# ═════════════════════════════════════════════
# ۱۲) هندلر اصلی گروه
# ═════════════════════════════════════════════
async def group_handler(event):
    try:
        if event.is_private:
            return
        chat = await event.get_chat()
        if not hasattr(chat, "title"):
            return

        me = await client.get_me()
        if event.sender_id == me.id:
            return

        # ذخیره اطلاعات گروه
        try:
            db.save_group(chat.id, getattr(chat, "title", "—"), getattr(chat, "username", None))
        except Exception as ex:
            logger.debug(f"save_group error: {ex}")

        raw_text = (event.raw_text or "").strip()

        # ═══ اول: بررسی درخواست واسطه ═══
        if is_mediator_group(event.chat_id, chat.id) and is_mediator_request(raw_text):
            try:
                sender = await event.get_sender()
            except Exception:
                sender = None
            replied_target = await get_reply_target(event)
            await handle_mediator_request(event, chat, sender, replied_target=replied_target)
            return

        # ═══ دوم: بررسی مجاز بودن برای اجرای دستور ═══
        try:
            sender = await event.get_sender()
        except Exception:
            sender = None

        can_cmd = await can_execute(chat.id, event.sender_id, user_obj=sender)

        if not can_cmd:
            if sender and not getattr(sender, "bot", False):
                try:
                    await check_antilink(event, chat, sender)
                except Exception as ex:
                    logger.debug(f"antilink error: {ex}")
                try:
                    await check_antispam(event, chat, sender)
                except Exception as ex:
                    logger.debug(f"antispam error: {ex}")
            return

        # ═══ از اینجا: کاربر مجاز ═══
        parsed = parse_command(raw_text)
        if not parsed:
            return
        cmd, rest = parsed

        target = await get_reply_target(event)
        if target is None:
            await event.reply(
                f"{prem('warning', '⚠️')} <b>لطفاً روی پیام کاربر مورد نظر ریپلای بزنید و سپس دستور را ارسال کنید.</b>",
                parse_mode="html",
            )
            return

        if await is_admin_or_owner(chat.id, target.id):
            await event.reply(
                f"{prem('cross', '🚫')} <b>نمی‌توانید روی مالک یا ادمین‌ها عملیات مدیریتی انجام دهید.</b>",
                parse_mode="html",
            )
            return

        admin_user = sender if sender is not None else await event.get_sender()
        target_msg = await get_replied_message_text(event)

        if cmd == "ban":
            if not rest:
                await event.reply(
                    f"{prem('warning', '⚠️')} <b>برای بن کردن کاربر، حتماً باید دلیل بنویسید.</b>\n\n"
                    f"📌 <b>مثال:</b>\n"
                    f"┃ <code>بن تبلیغات</code>\n"
                    f"┃ <code>سیک اسپم</code>\n"
                    f"┃ <code>صیک ارسال لینک</code>",
                    parse_mode="html",
                )
                return
            await do_ban(event, chat, target, rest, admin_user, target_msg=target_msg)

        elif cmd == "unban":
            await do_unban(event, chat, target, rest, admin_user)

        elif cmd == "mute":
            seconds, reason = parse_duration(rest)
            if not reason:
                await event.reply(
                    f"{prem('warning', '⚠️')} <b>برای سکوت کردن کاربر، حتماً باید دلیل بنویسید.</b>\n\n"
                    f"📌 <b>مثال‌ها:</b>\n"
                    f"┃ <code>سکوت 30m اسپم</code>\n"
                    f"┃ <code>سکوت 2h تبلیغات</code>\n"
                    f"┃ <code>سکوت 20 ارسال لینک</code> <i>(۲۰ دقیقه)</i>\n"
                    f"┃ <code>سکوت 555 تبلیغ</code> <i>(۵۵۵ دقیقه)</i>\n"
                    f"┃ <code>سکوت 1d بی‌احترامی</code>",
                    parse_mode="html",
                )
                return
            await do_mute(event, chat, target, reason, admin_user, seconds, target_msg=target_msg)

        elif cmd == "unmute":
            await do_unmute(event, chat, target, rest, admin_user)

        elif cmd == "warn":
            if not rest:
                await event.reply(
                    f"{prem('warning', '⚠️')} <b>برای اخطار دادن، حتماً باید دلیل بنویسید.</b>\n\n"
                    f"📌 <b>مثال:</b>\n"
                    f"┃ <code>اخطار تبلیغات</code>\n"
                    f"┃ <code>اخطار بی‌احترامی</code>\n"
                    f"┃ <code>اخطار اسپم</code>",
                    parse_mode="html",
                )
                return
            await do_warn(event, chat, target, rest, admin_user, target_msg=target_msg)

        elif cmd == "unwarn":
            await do_unwarn(event, chat, target, rest, admin_user)

    except Exception as ex:
        logger.exception(f"خطا در group_handler: {ex}")


client.add_event_handler(group_handler, events.NewMessage())
client.add_event_handler(group_handler, events.MessageEdited())


# ═════════════════════════════════════════════
# ۱۳) هندلر /start در پیوی
# ═════════════════════════════════════════════
MAIN_MENU_TEXT = (
    "╔══════════════════════════════════╗\n"
    f"   {prem('crown', '👑')} <b>پنل مدیریت ربات</b> {prem('crown', '👑')}\n"
    "╚══════════════════════════════════╝\n\n"
    "سلام مالک عزیز 👋\n"
    "به پنل مدیریت <b>گروه‌بان</b> خوش آمدید.\n\n"
    f"{prem('star', '⭐')} <b>قابلیت‌ها:</b>\n"
    f"┃ {prem('ban', '🚫')} بن کردن کاربر (بن / سیک / صیک)\n"
    f"┃ {prem('unban', '✅')} آنبن کردن کاربر\n"
    f"┃ {prem('mute', '🔇')} میوت کردن کاربر\n"
    f"┃ {prem('unmute', '🔊')} آن‌میوت کردن کاربر\n"
    f"┃ {prem('warning', '⚠️')} اخطار و بن خودکار\n"
    f"┃ {prem('building', '🏢')} مدیریت گروه‌ها\n"
    f"┃ {prem('handshake', '🤝')} درخواست واسطه\n"
    f"┃ {prem('alert', '🚨')} ضد اسپم و ضد لینک\n\n"
    "از دکمه‌های زیر استفاده کنید:"
)

MAIN_MENU_BUTTONS = [
    [Button.inline("🏢 مدیریت گروه‌ها", data=b"grp_list")],
    [Button.inline("📊 آمار کلی ربات", data=b"stats")],
    [
        Button.inline("📋 لیست بن‌شده‌ها", data=b"bans"),
        Button.inline("🔇 لیست میوت‌شده‌ها", data=b"mutes"),
    ],
    [Button.inline("⚠️ لیست اخطارها", data=b"warns")],
    [Button.inline("🛡 مدیریت ادمین‌ها", data=b"admins")],
    [Button.inline("📖 راهنمای دستورات", data=b"help")],
    [Button.inline("⚙️ تنظیمات", data=b"settings")],
]


@client.on(events.NewMessage(pattern=r"^/start(?:@\w+)?$", func=lambda ev: ev.is_private))
async def cmd_start(event):
    try:
        if event.sender_id in SUPER_ADMINS:
            await event.respond(MAIN_MENU_TEXT, buttons=MAIN_MENU_BUTTONS, parse_mode="html")
        else:
            await event.respond(
                "👋 <b>سلام!</b>\n\n"
                "من ربات مدیریت حرفه‌ای گروه‌های تلگرام هستم. 🛡\n\n"
                "برای استفاده از من، مرا به گروه خود اضافه کنید و "
                "به عنوان ادمین تنظیم کنید.\n\n"
                "🔹 سپس با ریپلای روی پیام کاربران، از دستورات "
                "<code>بن</code>، <code>سکوت</code>، <code>اخطار</code>، "
                "<code>آنبن</code> و <code>آن‌میوت</code> استفاده کنید.",
                parse_mode="html",
            )
    except Exception as ex:
        logger.exception(f"خطا در /start: {ex}")


# ═════════════════════════════════════════════
# ۱۴) هندلر دکمه‌های شیشه‌ای
# ═════════════════════════════════════════════
@client.on(events.CallbackQuery)
async def on_callback(event):
    try:
        if event.sender_id not in SUPER_ADMINS:
            await event.answer("⛔ شما دسترسی به این بخش را ندارید.", alert=True)
            return

        data = event.data.decode("utf-8", "ignore")

        # ─── دکمه‌های واسطه ───
        if data.startswith("med_a:") or data.startswith("med_r:"):
            await handle_mediator_callback(event, data)
            return

        # ─── دکمه‌های مدیریت گروه‌ها ───
        if data == "grp_list":
            await show_groups_list(event)
            await event.answer()
            return
        if data.startswith("grp:"):
            gid = int(data.split(":", 1)[1])
            await event.answer("⏳ در حال بارگذاری...", alert=False)
            await show_group_detail(event, gid)
            return
        if data.startswith("flw_t:"):
            parts = data.split(":")
            uid = int(parts[1])
            gid = int(parts[2])
            new_state = db.toggle_follow(uid, gid)
            if new_state is None:
                await event.answer("⚠️ کاربر یافت نشد!", alert=True)
                return
            status = "✅ پیروی می‌کنم" if new_state == 1 else "❌ پیروی نمی‌کنم"
            await event.answer(f"{status}", alert=False)
            await show_group_detail(event, gid)
            return

        # ─── منوی اصلی ───
        if data == "back":
            await event.edit(MAIN_MENU_TEXT, buttons=MAIN_MENU_BUTTONS, parse_mode="html")
            await event.answer()
        elif data == "stats":
            await cb_stats(event)
            await event.answer()
        elif data == "bans":
            await cb_bans(event)
            await event.answer()
        elif data == "mutes":
            await cb_mutes(event)
            await event.answer()
        elif data == "warns":
            await cb_warns(event)
            await event.answer()
        elif data == "admins":
            await cb_admins(event)
            await event.answer()
        elif data == "help":
            await cb_help(event)
            await event.answer()
        elif data == "settings":
            await cb_settings(event)
            await event.answer()
        elif data == "toggle_antispam":
            cur = db.get_bool_setting("anti_spam", False)
            db.set_setting("anti_spam", "0" if cur else "1")
            await cb_settings(event)
            await event.answer(f"ضد اسپم {'خاموش' if cur else 'روشن'} شد ✅")
        elif data == "toggle_antilink":
            cur = db.get_bool_setting("anti_link", False)
            db.set_setting("anti_link", "0" if cur else "1")
            await cb_settings(event)
            await event.answer(f"ضد لینک {'خاموش' if cur else 'روشن'} شد ✅")
        else:
            await event.answer("دستور ناشناخته", alert=False)

    except MessageNotModifiedError:
        await event.answer()
    except Exception as ex:
        logger.exception(f"خطا در callback: {ex}")
        try:
            await event.answer("خطایی رخ داد!", alert=True)
        except Exception:
            pass


# ═════════════════════════════════════════════
# نمایش لیست گروه‌ها
# ═════════════════════════════════════════════
async def show_groups_list(event):
    groups = db.get_bot_groups()
    if not groups:
        text = (
            "╔══════════════════════════════════╗\n"
            f"   {prem('building', '🏢')} <b>مدیریت گروه‌ها</b> {prem('building', '🏢')}\n"
            "╚══════════════════════════════════╝\n\n"
            f"{prem('info', 'ℹ️')} <i>ربات هنوز در هیچ گروهی نیست.</i>\n\n"
            f"{prem('wave', '👋')} ربات را به گروه اضافه کنید تا اینجا نمایش داده شود."
        )
        buttons = [[Button.inline("🔙 بازگشت", data=b"back")]]
        try:
            await event.edit(text, buttons=buttons, parse_mode="html")
        except MessageNotModifiedError:
            pass
        return

    lines = [
        "╔══════════════════════════════════╗",
        f"   {prem('building', '🏢')} <b>مدیریت گروه‌ها</b> {prem('building', '🏢')}",
        "╚══════════════════════════════════╝\n",
        f"{prem('list', '📋')} <b>گروه‌هایی که ربات در آن‌ها حضور دارد:</b> ({len(groups)} گروه)\n",
    ]
    for i, g in enumerate(groups[:20], 1):
        title = g.get("title") or "—"
        lines.append(
            f"{prem('fire', '🔥')} <b>#{i}</b> {h(title)}\n"
            f"┃ {prem('id', '🆔')} <code>{g['group_id']}</code>\n"
            f"┃ {prem('time', '⏱')} آخرین فعالیت: {h((g.get('last_seen') or '')[:19])}"
        )

    if len(groups) > 20:
        lines.append(f"\n{prem('info', 'ℹ️')} <i>و {len(groups) - 20} گروه دیگر...</i>")

    text = "\n".join(lines)

    buttons = []
    row = []
    for g in groups[:20]:
        title = (g.get("title") or "—")[:25]
        row.append(Button.inline(f"🏢 {title}", data=f"grp:{g['group_id']}".encode()))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    buttons.append([Button.inline("🔙 بازگشت", data=b"back")])

    try:
        await event.edit(text, buttons=buttons, parse_mode="html")
    except MessageNotModifiedError:
        pass


# ═════════════════════════════════════════════
# نمایش جزئیات گروه + سینک همه ادمین‌ها
# ═════════════════════════════════════════════
async def show_group_detail(event, group_id):
    group = db.get_bot_group(group_id)
    group_title = group.get("title") if group else "—"
    group_username = group.get("username") if group else None

    # ═══ سینک همه ادمین‌های گروه از تلگرام ═══
    try:
        await sync_group_admins(group_id)
    except Exception as ex:
        logger.debug(f"sync admins error: {ex}")

    users = db.get_followed_users(group_id)

    followed_count = sum(1 for u in users if u.get("is_followed") == 1)
    not_followed_count = len(users) - followed_count

    title_txt = (
        f'<a href="https://t.me/{group_username}">{h(group_title)}</a>'
        if group_username else h(group_title)
    )

    lines = [
        "╔══════════════════════════════════╗",
        f"   {prem('building', '🏢')} <b>جزئیات گروه</b>",
        "╚══════════════════════════════════╝\n",
        "┏━━━ " + prem('group', '📌') + " <b>اطلاعات گروه</b> ━━━┓\n"
        f"┃ {prem('tag', '🏷️')} <b>نام:</b> {title_txt}\n"
        f"┃ {prem('id', '🆔')} <b>آیدی:</b> <code>{group_id}</code>\n"
        "┗━━━━━━━━━━━━━━━━━━━━┛\n",
        f"\n{prem('eye', '👁')} <b>افرادی که ربات ازشون پیروی می‌کنه:</b>",
        f"┃ {prem('check', '✅')} فعال: <code>{followed_count}</code>\n"
        f"┃ {prem('cross', '❌')} غیرفعال: <code>{not_followed_count}</code>\n",
    ]

    if not users:
        lines.append(
            f"\n{prem('info', 'ℹ️')} <i>هیچ کسی هنوز ثبت نشده.</i>\n"
            f"اگه ربات دسترسی ادمین داره، ادمین‌های گروه خودکار اضافه می‌شن."
        )
    else:
        for u in users[:15]:
            uid = u["user_id"]
            name = u.get("user_name") or "—"
            uname = u.get("username")
            is_followed = u.get("is_followed") == 1

            uname_line = ""
            if uname:
                uname_line = f"\n┃ {prem('link', '🔗')} <a href=\"https://t.me/{uname}\">@{uname}</a>"

            if uid in SUPER_ADMINS:
                lines.append(
                    f"\n{prem('crown', '👑')} <b>[ادمین ارشد]</b> {h(name)}\n"
                    f"┃ {prem('id', '🆔')} <a href=\"tg://user?id={uid}\">{uid}</a>"
                    f"{uname_line}"
                )
            else:
                status_icon = prem('green', '🟢') if is_followed else prem('red', '🔴')
                status_text = "پیروی می‌کنم" if is_followed else "پیروی نمی‌کنم"
                lines.append(
                    f"\n{status_icon} <b>{h(name)}</b>\n"
                    f"┃ {prem('id', '🆔')} <a href=\"tg://user?id={uid}\">{uid}</a>"
                    f"{uname_line}\n"
                    f"┃ 📍 وضعیت: {status_text}"
                )

        if len(users) > 15:
            lines.append(f"\n{prem('info', 'ℹ️')} <i>و {len(users) - 15} نفر دیگر...</i>")

    text = "\n".join(lines)

    # دکمه‌های toggle برای هر کاربر
    buttons = []
    for u in users[:15]:
        uid = u["user_id"]
        name = (u.get("user_name") or "—")[:20]
        is_followed = u.get("is_followed") == 1

        if uid in SUPER_ADMINS:
            continue

        if is_followed:
            label = f"✅ {name}"
        else:
            label = f"❌ {name}"

        buttons.append([
            Button.inline(label, data=f"flw_t:{uid}:{group_id}".encode())
        ])

    buttons.append([Button.inline("🔙 بازگشت به گروه‌ها", data=b"grp_list")])
    buttons.append([Button.inline("🏠 منوی اصلی", data=b"back")])

    try:
        await event.edit(text, buttons=buttons, parse_mode="html")
    except MessageNotModifiedError:
        pass


# ═════════════════════════════════════════════
# پردازش دکمه‌های واسطه
# ═════════════════════════════════════════════
async def handle_mediator_callback(event, data):
    try:
        action, request_id = data.split(":", 1)
        req = _MEDIATOR_REQUESTS.get(request_id)
        if not req:
            await event.answer("⚠️ این درخواست منقضی شده است!", alert=True)
            return

        if req["status"] != "pending":
            await event.answer(
                f"⚠️ این درخواست قبلاً توسط {req.get('admin_name', 'ادمین')} پردازش شده!",
                alert=True,
            )
            return

        admin_user = await event.get_sender()
        admin_name = user_display(admin_user)

        is_approve = action == "med_a"
        req["status"] = "approved" if is_approve else "rejected"
        req["admin_name"] = admin_name
        req["admin_id"] = admin_user.id

        requester_id = req["requester"]
        target_id = req["target"]
        requester_link = f'<a href="tg://user?id={requester_id}">{requester_id}</a>'
        admin_id_link = f'<a href="tg://user?id={admin_user.id}">{admin_user.id}</a>'

        target_line = ""
        if target_id:
            target_link = f'<a href="tg://user?id={target_id}">{target_id}</a>'
            target_line = (
                "\n┏━━━ " + prem('target', '🎯') + " <b>طرف مقابل</b> ━━━┓\n"
                f"┃ {prem('id', '🆔')} <b>آیدی:</b> {target_link}\n"
                "┗━━━━━━━━━━━━━━━━━━━━┛\n"
            )

        if is_approve:
            status_line = f"{prem('green', '🟢')} <b>وضعیت:</b> تایید شد"
            header_emoji = prem('check', '✅')
            header_text = "واسطه تایید شد"
        else:
            status_line = f"{prem('red', '🔴')} <b>وضعیت:</b> رد شد"
            header_emoji = prem('cross', '❌')
            header_text = "واسطه رد شد"

        result_msg = (
            "╔══════════════════════════════════╗\n"
            f"   {header_emoji} <b>{header_text}</b> {header_emoji}\n"
            "╚══════════════════════════════════╝\n\n"
            f"◆ {status_line}\n"
            f"◆ {prem('time', '⏱')} <b>زمان:</b> <code>{now_iran_str()}</code>\n\n"
            "┏━━━ " + prem('admin', '🛡') + " <b>ادمین پردازش‌کننده</b> ━━━┓\n"
            f"┃ {prem('tag', '🏷️')} <b>نام:</b> {h(admin_name)}\n"
            f"┃ {prem('id', '🆔')} <b>آیدی:</b> {admin_id_link}\n"
            "┗━━━━━━━━━━━━━━━━━━━━┛\n\n"
            "┏━━━ " + prem('user', '👤') + " <b>درخواست‌دهنده</b> ━━━┓\n"
            f"┃ {prem('id', '🆔')} <b>آیدی:</b> {requester_link}\n"
            "┗━━━━━━━━━━━━━━━━━━━━┛"
            f"{target_line}"
            "\n"
            f"{prem('sparkles', '✨')} <i>این درخواست توسط ادمین {h(admin_name)} "
            f"{'پذیرفته' if is_approve else 'رد'} شد.</i>\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🤖 <i>Group Manager Bot</i>"
        )

        for admin_id, msg_id in req["messages"]:
            try:
                await client.edit_message(
                    admin_id, msg_id, result_msg,
                    parse_mode="html", buttons=None,
                )
            except MessageNotModifiedError:
                pass
            except Exception as ex:
                logger.debug(f"ویرایش پیام واسطه برای {admin_id} ناموفق: {ex}")

        try:
            if is_approve:
                req_msg = (
                    f"{prem('check', '✅')} <b>درخواست واسطه شما تایید شد!</b>\n\n"
                    f"┏━━━ " + prem('admin', '🛡') + " <b>ادمین</b> ━━━┓\n"
                    f"┃ {prem('tag', '🏷️')} <b>نام:</b> {h(admin_name)}\n"
                    f"┃ {prem('id', '🆔')} <b>آیدی:</b> <code>{admin_user.id}</code>\n"
                    "┗━━━━━━━━━━━━━━━━━━━━┛\n\n"
                    f"{prem('clock', '⏱')} <b>زمان:</b> <code>{now_iran_str()}</code>\n\n"
                    f"{prem('handshake', '🤝')} <b>به‌زودی واسطه با شما تماس می‌گیرد.</b>"
                )
            else:
                req_msg = (
                    f"{prem('cross', '❌')} <b>درخواست واسطه شما رد شد.</b>\n\n"
                    f"┏━━━ " + prem('admin', '🛡') + " <b>ادمین</b> ━━━┓\n"
                    f"┃ {prem('tag', '🏷️')} <b>نام:</b> {h(admin_name)}\n"
                    f"┃ {prem('id', '🆔')} <b>آیدی:</b> <code>{admin_user.id}</code>\n"
                    "┗━━━━━━━━━━━━━━━━━━━━┛\n\n"
                    f"{prem('clock', '⏱')} <b>زمان:</b> <code>{now_iran_str()}</code>"
                )
            await client.send_message(requester_id, req_msg, parse_mode="html")
        except Exception as ex:
            logger.debug(f"ارسال به درخواست‌دهنده ناموفق: {ex}")

        if target_id:
            try:
                if is_approve:
                    tgt_msg = (
                        f"{prem('check', '✅')} <b>درخواست واسطه تایید شد!</b>\n\n"
                        f"┏━━━ " + prem('admin', '🛡') + " <b>ادمین</b> ━━━┓\n"
                        f"┃ {prem('tag', '🏷️')} <b>نام:</b> {h(admin_name)}\n"
                        "┗━━━━━━━━━━━━━━━━━━━━┛\n\n"
                        f"{prem('handshake', '🤝')} <b>به‌زودی واسطه با شما تماس می‌گیرد.</b>"
                    )
                else:
                    tgt_msg = (
                        f"{prem('cross', '❌')} <b>درخواست واسطه رد شد.</b>\n\n"
                        f"┏━━━ " + prem('admin', '🛡') + " <b>ادمین</b> ━━━┓\n"
                        f"┃ {prem('tag', '🏷️')} <b>نام:</b> {h(admin_name)}\n"
                        "┗━━━━━━━━━━━━━━━━━━━━┛"
                    )
                await client.send_message(target_id, tgt_msg, parse_mode="html")
            except Exception as ex:
                logger.debug(f"ارسال به طرف مقابل ناموفق: {ex}")

        db.add_log(
            "mediator-approve" if is_approve else "mediator-reject",
            requester_id, admin_user.id, 0,
            f"target={target_id}",
        )

        await event.answer(
            f"{'✅ تایید شد' if is_approve else '❌ رد شد'} - همه ادمین‌ها مطلع شدند",
            alert=False,
        )

    except Exception as ex:
        logger.exception(f"خطا در handle_mediator_callback: {ex}")
        try:
            await event.answer("خطا در پردازش!", alert=True)
        except Exception:
            pass


# ═════════════════════════════════════════════
# بقیه callback ها
# ═════════════════════════════════════════════
async def cb_stats(event):
    db.cleanup_expired_mutes()
    max_warns = db.get_setting("max_warnings", str(DEFAULT_MAX_WARNINGS)) or str(DEFAULT_MAX_WARNINGS)
    groups_count = len(db.get_bot_groups())
    text = (
        "╔══════════════════════════════════╗\n"
        f"   {prem('stats', '📊')} <b>آمار کلی ربات</b> {prem('stats', '📊')}\n"
        "╚══════════════════════════════════╝\n\n"
        "┏━━━ " + prem('fire', '🔥') + " <b>وضعیت ربات</b> ━━━┓\n"
        f"┃ {prem('ban', '🚫')} <b>بن‌های فعال:</b> <code>{db.count_bans()}</code>\n"
        f"┃ {prem('mute', '🔇')} <b>میوت‌های فعال:</b> <code>{db.count_mutes()}</code>\n"
        f"┃ {prem('warning', '⚠️')} <b>کل اخطارها:</b> <code>{db.count_warnings()}</code>\n"
        f"┃ {prem('admin', '🛡')} <b>ادمین‌های ثبت‌شده:</b> <code>{db.count_admins()}</code>\n"
        f"┃ {prem('building', '🏢')} <b>گروه‌های ربات:</b> <code>{groups_count}</code>\n"
        f"┃ 📝 <b>تعداد کل لاگ‌ها:</b> <code>{db.count_logs()}</code>\n"
        f"┃ {prem('target', '🎯')} <b>سقف اخطار:</b> <code>{max_warns}</code>\n"
        "┗━━━━━━━━━━━━━━━━━━━━┛\n\n"
        f"{prem('time', '⏱')} <b>آخرین بروزرسانی:</b>\n<code>{now_iran_str()}</code>"
    )
    await event.edit(text, buttons=[[Button.inline("🔙 بازگشت", data=b"back")]], parse_mode="html")


async def cb_bans(event):
    rows = db.get_bans()[:25]
    if not rows:
        text = (
            "╔══════════════════════════════════╗\n"
            f"   {prem('list', '📋')} <b>لیست بن‌شده‌ها</b>\n"
            "╚══════════════════════════════════╝\n\n"
            f"{prem('check', '✅')} <i>هیچ کاربر بن‌شده‌ای وجود ندارد.</i>"
        )
    else:
        lines = [
            "╔══════════════════════════════════╗",
            f"   {prem('list', '📋')} <b>لیست بن‌شده‌ها</b> ({len(rows)})",
            "╚══════════════════════════════════╝\n",
        ]
        for i, r in enumerate(rows, 1):
            user_link = f'<a href="tg://user?id={r["user_id"]}">{r["user_id"]}</a>'
            lines.append(
                f"{prem('fire', '🔥')} <b>#{i}</b>\n"
                f"┃ {prem('user', '👤')} <b>آیدی:</b> {user_link}\n"
                f"┃ {prem('group', '📌')} <b>گروه:</b> <code>{r['group_id']}</code>\n"
                f"┃ {prem('reason', '💬')} <b>دلیل:</b> {h(r['reason'] or '—')}\n"
                f"┃ {prem('time', '⏱')} <b>زمان:</b> {h((r['banned_at'] or '')[:19])}\n"
                "━━━━━━━━━━━━━"
            )
        text = "\n".join(lines)
    await event.edit(text, buttons=[[Button.inline("🔙 بازگشت", data=b"back")]], parse_mode="html")


async def cb_mutes(event):
    db.cleanup_expired_mutes()
    rows = db.get_mutes()[:25]
    if not rows:
        text = (
            "╔══════════════════════════════════╗\n"
            f"   {prem('mute', '🔇')} <b>لیست میوت‌شده‌ها</b>\n"
            "╚══════════════════════════════════╝\n\n"
            f"{prem('check', '✅')} <i>هیچ کاربر میوت‌شده‌ای وجود ندارد.</i>"
        )
    else:
        lines = [
            "╔══════════════════════════════════╗",
            f"   {prem('mute', '🔇')} <b>لیست میوت‌شده‌ها</b> ({len(rows)})",
            "╚══════════════════════════════════╝\n",
        ]
        for i, r in enumerate(rows, 1):
            user_link = f'<a href="tg://user?id={r["user_id"]}">{r["user_id"]}</a>'
            lines.append(
                f"{prem('fire', '🔥')} <b>#{i}</b>\n"
                f"┃ {prem('user', '👤')} <b>آیدی:</b> {user_link}\n"
                f"┃ {prem('group', '📌')} <b>گروه:</b> <code>{r['group_id']}</code>\n"
                f"┃ {prem('reason', '💬')} <b>دلیل:</b> {h(r['reason'] or '—')}\n"
                f"┃ {prem('time', '⏱')} <b>پایان:</b> {h((r['until'] or '')[:19])}\n"
                "━━━━━━━━━━━━━"
            )
        text = "\n".join(lines)
    await event.edit(text, buttons=[[Button.inline("🔙 بازگشت", data=b"back")]], parse_mode="html")


async def cb_warns(event):
    rows = db.get_warnings()[:25]
    max_warns = db.get_setting("max_warnings", str(DEFAULT_MAX_WARNINGS)) or str(DEFAULT_MAX_WARNINGS)
    if not rows:
        text = (
            "╔══════════════════════════════════╗\n"
            f"   {prem('warning', '⚠️')} <b>لیست اخطارها</b>\n"
            "╚══════════════════════════════════╝\n\n"
            f"{prem('check', '✅')} <i>هیچ کاربری اخطار فعال ندارد.</i>"
        )
    else:
        lines = [
            "╔══════════════════════════════════╗",
            f"   {prem('warning', '⚠️')} <b>لیست اخطارها</b> ({len(rows)})",
            "╚══════════════════════════════════╝\n",
            f"{prem('target', '🎯')} <b>سقف اخطار:</b> <code>{max_warns}</code>\n",
        ]
        for i, r in enumerate(rows, 1):
            user_link = f'<a href="tg://user?id={r["user_id"]}">{r["user_id"]}</a>'
            count = r["count"] or 0
            bar_total = 10
            filled = min(bar_total, int((count / int(max_warns)) * bar_total)) if max_warns else 0
            bar = "█" * filled + "░" * (bar_total - filled)
            lines.append(
                f"{prem('fire', '🔥')} <b>#{i}</b>\n"
                f"┃ {prem('user', '👤')} <b>آیدی:</b> {user_link}\n"
                f"┃ {prem('group', '📌')} <b>گروه:</b> <code>{r['group_id']}</code>\n"
                f"┃ 📊 <b>اخطار:</b> <code>{count}/{max_warns}</code>\n"
                f"┃ <code>{bar}</code>\n"
                f"┃ {prem('time', '⏱')} <b>آخرین:</b> {h((r['last_warned_at'] or '')[:19])}\n"
                "━━━━━━━━━━━━━"
            )
        text = "\n".join(lines)
    await event.edit(text, buttons=[[Button.inline("🔙 بازگشت", data=b"back")]], parse_mode="html")


async def cb_admins(event):
    rows = db.get_admins()
    lines = [
        "╔══════════════════════════════════╗",
        f"   {prem('admin', '🛡')} <b>مدیریت ادمین‌ها</b>",
        "╚══════════════════════════════════╝\n",
        f"{prem('info', 'ℹ️')} <b>راهنما:</b>\n",
        "┃ ➕ <code>/addadmin &lt;user_id&gt; [group_id]</code>\n",
        "┃ ➖ <code>/removeadmin &lt;user_id&gt; [group_id]</code>\n",
        f"┃ {prem('warning', '⚠️')} اگر <code>group_id</code> ندهید، ادمین <b>سراسری</b> می‌شود.\n",
    ]
    if not rows:
        lines.append(f"\n{prem('cross', '❌')} <i>هنوز ادمینی ثبت نشده است.</i>")
    else:
        lines.append(f"\n{prem('list', '📋')} <b>ادمین‌های ثبت‌شده ({len(rows)} نفر):</b>\n")
        for r in rows[:20]:
            gid = r["group_id"]
            gtxt = "🌐 سراسری" if gid == 0 else f"📌 <code>{gid}</code>"
            user_link = f'<a href="tg://user?id={r["user_id"]}">{r["user_id"]}</a>'
            lines.append(f"{prem('user', '👤')} {user_link} | {gtxt}")
    text = "\n".join(lines)
    await event.edit(text, buttons=[[Button.inline("🔙 بازگشت", data=b"back")]], parse_mode="html")


async def cb_help(event):
    text = (
        "╔══════════════════════════════════╗\n"
        f"   {prem('info', '📖')} <b>راهنمای دستورات</b> {prem('info', '📖')}\n"
        "╚══════════════════════════════════╝\n\n"
        "برای اجرای دستور، روی پیام کاربر هدف <b>ریپلای</b> بزنید:\n\n"
        "┏━━━ " + prem('ban', '🚫') + " <b>بن کردن</b> ━━━┓\n"
        "┃ <code>بن [دلیل]</code>\n"
        "┃ <code>سیک [دلیل]</code>\n"
        "┃ <code>صیک [دلیل]</code>\n"
        "┃ ⚠️ بدون دلیل اجرا نمی‌شود\n"
        "┗━━━━━━━━━━━━━━━┛\n\n"
        "┏━━━ " + prem('unban', '✅') + " <b>آنبن کردن</b> ━━━┓\n"
        "┃ <code>آنبن</code> | <code>unban</code>\n"
        "┗━━━━━━━━━━━━━━━┛\n\n"
        "┏━━━ " + prem('mute', '🔇') + " <b>سکوت (میوت)</b> ━━━┓\n"
        "┃ <code>سکوت [زمان] [دلیل]</code>\n"
        "┃ 🔹 مثال‌ها:\n"
        "┃   <code>سکوت 20 اسپم</code> → ۲۰ دقیقه\n"
        "┃   <code>سکوت 555 تبلیغ</code> → ۵۵۵ دقیقه\n"
        "┃   <code>سکوت 2h تبلیغ</code> → ۲ ساعت\n"
        "┃   <code>سکوت 1d بی‌احترامی</code> → ۱ روز\n"
        "┃ ⚠️ بدون دلیل اجرا نمی‌شود\n"
        "┗━━━━━━━━━━━━━━━┛\n\n"
        "┏━━━ " + prem('unmute', '🔊') + " <b>آن‌میوت</b> ━━━┓\n"
        "┃ <code>آن‌میوت</code> | <code>unmute</code>\n"
        "┗━━━━━━━━━━━━━━━┛\n\n"
        "┏━━━ " + prem('warning', '⚠️') + " <b>اخطار</b> ━━━┓\n"
        "┃ <code>اخطار [دلیل]</code>\n"
        "┃ 🔹 پس از <b>۳ اخطار</b> → بن خودکار\n"
        "┃ ⚠️ بدون دلیل اجرا نمی‌شود\n"
        "┗━━━━━━━━━━━━━━━┛\n\n"
        "┏━━━ " + prem('check', '✔️') + " <b>حذف اخطار</b> ━━━┓\n"
        "┃ <code>حذف اخطار</code>\n"
        "┗━━━━━━━━━━━━━━━┛\n\n"
        "┏━━━ " + prem('handshake', '🤝') + " <b>واسطه</b> ━━━┓\n"
        f"┃ در گپ <code>{MEDIATOR_CHAT_ID}</code> بنویسید:\n"
        "┃ <code>واسطه</code> (دقیقاً فقط همین کلمه)\n"
        "┗━━━━━━━━━━━━━━━┛"
    )
    await event.edit(text, buttons=[[Button.inline("🔙 بازگشت", data=b"back")]], parse_mode="html")


async def cb_settings(event):
    anti_spam = db.get_bool_setting("anti_spam", False)
    anti_link = db.get_bool_setting("anti_link", False)
    max_warns = db.get_setting("max_warnings", str(DEFAULT_MAX_WARNINGS)) or str(DEFAULT_MAX_WARNINGS)
    text = (
        "╔══════════════════════════════════╗\n"
        f"   {prem('settings', '⚙️')} <b>تنظیمات ربات</b> {prem('settings', '⚙️')}\n"
        "╚══════════════════════════════════╝\n\n"
        f"{prem('fire', '🔥')} <b>قابلیت‌های حفاظتی:</b>\n\n"
        f"┃ {prem('alert', '🚨')} ضد اسپم: {'✅ روشن' if anti_spam else '❌ خاموش'}\n"
        f"┃ {prem('link', '🔗')} ضد لینک: {'✅ روشن' if anti_link else '❌ خاموش'}\n"
        f"┃ {prem('warning', '⚠️')} سقف اخطار: <code>{max_warns}</code>\n\n"
        "برای تغییر، روی دکمه‌ها کلیک کنید:"
    )
    buttons = [
        [Button.inline(f"🚨 ضد اسپم: {'✅ روشن' if anti_spam else '❌ خاموش'}", data=b"toggle_antispam")],
        [Button.inline(f"🔗 ضد لینک: {'✅ روشن' if anti_link else '❌ خاموش'}", data=b"toggle_antilink")],
        [Button.inline("🔙 بازگشت", data=b"back")],
    ]
    await event.edit(text, buttons=buttons, parse_mode="html")


# ═════════════════════════════════════════════
# ۱۵) دستورات مدیریتی در پیوی
# ═════════════════════════════════════════════
@client.on(events.NewMessage(
    pattern=r"^/addadmin\s+(-?\d+)(?:\s+(-?\d+))?$",
    func=lambda ev: ev.is_private and ev.sender_id in SUPER_ADMINS,
))
async def cmd_addadmin(event):
    m = re.match(r"^/addadmin\s+(-?\d+)(?:\s+(-?\d+))?$", event.raw_text.strip())
    user_id = int(m.group(1))
    group_id = int(m.group(2)) if m.group(2) else 0
    db.add_admin(user_id, group_id, event.sender_id)
    gtxt = "سراسری 🌐" if group_id == 0 else f"گروه <code>{group_id}</code>"
    user_link = f'<a href="tg://user?id={user_id}">{user_id}</a>'
    await event.reply(
        f"{prem('check', '✅')} کاربر {user_link} به عنوان ادمین {gtxt} اضافه شد.",
        parse_mode="html",
    )


@client.on(events.NewMessage(
    pattern=r"^/removeadmin\s+(-?\d+)(?:\s+(-?\d+))?$",
    func=lambda ev: ev.is_private and ev.sender_id in SUPER_ADMINS,
))
async def cmd_removeadmin(event):
    m = re.match(r"^/removeadmin\s+(-?\d+)(?:\s+(-?\d+))?$", event.raw_text.strip())
    user_id = int(m.group(1))
    group_id = int(m.group(2)) if m.group(2) else 0
    ok = db.remove_admin(user_id, group_id)
    user_link = f'<a href="tg://user?id={user_id}">{user_id}</a>'
    if ok:
        await event.reply(
            f"{prem('check', '✅')} کاربر {user_link} از ادمین‌ها حذف شد.",
            parse_mode="html",
        )
    else:
        await event.reply(
            f"{prem('warning', '⚠️')} <b>این کاربر در لیست ادمین‌ها یافت نشد.</b>",
            parse_mode="html",
        )


@client.on(events.NewMessage(pattern=r"^/help(?:@\w+)?$", func=lambda ev: ev.is_private))
async def cmd_help(event):
    text = (
        f"{prem('info', '📖')} <b>راهنمای ربات گروه‌بان</b>\n\n"
        "🔹 ربات را به گروه اضافه کنید و ادمین کنید.\n"
        "🔹 سپس روی پیام کاربر ریپلای بزنید و ارسال کنید:\n\n"
        f"{prem('ban', '🚫')} <code>بن/سیک/صیک [دلیل]</code> - بن (دلیل اجباری)\n"
        f"{prem('unban', '✅')} <code>آنبن</code> - آنبن\n"
        f"{prem('mute', '🔇')} <code>سکوت [زمان] [دلیل]</code> - میوت (دلیل اجباری)\n"
        f"{prem('unmute', '🔊')} <code>آن‌میوت</code> - رفع میوت\n"
        f"{prem('warning', '⚠️')} <code>اخطار [دلیل]</code> - اخطار (دلیل اجباری)\n"
        f"{prem('check', '✔️')} <code>حذف اخطار</code> - پاک کردن اخطارها\n\n"
        f"{prem('handshake', '🤝')} <b>واسطه خودکار:</b>\n"
        f"در گپ <code>{MEDIATOR_CHAT_ID}</code> کافیه بنویسید <code>واسطه</code>\n\n"
        f"{prem('time', '⏱')} {now_iran_str()}"
    )
    await event.respond(text, parse_mode="html")


@client.on(events.NewMessage(
    pattern=r"^/stats(?:@\w+)?$",
    func=lambda ev: ev.is_private and ev.sender_id in SUPER_ADMINS,
))
async def cmd_stats(event):
    db.cleanup_expired_mutes()
    text = (
        f"{prem('stats', '📊')} <b>آمار کلی ربات</b>\n\n"
        f"{prem('ban', '🚫')} بن‌های فعال: <code>{db.count_bans()}</code>\n"
        f"{prem('mute', '🔇')} میوت‌های فعال: <code>{db.count_mutes()}</code>\n"
        f"{prem('warning', '⚠️')} کل اخطارها: <code>{db.count_warnings()}</code>\n"
        f"{prem('admin', '🛡')} ادمین‌ها: <code>{db.count_admins()}</code>\n"
        f"{prem('building', '🏢')} گروه‌های ربات: <code>{len(db.get_bot_groups())}</code>\n"
        f"📝 لاگ‌ها: <code>{db.count_logs()}</code>\n\n"
        f"{prem('time', '⏱')} {now_iran_str()}"
    )
    await event.respond(text, parse_mode="html")


# ═════════════════════════════════════════════
# ۱۶) وب‌سرور Flask
# ═════════════════════════════════════════════
web_app = Flask(__name__)


@web_app.route("/")
def index():
    return "🤖 Group Manager Bot is running", 200


@web_app.route("/health")
def health():
    return "OK", 200


def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🌐 وب‌سرور Flask روی پورت {port} اجرا شد")
    web_app.run(host="0.0.0.0", port=port, use_reloader=False)


# ═════════════════════════════════════════════
# ۱۷) راه‌اندازی ربات
# ═════════════════════════════════════════════
async def _periodic_cleanup():
    while True:
        try:
            db.cleanup_expired_mutes()
            if len(_MEDIATOR_REQUESTS) > 100:
                sorted_keys = sorted(_MEDIATOR_REQUESTS.keys())
                for k in sorted_keys[:50]:
                    _MEDIATOR_REQUESTS.pop(k, None)
        except Exception as ex:
            logger.warning(f"cleanup error: {ex}")
        await asyncio.sleep(60)


async def main():
    logger.info("🚀 راه‌اندازی ربات گروه‌بان...")
    logger.info(f"👥 ادمین‌های ارشد: {SUPER_ADMINS}")

    if not BOT_TOKEN:
        raise ValueError("❌ BOT_TOKEN باید تنظیم شود.")
    if not OWNER_ID:
        raise ValueError("❌ OWNER_ID باید تنظیم شود.")
    if not DATABASE_URL:
        raise ValueError("❌ DATABASE_URL باید تنظیم شود.")

    await client.start(bot_token=BOT_TOKEN)
    me = await client.get_me()
    logger.info(f"✅ ربات متصل شد: @{me.username} (ID: {me.id})")

    for admin_id in SUPER_ADMINS:
        try:
            await client.send_message(
                admin_id,
                "╔══════════════════════════════════╗\n"
                f"   {prem('check', '✅')} <b>ربات با موفقیت روشن شد</b>\n"
                "╚══════════════════════════════════╝\n\n"
                "┏━━━ " + prem('fire', '🔥') + " <b>اطلاعات ربات</b> ━━━┓\n"
                f"┃ 🤖 <b>یوزرنیم:</b> @{me.username}\n"
                f"┃ {prem('id', '🆔')} <b>آیدی:</b> <code>{me.id}</code>\n"
                f"┃ {prem('time', '⏱')} <b>زمان:</b> {now_iran_str()}\n"
                "┗━━━━━━━━━━━━━━━━━━━━┛\n\n"
                f"{prem('star', '⭐')} برای دیدن پنل مدیریت، /start را ارسال کنید.",
                parse_mode="html",
            )
        except Exception as ex:
            logger.warning(f"ارسال پیام راه‌اندازی به {admin_id} ناموفق: {ex}")

    asyncio.create_task(_periodic_cleanup())

    logger.info("✅ ربات آماده به کار است. در حال گوش دادن به پیام‌ها...")
    await client.run_until_disconnected()


# ═════════════════════════════════════════════
# ۱۸) نقطه ورود
# ═════════════════════════════════════════════
if __name__ == "__main__":
    try:
        web_thread = threading.Thread(target=run_web_server, daemon=True)
        web_thread.start()
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⛔ ربات توسط کاربر متوقف شد.")
    except Exception as ex:
        logger.exception(f"❌ خطای غیرمنتظره: {ex}")