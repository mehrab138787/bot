# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════╗
║         🤖  ربات مدیریت حرفه‌ای گروه‌های تلگرام  🤖           ║
║                    Group Manager Bot                         ║
║              Telethon + PostgreSQL + asyncio                 ║
║      ✨ ایموجی پرمیوم + پروکسی + گزارش کلیک‌پذیر ✨         ║
║              🚀 آماده استقرار روی Render                     ║
╚══════════════════════════════════════════════════════════════╝
"""

import os
import re
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque

import socks
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from telethon import TelegramClient, events, Button
from telethon.tl.functions.channels import EditBannedRequest, GetParticipantRequest
from telethon.tl.types import (
    ChatBannedRights,
    ChannelParticipantCreator,
    ChannelParticipantAdmin,
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
SESSION_NAME = os.getenv("SESSION_NAME", "group_manager_bot")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

_log_channel = os.getenv("LOG_CHANNEL_ID", "").strip()
LOG_CHANNEL_ID = int(_log_channel) if _log_channel.lstrip("-").isdigit() else None

# تنظیمات پروکسی (اختیاری - برای Render نیازی نیست)
PROXY_HOST = os.getenv("PROXY_HOST", "").strip()
PROXY_PORT = int(os.getenv("PROXY_PORT", "0") or 0)

IRAN_TZ = timezone(timedelta(hours=3, minutes=30))
MAX_MUTE_SECONDS = 366 * 86400

# لیست همه ادمین‌های ارشد (که لاگ می‌گیرند و به پنل دسترسی دارند)
SUPER_ADMINS = [uid for uid in (OWNER_ID, SECOND_ADMIN_ID) if uid]


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


# ═════════════════════════════════════════════
# ۳) ساخت کلاینت تلگرام (با/بدون پروکسی)
# ═════════════════════════════════════════════
if PROXY_HOST and PROXY_PORT:
    logger.info(f"🌐 اتصال از طریق پروکسی SOCKS5: {PROXY_HOST}:{PROXY_PORT}")
    client = TelegramClient(
        SESSION_NAME,
        API_ID,
        API_HASH,
        proxy=(socks.SOCKS5, PROXY_HOST, PROXY_PORT, True, None, None),
    )
else:
    logger.info("🌐 اتصال مستقیم (بدون پروکسی) - مناسب برای Render")
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)


# ═════════════════════════════════════════════
# ۴) کلاس مدیریت دیتابیس PostgreSQL
# ═════════════════════════════════════════════
class Database:
    """مدیریت دیتابیس PostgreSQL"""

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
            cur.execute(
                "DELETE FROM admins WHERE user_id=%s AND group_id=%s",
                (user_id, group_id),
            )
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
                cur.execute(
                    "SELECT * FROM admins WHERE group_id=%s ORDER BY added_at DESC",
                    (group_id,),
                )
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
            cur.execute(
                "UPDATE bans SET is_active=0 WHERE user_id=%s AND group_id=%s",
                (user_id, group_id),
            )

    def get_bans(self, group_id=None):
        with self._cursor() as cur:
            if group_id is not None:
                cur.execute(
                    "SELECT * FROM bans WHERE group_id=%s AND is_active=1 ORDER BY banned_at DESC",
                    (group_id,),
                )
            else:
                cur.execute(
                    "SELECT * FROM bans WHERE is_active=1 ORDER BY banned_at DESC"
                )
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
            cur.execute(
                "UPDATE mutes SET is_active=0 WHERE user_id=%s AND group_id=%s",
                (user_id, group_id),
            )

    def get_mutes(self, group_id=None):
        with self._cursor() as cur:
            if group_id is not None:
                cur.execute(
                    "SELECT * FROM mutes WHERE group_id=%s AND is_active=1 ORDER BY muted_at DESC",
                    (group_id,),
                )
            else:
                cur.execute(
                    "SELECT * FROM mutes WHERE is_active=1 ORDER BY muted_at DESC"
                )
            return [dict(r) for r in cur.fetchall()]

    def count_mutes(self):
        with self._cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM mutes WHERE is_active=1")
            return cur.fetchone()["c"]

    def cleanup_expired_mutes(self):
        now = datetime.now(IRAN_TZ).isoformat()
        with self._cursor() as cur:
            cur.execute(
                "UPDATE mutes SET is_active=0 "
                "WHERE is_active=1 AND until IS NOT NULL AND until < %s",
                (now,),
            )

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
}


def prem(key: str, fallback: str) -> str:
    """تبدیل یک ایموجی معمولی به ایموجی پرمیوم"""
    emoji_id = PREMIUM_EMOJI.get(key)
    if emoji_id:
        return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'
    return fallback


# ═════════════════════════════════════════════
# ۶) توابع کمکی
# ═════════════════════════════════════════════
COMMAND_KEYWORDS = {
    "unmute": ["unmute", "آنمیوت", "آن میوت", "انمیوت", "ان میوت"],
    "unban": ["unban", "آنبن", "آن بن", "انبن", "ان بن"],
    "mute": ["mute", "silence", "سکوت", "میوت"],
    "ban": ["ban", "مسدود", "بن"],
}


def parse_command(text):
    if not text:
        return None
    raw = text.strip()
    lower = raw.lower()
    for cmd in ("unmute", "unban", "mute", "ban"):
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
    m = re.match(r"^(\d+)\s*([smhdSMHD])(?:\s+(.*))?$", text, re.DOTALL)
    if m:
        try:
            num = int(m.group(1))
        except ValueError:
            return 3600, text
        unit = m.group(2).lower()
        reason = (m.group(3) or "").strip()
        mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
        seconds = num * mult
        if seconds > MAX_MUTE_SECONDS:
            seconds = MAX_MUTE_SECONDS
        return seconds, reason
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


def format_report(action_title, action_emoji_key, target, admin_user, chat, reason, link):
    """ساخت متن زیبای گزارش عملیات با ایموجی پرمیوم و لینک‌های کلیک‌پذیر"""
    # اطلاعات کاربر هدف
    target_name = user_display(target)
    target_username = getattr(target, "username", None)
    target_id_link = f'<a href="tg://user?id={target.id}">{target.id}</a>'
    target_username_txt = (
        f'<a href="https://t.me/{target_username}">@{target_username}</a>'
        if target_username else "—"
    )

    # اطلاعات ادمین
    admin_name = user_display(admin_user)
    admin_username = getattr(admin_user, "username", None)
    admin_id_link = f'<a href="tg://user?id={admin_user.id}">{admin_user.id}</a>'
    admin_username_txt = (
        f'<a href="https://t.me/{admin_username}">@{admin_username}</a>'
        if admin_username else "—"
    )

    # اطلاعات گروه
    chat_title = getattr(chat, "title", "—") or "—"
    chat_username = getattr(chat, "username", None)
    chat_title_txt = (
        f'<a href="https://t.me/{chat_username}">{h(chat_title)}</a>'
        if chat_username else h(chat_title)
    )

    reason_txt = h(reason) if reason else "—"
    link_txt = f'<a href="{link}">اینجا کلیک کنید</a>' if link else "—"

    return (
        "╔══════════════════════════════════╗\n"
        f"   {prem(action_emoji_key, '📢')} <b>گزارش عملیات جدید</b> {prem(action_emoji_key, '📢')}\n"
        "╚══════════════════════════════════╝\n\n"
        f"◆ {prem('fire', '🔥')} <b>نوع:</b> {action_title}\n"
        f"◆ {prem('time', '⏱')} <b>زمان:</b> <code>{now_iran_str()}</code>\n\n"
        "┏━━━ 👤 <b>کاربر هدف</b> ━━━┓\n"
        f"┃ 🏷️ <b>نام:</b> {h(target_name)}\n"
        f"┃ {prem('id', '🆔')} <b>آیدی:</b> {target_id_link}\n"
        f"┃ {prem('link', '🔗')} <b>یوزرنیم:</b> {target_username_txt}\n"
        "┗━━━━━━━━━━━━━━━━━━━━┛\n\n"
        "┏━━━ 🛡 <b>توسط ادمین</b> ━━━┓\n"
        f"┃ 🏷️ <b>نام:</b> {h(admin_name)}\n"
        f"┃ {prem('id', '🆔')} <b>آیدی:</b> {admin_id_link}\n"
        f"┃ {prem('link', '🔗')} <b>یوزرنیم:</b> {admin_username_txt}\n"
        "┗━━━━━━━━━━━━━━━━━━━━┛\n\n"
        "┏━━━ 📌 <b>گروه</b> ━━━┓\n"
        f"┃ 🏷️ <b>نام:</b> {chat_title_txt}\n"
        f"┃ {prem('id', '🆔')} <b>آیدی:</b> <code>{chat.id}</code>\n"
        f"┃ {prem('reason', '💬')} <b>دلیل:</b> {reason_txt}\n"
        f"┃ {prem('link', '🔗')} <b>پیام:</b> {link_txt}\n"
        "┗━━━━━━━━━━━━━━━━━━━━┛\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <i>Group Manager Bot</i>"
    )


async def is_telegram_admin(chat_id, user_id):
    try:
        p = await client(GetParticipantRequest(chat_id, user_id))
        return isinstance(
            p.participant, (ChannelParticipantCreator, ChannelParticipantAdmin)
        )
    except Exception:
        return False


async def is_admin_or_owner(chat_id, user_id):
    if user_id in SUPER_ADMINS:
        return True
    if db.is_admin(user_id, chat_id):
        return True
    return await is_telegram_admin(chat_id, user_id)


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


# ═════════════════════════════════════════════
# ۷) ارسال گزارش به همه ادمین‌های ارشد
# ═════════════════════════════════════════════
async def send_report(text):
    """ارسال گزارش به مالک، ادمین دوم و کانال لاگ"""
    for admin_id in SUPER_ADMINS:
        try:
            await client.send_message(admin_id, text, parse_mode="html", link_preview=False)
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
# ۸) ضد اسپم و ضد لینک
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
                f"{prem('warning', '🚨')} <b>کاربر {h(user_display(sender))} به دلیل ارسال پیام‌های پیاپی، "
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
# ۹) اجرای عملیات
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


async def do_ban(event, chat, target, reason, admin_user):
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
    report = format_report("بن کردن کاربر", "ban", target, admin_user, chat, reason, link)
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
    db.add_log("unban", target.id, admin_user.id, chat.id, reason or "")

    reason_txt = f"\n{prem('reason', '💬')} <b>دلیل:</b> {h(reason)}" if reason else ""
    await event.reply(
        f"{prem('unban', '✅')} <b>کاربر {h(user_display(target))} آنبن شد.</b>{reason_txt}",
        parse_mode="html",
    )

    link = build_message_link(chat.id, event.reply_to_msg_id or event.id)
    report = format_report("آنبن کردن کاربر", "unban", target, admin_user, chat, reason, link)
    await send_report(report)


async def do_mute(event, chat, target, reason, admin_user, seconds):
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
        chat, full_reason, link,
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


# ═════════════════════════════════════════════
# ۱۰) هندلر اصلی گروه
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

        try:
            sender = await event.get_sender()
        except Exception:
            sender = None

        if sender is not None and not getattr(sender, "bot", False):
            try:
                await check_antilink(event, chat, sender)
            except Exception as ex:
                logger.debug(f"antilink error: {ex}")
            try:
                await check_antispam(event, chat, sender)
            except Exception as ex:
                logger.debug(f"antispam error: {ex}")

        parsed = parse_command(event.raw_text or "")
        if not parsed:
            return
        cmd, rest = parsed

        if not await is_admin_or_owner(chat.id, event.sender_id):
            return

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

        if cmd == "ban":
            await do_ban(event, chat, target, rest, admin_user)
        elif cmd == "unban":
            await do_unban(event, chat, target, rest, admin_user)
        elif cmd == "mute":
            seconds, reason = parse_duration(rest)
            await do_mute(event, chat, target, reason, admin_user, seconds)
        elif cmd == "unmute":
            await do_unmute(event, chat, target, rest, admin_user)

    except Exception as ex:
        logger.exception(f"خطا در group_handler: {ex}")


client.add_event_handler(group_handler, events.NewMessage())
client.add_event_handler(group_handler, events.MessageEdited())


# ═════════════════════════════════════════════
# ۱۱) هندلر /start در پیوی
# ═════════════════════════════════════════════
MAIN_MENU_TEXT = (
    "╔══════════════════════════════════╗\n"
    f"   {prem('crown', '👑')} <b>پنل مدیریت ربات</b> {prem('crown', '👑')}\n"
    "╚══════════════════════════════════╝\n\n"
    "سلام مالک عزیز 👋\n"
    "به پنل مدیریت <b>گروه‌بان</b> خوش آمدید.\n\n"
    f"{prem('star', '⭐')} <b>قابلیت‌ها:</b>\n"
    f"┃ {prem('ban', '🚫')} بن کردن کاربر\n"
    f"┃ {prem('unban', '✅')} آنبن کردن کاربر\n"
    f"┃ {prem('mute', '🔇')} میوت کردن کاربر\n"
    f"┃ {prem('unmute', '🔊')} آن‌میوت کردن کاربر\n"
    f"┃ {prem('shield', '🛡')} مدیریت ادمین‌ها\n"
    f"┃ 🚨 ضد اسپم و ضد لینک\n\n"
    "از دکمه‌های زیر استفاده کنید:"
)

MAIN_MENU_BUTTONS = [
    [Button.inline("📊 آمار کلی ربات", data=b"stats")],
    [
        Button.inline("📋 لیست بن‌شده‌ها", data=b"bans"),
        Button.inline("🔇 لیست میوت‌شده‌ها", data=b"mutes"),
    ],
    [Button.inline("🛡 مدیریت ادمین‌ها", data=b"admins")],
    [Button.inline("📖 راهنمای دستورات", data=b"help")],
    [Button.inline("⚙️ تنظیمات", data=b"settings")],
]


@client.on(events.NewMessage(pattern=r"^/start(?:@\w+)?$", func=lambda ev: ev.is_private))
async def cmd_start(event):
    try:
        if event.sender_id in SUPER_ADMINS:
            await event.respond(
                MAIN_MENU_TEXT, buttons=MAIN_MENU_BUTTONS, parse_mode="html"
            )
        else:
            await event.respond(
                "👋 <b>سلام!</b>\n\n"
                "من ربات مدیریت حرفه‌ای گروه‌های تلگرام هستم. 🛡\n\n"
                "برای استفاده از من، مرا به گروه خود اضافه کنید و "
                "به عنوان ادمین تنظیم کنید.\n\n"
                "🔹 سپس با ریپلای روی پیام کاربران، از دستورات "
                "<code>بن</code>، <code>سکوت</code>، <code>آنبن</code> و "
                "<code>آن‌میوت</code> استفاده کنید.",
                parse_mode="html",
            )
    except Exception as ex:
        logger.exception(f"خطا در /start: {ex}")


# ═════════════════════════════════════════════
# ۱۲) هندلر دکمه‌های شیشه‌ای
# ═════════════════════════════════════════════
@client.on(events.CallbackQuery)
async def on_callback(event):
    try:
        if event.sender_id not in SUPER_ADMINS:
            await event.answer("⛔ شما دسترسی به این بخش را ندارید.", alert=True)
            return

        data = event.data.decode("utf-8", "ignore")

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


async def cb_stats(event):
    db.cleanup_expired_mutes()
    text = (
        "╔══════════════════════════════════╗\n"
        f"   {prem('stats', '📊')} <b>آمار کلی ربات</b> {prem('stats', '📊')}\n"
        "╚══════════════════════════════════╝\n\n"
        f"┏━━━ {prem('fire', '🔥')} <b>وضعیت ربات</b> ━━━┓\n"
        f"┃ {prem('ban', '🚫')} <b>بن‌های فعال:</b> <code>{db.count_bans()}</code>\n"
        f"┃ {prem('mute', '🔇')} <b>میوت‌های فعال:</b> <code>{db.count_mutes()}</code>\n"
        f"┃ {prem('admin', '🛡')} <b>ادمین‌های ثبت‌شده:</b> <code>{db.count_admins()}</code>\n"
        f"┃ 📝 <b>تعداد کل لاگ‌ها:</b> <code>{db.count_logs()}</code>\n"
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
        f"┏━━━ {prem('ban', '🚫')} <b>بن کردن</b> ━━━┓\n"
        "┃ <code>بن</code> | <code>ban</code> | <code>مسدود</code>\n"
        "┃ 🔹 مثال: <code>بن تبلیغات</code>\n"
        "┗━━━━━━━━━━━━━━━┛\n\n"
        f"┏━━━ {prem('unban', '✅')} <b>آنبن کردن</b> ━━━┓\n"
        "┃ <code>آنبن</code> | <code>unban</code>\n"
        "┗━━━━━━━━━━━━━━━┛\n\n"
        f"┏━━━ {prem('mute', '🔇')} <b>سکوت (میوت)</b> ━━━┓\n"
        "┃ <code>سکوت 4h</code> | <code>mute 30m</code>\n"
        "┃ 🔹 فرمت‌ها:\n"
        "┃   <code>s</code> ثانیه | <code>m</code> دقیقه\n"
        "┃   <code>h</code> ساعت | <code>d</code> روز\n"
        "┃ 🔹 پیش‌فرض: ۱ ساعت\n"
        "┃ 🔹 مثال: <code>سکوت 4h اسپم</code>\n"
        "┗━━━━━━━━━━━━━━━┛\n\n"
        f"┏━━━ {prem('unmute', '🔊')} <b>آن‌میوت</b> ━━━┓\n"
        "┃ <code>آن‌میوت</code> | <code>unmute</code>\n"
        "┗━━━━━━━━━━━━━━━┛"
    )
    await event.edit(text, buttons=[[Button.inline("🔙 بازگشت", data=b"back")]], parse_mode="html")


async def cb_settings(event):
    anti_spam = db.get_bool_setting("anti_spam", False)
    anti_link = db.get_bool_setting("anti_link", False)
    text = (
        "╔══════════════════════════════════╗\n"
        f"   {prem('settings', '⚙️')} <b>تنظیمات ربات</b> {prem('settings', '⚙️')}\n"
        "╚══════════════════════════════════╝\n\n"
        f"{prem('fire', '🔥')} <b>قابلیت‌های حفاظتی:</b>\n\n"
        f"┃ 🚨 ضد اسپم: {'✅ روشن' if anti_spam else '❌ خاموش'}\n"
        f"┃ 🔗 ضد لینک: {'✅ روشن' if anti_link else '❌ خاموش'}\n\n"
        "برای تغییر، روی دکمه‌ها کلیک کنید:"
    )
    buttons = [
        [Button.inline(f"🚨 ضد اسپم: {'✅ روشن' if anti_spam else '❌ خاموش'}", data=b"toggle_antispam")],
        [Button.inline(f"🔗 ضد لینک: {'✅ روشن' if anti_link else '❌ خاموش'}", data=b"toggle_antilink")],
        [Button.inline("🔙 بازگشت", data=b"back")],
    ]
    await event.edit(text, buttons=buttons, parse_mode="html")


# ═════════════════════════════════════════════
# ۱۳) دستورات مدیریتی در پیوی
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
        f"{prem('ban', '🚫')} <code>بن</code> - بن کردن\n"
        f"{prem('unban', '✅')} <code>آنبن</code> - آنبن کردن\n"
        f"{prem('mute', '🔇')} <code>سکوت 4h</code> - میوت کردن\n"
        f"{prem('unmute', '🔊')} <code>آن‌میوت</code> - رفع میوت\n\n"
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
        f"{prem('admin', '🛡')} ادمین‌ها: <code>{db.count_admins()}</code>\n"
        f"📝 لاگ‌ها: <code>{db.count_logs()}</code>\n\n"
        f"{prem('time', '⏱')} {now_iran_str()}"
    )
    await event.respond(text, parse_mode="html")


# ═════════════════════════════════════════════
# ۱۴) راه‌اندازی ربات
# ═════════════════════════════════════════════
async def _periodic_cleanup():
    while True:
        try:
            db.cleanup_expired_mutes()
        except Exception as ex:
            logger.warning(f"cleanup error: {ex}")
        await asyncio.sleep(60)


async def main():
    logger.info("🚀 راه‌اندازی ربات گروه‌بان...")

    if not BOT_TOKEN:
        raise ValueError("❌ BOT_TOKEN باید تنظیم شود.")
    if not OWNER_ID:
        raise ValueError("❌ OWNER_ID باید تنظیم شود.")
    if not DATABASE_URL:
        raise ValueError("❌ DATABASE_URL باید تنظیم شود.")

    await client.start(bot_token=BOT_TOKEN)
    me = await client.get_me()
    logger.info(f"✅ ربات متصل شد: @{me.username} (ID: {me.id})")

    # اعلان به همه ادمین‌های ارشد
    for admin_id in SUPER_ADMINS:
        try:
            await client.send_message(
                admin_id,
                "╔══════════════════════════════════╗\n"
                f"   {prem('check', '✅')} <b>ربات با موفقیت روشن شد</b>\n"
                "╚══════════════════════════════════╝\n\n"
                f"┏━━━ {prem('fire', '🔥')} <b>اطلاعات ربات</b> ━━━┓\n"
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


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⛔ ربات توسط کاربر متوقف شد.")
    except Exception as ex:
        logger.exception(f"❌ خطای غیرمنتظره: {ex}")