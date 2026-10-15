import os
import random
import logging
import sqlite3
import threading
import time
from datetime import datetime, timedelta, time as dtime
from functools import wraps
from contextlib import contextmanager
from zoneinfo import ZoneInfo

import telebot
from telebot.apihelper import ApiTelegramException

# ---------- Railway environment variables ----------
BOT_TOKEN = os.environ["BOT_TOKEN"]
CHANNEL_ID = os.environ["CHANNEL_ID"]
ADMIN_ID = int(os.environ.get("ADMIN_ID", "8469326059"))
DATA_DIR = os.environ.get("DATA_DIR", "/data")
DB_PATH = os.path.join(DATA_DIR, "gmgn.db")
GRACE_MINUTES = int(os.environ.get("GRACE_MINUTES", "30"))

TZ = ZoneInfo("Asia/Kolkata")
GM_WINDOW = (dtime(6, 0), dtime(11, 0))
GN_WINDOW = (dtime(21, 0), dtime(23, 30))
POLL_SECONDS = 30

os.makedirs(DATA_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nightbot")
bot = telebot.TeleBot(BOT_TOKEN)
db_lock = threading.RLock()

HELP_TEXT = (
    "👋 GM/GN Channel Bot\n\n"
    "/add_gm <message> — add a GM message\n"
    "/add_gn <message> — add a GN message\n"
    "/list_gm and /list_gn — view saved messages\n"
    "/del_gm <id> and /del_gn <id> — delete a message\n"
    "/resend_gm yes /resend_gn yes — send manually if not sent\n"
    "/resend_gm force /resend_gn force — confirm an unknown post was not sent\n"
    "/status — show today's schedule and message counts"
)

@contextmanager
def connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with db_lock, connect() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL CHECK(kind IN ('gm','gn')),
            text TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS slots (
            day TEXT NOT NULL,
            kind TEXT NOT NULL,
            status TEXT NOT NULL,
            detail TEXT DEFAULT '',
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(day, kind)
        );
        """)

def query(sql, params=()):
    with db_lock, connect() as conn:
        return conn.execute(sql, params).fetchall()

def execute(sql, params=()):
    with db_lock, connect() as conn:
        cur = conn.execute(sql, params)
        return cur.rowcount

def tx(fn):
    with db_lock, connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            result = fn(conn)
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise

def get_state(key, default=None):
    rows = query("SELECT value FROM state WHERE key=?", (key,))
    return rows[0]["value"] if rows else default

def put_state(conn, key, value):
    conn.execute(
        "INSERT INTO state(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )

def random_time(window):
    start, end = window
    start_min = start.hour * 60 + start.minute
    end_min = end.hour * 60 + end.minute
    minute = random.randint(start_min, end_min)
    return f"{minute // 60:02d}:{minute % 60:02d}"

def ensure_plan(conn, day):
    row = conn.execute("SELECT value FROM state WHERE key='plan_date'").fetchone()
    if row and row["value"] == day:
        return
    put_state(conn, "plan_date", day)
    put_state(conn, "gm_time", random_time(GM_WINDOW))
    put_state(conn, "gn_time", random_time(GN_WINDOW))

def set_slot(conn, day, kind, status, detail=""):
    conn.execute(
        "INSERT INTO slots(day,kind,status,detail) VALUES(?,?,?,?) "
        "ON CONFLICT(day,kind) DO UPDATE SET status=excluded.status, "
        "detail=excluded.detail, updated_at=CURRENT_TIMESTAMP",
        (day, kind, status, detail),
    )

def admin_only(func):
    @wraps(func)
    def wrapper(message, *args, **kwargs):
        if message.from_user and message.from_user.id == ADMIN_ID:
            return func(message, *args, **kwargs)
        bot.reply_to(message, "Sorry, this command is only available to the bot admin.")
    return wrapper

def notify_admin(message):
    try:
        bot.send_message(ADMIN_ID, message)
    except Exception:
        log.exception("Could not notify admin")

def choose_message(kind):
    rows = query("SELECT id,text FROM messages WHERE kind=? ORDER BY RANDOM() LIMIT 1", (kind,))
    return (rows[0]["id"], rows[0]["text"]) if rows else None

def send_scheduled(kind, day):
    def claim(conn):
        existing = conn.execute(
            "SELECT status,detail FROM slots WHERE day=? AND kind=?", (day, kind)
        ).fetchone()
        if existing:
            return ("exists", existing["status"])
        rows = conn.execute(
            "SELECT id,text FROM messages WHERE kind=? ORDER BY RANDOM() LIMIT 1", (kind,)
        ).fetchone()
        if not rows:
            set_slot(conn, day, kind, "skipped", "message list is empty")
            return ("empty", None)
        set_slot(conn, day, kind, "sending", f"message #{rows['id']}")
        return ("send", (rows["id"], rows["text"]))

    state, payload = tx(claim)
    if state == "exists":
        return
    if state == "empty":
        notify_admin(f"⚠️ {kind.upper()} was skipped today because its message list is empty.")
        return

    msg_id, message_text = payload
    try:
        bot.send_message(CHANNEL_ID, message_text)
    except ApiTelegramException as exc:
        code = getattr(exc, "error_code", None)
        status = "failed" if code and 400 <= code < 500 else "unknown"
        tx(lambda conn: set_slot(conn, day, kind, status, f"Telegram error {code}: {exc}"))
        notify_admin(f"❌ {kind.upper()} post {status}: {exc}\nCheck the channel before manually resending.")
        return
    except Exception as exc:
        tx(lambda conn: set_slot(conn, day, kind, "unknown", str(exc)))
        notify_admin(f"⚠️ {kind.upper()} result is unknown: {exc}\nCheck the channel before resending.")
        return

    tx(lambda conn: set_slot(conn, day, kind, "sent", f"message #{msg_id}"))
    log.info("%s posted for %s", kind.upper(), day)

def recover_stale(conn):
    rows = conn.execute(
        "SELECT day,kind FROM slots WHERE status='sending'"
    ).fetchall()
    for row in rows:
        set_slot(conn, row["day"], row["kind"], "unknown",
                 "process restarted while sending; check channel before retrying")
    return [(row["day"], row["kind"]) for row in rows]

def scheduler():
    while True:
        try:
            now = datetime.now(TZ)
            day = now.strftime("%Y-%m-%d")
            tx(lambda conn: ensure_plan(conn, day))
            for kind in ("gm", "gn"):
                planned = get_state(f"{kind}_time", "00:00")
                hour, minute = map(int, planned.split(":"))
                planned_dt = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if now < planned_dt:
                    continue
                if now <= planned_dt + timedelta(minutes=GRACE_MINUTES):
                    send_scheduled(kind, day)
                else:
                    def skip(conn, k=kind):
                        row = conn.execute(
                            "SELECT 1 FROM slots WHERE day=? AND kind=?", (day, k)
                        ).fetchone()
                        if not row:
                            set_slot(conn, day, k, "skipped", "grace period expired")
                    tx(skip)
        except Exception:
            log.exception("Scheduler loop error")
        time.sleep(POLL_SECONDS)

@bot.message_handler(commands=["start", "help"], chat_types=["private"])
@admin_only
def help_cmd(message):
    bot.reply_to(message, HELP_TEXT)

@bot.message_handler(commands=["add_gm", "add_gn"], chat_types=["private"])
@admin_only
def add_msg(message):
    kind = "gm" if message.text.split()[0].split("@")[0] == "/add_gm" else "gn"
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        bot.reply_to(message, f"Usage: /add_{kind} Your message here")
        return
    with db_lock, connect() as conn:
        cur = conn.execute("INSERT INTO messages(kind,text) VALUES(?,?)", (kind, parts[1].strip()))
        msg_id = cur.lastrowid
    bot.reply_to(message, f"✅ Added {kind.upper()} message #{msg_id}.")

@bot.message_handler(commands=["list_gm", "list_gn"], chat_types=["private"])
@admin_only
def list_msgs(message):
    kind = "gm" if message.text.split()[0].split("@")[0] == "/list_gm" else "gn"
    rows = query("SELECT id,text FROM messages WHERE kind=? ORDER BY id", (kind,))
    if not rows:
        bot.reply_to(message, f"The {kind.upper()} message list is empty.")
        return
    output = "\n\n".join(f"#{row['id']} — {row['text']}" for row in rows)
    for start in range(0, len(output), 3500):
        bot.send_message(message.chat.id, output[start:start + 3500])

@bot.message_handler(commands=["del_gm", "del_gn"], chat_types=["private"])
@admin_only
def del_msg(message):
    kind = "gm" if message.text.split()[0].split("@")[0] == "/del_gm" else "gn"
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        bot.reply_to(message, f"Usage: /del_{kind} <id>")
        return
    count = execute("DELETE FROM messages WHERE id=? AND kind=?", (int(parts[1]), kind))
    bot.reply_to(message, "🗑 Message deleted." if count else "That message ID was not found.")

@bot.message_handler(commands=["resend_gm", "resend_gn"], chat_types=["private"])
@admin_only
def resend(message):
    kind = "gm" if message.text.split()[0].split("@")[0] == "/resend_gm" else "gn"
    parts = message.text.split()
    mode = parts[1].lower() if len(parts) > 1 else ""
    day = datetime.now(TZ).strftime("%Y-%m-%d")
    rows = query("SELECT status,detail FROM slots WHERE day=? AND kind=?", (day, kind))
    status = rows[0]["status"] if rows else "pending"
    detail = rows[0]["detail"] if rows else ""

    if status == "sent":
        bot.reply_to(message, f"Today's {kind.upper()} is already marked sent; no duplicate was sent.")
        return
    if status == "unknown" and mode != "force":
        bot.reply_to(
            message,
            f"Today's {kind.upper()} result is unknown. Check the channel first. "
            f"If it definitely was NOT posted, send /resend_{kind} force.",
        )
        return
    if status in ("failed", "skipped", "pending") and mode not in ("yes", "force"):
        bot.reply_to(message, f"To send now, use /resend_{kind} yes.")
        return
    if status == "unknown" and mode == "force":
        # Explicit admin confirmation is required to avoid accidental duplicate posts.
        pass

    chosen = choose_message(kind)
    if not chosen:
        bot.reply_to(message, f"The {kind.upper()} message list is empty.")
        return
    msg_id, text = chosen
    tx(lambda conn: set_slot(conn, day, kind, "sending", f"manual message #{msg_id}"))
    try:
        bot.send_message(CHANNEL_ID, text)
    except Exception as exc:
        tx(lambda conn: set_slot(conn, day, kind, "unknown", f"manual resend: {exc}"))
        bot.reply_to(message, f"Resend result is unknown: {exc}. Check the channel before trying again.")
        return
    tx(lambda conn: set_slot(conn, day, kind, "sent", f"manual message #{msg_id}"))
    bot.reply_to(message, f"✅ {kind.upper()} message sent.")

@bot.message_handler(commands=["status"], chat_types=["private"])
@admin_only
def status_cmd(message):
    day = datetime.now(TZ).strftime("%Y-%m-%d")
    counts = {
        kind: query("SELECT COUNT(*) AS total FROM messages WHERE kind=?", (kind,))[0]["total"]
        for kind in ("gm", "gn")
    }
    def slot_line(kind):
        planned = get_state(f"{kind}_time", "not planned")
        rows = query("SELECT status,detail FROM slots WHERE day=? AND kind=?", (day, kind))
        if rows:
            detail = f" ({rows[0]['detail']})" if rows[0]["detail"] else ""
            return f"{planned} IST → {rows[0]['status']}{detail}"
        return f"{planned} IST → pending"
    bot.reply_to(
        message,
        f"📊 Status for {day}\n"
        f"GM: {slot_line('gm')}\n"
        f"GN: {slot_line('gn')}\n"
        f"GM messages: {counts['gm']}\n"
        f"GN messages: {counts['gn']}\n"
        f"Grace period: {GRACE_MINUTES} minutes",
    )

if __name__ == "__main__":
    init_db()
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    tx(lambda conn: ensure_plan(conn, today))
    stale = tx(recover_stale)
    threading.Thread(target=scheduler, daemon=True, name="scheduler").start()
    for day, kind in stale:
        notify_admin(
            f"⚠️ {kind.upper()} ({day}) was marked unknown after restart. "
            "Check the channel before manually resending."
        )
    log.info("Bot started; timezone Asia/Kolkata")
    bot.infinity_polling(skip_pending=True, timeout=30, long_polling_timeout=25)
