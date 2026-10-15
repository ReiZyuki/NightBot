import os
import random
import logging
import sqlite3
import threading
import time
from datetime import datetime, timedelta, time as dtime
from functools import partial
from zoneinfo import ZoneInfo

import requests
import urllib3
import telebot
from telebot.apihelper import ApiTelegramException

# ---------- Config (Railway Variables) ----------

BOT_TOKEN = os.environ["BOT_TOKEN"]
CHANNEL_ID = os.environ["CHANNEL_ID"]  # -100xxxxxxxxxx ya @channelusername
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
log = logging.getLogger("gmbot")

bot = telebot.TeleBot(BOT_TOKEN)
db_lock = threading.Lock()

HELP_TEXT = (
    "👋 GM/GN Channel Bot\n\n"
    "➕ /add_gm <message>\n"
    "➕ /add_gn <message>\n"
    "📜 /list_gm  |  /list_gn\n"
    "🗑 /del_gm <id>  |  /del_gn <id>\n"
    "🔁 /resend_gm  |  /resend_gn (instructions dikhata hai)\n"
    "📊 /status"
)

SAFE_RETRY, PERMANENT, UNKNOWN = "safe_retry", "permanent", "unknown"

# ---------- SQLite helpers ----------

def init_db():
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        try:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS messages (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                kind       TEXT NOT NULL CHECK (kind IN ('gm', 'gn')),
                text       TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS state (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS slots (
                day        TEXT NOT NULL,
                kind       TEXT NOT NULL,
                status     TEXT NOT NULL,
                detail     TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (day, kind)
            );
            """)
            conn.commit()
        finally:
            conn.close()

def query(sql, params=()):
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()

def execute(sql, params=()):
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.execute(sql, params)
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()

def tx(fn):
    """fn(conn) ek atomic transaction me chalta hai.
    fn ke andar query()/execute() mat call karna (db_lock deadlock hoga)."""
    with db_lock:
        conn = sqlite3.connect(DB_PATH, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("BEGIN IMMEDIATE")
            result = fn(conn)
            conn.execute("COMMIT")
            return result
        except BaseException:
            if conn.in_transaction:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    log.exception("ROLLBACK fail hua")
            raise
        finally:
            conn.close()

def upsert_state(conn, key, value):
    conn.execute(
        "INSERT INTO state (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )

def get_state(key, default=None):
    rows = query("SELECT value FROM state WHERE key = ?", (key,))
    return rows[0]["value"] if rows else default

def rand_time(window):
    start, end = window
    s = start.hour * 60 + start.minute
    e = end.hour * 60 + end.minute
    m = random.randint(s, e)
    return f"{m // 60:02d}:{m % 60:02d}"

# ---------- Error classification ----------

def _reaches_connect_stage(exc):
    """Exception chain me dekho ki fail connect stage pe hua (request Telegram tak gayi hi nahi)."""
    stack, seen = [exc], set()
    while stack:
        e = stack.pop()
        if e is None or id(e) in seen:
            continue
        seen.add(id(e))
        if isinstance(e, (
            urllib3.exceptions.ConnectTimeoutError,
            urllib3.exceptions.NewConnectionError,
        )):
            return True
        stack.extend([
            getattr(e, "__cause__", None),
            getattr(e, "__context__", None),
            getattr(e, "reason", None),
        ])
    return False

def classify(exc):
    """Returns (verdict, reason).
    SAFE_RETRY : message bhejne se pehle hi fail (connect stage) ya 429 -> retry safe
    PERMANENT  : Telegram ne 4xx diya -> retry nahi, config check karo
    UNKNOWN    : request gayi par jawab nahi mila / 5xx / baaki sab -> auto-retry nahi"""
    if isinstance(exc, ApiTelegramException):
        code = exc.error_code
        desc = getattr(exc, "description", None) or str(exc)
        if code == 429:
            return SAFE_RETRY, "rate limit (429)"
        if code is not None and 400 <= code < 500:
            return PERMANENT, f"Telegram {code}: {desc}"
        return UNKNOWN, f"Telegram {code}: {desc}"
    if isinstance(exc, requests.exceptions.ConnectionError) and _reaches_connect_stage(exc):
        return SAFE_RETRY, "connect stage pe fail, message Telegram tak nahi gaya"
    return UNKNOWN, f"{type(exc).__name__}: {exc}"

# ---------- Transaction-safe slot ops ----------

def ensure_plan(conn, today):
    row = conn.execute("SELECT value FROM state WHERE key = 'plan_date'").fetchone()
    if row and row["value"] == today:
        return
    upsert_state(conn, "plan_date", today)
    upsert_state(conn, "gm_time", rand_time(GM_WINDOW))
    upsert_state(conn, "gn_time", rand_time(GN_WINDOW))

def set_slot(conn, day, kind, status, detail=""):
    conn.execute(
        "INSERT INTO slots (day, kind, status, detail) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(day, kind) DO UPDATE SET "
        "status = excluded.status, detail = excluded.detail, updated_at = CURRENT_TIMESTAMP",
        (day, kind, status, detail),
    )

def claim_slot(conn, day, kind):
    """Returns (msg_id, text), 'empty', ya None (slot pehle se hai)."""
    if conn.execute(
        "SELECT 1 FROM slots WHERE day = ? AND kind = ?", (day, kind)
    ).fetchone():
        return None
    msg = conn.execute(
        "SELECT id, text FROM messages WHERE kind = ? ORDER BY RANDOM() LIMIT 1",
        (kind,),
    ).fetchone()
    if msg is None:
        return "empty"
    set_slot(conn, day, kind, "sending", f"msg #{msg['id']}")
    return msg["id"], msg["text"]

def release_slot(conn, day, kind):
    conn.execute("DELETE FROM slots WHERE day = ? AND kind = ?", (day, kind))

def skip_slot(conn, day, kind):
    if conn.execute(
        "SELECT 1 FROM slots WHERE day = ? AND kind = ?", (day, kind)
    ).fetchone():
        return False
    set_slot(conn, day, kind, "skipped", "deadline nikal gaya")
    return True

def recover_stale(conn):
    """Restart ke waqt jo slot 'sending' tha, uska result pata nahi -> unknown."""
    rows = conn.execute(
        "SELECT day, kind FROM slots WHERE status = 'sending'"
    ).fetchall()
    for row in rows:
        set_slot(
            conn, row["day"], row["kind"], "unknown",
            "restart ke waqt send ho raha tha, channel check karo",
        )
    return [(row["day"], row["kind"]) for row in rows]

def manual_pick(conn, day, kind, word):
    """Returns (state, payload):
    ('already', status)    -> sent/sending hai, kuch nahi karna
    ('need_force', detail) -> unknown slot, 'force' chahiye
    ('need_yes', None)     -> failed/khaali slot, 'yes' chahiye
    ('empty', None)        -> list khaali
    ('ok', (msg_id, text)) -> claim ho gaya, ab send karo
    """
    row = conn.execute(
        "SELECT status, detail FROM slots WHERE day = ? AND kind = ?",
        (day, kind),
    ).fetchone()
    status = row["status"] if row else None
    if status in ("sent", "sending"):
        return "already", status
    if status == "unknown" and word != "force":
        return "need_force", row["detail"]
    if status != "unknown" and word not in ("yes", "force"):
        return "need_yes", None
    msg = conn.execute(
        "SELECT id, text FROM messages WHERE kind = ? ORDER BY RANDOM() LIMIT 1",
        (kind,),
    ).fetchone()
    if msg is None:
        return "empty", None
    set_slot(conn, day, kind, "sending", f"manual msg #{msg['id']}")
    return "ok", (msg["id"], msg["text"])

# ---------- Helpers ----------

def deadline_for(kind, planned_dt):
    window_end = GM_WINDOW[1] if kind == "gm" else GN_WINDOW[1]
    end_dt = datetime.combine(planned_dt.date(), window_end, tzinfo=TZ)
    return min(planned_dt + timedelta(minutes=GRACE_MINUTES), end_dt)

def notify_admin(text):
    try:
        bot.send_message(ADMIN_ID, text)
    except Exception as e:
        log.error("Admin notify fail: %s", e)

def admin_only(func):
    def wrapper(m):
        if not m.from_user or m.from_user.id != ADMIN_ID:
            bot.reply_to(m, "⛔ Ye bot sirf admin ke liye hai.")
            return
        return func(m)
    wrapper.__name__ = func.__name__
    return wrapper

def failure_alert(kind, day, verdict, reason):
    if verdict == PERMANENT:
        return (
            f"❌ {kind.upper()} ({day}) post fail hua.\n{reason}\n"
            "Permanent error hai, auto-retry nahi hoga. Config check karo "
            "(bot channel me admin hai? CHANNEL_ID sahi hai?)."
        )
    return (
        f"⚠️ {kind.upper()} ({day}) ka result unknown hai.\n{reason}\n"
        "Auto-retry nahi hoga taaki duplicate na ho. Channel check karo; "
        f"agar post nahi hua to /resend_{kind} force bhejo."
    )

# ---------- Scheduler ----------

def scheduler():
    warned = set()
    while True:
        try:
            now = datetime.now(TZ)
            today = now.strftime("%Y-%m-%d")
            tx(partial(ensure_plan, today=today))

            for kind in ("gm", "gn"):
                planned_text = get_state(f"{kind}_time")
                if not planned_text:
                    continue
                planned = datetime.combine(
                    now.date(),
                    dtime.fromisoformat(planned_text),
                    tzinfo=TZ,
                )
                if now < planned:
                    continue

                if query(
                    "SELECT 1 FROM slots WHERE day = ? AND kind = ?",
                    (today, kind),
                ):
                    continue

                deadline = deadline_for(kind, planned)
                if now > deadline:
                    if tx(partial(skip_slot, day=today, kind=kind)):
                        log.warning("%s skip", kind.upper())
                        notify_admin(
                            f"⚠️ Aaj ka {kind.upper()} skip ho gaya.\n"
                            f"Planned: {planned:%H:%M} IST, deadline: {deadline:%H:%M} IST."
                        )
                    continue

                claimed = tx(partial(claim_slot, day=today, kind=kind))
                if claimed is None:
                    continue
                if claimed == "empty":
                    if (kind, today) not in warned:
                        warned.add((kind, today))
                        log.warning("%s list khali hai", kind.upper())
                        notify_admin(
                            f"⚠️ {kind.upper()} list khali hai!\n"
                            f"{deadline:%H:%M} IST tak message add karo, warna aaj ka {kind.upper()} skip ho jayega."
                        )
                    continue

                msg_id, text = claimed
                try:
                    bot.send_message(CHANNEL_ID, text)
                except Exception as e:
                    verdict, reason = classify(e)
                    if verdict == SAFE_RETRY:
                        tx(partial(release_slot, day=today, kind=kind))
                        log.warning("%s safe retry: %s", kind.upper(), reason)
                        continue
                    status = "failed" if verdict == PERMANENT else "unknown"
                    tx(partial(set_slot, day=today, kind=kind, status=status, detail=reason))
                    log.error("%s %s: %s", kind.upper(), status, reason)
                    notify_admin(failure_alert(kind, today, verdict, reason))
                    continue

                tx(partial(
                    set_slot, day=today, kind=kind, status="sent",
                    detail=f"msg #{msg_id}",
                ))
                log.info("%s post ho gaya (msg #%s)", kind.upper(), msg_id)
        except Exception:
            log.exception("Scheduler error")
        time.sleep(POLL_SECONDS)

# ---------- Commands (sirf private chat, sirf admin) ----------

@bot.message_handler(commands=["start", "help"], chat_types=["private"])
@admin_only
def help_cmd(m):
    bot.reply_to(m, HELP_TEXT)

@bot.message_handler(commands=["add_gm", "add_gn"], chat_types=["private"])
@admin_only
def add_msg(m):
    kind = "gm" if m.text.split()[0].split("@")[0] == "/add_gm" else "gn"
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        bot.reply_to(m, f"Usage: /add_{kind} <message>")
        return
    execute("INSERT INTO messages (kind, text) VALUES (?, ?)", (kind, parts[1].strip()))
    count = query("SELECT COUNT(*) AS c FROM messages WHERE kind = ?", (kind,))[0]["c"]
    bot.reply_to(m, f"✅ {kind.upper()} message add ho gaya (total: {count})")

@bot.message_handler(commands=["list_gm", "list_gn"], chat_types=["private"])
@admin_only
def list_msgs(m):
    kind = "gm" if m.text.split()[0].split("@")[0] == "/list_gm" else "gn"
    rows = query("SELECT id, text FROM messages WHERE kind = ? ORDER BY id", (kind,))
    if not rows:
        bot.reply_to(m, f"{kind.upper()} list khali hai.")
        return
    output = "\n\n".join(f"#{row['id']} {row['text']}" for row in rows)
    bot.send_message(m.chat.id, output[:4000])

@bot.message_handler(commands=["del_gm", "del_gn"], chat_types=["private"])
@admin_only
def del_msg(m):
    kind = "gm" if m.text.split()[0].split("@")[0] == "/del_gm" else "gn"
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        bot.reply_to(m, f"Usage: /del_{kind} <id> (id /list_{kind} me dikhegi)")
        return
    msg_id = int(parts[1].strip())
    deleted = execute(
        "DELETE FROM messages WHERE id = ? AND kind = ?", (msg_id, kind)
    )
    if deleted:
        bot.reply_to(m, f"🗑 {kind.upper()} #{msg_id} delete ho gaya")
    else:
        bot.reply_to(m, "❌ Ye id nahi mili")

@bot.message_handler(commands=["resend_gm", "resend_gn"], chat_types=["private"])
@admin_only
def resend(m):
    kind = "gm" if m.text.split()[0].split("@")[0] == "/resend_gm" else "gn"
    KIND = kind.upper()
    parts = m.text.split()
    word = parts[1].lower() if len(parts) > 1 else ""
    today = datetime.now(TZ).strftime("%Y-%m-%d")

    state, payload = tx(partial(manual_pick, day=today, kind=kind, word=word))

    if state == "already":
        bot.reply_to(m, f"ℹ️ Aaj ka {KIND} already '{payload}' hai, resend nahi hoga.")
        return
    if state == "need_force":
        bot.reply_to(
            m,
            f"⚠️ Aaj ka {KIND} 'unknown' hai.\n"
            f"Detail: {payload}\n\n"
            "Pehle channel check karo. Agar post dikh raha hai to kuch mat karo "
            "(duplicate ho jayega).\n"
            f"Sirf pakka post nahi hua ho to bhejo: /resend_{kind} force",
        )
        return
    if state == "need_yes":
        bot.reply_to(
            m,
            f"ℹ️ Aaj ka {KIND} abhi bheja nahi gaya (ya fail hua).\n"
            f"Abhi bhejna hai to bhejo: /resend_{kind} yes",
        )
        return
    if state == "empty":
        bot.reply_to(m, f"❌ {KIND} list khali hai, pehle message add karo.")
        return

    msg_id, text = payload
    try:
        bot.send_message(CHANNEL_ID, text)
    except Exception as e:
        verdict, reason = classify(e)
        if verdict == SAFE_RETRY:
            tx(partial(release_slot, day=today, kind=kind))
            bot.reply_to(
                m,
                "⏳ Temporary issue, message channel tak nahi gaya.\n"
                f"{reason}\n"
                f"Slot release kiya. Thodi der me /resend_{kind} yes bhejo, "
                "ya scheduler agle poll pe apne aap try karega.",
            )
            return
        status = "failed" if verdict == PERMANENT else "unknown"
        tx(partial(
            set_slot, day=today, kind=kind, status=status,
            detail=f"manual resend: {reason}",
        ))
        bot.reply_to(m, f"❌ Resend fail ({status}): {reason}")
        return

    tx(partial(
        set_slot, day=today, kind=kind, status="sent",
        detail=f"manual msg #{msg_id}",
    ))
    bot.reply_to(m, f"✅ {KIND} resend ho gaya")

@bot.message_handler(commands=["status"], chat_types=["private"])
@admin_only
def status(m):
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    gm_count = query("SELECT COUNT(*) AS c FROM messages WHERE kind = 'gm'")[0]["c"]
    gn_count = query("SELECT COUNT(*) AS c FROM messages WHERE kind = 'gn'")[0]["c"]

    def slot_line(kind):
        planned = get_state(f"{kind}_time", "-")
        rows = query(
            "SELECT status, detail FROM slots WHERE day = ? AND kind = ?",
            (today, kind),
        )
        if rows:
            st, detail = rows[0]["status"], rows[0]["detail"] or ""
            extra = f" ({detail})" if detail else ""
            return f"{planned} IST → {st}{extra}"
        return f"{planned} IST → pending"

    bot.reply_to(
        m,
        f"📊 Status\n"
        f"Aaj GM: {slot_line('gm')}\n"
        f"Aaj GN: {slot_line('gn')}\n"
        f"GM messages: {gm_count}\n"
        f"GN messages: {gn_count}\n"
        f"Grace: {GRACE_MINUTES} min",
    )

# ---------- Main ----------

if __name__ == "__main__":
    init_db()
    stale = tx(recover_stale)
    threading.Thread(target=scheduler, daemon=True).start()
    log.info("Bot start ho gaya")
    for day, kind in stale:
        notify_admin(
            f"⚠️ {kind.upper()} ({day}) restart ke waqt send ho raha tha. "
            "Status 'unknown' set kiya. Channel check karo."
        )
    bot.infinity_polling(skip_pending=True)
