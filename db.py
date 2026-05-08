import aiosqlite
import time
from datetime import datetime, timedelta
from msk_time import now_msk
from typing import Any, Optional
from config import DB_PATH, DEFAULTS, OWNER_ID

FUNTIME_TEST_DEFAULTS = (
    ("test4", "test4.funtime.sh"),
    ("test-neo", "test-neo.funtime.sh"),
)

TOKEN_SHOP_DEFAULT_CATEGORIES = (
    ("Ресурсы", "<b>Ресурсы</b>\n\nВыберите товар из категории ниже.", 10),
    ("Редкое", "<b>Редкое</b>\n\nЗдесь собраны более редкие товары.", 20),
    ("Донат", "<b>Донат</b>\n\nВыберите донат-товар для покупки.", 30),
    ("Наборы", "<b>Наборы</b>\n\nГотовые наборы доступны ниже.", 40),
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    full_name TEXT,
    balance INTEGER DEFAULT 0,
    referrals INTEGER DEFAULT 0,
    referrer_id INTEGER,
    referral_rewarded INTEGER DEFAULT 0,
    captcha_passed INTEGER DEFAULT 0,
    last_bonus INTEGER DEFAULT 0,
    last_theft INTEGER DEFAULT 0,
    last_subscription_check INTEGER DEFAULT 0,
    minecraft_nick TEXT,
    banned INTEGER DEFAULT 0,
    protected INTEGER DEFAULT 0,
    created_at INTEGER,
    daily_actions INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS admins (
    user_id INTEGER PRIMARY KEY,
    added_by INTEGER,
    added_at INTEGER
);
CREATE TABLE IF NOT EXISTS channels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT, -- 'start' or 'tasks'
    link TEXT,
    chat_id TEXT,
    title TEXT,
    is_private INTEGER DEFAULT 0,
    max_subs INTEGER DEFAULT 0, -- 0 = infinite
    current_subs INTEGER DEFAULT 0,
    active INTEGER DEFAULT 1,
    invite_link TEXT,           -- бот-генерируемая ссылка (creates_join_request=true)
    invite_link_name TEXT       -- имя/метка ссылки в Telegram
);
CREATE TABLE IF NOT EXISTS channel_join_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id INTEGER,
    user_id INTEGER,
    invite_link TEXT,
    event_type TEXT DEFAULT 'request',
    created_at INTEGER
);
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    reward INTEGER DEFAULT 0,
    max_completions INTEGER DEFAULT 0,
    completions INTEGER DEFAULT 0,
    active INTEGER DEFAULT 1,
    task_type TEXT DEFAULT 'manual',
    channel_id INTEGER,
    check_text TEXT DEFAULT '',
    check_scope TEXT DEFAULT 'name',
    reset_period TEXT DEFAULT 'once',
    last_reset_date TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS task_completions (
    user_id INTEGER,
    task_id INTEGER,
    completed_at INTEGER,
    PRIMARY KEY(user_id, task_id)
);
CREATE TABLE IF NOT EXISTS task_submissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    task_id INTEGER,
    submission_text TEXT,
    status TEXT DEFAULT 'pending', -- pending, approved, rejected
    created_at INTEGER,
    processed_at INTEGER,
    processed_by INTEGER
);
CREATE TABLE IF NOT EXISTS shop_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    description TEXT,
    price INTEGER,
    income_per_day INTEGER DEFAULT 0,
    emoji_icon TEXT DEFAULT '',
    active INTEGER DEFAULT 1
);
CREATE TABLE IF NOT EXISTS user_farms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    item_id INTEGER,
    bought_at INTEGER,
    last_collected INTEGER
);
CREATE TABLE IF NOT EXISTS token_shop_categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    menu_text TEXT DEFAULT '',
    emoji_icon TEXT DEFAULT '',
    active INTEGER DEFAULT 1,
    sort_order INTEGER DEFAULT 0,
    created_at INTEGER
);
CREATE TABLE IF NOT EXISTS token_shop_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category_id INTEGER,
    name TEXT,
    description TEXT DEFAULT '',
    price INTEGER DEFAULT 0,
    emoji_icon TEXT DEFAULT '',
    active INTEGER DEFAULT 1,
    sort_order INTEGER DEFAULT 0,
    created_at INTEGER
);
CREATE TABLE IF NOT EXISTS token_shop_inventory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    item_id INTEGER,
    category_id INTEGER,
    item_name TEXT,
    category_name TEXT,
    price INTEGER DEFAULT 0,
    emoji_icon TEXT DEFAULT '',
    status TEXT DEFAULT 'available',
    bought_at INTEGER,
    updated_at INTEGER
);
CREATE TABLE IF NOT EXISTS token_shop_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inventory_id INTEGER,
    user_id INTEGER,
    item_id INTEGER,
    category_id INTEGER,
    item_name TEXT,
    category_name TEXT,
    price INTEGER DEFAULT 0,
    anarchy_number TEXT DEFAULT '',
    minecraft_nick TEXT DEFAULT '',
    status TEXT DEFAULT 'pending',
    created_at INTEGER,
    processed_at INTEGER,
    processed_by INTEGER
);
CREATE TABLE IF NOT EXISTS token_shop_purchases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    item_id INTEGER,
    category_id INTEGER,
    item_name TEXT,
    category_name TEXT,
    price INTEGER DEFAULT 0,
    anarchy_number TEXT DEFAULT '',
    minecraft_nick TEXT DEFAULT '',
    status TEXT DEFAULT 'inventory',
    bought_at INTEGER,
    processed_at INTEGER,
    processed_by INTEGER
);
CREATE TABLE IF NOT EXISTS promocodes (
    code TEXT PRIMARY KEY,
    amount INTEGER,
    activations_left INTEGER,
    activations_total INTEGER
);
CREATE TABLE IF NOT EXISTS promo_uses (
    code TEXT,
    user_id INTEGER,
    used_at INTEGER,
    PRIMARY KEY(code, user_id)
);
CREATE TABLE IF NOT EXISTS withdrawals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount INTEGER,
    minecraft_nick TEXT,
    status TEXT DEFAULT 'pending', -- pending, approved, rejected_refund, rejected_no_refund
    created_at INTEGER,
    processed_at INTEGER,
    processed_by INTEGER
);
CREATE TABLE IF NOT EXISTS funtime_test_ips (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT,
    ip TEXT
);
CREATE TABLE IF NOT EXISTS admin_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    admin_id INTEGER,
    action TEXT,
    created_at INTEGER
);
CREATE TABLE IF NOT EXISTS player_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    action TEXT,
    created_at INTEGER
);
CREATE TABLE IF NOT EXISTS channel_stats (
    channel_id INTEGER PRIMARY KEY,
    join_requests INTEGER DEFAULT 0,
    members INTEGER DEFAULT 0,
    reach INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS token_shop_cart (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    item_id INTEGER NOT NULL,
    category_id INTEGER NOT NULL,
    item_name TEXT NOT NULL,
    category_name TEXT NOT NULL,
    price INTEGER NOT NULL,
    emoji_icon TEXT DEFAULT '',
    added_at INTEGER NOT NULL
);
"""


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        # --- лёгкие миграции для существующих БД ---
        async with db.execute("PRAGMA table_info(users)") as cur:
            user_cols = {row[1] for row in await cur.fetchall()}
        if "referral_rewarded" not in user_cols:
            await db.execute("ALTER TABLE users ADD COLUMN referral_rewarded INTEGER DEFAULT 0")
            await db.execute(
                "UPDATE users SET referral_rewarded=1 WHERE referrer_id IS NOT NULL"
            )
        if "last_theft" not in user_cols:
            await db.execute("ALTER TABLE users ADD COLUMN last_theft INTEGER DEFAULT 0")
        if "last_subscription_check" not in user_cols:
            await db.execute("ALTER TABLE users ADD COLUMN last_subscription_check INTEGER DEFAULT 0")
        if "daily_actions" not in user_cols:
            await db.execute("ALTER TABLE users ADD COLUMN daily_actions INTEGER DEFAULT 0")
        async with db.execute("PRAGMA table_info(channel_join_log)") as cur:
            channel_join_cols = {row[1] for row in await cur.fetchall()}
        if "event_type" not in channel_join_cols:
            await db.execute("ALTER TABLE channel_join_log ADD COLUMN event_type TEXT DEFAULT 'request'")
        async with db.execute("PRAGMA table_info(channels)") as cur:
            cols = {row[1] for row in await cur.fetchall()}
        if "invite_link" not in cols:
            await db.execute("ALTER TABLE channels ADD COLUMN invite_link TEXT")
        if "invite_link_name" not in cols:
            await db.execute("ALTER TABLE channels ADD COLUMN invite_link_name TEXT")
        async with db.execute("PRAGMA table_info(tasks)") as cur:
            task_cols = {row[1] for row in await cur.fetchall()}
        if "task_type" not in task_cols:
            await db.execute("ALTER TABLE tasks ADD COLUMN task_type TEXT DEFAULT 'manual'")
        if "channel_id" not in task_cols:
            await db.execute("ALTER TABLE tasks ADD COLUMN channel_id INTEGER")
        if "check_text" not in task_cols:
            await db.execute("ALTER TABLE tasks ADD COLUMN check_text TEXT DEFAULT ''")
        if "check_scope" not in task_cols:
            await db.execute("ALTER TABLE tasks ADD COLUMN check_scope TEXT DEFAULT 'name'")
        if "reset_period" not in task_cols:
            await db.execute("ALTER TABLE tasks ADD COLUMN reset_period TEXT DEFAULT 'once'")
        if "last_reset_date" not in task_cols:
            await db.execute("ALTER TABLE tasks ADD COLUMN last_reset_date TEXT DEFAULT ''")
        async with db.execute("PRAGMA table_info(shop_items)") as cur:
            shop_cols = {row[1] for row in await cur.fetchall()}
        if "income_per_day" not in shop_cols:
            await db.execute("ALTER TABLE shop_items ADD COLUMN income_per_day INTEGER DEFAULT 0")
            if "income_per_hour" in shop_cols:
                await db.execute(
                    "UPDATE shop_items SET income_per_day=COALESCE(income_per_hour, 0) * 24 "
                    "WHERE COALESCE(income_per_day, 0)=0"
                )
        if "emoji_icon" not in shop_cols:
            await db.execute("ALTER TABLE shop_items ADD COLUMN emoji_icon TEXT DEFAULT ''")
        async with db.execute("PRAGMA table_info(token_shop_purchases)") as cur:
            token_shop_purchase_cols = {row[1] for row in await cur.fetchall()}
        if "anarchy_number" not in token_shop_purchase_cols:
            await db.execute("ALTER TABLE token_shop_purchases ADD COLUMN anarchy_number TEXT DEFAULT ''")
        if "minecraft_nick" not in token_shop_purchase_cols:
            await db.execute("ALTER TABLE token_shop_purchases ADD COLUMN minecraft_nick TEXT DEFAULT ''")
        if "status" not in token_shop_purchase_cols:
            await db.execute("ALTER TABLE token_shop_purchases ADD COLUMN status TEXT DEFAULT 'inventory'")
        if "processed_at" not in token_shop_purchase_cols:
            await db.execute("ALTER TABLE token_shop_purchases ADD COLUMN processed_at INTEGER")
        if "processed_by" not in token_shop_purchase_cols:
            await db.execute("ALTER TABLE token_shop_purchases ADD COLUMN processed_by INTEGER")
        await db.execute(
            "UPDATE token_shop_purchases SET status='inventory' "
            "WHERE COALESCE(status, '')=''"
        )
        # seed settings
        for k, v in DEFAULTS.items():
            await db.execute(
                "INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (k, v)
            )
        async with db.execute(
            "SELECT value FROM settings WHERE key='funtime_test_ips_seeded'"
        ) as cur:
            funtime_seeded = await cur.fetchone()
        if not funtime_seeded:
            for label, ip in FUNTIME_TEST_DEFAULTS:
                await db.execute(
                    "INSERT INTO funtime_test_ips(label, ip) "
                    "SELECT ?, ? WHERE NOT EXISTS ("
                    "SELECT 1 FROM funtime_test_ips WHERE ip=?"
                    ")",
                    (label, ip, ip),
                )
            await db.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)",
                ("funtime_test_ips_seeded", "1"),
            )
        async with db.execute("SELECT COUNT(*) FROM token_shop_categories") as cur:
            token_shop_categories_row = await cur.fetchone()
        if not token_shop_categories_row or token_shop_categories_row[0] == 0:
            now = int(time.time())
            for name, menu_text, sort_order in TOKEN_SHOP_DEFAULT_CATEGORIES:
                await db.execute(
                    "INSERT INTO token_shop_categories"
                    "(name, menu_text, emoji_icon, active, sort_order, created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (name, menu_text, "", 1, sort_order, now),
                )
        # seed owner as admin
        await db.execute(
            "INSERT OR IGNORE INTO admins(user_id, added_by, added_at) VALUES(?,?,?)",
            (OWNER_ID, OWNER_ID, int(time.time())),
        )
        await db.commit()


async def get_setting(key: str, default: Optional[str] = None) -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT value FROM settings WHERE key=?", (key,)) as cur:
            row = await cur.fetchone()
    if row:
        return row[0]
    return default if default is not None else DEFAULTS.get(key, "")


async def set_setting(key: str, value: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        await db.commit()


async def execute(sql: str, params: tuple = ()):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(sql, params)
        await db.commit()
        return cur.lastrowid


async def fetchone(sql: str, params: tuple = ()) -> Optional[tuple]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, params) as cur:
            return await cur.fetchone()


async def fetchall(sql: str, params: tuple = ()) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, params) as cur:
            return list(await cur.fetchall())


def normalize_reset_time(raw_value: str | None) -> str:
    fallback = DEFAULTS.get("daily_task_reset_time", "00:00")
    raw = (raw_value or fallback).strip()
    try:
        hours_raw, minutes_raw = raw.split(":", 1)
        hours = int(hours_raw)
        minutes = int(minutes_raw)
        if not (0 <= hours <= 23 and 0 <= minutes <= 59):
            raise ValueError
    except (ValueError, AttributeError):
        return fallback
    return f"{hours:02d}:{minutes:02d}"


def get_task_reset_marker(reset_time: str | None = None) -> str:
    now = now_msk()
    hours, minutes = map(int, normalize_reset_time(reset_time).split(":"))
    boundary = now.replace(hour=hours, minute=minutes, second=0, microsecond=0)
    if now < boundary:
        boundary -= timedelta(days=1)
    return boundary.date().isoformat()


async def reset_due_daily_tasks(task_id: int | None = None) -> list[int]:
    params: list[Any] = []
    where_sql = ""
    if task_id is not None:
        where_sql = " AND id=?"
        params.append(task_id)

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT value FROM settings WHERE key='daily_task_reset_time'"
        ) as cur:
            reset_time_row = await cur.fetchone()
        current_marker = get_task_reset_marker(reset_time_row[0] if reset_time_row else None)

        async with db.execute(
            "SELECT id FROM tasks "
            "WHERE reset_period='daily' "
            "AND COALESCE(last_reset_date, '')<>? "
            f"{where_sql}",
            (current_marker, *params),
        ) as cur:
            rows = await cur.fetchall()

        reset_ids = [row[0] for row in rows]
        if not reset_ids:
            return []

        for current_task_id in reset_ids:
            await db.execute("DELETE FROM task_submissions WHERE task_id=?", (current_task_id,))
            await db.execute("DELETE FROM task_completions WHERE task_id=?", (current_task_id,))
            await db.execute(
                "UPDATE tasks SET completions=0, active=1, last_reset_date=? WHERE id=?",
                (current_marker, current_task_id),
            )
        await db.commit()
        return reset_ids


async def get_or_create_user(user_id: int, username: str | None, full_name: str, referrer_id: int | None = None):
    row = await fetchone("SELECT user_id FROM users WHERE user_id=?", (user_id,))
    if row:
        await execute(
            "UPDATE users SET username=?, full_name=? WHERE user_id=?",
            (username or "", full_name or "", user_id),
        )
        return False
    clean_referrer_id = None
    if referrer_id and referrer_id != user_id:
        ref_row = await fetchone(
            "SELECT user_id FROM users WHERE user_id=? AND banned=0",
            (referrer_id,),
        )
        if ref_row:
            clean_referrer_id = referrer_id
    await execute(
        "INSERT INTO users(user_id, username, full_name, referrer_id, created_at) VALUES(?,?,?,?,?)",
        (user_id, username or "", full_name or "", clean_referrer_id, int(time.time())),
    )
    return True


async def reward_pending_referral(user_id: int) -> tuple[int, int] | None:
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT referrer_id, referral_rewarded, banned FROM users WHERE user_id=?",
            (user_id,),
        ) as cur:
            user_row = await cur.fetchone()
        if not user_row:
            return None

        referrer_id, already_rewarded, user_banned = user_row
        if user_banned or not referrer_id or referrer_id == user_id or already_rewarded:
            return None

        async with db.execute(
            "SELECT value FROM settings WHERE key='maintenance_enabled'"
        ) as cur:
            maintenance_row = await cur.fetchone()
        if maintenance_row and maintenance_row[0] == "1":
            return None

        async with db.execute(
            "SELECT user_id FROM users WHERE user_id=? AND banned=0",
            (referrer_id,),
        ) as cur:
            ref_row = await cur.fetchone()
        if not ref_row:
            return None

        async with db.execute("SELECT value FROM settings WHERE key='ref_reward'") as cur:
            reward_row = await cur.fetchone()
        try:
            reward = int(reward_row[0] if reward_row else DEFAULTS.get("ref_reward", "20"))
        except (TypeError, ValueError):
            reward = 20

        cur = await db.execute(
            "UPDATE users SET referral_rewarded=1 "
            "WHERE user_id=? AND referral_rewarded=0",
            (user_id,),
        )
        if cur.rowcount != 1:
            await db.rollback()
            return None

        await db.execute(
            "UPDATE users SET referrals=referrals+1, balance=balance+? WHERE user_id=?",
            (reward, referrer_id),
        )
        await db.execute(
            "INSERT INTO player_logs(user_id, action, created_at) VALUES(?,?,?)",
            (user_id, f"Зарегистрировался по рефералу {referrer_id} после ОП", now),
        )
        await db.commit()
        return referrer_id, reward


async def is_admin(user_id: int) -> bool:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    if uid == OWNER_ID:
        return True
    row = await fetchone("SELECT 1 FROM admins WHERE CAST(user_id AS INTEGER)=?", (uid,))
    return row is not None


async def log_admin(admin_id: int, action: str):
    await execute(
        "INSERT INTO admin_logs(admin_id, action, created_at) VALUES(?,?,?)",
        (admin_id, action, int(time.time())),
    )


async def log_player(user_id: int, action: str):
    await execute(
        "INSERT INTO player_logs(user_id, action, created_at) VALUES(?,?,?)",
        (user_id, action, int(time.time())),
    )


async def approve_task_submission(submission_id: int, admin_id: int) -> dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT s.id, s.user_id, s.task_id, s.status, s.submission_text, s.created_at, "
            "t.name, t.reward, t.max_completions, t.completions, t.active, "
            "u.full_name, u.username "
            "FROM task_submissions s "
            "JOIN tasks t ON t.id=s.task_id "
            "LEFT JOIN users u ON u.user_id=s.user_id "
            "WHERE s.id=?",
            (submission_id,),
        ) as cur:
            row = await cur.fetchone()

        if not row:
            return {"status": "not_found"}

        (
            _sid, user_id, task_id, status, submission_text, created_at,
            task_name, reward, max_completions, completions, active,
            full_name, username,
        ) = row

        result = {
            "status": status,
            "submission_id": submission_id,
            "user_id": user_id,
            "task_id": task_id,
            "task_name": task_name,
            "reward": reward,
            "submission_text": submission_text,
            "created_at": created_at,
            "full_name": full_name,
            "username": username,
        }

        if status != "pending":
            return result
        if not active:
            result["status"] = "task_inactive"
            return result
        if max_completions and completions >= max_completions:
            result["status"] = "limit_reached"
            return result

        async with db.execute(
            "SELECT 1 FROM task_completions WHERE user_id=? AND task_id=?",
            (user_id, task_id),
        ) as cur:
            done = await cur.fetchone()

        now = int(time.time())
        if done:
            await db.execute(
                "UPDATE task_submissions SET status='approved', processed_at=?, processed_by=? "
                "WHERE id=?",
                (now, admin_id, submission_id),
            )
            await db.commit()
            result["status"] = "already_completed"
            return result

        try:
            await db.execute(
                "INSERT INTO task_completions(user_id, task_id, completed_at) VALUES(?,?,?)",
                (user_id, task_id, now),
            )
        except aiosqlite.IntegrityError:
            await db.execute(
                "UPDATE task_submissions SET status='approved', processed_at=?, processed_by=? "
                "WHERE id=?",
                (now, admin_id, submission_id),
            )
            await db.commit()
            result["status"] = "already_completed"
            return result

        await db.execute("UPDATE tasks SET completions=completions+1 WHERE id=?", (task_id,))
        await db.execute("UPDATE users SET balance=balance+? WHERE user_id=?", (reward, user_id))
        await db.execute(
            "UPDATE task_submissions SET status='approved', processed_at=?, processed_by=? "
            "WHERE id=?",
            (now, admin_id, submission_id),
        )
        await db.commit()
        result["status"] = "approved"
        return result


async def reject_task_submission(submission_id: int, admin_id: int) -> dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT s.id, s.user_id, s.task_id, s.status, s.submission_text, s.created_at, "
            "t.name, t.reward, u.full_name, u.username "
            "FROM task_submissions s "
            "JOIN tasks t ON t.id=s.task_id "
            "LEFT JOIN users u ON u.user_id=s.user_id "
            "WHERE s.id=?",
            (submission_id,),
        ) as cur:
            row = await cur.fetchone()

        if not row:
            return {"status": "not_found"}

        (
            _sid, user_id, task_id, status, submission_text, created_at,
            task_name, reward, full_name, username,
        ) = row

        result = {
            "status": status,
            "submission_id": submission_id,
            "user_id": user_id,
            "task_id": task_id,
            "task_name": task_name,
            "reward": reward,
            "submission_text": submission_text,
            "created_at": created_at,
            "full_name": full_name,
            "username": username,
        }
        if status != "pending":
            return result

        await db.execute(
            "UPDATE task_submissions SET status='rejected', processed_at=?, processed_by=? "
            "WHERE id=?",
            (int(time.time()), admin_id, submission_id),
        )
        await db.commit()
        result["status"] = "rejected"
        return result


async def purge_user_data(user_id: int) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "user_id": user_id,
        "had_user_row": False,
        "users_deleted": 0,
        "admins_deleted": 0,
        "admin_logs_deleted": 0,
        "player_logs_deleted": 0,
        "task_completions_deleted": 0,
        "task_submissions_deleted": 0,
        "user_farms_deleted": 0,
        "token_shop_inventory_deleted": 0,
        "token_shop_requests_deleted": 0,
        "token_shop_purchases_deleted": 0,
        "promo_uses_deleted": 0,
        "withdrawals_deleted": 0,
        "channel_join_logs_deleted": 0,
        "settings_deleted": 0,
        "referrals_detached": 0,
        "parent_referral_decremented": False,
    }
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT referrer_id, referral_rewarded FROM users WHERE user_id=?",
            (user_id,),
        ) as cur:
            user_row = await cur.fetchone()
        if user_row:
            summary["had_user_row"] = True
            referrer_id, referral_rewarded = user_row
            if referrer_id and referral_rewarded:
                await db.execute(
                    "UPDATE users SET referrals=MAX(0, referrals-1) WHERE user_id=?",
                    (referrer_id,),
                )
                summary["parent_referral_decremented"] = True
            async with db.execute(
                "SELECT COUNT(*) FROM users WHERE referrer_id=?",
                (user_id,),
            ) as cur:
                row = await cur.fetchone()
                summary["referrals_detached"] = row[0] if row else 0
            await db.execute(
                "UPDATE users SET referrer_id=NULL, referral_rewarded=0 WHERE referrer_id=?",
                (user_id,),
            )

        async with db.execute(
            "SELECT task_id, COUNT(*) FROM task_completions WHERE user_id=? GROUP BY task_id",
            (user_id,),
        ) as cur:
            task_rows = await cur.fetchall()
        for task_id, count in task_rows:
            await db.execute(
                "UPDATE tasks SET completions=MAX(0, completions-?) WHERE id=?",
                (count, task_id),
            )
        cur = await db.execute("DELETE FROM task_completions WHERE user_id=?", (user_id,))
        summary["task_completions_deleted"] = cur.rowcount
        cur = await db.execute("DELETE FROM task_submissions WHERE user_id=?", (user_id,))
        summary["task_submissions_deleted"] = cur.rowcount

        cur = await db.execute("DELETE FROM user_farms WHERE user_id=?", (user_id,))
        summary["user_farms_deleted"] = cur.rowcount
        cur = await db.execute("DELETE FROM token_shop_requests WHERE user_id=?", (user_id,))
        summary["token_shop_requests_deleted"] = cur.rowcount
        cur = await db.execute("DELETE FROM token_shop_inventory WHERE user_id=?", (user_id,))
        summary["token_shop_inventory_deleted"] = cur.rowcount
        cur = await db.execute("DELETE FROM token_shop_purchases WHERE user_id=?", (user_id,))
        summary["token_shop_purchases_deleted"] = cur.rowcount
        cur = await db.execute("DELETE FROM promo_uses WHERE user_id=?", (user_id,))
        summary["promo_uses_deleted"] = cur.rowcount
        cur = await db.execute("DELETE FROM withdrawals WHERE user_id=?", (user_id,))
        summary["withdrawals_deleted"] = cur.rowcount
        cur = await db.execute("DELETE FROM channel_join_log WHERE user_id=?", (user_id,))
        summary["channel_join_logs_deleted"] = cur.rowcount
        cur = await db.execute("DELETE FROM player_logs WHERE user_id=?", (user_id,))
        summary["player_logs_deleted"] = cur.rowcount
        cur = await db.execute("DELETE FROM admin_logs WHERE admin_id=?", (user_id,))
        summary["admin_logs_deleted"] = cur.rowcount
        cur = await db.execute("DELETE FROM admins WHERE user_id=?", (user_id,))
        summary["admins_deleted"] = cur.rowcount
        cur = await db.execute("DELETE FROM settings WHERE key=?", (f"ref_clicks:{user_id}",))
        summary["settings_deleted"] = cur.rowcount
        cur = await db.execute("DELETE FROM users WHERE user_id=?", (user_id,))
        summary["users_deleted"] = cur.rowcount
        await db.commit()
    return summary


async def increment_user_daily_action(user_id: int) -> None:
    await execute(
        "UPDATE users SET daily_actions = daily_actions + 1 WHERE user_id=?",
        (user_id,),
    )


async def reset_daily_user_actions() -> None:
    await execute("UPDATE users SET daily_actions = 0")
