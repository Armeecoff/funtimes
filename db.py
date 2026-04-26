import aiosqlite
import time
from typing import Any, Optional
from config import DB_PATH, DEFAULTS, OWNER_ID

FUNTIME_TEST_DEFAULTS = (
    ("test4", "test4.funtime.sh"),
    ("test-neo", "test-neo.funtime.sh"),
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
    captcha_passed INTEGER DEFAULT 0,
    last_bonus INTEGER DEFAULT 0,
    minecraft_nick TEXT,
    banned INTEGER DEFAULT 0,
    protected INTEGER DEFAULT 0,
    created_at INTEGER
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
    channel_id INTEGER
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
"""


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        # --- лёгкие миграции для существующих БД ---
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


async def get_or_create_user(user_id: int, username: str | None, full_name: str, referrer_id: int | None = None):
    row = await fetchone("SELECT user_id FROM users WHERE user_id=?", (user_id,))
    if row:
        await execute(
            "UPDATE users SET username=?, full_name=? WHERE user_id=?",
            (username or "", full_name or "", user_id),
        )
        return False
    await execute(
        "INSERT INTO users(user_id, username, full_name, referrer_id, created_at) VALUES(?,?,?,?,?)",
        (user_id, username or "", full_name or "", referrer_id, int(time.time())),
    )
    if referrer_id and referrer_id != user_id:
        ref_row = await fetchone("SELECT user_id FROM users WHERE user_id=?", (referrer_id,))
        if ref_row:
            try:
                reward = int(await get_setting("ref_reward", "20"))
            except Exception:
                reward = 20
            await execute(
                "UPDATE users SET referrals=referrals+1, balance=balance+? WHERE user_id=?",
                (reward, referrer_id),
            )
            await execute(
                "INSERT INTO player_logs(user_id, action, created_at) VALUES(?,?,?)",
                (user_id, f"Зарегистрировался по рефералу {referrer_id}", int(time.time())),
            )
    return True


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
        "promo_uses_deleted": 0,
        "withdrawals_deleted": 0,
        "channel_join_logs_deleted": 0,
        "settings_deleted": 0,
        "referrals_detached": 0,
        "parent_referral_decremented": False,
    }
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT referrer_id FROM users WHERE user_id=?",
            (user_id,),
        ) as cur:
            user_row = await cur.fetchone()
        if user_row:
            summary["had_user_row"] = True
            referrer_id = user_row[0]
            if referrer_id:
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
            await db.execute("UPDATE users SET referrer_id=NULL WHERE referrer_id=?", (user_id,))

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
