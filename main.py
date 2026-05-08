import asyncio
import contextlib
import logging
import time
from datetime import datetime, timedelta
from msk_time import now_msk
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

from config import BOT_TOKEN
from db import fetchall, fetchone, init_db, reset_due_daily_tasks, reset_daily_user_actions, get_setting, set_setting
import admin as h_admin
import menu as h_menu
import start as h_start
from op_guard import StartOpGuardMiddleware
from utils import refresh_user_start_subscription

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

SUBSCRIPTION_RECHECK_INTERVAL_SEC = 60
SUBSCRIPTION_RECHECK_BATCH_SIZE = 10
SUBSCRIPTION_RECHECK_PERIOD_SEC = 24 * 60 * 60


async def daily_task_reset_worker():
    while True:
        try:
            await reset_due_daily_tasks()
        except Exception:
            logging.exception("Failed to reset daily tasks")
        await asyncio.sleep(300)


async def auto_broadcast_worker(bot: Bot):
    while True:
        try:
            enabled = (await get_setting("auto_broadcast_enabled", "0")) == "1"
            if enabled:
                now = now_msk()
                global_time_str = (await get_setting("auto_broadcast_time", "09:00") or "09:00").strip()
                legacy = (await get_setting("auto_broadcast_text", "") or "").strip()
                for i in (1, 2, 3):
                    t = (await get_setting(f"auto_broadcast_text_{i}", "") or "").strip()
                    if not t and i == 1 and legacy:
                        t = legacy
                    if not t:
                        continue
                    time_str = (await get_setting(f"auto_broadcast_time_{i}", "") or "").strip()
                    if not time_str:
                        time_str = global_time_str
                    try:
                        h, m = map(int, time_str.split(":"))
                    except Exception:
                        h, m = 9, 0
                    boundary = now.replace(hour=h, minute=m, second=0, microsecond=0)
                    if now < boundary:
                        boundary -= timedelta(days=1)
                    current_marker = boundary.strftime("%Y-%m-%d %H:%M")
                    last_sent = await get_setting(f"auto_broadcast_last_sent_{i}", "")
                    if last_sent != current_marker and now >= boundary:
                        rows = await fetchall("SELECT user_id FROM users WHERE banned=0")
                        sent = failed = 0
                        for (uid,) in rows:
                            try:
                                await bot.send_message(uid, t, parse_mode="HTML")
                                sent += 1
                            except Exception:
                                failed += 1
                            await asyncio.sleep(0.04)
                        await set_setting(f"auto_broadcast_last_sent_{i}", current_marker)
                        logging.info("Auto broadcast slot %s sent: %s ok, %s failed", i, sent, failed)
        except Exception:
            logging.exception("Auto broadcast worker error")
        await asyncio.sleep(60)


async def midnight_user_actions_reset_worker():
    while True:
        now = now_msk()
        reset_time_str = (await get_setting("daily_actions_reset_time", "00:00") or "00:00").strip()
        try:
            h, m = map(int, reset_time_str.split(":"))
        except Exception:
            h, m = 0, 0
        next_reset = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if now >= next_reset:
            next_reset += timedelta(days=1)
        sleep_seconds = (next_reset - now).total_seconds()
        await asyncio.sleep(sleep_seconds)
        try:
            await reset_daily_user_actions()
            logging.info("Daily user actions reset at %s", reset_time_str)
        except Exception:
            logging.exception("Failed to reset daily user actions")


async def subscription_recheck_worker(bot: Bot):
    while True:
        try:
            cutoff = int(time.time()) - SUBSCRIPTION_RECHECK_PERIOD_SEC
            rows = await fetchall(
                "SELECT user_id FROM users "
                "WHERE banned=0 AND COALESCE(last_subscription_check, 0)<? "
                "ORDER BY COALESCE(last_subscription_check, 0) ASC, created_at ASC, user_id ASC "
                "LIMIT ?",
                (cutoff, SUBSCRIPTION_RECHECK_BATCH_SIZE),
            )
            checked_count = 0
            demoted_count = 0
            restored_count = 0
            for (user_id,) in rows:
                before_row = await fetchone(
                    "SELECT captcha_passed FROM users WHERE user_id=?",
                    (user_id,),
                )
                before = before_row[0] if before_row else 0
                after = await refresh_user_start_subscription(bot, user_id)
                if after is None:
                    continue
                checked_count += 1
                if before and not after:
                    demoted_count += 1
                elif not before and after:
                    restored_count += 1
                await asyncio.sleep(0.2)
            if checked_count:
                logging.info(
                    "Subscription recheck: checked=%s demoted=%s restored=%s",
                    checked_count,
                    demoted_count,
                    restored_count,
                )
        except Exception:
            logging.exception("Failed to recheck user subscriptions")
        await asyncio.sleep(SUBSCRIPTION_RECHECK_INTERVAL_SEC)


async def main():
    await init_db()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    reset_worker = asyncio.create_task(daily_task_reset_worker())
    midnight_reset_worker = asyncio.create_task(midnight_user_actions_reset_worker())
    auto_broadcast = asyncio.create_task(auto_broadcast_worker(bot))
    subscription_worker = asyncio.create_task(subscription_recheck_worker(bot))
    dp = Dispatcher(storage=MemoryStorage())
    start_op_guard = StartOpGuardMiddleware()
    dp.message.outer_middleware(start_op_guard)
    dp.callback_query.outer_middleware(start_op_guard)
    # admin first so admin's reply-button labels match before user handlers
    dp.include_router(h_admin.router)
    dp.include_router(h_start.router)
    dp.include_router(h_menu.router)
    me = await bot.me()
    logging.info("Bot started: @%s (id=%s)", me.username, me.id)
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        reset_worker.cancel()
        midnight_reset_worker.cancel()
        auto_broadcast.cancel()
        subscription_worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await reset_worker
        with contextlib.suppress(asyncio.CancelledError):
            await midnight_reset_worker
        with contextlib.suppress(asyncio.CancelledError):
            await auto_broadcast
        with contextlib.suppress(asyncio.CancelledError):
            await subscription_worker


if __name__ == "__main__":
    asyncio.run(main())
