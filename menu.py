import html
import time
import random
import asyncio
import re
from datetime import datetime, timezone, timedelta
from msk_time import now_msk
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext

from db import fetchone, fetchall, execute, get_setting, log_player
from config import OWNER_ID
from keyboards import (
    profile_kb, earn_kb, casino_kb, funtime_kb, theft_kb,
    leaderboard_kb, back_to_menu_kb, back_to_lb_kb,
    back_to_earn_kb, farms_kb, cancel_kb, mk_btn,
)
from states import WithdrawSG, PromoSG, ManualTaskSG, TokenShopPurchaseSG
from utils import (
    send_section,
    check_user_subscriptions,
    query_minecraft_status,
    is_user_subscribed_to_chat,
    format_shop_item_block,
    format_token_shop_item_block,
    render_stored_icon_html,
    render_config_icon_html,
    extract_custom_emoji_id,
    apply_stored_icon_to_button_text,
    build_subscription_gate_text,
    build_subscription_gate_kb,
)

router = Router()


async def int_setting(key: str, default: int = 0) -> int:
    try:
        return int(await get_setting(key, str(default)))
    except (TypeError, ValueError):
        return default


def format_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    parts = []
    if days:
        parts.append(f"{days}д")
    if hours:
        parts.append(f"{hours}ч")
    if minutes:
        parts.append(f"{minutes}м")
    if seconds or not parts:
        parts.append(f"{seconds}с")
    return " ".join(parts)


def button_icon_from_setting(base_text: str, icon_value: str | None, fallback: str) -> tuple[str, str | None]:
    icon_id = extract_custom_emoji_id(icon_value)
    if icon_id:
        return base_text, icon_id

    value = (icon_value or "").strip()
    if value.startswith("tx:"):
        value = value[3:].strip()
    if not value or "<tg-emoji" in value:
        value = fallback
    return f"{value} {base_text}".strip(), None


async def complete_task_reward(user_id: int, task_id: int, reward: int):
    await execute(
        "INSERT INTO task_completions(user_id, task_id, completed_at) VALUES(?,?,?)",
        (user_id, task_id, int(time.time())),
    )
    await execute("UPDATE tasks SET completions=completions+1 WHERE id=?", (task_id,))
    await execute("UPDATE users SET balance=balance+? WHERE user_id=?", (reward, user_id))


async def render_subscription_task(call: CallbackQuery, task_id: int, bot: Bot):
    row = await fetchone(
        "SELECT t.name, t.reward, t.max_completions, t.completions, t.active, "
        "c.id, c.title, c.link, c.chat_id "
        "FROM tasks t LEFT JOIN channels c ON c.id=t.channel_id "
        "WHERE t.id=?",
        (task_id,),
    )
    if not row:
        await call.answer("Задание не найдено", show_alert=True)
        return

    name, reward, max_completions, completions, active, channel_id, channel_title, channel_link, chat_id = row
    if not active:
        await call.answer("Задание недоступно", show_alert=True)
        return
    if not channel_id or not chat_id:
        await call.answer("Канал для задания не настроен", show_alert=True)
        return
    if max_completions and completions >= max_completions:
        await call.answer("Лимит выполнений достигнут", show_alert=True)
        return

    done = await fetchone(
        "SELECT 1 FROM task_completions WHERE user_id=? AND task_id=?",
        (call.from_user.id, task_id),
    )
    status = "Выполнено" if done else "Не выполнено"
    text = (
        f"📡 <b>{name}</b>\n\n"
        f"<blockquote>"
        f"Награда: <b>+{reward}</b>\n"
        f"Канал: <b>{channel_title or channel_link or chat_id}</b>\n"
        f"Статус: <b>{status}</b>\n"
        f"Выполнений: <b>{completions}/{max_completions or '∞'}</b>"
        f"</blockquote>\n\n"
        "Подпишитесь на канал и нажмите кнопку проверки."
    )
    rows = []
    if channel_link:
        rows.append([InlineKeyboardButton(text="Открыть канал", url=channel_link)])
    if not done:
        rows.append([InlineKeyboardButton(text="Проверить подписку", callback_data=f"task_check:{task_id}")])
    rows.append([InlineKeyboardButton(text="Назад к заданиям", callback_data="nav:tasks")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="nav:menu")])
    await send_section(call, text, "tasks_photo", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


async def render_manual_task(call: CallbackQuery, task_id: int):
    row = await fetchone(
        "SELECT name, reward, max_completions, completions, active, task_type "
        "FROM tasks WHERE id=?",
        (task_id,),
    )
    if not row:
        await call.answer("Задание не найдено", show_alert=True)
        return

    name, reward, max_completions, completions, active, task_type = row
    if task_type == "subscribe":
        await call.answer("Это задание-подписка", show_alert=True)
        return
    if not active:
        await call.answer("Задание недоступно", show_alert=True)
        return

    done = await fetchone(
        "SELECT 1 FROM task_completions WHERE user_id=? AND task_id=?",
        (call.from_user.id, task_id),
    )
    pending = await fetchone(
        "SELECT id FROM task_submissions WHERE user_id=? AND task_id=? AND status='pending' "
        "ORDER BY id DESC LIMIT 1",
        (call.from_user.id, task_id),
    )

    if done:
        status = "Выполнено"
    elif pending:
        status = "На проверке"
    elif max_completions and completions >= max_completions:
        status = "Лимит выполнений достигнут"
    else:
        status = "Не выполнено"

    text = (
        f"📝 <b>{name}</b>\n\n"
        f"<blockquote>"
        f"Награда: <b>+{reward}</b>\n"
        f"Статус: <b>{status}</b>\n"
        f"Выполнений: <b>{completions}/{max_completions or '∞'}</b>"
        f"</blockquote>\n\n"
        "Отправьте ответ одним сообщением. Награда начисляется только после подтверждения администратором."
    )
    rows = []
    if not done and not pending and not (max_completions and completions >= max_completions):
        rows.append([InlineKeyboardButton(text="Отправить ответ на проверку", callback_data=f"task_submit:{task_id}")])
    rows.append([InlineKeyboardButton(text="Назад к заданиям", callback_data="nav:tasks")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="nav:menu")])
    await send_section(call, text, "tasks_photo", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


async def notify_admins_about_task_submission(
    bot: Bot,
    submission_id: int,
    user_id: int,
    user_label: str,
    task_name: str,
    submission_text: str,
):
    admin_rows = await fetchall("SELECT user_id FROM admins")
    admin_ids = {OWNER_ID, *[row[0] for row in admin_rows]}
    text = (
        "🆕 <b>Новая заявка по заданию</b>\n\n"
        f"Задание: <b>{html.escape(task_name)}</b>\n"
        f"Пользователь: <b>{html.escape(user_label)}</b>\n"
        f"ID: <code>{user_id}</code>\n\n"
        f"<blockquote>{html.escape(submission_text)}</blockquote>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Открыть заявку", callback_data=f"task_submission:{submission_id}")],
        [
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"task_approve:{submission_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"task_reject:{submission_id}"),
        ],
    ])
    for admin_id in admin_ids:
        try:
            await bot.send_message(admin_id, text, reply_markup=kb)
        except Exception:
            pass


async def notify_admins_about_token_shop_request(
    bot: Bot,
    request_id: int,
    user_id: int,
    user_label: str,
    item_name: str,
    category_name: str,
    price: int,
    anarchy_number: str,
    minecraft_nick: str,
):
    admin_rows = await fetchall("SELECT user_id FROM admins")
    admin_ids = {OWNER_ID, *[row[0] for row in admin_rows]}
    currency_name = await get_setting("currency_name", "токенов")
    text = (
        "🛍 <b>Новая заявка ресурсов</b>\n\n"
        f"Заявка: <code>#{request_id}</code>\n"
        f"Товар: <b>{html.escape(item_name)}</b>\n"
        f"Категория: <b>{html.escape(category_name)}</b>\n"
        f"Цена: <b>{price} {html.escape(currency_name)}</b>\n"
        f"Пользователь: <b>{html.escape(user_label)}</b>\n"
        f"ID: <code>{user_id}</code>\n"
        f"Анархия: <b>{html.escape(anarchy_number)}</b>\n"
        f"Ник в Minecraft: <code>{html.escape(minecraft_nick)}</code>\n\n"
        "⚠️ Пользователь должен находиться в игре во время выдачи."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Открыть заявку #{request_id}", callback_data=f"tsadm:req_open:{request_id}")],
        [
            InlineKeyboardButton(text="✅ Выдано", callback_data=f"tsadm:req_issue:{request_id}"),
            InlineKeyboardButton(text="↩️ Возврат", callback_data=f"tsadm:req_return:{request_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"tsadm:req_reject:{request_id}"),
        ],
    ])
    for admin_id in admin_ids:
        try:
            await bot.send_message(admin_id, text, reply_markup=kb)
        except Exception:
            pass


# ---------- Profile ----------

async def show_profile(call: CallbackQuery):
    u = await fetchone(
        "SELECT username, full_name, balance, referrals FROM users WHERE user_id=?",
        (call.from_user.id,),
    )
    if not u:
        await call.message.answer("Сначала /start"); return
    me = await call.bot.me()
    ref_link = f"https://t.me/{me.username}?start={call.from_user.id}"
    cur = await get_setting("currency_name")
    cem = await get_setting("currency_emoji")
    emoji = await get_setting("section_emoji_profile", "👤")
    ref_reward = await get_setting("ref_reward", "20")
    full_name = html.escape(u[1] or "Без имени")
    username = html.escape(u[0] or "—")
    safe_ref_link = html.escape(ref_link)
    safe_cur = html.escape(cur or "токенов")
    safe_cem = cem or ""
    safe_ref_reward = html.escape(ref_reward or "0")
    text = (
        f"{emoji} <b>Профиль</b>\n"
        f"\n"
        f"<blockquote>"
        f"<tg-emoji emoji-id=\"5316727448644103237\">👤</tg-emoji>Имя: {full_name}\n"
        f"<tg-emoji emoji-id=\"5879585266426973039\">🌐</tg-emoji>Username: @{username}\n"
        f"<tg-emoji emoji-id=\"5258096772776991776\">⚙</tg-emoji>ID: <code>{call.from_user.id}</code>\n"
        f"<tg-emoji emoji-id=\"5258368777350816286\">🪙</tg-emoji>Баланс: {u[2]} {safe_cem} {safe_cur}\n"
        f"<tg-emoji emoji-id=\"5258513401784573443\">👥</tg-emoji>Рефералов: {u[3]}"
        f"</blockquote>\n"
        f"\n"
        f"<tg-emoji emoji-id=\"5258073068852485953\">✈️</tg-emoji><b>Реферальная ссылка:</b>\n"
        f"<blockquote>{safe_ref_link}</blockquote>\n\n"
        f"<blockquote>📋 <b>За каждого приглашённого человека, вы будете получать</b> "
        f"{safe_ref_reward} {safe_cem} {safe_cur}</blockquote>"
    )
    await send_section(call, text, "profile_photo", reply_markup=await profile_kb())


@router.callback_query(F.data == "nav:profile")
async def cb_profile(call: CallbackQuery):
    await call.answer()
    await show_profile(call)


@router.callback_query(F.data == "nav:info")
async def cb_info(call: CallbackQuery):
    await call.answer()
    info_text = await get_setting("info_text", "")
    if not info_text:
        info_text = "ℹ️ <b>Информация</b>\n\nТекст ещё не задан. Администратор может настроить его в разделе «Тексты»."
    else:
        info_text = "ℹ️ <b>Информация</b>\n\n" + info_text
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="nav:profile")]
    ])
    await send_section(call, info_text, None, reply_markup=kb)


@router.callback_query(F.data == "nav:bonus")
async def cb_bonus(call: CallbackQuery):
    u = await fetchone("SELECT last_bonus FROM users WHERE user_id=?", (call.from_user.id,))
    now = int(time.time())
    if u and u[0] and now - u[0] < 86400:
        rem = 86400 - (now - u[0])
        h, m = rem // 3600, (rem % 3600) // 60
        await call.answer(f"⏳ Следующий бонус через {h}ч {m}м", show_alert=True)
        return
    bmin = int(await get_setting("bonus_min", "10"))
    bmax = int(await get_setting("bonus_max", "100"))
    amt = random.randint(bmin, bmax) * max(1, int(await get_setting("event_x_mult", "1")))
    await execute(
        "UPDATE users SET balance=balance+?, last_bonus=? WHERE user_id=?",
        (amt, now, call.from_user.id),
    )
    cur = await get_setting("currency_name"); cem = await get_setting("currency_emoji")
    await call.answer(f"🎁 Получено {amt} {cem} {cur}", show_alert=True)
    await show_profile(call)


# ---------- Withdraw ----------

@router.callback_query(F.data == "nav:withdraw")
async def cb_withdraw(call: CallbackQuery, state: FSMContext):
    minw = int(await get_setting("min_withdraw", "100"))
    u = await fetchone("SELECT balance, minecraft_nick FROM users WHERE user_id=?", (call.from_user.id,))
    if not u or u[0] < minw:
        await call.answer(f"❗ Минимальный вывод: {minw}", show_alert=True); return
    await state.set_state(WithdrawSG.nick)
    await call.message.answer(
        f"Введите ваш ник в Minecraft{(' (текущий: ' + u[1] + ')') if u[1] else ''}:",
        reply_markup=await cancel_kb("menu"),
    )
    await call.answer()


@router.message(WithdrawSG.nick)
async def withdraw_nick(message: Message, state: FSMContext):
    nick = (message.text or "").strip()
    if len(nick) < 3 or len(nick) > 20:
        await message.answer("Некорректный ник (3-20 символов), попробуйте снова")
        return
    await state.update_data(nick=nick)
    await state.set_state(WithdrawSG.amount)
    minw = await get_setting("min_withdraw", "100")
    await message.answer(f"Введите сумму к выводу (мин. {minw}):", reply_markup=await cancel_kb("menu"))


@router.message(WithdrawSG.amount)
async def withdraw_amount(message: Message, state: FSMContext):
    try:
        amount = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число"); return
    minw = int(await get_setting("min_withdraw", "100"))
    if amount < minw:
        await message.answer(f"Минимум {minw}"); return
    u = await fetchone("SELECT balance FROM users WHERE user_id=?", (message.from_user.id,))
    if amount > u[0]:
        await message.answer("Недостаточно средств"); return
    data = await state.get_data()
    nick = data["nick"]
    await execute("UPDATE users SET balance=balance-?, minecraft_nick=? WHERE user_id=?",
                  (amount, nick, message.from_user.id))
    await execute(
        "INSERT INTO withdrawals(user_id, amount, minecraft_nick, created_at) VALUES(?,?,?,?)",
        (message.from_user.id, amount, nick, int(time.time())),
    )
    await log_player(message.from_user.id, f"Создал заявку на вывод {amount} (ник: {nick})")
    await state.clear()
    await message.answer("✅ Заявка на вывод создана. Ожидайте обработки.", reply_markup=await back_to_menu_kb())


# ---------- Tasks ----------

async def show_tasks(call: CallbackQuery, bot: Bot):
    title_emoji = await get_setting("section_emoji_tasks", "📋")
    if (await get_setting("tasks_op_enabled", "0")) == "1":
        not_subbed = await check_user_subscriptions(bot, call.from_user.id, "tasks")
        if not_subbed:
            await send_section(
                call,
                build_subscription_gate_text(not_subbed, "задания"),
                "op_photo",
                reply_markup=build_subscription_gate_kb(not_subbed, "tasks", back_to_menu=True),
            )
            return

    items = await fetchall(
        "SELECT t.id, t.name, t.reward, t.max_completions, t.completions, t.task_type, "
        "t.channel_id, c.title, c.link, c.invite_link "
        "FROM tasks t LEFT JOIN channels c ON c.id=t.channel_id "
        "WHERE t.active=1 ORDER BY t.id DESC"
    )
    available_rows = []
    visible_count = 0
    text = f"{title_emoji} <b>Доступные задания:</b>\n\n"
    done_icon = render_config_icon_html(await get_setting("tasks_done_icon", "✅"), "✅")
    not_done_icon = render_config_icon_html(await get_setting("tasks_not_done_icon", "❌"), "❌")
    received_icon = await get_setting("tasks_received_icon", "✅")
    open_icon = await get_setting("tasks_open_icon", "➕")
    check_icon = await get_setting("tasks_check_icon", "✔️")
    for (
        tid, name, reward, max_completions, completions, task_type,
        channel_id, channel_title, channel_link, invite_link,
    ) in items:
        done = await fetchone(
            "SELECT 1 FROM task_completions WHERE user_id=? AND task_id=?",
            (call.from_user.id, tid),
        )
        pending = None
        if task_type != "subscribe":
            pending = await fetchone(
                "SELECT 1 FROM task_submissions WHERE user_id=? AND task_id=? AND status='pending'",
                (call.from_user.id, tid),
            )
        if max_completions and completions >= max_completions and not done and not pending:
            continue

        safe_name = html.escape(str(name or channel_title or "Задание"))
        visible_count += 1
        if task_type == "subscribe":
            channel_display = html.escape(str(channel_title or name or "Канал"))
            reward_text = html.escape(str(reward))
            if done:
                text += (
                    f"{done_icon} <b>{channel_display}</b> — "
                    f"<code>Выполнено</code> <code>(+{reward_text})</code>\n"
                )
                button_text, button_icon_id = button_icon_from_setting(
                    f"{channel_title or name or 'Канал'} (Получено)",
                    received_icon,
                    "✅",
                )
                available_rows.append([InlineKeyboardButton(
                    text=button_text,
                    callback_data=f"task_done:{tid}",
                    icon_custom_emoji_id=button_icon_id,
                )])
                continue

            text += f"{not_done_icon} <b>{channel_display}</b> — <code>{reward_text} токенов</code>\n"
            row = []
            open_url = invite_link or channel_link
            if open_url:
                open_text, open_icon_id = button_icon_from_setting(
                    str(channel_title or name or "Канал"),
                    open_icon,
                    "➕",
                )
                row.append(InlineKeyboardButton(
                    text=open_text,
                    url=open_url,
                    icon_custom_emoji_id=open_icon_id,
                ))
            check_text, check_icon_id = button_icon_from_setting("Проверить", check_icon, "✔️")
            row.append(InlineKeyboardButton(
                text=check_text,
                callback_data=f"task_check:{tid}",
                icon_custom_emoji_id=check_icon_id,
            ))
            available_rows.append(row)
            continue

        button_text = f"{name} (+{reward} токенов)"
        reward_text = html.escape(str(reward))
        if done:
            text += (
                f"{done_icon} <b>{safe_name}</b> — "
                f"<code>Выполнено</code> <code>(+{reward_text})</code>\n"
            )
        elif pending:
            text += f"⏳ <b>{safe_name}</b> — <code>На проверке</code> <code>(+{reward_text})</code>\n"
        else:
            text += f"{not_done_icon} <b>{safe_name}</b> — <code>{reward_text} токенов</code>\n"
        if not done or pending:
            available_rows.append([InlineKeyboardButton(text=button_text, callback_data=f"task:{tid}")])

    if visible_count == 0:
        text = "Сейчас активных заданий нет."

    available_rows.append([InlineKeyboardButton(text="Назад", callback_data="nav:menu")])
    await send_section(call, text, "tasks_photo", reply_markup=InlineKeyboardMarkup(inline_keyboard=available_rows))


@router.callback_query(F.data == "nav:tasks")
async def cb_tasks(call: CallbackQuery, bot: Bot):
    await call.answer()
    await show_tasks(call, bot)


@router.callback_query(F.data.startswith("task_open:"))
async def cb_task_open(call: CallbackQuery, bot: Bot):
    await call.answer()
    await render_subscription_task(call, int(call.data.split(":")[1]), bot)


@router.callback_query(F.data.startswith("task_check:"))
async def cb_task_check(call: CallbackQuery, bot: Bot):
    tid = int(call.data.split(":")[1])
    task = await fetchone(
        "SELECT t.reward, t.max_completions, t.completions, t.active, t.task_type, c.chat_id "
        "FROM tasks t LEFT JOIN channels c ON c.id=t.channel_id "
        "WHERE t.id=?",
        (tid,),
    )
    if not task or not task[3]:
        await call.answer("Задание недоступно", show_alert=True); return
    if task[4] != "subscribe":
        await call.answer("Это не задание-подписка", show_alert=True); return

    done = await fetchone(
        "SELECT 1 FROM task_completions WHERE user_id=? AND task_id=?",
        (call.from_user.id, tid),
    )
    if done:
        await call.answer("Уже выполнено", show_alert=True)
        await show_tasks(call, bot)
        return
    if task[1] and task[2] >= task[1]:
        await call.answer("Лимит выполнений достигнут", show_alert=True); return

    chat_id = task[5]
    if not chat_id:
        await call.answer("Канал для проверки не настроен", show_alert=True); return

    try:
        subscribed = await is_user_subscribed_to_chat(bot, call.from_user.id, chat_id)
    except Exception:
        subscribed = False
    if not subscribed:
        await call.answer("Подписка не найдена. Сначала подпишитесь на канал.", show_alert=True)
        return

    await complete_task_reward(call.from_user.id, tid, task[0])
    await call.answer(f"✅ +{task[0]}", show_alert=True)
    await show_tasks(call, bot)


@router.callback_query(F.data.startswith("task_done:"))
async def cb_task_done(call: CallbackQuery):
    await call.answer("Уже получено", show_alert=True)


@router.callback_query(F.data.startswith("task:"))
async def cb_task(call: CallbackQuery, bot: Bot):
    tid = int(call.data.split(":")[1])
    task = await fetchone(
        "SELECT reward, max_completions, completions, active, task_type FROM tasks WHERE id=?",
        (tid,),
    )
    if not task or not task[3]:
        await call.answer("Недоступно", show_alert=True); return
    if task[4] == "subscribe":
        await render_subscription_task(call, tid, bot)
        return
    await call.answer()
    await render_manual_task(call, tid)


@router.callback_query(F.data.startswith("task_submit:"))
async def cb_task_submit(call: CallbackQuery, state: FSMContext):
    tid = int(call.data.split(":")[1])
    task = await fetchone(
        "SELECT name, max_completions, completions, active, task_type FROM tasks WHERE id=?",
        (tid,),
    )
    if not task or not task[3]:
        await call.answer("Задание недоступно", show_alert=True); return
    if task[4] == "subscribe":
        await call.answer("Это задание-подписка", show_alert=True); return
    done = await fetchone(
        "SELECT 1 FROM task_completions WHERE user_id=? AND task_id=?",
        (call.from_user.id, tid),
    )
    if done:
        await call.answer("Уже выполнено", show_alert=True); return
    pending = await fetchone(
        "SELECT 1 FROM task_submissions WHERE user_id=? AND task_id=? AND status='pending'",
        (call.from_user.id, tid),
    )
    if pending:
        await call.answer("Заявка уже отправлена на проверку", show_alert=True); return
    if task[1] and task[2] >= task[1]:
        await call.answer("Лимит выполнений достигнут", show_alert=True); return

    await state.update_data(task_id=tid)
    await state.set_state(ManualTaskSG.waiting_text)
    await call.message.answer(
        f"Отправьте одним сообщением ответ для задания «{task[0]}».\n"
        "После этого заявка уйдёт администраторам на проверку.",
        reply_markup=await cancel_kb("menu"),
    )
    await call.answer()


@router.message(ManualTaskSG.waiting_text)
async def manual_task_submit_text(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    tid = data.get("task_id")
    submission_text = (message.text or message.caption or "").strip()
    if not tid:
        await state.clear()
        await message.answer("Сессия задания сброшена. Откройте задание заново.")
        return
    if not submission_text:
        await message.answer("Отправьте текст одним сообщением.")
        return

    task = await fetchone(
        "SELECT name, reward, max_completions, completions, active, task_type FROM tasks WHERE id=?",
        (tid,),
    )
    if not task or not task[4]:
        await state.clear()
        await message.answer("Задание уже недоступно.", reply_markup=await back_to_menu_kb())
        return
    if task[5] == "subscribe":
        await state.clear()
        await message.answer("Это задание не требует ручной проверки.", reply_markup=await back_to_menu_kb())
        return
    if task[2] and task[3] >= task[2]:
        await state.clear()
        await message.answer("Лимит выполнений по этому заданию уже достигнут.", reply_markup=await back_to_menu_kb())
        return

    done = await fetchone(
        "SELECT 1 FROM task_completions WHERE user_id=? AND task_id=?",
        (message.from_user.id, tid),
    )
    if done:
        await state.clear()
        await message.answer("Это задание уже отмечено как выполненное.", reply_markup=await back_to_menu_kb())
        return

    pending = await fetchone(
        "SELECT 1 FROM task_submissions WHERE user_id=? AND task_id=? AND status='pending'",
        (message.from_user.id, tid),
    )
    if pending:
        await state.clear()
        await message.answer("Заявка уже находится на проверке.", reply_markup=await back_to_menu_kb())
        return

    submission_id = await execute(
        "INSERT INTO task_submissions(user_id, task_id, submission_text, status, created_at) "
        "VALUES(?,?,?,?,?)",
        (message.from_user.id, tid, submission_text, "pending", int(time.time())),
    )
    await log_player(message.from_user.id, f"Отправил задание #{tid} на проверку")

    user_label = message.from_user.full_name or str(message.from_user.id)
    if message.from_user.username:
        user_label += f" (@{message.from_user.username})"
    await notify_admins_about_task_submission(
        bot,
        submission_id,
        message.from_user.id,
        user_label,
        task[0],
        submission_text,
    )

    await state.clear()
    await message.answer(
        "✅ Ответ отправлен администраторам. Награда будет начислена после подтверждения.",
        reply_markup=await back_to_menu_kb(),
    )


# ---------- Earn ----------

@router.callback_query(F.data == "nav:earn")
async def cb_earn(call: CallbackQuery):
    await call.answer()
    text = await get_setting("earn_text")
    await send_section(call, text, "earn_photo", reply_markup=await earn_kb())


@router.callback_query(F.data == "nav:shop")
async def cb_shop(call: CallbackQuery):
    items = await fetchall(
        "SELECT id, name, description, price, income_per_day, emoji_icon "
        "FROM shop_items WHERE active=1 ORDER BY id DESC"
    )
    discount = int(await get_setting("event_shop_discount", "0"))
    currency_name = await get_setting("currency_name")
    if not items:
        await send_section(call, "Магазин пуст", None, reply_markup=await back_to_earn_kb())
        await call.answer(); return
    text = "🛒 <b>Магазин фармилок:</b>\n\n"
    rows = []
    for iid, name, desc, price, income_per_day, emoji_icon in items:
        eff = price - price * discount // 100
        text += (
            format_shop_item_block(
                name=name,
                price=price,
                income_per_day=income_per_day,
                active=True,
                currency_name=currency_name,
                emoji_icon=emoji_icon,
                discount_pct=discount,
                description=desc,
            )
            + "\n\n"
        )
        button_text, button_icon_id = apply_stored_icon_to_button_text(
            f"Купить «{name}» — {eff}",
            emoji_icon,
        )
        rows.append([InlineKeyboardButton(
            text=button_text,
            callback_data=f"buy:{iid}",
            icon_custom_emoji_id=button_icon_id,
        )])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="nav:earn")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="nav:menu")])
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await call.answer()


@router.callback_query(F.data.startswith("buy:"))
async def cb_buy(call: CallbackQuery):
    iid = int(call.data.split(":")[1])
    item = await fetchone("SELECT name, price, active FROM shop_items WHERE id=?", (iid,))
    if not item or not item[2]:
        await call.answer("Недоступно", show_alert=True); return
    discount = int(await get_setting("event_shop_discount", "0"))
    eff = item[1] - item[1] * discount // 100
    u = await fetchone("SELECT balance FROM users WHERE user_id=?", (call.from_user.id,))
    if u[0] < eff:
        await call.answer("Недостаточно средств", show_alert=True); return
    now = int(time.time())
    await execute("UPDATE users SET balance=balance-? WHERE user_id=?", (eff, call.from_user.id))
    await execute("INSERT INTO user_farms(user_id, item_id, bought_at, last_collected) VALUES(?,?,?,?)",
                  (call.from_user.id, iid, now, now))
    await call.answer(f"✅ Куплено: {item[0]}", show_alert=True)


@router.callback_query(F.data == "nav:farms")
async def cb_farms(call: CallbackQuery):
    rows = await fetchall(
        "SELECT uf.id, si.name, si.income_per_day, uf.last_collected, si.emoji_icon "
        "FROM user_farms uf JOIN shop_items si ON si.id=uf.item_id WHERE uf.user_id=?",
        (call.from_user.id,),
    )
    if not rows:
        await send_section(call, "У вас нет фармилок", None, reply_markup=await back_to_earn_kb())
        await call.answer(); return
    now = int(time.time())
    total = 0
    text = "🌾 <b>Ваши фармилки:</b>\n\n"
    mult = max(1, int(await get_setting("event_x_mult", "1")))
    for fid, name, inc, lc, emoji_icon in rows:
        earned = int(((now - lc) * inc) / 86400) * mult
        total += earned
        icon = render_stored_icon_html(emoji_icon) or "•"
        text += f"{icon} {html.escape(name or 'Фармилка')}: +{earned} (доход {inc}/день)\n"
        await execute("UPDATE user_farms SET last_collected=? WHERE id=?", (now, fid))
    if total:
        await execute("UPDATE users SET balance=balance+? WHERE user_id=?", (total, call.from_user.id))
    text += f"\n💰 Зачислено: <b>{total}</b>"
    await send_section(call, text, None, reply_markup=await farms_kb())
    await call.answer()


# ---------- Token Shop ----------

def token_shop_request_window_open() -> bool:
    now = now_msk()
    current_minutes = now.hour * 60 + now.minute
    return 19 * 60 <= current_minutes < 21 * 60


async def show_token_shop_root(call: CallbackQuery):
    categories = await fetchall(
        "SELECT id, name, emoji_icon FROM token_shop_categories "
        "WHERE active=1 ORDER BY sort_order ASC, id ASC"
    )
    text = (await get_setting("token_shop_text", "")).strip() or (
        "🛍 <b>Магазин ресурсов</b>\n\nВыберите категорию ниже."
    )
    if not categories:
        await send_section(
            call,
            "Магазин ресурсов пока пуст.",
            "token_shop_photo",
            reply_markup=await back_to_menu_kb(),
        )
        return

    rows: list[list[InlineKeyboardButton]] = []
    current_row: list[InlineKeyboardButton] = []
    for category_id, name, emoji_icon in categories:
        button_text, button_icon_id = apply_stored_icon_to_button_text(
            name or f"Категория {category_id}",
            emoji_icon,
        )
        current_row.append(
            InlineKeyboardButton(
                text=button_text,
                callback_data=f"tshop:cat:{category_id}",
                icon_custom_emoji_id=button_icon_id,
            )
        )
        if len(current_row) >= 2:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    cart_count_row = await fetchone(
        "SELECT COUNT(*) FROM token_shop_cart WHERE user_id=?", (call.from_user.id,)
    )
    cart_count = cart_count_row[0] if cart_count_row else 0
    cart_label = f"🛒 Корзина ({cart_count})" if cart_count else "🛒 Корзина"
    rows.append([await mk_btn(cart_label, callback_data="nav:token_shop_cart")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="nav:menu")])
    await send_section(
        call,
        text,
        "token_shop_photo",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )


async def show_token_shop_category(call: CallbackQuery, category_id: int):
    category = await fetchone(
        "SELECT name, menu_text, emoji_icon, active "
        "FROM token_shop_categories WHERE id=?",
        (category_id,),
    )
    if not category or not category[3]:
        await call.answer("Категория недоступна", show_alert=True)
        return

    items = await fetchall(
        "SELECT id, name, description, price, emoji_icon "
        "FROM token_shop_items "
        "WHERE category_id=? AND active=1 "
        "ORDER BY sort_order ASC, id DESC",
        (category_id,),
    )
    currency_name = await get_setting("currency_name", "токенов")
    category_name, menu_text, emoji_icon, _ = category
    text = (menu_text or "").strip()
    if not text:
        title_icon = render_stored_icon_html(emoji_icon) or "🛍"
        text = (
            f"{title_icon} <b>{html.escape(category_name or 'Категория')}</b>\n\n"
            "Выберите товар ниже."
        )
    if items:
        blocks = [
            format_token_shop_item_block(
                name=name,
                price=price,
                currency_name=currency_name,
                emoji_icon=item_emoji_icon,
                description=description,
            )
            for _, name, description, price, item_emoji_icon in items
        ]
        text = f"{text}\n\n" + "\n\n".join(blocks)
    else:
        text = f"{text}\n\nТовары в этой категории пока не добавлены."

    cart_count_row = await fetchone(
        "SELECT COUNT(*) FROM token_shop_cart WHERE user_id=?", (call.from_user.id,)
    )
    cart_count = cart_count_row[0] if cart_count_row else 0
    rows = []
    for item_id, name, _, price, item_emoji_icon in items:
        button_text, button_icon_id = apply_stored_icon_to_button_text(
            f"В корзину «{name}» — {price} токенов",
            item_emoji_icon,
        )
        rows.append([
            InlineKeyboardButton(
                text=button_text,
                callback_data=f"tshop:buy:{item_id}",
                icon_custom_emoji_id=button_icon_id,
            )
        ])
    cart_label = f"🛒 Корзина ({cart_count})" if cart_count else "🛒 Корзина"
    rows.append([await mk_btn(cart_label, callback_data="nav:token_shop_cart")])
    rows.append([InlineKeyboardButton(text="Назад к категориям", callback_data="nav:token_shop")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="nav:menu")])
    await send_section(
        call,
        text,
        "token_shop_photo",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )


async def show_token_shop_inventory(call: CallbackQuery):
    rows = await fetchall(
        "SELECT inv.id, inv.item_name, inv.category_name, inv.price, inv.emoji_icon, inv.status, "
        "(SELECT r.id FROM token_shop_requests r WHERE r.inventory_id=inv.id ORDER BY r.id DESC LIMIT 1) "
        "FROM token_shop_inventory inv "
        "WHERE inv.user_id=? ORDER BY inv.id DESC",
        (call.from_user.id,),
    )
    if not rows:
        await send_section(
            call,
            "📦 <b>Инвентарь пуст.</b>",
            "token_shop_photo",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="К магазину ресурсов", callback_data="nav:token_shop")],
                [InlineKeyboardButton(text="Назад", callback_data="nav:menu")],
            ]),
        )
        return

    status_map = {
        "available": "В инвентаре",
        "pending": "Заявка на рассмотрении",
        "issued": "Выдано",
        "returned": "Возвращено",
        "rejected": "Отклонено",
    }
    text = "📦 <b>Инвентарь</b>\n\n"
    buttons = []
    for inventory_id, item_name, category_name, price, emoji_icon, status, request_id in rows:
        icon = render_stored_icon_html(emoji_icon) or "•"
        text += (
            f"{icon} <b>{html.escape(item_name)}</b>\n"
            f"Категория: <b>{html.escape(category_name)}</b>\n"
            f"Цена: <b>{price} токенов</b>\n"
            f"Статус: <b>{status_map.get(status, status)}</b>\n"
        )
        if request_id and status == "pending":
            text += f"Заявка: <code>#{request_id}</code>\n"
        text += "\n"
        if status == "available":
            buttons.append([
                InlineKeyboardButton(
                    text=f"Вывести #{inventory_id}",
                    callback_data=f"tshop:withdraw:{inventory_id}",
                )
            ])

    buttons.append([InlineKeyboardButton(text="К магазину ресурсов", callback_data="nav:token_shop")])
    buttons.append([InlineKeyboardButton(text="Назад", callback_data="nav:menu")])
    await send_section(
        call,
        text,
        "token_shop_photo",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


async def show_token_shop_cart(call: CallbackQuery):
    rows = await fetchall(
        "SELECT id, item_name, category_name, price, emoji_icon "
        "FROM token_shop_cart WHERE user_id=? ORDER BY added_at ASC",
        (call.from_user.id,),
    )
    if not rows:
        await send_section(
            call,
            "🛒 <b>Корзина пуста</b>\n\nДобавьте товары из магазина ресурсов.",
            "token_shop_photo",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="К магазину", callback_data="nav:token_shop")],
                [InlineKeyboardButton(text="Назад", callback_data="nav:menu")],
            ]),
        )
        return

    total_price = sum(r[3] for r in rows)
    text = "🛒 <b>Корзина</b>\n\n"
    buttons = []
    for cart_id, item_name, category_name, price, emoji_icon in rows:
        icon = render_stored_icon_html(emoji_icon) or "•"
        text += f"{icon} <b>{html.escape(item_name)}</b> — {price} токенов\n"
        buttons.append([
            InlineKeyboardButton(
                text=f"📤 Вывести «{item_name[:22]}»",
                callback_data=f"tshop:cart_withdraw:{cart_id}",
            ),
            InlineKeyboardButton(
                text="✕",
                callback_data=f"tshop:cart_remove:{cart_id}",
            ),
        ])

    text += f"\n💰 Итого: <b>{total_price} токенов</b>"
    buttons.append([InlineKeyboardButton(
        text=f"📤 Вывести всё ({total_price} токенов)",
        callback_data="tshop:cart_withdraw_all",
    )])
    buttons.append([InlineKeyboardButton(text="К магазину", callback_data="nav:token_shop")])
    buttons.append([InlineKeyboardButton(text="Назад", callback_data="nav:menu")])
    await send_section(call, text, "token_shop_photo", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data == "nav:token_shop")
async def cb_token_shop(call: CallbackQuery):
    await call.answer()
    await show_token_shop_root(call)


@router.callback_query(F.data == "nav:token_shop_inventory")
async def cb_token_shop_inventory(call: CallbackQuery):
    await call.answer()
    await show_token_shop_root(call)


@router.callback_query(F.data.startswith("tshop:cat:"))
async def cb_token_shop_category(call: CallbackQuery):
    await call.answer()
    await show_token_shop_category(call, int(call.data.split(":")[2]))


@router.callback_query(F.data.startswith("tshop:buy:"))
async def cb_token_shop_buy(call: CallbackQuery):
    item_id = int(call.data.split(":")[2])
    row = await fetchone(
        "SELECT i.id, i.category_id, i.name, i.price, i.active, "
        "c.name, c.active, i.emoji_icon "
        "FROM token_shop_items i "
        "JOIN token_shop_categories c ON c.id=i.category_id "
        "WHERE i.id=?",
        (item_id,),
    )
    if not row or not row[4] or not row[6]:
        await call.answer("Товар недоступен", show_alert=True)
        return

    _, category_id, item_name, price, _, category_name, _, emoji_icon = row

    existing = await fetchone(
        "SELECT id FROM token_shop_cart WHERE user_id=? AND item_id=?",
        (call.from_user.id, item_id),
    )
    if existing:
        await call.answer(f"«{item_name}» уже в корзине!", show_alert=True)
        return

    await execute(
        "INSERT INTO token_shop_cart(user_id, item_id, category_id, item_name, category_name, price, emoji_icon, added_at) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (call.from_user.id, item_id, category_id, item_name, category_name, price, emoji_icon or "", int(time.time())),
    )
    await log_player(call.from_user.id, f"Добавил в корзину «{item_name}» ({category_name}), {price} токенов")
    await call.answer(f"🛒 «{item_name}» добавлен в корзину!", show_alert=True)


@router.callback_query(F.data == "nav:token_shop_cart")
async def cb_token_shop_cart(call: CallbackQuery):
    await call.answer()
    await show_token_shop_cart(call)


@router.callback_query(F.data.startswith("tshop:cart_remove:"))
async def cb_cart_remove(call: CallbackQuery):
    cart_id = int(call.data.split(":")[2])
    row = await fetchone("SELECT item_name FROM token_shop_cart WHERE id=? AND user_id=?",
                         (cart_id, call.from_user.id))
    if not row:
        await call.answer("Не найдено в корзине", show_alert=True)
        return
    await execute("DELETE FROM token_shop_cart WHERE id=?", (cart_id,))
    await call.answer(f"«{row[0]}» убран из корзины")
    await show_token_shop_cart(call)


async def _buy_cart_item(user_id: int, cart_id: int, bot_balance: int) -> tuple[bool, str]:
    row = await fetchone(
        "SELECT id, item_id, category_id, item_name, category_name, price, emoji_icon "
        "FROM token_shop_cart WHERE id=? AND user_id=?",
        (cart_id, user_id),
    )
    if not row:
        return False, "Товар не найден в корзине"
    _, item_id, category_id, item_name, category_name, price, emoji_icon = row
    if bot_balance < price:
        return False, f"Недостаточно средств для «{item_name}» ({price} токенов)"
    now = int(time.time())
    await execute("UPDATE users SET balance=balance-? WHERE user_id=?", (price, user_id))
    await execute(
        "INSERT INTO token_shop_inventory"
        "(user_id, item_id, category_id, item_name, category_name, price, emoji_icon, status, bought_at, updated_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        (user_id, item_id, category_id, item_name, category_name, price, emoji_icon or "", "available", now, now),
    )
    await execute(
        "INSERT INTO token_shop_purchases"
        "(user_id, item_id, category_id, item_name, category_name, price, status, bought_at) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (user_id, item_id, category_id, item_name, category_name, price, "inventory", now),
    )
    await execute("DELETE FROM token_shop_cart WHERE id=?", (cart_id,))
    return True, item_name


@router.callback_query(F.data.startswith("tshop:cart_withdraw:"))
async def cb_cart_withdraw_one(call: CallbackQuery, state: FSMContext):
    cart_id = int(call.data.split(":")[2])
    row = await fetchone(
        "SELECT id, item_name, category_name, price FROM token_shop_cart WHERE id=? AND user_id=?",
        (cart_id, call.from_user.id),
    )
    if not row:
        await call.answer("Товар не найден в корзине", show_alert=True)
        return
    _, item_name, category_name, price = row
    user_row = await fetchone("SELECT balance FROM users WHERE user_id=?", (call.from_user.id,))
    if not user_row or user_row[0] < price:
        await call.answer(f"Недостаточно средств. Нужно {price} токенов.", show_alert=True)
        return
    if not token_shop_request_window_open():
        await call.answer(
            "⏰ Вывод ресурсов доступен только с 19:00 до 21:00 (МСК).",
            show_alert=True,
        )
        return
    await state.update_data(
        tshop_cart_withdraw_id=cart_id,
        tshop_cart_withdraw_all=False,
        tshop_withdraw_item_name=item_name,
        tshop_withdraw_category_name=category_name,
        tshop_withdraw_price=price,
    )
    await state.set_state(TokenShopPurchaseSG.anarchy)
    await call.answer()
    await call.message.answer(
        f"📤 <b>Вывод предмета</b>\n"
        f"Товар: <b>{html.escape(item_name)}</b>\n"
        f"Категория: <b>{html.escape(category_name)}</b>\n"
        f"Цена: <b>{price} токенов</b>\n"
        "Введите номер анархии:",
        reply_markup=await cancel_kb("menu"),
    )


@router.callback_query(F.data == "tshop:cart_withdraw_all")
async def cb_cart_withdraw_all(call: CallbackQuery, state: FSMContext):
    if not token_shop_request_window_open():
        await call.answer(
            "⏰ Вывод ресурсов доступен только с 19:00 до 21:00 (МСК).",
            show_alert=True,
        )
        return
    cart_rows = await fetchall(
        "SELECT id, price FROM token_shop_cart WHERE user_id=? ORDER BY added_at ASC",
        (call.from_user.id,),
    )
    if not cart_rows:
        await call.answer("Корзина пуста", show_alert=True); return
    total = sum(r[1] for r in cart_rows)
    balance_row = await fetchone("SELECT balance FROM users WHERE user_id=?", (call.from_user.id,))
    balance = balance_row[0] if balance_row else 0
    if balance < total:
        await call.answer(f"Недостаточно средств. Нужно {total}, у вас {balance} токенов.", show_alert=True); return
    cart_ids = [r[0] for r in cart_rows]
    await state.update_data(
        tshop_cart_withdraw_ids=cart_ids,
        tshop_cart_withdraw_id=None,
        tshop_cart_withdraw_all=True,
    )
    await state.set_state(TokenShopPurchaseSG.anarchy)
    await call.answer()
    await call.message.answer(
        f"📤 <b>Вывод всех предметов из корзины</b>\n"
        f"Количество: <b>{len(cart_ids)}</b>\n"
        f"Итого: <b>{total} токенов</b>\n"
        "Введите номер анархии:",
        reply_markup=await cancel_kb("menu"),
    )


# tshop:withdraw: (inventory-based) removed — withdrawal now happens directly from cart


@router.message(TokenShopPurchaseSG.anarchy)
async def token_shop_withdraw_anarchy(message: Message, state: FSMContext):
    anarchy_number = (message.text or "").strip()
    if not anarchy_number.isdigit():
        await message.answer("Введите номер анархии числом.")
        return
    await state.update_data(tshop_withdraw_anarchy=anarchy_number)
    await state.set_state(TokenShopPurchaseSG.nick)
    await message.answer(
        "Теперь отправьте ваш ник в Minecraft.\n"
        "⚠️ Во время выдачи товара нужно находиться в игре.",
        reply_markup=await cancel_kb("menu"),
    )


@router.message(TokenShopPurchaseSG.nick)
async def token_shop_withdraw_nick(message: Message, state: FSMContext, bot: Bot):
    minecraft_nick = (message.text or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_]{3,16}", minecraft_nick):
        await message.answer(
            "Ник Minecraft должен быть длиной 3-16 символов и содержать только буквы, цифры или _."
        )
        return

    data = await state.get_data()
    anarchy_number = data.get("tshop_withdraw_anarchy", "")
    now = int(time.time())
    user_label = message.from_user.full_name or str(message.from_user.id)
    if message.from_user.username:
        user_label += f" (@{message.from_user.username})"

    withdraw_all = data.get("tshop_cart_withdraw_all", False)
    cart_ids = data.get("tshop_cart_withdraw_ids") if withdraw_all else None
    cart_id_single = data.get("tshop_cart_withdraw_id") if not withdraw_all else None

    if withdraw_all and cart_ids:
        created = []
        for cid in cart_ids:
            crow = await fetchone(
                "SELECT id, item_id, category_id, item_name, category_name, price "
                "FROM token_shop_cart WHERE id=? AND user_id=?",
                (cid, message.from_user.id),
            )
            if not crow:
                continue
            _, item_id, category_id, item_name, category_name, price = crow
            cur_bal_row = await fetchone("SELECT balance FROM users WHERE user_id=?", (message.from_user.id,))
            cur_bal = cur_bal_row[0] if cur_bal_row else 0
            if cur_bal < price:
                continue
            await execute("UPDATE users SET balance=balance-? WHERE user_id=?", (price, message.from_user.id))
            req_id = await execute(
                "INSERT INTO token_shop_requests"
                "(inventory_id, user_id, item_id, category_id, item_name, category_name, "
                "price, anarchy_number, minecraft_nick, status, created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (0, message.from_user.id, item_id, category_id, item_name, category_name,
                 price, anarchy_number, minecraft_nick, "pending", now),
            )
            await execute(
                "INSERT INTO token_shop_purchases"
                "(user_id, item_id, category_id, item_name, category_name, price, status, bought_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (message.from_user.id, item_id, category_id, item_name, category_name, price, "pending", now),
            )
            await execute("DELETE FROM token_shop_cart WHERE id=?", (cid,))
            await notify_admins_about_token_shop_request(
                bot, req_id, message.from_user.id, user_label,
                item_name, category_name, price, anarchy_number, minecraft_nick,
            )
            created.append(item_name)
        await log_player(message.from_user.id, f"Вывел из корзины: {', '.join(created)}; анархия {anarchy_number}; ник {minecraft_nick}")
        await state.clear()
        await message.answer(
            f"✅ Оформлено <b>{len(created)}</b> заявок на вывод.\n"
            "Ожидайте, пока администратор выдаст ресурсы.\n"
            "⚠️ Находитесь в игре во время выдачи.",
            reply_markup=await back_to_menu_kb(),
        )
        return

    if cart_id_single:
        crow = await fetchone(
            "SELECT id, item_id, category_id, item_name, category_name, price "
            "FROM token_shop_cart WHERE id=? AND user_id=?",
            (cart_id_single, message.from_user.id),
        )
        if not crow:
            await state.clear()
            await message.answer("Товар не найден в корзине.", reply_markup=await back_to_menu_kb())
            return
        _, item_id, category_id, item_name, category_name, price = crow
        user_row = await fetchone("SELECT balance FROM users WHERE user_id=?", (message.from_user.id,))
        if not user_row or user_row[0] < price:
            await state.clear()
            await message.answer(f"Недостаточно средств. Нужно {price} токенов.", reply_markup=await back_to_menu_kb())
            return
        await execute("UPDATE users SET balance=balance-? WHERE user_id=?", (price, message.from_user.id))
        request_id = await execute(
            "INSERT INTO token_shop_requests"
            "(inventory_id, user_id, item_id, category_id, item_name, category_name, "
            "price, anarchy_number, minecraft_nick, status, created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (0, message.from_user.id, item_id, category_id, item_name, category_name,
             price, anarchy_number, minecraft_nick, "pending", now),
        )
        await execute(
            "INSERT INTO token_shop_purchases"
            "(user_id, item_id, category_id, item_name, category_name, price, status, bought_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (message.from_user.id, item_id, category_id, item_name, category_name, price, "pending", now),
        )
        await execute("DELETE FROM token_shop_cart WHERE id=?", (cart_id_single,))
        await log_player(
            message.from_user.id,
            f"Оформил заявку #{request_id} на вывод «{item_name}» ({category_name}); анархия {anarchy_number}; ник {minecraft_nick}",
        )
        await notify_admins_about_token_shop_request(
            bot, request_id, message.from_user.id, user_label,
            item_name, category_name, price, anarchy_number, minecraft_nick,
        )
        await state.clear()
        await message.answer(
            f"✅ Заявка <code>#{request_id}</code> на вывод оформлена.\n"
            "Ожидайте, пока администратор выдаст ресурс.\n"
            "⚠️ Находитесь в игре во время выдачи.",
            reply_markup=await back_to_menu_kb(),
        )
        return

    await state.clear()
    await message.answer("Сессия сброшена. Попробуйте снова.", reply_markup=await back_to_menu_kb())


# ---------- Casino ----------

@router.callback_query(F.data == "nav:casino")
async def cb_casino(call: CallbackQuery):
    await call.answer()
    emoji = await get_setting("section_emoji_casino", "🎰")
    bet_d = await get_setting("casino_dice_bet"); win_d = await get_setting("casino_dice_win")
    bet_b = await get_setting("casino_basket_bet"); win_b = await get_setting("casino_basket_win")
    text = (
        f"<blockquote>{emoji} <b>Казино</b></blockquote>\n\n"
        f"<blockquote><b>"
        f"🎲 Кубик: ставка {html.escape(str(bet_d))}, выигрыш {html.escape(str(win_d))}\n"
        f"🏀 Баскетбол: ставка {html.escape(str(bet_b))}, выигрыш {html.escape(str(win_b))}"
        f"</b></blockquote>"
    )
    await send_section(call, text, "casino_photo", reply_markup=await casino_kb())


async def play_game(call: CallbackQuery, bet_key, win_key, chance_key, emoji):
    bet = int(await get_setting(bet_key))
    win = int(await get_setting(win_key))
    chance = int(await get_setting(chance_key))
    u = await fetchone("SELECT balance FROM users WHERE user_id=?", (call.from_user.id,))
    if u[0] < bet:
        await call.answer("Недостаточно средств", show_alert=True); return
    await execute("UPDATE users SET balance=balance-? WHERE user_id=?", (bet, call.from_user.id))
    await call.bot.send_dice(call.message.chat.id, emoji=emoji)
    if random.randint(1, 100) <= chance:
        await execute("UPDATE users SET balance=balance+? WHERE user_id=?", (win, call.from_user.id))
        await call.message.answer(f"🎉 Выигрыш: +{win}", reply_markup=await casino_kb())
    else:
        await call.message.answer(f"😞 Проигрыш: -{bet}", reply_markup=await casino_kb())
    await call.answer()


@router.callback_query(F.data == "nav:dice")
async def cb_dice(call: CallbackQuery):
    await play_game(call, "casino_dice_bet", "casino_dice_win", "casino_dice_chance", "🎲")


@router.callback_query(F.data == "nav:basket")
async def cb_basket(call: CallbackQuery):
    await play_game(call, "casino_basket_bet", "casino_basket_win", "casino_basket_chance", "🏀")


# ---------- Funtime ----------

@router.callback_query(F.data == "nav:funtime")
async def cb_funtime(call: CallbackQuery):
    await call.answer("Опрос серверов…")
    emoji = await get_setting("section_emoji_funtime", "🎮")
    main_ips = [ip.strip() for ip in (await get_setting("funtime_main_ips")).split(",") if ip.strip()]
    test_ips = await fetchall("SELECT label, ip FROM funtime_test_ips")
    text = f"{emoji} <b>Мониторинг Funtime</b>\n\n<b>Основные IP:</b>\n"

    main_tasks = [asyncio.create_task(query_minecraft_status(ip)) for ip in main_ips]
    test_tasks = [asyncio.create_task(query_minecraft_status(ip)) for _, ip in test_ips]
    main_results = await asyncio.gather(*main_tasks, return_exceptions=True)
    test_results = await asyncio.gather(*test_tasks, return_exceptions=True)

    for ip, online in zip(main_ips, main_results):
        if isinstance(online, Exception):
            online = None
        st = f"🟢 {online} онлайн" if online is not None else "🔴 оффлайн"
        text += f"<code>{html.escape(ip)}</code> — {st}\n"
    if test_ips:
        text += "\n<b>Тестовые/Neo:</b>\n"
        for (label, ip), online in zip(test_ips, test_results):
            if isinstance(online, Exception):
                online = None
            st = f"🟢 {online}" if online is not None else "🔴 офф"
            text += f"{html.escape(label or 'IP')}: <code>{html.escape(ip or '')}</code> — {st}\n"
    text += "\n💡 IP можно скопировать тапом."
    await send_section(call, text, "funtime_photo", reply_markup=await funtime_kb())


# ---------- Theft ----------

@router.callback_query(F.data == "nav:theft")
async def cb_theft(call: CallbackQuery):
    await show_theft(call)


async def show_theft(call: CallbackQuery, *, answer: bool = True):
    if answer:
        await call.answer()
    emoji = await get_setting("section_emoji_theft", "🥷")
    chance = await int_setting("theft_chance", 30)
    win = await int_setting("theft_win_pct", 10)
    lose = await int_setting("theft_lose_pct", 10)
    cooldown = max(0, await int_setting("theft_cooldown_sec", 3600))
    min_balance = max(0, await int_setting("theft_min_balance", 0))
    currency_name = await get_setting("currency_name", "токенов")
    currency_emoji = await get_setting("currency_emoji", "")
    user_row = await fetchone(
        "SELECT balance, last_theft FROM users WHERE user_id=?",
        (call.from_user.id,),
    )
    balance = user_row[0] if user_row else 0
    last_theft = user_row[1] if user_row and user_row[1] else 0
    now = int(time.time())
    remaining = max(0, last_theft + cooldown - now) if cooldown else 0
    if remaining:
        status = f"⏳ Следующая попытка через {format_duration(remaining)}"
    elif min_balance and balance < min_balance:
        status = (
            f"❗ Нужен баланс от {min_balance} "
            f"{html.escape(currency_emoji or '')} {html.escape(currency_name or 'токенов')}"
        )
    else:
        status = "✅ Кража доступна прямо сейчас!"
    text = (
        f"<blockquote>{emoji} <b>Кража</b></blockquote>\n\n"
        "━━━━━━━━━━━━━━\n\n"
        "<blockquote>"
        "Попробуй ограбить случайного игрока!\n\n"
        f"<b>Шанс успеха:</b> {chance}%\n"
        f"<b>КД:</b> {format_duration(cooldown)}\n"
        f"<b>Мин. баланс:</b> {min_balance} {html.escape(currency_emoji or '')} {html.escape(currency_name or 'токенов')}\n"
        f"<tg-emoji emoji-id=\"5260726538302660868\">✅</tg-emoji><b>Победа:</b> +{win}% от баланса жертвы\n"
        f"<tg-emoji emoji-id=\"5260342697075416641\">❌</tg-emoji><b>Провал:</b> -{lose}% твоего баланса → жертве\n\n"
        f"{status}"
        "</blockquote>"
    )
    await send_section(call, text, "theft_photo", reply_markup=await theft_kb())


@router.callback_query(F.data == "nav:rob")
async def cb_rob(call: CallbackQuery):
    chance = await int_setting("theft_chance", 30)
    win_pct = await int_setting("theft_win_pct", 10)
    lose_pct = await int_setting("theft_lose_pct", 10)
    cooldown = max(0, await int_setting("theft_cooldown_sec", 3600))
    min_balance = max(0, await int_setting("theft_min_balance", 0))
    me = await fetchone("SELECT balance, last_theft FROM users WHERE user_id=?", (call.from_user.id,))
    my_balance = me[0] if me else 0
    last_theft = me[1] if me and me[1] else 0
    now = int(time.time())
    remaining = max(0, last_theft + cooldown - now) if cooldown else 0
    if remaining:
        await call.answer(f"⏳ Следующая попытка через {format_duration(remaining)}", show_alert=True)
        await show_theft(call, answer=False)
        return
    if min_balance and my_balance < min_balance:
        await call.answer(f"❗ Для ограбления нужен баланс от {min_balance}", show_alert=True)
        await show_theft(call, answer=False)
        return
    victim = await fetchone(
        "SELECT user_id, balance, full_name FROM users "
        "WHERE user_id!=? AND balance>0 AND protected=0 AND banned=0 "
        "ORDER BY RANDOM() LIMIT 1",
        (call.from_user.id,),
    )
    if not victim:
        await call.answer("Подходящих жертв не найдено", show_alert=True); return
    await execute("UPDATE users SET last_theft=? WHERE user_id=?", (now, call.from_user.id))
    if random.randint(1, 100) <= chance:
        gain = min(victim[1], max(1, victim[1] * win_pct // 100))
        await execute("UPDATE users SET balance=balance-? WHERE user_id=?", (gain, victim[0]))
        await execute("UPDATE users SET balance=balance+? WHERE user_id=?", (gain, call.from_user.id))
        await log_player(call.from_user.id, f"Ограбил {victim[0]} на {gain}")
        await call.answer(f"✅ Ограбление удалось! +{gain}", show_alert=True)
    else:
        loss = min(my_balance, max(0, my_balance * lose_pct // 100))
        if loss:
            await execute("UPDATE users SET balance=balance-? WHERE user_id=?", (loss, call.from_user.id))
            await execute("UPDATE users SET balance=balance+? WHERE user_id=?", (loss, victim[0]))
        await log_player(call.from_user.id, f"Провалил кражу у {victim[0]}, потерял {loss}")
        if loss:
            await call.answer(f"❌ Поймали! -{loss} ушло жертве", show_alert=True)
        else:
            await call.answer("❌ Поймали! Баланс не пострадал", show_alert=True)
    await show_theft(call, answer=False)


# ---------- Leaderboard ----------

async def _leaderboard_place_icon(place: int) -> str:
    defaults = {1: "🥇", 2: "🥈", 3: "🥉"}
    value = (await get_setting(
        f"leaderboard_place_{place}_icon",
        defaults.get(place, f"{place}."),
    )).strip()
    if not value:
        return defaults.get(place, f"{place}.")
    if value.startswith("id:"):
        fallback = defaults.get(place, f"{place}.")
        return f'<tg-emoji emoji-id="{html.escape(value[3:].strip(), quote=True)}">{fallback}</tg-emoji>'
    if value.startswith("tx:"):
        return html.escape(value[3:].strip())
    return value


@router.callback_query(F.data == "nav:lb")
async def cb_lb(call: CallbackQuery):
    await call.answer()
    emoji = await get_setting("section_emoji_leaderboard", "🏆")
    await send_section(call, f"<blockquote>{emoji} <b>Топы:</b></blockquote>", "leaderboard_photo", reply_markup=await leaderboard_kb())


@router.callback_query(F.data == "nav:lb_refs")
async def cb_lb_refs(call: CallbackQuery):
    await call.answer()
    emoji = await get_setting("section_emoji_leaderboard", "🏆")
    rows = await fetchall("SELECT full_name, referrals FROM users ORDER BY referrals DESC LIMIT 3")
    text = f"<blockquote>{emoji} <b>Топ по рефералам:</b></blockquote>\n\n"
    if not rows:
        text += "<blockquote>Пока пусто</blockquote>"
    for i, (n, r) in enumerate(rows, 1):
        icon = await _leaderboard_place_icon(i)
        text += f"<blockquote>{icon} <b>{html.escape(n or 'Без имени')}</b> — {r}</blockquote>\n"
    await send_section(call, text or "Пусто", None, reply_markup=await back_to_lb_kb())


@router.callback_query(F.data == "nav:lb_tokens")
async def cb_lb_tokens(call: CallbackQuery):
    await call.answer()
    rows = await fetchall("SELECT full_name, balance FROM users ORDER BY balance DESC LIMIT 3")
    cur = await get_setting("currency_name")
    emoji = await get_setting("section_emoji_leaderboard", "🏆")
    text = f"<blockquote>{emoji} <b>Топ по {html.escape(cur or 'токенам')}:</b></blockquote>\n\n"
    if not rows:
        text += "<blockquote>Пока пусто</blockquote>"
    for i, (n, b) in enumerate(rows, 1):
        icon = await _leaderboard_place_icon(i)
        text += f"<blockquote>{icon} <b>{html.escape(n or 'Без имени')}</b> — {b}</blockquote>\n"
    await send_section(call, text or "Пусто", None, reply_markup=await back_to_lb_kb())


# ---------- Rules ----------

@router.callback_query(F.data == "nav:rules")
async def cb_rules(call: CallbackQuery):
    await call.answer()
    text = await get_setting("rules_text")
    await send_section(call, text, "rules_photo", reply_markup=await back_to_menu_kb())


# ---------- Promocode ----------

@router.callback_query(F.data == "nav:promo")
async def cb_promo(call: CallbackQuery, state: FSMContext):
    await state.set_state(PromoSG.code)
    await call.message.answer(
        '<tg-emoji emoji-id="5258073068852485953">🎁</tg-emoji> '
        '<b>Введите промокод:</b>',
        reply_markup=await cancel_kb("menu"),
    )
    await call.answer()


@router.message(PromoSG.code)
async def promo_apply(message: Message, state: FSMContext):
    code = (message.text or "").strip()
    p = await fetchone("SELECT amount, activations_left FROM promocodes WHERE code=?", (code,))
    await state.clear()
    if not p:
        await message.answer("❌ Промокод не найден", reply_markup=await back_to_menu_kb()); return
    if p[1] <= 0:
        await message.answer("❌ Активации закончились", reply_markup=await back_to_menu_kb()); return
    used = await fetchone("SELECT 1 FROM promo_uses WHERE code=? AND user_id=?",
                          (code, message.from_user.id))
    if used:
        await message.answer("❌ Уже активирован", reply_markup=await back_to_menu_kb()); return
    await execute("UPDATE promocodes SET activations_left=activations_left-1 WHERE code=?", (code,))
    await execute("INSERT INTO promo_uses(code, user_id, used_at) VALUES(?,?,?)",
                  (code, message.from_user.id, int(time.time())))
    await execute("UPDATE users SET balance=balance+? WHERE user_id=?",
                  (p[0], message.from_user.id))
    await log_player(message.from_user.id, f"Активировал промокод {code} на {p[0]}")
    await message.answer(f"✅ Получено: +{p[0]}", reply_markup=await back_to_menu_kb())
