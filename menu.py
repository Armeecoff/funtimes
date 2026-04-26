import html
import time
import random
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext

from db import fetchone, fetchall, execute, get_setting, log_player
from config import OWNER_ID
from keyboards import (
    profile_kb, earn_kb, casino_kb, funtime_kb, theft_kb,
    leaderboard_kb, back_to_menu_kb, back_to_lb_kb,
    back_to_earn_kb, farms_kb, cancel_kb,
)
from states import WithdrawSG, PromoSG, ManualTaskSG
from utils import (
    send_section,
    check_user_subscriptions,
    query_minecraft_status,
    is_user_subscribed_to_chat,
    format_shop_item_block,
    apply_stored_icon_to_button_text,
    build_subscription_gate_text,
)

router = Router()


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
            rows = [[InlineKeyboardButton(text=title, url=link)] for link, title in not_subbed]
            rows.append([InlineKeyboardButton(text="Проверить доступ", callback_data="op_check:tasks")])
            rows.append([InlineKeyboardButton(text="Назад", callback_data="nav:menu")])
            await send_section(
                call,
                build_subscription_gate_text(not_subbed, "задания"),
                "op_photo",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
            )
            return

    items = await fetchall(
        "SELECT t.id, t.name, t.reward, t.max_completions, t.completions, t.task_type, "
        "t.channel_id, c.title, c.link "
        "FROM tasks t LEFT JOIN channels c ON c.id=t.channel_id "
        "WHERE t.active=1 ORDER BY t.id DESC"
    )
    available_rows = []
    text = "📋 <b>Доступные задания:</b>\n\n"
    visible_count = 0
    text = f"<blockquote>{title_emoji} <b>Доступные задания:</b></blockquote>\n\n"
    for tid, name, reward, max_completions, completions, task_type, channel_id, channel_title, channel_link in items:
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

        if done:
            status = "✅ Выполнено"
        elif pending:
            status = "⏳"
        else:
            status = "▫️"

        button_text = f"{name} (+{reward} токенов)"
        callback_data = f"task_open:{tid}" if task_type == "subscribe" else f"task:{tid}"
        text += f"<blockquote>{status} {html.escape(str(name or ''))} — +{reward} токенов</blockquote>\n"
        visible_count += 1
        if task_type == "subscribe" and not done:
            available_rows.append([InlineKeyboardButton(text=button_text, callback_data=callback_data)])
        elif task_type != "subscribe" and (not done or pending):
            available_rows.append([InlineKeyboardButton(text=button_text, callback_data=callback_data)])

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
    if task[1] and task[2] >= task[1]:
        await call.answer("Лимит выполнений достигнут", show_alert=True); return

    done = await fetchone(
        "SELECT 1 FROM task_completions WHERE user_id=? AND task_id=?",
        (call.from_user.id, tid),
    )
    if done:
        await call.answer("Уже выполнено", show_alert=True); return

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
    await render_subscription_task(call, tid, bot)


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
        "SELECT uf.id, si.name, si.income_per_day, uf.last_collected "
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
    for fid, name, inc, lc in rows:
        earned = int(((now - lc) * inc) / 86400) * mult
        total += earned
        text += f"• {name}: +{earned} (доход {inc}/день)\n"
        await execute("UPDATE user_farms SET last_collected=? WHERE id=?", (now, fid))
    if total:
        await execute("UPDATE users SET balance=balance+? WHERE user_id=?", (total, call.from_user.id))
    text += f"\n💰 Зачислено: <b>{total}</b>"
    await send_section(call, text, None, reply_markup=await farms_kb())
    await call.answer()


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
    main_ips = (await get_setting("funtime_main_ips")).split(",")
    test_ips = await fetchall("SELECT label, ip FROM funtime_test_ips")
    text = f"{emoji} <b>Мониторинг Funtime</b>\n\n<b>Основные IP:</b>\n"
    for ip in main_ips:
        ip = ip.strip()
        if not ip:
            continue
        online = await query_minecraft_status(ip)
        st = f"🟢 {online} онлайн" if online is not None else "🔴 оффлайн"
        text += f"<code>{ip}</code> — {st}\n"
    if test_ips:
        text += "\n<b>Тестовые/Neo:</b>\n"
        for label, ip in test_ips:
            online = await query_minecraft_status(ip)
            st = f"🟢 {online}" if online is not None else "🔴 офф"
            text += f"{label}: <code>{ip}</code> — {st}\n"
    text += "\n💡 IP можно скопировать тапом."
    await send_section(call, text, "funtime_photo", reply_markup=await funtime_kb())


# ---------- Theft ----------

@router.callback_query(F.data == "nav:theft")
async def cb_theft(call: CallbackQuery):
    await call.answer()
    emoji = await get_setting("section_emoji_theft", "🥷")
    chance = await get_setting("theft_chance")
    win = await get_setting("theft_win_pct"); lose = await get_setting("theft_lose_pct")
    text = (
        f"<blockquote>{emoji} <b>Кража</b></blockquote>\n\n"
        "━━━━━━━━━━━━━━\n\n"
        "<blockquote>"
        "Попробуй ограбить случайного игрока!\n\n"
        f"<b>Шанс успеха:</b> {html.escape(str(chance))}%\n"
        f"<tg-emoji emoji-id=\"5260726538302660868\">✅</tg-emoji><b>Победа:</b> +{html.escape(str(win))}% от баланса жертвы\n"
        f"<tg-emoji emoji-id=\"5260342697075416641\">❌</tg-emoji><b>Провал:</b> -{html.escape(str(lose))}% твоего баланса → жертве\n\n"
        f"<tg-emoji emoji-id=\"5260726538302660868\">✅</tg-emoji>Кража доступна прямо сейчас!"
        "</blockquote>"
    )
    await send_section(call, text, "theft_photo", reply_markup=await theft_kb())


@router.callback_query(F.data == "nav:rob")
async def cb_rob(call: CallbackQuery):
    chance = int(await get_setting("theft_chance"))
    win_pct = int(await get_setting("theft_win_pct"))
    lose_pct = int(await get_setting("theft_lose_pct"))
    me = await fetchone("SELECT balance FROM users WHERE user_id=?", (call.from_user.id,))
    my_balance = me[0] if me else 0
    victim = await fetchone(
        "SELECT user_id, balance, full_name FROM users "
        "WHERE user_id!=? AND balance>0 AND protected=0 AND banned=0 "
        "ORDER BY RANDOM() LIMIT 1",
        (call.from_user.id,),
    )
    if not victim:
        await call.answer("Подходящих жертв не найдено", show_alert=True); return
    if random.randint(1, 100) <= chance:
        gain = max(1, victim[1] * win_pct // 100)
        await execute("UPDATE users SET balance=balance-? WHERE user_id=?", (gain, victim[0]))
        await execute("UPDATE users SET balance=balance+? WHERE user_id=?", (gain, call.from_user.id))
        await log_player(call.from_user.id, f"Ограбил {victim[0]} на {gain}")
        await call.answer(f"✅ Ограбление удалось! +{gain}", show_alert=True)
    else:
        loss = max(0, my_balance * lose_pct // 100)
        if loss:
            await execute("UPDATE users SET balance=balance-? WHERE user_id=?", (loss, call.from_user.id))
            await execute("UPDATE users SET balance=balance+? WHERE user_id=?", (loss, victim[0]))
        await log_player(call.from_user.id, f"Провалил кражу у {victim[0]}, потерял {loss}")
        if loss:
            await call.answer(f"❌ Поймали! -{loss} ушло жертве", show_alert=True)
        else:
            await call.answer("❌ Поймали! Баланс не пострадал", show_alert=True)


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
    await call.message.answer("Введите промокод:", reply_markup=await cancel_kb("menu"))
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
