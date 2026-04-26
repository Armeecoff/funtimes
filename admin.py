import time
import asyncio
import html
from aiogram import Router, F, Bot
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.context import FSMContext

from db import (
    is_admin, fetchone, fetchall, execute,
    get_setting, set_setting, log_admin, log_player, purge_user_data,
    approve_task_submission, reject_task_submission,
)
from config import OWNER_ID
from keyboards import admin_kb, admin_back_kb, cancel_kb
from utils import (
    send_section, normalize_channel_target, get_channel_members_count,
    format_shop_item_block,
)
from states import (
    AdminBroadcast, AdminEcon, AdminChannel, AdminPromo, AdminTask,
    AdminShop, AdminFuntime, AdminAddAdmin, AdminPhoto, AdminText,
    AdminUserAction, AdminRulesEdit, AdminBtnStyle,
)

router = Router()


# ----- guards -----

async def _is_admin(uid: int) -> bool:
    try:
        uid_int = int(uid)
    except (TypeError, ValueError):
        return False
    return await is_admin(uid_int)


async def build_channel_stats_snapshot(bot: Bot, channel_id: int, chat_id: str):
    join_requests_row = await fetchone(
        "SELECT COUNT(*) FROM channel_join_log WHERE channel_id=?",
        (channel_id,),
    )
    join_requests = join_requests_row[0] if join_requests_row else 0
    reach = join_requests
    members = await get_channel_members_count(bot, chat_id)
    await execute(
        "INSERT INTO channel_stats(channel_id, join_requests, members, reach) "
        "VALUES(?,?,?,?) "
        "ON CONFLICT(channel_id) DO UPDATE SET "
        "join_requests=excluded.join_requests, members=excluded.members, reach=excluded.reach",
        (channel_id, join_requests, members or 0, reach),
    )
    return join_requests, members, reach


def task_type_label(task_type: str) -> str:
    return "подписка" if task_type == "subscribe" else "с проверкой"


def trim_button_text(text: str, limit: int = 42) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


async def resolve_user_target(raw: str) -> tuple[int | None, str]:
    token = (raw or "").strip()
    if not token:
        return None, ""
    if token.isdigit():
        uid = int(token)
        row = await fetchone("SELECT username FROM users WHERE user_id=?", (uid,))
        if row and row[0]:
            return uid, f"@{row[0]}"
        return uid, str(uid)
    username = token[1:] if token.startswith("@") else token
    if not username:
        return None, ""
    row = await fetchone(
        "SELECT user_id, username FROM users WHERE LOWER(username)=LOWER(?)",
        (username,),
    )
    if not row:
        return None, ""
    return row[0], f"@{row[1] or username}"


def extract_stored_icon(text: str, entities) -> tuple[str, str]:
    if entities:
        for ent in entities:
            if ent.type == "custom_emoji" and getattr(ent, "custom_emoji_id", None):
                cid = ent.custom_emoji_id
                return f"id:{cid}", f"премиум-эмодзи <code>{cid}</code>"
    clean_text = (text or "").strip()
    if clean_text in {"", "-", "—", "нет", "none"}:
        return "", "без эмодзи"
    prefix = clean_text.split()[0][:16]
    return f"tx:{prefix}", f"эмодзи {prefix}"


def parse_shop_active(raw: str) -> int | None:
    value = (raw or "").strip().lower()
    if value in {"1", "+", "on", "yes", "y", "да", "д", "доступно", "доступен"}:
        return 1
    if value in {"0", "-", "off", "no", "n", "нет", "н", "недоступно", "недоступен"}:
        return 0
    return None

def extract_photo_file_id(message: Message) -> str | None:
    if message.photo:
        return message.photo[-1].file_id
    document = message.document
    if document and (document.mime_type or "").startswith("image/"):
        return document.file_id
    return None


def current_month_start_ts() -> int:
    now = time.localtime()
    return int(time.mktime((now.tm_year, now.tm_mon, 1, 0, 0, 0, 0, 0, -1)))


async def render_stats_overview(call: CallbackQuery, bot: Bot):
    await call.answer("Загрузка…")
    try:
        link_clicks = int(await get_setting("bot_link_clicks", "0") or "0")
    except ValueError:
        link_clicks = 0

    total_users_row = await fetchone("SELECT COUNT(*) FROM users")
    total_users = total_users_row[0] if total_users_row else 0
    active_row = await fetchone("SELECT COUNT(*) FROM users WHERE banned=0 AND captcha_passed=1")
    active_users = active_row[0] if active_row else 0
    month_start = current_month_start_ts()
    month_users_row = await fetchone("SELECT COUNT(*) FROM users WHERE created_at>=?", (month_start,))
    month_users = month_users_row[0] if month_users_row else 0
    me = await bot.get_me()

    text = (
        "🤖 <b>Статистика бота:</b>\n"
        f"• Ссылка: <code>https://t.me/{me.username}</code>\n"
        f"• Переходов по ссылке (/start): <b>{link_clicks}</b>\n"
        f"• Охват: <b>{total_users}</b>\n"
        f"• Активных пользователей: <b>{active_users}</b>\n"
        f"• Новых за текущий месяц: <b>{month_users}</b>\n\n"
    )
    channels = await fetchall(
        "SELECT id, category, link, chat_id, title, is_private, active FROM channels ORDER BY id DESC"
    )
    if not channels:
        await send_section(call, text + "Каналов нет", None, reply_markup=await admin_back_kb())
        return

    text += "📊 <b>Статистика каналов:</b>\n\n"
    kb_rows = []
    for cid, category, link, chat_id, title, is_private, active in channels:
        join_requests, members, reach = await build_channel_stats_snapshot(bot, cid, chat_id)
        text += (
            f"• <b>{title or link or chat_id}</b>\n"
            f"  ID: <code>{chat_id}</code> | {'private' if is_private else 'public'} | "
            f"{'active' if active else 'inactive'}\n"
            f"  Категория: {category}\n"
            f"  Подписчиков: {members if members is not None else '?'} | "
            f"Заявок: {join_requests} | Охват: {reach}\n"
            f"  Ссылка: {link or '—'}\n\n"
        )
        kb_rows.append([
            InlineKeyboardButton(
                text=f"Открыть #{cid} {trim_button_text(title or link or chat_id, 30)}",
                callback_data=f"stat_ch:{cid}",
            )
        ])

    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


async def render_channel_stats(call: CallbackQuery, bot: Bot, channel_id: int):
    row = await fetchone(
        "SELECT id, category, link, chat_id, title, is_private, active, invite_link "
        "FROM channels WHERE id=?",
        (channel_id,),
    )
    if not row:
        await call.answer("Канал не найден", show_alert=True)
        return

    _, category, link, chat_id, title, is_private, active, invite_link = row
    join_requests, members, reach = await build_channel_stats_snapshot(bot, channel_id, chat_id)
    linked_tasks = await fetchall(
        "SELECT id, name, reward, completions, active FROM tasks WHERE channel_id=? ORDER BY id DESC",
        (channel_id,),
    )
    recent_requests = await fetchall(
        "SELECT user_id, created_at FROM channel_join_log WHERE channel_id=? ORDER BY id DESC LIMIT 10",
        (channel_id,),
    )

    text = (
        f"📡 <b>{title or link or chat_id}</b>\n\n"
        f"ID записи: <code>{channel_id}</code>\n"
        f"Chat ID: <code>{chat_id}</code>\n"
        f"Категория: <b>{category}</b>\n"
        f"Тип: <b>{'private' if is_private else 'public'}</b>\n"
        f"Статус: <b>{'active' if active else 'inactive'}</b>\n"
        f"Подписчиков: <b>{members if members is not None else '?'}</b>\n"
        f"Заявок на вступление: <b>{join_requests}</b>\n"
        f"Охват по заявкам: <b>{reach}</b>\n"
        f"Ссылка для пользователей: {link or '—'}\n"
        f"Invite link бота: {invite_link or '—'}\n\n"
    )
    if linked_tasks:
        text += "<b>Привязанные задания:</b>\n"
        for task_id, name, reward, completions, task_active in linked_tasks:
            text += f"• #{task_id} {name} | +{reward} | {completions} | {'on' if task_active else 'off'}\n"
        text += "\n"
    else:
        text += "<b>Привязанные задания:</b> нет\n\n"

    if recent_requests:
        text += "<b>Последние заявки:</b>\n"
        for user_id, created_at in recent_requests:
            stamp = time.strftime("%d.%m.%Y %H:%M", time.localtime(created_at))
            text += f"• <code>{user_id}</code> | {stamp}\n"

    kb_rows = []
    if link:
        kb_rows.append([InlineKeyboardButton(text="Открыть канал", url=link)])
    kb_rows.append([InlineKeyboardButton(text="Назад к статистике", callback_data="adm:stats")])
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


async def process_channel_reference_input(message: Message, state: FSMContext, bot: Bot):
    chat_id_raw = (message.text or "").strip()
    if not chat_id_raw:
        await message.answer("Пустой ввод. Пришлите chat_id, @username или ссылку на канал.")
        return

    try:
        chat_target = normalize_channel_target(chat_id_raw)
    except ValueError as e:
        reason = str(e)
        if reason == "invite_link_not_supported":
            await message.answer(
                "Ссылки-приглашения вида `https://t.me/+...` Telegram не даёт превратить в chat_id через Bot API.\n"
                "Пришлите public-ссылку канала, `@username` или `chat_id`."
            )
        elif reason == "internal_link_not_supported":
            await message.answer(
                "Ссылки вида `https://t.me/c/...` не подходят. Пришлите public-ссылку канала, `@username` или `chat_id`."
            )
        else:
            await message.answer("Не удалось распознать ссылку. Пришлите chat_id, @username или public t.me ссылку.")
        return

    try:
        chat = await bot.get_chat(chat_target)
    except Exception as e:
        await message.answer(
            f"Не получилось открыть канал: <code>{e}</code>\n"
            "Убедитесь, что ссылка или username правильные и что бот добавлен в канал."
        )
        return

    chat_id = str(chat.id)
    title = chat.title or chat_id
    is_private = 0 if chat.username else 1
    public_link = f"https://t.me/{chat.username}" if chat.username else ""

    try:
        me = await bot.me()
        member = await bot.get_chat_member(chat.id, me.id)
    except Exception as e:
        await message.answer(f"Не удалось проверить права бота: <code>{e}</code>")
        return

    status = getattr(member, "status", None)
    if status not in ("administrator", "creator"):
        await message.answer(
            "Бот не является администратором этого канала. Добавьте его админом и повторите."
        )
        return

    can_invite = getattr(member, "can_invite_users", None)
    if status == "administrator" and not can_invite:
        await message.answer(
            "У бота нет права на создание пригласительных ссылок. Включите `can_invite_users` и повторите."
        )
        return

    invite_link_url = ""
    invite_link_name = f"OP-{message.from_user.id}-{int(time.time())}"
    try:
        inv = await bot.create_chat_invite_link(
            chat.id,
            name=invite_link_name[:32],
            creates_join_request=True,
        )
        invite_link_url = inv.invite_link
    except Exception as e:
        await message.answer(f"Не удалось создать ссылку приглашения: <code>{e}</code>")
        return

    await state.update_data(
        chat_id=chat_id,
        title=title,
        is_private=is_private,
        public_link=public_link,
        invite_link=invite_link_url,
        invite_link_name=invite_link_name,
    )
    await state.set_state(AdminChannel.max_subs)
    await message.answer(
        f"✅ Канал найден: <b>{title}</b>\n"
        f"Ссылка для входа: {invite_link_url}\n\n"
        "Введите максимум подписчиков (0 = без ограничения):",
        reply_markup=await cancel_kb("admin"),
    )


async def render_channels_menu(call: CallbackQuery):
    rows = await fetchall("SELECT id, category, link, title, active FROM channels ORDER BY category, id DESC")
    s_op = await get_setting("start_op_enabled")
    t_op = await get_setting("tasks_op_enabled")
    cap = await get_setting("captcha_enabled")
    text = (
        "📡 <b>Каналы</b>\n\n"
        "Категории: <b>start</b>, <b>tasks</b>, <b>reward</b>\n\n"
        + "".join(
            f"• [{category}] {title or link} ({'on' if active else 'off'}) — id <code>{cid}</code>\n"
            for cid, category, link, title, active in rows
        )
        + f"\nОП старт: {'on' if s_op == '1' else 'off'} | "
          f"ОП задания: {'on' if t_op == '1' else 'off'} | Капча: {'on' if cap == '1' else 'off'}"
    )
    kb_rows = [
        [InlineKeyboardButton(text="В Старт", callback_data="ch_add:start"),
         InlineKeyboardButton(text="В ОП заданий", callback_data="ch_add:tasks")],
        [InlineKeyboardButton(text="В Каналы наград", callback_data="ch_add:reward")],
        [InlineKeyboardButton(text=f"ОП Старт: {'ON' if s_op == '1' else 'off'}",
                              callback_data="ch_toggle:start_op_enabled"),
         InlineKeyboardButton(text=f"ОП Задания: {'ON' if t_op == '1' else 'off'}",
                              callback_data="ch_toggle:tasks_op_enabled")],
        [InlineKeyboardButton(text=f"Капча: {'ON' if cap == '1' else 'off'}",
                              callback_data="ch_toggle:captcha_enabled")],
    ]
    for cid, category, link, title, active in rows:
        kb_rows.append([
            InlineKeyboardButton(
                text=f"Удалить #{cid} {trim_button_text(title or link or category, 32)}",
                callback_data=f"ch_del:{cid}",
            )
        ])
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


async def render_admin_tasks(call: CallbackQuery):
    rows = await fetchall(
        "SELECT t.id, t.name, t.reward, t.max_completions, t.completions, t.active, "
        "t.task_type, t.channel_id, c.title, COALESCE(s.pending_count, 0) "
        "FROM tasks t LEFT JOIN channels c ON c.id=t.channel_id "
        "LEFT JOIN ("
        "    SELECT task_id, COUNT(*) AS pending_count "
        "    FROM task_submissions "
        "    WHERE status='pending' "
        "    GROUP BY task_id"
        ") s ON s.task_id=t.id "
        "ORDER BY t.id DESC"
    )
    pending_total_row = await fetchone(
        "SELECT COUNT(*) FROM task_submissions WHERE status='pending'"
    )
    pending_total = pending_total_row[0] if pending_total_row else 0
    text = "📋 <b>Задания:</b>\n\n"
    kb_rows = []
    for tid, name, reward, max_completions, completions, active, task_type, channel_id, channel_title, pending_count in rows:
        details = f"{task_type_label(task_type)}"
        if channel_id:
            details += f" | channel #{channel_id} {channel_title or ''}".rstrip()
        text += (
            f"• #{tid} {name} | +{reward} | {completions}/{max_completions or '∞'} | "
            f"{'on' if active else 'off'} | {details}"
        )
        if task_type != "subscribe":
            text += f" | ждут проверки: {pending_count}"
        text += "\n"
        kb_rows.append([InlineKeyboardButton(text=f"Удалить {trim_button_text(name, 35)}", callback_data=f"task_del:{tid}")])

    if pending_total:
        kb_rows.append([InlineKeyboardButton(text=f"Заявки на проверку ({pending_total})", callback_data="adm:task_submissions")])
    kb_rows.append([InlineKeyboardButton(text="Добавить задание", callback_data="task_add")])
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(
        call,
        text if rows else "Нет заданий",
        None,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows),
    )


async def send_task_type_picker(target):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Обычное задание (проверка)", callback_data="task_kind:manual")],
        [InlineKeyboardButton(text="Подписка на канал", callback_data="task_kind:subscribe")],
        [InlineKeyboardButton(text="Отмена", callback_data="cancel:admin")],
    ])
    await target.answer("Выберите тип задания:", reply_markup=kb)


async def send_task_channel_picker(target):
    channels = await fetchall(
        "SELECT id, title, link, category, active FROM channels "
        "WHERE category='reward' ORDER BY id DESC"
    )
    if not channels:
        await target.answer("Сначала добавьте хотя бы один канал в разделе каналов.", reply_markup=await admin_back_kb())
        return False

    rows = []
    for cid, title, link, category, active in channels:
        rows.append([
            InlineKeyboardButton(
                text=f"#{cid} {trim_button_text(title or link or 'Канал', 30)} [{category}] {'on' if active else 'off'}",
                callback_data=f"task_channel:{cid}",
            )
        ])
    rows.append([InlineKeyboardButton(text="Отмена", callback_data="cancel:admin")])
    await target.answer("Выберите канал для задания-подписки:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    return True


async def render_task_submissions_queue(call: CallbackQuery):
    rows = await fetchall(
        "SELECT s.id, s.user_id, s.created_at, t.name, t.reward, "
        "u.full_name, u.username "
        "FROM task_submissions s "
        "JOIN tasks t ON t.id=s.task_id "
        "LEFT JOIN users u ON u.user_id=s.user_id "
        "WHERE s.status='pending' "
        "ORDER BY s.id DESC"
    )
    text = "🧾 <b>Заявки на проверку:</b>\n\n"
    kb_rows = []
    for sid, user_id, created_at, task_name, reward, full_name, username in rows:
        stamp = time.strftime("%d.%m.%Y %H:%M", time.localtime(created_at))
        user_label = full_name or f"user {user_id}"
        if username:
            user_label += f" (@{username})"
        text += (
            f"• #{sid} {html.escape(task_name)}\n"
            f"  Пользователь: {html.escape(user_label)}\n"
            f"  Награда: +{reward} | {stamp}\n\n"
        )
        kb_rows.append([
            InlineKeyboardButton(
                text=f"Открыть заявку #{sid}",
                callback_data=f"task_submission:{sid}",
            )
        ])

    if not rows:
        text += "Сейчас новых заявок нет."
    kb_rows.append([InlineKeyboardButton(text="Назад к заданиям", callback_data="adm:tasks")])
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


async def render_task_submission_card(call: CallbackQuery, submission_id: int):
    row = await fetchone(
        "SELECT s.id, s.user_id, s.task_id, s.submission_text, s.status, s.created_at, "
        "t.name, t.reward, u.full_name, u.username "
        "FROM task_submissions s "
        "JOIN tasks t ON t.id=s.task_id "
        "LEFT JOIN users u ON u.user_id=s.user_id "
        "WHERE s.id=?",
        (submission_id,),
    )
    if not row:
        await call.answer("Заявка не найдена", show_alert=True)
        return

    sid, user_id, task_id, submission_text, status, created_at, task_name, reward, full_name, username = row
    stamp = time.strftime("%d.%m.%Y %H:%M", time.localtime(created_at))
    user_label = full_name or f"user {user_id}"
    if username:
        user_label += f" (@{username})"
    status_map = {
        "pending": "На проверке",
        "approved": "Подтверждено",
        "rejected": "Отклонено",
    }
    text = (
        f"🧾 <b>Заявка #{sid}</b>\n\n"
        f"Задание: <b>{html.escape(task_name)}</b>\n"
        f"Task ID: <code>{task_id}</code>\n"
        f"Пользователь: <b>{html.escape(user_label)}</b>\n"
        f"User ID: <code>{user_id}</code>\n"
        f"Награда: <b>+{reward}</b>\n"
        f"Статус: <b>{status_map.get(status, status)}</b>\n"
        f"Отправлено: <b>{stamp}</b>\n\n"
        f"<blockquote>{html.escape(submission_text or '')}</blockquote>"
    )
    kb_rows = []
    if status == "pending":
        kb_rows.append([
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"task_approve:{sid}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"task_reject:{sid}"),
        ])
    kb_rows.append([InlineKeyboardButton(text="Назад к заявкам", callback_data="adm:task_submissions")])
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


# ----- entry -----

async def show_admin_home(target):
    text = "👮 <b>Админ-панель</b>"
    await send_section(target, text, "admin_photo", reply_markup=await admin_kb())


@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not await _is_admin(message.from_user.id):
        await message.answer(
            "⛔ Нет доступа.\n"
            f"Ваш ID: <code>{message.from_user.id}</code>\n"
            "Попросите владельца добавить именно этот ID в разделе «Админы»."
        )
        return
    await state.clear()
    await show_admin_home(message)


@router.callback_query(F.data == "adm:home")
async def cb_admin_home(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    await state.clear()
    await call.answer()
    await show_admin_home(call)


# ----- /search -----

def user_card_kb(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Баланс", callback_data=f"u_addbal:{uid}"),
         InlineKeyboardButton(text="Баланс", callback_data=f"u_subbal:{uid}")],
        [InlineKeyboardButton(text="🚫 Бан/разбан", callback_data=f"u_ban:{uid}"),
         InlineKeyboardButton(text="Защита", callback_data=f"u_prot:{uid}")],
        [InlineKeyboardButton(text="Сбросить рефералов", callback_data=f"u_resetref:{uid}")],
        [InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")],
    ])


async def render_user_card(target, uid: int):
    u = await fetchone(
        "SELECT user_id, username, full_name, balance, referrals, banned, protected, "
        "captcha_passed, minecraft_nick FROM users WHERE user_id=?", (uid,))
    if not u:
        if isinstance(target, CallbackQuery):
            await target.answer("Не найден", show_alert=True)
        else:
            await target.answer("Не найден")
        return
    is_adm = await is_admin(uid)
    text = (
        f"👤 <b>Карточка пользователя</b>\n"
        f"ID: <code>{u[0]}</code>\n"
        f"Username: @{u[1] or '—'}\n"
        f"Имя: {u[2]}\n"
        f"Баланс: {u[3]}\n"
        f"Рефералов: {u[4]}\n"
        f"Бан: {'да' if u[5] else 'нет'}\n"
        f"Защита: {'да' if u[6] else 'нет'}\n"
        f"Капча: {'пройдена' if u[7] else 'нет'}\n"
        f"Minecraft: {u[8] or '—'}\n"
        f"Админ: {'да' if is_adm else 'нет'}"
    )
    await send_section(target, text, None, reply_markup=user_card_kb(uid))


@router.message(Command("search"))
async def cmd_search(message: Message):
    if not await _is_admin(message.from_user.id): return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /search &lt;user_id&gt;"); return
    try:
        uid = int(parts[1].strip())
    except ValueError:
        await message.answer("ID должен быть числом"); return
    await render_user_card(message, uid)


@router.message(Command("scs"))
async def cmd_scs(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /scs <user_id|@username>")
        return
    uid, label = await resolve_user_target(parts[1])
    if uid is None:
        await message.answer("Пользователь с таким username не найден в базе.")
        return
    if uid == OWNER_ID:
        await message.answer("Владельца удалять нельзя.")
        return
    summary = await purge_user_data(uid)
    await message.answer(
        "Очистка завершена.\n"
        f"Цель: <b>{label or uid}</b>\n"
        f"Удалён пользователь: <b>{summary['users_deleted']}</b>\n"
        f"Удалены админ-права: <b>{summary['admins_deleted']}</b>\n"
        f"Удалены логи/заявки/фармилки/выполнения: "
        f"<b>{summary['admin_logs_deleted'] + summary['player_logs_deleted'] + summary['withdrawals_deleted'] + summary['user_farms_deleted'] + summary['task_completions_deleted'] + summary['task_submissions_deleted']}</b>\n"
        f"Отвязано рефералов: <b>{summary['referrals_detached']}</b>"
    )


@router.callback_query(F.data.startswith("u_"))
async def cb_user_action(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    action, uid_s = call.data.split(":")
    uid = int(uid_s)
    if action in ("u_addbal", "u_subbal"):
        await state.set_state(AdminUserAction.amount)
        await state.update_data(action=action, uid=uid)
        await call.message.answer(
            f"Введите сумму для {'+' if action=='u_addbal' else '-'} баланса:",
            reply_markup=await cancel_kb("admin"),
        )
        await call.answer(); return
    if action == "u_ban":
        cur = await fetchone("SELECT banned FROM users WHERE user_id=?", (uid,))
        nv = 0 if cur[0] else 1
        await execute("UPDATE users SET banned=? WHERE user_id=?", (nv, uid))
        await log_admin(call.from_user.id, f"{'Забанил' if nv else 'Разбанил'} {uid}")
    elif action == "u_prot":
        cur = await fetchone("SELECT protected FROM users WHERE user_id=?", (uid,))
        nv = 0 if cur[0] else 1
        await execute("UPDATE users SET protected=? WHERE user_id=?", (nv, uid))
        await log_admin(call.from_user.id, f"{'Включил' if nv else 'Выключил'} защиту {uid}")
    elif action == "u_resetref":
        await execute("UPDATE users SET referrals=0 WHERE user_id=?", (uid,))
        await log_admin(call.from_user.id, f"Сбросил рефералов у {uid}")
        try:
            await call.bot.send_message(uid, "ℹ️ Все ваши рефералы были сброшены администратором.")
        except Exception:
            pass
    await call.answer("Готово")
    await render_user_card(call, uid)


@router.message(AdminUserAction.amount)
async def user_action_amount(message: Message, state: FSMContext):
    try:
        amt = int((message.text or "").strip())
    except ValueError:
        await message.answer("Число!"); return
    data = await state.get_data()
    uid = data["uid"]; action = data["action"]
    delta = amt if action == "u_addbal" else -amt
    await execute("UPDATE users SET balance=balance+? WHERE user_id=?", (delta, uid))
    await log_admin(message.from_user.id, f"Изменил баланс {uid} на {delta}")
    await state.clear()
    await message.answer("✅ Готово", reply_markup=await admin_back_kb())


# ----- Users list -----

@router.callback_query(F.data == "adm:users")
async def adm_users(call: CallbackQuery):
    if not await _is_admin(call.from_user.id):
        await call.answer(); return
    month_start = current_month_start_ts()
    month_count_row = await fetchone("SELECT COUNT(*) FROM users WHERE created_at>=?", (month_start,))
    month_count = month_count_row[0] if month_count_row else 0
    rows = await fetchall("SELECT user_id, username, full_name FROM users ORDER BY created_at DESC LIMIT 50")
    text = (
        "👥 <b>Пользователи (последние 50):</b>\n\n"
        f"Новых за текущий месяц: <b>{month_count}</b>\n\n"
    )
    for uid, un, fn in rows:
        text += f"• {html.escape(fn or 'Без имени')} | @{html.escape(un or '—')} | <code>{uid}</code>\n"
    text += "\nДля деталей: /search &lt;id&gt;"
    await send_section(
        call,
        text,
        None,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Пользователи за месяц", callback_data="adm:users_month")],
            [InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")],
        ]),
    )
    await call.answer()


@router.callback_query(F.data == "adm:users_month")
async def adm_users_month(call: CallbackQuery):
    if not await _is_admin(call.from_user.id):
        await call.answer(); return
    month_start = current_month_start_ts()
    rows = await fetchall(
        "SELECT user_id, username, full_name, created_at "
        "FROM users WHERE created_at>=? ORDER BY created_at DESC LIMIT 100",
        (month_start,),
    )
    count_row = await fetchone("SELECT COUNT(*) FROM users WHERE created_at>=?", (month_start,))
    count = count_row[0] if count_row else 0
    text = f"👥 <b>Пользователи за текущий месяц:</b> <b>{count}</b>\n\n"
    if not rows:
        text += "За текущий месяц новых пользователей нет."
    else:
        for uid, un, fn, created_at in rows:
            stamp = time.strftime("%d.%m.%Y", time.localtime(created_at or 0))
            text += (
                f"• {html.escape(fn or 'Без имени')} | @{html.escape(un or '—')} | "
                f"<code>{uid}</code> | {stamp}\n"
            )
        if count > len(rows):
            text += f"\nПоказаны последние {len(rows)} из {count}."
    await send_section(
        call,
        text,
        None,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад к пользователям", callback_data="adm:users")],
            [InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")],
        ]),
    )
    await call.answer()


# ----- Channel stats -----

@router.callback_query(F.data == "adm:stats")
async def adm_stats(call: CallbackQuery, bot: Bot):
    if not await _is_admin(call.from_user.id):
        await call.answer(); return
    await render_stats_overview(call, bot)
    return
    await call.answer("Загрузка…")
    # --- Статистика бота: переходы по ссылке + охват ---
    try:
        link_clicks = int(await get_setting("bot_link_clicks", "0") or "0")
    except ValueError:
        link_clicks = 0
    total_users_row = await fetchone("SELECT COUNT(*) FROM users")
    total_users = total_users_row[0] if total_users_row else 0
    active_row = await fetchone(
        "SELECT COUNT(*) FROM users WHERE banned=0 AND captcha_passed=1"
    )
    active_users = active_row[0] if active_row else 0
    me = await bot.get_me()
    bot_text = (
        "🤖 <b>Статистика бота:</b>\n"
        f"• Ссылка: <code>https://t.me/{me.username}</code>\n"
        f"• Переходов по ссылке (всего /start): <b>{link_clicks}</b>\n"
        f"• Охват (уникальных пользователей): <b>{total_users}</b>\n"
        f"• Активных (прошли капчу, не забанены): <b>{active_users}</b>\n\n"
    )
    chans = await fetchall("SELECT id, category, link, chat_id, title, is_private, active FROM channels")
    if not chans:
        await send_section(call, bot_text + "Каналов нет", None,
                           reply_markup=await admin_back_kb()); return
    text = bot_text + "📊 <b>Статистика каналов:</b>\n\n"
    for cid, cat, link, chat_id, title, is_private, active in chans:
        members = "?"
        try:
            members = await bot.get_chat_member_count(chat_id)
        except Exception:
            pass
        st = await fetchone("SELECT join_requests, reach FROM channel_stats WHERE channel_id=?", (cid,))
        jr, reach = (st or (0, 0))
        text += (
            f"• <b>{title or link}</b>\n"
            f"  ID: <code>{chat_id}</code> | {'частный' if is_private else 'публичный'} | "
            f"{'активен' if active else 'неактивен'}\n"
            f"  Категория: {cat}\n"
            f"  Подписчиков: {members} | Заявок: {jr} | Охват: {reach}\n"
            f"  Ссылка: {link}\n\n"
        )
    await send_section(call, text, None, reply_markup=await admin_back_kb())


@router.callback_query(F.data.startswith("stat_ch:"))
async def adm_stats_channel(call: CallbackQuery, bot: Bot):
    if not await _is_admin(call.from_user.id):
        await call.answer(); return
    await call.answer()
    await render_channel_stats(call, bot, int(call.data.split(":")[1]))


# ----- Broadcast -----

@router.callback_query(F.data == "adm:broadcast")
async def adm_broadcast(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id):
        await call.answer(); return
    await state.set_state(AdminBroadcast.text)
    await call.message.answer(
        "Отправьте сообщение для рассылки (текст/фото).\n"
        "Поддерживаются HTML и премиум-эмодзи через "
        "<code>&lt;tg-emoji emoji-id=\"ID\"&gt;😀&lt;/tg-emoji&gt;</code>",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.message(AdminBroadcast.text)
async def broadcast_send(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    rows = await fetchall("SELECT user_id FROM users WHERE banned=0")
    sent = failed = 0
    for (uid,) in rows:
        try:
            await message.copy_to(uid)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.04)
    await log_admin(message.from_user.id, f"Рассылка: {sent} отправлено, {failed} ошибок")
    await message.answer(f"✅ Отправлено: {sent}, ошибок: {failed}", reply_markup=await admin_back_kb())


# ----- Economy -----

ECON_FIELDS = [
    ("currency_name", "Название валюты"),
    ("currency_emoji", "Эмодзи валюты"),
    ("min_withdraw", "Мин. вывод"),
    ("bonus_min", "Бонус мин."),
    ("bonus_max", "Бонус макс."),
    ("casino_dice_bet", "Кубик: ставка"),
    ("casino_dice_win", "Кубик: выигрыш"),
    ("casino_dice_chance", "Кубик: шанс %"),
    ("casino_basket_bet", "Баскет: ставка"),
    ("casino_basket_win", "Баскет: выигрыш"),
    ("casino_basket_chance", "Баскет: шанс %"),
    ("theft_chance", "Кража: шанс %"),
    ("theft_win_pct", "Кража: выигрыш %"),
    ("theft_lose_pct", "Кража: проигрыш %"),
    ("ref_reward", "Награда за реферала"),
]


@router.callback_query(F.data == "adm:econ")
async def adm_econ(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    rows = []
    for k, label in ECON_FIELDS:
        val = await get_setting(k)
        rows.append([InlineKeyboardButton(text=f"{label}: {val}", callback_data=f"econ:{k}")])
    rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, "💱 Экономика — выберите параметр:", None,
                       reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await call.answer()


@router.callback_query(F.data.startswith("econ:"))
async def cb_econ(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    key = call.data.split(":")[1]
    await state.set_state(AdminEcon.value)
    await state.update_data(key=key)
    await call.message.answer(f"Введите новое значение для <code>{key}</code>:",
                              reply_markup=await cancel_kb("admin"))
    await call.answer()


@router.message(AdminEcon.value)
async def econ_set(message: Message, state: FSMContext):
    data = await state.get_data()
    await set_setting(data["key"], (message.text or "").strip())
    await log_admin(message.from_user.id, f"Изменил экономику: {data['key']} = {message.text.strip()}")
    await state.clear()
    await message.answer("✅ Сохранено", reply_markup=await admin_back_kb())


# ----- Channels -----

@router.callback_query(F.data == "adm:channels")
async def adm_channels(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await render_channels_menu(call)
    await call.answer()
    return
    rows = await fetchall("SELECT id, category, link, title, active FROM channels ORDER BY category")
    s_op = await get_setting("start_op_enabled"); t_op = await get_setting("tasks_op_enabled")
    cap = await get_setting("captcha_enabled")
    text = (
        "📡 <b>Каналы ОП</b>\n\nКатегории: <b>start</b>, <b>tasks</b>\n\n"
        + "".join(f"• [{cat}] {title or link} ({'on' if active else 'off'}) — id <code>{cid}</code>\n"
                 for cid, cat, link, title, active in rows)
        + f"\nОП старт: {'on' if s_op=='1' else 'off'} | "
          f"ОП задания: {'on' if t_op=='1' else 'off'} | Капча: {'on' if cap=='1' else 'off'}"
    )
    kb_rows = [
        [InlineKeyboardButton(text="В Старт", callback_data="ch_add:start"),
         InlineKeyboardButton(text="В Задания", callback_data="ch_add:tasks")],
        [InlineKeyboardButton(text=f"ОП Старт: {'ON' if s_op=='1' else 'off'}",
                              callback_data="ch_toggle:start_op_enabled"),
         InlineKeyboardButton(text=f"ОП Задания: {'ON' if t_op=='1' else 'off'}",
                              callback_data="ch_toggle:tasks_op_enabled")],
        [InlineKeyboardButton(text=f"Капча: {'ON' if cap=='1' else 'off'}",
                              callback_data="ch_toggle:captcha_enabled")],
    ]
    for cid, cat, link, title, active in rows:
        kb_rows.append([InlineKeyboardButton(text=f"Удалить #{cid} {title or link}",
                                             callback_data=f"ch_del:{cid}")])
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await call.answer()


@router.callback_query(F.data.startswith("ch_toggle:"))
async def cb_ch_toggle(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    key = call.data.split(":")[1]
    cur = await get_setting(key, "0")
    new = "0" if cur == "1" else "1"
    await set_setting(key, new)
    await log_admin(call.from_user.id, f"Toggle {key} -> {new}")
    await call.answer(f"{key}={new}")
    await adm_channels(call)


@router.callback_query(F.data.startswith("ch_del:"))
async def cb_ch_del(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    cid = int(call.data.split(":")[1])
    linked_tasks = await fetchone("SELECT COUNT(*) FROM tasks WHERE channel_id=?", (cid,))
    linked_tasks_count = linked_tasks[0] if linked_tasks else 0
    await execute("DELETE FROM tasks WHERE channel_id=?", (cid,))
    await execute("DELETE FROM channels WHERE id=?", (cid,))
    await log_admin(call.from_user.id, f"Удалил канал {cid}")
    await call.answer("Удалено")
    await adm_channels(call)


@router.callback_query(F.data.startswith("ch_add:"))
async def cb_ch_add(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    cat = call.data.split(":")[1]
    await state.set_state(AdminChannel.chat_id)
    await state.update_data(category=cat)
    await call.message.answer(
        "Пришлите chat_id, @username или public-ссылку на канал.\n\n"
        "Поддерживаются форматы:\n"
        "• <code>-1001234567890</code>\n"
        "• <code>@channel_name</code>\n"
        "• <code>https://t.me/channel_name</code>\n\n"
        "Бот должен быть администратором канала с правом создавать пригласительные ссылки.",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()
    return
    await call.message.answer(
        "Отправьте <b>chat_id</b> канала (например <code>-1001234567890</code>) "
        "или <b>@username</b> публичного канала.\n\n"
        "Бот должен быть администратором канала с правом «Пригласительные ссылки» "
        "(can_invite_users) — иначе он не сможет создать собственную ссылку.",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.message(AdminChannel.chat_id)
async def ch_chat_id(message: Message, state: FSMContext, bot: Bot):
    await process_channel_reference_input(message, state, bot)
    return
    chat_id_raw = (message.text or "").strip()
    if not chat_id_raw:
        await message.answer("Пустой ввод. Пришлите chat_id или @username.")
        return
    # 1) Проверяем, что бот вообще видит чат
    try:
        chat = await bot.get_chat(chat_id_raw)
    except Exception as e:
        await message.answer(
            f"❌ Не получилось открыть канал: <code>{e}</code>\n"
            "Убедитесь, что ID/username правильный и что бот добавлен в канал."
        )
        return
    chat_id = str(chat.id)
    title = chat.title or chat_id
    is_private = 0 if chat.username else 1
    public_link = f"https://t.me/{chat.username}" if chat.username else ""

    # 2) Проверяем, что бот — админ с правом приглашать
    try:
        me = await bot.me()
        member = await bot.get_chat_member(chat.id, me.id)
    except Exception as e:
        await message.answer(f"❌ Не удалось проверить права бота: <code>{e}</code>")
        return
    status = getattr(member, "status", None)
    if status not in ("administrator", "creator"):
        await message.answer(
            "❌ Бот не является администратором этого канала. "
            "Добавьте его админом и повторите."
        )
        return
    can_invite = getattr(member, "can_invite_users", None)
    if status == "administrator" and not can_invite:
        await message.answer(
            "❌ У бота нет права <b>«Пригласительные ссылки»</b> "
            "(can_invite_users). Включите его в настройках админа канала и повторите."
        )
        return

    # 3) Создаём собственную ссылку с заявками на вступление
    invite_link_url = ""
    invite_link_name = f"OP-{message.from_user.id}-{int(time.time())}"
    try:
        inv = await bot.create_chat_invite_link(
            chat.id,
            name=invite_link_name[:32],
            creates_join_request=True,
        )
        invite_link_url = inv.invite_link
    except Exception as e:
        await message.answer(
            f"❌ Не удалось создать ссылку приглашения: <code>{e}</code>"
        )
        return

    await state.update_data(
        chat_id=chat_id, title=title, is_private=is_private,
        public_link=public_link, invite_link=invite_link_url,
        invite_link_name=invite_link_name,
    )
    await state.set_state(AdminChannel.max_subs)
    await message.answer(
        f"✅ Канал найден: <b>{title}</b>\n"
        f"Своя ссылка-приглашение: {invite_link_url}\n\n"
        "Введите максимум подписчиков (0 = без ограничения):",
        reply_markup=await cancel_kb("admin"),
    )


@router.message(AdminChannel.max_subs)
async def ch_max(message: Message, state: FSMContext):
    try:
        mx = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число."); return
    d = await state.get_data()
    # В колонку link сохраняем приоритетно invite-link (бот будет показывать
    # пользователям именно её), public_link оставим как title-fallback при выводе.
    display_link = d.get("invite_link") or d.get("public_link") or ""
    await execute(
        "INSERT INTO channels(category, link, chat_id, title, is_private, max_subs, "
        "invite_link, invite_link_name) VALUES(?,?,?,?,?,?,?,?)",
        (
            d["category"], display_link, d["chat_id"], d["title"],
            d["is_private"], mx,
            d.get("invite_link") or "", d.get("invite_link_name") or "",
        ),
    )
    await log_admin(
        message.from_user.id,
        f"Добавил канал {d['title']} ({d['category']}) с invite={d.get('invite_link')}",
    )
    await state.clear()
    await message.answer(
        "✅ Канал добавлен. Заявки на вступление, поданные через эту ссылку, "
        "теперь будут считаться ботом.",
        reply_markup=await admin_back_kb(),
    )


# ----- Promocodes -----

@router.callback_query(F.data == "adm:promo")
async def adm_promo(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    rows = await fetchall("SELECT code, amount, activations_left, activations_total FROM promocodes")
    text = "🎟 <b>Промокоды:</b>\n\n"
    for c, a, l, t in rows:
        text += f"• <code>{c}</code> | {a} | осталось {l}/{t}\n"
    kb_rows = [
        [InlineKeyboardButton(text="Добавить промокод", callback_data="promo_add")],
        [InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")],
    ]
    await send_section(call, text or "Нет промокодов", None,
                       reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await call.answer()


@router.callback_query(F.data == "promo_add")
async def cb_promo_add(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await state.set_state(AdminPromo.code)
    await call.message.answer("Введите название нового промокода:", reply_markup=await cancel_kb("admin"))
    await call.answer()


@router.message(AdminPromo.code)
async def promo_a_code(message: Message, state: FSMContext):
    await state.update_data(code=(message.text or "").strip())
    await state.set_state(AdminPromo.amount)
    await message.answer("Сколько валюты выдаёт?", reply_markup=await cancel_kb("admin"))


@router.message(AdminPromo.amount)
async def promo_a_amount(message: Message, state: FSMContext):
    try:
        a = int((message.text or "").strip())
    except ValueError:
        await message.answer("Число!"); return
    await state.update_data(amount=a)
    await state.set_state(AdminPromo.activations)
    await message.answer("Сколько активаций?", reply_markup=await cancel_kb("admin"))


@router.message(AdminPromo.activations)
async def promo_a_act(message: Message, state: FSMContext):
    try:
        n = int((message.text or "").strip())
    except ValueError:
        await message.answer("Число!"); return
    d = await state.get_data()
    await execute(
        "INSERT OR REPLACE INTO promocodes(code, amount, activations_left, activations_total) VALUES(?,?,?,?)",
        (d["code"], d["amount"], n, n),
    )
    await log_admin(message.from_user.id, f"Создал промокод {d['code']}")
    await state.clear()
    await message.answer("✅ Создан", reply_markup=await admin_back_kb())


# ----- Withdrawals -----

@router.callback_query(F.data == "adm:wd")
async def adm_wd(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    rows = await fetchall(
        "SELECT w.id, w.user_id, w.amount, w.minecraft_nick, u.username "
        "FROM withdrawals w JOIN users u ON u.user_id=w.user_id "
        "WHERE w.status='pending' ORDER BY w.created_at LIMIT 20"
    )
    if not rows:
        await send_section(call, "Нет заявок", None, reply_markup=await admin_back_kb())
        await call.answer(); return
    await send_section(call, f"💳 Заявок: {len(rows)}", None, reply_markup=await admin_back_kb())
    for wid, uid, amt, nick, un in rows:
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Выплатить", callback_data=f"wd_ok:{wid}"),
            InlineKeyboardButton(text="Откл. (возврат)", callback_data=f"wd_ref:{wid}"),
            InlineKeyboardButton(text="Откл. без возврата", callback_data=f"wd_no:{wid}"),
        ]])
        await call.message.answer(
            f"#{wid} | @{un or '—'} | <code>{uid}</code>\nMC: {nick}\nСумма: {amt}",
            reply_markup=kb,
        )
    await call.answer()


@router.callback_query(F.data.startswith("wd_"))
async def cb_wd(call: CallbackQuery, bot: Bot):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    action, wid_s = call.data.split(":"); wid = int(wid_s)
    w = await fetchone("SELECT user_id, amount, status FROM withdrawals WHERE id=?", (wid,))
    if not w or w[2] != "pending":
        await call.answer("Уже обработано"); return
    if action == "wd_ok":
        await execute("UPDATE withdrawals SET status='approved', processed_at=?, processed_by=? WHERE id=?",
                      (int(time.time()), call.from_user.id, wid))
        try: await bot.send_message(w[0], f"✅ Ваша заявка #{wid} ({w[1]}) одобрена.")
        except Exception: pass
        await log_admin(call.from_user.id, f"Одобрил вывод #{wid}")
    elif action == "wd_ref":
        await execute("UPDATE withdrawals SET status='rejected_refund', processed_at=?, processed_by=? WHERE id=?",
                      (int(time.time()), call.from_user.id, wid))
        await execute("UPDATE users SET balance=balance+? WHERE user_id=?", (w[1], w[0]))
        try: await bot.send_message(w[0], f"❌ Заявка #{wid} отклонена. Средства возвращены.")
        except Exception: pass
        await log_admin(call.from_user.id, f"Отклонил с возвратом #{wid}")
    elif action == "wd_no":
        await execute("UPDATE withdrawals SET status='rejected_no_refund', processed_at=?, processed_by=? WHERE id=?",
                      (int(time.time()), call.from_user.id, wid))
        try: await bot.send_message(w[0], f"❌ Заявка #{wid} отклонена без возврата.")
        except Exception: pass
        await log_admin(call.from_user.id, f"Отклонил без возврата #{wid}")
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await call.answer("Готово")


# ----- Referrals -----

@router.callback_query(F.data == "adm:refs")
async def adm_refs(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    rows = await fetchall(
        "SELECT user_id, full_name, referrals FROM users WHERE referrals>0 "
        "ORDER BY referrals DESC LIMIT 30"
    )
    text = "🤝 <b>Рефералы:</b>\n\n"
    kb_rows = []
    for uid, fn, r in rows:
        text += f"• {fn} (<code>{uid}</code>) — {r}\n"
        kb_rows.append([InlineKeyboardButton(text=f"{fn}: {r}", callback_data=f"ref_view:{uid}")])
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, text or "Пусто", None,
                       reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await call.answer()


async def _render_ref_view(call: CallbackQuery, uid: int):
    rows = await fetchall(
        "SELECT user_id, username, full_name FROM users WHERE referrer_id=? ORDER BY user_id",
        (uid,),
    )
    text = f"Рефералы пользователя <code>{uid}</code> ({len(rows)}):\n\n"
    kb_rows = []
    for u, un, fn in rows:
        text += f"• {fn} | @{un or '—'} | <code>{u}</code>\n"
        label = (fn or str(u))[:24]
        kb_rows.append([InlineKeyboardButton(
            text=f"{label} (@{un or u})",
            callback_data=f"ref_del:{uid}:{u}",
        )])
    kb_rows.append([InlineKeyboardButton(text="Сбросить ВСЕХ рефералов",
                                         callback_data=f"u_resetref:{uid}")])
    kb_rows.append([InlineKeyboardButton(text="Назад", callback_data="adm:refs")])
    await send_section(call, text or "Пусто", None,
                       reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


@router.callback_query(F.data.startswith("ref_view:"))
async def cb_ref_view(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    uid = int(call.data.split(":")[1])
    await _render_ref_view(call, uid)
    await call.answer()


@router.callback_query(F.data.startswith("ref_del:"))
async def cb_ref_del(call: CallbackQuery):
    if not await _is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    _, ref_owner_s, ref_user_s = call.data.split(":")
    ref_owner = int(ref_owner_s); ref_user = int(ref_user_s)
    row = await fetchone(
        "SELECT 1 FROM users WHERE user_id=? AND referrer_id=?",
        (ref_user, ref_owner),
    )
    if not row:
        await call.answer("Уже удалено", show_alert=True)
        await cb_ref_view(call); return
    await execute("UPDATE users SET referrer_id=NULL WHERE user_id=?", (ref_user,))
    await execute(
        "UPDATE users SET referrals=MAX(0, referrals-1) WHERE user_id=?",
        (ref_owner,),
    )
    await log_admin(call.from_user.id,
                    f"Удалил реферала {ref_user} у {ref_owner}")
    try:
        await call.bot.send_message(
            ref_owner,
            f"ℹ️ Администратор удалил у вас реферала <code>{ref_user}</code>.",
        )
    except Exception:
        pass
    await call.answer("✅ Реферал удалён")
    await _render_ref_view(call, ref_owner)


# ----- Logs -----

@router.callback_query(F.data == "adm:logs")
async def adm_logs(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Логи админов", callback_data="logs:admin"),
         InlineKeyboardButton(text="Логи игроков", callback_data="logs:player")],
        [InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")],
    ])
    await send_section(call, "Выберите тип логов:", None, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("logs:"))
async def cb_logs(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    kind = call.data.split(":")[1]
    if kind == "admin":
        rows = await fetchall(
            "SELECT a.admin_id, u.full_name, a.action, a.created_at FROM admin_logs a "
            "LEFT JOIN users u ON u.user_id=a.admin_id ORDER BY a.id DESC LIMIT 30"
        )
        text = "📜 <b>Логи админов:</b>\n\n"
    else:
        rows = await fetchall(
            "SELECT p.user_id, u.full_name, p.action, p.created_at FROM player_logs p "
            "LEFT JOIN users u ON u.user_id=p.user_id ORDER BY p.id DESC LIMIT 30"
        )
        text = "📜 <b>Логи игроков:</b>\n\n"
    for uid, fn, act, ts in rows:
        t = time.strftime("%d.%m %H:%M", time.localtime(ts))
        text += f"[{t}] {fn or uid}: {act}\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="adm:logs")],
    ])
    await send_section(call, text or "Пусто", None, reply_markup=kb)
    await call.answer()


# ----- My protection -----

@router.callback_query(F.data == "adm:protect")
async def adm_protect(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    cur = await fetchone("SELECT protected FROM users WHERE user_id=?", (call.from_user.id,))
    nv = 0 if (cur and cur[0]) else 1
    await execute("UPDATE users SET protected=? WHERE user_id=?", (nv, call.from_user.id))
    await log_admin(call.from_user.id, f"Защита: {nv}")
    await call.answer(f"🛡 Защита {'включена' if nv else 'выключена'}", show_alert=True)


# ----- Events -----

@router.callback_query(F.data == "adm:events")
async def adm_events(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    cur_m = await get_setting("event_x_mult"); cur_d = await get_setting("event_shop_discount")
    text = f"🎉 Текущий множитель: x{cur_m}, скидка магазина: {cur_d}%"
    rows = [
        [InlineKeyboardButton(text="x1", callback_data="ev_mult:1"),
         InlineKeyboardButton(text="x2", callback_data="ev_mult:2"),
         InlineKeyboardButton(text="x3", callback_data="ev_mult:3")],
        [InlineKeyboardButton(text=f"{d}%", callback_data=f"ev_disc:{d}") for d in (0, 10, 20, 30, 40)],
        [InlineKeyboardButton(text=f"{d}%", callback_data=f"ev_disc:{d}") for d in (50, 60, 70, 80, 90)],
        [InlineKeyboardButton(text="100%", callback_data="ev_disc:100")],
        [InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")],
    ]
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await call.answer()


@router.callback_query(F.data.startswith("ev_"))
async def cb_event(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    kind, val = call.data.split(":")
    await set_setting("event_x_mult" if kind == "ev_mult" else "event_shop_discount", val)
    await log_admin(call.from_user.id, f"Ивент {kind}={val}")
    await call.answer(f"Установлено {val}")
    await adm_events(call)


# ----- Admins -----

@router.callback_query(F.data == "adm:admins")
async def adm_admins(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        await call.answer("Только владелец", show_alert=True); return
    rows = await fetchall(
        "SELECT a.user_id, u.full_name, u.username FROM admins a "
        "LEFT JOIN users u ON u.user_id=a.user_id"
    )
    text = "👮 <b>Админы:</b>\n\n"
    kb_rows = []
    for uid, fn, un in rows:
        text += f"• {fn or uid} | @{un or '—'} | <code>{uid}</code>\n"
        if uid != OWNER_ID:
            kb_rows.append([InlineKeyboardButton(text=f"Уволить {fn or uid}",
                                                 callback_data=f"adm_fire:{uid}")])
    kb_rows.append([InlineKeyboardButton(text="Добавить админа", callback_data="adm_add")])
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await call.answer()


@router.callback_query(F.data.startswith("adm_fire:"))
async def cb_adm_fire(call: CallbackQuery):
    if call.from_user.id != OWNER_ID: await call.answer(); return
    uid = int(call.data.split(":")[1])
    await execute("DELETE FROM admins WHERE user_id=?", (uid,))
    await log_admin(call.from_user.id, f"Уволил админа {uid}")
    await call.answer("Уволен")
    await adm_admins(call)


@router.callback_query(F.data == "adm_add")
async def cb_adm_add(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != OWNER_ID: await call.answer(); return
    await state.set_state(AdminAddAdmin.user_id)
    await call.message.answer("Введите user_id или @username нового админа:", reply_markup=await cancel_kb("admin"))
    await call.answer()


@router.message(AdminAddAdmin.user_id)
async def adm_add_uid(message: Message, state: FSMContext):
    forwarded_user = getattr(message, "forward_from", None)
    if forwarded_user:
        uid, label = forwarded_user.id, forwarded_user.full_name or str(forwarded_user.id)
    else:
        uid, label = await resolve_user_target(message.text or "")
    if uid is None:
        await message.answer(
            "Пользователь с таким username не найден в базе. "
            "Попросите его нажать /start или отправьте числовой user_id."
        )
        return
    await execute("INSERT OR IGNORE INTO admins(user_id, added_by, added_at) VALUES(?,?,?)",
                  (uid, message.from_user.id, int(time.time())))
    await log_admin(message.from_user.id, f"Добавил админа {uid}")
    if not await is_admin(uid):
        await message.answer("Не получилось сохранить права. Попробуйте добавить числовой user_id.")
        return
    await state.clear()
    await message.answer(f"✅ Добавлен: {label or uid}", reply_markup=await admin_back_kb())


# ----- Tasks management -----

@router.callback_query(F.data == "adm:tasks")
async def adm_tasks(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await render_admin_tasks(call)
    await call.answer()


@router.callback_query(F.data == "adm:task_submissions")
async def adm_task_submissions(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await render_task_submissions_queue(call)
    await call.answer()


@router.callback_query(F.data.startswith("task_submission:"))
async def cb_task_submission(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    sid = int(call.data.split(":")[1])
    await render_task_submission_card(call, sid)
    await call.answer()


@router.callback_query(F.data.startswith("task_approve:"))
async def cb_task_approve(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    sid = int(call.data.split(":")[1])
    result = await approve_task_submission(sid, call.from_user.id)
    status = result.get("status")
    if status == "not_found":
        await call.answer("Заявка не найдена", show_alert=True)
        return
    if status == "task_inactive":
        await call.answer("Задание выключено", show_alert=True)
        await render_task_submission_card(call, sid)
        return
    if status == "limit_reached":
        await call.answer("Лимит выполнений уже достигнут", show_alert=True)
        await render_task_submission_card(call, sid)
        return
    if status not in {"approved", "already_completed"}:
        await call.answer("Заявка уже обработана", show_alert=True)
        await render_task_submission_card(call, sid)
        return

    await log_admin(
        call.from_user.id,
        f"Подтвердил заявку #{sid} по заданию #{result['task_id']} пользователю {result['user_id']}",
    )
    await log_player(result["user_id"], f"Заявка по заданию #{result['task_id']} подтверждена админом")
    try:
        if status == "approved":
            await call.bot.send_message(
                result["user_id"],
                f"✅ Администратор подтвердил выполнение задания «{html.escape(result['task_name'])}».\n"
                f"Начислено: <b>+{result['reward']}</b>",
            )
        else:
            await call.bot.send_message(
                result["user_id"],
                f"ℹ️ Задание «{html.escape(result['task_name'])}» уже было засчитано ранее.",
            )
    except Exception:
        pass
    await call.answer("Заявка подтверждена")
    await render_task_submissions_queue(call)


@router.callback_query(F.data.startswith("task_reject:"))
async def cb_task_reject(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    sid = int(call.data.split(":")[1])
    result = await reject_task_submission(sid, call.from_user.id)
    status = result.get("status")
    if status == "not_found":
        await call.answer("Заявка не найдена", show_alert=True)
        return
    if status != "rejected":
        await call.answer("Заявка уже обработана", show_alert=True)
        await render_task_submission_card(call, sid)
        return

    await log_admin(
        call.from_user.id,
        f"Отклонил заявку #{sid} по заданию #{result['task_id']} пользователю {result['user_id']}",
    )
    await log_player(result["user_id"], f"Заявка по заданию #{result['task_id']} отклонена админом")
    try:
        await call.bot.send_message(
            result["user_id"],
            f"❌ Администратор отклонил выполнение задания «{html.escape(result['task_name'])}».\n"
            "Можно открыть задание и отправить ответ заново.",
        )
    except Exception:
        pass
    await call.answer("Заявка отклонена")
    await render_task_submissions_queue(call)


@router.callback_query(F.data.startswith("task_del:"))
async def cb_task_del(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    tid = int(call.data.split(":")[1])
    await execute("DELETE FROM task_submissions WHERE task_id=?", (tid,))
    await execute("DELETE FROM task_completions WHERE task_id=?", (tid,))
    await execute("DELETE FROM tasks WHERE id=?", (tid,))
    await log_admin(call.from_user.id, f"Удалил задание {tid}")
    await call.answer("Удалено")
    await adm_tasks(call)


@router.callback_query(F.data == "task_add")
async def cb_task_add(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await state.clear()
    await state.set_state(AdminTask.task_type)
    await send_task_type_picker(call.message)
    await call.answer()


@router.callback_query(F.data.startswith("task_kind:"))
async def cb_task_kind(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    task_type = call.data.split(":", 1)[1]
    await state.update_data(task_type=task_type, channel_id=None)
    if task_type == "subscribe":
        await state.set_state(AdminTask.channel_id)
        ok = await send_task_channel_picker(call.message)
        await call.answer()
        if not ok:
            await state.clear()
        return

    await state.set_state(AdminTask.name)
    await call.message.answer("Название задания:", reply_markup=await cancel_kb("admin"))
    await call.answer()


@router.callback_query(F.data.startswith("task_channel:"))
async def cb_task_channel(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    cid = int(call.data.split(":")[1])
    row = await fetchone("SELECT title, link FROM channels WHERE id=?", (cid,))
    if not row:
        await call.answer("Канал не найден", show_alert=True); return

    title, link = row
    await state.update_data(channel_id=cid, task_type="subscribe")
    await state.set_state(AdminTask.name)
    await call.message.answer(
        f"Канал выбран: <b>{title or link or cid}</b>\n"
        "Введите название задания или отправьте <code>-</code> для шаблона по умолчанию.",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.message(AdminTask.name)
async def task_name(message: Message, state: FSMContext):
    data = await state.get_data()
    raw_name = (message.text or "").strip()
    if data.get("task_type") == "subscribe" and raw_name == "-":
        row = await fetchone("SELECT title, link FROM channels WHERE id=?", (data.get("channel_id"),))
        title = row[0] if row else None
        raw_name = f"Подписка на {title or 'канал'}"
    await state.update_data(name=raw_name)
    await state.set_state(AdminTask.reward)
    await message.answer("Награда:", reply_markup=await cancel_kb("admin"))


@router.message(AdminTask.reward)
async def task_reward(message: Message, state: FSMContext):
    try:
        r = int((message.text or "").strip())
    except ValueError:
        await message.answer("Число!"); return
    await state.update_data(reward=r)
    await state.set_state(AdminTask.max_completions)
    await message.answer("Макс. кол-во выполнений (0 = ∞):", reply_markup=await cancel_kb("admin"))


@router.message(AdminTask.max_completions)
async def task_mx(message: Message, state: FSMContext):
    try:
        mx = int((message.text or "").strip())
    except ValueError:
        await message.answer("Число!"); return
    d = await state.get_data()
    await execute(
        "INSERT INTO tasks(name, reward, max_completions, task_type, channel_id) VALUES(?,?,?,?,?)",
        (d["name"], d["reward"], mx, d.get("task_type", "manual"), d.get("channel_id")),
    )
    await log_admin(message.from_user.id, f"Добавил задание {d['name']}")
    await state.clear()
    await message.answer("✅ Добавлено", reply_markup=await admin_back_kb())


# ----- Shop management -----

@router.callback_query(F.data == "adm:shop")
async def adm_shop(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    currency_name = await get_setting("currency_name")
    rows = await fetchall(
        "SELECT id, name, description, price, income_per_day, active, emoji_icon "
        "FROM shop_items ORDER BY id DESC"
    )
    text = "🛒 <b>Магазин:</b>\n\n"
    kb_rows = []
    for iid, n, desc, p, inc, a, emoji_icon in rows:
        text += (
            f"<b>#{iid}</b>\n"
            f"{format_shop_item_block(name=n, price=p, income_per_day=inc, active=bool(a), currency_name=currency_name, emoji_icon=emoji_icon, description=desc)}\n\n"
        )
        toggle_text = "Выключить" if a else "Включить"
        kb_rows.append([
            InlineKeyboardButton(text=toggle_text, callback_data=f"shop_toggle:{iid}"),
            InlineKeyboardButton(text=f"Удалить {trim_button_text(n, 20)}", callback_data=f"shop_del:{iid}"),
        ])
    kb_rows.append([InlineKeyboardButton(text="Добавить товар", callback_data="shop_add")])
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, text or "Пусто", None,
                       reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await call.answer()


@router.callback_query(F.data.startswith("shop_toggle:"))
async def cb_shop_toggle(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    iid = int(call.data.split(":")[1])
    row = await fetchone("SELECT active FROM shop_items WHERE id=?", (iid,))
    if not row:
        await call.answer("Товар не найден", show_alert=True)
        return
    new_value = 0 if row[0] else 1
    await execute("UPDATE shop_items SET active=? WHERE id=?", (new_value, iid))
    await log_admin(call.from_user.id, f"Изменил статус товара {iid} -> {new_value}")
    await call.answer("Статус обновлён")
    await adm_shop(call)


@router.callback_query(F.data.startswith("shop_del:"))
async def cb_shop_del(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    iid = int(call.data.split(":")[1])
    await execute("DELETE FROM shop_items WHERE id=?", (iid,))
    await log_admin(call.from_user.id, f"Удалил товар {iid}")
    await call.answer("Удалено")
    await adm_shop(call)


@router.callback_query(F.data == "shop_add")
async def cb_shop_add(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await state.set_state(AdminShop.emoji)
    await call.message.answer(
        "Отправьте премиум-эмодзи для товара.\nЕсли эмодзи не нужен, отправьте `-`.",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.message(AdminShop.emoji)
async def shop_emoji(message: Message, state: FSMContext):
    stored, human = extract_stored_icon(message.text or "", message.entities)
    await state.update_data(emoji_icon=stored)
    await state.set_state(AdminShop.name)
    await message.answer(f"Эмодзи сохранён: {human}\nТеперь отправьте название товара:",
                         reply_markup=await cancel_kb("admin"))


@router.message(AdminShop.name)
async def shop_n(message: Message, state: FSMContext):
    name = (message.text or "").strip()
    if not name:
        await message.answer("Введите название товара."); return
    await state.update_data(name=name)
    await state.set_state(AdminShop.price)
    await message.answer("Цена в токенах:", reply_markup=await cancel_kb("admin"))


@router.message(AdminShop.price)
async def shop_p(message: Message, state: FSMContext):
    try:
        p = int((message.text or "").strip())
    except ValueError:
        await message.answer("Число!"); return
    if p < 0:
        await message.answer("Цена не может быть отрицательной."); return
    await state.update_data(price=p)
    await state.set_state(AdminShop.income)
    await message.answer("Доход в день:", reply_markup=await cancel_kb("admin"))


@router.message(AdminShop.income)
async def shop_i(message: Message, state: FSMContext):
    try:
        inc = int((message.text or "").strip())
    except ValueError:
        await message.answer("Число!"); return
    if inc < 0:
        await message.answer("Доход не может быть отрицательным."); return
    await state.update_data(income_per_day=inc)
    await state.set_state(AdminShop.active)
    await message.answer(
        "Статус товара: `доступно` или `недоступно`",
        reply_markup=await cancel_kb("admin"),
    )


@router.message(AdminShop.active)
async def shop_active(message: Message, state: FSMContext):
    active = parse_shop_active(message.text or "")
    if active is None:
        await message.answer("Введите `доступно` или `недоступно`."); return
    d = await state.get_data()
    await execute(
        "INSERT INTO shop_items(name, description, price, income_per_day, active, emoji_icon) "
        "VALUES(?,?,?,?,?,?)",
        (d["name"], "", d["price"], d["income_per_day"], active, d.get("emoji_icon", "")),
    )
    await log_admin(message.from_user.id, f"Добавил товар {d['name']}")
    await state.clear()
    currency_name = await get_setting("currency_name")
    preview = format_shop_item_block(
        name=d["name"],
        price=d["price"],
        income_per_day=d["income_per_day"],
        active=bool(active),
        currency_name=currency_name,
        emoji_icon=d.get("emoji_icon", ""),
    )
    await message.answer(f"✅ Добавлено\n\n{preview}", reply_markup=await admin_back_kb())


# ----- Funtime IP -----

@router.callback_query(F.data == "adm:funtime")
async def adm_funtime(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    main = await get_setting("funtime_main_ips")
    rows = await fetchall("SELECT id, label, ip FROM funtime_test_ips")
    text = f"🌐 <b>Funtime</b>\n\nОсновные IP (CSV): <code>{main}</code>\n\nТестовые:\n"
    kb_rows = []
    for tid, label, ip in rows:
        text += f"• {label}: <code>{ip}</code>\n"
        kb_rows.append([InlineKeyboardButton(text=f"Удалить {label}", callback_data=f"ft_del:{tid}")])
    kb_rows.append([InlineKeyboardButton(text="Добавить тестовый IP", callback_data="ft_add")])
    kb_rows.append([InlineKeyboardButton(text="Изменить основные IP", callback_data="ft_main")])
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await call.answer()


@router.callback_query(F.data.startswith("ft_del:"))
async def cb_ft_del(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    tid = int(call.data.split(":")[1])
    await execute("DELETE FROM funtime_test_ips WHERE id=?", (tid,))
    await call.answer("Удалено")
    await adm_funtime(call)


@router.callback_query(F.data == "ft_add")
async def cb_ft_add(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await state.set_state(AdminFuntime.label)
    await call.message.answer("Метка (например 'TestServer'):", reply_markup=await cancel_kb("admin"))
    await call.answer()


@router.callback_query(F.data == "ft_main")
async def cb_ft_main(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await state.set_state(AdminText.waiting)
    await state.update_data(setting_key="funtime_main_ips")
    await call.message.answer("Введите основные IP через запятую (host:port,host:port):",
                              reply_markup=await cancel_kb("admin"))
    await call.answer()


@router.message(AdminFuntime.label)
async def ft_label(message: Message, state: FSMContext):
    await state.update_data(label=(message.text or "").strip())
    await state.set_state(AdminFuntime.ip)
    await message.answer("IP:port:", reply_markup=await cancel_kb("admin"))


@router.message(AdminFuntime.ip)
async def ft_ip(message: Message, state: FSMContext):
    d = await state.get_data()
    await execute("INSERT INTO funtime_test_ips(label, ip) VALUES(?,?)",
                  (d["label"], (message.text or "").strip()))
    await state.clear()
    await message.answer("✅ Добавлено", reply_markup=await admin_back_kb())


# ----- Rules edit -----

@router.callback_query(F.data == "adm:rules")
async def adm_rules(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await state.set_state(AdminRulesEdit.text)
    await call.message.answer(
        "Отправьте новый текст правил (HTML, поддерживаются <code>&lt;tg-emoji&gt;</code>):",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.message(AdminRulesEdit.text)
async def rules_save(message: Message, state: FSMContext):
    await set_setting("rules_text", message.html_text)
    await log_admin(message.from_user.id, "Изменил правила")
    await state.clear()
    await message.answer("✅ Сохранено", reply_markup=await admin_back_kb())


# ----- Photos for sections -----

PHOTO_KEYS = {
    "admin": "admin_photo",
    "menu": "menu_photo", "profile": "profile_photo", "earn": "earn_photo",
    "casino": "casino_photo", "funtime": "funtime_photo", "theft": "theft_photo",
    "leaderboard": "leaderboard_photo", "rules": "rules_photo",
    "promo": "promo_photo", "tasks": "tasks_photo",
    "op": "op_photo",
}
TEXT_KEYS = {
    "menu": "menu_text",
    "earn": "earn_text",
    "rules": "rules_text",
    "emoji_profile": "section_emoji_profile",
    "emoji_tasks": "section_emoji_tasks",
    "emoji_casino": "section_emoji_casino",
    "emoji_funtime": "section_emoji_funtime",
    "emoji_theft": "section_emoji_theft",
    "emoji_leaderboard": "section_emoji_leaderboard",
    "lb_place_1": "leaderboard_place_1_icon",
    "lb_place_2": "leaderboard_place_2_icon",
    "lb_place_3": "leaderboard_place_3_icon",
}
SECTION_LABELS = {
    "admin": "Админ-панель",
    "emoji_profile": "Эмодзи·Профиль",
    "emoji_tasks": "Эмодзи·Задания",
    "emoji_casino": "Эмодзи·Казино",
    "emoji_funtime": "Эмодзи·Фантайм",
    "emoji_theft": "Эмодзи·Кража",
    "emoji_leaderboard": "Эмодзи·Лидерборд",
    "lb_place_1": "Лидерборд·1 место",
    "lb_place_2": "Лидерборд·2 место",
    "lb_place_3": "Лидерборд·3 место",
    "menu": "Меню",
    "profile": "Профиль",
    "earn": "Заработок",
    "casino": "Казино",
    "funtime": "Фантайм",
    "theft": "Кража",
    "leaderboard": "Лидерборд",
    "rules": "Правила",
    "promo": "Промокод",
    "tasks": "Задания",
    "op": "ОП/подписка",
}


@router.callback_query(F.data == "adm:photos")
async def adm_photos(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    kb_rows = [[InlineKeyboardButton(
                    text=SECTION_LABELS.get(k, k),
                    callback_data=f"photo_set:{k}")]
               for k in PHOTO_KEYS]
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, "Выберите раздел для замены/удаления фото:", None,
                       reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await call.answer()


@router.callback_query(F.data.startswith("photo_set:"))
async def cb_photo_set(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    key = call.data.split(":")[1]
    await state.set_state(AdminPhoto.waiting)
    await state.update_data(setting_key=PHOTO_KEYS[key])
    await call.message.answer(
        f"Отправьте новое фото для раздела «{SECTION_LABELS.get(key, key)}» "
        "(или текст 'clear' чтобы очистить):",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.message(AdminPhoto.waiting)
async def photo_save(message: Message, state: FSMContext):
    d = await state.get_data()
    key = d["setting_key"]
    if message.text and message.text.strip().lower() == "clear":
        await set_setting(key, "")
        await state.clear()
        await message.answer("Очищено", reply_markup=await admin_back_kb()); return
    file_id = extract_photo_file_id(message)
    if not file_id:
        await message.answer("Это не фото, отправьте картинку или 'clear'"); return
    await set_setting(key, file_id)
    await log_admin(message.from_user.id, f"Изменил фото {key}")
    await state.clear()
    await message.answer("✅ Сохранено", reply_markup=await admin_back_kb())


# ----- Texts for sections -----

@router.callback_query(F.data == "adm:texts")
async def adm_texts(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    kb_rows = [[InlineKeyboardButton(
                    text=SECTION_LABELS.get(k, k),
                    callback_data=f"text_set:{k}")]
               for k in TEXT_KEYS]
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, "Выберите текст для изменения:", None,
                       reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await call.answer()


@router.callback_query(F.data.startswith("text_set:"))
async def cb_text_set(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    key = call.data.split(":")[1]
    await state.set_state(AdminText.waiting)
    await state.update_data(setting_key=TEXT_KEYS[key])
    await call.message.answer(
        "Отправьте новый текст (HTML, поддерживаются <code>&lt;tg-emoji&gt;</code>):",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.message(AdminText.waiting)
async def text_setting_save(message: Message, state: FSMContext):
    d = await state.get_data()
    key = d.get("setting_key")
    if key:
        await set_setting(key, message.html_text or message.text or "")
        await log_admin(message.from_user.id, f"Изменил текст {key}")
    await state.clear()
    await message.answer("✅ Сохранено", reply_markup=await admin_back_kb())


# ----- Button styles & premium icons (Bot API 9.4) -----

# Catalog of buttons that admin can re-style. Key = callback_data.
STYLEABLE_BUTTONS = [
    # User menu
    ("nav:profile", "Профиль"),
    ("nav:tasks", "Задания"),
    ("nav:earn", "Заработок"),
    ("nav:casino", "Казино"),
    ("nav:funtime", "Фантайм"),
    ("nav:theft", "Кража"),
    ("nav:lb", "Лидерборд"),
    ("nav:rules", "Правила"),
    ("nav:promo", "Промокод"),
    ("nav:bonus", "Бонус"),
    ("nav:withdraw", "Вывести"),
    ("nav:dice", "Кубик"),
    ("nav:basket", "Баскетбол"),
    ("nav:rob", "Ограбить"),
    ("nav:menu", "Назад"),
    ("op_channel_link", "Кнопки каналов"),
    # Admin
    ("adm:users", "Адм·Пользователи"),
    ("adm:stats", "Адм·Статистика"),
    ("adm:broadcast", "Адм·Рассылка"),
    ("adm:econ", "Адм·Экономика"),
    ("adm:channels", "Адм·Каналы"),
    ("adm:promo", "Адм·Промокоды"),
    ("adm:wd", "Адм·Заявки"),
    ("adm:refs", "Адм·Рефералы"),
    ("adm:logs", "Адм·Логи"),
    ("adm:protect", "Адм·Защита"),
    ("adm:events", "Адм·Ивенты"),
    ("adm:admins", "Адм·Админы"),
    ("adm:tasks", "Адм·Задания"),
    ("adm:shop", "Адм·Магазин"),
    ("adm:funtime", "Адм·Фантайм"),
    ("adm:rules", "Адм·Правила"),
    ("adm:photos", "Адм·Фото"),
    ("adm:texts", "Адм·Тексты"),
    ("adm:styles", "Адм·Стили"),
    ("adm:home", "В админ-панель"),
]
STYLE_LABELS = {"default": "по умолчанию", "primary": "🔵 primary",
                "success": "🟢 success", "danger": "🔴 danger"}


SIZE_LABELS = {"default": "auto", "full": "full row"}


async def render_styles_menu(call: CallbackQuery):
    kb_rows = []
    row = []
    for i, (cd, label) in enumerate(STYLEABLE_BUTTONS, 1):
        style = (await get_setting(f"btn_style:{cd}", "")) or "default"
        icon = "🖼" if (await get_setting(f"btn_icon:{cd}", "")) else ""
        size = (await get_setting(f"btn_size:{cd}", "default")) or "default"
        row.append(InlineKeyboardButton(
            text=f"{label} [{STYLE_LABELS.get(style, style)}|{SIZE_LABELS.get(size, size)}{icon}]",
            callback_data=f"style_pick:{cd}",
        ))
        if i % 2 == 0:
            kb_rows.append(row)
            row = []
    if row:
        kb_rows.append(row)
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(
        call,
        "🎛 <b>Стили, размеры и иконки кнопок</b>\n\nВыберите кнопку для настройки.",
        None,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows),
    )


async def render_style_detail(call: CallbackQuery, cd: str):
    cur_style = await get_setting(f"btn_style:{cd}", "") or "default"
    cur_icon = await get_setting(f"btn_icon:{cd}", "")
    cur_size = await get_setting(f"btn_size:{cd}", "default") or "default"
    label = next((lbl for k, lbl in STYLEABLE_BUTTONS if k == cd), cd)
    text = (
        f"Кнопка: <b>{label}</b>\n"
        f"Текущий стиль: <b>{STYLE_LABELS.get(cur_style, cur_style)}</b>\n"
        f"Текущая иконка: <code>{cur_icon or '—'}</code>\n"
        f"Размер: <b>{SIZE_LABELS.get(cur_size, cur_size)}</b>"
    )
    rows = [
        [InlineKeyboardButton(text=("• " if cur_style == s else "") + lbl,
                              callback_data=f"style_set:{cd}:{s}")]
        for s, lbl in STYLE_LABELS.items()
    ]
    rows.extend([
        [InlineKeyboardButton(text=("• " if cur_size == s else "") + f"Размер: {lbl}",
                              callback_data=f"size_set:{cd}:{s}")]
        for s, lbl in SIZE_LABELS.items()
    ])
    rows.append([InlineKeyboardButton(text="Задать премиум-иконку", callback_data=f"icon_set:{cd}")])
    if cur_icon:
        rows.append([InlineKeyboardButton(text="Убрать иконку", callback_data=f"icon_clr:{cd}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="adm:styles")])
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


@router.callback_query(F.data == "adm:styles")
async def adm_styles(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await render_styles_menu(call)
    await call.answer()
    return
    kb_rows = []
    row = []
    for i, (cd, label) in enumerate(STYLEABLE_BUTTONS, 1):
        st = (await get_setting(f"btn_style:{cd}", "")) or "—"
        ic = "🖼" if (await get_setting(f"btn_icon:{cd}", "")) else ""
        row.append(InlineKeyboardButton(
            text=f"{label} [{st}{ic}]",
            callback_data=f"style_pick:{cd}",
        ))
        if i % 2 == 0:
            kb_rows.append(row); row = []
    if row: kb_rows.append(row)
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    text = (
        "🎛 <b>Стиль и премиум-иконки кнопок</b>\n\n"
    )
    await send_section(call, text, None,
                       reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await call.answer()


@router.callback_query(F.data.startswith("style_pick:"))
async def cb_style_pick(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    cd = call.data.split(":", 1)[1]
    await render_style_detail(call, cd)
    await call.answer()
    return
    cur_style = await get_setting(f"btn_style:{cd}", "") or "default"
    cur_icon = await get_setting(f"btn_icon:{cd}", "")
    label = next((lbl for k, lbl in STYLEABLE_BUTTONS if k == cd), cd)
    text = (
        f"Кнопка: <b>{label}</b>\n"
        f"Текущий стиль: <b>{STYLE_LABELS.get(cur_style, cur_style)}</b>\n"
        f"Текущая иконка: <code>{cur_icon or '—'}</code>"
    )
    rows = [
        [InlineKeyboardButton(text=("• " if cur_style == s else "") + lbl,
                              callback_data=f"style_set:{cd}:{s}")]
        for s, lbl in STYLE_LABELS.items()
    ]
    rows.append([InlineKeyboardButton(text="Задать премиум-иконку",
                                      callback_data=f"icon_set:{cd}")])
    if cur_icon:
        rows.append([InlineKeyboardButton(text="Убрать иконку",
                                          callback_data=f"icon_clr:{cd}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="adm:styles")])
    await send_section(call, text, None,
                       reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await call.answer()


@router.callback_query(F.data.startswith("style_set:"))
async def cb_style_set(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    payload = call.data.removeprefix("style_set:")
    try:
        cd, s = payload.rsplit(":", 1)
    except ValueError:
        await call.answer("Некорректный стиль", show_alert=True)
        return
    if s not in STYLE_LABELS:
        await call.answer("Некорректный стиль", show_alert=True)
        return
    val = "" if s == "default" else s
    await set_setting(f"btn_style:{cd}", val)
    await log_admin(call.from_user.id, f"Стиль кнопки {cd} = {val or 'default'}")
    await call.answer(f"Стиль: {STYLE_LABELS.get(s, s)}")
    # re-render the pick screen
    await cb_style_pick_render(call, cd)


@router.callback_query(F.data.startswith("size_set:"))
async def cb_size_set(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    payload = call.data.removeprefix("size_set:")
    try:
        cd, size = payload.rsplit(":", 1)
    except ValueError:
        await call.answer("Некорректный размер", show_alert=True)
        return
    if size not in SIZE_LABELS:
        await call.answer("Некорректный размер", show_alert=True)
        return
    val = "default" if size == "default" else "full"
    await set_setting(f"btn_size:{cd}", val)
    await log_admin(call.from_user.id, f"Размер кнопки {cd} = {val}")
    await call.answer(f"Размер: {SIZE_LABELS.get(val, val)}")
    await cb_style_pick_render(call, cd)


@router.callback_query(F.data.startswith("icon_clr:"))
async def cb_icon_clr(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    cd = call.data.split(":", 1)[1]
    await set_setting(f"btn_icon:{cd}", "")
    await log_admin(call.from_user.id, f"Иконка кнопки {cd} удалена")
    await call.answer("Иконка убрана")
    await cb_style_pick_render(call, cd)


@router.callback_query(F.data.startswith("icon_set:"))
async def cb_icon_set(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    cd = call.data.split(":", 1)[1]
    await state.set_state(AdminBtnStyle.pick_icon)
    await state.update_data(target_cd=cd)
    await call.message.answer(
        "Отправьте один из вариантов:\n"
        "• <b>премиум-эмодзи</b> — бот возьмёт его <code>custom_emoji_id</code> "
        "и поставит как иконку кнопки;\n"
        "• <b>обычное эмодзи</b> (например 🔥) — бот добавит его в начало текста кнопки;\n"
        "• <b>число</b> — это будет воспринято как <code>custom_emoji_id</code>.",
        reply_markup=cancel_kb_inline_admin(),
    )
    await call.answer()


def cancel_kb_inline_admin():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Отмена", callback_data="cancel:admin")
    ]])


@router.message(AdminBtnStyle.pick_icon)
async def btn_icon_save(message: Message, state: FSMContext):
    data = await state.get_data()
    cd = data["target_cd"]
    stored = None
    human = ""
    # 1) Премиум-эмодзи (custom_emoji entity)
    if message.entities:
        for ent in message.entities:
            if ent.type == "custom_emoji" and getattr(ent, "custom_emoji_id", None):
                stored = f"id:{ent.custom_emoji_id}"
                human = f"премиум-иконка <code>{ent.custom_emoji_id}</code>"
                break
    text = (message.text or "").strip()
    # 2) Чистое число — трактуем как custom_emoji_id
    if not stored and text.isdigit():
        stored = f"id:{text}"
        human = f"премиум-иконка <code>{text}</code>"
    # 3) Обычное эмодзи / любой короткий символьный префикс
    if not stored and text:
        # Берём только первый «графический кластер» — обычно это и есть эмодзи.
        # Ограничим длину, чтобы кнопку не разнесло.
        prefix = text.split()[0][:8]
        stored = f"tx:{prefix}"
        human = f"эмодзи в тексте: {prefix}"
    if not stored:
        await message.answer("Не нашёл эмодзи в сообщении. Пришлите ещё раз или /cancel.")
        return
    await set_setting(f"btn_icon:{cd}", stored)
    await log_admin(message.from_user.id, f"Иконка кнопки {cd} = {stored}")
    await state.clear()
    await message.answer(f"✅ Сохранено: {human}",
                         reply_markup=await admin_back_kb_async())


async def admin_back_kb_async():
    return await admin_back_kb()


async def cb_style_pick_render(call: CallbackQuery, cd: str):
    await render_style_detail(call, cd)
    return
    cur_style = await get_setting(f"btn_style:{cd}", "") or "default"
    cur_icon = await get_setting(f"btn_icon:{cd}", "")
    label = next((lbl for k, lbl in STYLEABLE_BUTTONS if k == cd), cd)
    text = (
        f"Кнопка: <b>{label}</b>\n"
        f"Текущий стиль: <b>{STYLE_LABELS.get(cur_style, cur_style)}</b>\n"
        f"Текущая иконка: <code>{cur_icon or '—'}</code>"
    )
    rows = [
        [InlineKeyboardButton(text=("• " if cur_style == s else "") + lbl,
                              callback_data=f"style_set:{cd}:{s}")]
        for s, lbl in STYLE_LABELS.items()
    ]
    rows.append([InlineKeyboardButton(text="Задать премиум-иконку",
                                      callback_data=f"icon_set:{cd}")])
    if cur_icon:
        rows.append([InlineKeyboardButton(text="Убрать иконку",
                                          callback_data=f"icon_clr:{cd}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="adm:styles")])
    await send_section(call, text, None,
                       reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


