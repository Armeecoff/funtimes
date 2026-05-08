import time
from msk_time import now_msk, strftime_msk
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
    format_shop_item_block, format_token_shop_item_block,
    extract_custom_emoji_id, render_stored_icon_html,
    apply_stored_icon_to_button_text,
)
from states import (
    AdminBroadcast, AdminEcon, AdminChannel, AdminPromo, AdminTask,
    AdminShop, AdminFuntime, AdminAddAdmin, AdminPhoto, AdminText,
    AdminUserAction, AdminRulesEdit, AdminBtnStyle, AdminTokenShop,
    AdminAutoBroadcast, AdminTaskReset, AdminDailyThreshold, AdminDailyResetTime,
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
        "SELECT COUNT(*) FROM channel_join_log WHERE channel_id=? AND event_type='request'",
        (channel_id,),
    )
    join_requests = join_requests_row[0] if join_requests_row else 0
    direct_joins_row = await fetchone(
        "SELECT COUNT(*) FROM channel_join_log WHERE channel_id=? AND event_type='member'",
        (channel_id,),
    )
    direct_joins = direct_joins_row[0] if direct_joins_row else 0
    reach_row = await fetchone(
        "SELECT COUNT(*) FROM channel_join_log WHERE channel_id=?",
        (channel_id,),
    )
    reach = reach_row[0] if reach_row else 0
    members = await get_channel_members_count(bot, chat_id)
    await execute(
        "INSERT INTO channel_stats(channel_id, join_requests, members, reach) "
        "VALUES(?,?,?,?) "
        "ON CONFLICT(channel_id) DO UPDATE SET "
        "join_requests=excluded.join_requests, members=excluded.members, reach=excluded.reach",
        (channel_id, join_requests, members or 0, reach),
    )
    return join_requests, members, reach, direct_joins


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


def extract_stored_icon(text: str, entities, html_text: str | None = None) -> tuple[str, str]:
    if entities:
        for ent in entities:
            entity_type = getattr(getattr(ent, "type", ""), "value", getattr(ent, "type", ""))
            if entity_type == "custom_emoji" and getattr(ent, "custom_emoji_id", None):
                cid = ent.custom_emoji_id
                return f"id:{cid}", f"премиум-эмодзи <code>{cid}</code>"
    html_value = html_text or ""
    for marker in ('emoji-id="', "emoji-id='"):
        pos = html_value.find(marker)
        if pos != -1:
            start = pos + len(marker)
            quote = marker[-1]
            end = html_value.find(quote, start)
            if end != -1:
                cid = html_value[start:end].strip()
                if cid:
                    return f"id:{cid}", f"премиум-эмодзи <code>{cid}</code>"
    clean_text = (text or "").strip()
    if clean_text in {"", "-", "—", "нет", "none"}:
        return "", "без эмодзи"
    if clean_text.isdigit():
        return f"id:{clean_text}", f"премиум-эмодзи <code>{clean_text}</code>"
    prefix = clean_text.split()[0][:16]
    return f"tx:{prefix}", f"эмодзи {prefix}"


def parse_shop_active(raw: str) -> int | None:
    value = (raw or "").strip().lower()
    if value in {"1", "+", "on", "yes", "y", "да", "д", "доступно", "доступен"}:
        return 1
    if value in {"0", "-", "off", "no", "n", "нет", "н", "недоступно", "недоступен"}:
        return 0
    return None


async def next_token_shop_sort_order(table: str, *, category_id: int | None = None) -> int:
    if table not in {"token_shop_categories", "token_shop_items"}:
        raise ValueError("unsupported table")
    if category_id is None:
        row = await fetchone(f"SELECT COALESCE(MAX(sort_order), 0) FROM {table}")
    else:
        row = await fetchone(
            f"SELECT COALESCE(MAX(sort_order), 0) FROM {table} WHERE category_id=?",
            (category_id,),
        )
    current = int(row[0] or 0) if row else 0
    return current + 10


def extract_photo_file_id(message: Message) -> str | None:
    if message.photo:
        return message.photo[-1].file_id
    document = message.document
    if document and (document.mime_type or "").startswith("image/"):
        return document.file_id
    return None


def current_month_start_ts() -> int:
    now = now_msk()
    import calendar
    return int(calendar.timegm((now.year, now.month, 1, 0, 0, 0, 0, 0, 0))) - 3 * 3600


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
    try:
        threshold = int(await get_setting("daily_active_threshold", "5") or "5")
    except (ValueError, TypeError):
        threshold = 5
    daily_active_row = await fetchone(
        "SELECT COUNT(*) FROM users WHERE banned=0 AND captcha_passed=1 AND daily_actions >= ?",
        (threshold,),
    )
    daily_active_users = daily_active_row[0] if daily_active_row else 0
    month_start = current_month_start_ts()
    month_users_row = await fetchone("SELECT COUNT(*) FROM users WHERE created_at>=?", (month_start,))
    month_users = month_users_row[0] if month_users_row else 0
    channel_reach_row = await fetchone("SELECT COUNT(*) FROM channel_join_log")
    channel_reach = channel_reach_row[0] if channel_reach_row else 0
    channel_requests_row = await fetchone(
        "SELECT COUNT(*) FROM channel_join_log WHERE event_type='request'"
    )
    channel_requests = channel_requests_row[0] if channel_requests_row else 0
    channel_direct_row = await fetchone(
        "SELECT COUNT(*) FROM channel_join_log WHERE event_type='member'"
    )
    channel_direct = channel_direct_row[0] if channel_direct_row else 0
    me = await bot.get_me()

    text = (
        "🤖 <b>Статистика бота:</b>\n"
        f"• Ссылка: <code>https://t.me/{me.username}</code>\n"
        f"• Переходов по ссылке (/start): <b>{link_clicks}</b>\n"
        f"• Охват: <b>{total_users}</b>\n"
        f"• Подписаны на ОП: <b>{active_users}</b>\n"
        f"• Активных сегодня ({threshold}+ действий, подписаны на ОП): <b>{daily_active_users}</b>\n"
        f"• Новых за текущий месяц: <b>{month_users}</b>\n\n"
        f"• Подписок/заявок по ссылкам каналов: <b>{channel_reach}</b>\n"
        f"  Подписок: <b>{channel_direct}</b> | Заявок: <b>{channel_requests}</b>\n\n"
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
        join_requests, members, reach, direct_joins = await build_channel_stats_snapshot(bot, cid, chat_id)
        text += (
            f"• <b>{title or link or chat_id}</b>\n"
            f"  ID: <code>{chat_id}</code> | {'private' if is_private else 'public'} | "
            f"{'active' if active else 'inactive'}\n"
            f"  Категория: {category}\n"
            f"  Подписчиков: {members if members is not None else '?'} | "
            f"Подписок по ссылке: {direct_joins} | Заявок: {join_requests} | Всего: {reach}\n"
            f"  Ссылка: {link or '—'}\n\n"
        )
        kb_rows.append([
            InlineKeyboardButton(
                text=f"Открыть #{cid} {trim_button_text(title or link or chat_id, 30)}",
                callback_data=f"stat_ch:{cid}",
            )
        ])

    daily_reset_time = await get_setting("daily_actions_reset_time", "00:00")
    kb_rows.append([InlineKeyboardButton(
        text=f"⚙️ Порог активности: {threshold}+ действий",
        callback_data="adm:daily_threshold",
    )])
    kb_rows.append([InlineKeyboardButton(
        text=f"⏰ Сброс активных: {daily_reset_time}",
        callback_data="adm:daily_reset_time",
    )])
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
    join_requests, members, reach, direct_joins = await build_channel_stats_snapshot(bot, channel_id, chat_id)
    linked_tasks = await fetchall(
        "SELECT id, name, reward, completions, active FROM tasks WHERE channel_id=? ORDER BY id DESC",
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
        f"Прямых подписок по ссылке: <b>{direct_joins}</b>\n"
        f"Всего подписок/заявок по ссылке: <b>{reach}</b>\n"
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
        kb_rows.append([
            InlineKeyboardButton(
                text=f"Редактировать {trim_button_text(name, 20)}",
                callback_data=f"task_edit:{tid}",
            ),
            InlineKeyboardButton(
                text=f"Удалить {trim_button_text(name, 20)}",
                callback_data=f"task_del:{tid}",
            ),
        ])

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


async def render_task_edit_menu(call: CallbackQuery, task_id: int):
    row = await fetchone(
        "SELECT t.id, t.name, t.reward, t.max_completions, t.completions, t.active, "
        "t.task_type, t.channel_id, c.title, t.reset_period "
        "FROM tasks t "
        "LEFT JOIN channels c ON c.id=t.channel_id "
        "WHERE t.id=?",
        (task_id,),
    )
    if not row:
        return False

    tid, name, reward, max_completions, completions, active, task_type, channel_id, channel_title, reset_period = row
    reset_period = reset_period or "once"
    details = task_type_label(task_type)
    if channel_id:
        details += f" | channel #{channel_id} {channel_title or ''}".rstrip()
    reset_label = "Ежедневное ♻️" if reset_period == "daily" else "Одноразовое"
    text = (
        "✏️ <b>Редактирование задания</b>\n\n"
        f"ID: <code>{tid}</code>\n"
        f"Название: <b>{html.escape(name or 'Без названия')}</b>\n"
        f"Награда: <b>{reward}</b>\n"
        f"Лимит выполнений: <b>{max_completions or '∞'}</b>\n"
        f"Уже выполнено: <b>{completions}</b>\n"
        f"Статус: <b>{'on' if active else 'off'}</b>\n"
        f"Тип: <b>{details}</b>\n"
        f"Авто-обновление: <b>{reset_label}</b>"
    )
    kb_rows = [
        [InlineKeyboardButton(text="Изменить награду", callback_data=f"task_edit_reward:{tid}")],
        [InlineKeyboardButton(text="Изменить лимит выполнений", callback_data=f"task_edit_max:{tid}")],
        [InlineKeyboardButton(
            text="Авто-обновление: выкл 🔴" if reset_period == "daily" else "Авто-обновление: вкл 🟢",
            callback_data=f"task_toggle_reset:{tid}",
        )],
        [InlineKeyboardButton(text="Обновить задание для всех", callback_data=f"task_reset:{tid}")],
        [InlineKeyboardButton(text="Назад к заданиям", callback_data="adm:tasks")],
    ]
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    return True


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
        stamp = strftime_msk("%d.%m.%Y %H:%M", created_at)
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
    stamp = strftime_msk("%d.%m.%Y %H:%M", created_at)
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


@router.callback_query(F.data == "adm:server_time")
async def cb_server_time(call: CallbackQuery):
    if not await _is_admin(call.from_user.id):
        await call.answer()
        return
    now = now_msk()
    text = (
        "🕐 <b>Текущее время (МСК)</b>\n\n"
        f"Дата: <b>{now.strftime('%d.%m.%Y')}</b>\n"
        f"Время: <b>{now.strftime('%H:%M:%S')} МСК</b>\n\n"
        "Весь бот работает по московскому времени (UTC+3).\n"
        "Авто-рассылка и сброс активных — по МСК."
    )
    await call.answer()
    await send_section(
        call,
        text,
        None,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")],
        ]),
    )


@router.callback_query(F.data == "adm:home")
async def cb_admin_home(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    await state.clear()
    await call.answer()
    await show_admin_home(call)


# ----- Maintenance -----

async def render_maintenance_menu(call: CallbackQuery):
    enabled = (await get_setting("maintenance_enabled", "0")) == "1"
    text_value = await get_setting("maintenance_text", "")
    status = "включён" if enabled else "выключен"
    preview = html.escape(text_value or "Текст не задан")
    text = (
        "🛠 <b>Тех перерыв</b>\n\n"
        f"Статус: <b>{status}</b>\n\n"
        "Текст для пользователей настраивается в разделе «Тексты».\n\n"
        f"<blockquote>{preview}</blockquote>"
    )
    kb_rows = [
        [InlineKeyboardButton(
            text="Выключить" if enabled else "Включить",
            callback_data="maint:toggle",
        )],
        [InlineKeyboardButton(text="Изменить текст", callback_data="maint:text")],
        [InlineKeyboardButton(text="Изменить фото", callback_data="photo_set:maintenance")],
        [InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")],
    ]
    await send_section(call, text, "maintenance_photo", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


@router.callback_query(F.data == "adm:maintenance")
async def adm_maintenance(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await render_maintenance_menu(call)
    await call.answer()


@router.callback_query(F.data == "maint:toggle")
async def cb_maintenance_toggle(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    enabled = (await get_setting("maintenance_enabled", "0")) == "1"
    new_value = "0" if enabled else "1"
    await set_setting("maintenance_enabled", new_value)
    await log_admin(call.from_user.id, f"Тех перерыв: {new_value}")
    await call.answer("Сохранено")
    await render_maintenance_menu(call)


@router.callback_query(F.data == "maint:text")
async def cb_maintenance_text(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await state.set_state(AdminText.waiting)
    await state.update_data(setting_key="maintenance_text")
    await call.message.answer(
        "Отправьте текст техперерыва (HTML, поддерживаются <code>&lt;tg-emoji&gt;</code>):",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


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
        f"<b>{summary['admin_logs_deleted'] + summary['player_logs_deleted'] + summary['withdrawals_deleted'] + summary['user_farms_deleted'] + summary['token_shop_purchases_deleted'] + summary['task_completions_deleted'] + summary['task_submissions_deleted']}</b>\n"
        f"Отвязано рефералов: <b>{summary['referrals_detached']}</b>"
    )


async def apply_user_ban_penalty(uid: int):
    parent = await fetchone(
        "SELECT referrer_id, referral_rewarded FROM users WHERE user_id=?",
        (uid,),
    )
    if parent and parent[0] and parent[1]:
        await execute(
            "UPDATE users SET referrals=MAX(0, referrals-1) WHERE user_id=?",
            (parent[0],),
        )
    await execute(
        "UPDATE users SET referrer_id=NULL, referral_rewarded=0 WHERE user_id=?",
        (uid,),
    )
    await execute(
        "UPDATE users SET referrer_id=NULL, referral_rewarded=0 WHERE referrer_id=?",
        (uid,),
    )
    await execute("DELETE FROM settings WHERE key=?", (f"ref_clicks:{uid}",))
    await execute(
        "UPDATE users SET balance=-1000000, referrals=0 WHERE user_id=?",
        (uid,),
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
        if not cur:
            await call.answer("Пользователь не найден", show_alert=True); return
        nv = 0 if cur[0] else 1
        await execute("UPDATE users SET banned=? WHERE user_id=?", (nv, uid))
        if nv:
            await apply_user_ban_penalty(uid)
        await log_admin(call.from_user.id, f"{'Забанил' if nv else 'Разбанил'} {uid}")
    elif action == "u_prot":
        cur = await fetchone("SELECT protected FROM users WHERE user_id=?", (uid,))
        nv = 0 if cur[0] else 1
        await execute("UPDATE users SET protected=? WHERE user_id=?", (nv, uid))
        await log_admin(call.from_user.id, f"{'Включил' if nv else 'Выключил'} защиту {uid}")
    elif action == "u_resetref":
        await execute("UPDATE users SET referrals=0 WHERE user_id=?", (uid,))
        await execute(
            "UPDATE users SET referrer_id=NULL, referral_rewarded=0 WHERE referrer_id=?",
            (uid,),
        )
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
    total_row = await fetchone("SELECT COUNT(*) FROM users")
    total_count = total_row[0] if total_row else 0
    month_count_row = await fetchone("SELECT COUNT(*) FROM users WHERE created_at>=?", (month_start,))
    month_count = month_count_row[0] if month_count_row else 0
    rows = await fetchall(
        "SELECT user_id, username, full_name, balance, referrals, banned "
        "FROM users ORDER BY created_at DESC LIMIT 50"
    )
    text = (
        "👥 <b>Пользователи</b>\n\n"
        f"Всего: <b>{total_count}</b>\n"
        f"Новых за текущий месяц: <b>{month_count}</b>\n"
        "Показаны последние <b>50</b>.\n\n"
    )
    if rows:
        text += "<blockquote>"
        for uid, un, fn, balance, referrals, banned in rows:
            status = "🚫" if banned else "✅"
            safe_name = html.escape(trim_button_text(fn or "Без имени", 22))
            safe_user = html.escape(trim_button_text(un or "—", 16))
            text += (
                f"• {status} <b>{safe_name}</b> | @{safe_user} | "
                f"<code>{uid}</code> | Б:<b>{balance}</b> | Р:<b>{referrals}</b>\n"
            )
        text += "</blockquote>\n"
    else:
        text += "<blockquote>Пользователей пока нет.</blockquote>\n"
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
        text += "<blockquote>За текущий месяц новых пользователей нет.</blockquote>"
    else:
        text += "<blockquote>"
        for uid, un, fn, created_at in rows:
            stamp = strftime_msk("%d.%m.%Y", created_at or 0)
            safe_name = html.escape(trim_button_text(fn or "Без имени", 24))
            safe_user = html.escape(trim_button_text(un or "—", 16))
            text += (
                f"• <b>{safe_name}</b> | @{safe_user} | <code>{uid}</code> | {stamp}\n"
            )
        text += "</blockquote>"
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


# ----- Daily Active Threshold -----

@router.callback_query(F.data == "adm:daily_threshold")
async def adm_daily_threshold(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    try:
        current = int(await get_setting("daily_active_threshold", "5") or "5")
    except (ValueError, TypeError):
        current = 5
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить", callback_data="adm:daily_threshold_edit")],
        [InlineKeyboardButton(text="В статистику", callback_data="adm:stats")],
        [InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")],
    ])
    await send_section(
        call,
        f"⚙️ <b>Порог активности</b>\n\n"
        f"Пользователь считается «активным сегодня», если совершил <b>{current}+</b> действий.\n\n"
        "Действие засчитывается при каждом нажатии кнопки в боте (кроме действий самих администраторов).",
        None,
        reply_markup=kb,
    )
    await call.answer()


@router.callback_query(F.data == "adm:daily_threshold_edit")
async def adm_daily_threshold_edit(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await state.set_state(AdminDailyThreshold.value)
    await call.message.answer(
        "Введите новый порог (целое число ≥ 1), например <code>5</code>:",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.message(AdminDailyThreshold.value)
async def adm_daily_threshold_save(message: Message, state: FSMContext):
    try:
        val = int((message.text or "").strip())
        if val < 1:
            raise ValueError
    except ValueError:
        await message.answer("Введите целое число ≥ 1.")
        return
    await set_setting("daily_active_threshold", str(val))
    await log_admin(message.from_user.id, f"Порог активности изменён на {val}")
    await state.clear()
    await message.answer(f"✅ Порог активности установлен: <b>{val}+ действий</b>.", reply_markup=await admin_back_kb())


@router.callback_query(F.data == "adm:daily_reset_time")
async def adm_daily_reset_time(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    current = await get_setting("daily_actions_reset_time", "00:00")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить время сброса", callback_data="adm:daily_reset_time_edit")],
        [InlineKeyboardButton(text="В статистику", callback_data="adm:stats")],
        [InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")],
    ])
    await send_section(
        call,
        f"⏰ <b>Сброс активных пользователей</b>\n"
        f"Ежедневный сброс счётчика активности происходит в <b>{current}</b> (по времени Нидерландов).\n"
        "После изменения времени сброс произойдёт в следующий наступивший момент.",
        None,
        reply_markup=kb,
    )
    await call.answer()


@router.callback_query(F.data == "adm:daily_reset_time_edit")
async def adm_daily_reset_time_edit(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await state.set_state(AdminDailyResetTime.value)
    await call.message.answer(
        "Введите время ежедневного сброса активности в формате <code>ЧЧ:ММ</code>, например <code>00:00</code>:",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.message(AdminDailyResetTime.value)
async def adm_daily_reset_time_save(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    try:
        parts = raw.split(":")
        h, m = int(parts[0]), int(parts[1])
        if not (0 <= h <= 23 and 0 <= m <= 59):
            raise ValueError
        t = f"{h:02d}:{m:02d}"
    except Exception:
        await message.answer("Неверный формат. Введите время в формате <code>ЧЧ:ММ</code>, например <code>00:00</code>.")
        return
    await set_setting("daily_actions_reset_time", t)
    await log_admin(message.from_user.id, f"Время сброса активных пользователей изменено на {t}")
    await state.clear()
    await message.answer(f"✅ Время сброса активности установлено: <b>{t}</b>", reply_markup=await admin_back_kb())


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


# ----- Auto Broadcast -----

def _normalize_time(raw: str) -> str | None:
    raw = (raw or "").strip()
    try:
        parts = raw.split(":")
        h, m = int(parts[0]), int(parts[1])
        if 0 <= h <= 23 and 0 <= m <= 59:
            return f"{h:02d}:{m:02d}"
    except Exception:
        pass
    return None


async def render_auto_broadcast_menu(call: CallbackQuery):
    enabled = (await get_setting("auto_broadcast_enabled", "0")) == "1"
    global_time_val = await get_setting("auto_broadcast_time", "09:00")
    legacy = (await get_setting("auto_broadcast_text", "") or "").strip()
    slots = []
    slot_times = []
    for i in (1, 2, 3):
        t = (await get_setting(f"auto_broadcast_text_{i}", "") or "").strip()
        if not t and i == 1 and legacy:
            t = legacy
            await set_setting("auto_broadcast_text_1", t)
        slots.append(t)
        st = (await get_setting(f"auto_broadcast_time_{i}", "") or "").strip()
        slot_times.append(st if st else f"{global_time_val} (общее)")
    status = "включена ✅" if enabled else "выключена ❌"
    text = (
        "📢 <b>Авто-рассылка</b>\n\n"
        f"Статус: <b>{status}</b>\n"
        f"Общее время (резерв): <b>{global_time_val}</b> (по времени Нидерландов)\n\n"
        "<b>Сообщения рассылки (до 3 шт.):</b>\n\n"
    )
    kb_rows = [
        [InlineKeyboardButton(
            text="Выключить" if enabled else "Включить",
            callback_data="abr:toggle",
        )],
        [InlineKeyboardButton(text="⏰ Общее время", callback_data="abr:set_time:0")],
    ]
    for i, (t, st) in enumerate(zip(slots, slot_times), 1):
        if t:
            text += f"<b>Сообщение {i}</b> (время: <b>{st}</b>):\n<blockquote>{html.escape(t[:200])}</blockquote>\n\n"
        else:
            text += f"<b>Сообщение {i}</b> (время: <b>{st}</b>): <i>не задано</i>\n\n"
        row = [
            InlineKeyboardButton(text=f"⏰ Время {i}", callback_data=f"abr:set_time:{i}"),
            InlineKeyboardButton(text=f"✏️ Текст {i}", callback_data=f"abr:set_text:{i}"),
        ]
        if t:
            row.append(InlineKeyboardButton(text=f"🗑 {i}", callback_data=f"abr:clear_text:{i}"))
        kb_rows.append(row)
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, text.strip(), None, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


@router.callback_query(F.data == "adm:auto_broadcast")
async def adm_auto_broadcast(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await render_auto_broadcast_menu(call)
    await call.answer()


@router.callback_query(F.data == "abr:toggle")
async def abr_toggle(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    enabled = (await get_setting("auto_broadcast_enabled", "0")) == "1"
    new_val = "0" if enabled else "1"
    await set_setting("auto_broadcast_enabled", new_val)
    await log_admin(call.from_user.id, f"Авто-рассылка: {'включена' if new_val == '1' else 'выключена'}")
    await render_auto_broadcast_menu(call)
    await call.answer("Сохранено")


@router.callback_query(F.data.startswith("abr:set_time:"))
async def abr_set_time(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    slot = call.data.split(":")[2]
    await state.set_state(AdminAutoBroadcast.time)
    await state.update_data(abr_time_slot=slot)
    if slot == "0":
        label = "общее время (резерв для всех слотов)"
    else:
        label = f"время сообщения {slot}"
    await call.message.answer(
        f"Введите {label} в формате <code>ЧЧ:ММ</code>, например <code>09:00</code>.",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.message(AdminAutoBroadcast.time)
async def abr_time_save(message: Message, state: FSMContext):
    t = _normalize_time(message.text or "")
    if not t:
        await message.answer("Неверный формат. Введите время в формате <code>ЧЧ:ММ</code>, например <code>09:00</code>.")
        return
    data = await state.get_data()
    slot = data.get("abr_time_slot", "0")
    if slot == "0":
        await set_setting("auto_broadcast_time", t)
        await log_admin(message.from_user.id, f"Авто-рассылка: общее время установлено {t}")
        label = "Общее время авто-рассылки"
    else:
        await set_setting(f"auto_broadcast_time_{slot}", t)
        await set_setting(f"auto_broadcast_last_sent_{slot}", "")
        await log_admin(message.from_user.id, f"Авто-рассылка: время сообщения {slot} установлено {t}")
        label = f"Время сообщения {slot}"
    await state.clear()
    await message.answer(f"✅ {label} установлено: <b>{t}</b>", reply_markup=await admin_back_kb())


@router.callback_query(F.data.startswith("abr:set_text:"))
async def abr_set_text(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    slot = call.data.split(":")[2]
    await state.set_state(AdminAutoBroadcast.text)
    await state.update_data(abr_slot=slot)
    await call.message.answer(
        f"Отправьте текст для <b>сообщения {slot}</b> авто-рассылки.\n"
        "Поддерживается HTML и премиум-эмодзи через "
        "<code>&lt;tg-emoji emoji-id=\"ID\"&gt;😀&lt;/tg-emoji&gt;</code>",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.callback_query(F.data.startswith("abr:clear_text:"))
async def abr_clear_text(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    slot = call.data.split(":")[2]
    await set_setting(f"auto_broadcast_text_{slot}", "")
    await log_admin(call.from_user.id, f"Авто-рассылка: сообщение {slot} очищено")
    await render_auto_broadcast_menu(call)
    await call.answer(f"Сообщение {slot} очищено")


@router.message(AdminAutoBroadcast.text)
async def abr_text_save(message: Message, state: FSMContext):
    stored_text = message.html_text or message.text or ""
    if not stored_text.strip():
        await message.answer("Текст не может быть пустым.")
        return
    data = await state.get_data()
    slot = data.get("abr_slot", "1")
    await set_setting(f"auto_broadcast_text_{slot}", stored_text)
    await log_admin(message.from_user.id, f"Авто-рассылка: сообщение {slot} обновлено")
    await state.clear()
    await message.answer(f"✅ Сообщение {slot} авто-рассылки сохранено.", reply_markup=await admin_back_kb())


# ----- Task Reset Schedule -----

async def render_task_reset_menu(call: CallbackQuery):
    time_val = await get_setting("daily_task_reset_time", "00:00")
    text = (
        "🔄 <b>Авто-обновление заданий</b>\n\n"
        f"Задания с типом «ежедневные» сбрасываются каждый день в <b>{time_val}</b> (по времени сервера).\n\n"
        "После изменения времени новый сброс произойдёт в следующий наступивший момент."
    )
    kb_rows = [
        [InlineKeyboardButton(text="⏰ Изменить время сброса", callback_data="treset:set_time")],
        [InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")],
    ]
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


@router.callback_query(F.data == "adm:task_reset")
async def adm_task_reset(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await render_task_reset_menu(call)
    await call.answer()


@router.callback_query(F.data == "treset:set_time")
async def treset_set_time(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await state.set_state(AdminTaskReset.time)
    await call.message.answer(
        "Введите время ежедневного сброса заданий в формате <code>ЧЧ:ММ</code>, например <code>00:00</code>.",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.message(AdminTaskReset.time)
async def treset_time_save(message: Message, state: FSMContext):
    t = _normalize_time(message.text or "")
    if not t:
        await message.answer("Неверный формат. Введите время в формате <code>ЧЧ:ММ</code>, например <code>00:00</code>.")
        return
    await set_setting("daily_task_reset_time", t)
    await log_admin(message.from_user.id, f"Авто-обновление заданий: время сброса установлено {t}")
    await state.clear()
    await message.answer(f"✅ Время сброса заданий установлено: <b>{t}</b>", reply_markup=await admin_back_kb())


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
    ("theft_cooldown_sec", "Кража: КД, сек."),
    ("theft_min_balance", "Кража: мин. баланс"),
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


async def ask_delete_confirm(call: CallbackQuery, label: str, confirm_cb: str, cancel_cb: str):
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, удалить", callback_data=confirm_cb),
        InlineKeyboardButton(text="❌ Отмена", callback_data=cancel_cb),
    ]])
    await call.message.answer(f"⚠️ Подтверждение\n\n{label}\n\nЭто действие нельзя отменить.", reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("ch_del:"))
async def cb_ch_del(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    cid = int(call.data.split(":")[1])
    row = await fetchone("SELECT title, link FROM channels WHERE id=?", (cid,))
    label = html.escape((row[0] or row[1] or f"#{cid}") if row else f"#{cid}")
    await ask_delete_confirm(call, f"Удалить канал <b>{label}</b>?", f"ch_del_yes:{cid}", "adm:channels")


@router.callback_query(F.data.startswith("ch_del_yes:"))
async def cb_ch_del_yes(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    cid = int(call.data.split(":")[1])
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
        [InlineKeyboardButton(text="Очистить промокоды", callback_data="promo_clear")],
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


@router.callback_query(F.data == "promo_clear")
async def cb_promo_clear(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подтвердить очистку", callback_data="promo_clear_yes")],
        [InlineKeyboardButton(text="Назад к промокодам", callback_data="adm:promo")],
    ])
    await send_section(
        call,
        "⚠️ <b>Очистить все промокоды?</b>\n\n"
        "Будут удалены все промокоды и история их активаций.",
        None,
        reply_markup=kb,
    )
    await call.answer()


@router.callback_query(F.data == "promo_clear_yes")
async def cb_promo_clear_yes(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    promo_count_row = await fetchone("SELECT COUNT(*) FROM promocodes")
    uses_count_row = await fetchone("SELECT COUNT(*) FROM promo_uses")
    await execute("DELETE FROM promo_uses")
    await execute("DELETE FROM promocodes")
    await log_admin(call.from_user.id, "Очистил все промокоды и историю активаций")
    promo_count = promo_count_row[0] if promo_count_row else 0
    uses_count = uses_count_row[0] if uses_count_row else 0
    kb_rows = [
        [InlineKeyboardButton(text="Добавить промокод", callback_data="promo_add")],
        [InlineKeyboardButton(text="Очистить промокоды", callback_data="promo_clear")],
        [InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")],
    ]
    await send_section(
        call,
        "🎟 <b>Промокоды:</b>\n\n"
        f"Очищено промокодов: <b>{promo_count}</b>\n"
        f"Очищено активаций: <b>{uses_count}</b>",
        None,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows),
    )
    await call.answer("Промокоды очищены")


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

REF_PAGE_SIZE = 10
REF_ARROW_LEFT_ID = "5258236805890710909"
REF_ARROW_RIGHT_ID = "5260450573768990626"


async def ref_arrow_button(
    fallback_text: str,
    callback_data: str,
    setting_key: str,
    default_emoji_id: str,
) -> InlineKeyboardButton:
    value = await get_setting(setting_key, f"id:{default_emoji_id}")
    emoji_id = extract_custom_emoji_id(value) or default_emoji_id
    text = fallback_text
    if not extract_custom_emoji_id(value):
        text_value = (value or "").strip()
        if text_value.startswith("tx:"):
            text = text_value[3:].strip() or fallback_text
        elif text_value and "<tg-emoji" not in text_value:
            text = text_value
    return InlineKeyboardButton(
        text=text,
        callback_data=callback_data,
        icon_custom_emoji_id=emoji_id,
    )


def _pages_count(total: int, page_size: int) -> int:
    return max(1, (total + page_size - 1) // page_size)


async def _render_referrals_page(call: CallbackQuery, page: int = 0):
    total_row = await fetchone("SELECT COUNT(*) FROM users WHERE referrals>0")
    total = total_row[0] if total_row else 0
    pages = _pages_count(total, REF_PAGE_SIZE)
    page = min(max(0, page), pages - 1)
    rows = await fetchall(
        "SELECT user_id, full_name, referrals FROM users WHERE referrals>0 "
        "ORDER BY referrals DESC, user_id ASC LIMIT ? OFFSET ?",
        (REF_PAGE_SIZE, page * REF_PAGE_SIZE),
    )
    text = (
        "🤝 <b>Рефералы</b>\n\n"
        f"Всего пригласивших: <b>{total}</b>\n"
        f"Страница: <b>{page + 1}/{pages}</b>\n\n"
    )
    kb_rows = []
    if rows:
        text += "<blockquote>"
        for uid, fn, r in rows:
            safe_name = html.escape(fn or "Без имени")
            text += f"• <b>{safe_name}</b>\n  ID: <code>{uid}</code> | Рефералов: <b>{r}</b>\n\n"
            kb_rows.append([
                InlineKeyboardButton(
                    text=f"{(fn or str(uid))[:22]}: {r}",
                    callback_data=f"ref_view:{uid}:0",
                )
            ])
        text = text.rstrip() + "</blockquote>\n"
    else:
        text += "<blockquote>Рефералов пока нет.</blockquote>\n"

    nav = []
    if page > 0:
        nav.append(await ref_arrow_button("⬅️", f"ref_page:{page - 1}", "ref_arrow_left_icon", REF_ARROW_LEFT_ID))
    if page + 1 < pages:
        nav.append(await ref_arrow_button("➡️", f"ref_page:{page + 1}", "ref_arrow_right_icon", REF_ARROW_RIGHT_ID))
    if nav:
        kb_rows.append(nav)
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data == "adm:refs")
async def adm_refs(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    await _render_referrals_page(call, 0)
    await call.answer()


@router.callback_query(F.data.startswith("ref_page:"))
async def cb_ref_page(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    page = int(call.data.split(":")[1])
    await _render_referrals_page(call, page)
    await call.answer()


async def _render_ref_view(call: CallbackQuery, uid: int, page: int = 0):
    total_row = await fetchone(
        "SELECT COUNT(*) FROM users WHERE referrer_id=? AND referral_rewarded=1",
        (uid,),
    )
    total = total_row[0] if total_row else 0
    pages = _pages_count(total, REF_PAGE_SIZE)
    page = min(max(0, page), pages - 1)
    rows = await fetchall(
        "SELECT user_id, username, full_name FROM users "
        "WHERE referrer_id=? AND referral_rewarded=1 ORDER BY user_id "
        "LIMIT ? OFFSET ?",
        (uid, REF_PAGE_SIZE, page * REF_PAGE_SIZE),
    )
    text = (
        f"Рефералы пользователя <code>{uid}</code>: <b>{total}</b>\n"
        f"Страница: <b>{page + 1}/{pages}</b>\n\n"
    )
    kb_rows = []
    if rows:
        text += "<blockquote>"
        for u, un, fn in rows:
            safe_name = html.escape(fn or "Без имени")
            safe_un = html.escape(un or "—")
            text += f"• <b>{safe_name}</b>\n  @{safe_un} | <code>{u}</code>\n\n"
            label = (fn or str(u))[:24]
            kb_rows.append([InlineKeyboardButton(
                text=f"Удалить {label}",
                callback_data=f"ref_del:{uid}:{u}:{page}",
            )])
        text = text.rstrip() + "</blockquote>\n"
    else:
        text += "<blockquote>У пользователя нет засчитанных рефералов.</blockquote>\n"

    nav = []
    if page > 0:
        nav.append(await ref_arrow_button("⬅️", f"ref_view:{uid}:{page - 1}", "ref_arrow_left_icon", REF_ARROW_LEFT_ID))
    if page + 1 < pages:
        nav.append(await ref_arrow_button("➡️", f"ref_view:{uid}:{page + 1}", "ref_arrow_right_icon", REF_ARROW_RIGHT_ID))
    if nav:
        kb_rows.append(nav)
    kb_rows.append([InlineKeyboardButton(text="Сбросить ВСЕХ рефералов",
                                         callback_data=f"u_resetref:{uid}")])
    kb_rows.append([InlineKeyboardButton(text="Назад", callback_data="adm:refs")])
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


@router.callback_query(F.data.startswith("ref_view:"))
async def cb_ref_view(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    parts = call.data.split(":")
    uid = int(parts[1])
    page = int(parts[2]) if len(parts) > 2 else 0
    await _render_ref_view(call, uid, page)
    await call.answer()


@router.callback_query(F.data.startswith("ref_del:"))
async def cb_ref_del(call: CallbackQuery):
    if not await _is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    parts = call.data.split(":")
    ref_owner = int(parts[1]); ref_user = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 else 0
    row = await fetchone(
        "SELECT 1 FROM users WHERE user_id=? AND referrer_id=? AND referral_rewarded=1",
        (ref_user, ref_owner),
    )
    if not row:
        await call.answer("Уже удалено", show_alert=True)
        await _render_ref_view(call, ref_owner, page); return
    await execute(
        "UPDATE users SET referrer_id=NULL, referral_rewarded=0 WHERE user_id=?",
        (ref_user,),
    )
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
    await _render_ref_view(call, ref_owner, page)


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
        t = strftime_msk("%d.%m %H:%M", ts)
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
    row = await fetchone("SELECT full_name FROM users WHERE user_id=?", (uid,))
    label = html.escape(row[0] or str(uid)) if row else str(uid)
    await ask_delete_confirm(call, f"Снять права администратора у <b>{label}</b>?", f"adm_fire_yes:{uid}", "adm:admins")


@router.callback_query(F.data.startswith("adm_fire_yes:"))
async def cb_adm_fire_yes(call: CallbackQuery):
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


@router.callback_query(F.data.startswith("task_edit:"))
async def cb_task_edit(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    tid = int(call.data.split(":")[1])
    ok = await render_task_edit_menu(call, tid)
    if not ok:
        await call.answer("Задание не найдено", show_alert=True)
        return
    await call.answer()


@router.callback_query(F.data.startswith("task_edit_reward:"))
async def cb_task_edit_reward(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    tid = int(call.data.split(":")[1])
    row = await fetchone("SELECT name, reward FROM tasks WHERE id=?", (tid,))
    if not row:
        await call.answer("Задание не найдено", show_alert=True)
        return
    await state.clear()
    await state.update_data(edit_task_id=tid)
    await state.set_state(AdminTask.edit_reward)
    await call.message.answer(
        f"Задание: <b>{html.escape(row[0])}</b>\n"
        f"Текущая награда: <b>{row[1]}</b>\n\n"
        "Введите новую награду:",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.callback_query(F.data.startswith("task_edit_max:"))
async def cb_task_edit_max(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    tid = int(call.data.split(":")[1])
    row = await fetchone("SELECT name, max_completions FROM tasks WHERE id=?", (tid,))
    if not row:
        await call.answer("Задание не найдено", show_alert=True)
        return
    await state.clear()
    await state.update_data(edit_task_id=tid)
    await state.set_state(AdminTask.edit_max_completions)
    await call.message.answer(
        f"Задание: <b>{html.escape(row[0])}</b>\n"
        f"Текущий лимит: <b>{row[1] or '∞'}</b>\n\n"
        "Введите новый лимит выполнений (0 = без лимита):",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.callback_query(F.data.startswith("task_toggle_reset:"))
async def cb_task_toggle_reset(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    tid = int(call.data.split(":")[1])
    row = await fetchone("SELECT reset_period FROM tasks WHERE id=?", (tid,))
    if not row:
        await call.answer("Задание не найдено", show_alert=True)
        return
    current = row[0] or "once"
    new_period = "daily" if current == "once" else "once"
    await execute("UPDATE tasks SET reset_period=? WHERE id=?", (new_period, tid))
    await log_admin(
        call.from_user.id,
        f"Задание #{tid}: авто-обновление изменено с '{current}' на '{new_period}'",
    )
    label = "включено ♻️" if new_period == "daily" else "выключено"
    await call.answer(f"Авто-обновление {label}", show_alert=False)
    await render_task_edit_menu(call, tid)


@router.callback_query(F.data.startswith("task_reset:"))
async def cb_task_reset(call: CallbackQuery):
    if not await _is_admin(call.from_user.id): await call.answer(); return
    tid = int(call.data.split(":")[1])
    row = await fetchone("SELECT name FROM tasks WHERE id=?", (tid,))
    if not row:
        await call.answer("Задание не найдено", show_alert=True)
        return
    submissions_row = await fetchone("SELECT COUNT(*) FROM task_submissions WHERE task_id=?", (tid,))
    completions_row = await fetchone("SELECT COUNT(*) FROM task_completions WHERE task_id=?", (tid,))
    await execute("DELETE FROM task_submissions WHERE task_id=?", (tid,))
    await execute("DELETE FROM task_completions WHERE task_id=?", (tid,))
    await execute("UPDATE tasks SET completions=0, active=1 WHERE id=?", (tid,))
    await log_admin(
        call.from_user.id,
        f"Сбросил доступность задания #{tid} {row[0]}: completions=0, удалены submissions/completions",
    )
    removed_submissions = submissions_row[0] if submissions_row else 0
    removed_completions = completions_row[0] if completions_row else 0
    await call.answer("Доступность задания сброшена")
    await render_task_edit_menu(call, tid)
    await call.message.answer(
        f"✅ Задание снова доступно всем.\n"
        f"Удалено отправок: <b>{removed_submissions}</b>\n"
        f"Удалено выполнений: <b>{removed_completions}</b>"
    )


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


@router.message(AdminTask.edit_reward)
async def task_edit_reward_save(message: Message, state: FSMContext):
    try:
        reward = int((message.text or "").strip())
    except ValueError:
        await message.answer("Число!")
        return
    data = await state.get_data()
    tid = data.get("edit_task_id")
    row = await fetchone("SELECT name FROM tasks WHERE id=?", (tid,))
    if not row:
        await state.clear()
        await message.answer("Задание не найдено", reply_markup=await admin_back_kb())
        return
    await execute("UPDATE tasks SET reward=? WHERE id=?", (reward, tid))
    await log_admin(message.from_user.id, f"Изменил награду задания #{tid} на {reward}")
    await state.clear()
    await message.answer(
        f"✅ Награда задания <b>{html.escape(row[0])}</b> изменена на <b>{reward}</b>.",
        reply_markup=await admin_back_kb(),
    )


@router.message(AdminTask.edit_max_completions)
async def task_edit_max_save(message: Message, state: FSMContext):
    try:
        mx = int((message.text or "").strip())
    except ValueError:
        await message.answer("Число!")
        return
    data = await state.get_data()
    tid = data.get("edit_task_id")
    row = await fetchone("SELECT name FROM tasks WHERE id=?", (tid,))
    if not row:
        await state.clear()
        await message.answer("Задание не найдено", reply_markup=await admin_back_kb())
        return
    await execute("UPDATE tasks SET max_completions=? WHERE id=?", (mx, tid))
    await log_admin(message.from_user.id, f"Изменил лимит задания #{tid} на {mx}")
    await state.clear()
    await message.answer(
        f"✅ Лимит задания <b>{html.escape(row[0])}</b> изменён на <b>{mx or '∞'}</b>.",
        reply_markup=await admin_back_kb(),
    )


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
    row = await fetchone("SELECT name FROM shop_items WHERE id=?", (iid,))
    label = html.escape(row[0] or f"#{iid}") if row else f"#{iid}"
    await ask_delete_confirm(call, f"Удалить товар магазина <b>{label}</b>?", f"shop_del_yes:{iid}", "adm:shop")


@router.callback_query(F.data.startswith("shop_del_yes:"))
async def cb_shop_del_yes(call: CallbackQuery):
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
        "Отправьте премиум-эмодзи для товара, его custom_emoji_id или HTML <code>&lt;tg-emoji&gt;</code>.\n"
        "Если эмодзи не нужен, отправьте `-`.",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.message(AdminShop.emoji)
async def shop_emoji(message: Message, state: FSMContext):
    stored, human = extract_stored_icon(
        message.text or "",
        message.entities,
        message.html_text,
    )
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


# ----- Token shop management -----

def token_shop_admin_back_markup(category_id: int | None = None) -> InlineKeyboardMarkup:
    target = f"tsadm:cat:{category_id}" if category_id is not None else "adm:token_shop"
    label = "К категории" if category_id is not None else "К магазину ресурсов"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=label, callback_data=target)],
        [InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")],
    ])


async def render_token_shop_admin(call: CallbackQuery):
    rows = await fetchall(
        "SELECT c.id, c.name, c.menu_text, c.emoji_icon, c.active, "
        "(SELECT COUNT(*) FROM token_shop_items i WHERE i.category_id=c.id) "
        "FROM token_shop_categories c "
        "ORDER BY c.sort_order ASC, c.id ASC"
    )
    text = (
        "🛍 <b>Магазин ресурсов</b>\n\n"
        "Главный текст и фото витрины настраиваются в разделах «Тексты» и «Фото разделов».\n\n"
    )
    kb_rows = []
    if not rows:
        text += "Категорий пока нет."
    for category_id, name, menu_text, emoji_icon, active, items_count in rows:
        icon_html = render_stored_icon_html(emoji_icon) or "•"
        status = "доступна" if active else "скрыта"
        text += (
            f"<b>#{category_id}</b> {icon_html} <b>{html.escape(name or f'Категория {category_id}')}</b>\n"
            f"Статус: <b>{status}</b> | Товаров: <b>{items_count}</b>\n"
        )
        if menu_text:
            text += f"Текст меню: <code>{html.escape((menu_text or '').strip()[:80])}</code>\n"
        text += "\n"
        button_text, button_icon_id = apply_stored_icon_to_button_text(
            f"Открыть {name or category_id}",
            emoji_icon,
        )
        kb_rows.append([
            InlineKeyboardButton(
                text=button_text,
                callback_data=f"tsadm:cat:{category_id}",
                icon_custom_emoji_id=button_icon_id,
            )
        ])
    kb_rows.append([InlineKeyboardButton(text="Добавить категорию", callback_data="tsadm:cat_add")])
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


async def render_token_shop_category_admin(call: CallbackQuery, category_id: int):
    category = await fetchone(
        "SELECT id, name, menu_text, emoji_icon, active "
        "FROM token_shop_categories WHERE id=?",
        (category_id,),
    )
    if not category:
        await call.answer("Категория не найдена", show_alert=True)
        return

    _, category_name, menu_text, emoji_icon, active = category
    items = await fetchall(
        "SELECT id, name, description, price, emoji_icon, active "
        "FROM token_shop_items WHERE category_id=? "
        "ORDER BY sort_order ASC, id DESC",
        (category_id,),
    )
    currency_name = await get_setting("currency_name", "токенов")
    category_icon = render_stored_icon_html(emoji_icon) or "🛍"
    text = (
        f"{category_icon} <b>{html.escape(category_name or f'Категория {category_id}')}</b>\n\n"
        f"Статус: <b>{'доступна' if active else 'скрыта'}</b>\n"
        f"Товаров: <b>{len(items)}</b>\n\n"
        f"<b>Текст меню категории:</b>\n<blockquote>{(menu_text or '—').strip() or '—'}</blockquote>\n\n"
    )
    if items:
        text += "<b>Товары:</b>\n\n"
        for item_id, item_name, description, price, item_emoji_icon, item_active in items:
            item_block = format_token_shop_item_block(
                name=item_name,
                price=price,
                currency_name=currency_name,
                emoji_icon=item_emoji_icon,
                description=description,
                active=bool(item_active),
            )
            text += f"<b>#{item_id}</b>\n{item_block}\n\n"
    else:
        text += "Товаров пока нет.\n\n"

    kb_rows = [
        [
            InlineKeyboardButton(
                text="Скрыть категорию" if active else "Показать категорию",
                callback_data=f"tsadm:cat_toggle:{category_id}",
            ),
            InlineKeyboardButton(
                text="Изменить эмодзи",
                callback_data=f"tsadm:cat_emoji:{category_id}",
            ),
        ],
        [InlineKeyboardButton(text="Изменить текст категории", callback_data=f"tsadm:cat_text:{category_id}")],
        [InlineKeyboardButton(text="Добавить товар", callback_data=f"tsadm:item_add:{category_id}")],
        [InlineKeyboardButton(text="🗑 Удалить категорию", callback_data=f"tsadm:cat_delete:{category_id}")],
    ]
    for item_id, item_name, _, _, _, item_active in items:
        kb_rows.append([
            InlineKeyboardButton(
                text=("Выключить " if item_active else "Включить ") + trim_button_text(item_name, 16),
                callback_data=f"tsadm:item_toggle:{item_id}",
            ),
            InlineKeyboardButton(
                text=f"Удалить {trim_button_text(item_name, 14)}",
                callback_data=f"tsadm:item_delete:{item_id}",
            ),
        ])
    kb_rows.append([InlineKeyboardButton(text="К категориям", callback_data="adm:token_shop")])
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


@router.callback_query(F.data == "adm:token_shop")
async def adm_token_shop(call: CallbackQuery):
    if not await _is_admin(call.from_user.id):
        await call.answer()
        return
    await render_token_shop_admin(call)
    await call.answer()


@router.callback_query(F.data == "tsadm:cat_add")
async def cb_token_shop_category_add(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id):
        await call.answer()
        return
    await state.set_state(AdminTokenShop.category_emoji)
    await call.message.answer(
        "Отправьте эмодзи для кнопки категории, premium-эмодзи, custom_emoji_id или HTML <code>&lt;tg-emoji&gt;</code>.\n"
        "Если эмодзи не нужен, отправьте <code>-</code>.",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.callback_query(F.data.startswith("tsadm:cat:"))
async def cb_token_shop_category_open(call: CallbackQuery):
    if not await _is_admin(call.from_user.id):
        await call.answer()
        return
    await render_token_shop_category_admin(call, int(call.data.split(":")[2]))
    await call.answer()


@router.callback_query(F.data.startswith("tsadm:cat_toggle:"))
async def cb_token_shop_category_toggle(call: CallbackQuery):
    if not await _is_admin(call.from_user.id):
        await call.answer()
        return
    category_id = int(call.data.split(":")[2])
    row = await fetchone("SELECT active, name FROM token_shop_categories WHERE id=?", (category_id,))
    if not row:
        await call.answer("Категория не найдена", show_alert=True)
        return
    new_value = 0 if row[0] else 1
    await execute("UPDATE token_shop_categories SET active=? WHERE id=?", (new_value, category_id))
    await log_admin(
        call.from_user.id,
        f"Изменил статус категории магазина ресурсов {category_id} -> {new_value}",
    )
    await render_token_shop_category_admin(call, category_id)
    await call.answer("Статус категории обновлён")


@router.callback_query(F.data.startswith("tsadm:cat_delete:"))
async def cb_token_shop_category_delete(call: CallbackQuery):
    if not await _is_admin(call.from_user.id):
        await call.answer(); return
    category_id = int(call.data.split(":")[2])
    row = await fetchone("SELECT name FROM token_shop_categories WHERE id=?", (category_id,))
    if not row:
        await call.answer("Категория не найдена", show_alert=True); return
    label = html.escape(row[0] or f"#{category_id}")
    await ask_delete_confirm(
        call, f"Удалить категорию <b>{label}</b> и все её товары?",
        f"tsadm:cat_del_yes:{category_id}", "adm:token_shop",
    )


@router.callback_query(F.data.startswith("tsadm:cat_del_yes:"))
async def cb_token_shop_category_delete_yes(call: CallbackQuery):
    if not await _is_admin(call.from_user.id):
        await call.answer(); return
    category_id = int(call.data.split(":")[2])
    row = await fetchone("SELECT name FROM token_shop_categories WHERE id=?", (category_id,))
    category_name = row[0] if row else str(category_id)
    await execute("DELETE FROM token_shop_items WHERE category_id=?", (category_id,))
    await execute("DELETE FROM token_shop_categories WHERE id=?", (category_id,))
    await log_admin(call.from_user.id, f"Удалил категорию магазина ресурсов #{category_id} ({category_name})")
    await render_token_shop_admin(call)
    await call.answer("Категория удалена", show_alert=True)


@router.callback_query(F.data.startswith("tsadm:cat_text:"))
async def cb_token_shop_category_text(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id):
        await call.answer()
        return
    category_id = int(call.data.split(":")[2])
    await state.set_state(AdminTokenShop.category_edit_text)
    await state.update_data(token_shop_category_id=category_id)
    await call.message.answer(
        "Отправьте новый текст меню категории (HTML поддерживается). Отправьте <code>-</code>, чтобы очистить текст.",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.callback_query(F.data.startswith("tsadm:cat_emoji:"))
async def cb_token_shop_category_emoji(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id):
        await call.answer()
        return
    category_id = int(call.data.split(":")[2])
    await state.set_state(AdminTokenShop.category_edit_emoji)
    await state.update_data(token_shop_category_id=category_id)
    await call.message.answer(
        "Отправьте новый эмодзи для кнопки категории или <code>-</code>, чтобы убрать его.",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.message(AdminTokenShop.category_emoji)
async def token_shop_category_emoji(message: Message, state: FSMContext):
    stored, human = extract_stored_icon(
        message.text or "",
        message.entities,
        message.html_text,
    )
    await state.update_data(category_emoji_icon=stored)
    await state.set_state(AdminTokenShop.category_name)
    await message.answer(
        f"Эмодзи сохранён: {human}\nТеперь отправьте название категории.",
        reply_markup=await cancel_kb("admin"),
    )


@router.message(AdminTokenShop.category_name)
async def token_shop_category_name(message: Message, state: FSMContext):
    name = (message.text or "").strip()
    if not name:
        await message.answer("Введите название категории.")
        return
    await state.update_data(category_name=name)
    await state.set_state(AdminTokenShop.category_text)
    await message.answer(
        "Отправьте текст меню категории (HTML поддерживается). Если нужен пустой текст, отправьте <code>-</code>.",
        reply_markup=await cancel_kb("admin"),
    )


@router.message(AdminTokenShop.category_text)
async def token_shop_category_text(message: Message, state: FSMContext):
    raw_text = (message.text or message.caption or "").strip()
    stored_text = "" if raw_text in {"", "-", "—"} else (message.html_text or message.text or message.caption or "")
    await state.update_data(category_menu_text=stored_text)
    await state.set_state(AdminTokenShop.category_active)
    await message.answer(
        "Статус категории: <code>доступно</code> или <code>недоступно</code>.",
        reply_markup=await cancel_kb("admin"),
    )


@router.message(AdminTokenShop.category_active)
async def token_shop_category_active(message: Message, state: FSMContext):
    active = parse_shop_active(message.text or "")
    if active is None:
        await message.answer("Введите <code>доступно</code> или <code>недоступно</code>.")
        return
    data = await state.get_data()
    sort_order = await next_token_shop_sort_order("token_shop_categories")
    await execute(
        "INSERT INTO token_shop_categories(name, menu_text, emoji_icon, active, sort_order, created_at) "
        "VALUES(?,?,?,?,?,?)",
        (
            data["category_name"],
            data.get("category_menu_text", ""),
            data.get("category_emoji_icon", ""),
            active,
            sort_order,
            int(time.time()),
        ),
    )
    await log_admin(message.from_user.id, f"Добавил категорию магазина ресурсов {data['category_name']}")
    await state.clear()
    await message.answer(
        "✅ Категория добавлена.",
        reply_markup=token_shop_admin_back_markup(),
    )


@router.message(AdminTokenShop.category_edit_text)
async def token_shop_category_edit_text_save(message: Message, state: FSMContext):
    data = await state.get_data()
    category_id = data.get("token_shop_category_id")
    if not category_id:
        await state.clear()
        await message.answer("Категория не найдена.", reply_markup=await admin_back_kb())
        return
    raw_text = (message.text or message.caption or "").strip()
    stored_text = "" if raw_text in {"", "-", "—"} else (message.html_text or message.text or message.caption or "")
    await execute(
        "UPDATE token_shop_categories SET menu_text=? WHERE id=?",
        (stored_text, category_id),
    )
    await log_admin(message.from_user.id, f"Изменил текст категории магазина ресурсов #{category_id}")
    await state.clear()
    await message.answer(
        "✅ Текст категории обновлён.",
        reply_markup=token_shop_admin_back_markup(category_id),
    )


@router.message(AdminTokenShop.category_edit_emoji)
async def token_shop_category_edit_emoji_save(message: Message, state: FSMContext):
    data = await state.get_data()
    category_id = data.get("token_shop_category_id")
    if not category_id:
        await state.clear()
        await message.answer("Категория не найдена.", reply_markup=await admin_back_kb())
        return
    stored, human = extract_stored_icon(
        message.text or "",
        message.entities,
        message.html_text,
    )
    await execute(
        "UPDATE token_shop_categories SET emoji_icon=? WHERE id=?",
        (stored, category_id),
    )
    await log_admin(message.from_user.id, f"Изменил эмодзи категории магазина ресурсов #{category_id}")
    await state.clear()
    await message.answer(
        f"✅ Эмодзи категории обновлён: {human}",
        reply_markup=token_shop_admin_back_markup(category_id),
    )


@router.callback_query(F.data.startswith("tsadm:item_add:"))
async def cb_token_shop_item_add(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.from_user.id):
        await call.answer()
        return
    category_id = int(call.data.split(":")[2])
    row = await fetchone("SELECT name FROM token_shop_categories WHERE id=?", (category_id,))
    if not row:
        await call.answer("Категория не найдена", show_alert=True)
        return
    await state.set_state(AdminTokenShop.item_emoji)
    await state.update_data(token_shop_category_id=category_id)
    await call.message.answer(
        f"Добавляем товар в категорию <b>{html.escape(row[0])}</b>.\n"
        "Отправьте эмодзи товара или <code>-</code>, если эмодзи не нужен.",
        reply_markup=await cancel_kb("admin"),
    )
    await call.answer()


@router.message(AdminTokenShop.item_emoji)
async def token_shop_item_emoji(message: Message, state: FSMContext):
    stored, human = extract_stored_icon(
        message.text or "",
        message.entities,
        message.html_text,
    )
    await state.update_data(token_shop_item_emoji=stored)
    await state.set_state(AdminTokenShop.item_name)
    await message.answer(
        f"Эмодзи сохранён: {human}\nТеперь отправьте название товара.",
        reply_markup=await cancel_kb("admin"),
    )


@router.message(AdminTokenShop.item_name)
async def token_shop_item_name(message: Message, state: FSMContext):
    name = (message.text or "").strip()
    if not name:
        await message.answer("Введите название товара.")
        return
    await state.update_data(token_shop_item_name=name)
    await state.set_state(AdminTokenShop.item_description)
    await message.answer(
        "Отправьте описание товара. Если описание не нужно, отправьте <code>-</code>.",
        reply_markup=await cancel_kb("admin"),
    )


@router.message(AdminTokenShop.item_description)
async def token_shop_item_description(message: Message, state: FSMContext):
    raw_text = (message.text or message.caption or "").strip()
    description = "" if raw_text in {"", "-", "—"} else raw_text
    await state.update_data(token_shop_item_description=description)
    await state.set_state(AdminTokenShop.item_price)
    await message.answer("Укажите цену товара в токенах.", reply_markup=await cancel_kb("admin"))


@router.message(AdminTokenShop.item_price)
async def token_shop_item_price(message: Message, state: FSMContext):
    try:
        price = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число.")
        return
    if price < 0:
        await message.answer("Цена не может быть отрицательной.")
        return
    await state.update_data(token_shop_item_price=price)
    await state.set_state(AdminTokenShop.item_active)
    await message.answer(
        "Статус товара: <code>доступно</code> или <code>недоступно</code>.",
        reply_markup=await cancel_kb("admin"),
    )


@router.message(AdminTokenShop.item_active)
async def token_shop_item_active(message: Message, state: FSMContext):
    active = parse_shop_active(message.text or "")
    if active is None:
        await message.answer("Введите <code>доступно</code> или <code>недоступно</code>.")
        return
    data = await state.get_data()
    category_id = data.get("token_shop_category_id")
    if not category_id:
        await state.clear()
        await message.answer("Категория потеряна. Начните добавление заново.", reply_markup=await admin_back_kb())
        return
    sort_order = await next_token_shop_sort_order("token_shop_items", category_id=category_id)
    await execute(
        "INSERT INTO token_shop_items(category_id, name, description, price, emoji_icon, active, sort_order, created_at) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (
            category_id,
            data["token_shop_item_name"],
            data.get("token_shop_item_description", ""),
            data["token_shop_item_price"],
            data.get("token_shop_item_emoji", ""),
            active,
            sort_order,
            int(time.time()),
        ),
    )
    await log_admin(
        message.from_user.id,
        f"Добавил товар магазина ресурсов {data['token_shop_item_name']} в категорию #{category_id}",
    )
    await state.clear()
    await message.answer(
        "✅ Товар добавлен.",
        reply_markup=token_shop_admin_back_markup(category_id),
    )


@router.callback_query(F.data.startswith("tsadm:item_toggle:"))
async def cb_token_shop_item_toggle(call: CallbackQuery):
    if not await _is_admin(call.from_user.id):
        await call.answer()
        return
    item_id = int(call.data.split(":")[2])
    row = await fetchone(
        "SELECT category_id, name, active FROM token_shop_items WHERE id=?",
        (item_id,),
    )
    if not row:
        await call.answer("Товар не найден", show_alert=True)
        return
    category_id, item_name, active = row
    new_value = 0 if active else 1
    await execute("UPDATE token_shop_items SET active=? WHERE id=?", (new_value, item_id))
    await log_admin(
        call.from_user.id,
        f"Изменил статус товара магазина ресурсов #{item_id} ({item_name}) -> {new_value}",
    )
    await render_token_shop_category_admin(call, category_id)
    await call.answer("Статус товара обновлён")


@router.callback_query(F.data.startswith("tsadm:item_delete:"))
async def cb_token_shop_item_delete(call: CallbackQuery):
    if not await _is_admin(call.from_user.id):
        await call.answer(); return
    item_id = int(call.data.split(":")[2])
    row = await fetchone("SELECT category_id, name FROM token_shop_items WHERE id=?", (item_id,))
    if not row:
        await call.answer("Товар не найден", show_alert=True); return
    category_id, item_name = row
    label = html.escape(item_name or f"#{item_id}")
    await ask_delete_confirm(
        call, f"Удалить товар <b>{label}</b>?",
        f"tsadm:item_del_yes:{item_id}:{category_id}", f"tsadm:cat:{category_id}",
    )


@router.callback_query(F.data.startswith("tsadm:item_del_yes:"))
async def cb_token_shop_item_delete_yes(call: CallbackQuery):
    if not await _is_admin(call.from_user.id):
        await call.answer(); return
    parts = call.data.split(":")
    item_id, category_id = int(parts[2]), int(parts[3])
    row = await fetchone("SELECT name FROM token_shop_items WHERE id=?", (item_id,))
    item_name = row[0] if row else str(item_id)
    await execute("DELETE FROM token_shop_items WHERE id=?", (item_id,))
    await log_admin(call.from_user.id, f"Удалил товар магазина ресурсов #{item_id} ({item_name})")
    await render_token_shop_category_admin(call, category_id)
    await call.answer("Товар удалён")


@router.callback_query(F.data.startswith("tsadm:purchase_issue:"))
async def cb_token_shop_purchase_issue(call: CallbackQuery):
    if not await _is_admin(call.from_user.id):
        await call.answer()
        return
    purchase_id = int(call.data.split(":")[2])
    row = await fetchone(
        "SELECT user_id, item_name, category_name, minecraft_nick, anarchy_number, price, status "
        "FROM token_shop_purchases WHERE id=?",
        (purchase_id,),
    )
    if not row:
        await call.answer("Покупка не найдена", show_alert=True)
        return

    user_id, item_name, category_name, minecraft_nick, anarchy_number, price, status = row
    if status != "pending":
        await call.answer("Эта покупка уже обработана", show_alert=True)
        return

    await execute(
        "UPDATE token_shop_purchases SET status=?, processed_at=?, processed_by=? WHERE id=?",
        ("issued", int(time.time()), call.from_user.id, purchase_id),
    )
    await log_admin(
        call.from_user.id,
        f"Выдал покупку магазина ресурсов #{purchase_id}: {item_name} ({category_name}) для {minecraft_nick} / анархия {anarchy_number}",
    )
    await log_player(
        user_id,
        f"Покупка магазина ресурсов «{item_name}» выдана администратором",
    )
    try:
        await call.bot.send_message(
            user_id,
            f"✅ Товар <b>{html.escape(item_name)}</b> из категории <b>{html.escape(category_name)}</b> выдан.\n"
            f"Ник: <code>{html.escape(minecraft_nick)}</code>\n"
            f"Анархия: <b>{html.escape(anarchy_number)}</b>",
        )
    except Exception:
        pass
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await call.answer("Покупка отмечена как выданная")


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
    row = await fetchone("SELECT label FROM funtime_test_ips WHERE id=?", (tid,))
    label = html.escape(row[0] or f"#{tid}") if row else f"#{tid}"
    await ask_delete_confirm(call, f"Удалить тестовый IP <b>{label}</b>?", f"ft_del_yes:{tid}", "adm:funtime")


@router.callback_query(F.data.startswith("ft_del_yes:"))
async def cb_ft_del_yes(call: CallbackQuery):
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
    "maintenance": "maintenance_photo",
    "menu": "menu_photo", "profile": "profile_photo", "earn": "earn_photo",
    "token_shop": "token_shop_photo",
    "casino": "casino_photo", "funtime": "funtime_photo", "theft": "theft_photo",
    "leaderboard": "leaderboard_photo", "rules": "rules_photo",
    "promo": "promo_photo", "tasks": "tasks_photo",
    "op": "op_photo",
}
TEXT_KEYS = {
    "menu": "menu_text",
    "earn": "earn_text",
    "token_shop": "token_shop_text",
    "rules": "rules_text",
    "maintenance": "maintenance_text",
    "info": "info_text",
    "emoji_profile": "section_emoji_profile",
    "emoji_tasks": "section_emoji_tasks",
    "emoji_casino": "section_emoji_casino",
    "emoji_funtime": "section_emoji_funtime",
    "emoji_theft": "section_emoji_theft",
    "emoji_leaderboard": "section_emoji_leaderboard",
    "lb_place_1": "leaderboard_place_1_icon",
    "lb_place_2": "leaderboard_place_2_icon",
    "lb_place_3": "leaderboard_place_3_icon",
    "ref_arrow_left": "ref_arrow_left_icon",
    "ref_arrow_right": "ref_arrow_right_icon",
    "task_done_icon": "tasks_done_icon",
    "task_not_done_icon": "tasks_not_done_icon",
    "task_received_icon": "tasks_received_icon",
    "task_open_icon": "tasks_open_icon",
    "task_check_icon": "tasks_check_icon",
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
    "ref_arrow_left": "Рефералы·стрелка назад",
    "ref_arrow_right": "Рефералы·стрелка вперёд",
    "task_done_icon": "Задания·выполнено",
    "task_not_done_icon": "Задания·не выполнено",
    "task_received_icon": "Задания·получено",
    "task_open_icon": "Задания·кнопка канала",
    "task_check_icon": "Задания·проверить",
    "menu": "Меню",
    "profile": "Профиль",
    "earn": "Заработок",
    "token_shop": "Магазин ресурсов",
    "casino": "Казино",
    "funtime": "Фантайм",
    "theft": "Кража",
    "leaderboard": "Лидерборд",
    "rules": "Правила",
    "maintenance": "Тех перерыв",
    "info": "Информация (профиль)",
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
    ("nav:token_shop", "Магазин ресурсов"),
    ("nav:casino", "Казино"),
    ("nav:funtime", "Фантайм"),
    ("nav:theft", "Кража"),
    ("nav:lb", "Лидерборд"),
    ("nav:rules", "Правила"),
    ("nav:promo", "Промокод"),
    ("nav:bonus", "Бонус"),
    ("nav:withdraw", "Вывести"),
    ("nav:info", "Информация"),
    ("nav:dice", "Кубик"),
    ("nav:basket", "Баскетбол"),
    ("nav:rob", "Ограбить"),
    ("nav:token_shop_cart", "Корзина ресурсов"),
    ("nav:token_shop_inventory", "Инвентарь ресурсов"),
    ("nav:menu", "Назад"),
    ("op_channel_link", "Кнопки каналов"),
    ("op_check_gate", "ОП · Проверить доступ"),
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
    ("adm:token_shop", "Адм·Магазин ресурсов"),
    ("adm:token_shop_requests", "Адм·Заявки ресурсов"),
    ("adm:funtime", "Адм·Фантайм"),
    ("adm:maintenance", "Адм·Тех перерыв"),
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


# ----- Resource Requests (Заявки ресурсов) -----

async def _render_request_detail(request_id: int) -> tuple[str, InlineKeyboardMarkup | None]:
    req = await fetchone(
        "SELECT r.id, r.user_id, r.item_name, r.category_name, r.price, "
        "r.anarchy_number, r.minecraft_nick, r.status, r.created_at, "
        "u.username, u.full_name "
        "FROM token_shop_requests r "
        "LEFT JOIN users u ON u.user_id=r.user_id "
        "WHERE r.id=?",
        (request_id,),
    )
    if not req:
        return f"Заявка #{request_id} не найдена.", None
    (
        rid, user_id, item_name, category_name, price,
        anarchy_number, minecraft_nick, status, created_at,
        username, full_name,
    ) = req
    user_label = full_name or str(user_id)
    if username:
        user_label += f" (@{username})"
    status_map = {
        "pending": "⏳ На рассмотрении",
        "issued": "✅ Выдано",
        "returned": "↩️ Возвращено в корзину",
        "rejected": "❌ Отклонено",
    }
    created_str = ""
    if created_at:
        from datetime import datetime
        dt = datetime.fromtimestamp(created_at)
        created_str = dt.strftime("%d.%m.%Y %H:%M")
    text = (
        f"🛍 <b>Заявка ресурсов #{rid}</b>\n\n"
        f"Товар: <b>{html.escape(item_name or '')}</b>\n"
        f"Категория: <b>{html.escape(category_name or '')}</b>\n"
        f"Цена: <b>{price} токенов</b>\n"
        f"Пользователь: <b>{html.escape(user_label)}</b>\n"
        f"ID: <code>{user_id}</code>\n"
        f"Анархия: <b>{html.escape(anarchy_number or '—')}</b>\n"
        f"Ник: <code>{html.escape(minecraft_nick or '—')}</code>\n"
        f"Статус: <b>{status_map.get(status, status)}</b>\n"
        f"Создана: {created_str}"
    )
    if status == "pending":
        kb_rows = [
            [
                InlineKeyboardButton(text="✅ Выдано", callback_data=f"tsadm:req_issue:{rid}"),
                InlineKeyboardButton(text="↩️ Возврат", callback_data=f"tsadm:req_return:{rid}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"tsadm:req_reject:{rid}"),
            ],
            [InlineKeyboardButton(text="К списку заявок", callback_data="adm:token_shop_requests")],
            [InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")],
        ]
    else:
        kb_rows = [
            [InlineKeyboardButton(text="К списку заявок", callback_data="adm:token_shop_requests")],
            [InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")],
        ]
    return text, InlineKeyboardMarkup(inline_keyboard=kb_rows)


@router.callback_query(F.data == "adm:token_shop_requests")
async def adm_token_shop_requests(call: CallbackQuery):
    if not await _is_admin(call.from_user.id):
        await call.answer()
        return
    await call.answer()
    rows = await fetchall(
        "SELECT r.id, r.item_name, r.category_name, r.status, r.created_at, u.username, u.full_name "
        "FROM token_shop_requests r "
        "LEFT JOIN users u ON u.user_id=r.user_id "
        "ORDER BY r.id DESC LIMIT 30"
    )
    if not rows:
        await send_section(
            call, "Заявок ресурсов пока нет.", None,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")],
            ]),
        )
        return
    status_map = {
        "pending": "⏳",
        "issued": "✅",
        "returned": "↩️",
        "rejected": "❌",
    }
    text = "🛍 <b>Заявки ресурсов</b> (последние 30)\n\n"
    kb_rows = []
    for rid, item_name, category_name, status, created_at, username, full_name in rows:
        user_label = full_name or "?"
        if username:
            user_label += f" (@{username})"
        icon = status_map.get(status, "?")
        text += f"{icon} <b>#{rid}</b> — {html.escape(item_name or '?')} — {html.escape(user_label)}\n"
        kb_rows.append([
            InlineKeyboardButton(
                text=f"{icon} #{rid} {item_name or '?'}",
                callback_data=f"tsadm:req_open:{rid}",
            )
        ])
    kb_rows.append([InlineKeyboardButton(text="В админ-панель", callback_data="adm:home")])
    await send_section(call, text, None, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


@router.callback_query(F.data.startswith("tsadm:req_open:"))
async def adm_req_open(call: CallbackQuery):
    if not await _is_admin(call.from_user.id):
        await call.answer()
        return
    await call.answer()
    request_id = int(call.data.split(":")[2])
    text, markup = await _render_request_detail(request_id)
    await send_section(call, text, None, reply_markup=markup)


@router.callback_query(F.data.startswith("tsadm:req_issue:"))
async def adm_req_issue(call: CallbackQuery, bot: Bot):
    if not await _is_admin(call.from_user.id):
        await call.answer()
        return
    request_id = int(call.data.split(":")[2])
    req = await fetchone(
        "SELECT id, inventory_id, user_id, item_name, status, price FROM token_shop_requests WHERE id=?",
        (request_id,),
    )
    if not req:
        await call.answer("Заявка не найдена", show_alert=True)
        return
    if req[4] != "pending":
        await call.answer("Заявка уже обработана", show_alert=True)
        return
    rid, inv_id, user_id, item_name, _, price = req
    now = int(time.time())
    await execute(
        "UPDATE token_shop_requests SET status='issued', processed_at=?, processed_by=? WHERE id=?",
        (now, call.from_user.id, request_id),
    )
    await execute(
        "UPDATE token_shop_inventory SET status='issued', updated_at=? WHERE id=?",
        (now, inv_id),
    )
    await log_admin(call.from_user.id, f"Выдал заявку ресурсов #{request_id} ({item_name})")
    try:
        await bot.send_message(
            user_id,
            f"✅ <b>Ваш запрос выполнен!</b>\n\n"
            f"Товар <b>{html.escape(item_name)}</b> (заявка #{request_id}) выдан.\n"
            "Спасибо за покупку!",
            parse_mode="HTML",
        )
    except Exception:
        pass
    await call.answer("✅ Отмечено как выдано", show_alert=True)
    text, markup = await _render_request_detail(request_id)
    await send_section(call, text, None, reply_markup=markup)


@router.callback_query(F.data.startswith("tsadm:req_return:"))
async def adm_req_return(call: CallbackQuery, bot: Bot):
    if not await _is_admin(call.from_user.id):
        await call.answer()
        return
    request_id = int(call.data.split(":")[2])
    req = await fetchone(
        "SELECT id, inventory_id, user_id, item_name, status, price FROM token_shop_requests WHERE id=?",
        (request_id,),
    )
    if not req:
        await call.answer("Заявка не найдена", show_alert=True)
        return
    if req[4] != "pending":
        await call.answer("Заявка уже обработана", show_alert=True)
        return
    rid, inv_id, user_id, item_name, _, price = req
    now = int(time.time())
    await execute(
        "UPDATE token_shop_requests SET status='returned', processed_at=?, processed_by=? WHERE id=?",
        (now, call.from_user.id, request_id),
    )
    await execute(
        "UPDATE token_shop_inventory SET status='available', updated_at=? WHERE id=?",
        (now, inv_id),
    )
    if price and price > 0:
        await execute(
            "UPDATE users SET balance=balance+? WHERE user_id=?",
            (price, user_id),
        )
    await log_admin(call.from_user.id, f"Вернул предмет в корзину по заявке ресурсов #{request_id} ({item_name}), возврат {price} токенов")
    try:
        await bot.send_message(
            user_id,
            f"↩️ <b>Заявка возвращена</b>\n\n"
            f"Товар <b>{html.escape(item_name)}</b> (заявка #{request_id}) возвращён.\n"
            f"На ваш баланс зачислено <b>{price} токенов</b>.",
            parse_mode="HTML",
        )
    except Exception:
        pass
    await call.answer("↩️ Предмет возвращён, токены зачислены", show_alert=True)
    text, markup = await _render_request_detail(request_id)
    await send_section(call, text, None, reply_markup=markup)


@router.callback_query(F.data.startswith("tsadm:req_reject:"))
async def adm_req_reject(call: CallbackQuery, bot: Bot):
    if not await _is_admin(call.from_user.id):
        await call.answer()
        return
    request_id = int(call.data.split(":")[2])
    req = await fetchone(
        "SELECT id, inventory_id, user_id, item_name, status, price FROM token_shop_requests WHERE id=?",
        (request_id,),
    )
    if not req:
        await call.answer("Заявка не найдена", show_alert=True)
        return
    if req[4] != "pending":
        await call.answer("Заявка уже обработана", show_alert=True)
        return
    rid, inv_id, user_id, item_name, _, price = req
    now = int(time.time())
    await execute(
        "UPDATE token_shop_requests SET status='rejected', processed_at=?, processed_by=? WHERE id=?",
        (now, call.from_user.id, request_id),
    )
    await execute(
        "UPDATE token_shop_inventory SET status='rejected', updated_at=? WHERE id=?",
        (now, inv_id),
    )
    await log_admin(call.from_user.id, f"Отклонил заявку ресурсов #{request_id} ({item_name})")
    try:
        await bot.send_message(
            user_id,
            f"❌ <b>Заявка отклонена</b>\n\n"
            f"Заявка #{request_id} на товар <b>{html.escape(item_name)}</b> отклонена администратором.\n"
            "Токены не возвращаются. По вопросам обратитесь к администратору.",
            parse_mode="HTML",
        )
    except Exception:
        pass
    await call.answer("❌ Заявка отклонена", show_alert=True)
    text, markup = await _render_request_detail(request_id)
    await send_section(call, text, None, reply_markup=markup)


@router.message(Command("open"))
async def cmd_open_request(message: Message):
    if not await _is_admin(message.from_user.id):
        return
    parts = (message.text or "").strip().split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Использование: /open <номер заявки>")
        return
    request_id = int(parts[1])
    text, markup = await _render_request_detail(request_id)
    await message.answer(text, reply_markup=markup)


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
