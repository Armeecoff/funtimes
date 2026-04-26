import random
import time
import logging
from aiogram import Router, F, Bot
from aiogram.filters import CommandStart, CommandObject
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ChatJoinRequest,
)
from aiogram.fsm.context import FSMContext

from db import get_or_create_user, get_setting, set_setting, fetchone, execute, is_admin
from states import CaptchaSG
from keyboards import main_menu_kb, remove_reply
from utils import (
    send_section,
    check_user_subscriptions,
    build_subscription_gate_text,
    build_subscription_gate_kb,
)

router = Router()


def captcha_kb(options: list[int], correct: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=str(o), callback_data=f"captcha:{o}:{correct}")] for o in options
    ])


async def show_main_menu(target, state: FSMContext):
    await state.clear()
    text = await get_setting("menu_text")
    uid = target.from_user.id
    admin_flag = await is_admin(uid)
    await send_section(target, text, "menu_photo",
                       reply_markup=await main_menu_kb(is_admin=admin_flag))


async def gate_and_show_menu(message_or_call, user_id: int, state: FSMContext, bot: Bot):
    """Run start-OP + captcha gate, then show menu. Works for Message or CallbackQuery."""
    if (await get_setting("start_op_enabled", "0")) == "1":
        not_subbed = await check_user_subscriptions(bot, user_id, "start")
        if not_subbed:
            text = build_subscription_gate_text(not_subbed, "бота")
            kb = build_subscription_gate_kb(not_subbed, "start")
            await send_section(message_or_call, text, "op_photo", reply_markup=kb)
            return False

    row = await fetchone("SELECT captcha_passed FROM users WHERE user_id=?", (user_id,))
    if (await get_setting("captcha_enabled", "0")) == "1" and not (row and row[0]):
        a, b = random.randint(2, 9), random.randint(2, 9)
        correct = a + b
        opts = list({correct, correct + random.randint(1, 5), max(1, correct - random.randint(1, 5)), random.randint(5, 30)})
        random.shuffle(opts)
        await state.set_state(CaptchaSG.waiting)
        await state.update_data(correct=correct)
        text = f"🤖 Капча: сколько будет {a} + {b}?"
        kb = captcha_kb(opts, correct)
        if isinstance(message_or_call, CallbackQuery):
            try:
                await message_or_call.message.edit_text(text, reply_markup=kb)
            except Exception:
                await message_or_call.message.answer(text, reply_markup=kb)
        else:
            await message_or_call.answer(text, reply_markup=kb)
        return False

    await execute("UPDATE users SET captcha_passed=1 WHERE user_id=?", (user_id,))
    await show_main_menu(message_or_call, state)
    return True


@router.message(CommandStart(deep_link=True))
@router.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject, state: FSMContext, bot: Bot):
    referrer_id = None
    if command.args:
        try:
            referrer_id = int(command.args)
        except ValueError:
            pass
    # Считаем переходы по ссылке бота: каждый /start — это клик.
    try:
        total = int(await get_setting("bot_link_clicks", "0") or "0")
    except ValueError:
        total = 0
    await set_setting("bot_link_clicks", str(total + 1))
    if referrer_id:
        try:
            ref_total = int(await get_setting(f"ref_clicks:{referrer_id}", "0") or "0")
        except ValueError:
            ref_total = 0
        await set_setting(f"ref_clicks:{referrer_id}", str(ref_total + 1))

    await get_or_create_user(
        message.from_user.id, message.from_user.username,
        message.from_user.full_name, referrer_id,
    )
    row = await fetchone("SELECT banned FROM users WHERE user_id=?", (message.from_user.id,))
    if row and row[0]:
        await message.answer("⛔ Вы заблокированы в этом боте.")
        return
    # remove any old reply keyboard if present
    try:
        m = await message.answer("…", reply_markup=remove_reply())
        await m.delete()
    except Exception:
        pass
    await gate_and_show_menu(message, message.from_user.id, state, bot)


@router.callback_query(F.data.startswith("captcha:"))
async def cb_captcha(call: CallbackQuery, state: FSMContext, bot: Bot):
    parts = call.data.split(":")
    chosen, correct = int(parts[1]), int(parts[2])
    if chosen != correct:
        await call.answer("❌ Неверно, попробуй ещё", show_alert=True)
        return
    await state.clear()
    await execute("UPDATE users SET captcha_passed=1 WHERE user_id=?", (call.from_user.id,))
    await call.answer("✅ Капча пройдена")
    await show_main_menu(call, state)


@router.callback_query(F.data.startswith("op_check:"))
async def cb_op_check(call: CallbackQuery, bot: Bot, state: FSMContext):
    category = call.data.split(":")[1]
    not_subbed = await check_user_subscriptions(bot, call.from_user.id, category)
    if not_subbed:
        await call.answer("Вы ещё не подписались на все каналы", show_alert=True)
        return
    await call.answer("✅ Спасибо!")
    if category == "start":
        await gate_and_show_menu(call, call.from_user.id, state, bot)
    else:
        # tasks OP — re-open tasks
        from menu import show_tasks
        await show_tasks(call, bot)


@router.callback_query(F.data == "nav:menu")
async def cb_menu(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await show_main_menu(call, state)


@router.callback_query(F.data.startswith("cancel:"))
async def cb_cancel(call: CallbackQuery, state: FSMContext):
    target = call.data.split(":")[1]
    await state.clear()
    await call.answer("Отменено")
    if target == "admin":
        from admin import show_admin_home
        await show_admin_home(call)
    else:
        await show_main_menu(call, state)


# ---------- Заявки на вступление в каналы ОП ----------

@router.chat_join_request()
async def on_chat_join_request(req: ChatJoinRequest, bot: Bot):
    """Срабатывает, когда пользователь подал заявку через invite-link с
    creates_join_request=True. Сопоставляем ссылку с каналом из БД и считаем."""
    used_link = ""
    if req.invite_link is not None:
        used_link = req.invite_link.invite_link or ""
    chat_id_str = str(req.chat.id)

    # Ищем канал: сначала по точному совпадению invite_link, иначе по chat_id
    row = None
    if used_link:
        row = await fetchone(
            "SELECT id FROM channels WHERE invite_link=?", (used_link,)
        )
    if row is None:
        row = await fetchone(
            "SELECT id FROM channels WHERE chat_id=?", (chat_id_str,)
        )
    if row is None:
        logging.info(
            "chat_join_request: канал %s не найден в БД (link=%s)",
            chat_id_str, used_link,
        )
        return
    channel_id = row[0]

    # Считаем заявку (без дублей по user_id)
    dup = await fetchone(
        "SELECT 1 FROM channel_join_log WHERE channel_id=? AND user_id=?",
        (channel_id, req.from_user.id),
    )
    if dup:
        return
    await execute(
        "INSERT INTO channel_join_log(channel_id, user_id, invite_link, created_at) "
        "VALUES(?,?,?,?)",
        (channel_id, req.from_user.id, used_link, int(time.time())),
    )
    await execute(
        "INSERT INTO channel_stats(channel_id, join_requests, members, reach) "
        "VALUES(?, 1, 0, 0) "
        "ON CONFLICT(channel_id) DO UPDATE SET join_requests = join_requests + 1",
        (channel_id,),
    )
    logging.info(
        "chat_join_request: +1 заявка в канал #%s от user %s",
        channel_id, req.from_user.id,
    )
