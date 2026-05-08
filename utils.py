import asyncio
import html
import re
import time
from aiogram import Bot
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputMediaPhoto,
)
from aiogram.exceptions import TelegramBadRequest

from db import execute, fetchall, fetchone, get_setting
from keyboards import style_markup


def render_stored_icon_html(stored: str | None) -> str:
    value = (stored or "").strip()
    if not value:
        return ""
    if value.startswith("id:"):
        cid = value[3:].strip()
        if cid:
            return f'<tg-emoji emoji-id="{html.escape(cid, quote=True)}">✨</tg-emoji>'
        return ""
    if value.startswith("tx:"):
        return html.escape(value[3:].strip())
    return html.escape(value)


def extract_custom_emoji_id(value: str | None) -> str | None:
    text = (value or "").strip()
    if not text:
        return None
    if text.startswith("id:"):
        cid = text[3:].strip()
        return cid or None
    if text.isdigit():
        return text
    match = re.search(r"emoji-id=[\"']([^\"']+)[\"']", text)
    if match:
        return match.group(1).strip() or None
    return None


def render_config_icon_html(value: str | None, fallback: str = "") -> str:
    text = (value or "").strip()
    if not text:
        return fallback
    cid = extract_custom_emoji_id(text)
    if cid:
        return f'<tg-emoji emoji-id="{html.escape(cid, quote=True)}">{html.escape(fallback or "✨")}</tg-emoji>'
    if text.startswith("tx:"):
        return html.escape(text[3:].strip())
    return text


def apply_stored_icon_to_button_text(text: str, stored: str | None) -> tuple[str, str | None]:
    value = (stored or "").strip()
    if not value:
        return text, None
    if value.startswith("id:"):
        cid = value[3:].strip()
        return text, cid or None
    if value.startswith("tx:"):
        prefix = value[3:].strip()
        if prefix:
            return f"{prefix} {text}", None
    return text, None


def build_subscription_gate_text(channels: list[tuple[str, str]], target_name: str = "бота") -> str:
    channel_lines = "\n".join(
        f"• {html.escape(title or 'Канал')}"
        for _, title in channels
    )
    if not channel_lines:
        channel_lines = "• Каналы скоро появятся"
    return (
        "<tg-emoji emoji-id=5260268501515377807>📣</tg-emoji> <b>Доступ ограничен</b>\n\n"
        f"<blockquote>Чтобы получать токены, подпишись на все каналы:\n\n"
        f"{channel_lines}</blockquote>\n\n"
        "<tg-emoji emoji-id=5258077307985207053>📹</tg-emoji> После подписки нажми «Проверить доступ»."
    )


def build_subscription_gate_kb(
    channels: list[tuple[str, str]],
    category: str,
    *,
    back_to_menu: bool = False,
) -> InlineKeyboardMarkup:
    rows = []
    for link, title in channels:
        if link:
            rows.append([InlineKeyboardButton(text=title, url=link)])
        else:
            rows.append([InlineKeyboardButton(text=title, callback_data=f"op_check:{category}")])
    rows.append([InlineKeyboardButton(text="Проверить доступ", callback_data=f"op_check:{category}")])
    if back_to_menu:
        rows.append([InlineKeyboardButton(text="Назад", callback_data="nav:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def format_shop_item_block(
    *,
    name: str,
    price: int,
    income_per_day: int,
    active: bool,
    currency_name: str,
    emoji_icon: str = "",
    discount_pct: int = 0,
    description: str = "",
) -> str:
    icon = render_stored_icon_html(emoji_icon) or "•"
    effective_price = max(0, price - price * max(0, discount_pct) // 100)
    lines = [
        f"{icon} <b>{html.escape(name)}</b>",
        f"Цена: <b>{effective_price} {html.escape(currency_name)}</b>",
        f"Доход: <b>{income_per_day} {html.escape(currency_name)}/день</b>",
        f"Статус: <b>{'Доступно' if active else 'Недоступно'}</b>",
    ]
    clean_description = (description or "").strip()
    if clean_description and clean_description != "-":
        lines.append(html.escape(clean_description))
    return "\n".join(lines)


def format_token_shop_item_block(
    *,
    name: str,
    price: int,
    currency_name: str,
    emoji_icon: str = "",
    description: str = "",
    active: bool | None = None,
) -> str:
    icon = render_stored_icon_html(emoji_icon) or "•"
    lines = [
        f"{icon} <b>{html.escape(name)}</b>",
        f"Цена: <b>{price} {html.escape(currency_name)}</b>",
    ]
    clean_description = (description or "").strip()
    if clean_description and clean_description != "-":
        lines.append(html.escape(clean_description))
    if active is not None:
        lines.append(f"Статус: <b>{'Доступно' if active else 'Недоступно'}</b>")
    return "\n".join(lines)


async def send_section(target, text: str, photo_key: str | None, reply_markup: InlineKeyboardMarkup | None = None):
    """Send a section view. `target` can be Message or CallbackQuery.
    For callbacks with photo: delete old message, send new photo. Without photo: edit text/caption.
    """
    photo = await get_setting(photo_key, "") if photo_key else ""
    if reply_markup is not None:
        reply_markup = await style_markup(reply_markup)

    if isinstance(target, CallbackQuery):
        msg = target.message
        if photo and msg.photo:
            try:
                await msg.edit_media(
                    media=InputMediaPhoto(media=photo, caption=text, parse_mode="HTML"),
                    reply_markup=reply_markup,
                )
                return
            except TelegramBadRequest:
                pass

        try:
            if msg.photo:
                await msg.edit_caption(caption=text, reply_markup=reply_markup)
            else:
                await msg.edit_text(text, reply_markup=reply_markup)
            return
        except TelegramBadRequest:
            pass

        # Fallback: delete old message and send a plain text message
        try:
            await msg.delete()
        except TelegramBadRequest:
            pass
        try:
            await target.message.answer(text, reply_markup=reply_markup)
        except TelegramBadRequest:
            pass
        return

    # Message
    if photo:
        try:
            await target.answer_photo(photo, caption=text, reply_markup=reply_markup)
            return
        except TelegramBadRequest:
            pass
    await target.answer(text, reply_markup=reply_markup)


async def get_active_channels(category: str) -> list:
    rows = await fetchall(
        "SELECT id, link, chat_id, title, is_private, max_subs, current_subs, active, "
        "invite_link "
        "FROM channels WHERE category=? AND active=1",
        (category,),
    )
    return rows


def normalize_channel_target(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        raise ValueError("empty")

    if re.fullmatch(r"-?\d+", text):
        return text

    if text.startswith("@"):
        return text

    match = re.match(r"^(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/(.+)$", text, re.IGNORECASE)
    if not match:
        return text

    path = match.group(1).split("?", 1)[0].split("#", 1)[0].strip("/")
    parts = [part for part in path.split("/") if part]
    if not parts:
        raise ValueError("empty_link")

    head = parts[0]
    if head == "joinchat" or head.startswith("+"):
        raise ValueError("invite_link_not_supported")
    if head == "c":
        raise ValueError("internal_link_not_supported")
    if head == "s":
        if len(parts) < 2:
            raise ValueError("bad_public_link")
        head = parts[1]

    if not re.fullmatch(r"[A-Za-z0-9_]{4,}", head):
        raise ValueError("bad_public_link")
    return f"@{head}"


async def has_pending_channel_request(user_id: int, chat_id: str | int) -> bool:
    row = await fetchone(
        "SELECT id FROM channels WHERE chat_id=?",
        (str(chat_id),),
    )
    if not row:
        return False
    pending = await fetchone(
        "SELECT 1 FROM channel_join_log WHERE channel_id=? AND user_id=?",
        (row[0], user_id),
    )
    return pending is not None


async def is_user_subscribed_to_chat(
    bot: Bot,
    user_id: int,
    chat_id: str | int,
    *,
    suppress_errors: bool = True,
) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
    except Exception:
        if suppress_errors:
            return False
        raise

    status = getattr(member.status, "value", member.status)
    if status in {"creator", "administrator", "member"}:
        return True
    if status == "restricted":
        return bool(getattr(member, "is_member", False))
    return False


async def get_channel_members_count(bot: Bot, chat_id: str | int) -> int | None:
    try:
        return await bot.get_chat_member_count(chat_id)
    except Exception:
        return None


async def check_user_subscriptions(bot: Bot, user_id: int, category: str) -> list:
    channels = await get_active_channels(category)
    not_subbed = []
    for ch in channels:
        chat_id = ch[2]
        link = ch[1]
        title = ch[3] or "Канал"
        invite_link = ch[8] if len(ch) > 8 else None
        # Бот-генерируемая ссылка приоритетнее — она ведёт на «подать заявку»
        display_link = invite_link or link
        if not chat_id:
            continue
        try:
            if not await is_user_subscribed_to_chat(bot, user_id, chat_id):
                not_subbed.append((display_link, title))
        except Exception:
            not_subbed.append((display_link, title))
    return not_subbed


async def refresh_user_start_subscription(bot: Bot, user_id: int) -> int | None:
    now = int(time.time())
    row = await fetchone(
        "SELECT captcha_passed, banned FROM users WHERE user_id=?",
        (user_id,),
    )
    if not row:
        return None

    current_captcha_passed, banned = row
    if banned:
        await execute(
            "UPDATE users SET last_subscription_check=? WHERE user_id=?",
            (now, user_id),
        )
        return 0

    if (await get_setting("start_op_enabled", "0")) != "1":
        await execute(
            "UPDATE users SET last_subscription_check=? WHERE user_id=?",
            (now, user_id),
        )
        return current_captcha_passed

    captcha_enabled = (await get_setting("captcha_enabled", "0")) == "1"
    channels = await get_active_channels("start")
    is_subscribed_everywhere = True

    for channel in channels:
        chat_id = channel[2]
        if not chat_id:
            continue
        try:
            subscribed = await is_user_subscribed_to_chat(
                bot,
                user_id,
                chat_id,
                suppress_errors=False,
            )
        except Exception:
            await execute(
                "UPDATE users SET last_subscription_check=? WHERE user_id=?",
                (now, user_id),
            )
            return current_captcha_passed
        if not subscribed:
            is_subscribed_everywhere = False
            break

    if not is_subscribed_everywhere:
        new_captcha_passed = 0
    elif captcha_enabled:
        new_captcha_passed = current_captcha_passed
    else:
        new_captcha_passed = 1

    await execute(
        "UPDATE users SET captcha_passed=?, last_subscription_check=? WHERE user_id=?",
        (new_captcha_passed, now, user_id),
    )
    return new_captcha_passed


async def query_minecraft_status(host_port: str) -> int | None:
    if ":" in host_port:
        host, port_s = host_port.rsplit(":", 1)
        try:
            port = int(port_s)
        except ValueError:
            port = 25565
    else:
        host, port = host_port, 25565

    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=4)
    except Exception:
        return None

    try:
        def varint(n):
            out = b""
            while True:
                b = n & 0x7F
                n >>= 7
                if n:
                    out += bytes([b | 0x80])
                else:
                    out += bytes([b])
                    break
            return out

        host_b = host.encode("utf-8")
        handshake = (
            b"\x00" + varint(47) + varint(len(host_b)) + host_b
            + port.to_bytes(2, "big") + b"\x01"
        )
        writer.write(varint(len(handshake)) + handshake)
        writer.write(varint(1) + b"\x00")
        await writer.drain()

        async def read_varint():
            n = 0; shift = 0
            for _ in range(5):
                b = await reader.readexactly(1)
                val = b[0]
                n |= (val & 0x7F) << shift
                if not (val & 0x80):
                    return n
                shift += 7
            return n

        length = await asyncio.wait_for(read_varint(), timeout=4)
        data = await asyncio.wait_for(reader.readexactly(length), timeout=4)
        i = 0
        while data[i] & 0x80:
            i += 1
        i += 1
        s_len = 0; shift = 0
        while True:
            b = data[i]; i += 1
            s_len |= (b & 0x7F) << shift
            if not (b & 0x80):
                break
            shift += 7
        json_bytes = data[i:i + s_len]
        import json as _json
        info = _json.loads(json_bytes.decode("utf-8", errors="ignore"))
        return int(info.get("players", {}).get("online", 0))
    except Exception:
        return None
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
