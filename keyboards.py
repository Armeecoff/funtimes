from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from db import get_setting


def remove_reply():
    return ReplyKeyboardRemove()


VALID_STYLES = ("default", "primary", "success", "danger")
VALID_SIZES = ("default", "full")
CHANNEL_LINK_STYLE_KEY = "op_channel_link"


def _is_telegram_link(url: str | None) -> bool:
    value = (url or "").strip().lower()
    return (
        value.startswith("https://t.me/")
        or value.startswith("http://t.me/")
        or value.startswith("https://telegram.me/")
        or value.startswith("http://telegram.me/")
    )


async def _button_setting_triplet(key: str) -> tuple[str, str, str]:
    style = (await get_setting(f"btn_style:{key}", "")).strip().lower()
    icon = (await get_setting(f"btn_icon:{key}", "")).strip()
    size = (await get_setting(f"btn_size:{key}", "default")).strip().lower()
    return style, icon, size


async def apply_button_settings(button: InlineKeyboardButton) -> tuple[InlineKeyboardButton, str]:
    key = button.callback_data or button.url
    if not key:
        return button, "default"

    style, icon, size = await _button_setting_triplet(key)
    if button.url and _is_telegram_link(button.url) and not style and not icon and size in ("", "default"):
        style, icon, size = await _button_setting_triplet(CHANNEL_LINK_STYLE_KEY)
    if size not in VALID_SIZES:
        size = "default"

    updates = {"style": None, "icon_custom_emoji_id": None}
    if style in VALID_STYLES and style != "default":
        updates["style"] = style

    if icon:
        if icon.startswith("tx:"):
            prefix = icon[3:].strip()
            if prefix and not button.text.startswith(f"{prefix} "):
                updates["text"] = f"{prefix} {button.text}"
        else:
            cid = icon[3:] if icon.startswith("id:") else icon
            if cid:
                updates["icon_custom_emoji_id"] = cid

    return button.model_copy(update=updates), size


async def style_markup(markup: InlineKeyboardMarkup) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for row in markup.inline_keyboard:
        current: list[InlineKeyboardButton] = []
        for button in row:
            styled_button, size = await apply_button_settings(button)
            if size == "full":
                if current:
                    rows.append(current)
                    current = []
                rows.append([styled_button])
                continue
            current.append(styled_button)
        if current:
            rows.append(current)
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def mk_btn(text: str, *, callback_data: str | None = None, url: str | None = None,
                 web_app=None) -> InlineKeyboardButton:
    """Build an inline button and apply per-callback style/icon from settings.
    Bot API 9.4+: native `style` (primary/success/danger) and
    `icon_custom_emoji_id` (премиум-эмодзи слева от текста).
    """
    kwargs: dict = {"text": text}
    if callback_data is not None:
        kwargs["callback_data"] = callback_data
    if url is not None:
        kwargs["url"] = url
    if web_app is not None:
        kwargs["web_app"] = web_app

    button, _ = await apply_button_settings(InlineKeyboardButton(**kwargs))
    return button


async def kb(rows: list[list[InlineKeyboardButton]]) -> InlineKeyboardMarkup:
    return await style_markup(InlineKeyboardMarkup(inline_keyboard=rows))


async def auto_kb(buttons: list[InlineKeyboardButton], *, columns: int = 2) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    current: list[InlineKeyboardButton] = []
    for btn in buttons:
        btn, size = await apply_button_settings(btn)
        if size == "full":
            if current:
                rows.append(current)
                current = []
            rows.append([btn])
            continue
        current.append(btn)
        if len(current) >= columns:
            rows.append(current)
            current = []
    if current:
        rows.append(current)
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ---------- USER KBs ----------

async def main_menu_kb(is_admin: bool = False) -> InlineKeyboardMarkup:
    buttons = [
        await mk_btn("Профиль", callback_data="nav:profile"),
        await mk_btn("Задания", callback_data="nav:tasks"),
        await mk_btn("Заработок с ферм  ", callback_data="nav:earn"),
        await mk_btn("Казино", callback_data="nav:casino"),
        await mk_btn("Фантайм", callback_data="nav:funtime"),
        await mk_btn("Кража", callback_data="nav:theft"),
        await mk_btn("Лидерборд", callback_data="nav:lb"),
        await mk_btn("Правила бота", callback_data="nav:rules"),
        await mk_btn("Промокоды  ", callback_data="nav:promo"),
    ]
    if is_admin:
        buttons.append(await mk_btn("Админ панель", callback_data="adm:home"))
    return await auto_kb(buttons)
    rows = [
        [await mk_btn("Профиль", callback_data="nav:profile"),
         await mk_btn("Задания", callback_data="nav:tasks")],
        [await mk_btn("Заработок", callback_data="nav:earn"),
         await mk_btn("Казино", callback_data="nav:casino")],
        [await mk_btn("Фантайм", callback_data="nav:funtime"),
         await mk_btn("Кража", callback_data="nav:theft")],
        [await mk_btn("Лидерборд", callback_data="nav:lb"),
         await mk_btn("Правила", callback_data="nav:rules")],
        [await mk_btn("Промокод", callback_data="nav:promo")],
    ]
    if is_admin:
        rows.append([await mk_btn("Админ панель", callback_data="adm:home")])
    return await kb(rows)


async def back_to_menu_kb() -> InlineKeyboardMarkup:
    return await kb([[await mk_btn("Назад", callback_data="nav:menu")]])


async def profile_kb() -> InlineKeyboardMarkup:
    return await auto_kb([
        await mk_btn("Бонус", callback_data="nav:bonus"),
        await mk_btn("Вывести", callback_data="nav:withdraw"),
        await mk_btn("Назад", callback_data="nav:menu"),
    ])
    return await kb([
        [await mk_btn("Бонус", callback_data="nav:bonus"),
         await mk_btn("Вывести", callback_data="nav:withdraw")],
        [await mk_btn("Назад", callback_data="nav:menu")],
    ])


async def earn_kb() -> InlineKeyboardMarkup:
    return await auto_kb([
        await mk_btn("Магазин", callback_data="nav:shop"),
        await mk_btn("Мои фармилки", callback_data="nav:farms"),
        await mk_btn("Назад", callback_data="nav:menu"),
    ])
    return await kb([
        [await mk_btn("Магазин", callback_data="nav:shop"),
         await mk_btn("Мои фармилки", callback_data="nav:farms")],
        [await mk_btn("Назад", callback_data="nav:menu")],
    ])


async def casino_kb() -> InlineKeyboardMarkup:
    return await auto_kb([
        await mk_btn("Кубик", callback_data="nav:dice"),
        await mk_btn("Баскетбол", callback_data="nav:basket"),
        await mk_btn("Назад", callback_data="nav:menu"),
    ])
    return await kb([
        [await mk_btn("Кубик", callback_data="nav:dice"),
         await mk_btn("Баскетбол", callback_data="nav:basket")],
        [await mk_btn("Назад", callback_data="nav:menu")],
    ])


async def funtime_kb() -> InlineKeyboardMarkup:
    return await auto_kb([
        await mk_btn("Обновить онлайн", callback_data="nav:funtime"),
        await mk_btn("Назад", callback_data="nav:menu"),
    ])
    return await kb([
        [await mk_btn("Обновить онлайн", callback_data="nav:funtime")],
        [await mk_btn("Назад", callback_data="nav:menu")],
    ])


async def theft_kb() -> InlineKeyboardMarkup:
    return await auto_kb([
        await mk_btn("Ограбить", callback_data="nav:rob"),
        await mk_btn("Назад", callback_data="nav:menu"),
    ])
    return await kb([
        [await mk_btn("Ограбить", callback_data="nav:rob")],
        [await mk_btn("Назад", callback_data="nav:menu")],
    ])


async def leaderboard_kb() -> InlineKeyboardMarkup:
    return await auto_kb([
        await mk_btn("Топ по рефералам", callback_data="nav:lb_refs"),
        await mk_btn("Топ по токенам", callback_data="nav:lb_tokens"),
        await mk_btn("Назад", callback_data="nav:menu"),
    ])
    return await kb([
        [await mk_btn("Топ по рефералам", callback_data="nav:lb_refs"),
         await mk_btn("Топ по токенам", callback_data="nav:lb_tokens")],
        [await mk_btn("Назад", callback_data="nav:menu")],
    ])


async def back_to_lb_kb() -> InlineKeyboardMarkup:
    return await kb([
        [await mk_btn("Назад", callback_data="nav:lb")],
        [await mk_btn("Назад", callback_data="nav:menu")],
    ])


async def back_to_earn_kb() -> InlineKeyboardMarkup:
    return await kb([
        [await mk_btn("Назад", callback_data="nav:earn")],
        [await mk_btn("Назад", callback_data="nav:menu")],
    ])


async def farms_kb() -> InlineKeyboardMarkup:
    return await kb([
        [await mk_btn("Обновить", callback_data="nav:farms")],
        [await mk_btn("Назад", callback_data="nav:earn")],
        [await mk_btn("Назад", callback_data="nav:menu")],
    ])


async def cancel_kb(target: str = "menu") -> InlineKeyboardMarkup:
    return await kb([[await mk_btn("Отмена", callback_data=f"cancel:{target}")]])


# ---------- ADMIN KBs ----------

async def admin_kb() -> InlineKeyboardMarkup:
    return await auto_kb([
        await mk_btn("Пользователи", callback_data="adm:users"),
        await mk_btn("Статистика", callback_data="adm:stats"),
        await mk_btn("Рассылка", callback_data="adm:broadcast"),
        await mk_btn("Экономика", callback_data="adm:econ"),
        await mk_btn("Каналы", callback_data="adm:channels"),
        await mk_btn("Промокоды", callback_data="adm:promo"),
        await mk_btn("Заявки на вывод", callback_data="adm:wd"),
        await mk_btn("Рефералы", callback_data="adm:refs"),
        await mk_btn("Логи", callback_data="adm:logs"),
        await mk_btn("Моя защита", callback_data="adm:protect"),
        await mk_btn("Ивенты", callback_data="adm:events"),
        await mk_btn("Админы", callback_data="adm:admins"),
        await mk_btn("Задания", callback_data="adm:tasks"),
        await mk_btn("Магазин", callback_data="adm:shop"),
        await mk_btn("Фантайм IP", callback_data="adm:funtime"),
        await mk_btn("Правила", callback_data="adm:rules"),
        await mk_btn("Фото разделов", callback_data="adm:photos"),
        await mk_btn("Тексты", callback_data="adm:texts"),
        await mk_btn("Стиль кнопок", callback_data="adm:styles"),
        await mk_btn("В меню пользователя", callback_data="nav:menu"),
    ])
    return await kb([
        [await mk_btn("Пользователи", callback_data="adm:users"),
         await mk_btn("Статистика", callback_data="adm:stats")],
        [await mk_btn("Рассылка", callback_data="adm:broadcast"),
         await mk_btn("Экономика", callback_data="adm:econ")],
        [await mk_btn("Каналы", callback_data="adm:channels"),
         await mk_btn("Промокоды", callback_data="adm:promo")],
        [await mk_btn("Заявки на вывод", callback_data="adm:wd"),
         await mk_btn("Рефералы", callback_data="adm:refs")],
        [await mk_btn("Логи", callback_data="adm:logs"),
         await mk_btn("Моя защита", callback_data="adm:protect")],
        [await mk_btn("Ивенты", callback_data="adm:events"),
         await mk_btn("Админы", callback_data="adm:admins")],
        [await mk_btn("Задания", callback_data="adm:tasks"),
         await mk_btn("Магазин", callback_data="adm:shop")],
        [await mk_btn("Фантайм IP", callback_data="adm:funtime"),
         await mk_btn("Правила", callback_data="adm:rules")],
        [await mk_btn("Фото разделов", callback_data="adm:photos"),
         await mk_btn("Тексты", callback_data="adm:texts")],
        [await mk_btn("Стиль кнопок", callback_data="adm:styles")],
        [await mk_btn("В меню пользователя", callback_data="nav:menu")],
    ])


async def admin_back_kb() -> InlineKeyboardMarkup:
    return await kb([[await mk_btn("В админ-панель", callback_data="adm:home")]])
