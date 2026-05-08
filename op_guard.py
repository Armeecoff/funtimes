from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware, Bot
from aiogram.types import CallbackQuery, Message, TelegramObject

from db import get_setting, is_admin, fetchone, increment_user_daily_action
from utils import (
    build_subscription_gate_kb,
    build_subscription_gate_text,
    check_user_subscriptions,
    send_section,
)


class StartOpGuardMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        bot = data.get("bot")
        user = getattr(event, "from_user", None)
        if not isinstance(bot, Bot) or user is None:
            return await handler(event, data)

        banned = await fetchone("SELECT banned FROM users WHERE user_id=?", (user.id,))
        if banned and banned[0]:
            return None

        if (await get_setting("maintenance_enabled", "0")) == "1" and not await is_admin(user.id):
            await self._show_maintenance(event)
            return None

        if await self._is_allowed_without_start_op(event):
            return await handler(event, data)

        if (await get_setting("start_op_enabled", "0")) != "1":
            if not await is_admin(user.id):
                await increment_user_daily_action(user.id)
            return await handler(event, data)

        if await self._is_admin_event(event, data):
            return await handler(event, data)

        not_subbed = await check_user_subscriptions(bot, user.id, "start")
        if not not_subbed:
            await increment_user_daily_action(user.id)
            return await handler(event, data)

        await self._show_start_op_gate(event, not_subbed)
        return None

    async def _is_allowed_without_start_op(self, event: TelegramObject) -> bool:
        if isinstance(event, Message):
            text = (event.text or "").strip()
            command = text.split(maxsplit=1)[0] if text else ""
            return command == "/start" or command.startswith("/start@")

        if isinstance(event, CallbackQuery):
            return (event.data or "") == "op_check:start"

        return False

    async def _is_admin_event(self, event: TelegramObject, data: dict[str, Any]) -> bool:
        user = getattr(event, "from_user", None)
        if user is None or not await is_admin(user.id):
            return False

        if isinstance(event, Message):
            state = data.get("state")
            if state is not None:
                current_state = await state.get_state()
                if current_state and current_state.startswith("Admin"):
                    return True

            text = (event.text or "").strip()
            command = text.split(maxsplit=1)[0].lower() if text else ""
            command = command.split("@", 1)[0]
            return command in {"/admin", "/search", "/scs"}

        if isinstance(event, CallbackQuery):
            value = event.data or ""
            admin_exact = {"adm_add", "cancel:admin", "task_add"}
            admin_prefixes = (
                "adm:",
                "adm_",
                "u_",
                "stat_ch:",
                "ch_",
                "econ:",
                "promo_",
                "wd_",
                "ref_",
                "logs:",
                "ev_",
                "task_submission:",
                "task_approve:",
                "task_reject:",
                "task_del:",
                "task_kind:",
                "task_channel:",
                "shop_",
                "tsadm:",
                "ft_",
                "maint:",
                "photo_set:",
                "text_set:",
                "style_",
                "size_",
                "icon_",
                "abr:",
                "treset:",
                "task_toggle_reset:",
                "adm:daily_threshold",
                "ch_del_yes:",
                "shop_del_yes:",
                "tsadm:cat_del_yes:",
                "tsadm:item_del_yes:",
                "ft_del_yes:",
                "adm_fire_yes:",
            )
            return value in admin_exact or value.startswith(admin_prefixes)

        return False

    async def _show_start_op_gate(
        self,
        event: TelegramObject,
        channels: list[tuple[str, str]],
    ) -> None:
        text = build_subscription_gate_text(channels, "бота")
        markup = build_subscription_gate_kb(channels, "start")

        if isinstance(event, CallbackQuery):
            try:
                await event.answer("Сначала подпишитесь на обязательные каналы", show_alert=True)
            except Exception:
                pass
            if event.message:
                await send_section(event, text, "op_photo", reply_markup=markup)
            return

        if isinstance(event, Message):
            await send_section(event, text, "op_photo", reply_markup=markup)

    async def _show_maintenance(self, event: TelegramObject) -> None:
        text = (await get_setting("maintenance_text", "")).strip()
        if not text:
            text = "<b>Технический перерыв</b>\n\nБот временно недоступен. Попробуйте позже."

        if isinstance(event, CallbackQuery):
            try:
                await event.answer("Технический перерыв", show_alert=True)
            except Exception:
                pass
            if event.message:
                await send_section(event, text, "maintenance_photo")
            return

        if isinstance(event, Message):
            await send_section(event, text, "maintenance_photo")
