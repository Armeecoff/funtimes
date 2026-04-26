import os
from pathlib import Path


def _load_env_file() -> None:
    env_path = Path(__file__).with_name(".env")
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        os.environ.setdefault(key, value)


_load_env_file()

BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID = int(os.environ["OWNER_ID"])
DB_PATH = os.environ.get("DB_PATH", "bot.db")

DEFAULTS = {
    "currency_name": "токенов",
    "currency_emoji": "💰",
    "start_op_enabled": "0",
    "captcha_enabled": "0",
    "tasks_op_enabled": "0",
    "admin_photo": "",
    "menu_photo": "",
    "profile_photo": "",
    "earn_photo": "",
    "casino_photo": "",
    "funtime_photo": "",
    "theft_photo": "",
    "leaderboard_photo": "",
    "rules_photo": "",
    "promo_photo": "",
    "tasks_photo": "",
    "op_photo": "",
    "menu_text": "Главное меню",
    "earn_text": "Здесь ты можешь зарабатывать. Покупай фармилки и собирай валюту!",
    "rules_text": "Правила пока не заданы.",
    "section_emoji_profile": "👤",
    "section_emoji_tasks": "📋",
    "section_emoji_casino": "🎰",
    "section_emoji_funtime": "🎮",
    "section_emoji_theft": "🥷",
    "section_emoji_leaderboard": "🏆",
    "leaderboard_place_1_icon": "🥇",
    "leaderboard_place_2_icon": "🥈",
    "leaderboard_place_3_icon": "🥉",
    "bonus_min": "10",
    "bonus_max": "100",
    "min_withdraw": "100",
    "casino_dice_bet": "10",
    "casino_dice_win": "20",
    "casino_dice_chance": "40",
    "casino_basket_bet": "10",
    "casino_basket_win": "20",
    "casino_basket_chance": "40",
    "theft_chance": "30",
    "theft_win_pct": "10",
    "theft_lose_pct": "10",
    "ref_reward": "20",
    "event_x_mult": "1",
    "event_shop_discount": "0",
    "funtime_main_ips": "play.funtime.su,play2.funtime.su,mc.funtime.su,tcp.funtime.sh,neo.funtime.sh,funtime.su,connect.funtime.su,antiddos.funtime.su",
    "btn_emoji_profile": "👤",
    "btn_emoji_tasks": "",
    "btn_emoji_earn": "💼",
    "btn_emoji_casino": "🎰",
    "btn_emoji_funtime": "🎮",
    "btn_emoji_theft": "🦹",
    "btn_emoji_leaderboard": "🏆",
    "btn_emoji_rules": "📜",
    "btn_emoji_promo": "🎁",
}
