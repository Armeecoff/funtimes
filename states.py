from aiogram.fsm.state import State, StatesGroup


class CaptchaSG(StatesGroup):
    waiting = State()


class WithdrawSG(StatesGroup):
    nick = State()
    amount = State()


class PromoSG(StatesGroup):
    code = State()


class TokenShopPurchaseSG(StatesGroup):
    anarchy = State()
    nick = State()


class ManualTaskSG(StatesGroup):
    waiting_text = State()


class AdminBroadcast(StatesGroup):
    text = State()


class AdminEcon(StatesGroup):
    field = State()
    value = State()


class AdminChannel(StatesGroup):
    category = State()
    chat_id = State()
    max_subs = State()


class AdminPromo(StatesGroup):
    code = State()
    amount = State()
    activations = State()


class AdminTask(StatesGroup):
    task_type = State()
    channel_id = State()
    name = State()
    reward = State()
    max_completions = State()
    edit_reward = State()
    edit_max_completions = State()


class AdminShop(StatesGroup):
    emoji = State()
    name = State()
    price = State()
    income = State()
    active = State()


class AdminTokenShop(StatesGroup):
    category_emoji = State()
    category_name = State()
    category_text = State()
    category_active = State()
    category_edit_text = State()
    category_edit_emoji = State()
    item_emoji = State()
    item_name = State()
    item_description = State()
    item_price = State()
    item_active = State()


class AdminFuntime(StatesGroup):
    label = State()
    ip = State()


class AdminAddAdmin(StatesGroup):
    user_id = State()


class AdminPhoto(StatesGroup):
    waiting = State()


class AdminText(StatesGroup):
    waiting = State()


class AdminUserAction(StatesGroup):
    amount = State()


class AdminRulesEdit(StatesGroup):
    text = State()


class AdminBtnStyle(StatesGroup):
    pick_style = State()
    pick_icon = State()


class AdminAutoBroadcast(StatesGroup):
    text = State()
    time = State()


class AdminTaskReset(StatesGroup):
    time = State()


class AdminDailyThreshold(StatesGroup):
    value = State()


class AdminDailyResetTime(StatesGroup):
    value = State()
