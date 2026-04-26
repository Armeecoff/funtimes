from aiogram.fsm.state import State, StatesGroup


class CaptchaSG(StatesGroup):
    waiting = State()


class WithdrawSG(StatesGroup):
    nick = State()
    amount = State()


class PromoSG(StatesGroup):
    code = State()


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


class AdminShop(StatesGroup):
    emoji = State()
    name = State()
    price = State()
    income = State()
    active = State()


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
