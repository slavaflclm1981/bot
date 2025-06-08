import re
from datetime import datetime, timedelta, time
import holidays
import gspread
from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from oauth2client.service_account import ServiceAccountCredentials
import asyncio
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import logging
import os
import sys

RU_HOLIDAYS = holidays.RU(years=[2025,2026,2027])  # можно добавить нужные года

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(message)s",
        filename="bot.log",
        filemode="a",
        encoding="utf-8"
    )
    # Если хочешь видеть логи и в консоли:
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    formatter = logging.Formatter("[%(asctime)s] %(message)s")
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)

    logging.getLogger('aiogram').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    logging.getLogger('apscheduler').setLevel(logging.WARNING)

setup_logging()
logger = logging.getLogger(__name__)

def log_event(event_type: str, user_data: dict = None, details: str = ""):
    org_info = ""
    if user_data:
        org_info = f" | Организация: {user_data.get('org', 'N/A')} ({user_data.get('name', 'N/A')})"
    logger.info(f"{event_type}{org_info} | {details}")

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def get_main_inline_kb(offers_allowed=True):
    kb = []
    if offers_allowed:
        kb.append([InlineKeyboardButton(text="📨 Направить предложение о покупке", callback_data="start_offer")])
    kb.append([InlineKeyboardButton(text="💬 Помощь", callback_data="help_menu")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_reg_inline_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Регистрация", callback_data="registration")]
        ]
    )

def get_metals_inline_kb(with_cancel=True):
    kb = [
        [InlineKeyboardButton(text="Золото", callback_data="metal_gold"), InlineKeyboardButton(text="Серебро", callback_data="metal_silver")]
    ]
    if with_cancel:
        kb.append([InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_offer")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_notification_inline_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📈 Отправить котировки", callback_data="send_quotes")],
            [InlineKeyboardButton(text="🚫 Не отправлять котировки", callback_data="decline_quotes")]
        ]
    )

def get_org_type_inline_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Банк РФ", callback_data="orgtype_Банк РФ")],
            [InlineKeyboardButton(text="Организация в РФ", callback_data="orgtype_Организация в РФ")],
            [InlineKeyboardButton(text="Организация из ЕАЭС", callback_data="orgtype_Организация из ЕАЭС")],
            [InlineKeyboardButton(text="Организация вне ЕАЭС", callback_data="orgtype_Организация вне ЕАЭС")],
        ]
    )

def get_skip_inline_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Не указывать", callback_data="skip_contacts")]
        ]
    )

def get_yes_no_inline_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Да", callback_data="yes_second_metal"), InlineKeyboardButton(text="Нет", callback_data="no_second_metal")]
        ]
    )
    
def offers_today_count(user_id, metal):
    """Возвращает количество предложений пользователя по металлу на сегодня."""
    today = datetime.now().date()
    all_offers = offers_sheet.get_all_records()
    count = 0
    for row in all_offers:
        try:
            if str(row.get("ID Telegram", "")) == str(user_id) and row.get("Металл", "") == metal:
                date_str = str(row.get("Дата", "")).strip()  # Укажи точно, как называется столбец с датой!
                if date_str:
                    row_date = datetime.strptime(date_str.split()[0], "%d.%m.%Y").date()
                    if row_date == today:
                        count += 1
        except Exception:
            continue
    return count

TOKEN = "7776660810:AAE4YZm4JkZYsUdWZcngEdwz0SajINcgTas"
GOOGLE_SHEET_NAME = "Данные из бота"
CREDENTIALS_FILE = "credentials.json"
SHEET_NAME = "Пользователи"
chat_id = '-4787764944'
active_timers = {}
MAX_MESSAGE_AGE = timedelta(minutes=2)
NOTIFICATION_COLUMN = 7

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
gc = gspread.authorize(credentials)
users_sheet = gc.open(GOOGLE_SHEET_NAME).worksheet(SHEET_NAME)
offers_sheet = gc.open(GOOGLE_SHEET_NAME).worksheet("Предложения о покупке")
requests_sheet = gc.open(GOOGLE_SHEET_NAME).worksheet("Запрос")
gold_sheet = gc.open(GOOGLE_SHEET_NAME).worksheet("Золото")
silver_sheet = gc.open(GOOGLE_SHEET_NAME).worksheet("Серебро")
settings_sheet = gc.open(GOOGLE_SHEET_NAME).worksheet("Настройки")

class Form(StatesGroup):
    name = State()
    organization = State()
    org_type = State()
    contacts = State()
    offer_metal = State()
    offer_quantity = State()
    offer_quote = State()
    quote_metal = State()
    quote_value = State()
    quote_second_metal = State()
    deadline = State()
    @classmethod
    def timeout(cls):
        return timedelta(minutes=30)

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

@dp.update.middleware()
async def check_message_age_middleware(handler, event, data):
    if isinstance(event, types.Message):
        message_time = event.date.replace(tzinfo=None)
        if (datetime.now() - message_time) > MAX_MESSAGE_AGE:
            print(f"Пропущено устаревшее сообщение от {event.from_user.id}")
            return
    return await handler(event, data)

def get_user(user_id: int):
    records = users_sheet.get_all_records()
    for user in records:
        if str(user["ID Telegram"]) == str(user_id):
            return {
                "name": user["Имя"],
                "org": user["Организация"],
                "org_type": user["Тип организации"]
            }
    return None

def is_registered(user_id: int) -> bool:
    return get_user(user_id) is not None
    
def is_offer_allowed():
    try:
        settings = settings_sheet.get_all_records()
        for row in settings:
            if row.get("Настройка", "").strip() == "Разрешить отправлять предложения":
                return row.get("Признак", "").strip().lower() == "да"
    except Exception as e:
        logger.error(f"Ошибка чтения листа 'Настройки': {e}")
    return False

def is_working_day_and_hours():
    msk_tz = pytz.timezone("Europe/Moscow")
    now = datetime.now(msk_tz)
    today = now.date()
    # Будний день?
    is_weekday = now.weekday() < 6  # 1-пн, 5-пт
    # Не праздник?
    is_not_holiday = today not in RU_HOLIDAYS
    # Время рабочее?
    working_time = time(9, 0) <= now.time() <= time(23, 0)
    return is_weekday and is_not_holiday and working_time   

async def check_session_expired(chat_id: int, user_id: int) -> bool:
    state = dp.fsm.resolve_context(bot, chat_id=chat_id, user_id=user_id)
    data = await state.get_data()
    if not data.get('deadline'):
        return False
    if datetime.now() > data['deadline']:
        if user_id in active_timers:
            active_timers[user_id].cancel()
            del active_timers[user_id]
        await state.clear()
        return True
    return False

async def send_timeout_notification(user_id: int, deadline: datetime):
    user_data = get_user(user_id)
    try:
        now = datetime.now()
        wait_seconds = (deadline - now).total_seconds()
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)
        if user_id not in active_timers:
            return
        state = dp.fsm.resolve_context(bot, chat_id=user_id, user_id=user_id)
        data = await state.get_data()
        if data.get('deadline') == deadline and not data.get('timeout'):
            timestamp = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
            # Если пользователь не предоставил котировку по первому металлу
            if 'quote_value' not in data:
                gold_sheet.append_row([user_id, user_data["name"], user_data["org"], user_data["org_type"], timestamp, "Время вышло"])
                silver_sheet.append_row([user_id, user_data["name"], user_data["org"], user_data["org_type"], timestamp, "Время вышло"])
                log_event("QUOTE", user_data, "Время вышло | Не предоставлены котировки")
            # Если пользователь предоставил котировку по первому металлу, но не по второму
            elif 'quote_value' in data and 'second_metal' in data:
                second_metal = data['second_metal']
                sheet = gold_sheet if second_metal == "Золото" else silver_sheet
                sheet.append_row([user_id, user_data["name"], user_data["org"], user_data["org_type"], timestamp, "Время вышло"])
                log_event("QUOTE", user_data, f"Время вышло | Не предоставлена котировка для {second_metal}")
            elif 'quote_value' in data and 'second_metal' not in data:
                second_metal = "Серебро" if data['metal'] == "Золото" else "Золото"
                sheet = gold_sheet if second_metal == "Золото" else silver_sheet
                sheet.append_row([user_id, user_data["name"], user_data["org"], user_data["org_type"], timestamp, "Время вышло"])
                log_event("QUOTE", user_data, f"Время вышло | Предоставлена только котировка для {data['metal']}")
            await state.update_data(timeout=True)
            try:
                last_msg_id = data.get("last_inline_msg_id")
                if last_msg_id:
                    await bot.edit_message_reply_markup(chat_id=user_id, message_id=last_msg_id, reply_markup=None)
            except Exception:
                pass
            await bot.send_message(
                chat_id=user_id,
                text="⌛ Время вышло!"
            )
            await clear_state_safely(user_id, state)
    except asyncio.CancelledError:
        log_event("QUOTE", user_data, "Отказ от предоставления")
    except Exception as e:
        log_event("ERROR", None, f"Ошибка в send_timeout_notification: {e}")
    finally:
        if user_id in active_timers:
            del active_timers[user_id]

def record_decline(user_id: int):
    user_data = get_user(user_id)
    if not user_data:
        return False
    timestamp = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    gold_sheet.append_row([
        user_id,
        user_data["name"],
        user_data["org"],
        user_data["org_type"],
        timestamp,
        "Отказ от предоставления"
    ])
    silver_sheet.append_row([
        user_id,
        user_data["name"],
        user_data["org"],
        user_data["org_type"],
        timestamp,
        "Отказ от предоставления"
    ])
    return True

async def clear_state_safely(user_id: int, state: FSMContext):
    try:
        if user_id in active_timers:
            active_timers[user_id].cancel()
            del active_timers[user_id]
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка при очистке состояния для {user_id}: {e}")
        raise

def validate_name(text: str) -> tuple[bool, str]:
    text = text.strip()
    if len(text) < 3 or len(text) > 25:
        return False, "Длина имени должна быть от 3 до 25 символов"
    if not re.fullmatch(r'^[а-яА-ЯёЁa-zA-Z\s-]+$', text):
        return False, "Можно использовать только буквы, пробелы и дефис"
    return True, ""

def validate_org(text: str) -> tuple[bool, str]:
    text = text.strip()
    if len(text) < 3 or len(text) > 25:
        return False, "Длина названия должна быть от 3 до 25 символов"
    if not re.fullmatch(r'^[а-яА-ЯёЁa-zA-Z0-9\s\.,!?:;\-\'"]+$', text):
        return False, "Недопустимые символы в названии"
    return True, ""

def validate_contacts(text: str) -> tuple[bool, str]:
    if text == "Не указывать":
        return True, ""
    text = text.strip()
    if len(text) < 3 or len(text) > 25:
        return False, "Длина контактов должна быть от 3 до 25 символов"
    return True, ""

def validate_quote(text: str) -> tuple[bool, str]:
    try:
        quote = float(text.replace(",", "."))
        if not -100 <= quote <= 100:
            return False, "Котировка должна быть между -100 и 100"
        return True, ""
    except ValueError:
        return False, "Введите число (например: 1,5 или -0,5)"

async def send_scheduled_notifications():
    try:
        msk_timezone = pytz.timezone('Europe/Moscow')
        now = datetime.now(msk_timezone)
        current_time = now.strftime("%H:%M")
        logger.info(f"Проверка уведомлений в {now.strftime('%H:%M:%S')}")
        try:
            times = []
            for record in requests_sheet.get_all_records():
                send_time_str = record.get("Время отправки, МСК")
                if send_time_str:
                    try:
                        send_time = datetime.strptime(send_time_str.strip(), "%H:%M").replace(
                            year=now.year, month=now.month, day=now.day, tzinfo=msk_timezone)
                        if send_time > now:
                            times.append(send_time)
                    except ValueError:
                        continue
            if times:
                nearest = min(times)
                delta = nearest - now
                logger.info(f"Ближайшее уведомление запланировано на {nearest.strftime('%H:%M:%S')}")
            else:
                logger.info("На сегодня больше уведомлений не запланировано.")
        except Exception as e:
            logger.warning(f"Ошибка при попытке определить ближайшее уведомление: {e}")
        records = requests_sheet.get_all_records()
        for record in records:
            if not record.get("Время отправки, МСК"):
                continue
            if record["Время отправки, МСК"].strip() == current_time:
                notification_type = record.get("Тип уведомления", "").strip().lower()
                users_data = users_sheet.get_all_values()
                header = users_data[0]
                try:
                    user_id_index = header.index("ID Telegram")
                    notify_index = header.index("Отправка уведомления")
                except ValueError as e:
                    log_event("ERROR", None, f"Столбец не найден в заголовках: {header}")
                    continue
                users_to_notify = [
                    row[user_id_index] for row in users_data[1:]
                    if len(row) > notify_index and row[notify_index].strip().capitalize() == "Да"
                ]
                for user_id in users_to_notify:
                    try:
                        user_id = int(user_id)
                        # === Текстовое уведомление ===
                        if notification_type == "текст":
                            await bot.send_message(
                                chat_id=user_id,
                                text=record['Текст запроса'].strip()
                            )
                            user_data = get_user(user_id)
                            if user_data:
                                log_event("NOTIFY", user_data,
                                          f"Текст: Текстовое уведомление отправлено")
                            continue
                        # === Котировка ===
                        # Корректная обработка времени ответа:
                        response_time_str = str(record.get("Время ответа", "")).strip()
                        if response_time_str.isdigit():
                            response_time = int(response_time_str)
                        else:
                            response_time = 15  # Значение по умолчанию
                        state = dp.fsm.resolve_context(bot, chat_id=user_id, user_id=user_id)
                        if user_id in active_timers:
                            active_timers[user_id].cancel()
                        deadline = datetime.now() + timedelta(minutes=response_time)
                        await state.update_data(
                            notification_time=datetime.now(),
                            deadline=deadline
                        )
                        task = asyncio.create_task(
                            send_timeout_notification(user_id, deadline)
                        )
                        active_timers[user_id] = task
                        user_data = get_user(user_id)
                        if user_data:
                            log_event("NOTIFY", user_data,
                                      f"Текст: Уведомление отправлено | Время ответа: {response_time} мин")
                        await bot.send_message(
                            chat_id=user_id,
                            text=f"{record['Текст запроса'].strip()}\n\n⏱ На предоставление котировок даётся {response_time} минут",
                            reply_markup=get_notification_inline_kb()
                        )
                    except Exception as e:
                        log_event("ERROR", None, f"Ошибка отправки user_id={user_id}: {e}")
    except Exception as e:
        log_event("ERROR", None, f"Ошибка рассылки: {e}")

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    offers_allowed = is_offer_allowed()
    if is_registered(message.from_user.id):
        await message.answer("Главное меню:", reply_markup=get_main_inline_kb(offers_allowed=offers_allowed))
    else:
        await message.answer(
            "Добрый день! Для регистрации нажмите кнопку Регистрация:",
            reply_markup=get_reg_inline_kb()
        )

@dp.message(Command("send_offer"))
async def send_offer_command(message: types.Message, state: FSMContext):
    if not is_offer_allowed():
        await message.answer("Подача предложений временно недоступна.")
        return
    # --- Ограничение по времени и праздникам ---
    if not is_working_day_and_hours():
        await message.answer("❌ Предложения принимаются только в рабочие дни (Пн–Пт, кроме праздников) и с 09:00 до 18:00 по Москве.")
        await state.clear()
        return
    # --- конец проверки ---
    if is_registered(message.from_user.id):
        await state.set_state(Form.offer_metal)
        await message.answer(
            "Выберите, пожалуйста, металл:",
            reply_markup=get_metals_inline_kb()
        )
    else:
        await message.answer(
            "Для подачи предложения нужно пройти регистрацию.",
            reply_markup=get_reg_inline_kb()
        )

@dp.message(Command("help"))
async def help_command(message: types.Message):
    await message.answer(
        "По возникшим вопросам просьба обращаться в Отдел сопровождения продаж готовой продукции по телефону +7 812 334-36-64."
    )

@dp.callback_query(F.data == "help_menu")
async def help_menu_callback(callback: types.CallbackQuery):
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(
        "По возникшим вопросам просьба обращаться в Отдел сопровождения продаж готовой продукции по телефону +7 812 334-36-64."
    )

@dp.callback_query(F.data == "registration")
async def callback_registration(callback: types.CallbackQuery, state: FSMContext):
    if is_registered(callback.from_user.id):
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer("Вы уже зарегистрированы!")
        return
    await callback.message.edit_reply_markup(reply_markup=None)
    await state.set_state(Form.name)
    await callback.message.answer(
        "Введите, пожалуйста, Ваше имя (только буквы и дефис, 3-25 символов):"
    )

@dp.message(Form.name)
async def process_name(message: types.Message, state: FSMContext):
    is_valid, error_msg = validate_name(message.text)
    if not is_valid:
        await message.answer(f"❌ {error_msg}\nПопробуйте еще раз:")
        return
    await state.update_data(name=message.text.strip())
    await state.set_state(Form.organization)
    await message.answer(
        "Спасибо! Теперь введите, пожалуйста, название Вашей организации (3-25 символов, можно использовать цифры и знаки препинания):"
    )

@dp.message(Form.organization)
async def process_org(message: types.Message, state: FSMContext):
    is_valid, error_msg = validate_org(message.text)
    if not is_valid:
        await message.answer(f"❌ {error_msg}\nПопробуйте еще раз:")
        return
    await state.update_data(organization=message.text.strip())
    await state.set_state(Form.org_type)
    await message.answer(
        "Теперь выберите тип Вашей организации:",
        reply_markup=get_org_type_inline_kb()
    )

@dp.callback_query(lambda call: call.data.startswith("orgtype_"))
async def process_org_type_cb(callback: types.CallbackQuery, state: FSMContext):
    org_type = callback.data[len("orgtype_") :]
    await callback.message.edit_reply_markup(reply_markup=None)
    await state.update_data(org_type=org_type)
    await state.set_state(Form.contacts)
    await callback.message.answer(
        "Оставьте, пожалуйста, Ваши контакты (телефон/почта) или нажмите «Не указывать»:",
        reply_markup=get_skip_inline_kb()
    )

@dp.callback_query(F.data == "skip_contacts")
async def skip_contacts_cb(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_reply_markup(reply_markup=None)
    data = await state.get_data()
    users_sheet.append_row([
        datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
        callback.from_user.id,
        data['name'],
        data['organization'],
        "Не указано",
        data['org_type'],
        "Да"
    ])
    user_data = {
        "name": data['name'],
        "org": data['organization'],
        "org_type": data['org_type']
    }
    log_event("REGISTER", user_data, "Новая регистрация пользователя")
    await clear_state_safely(callback.from_user.id, state)
    await callback.message.answer("✅ Отлично, регистрация завершена! Теперь вы можете направлять запросы о покупке драгоценных металлов.")

@dp.message(Form.contacts)
async def process_contacts(message: types.Message, state: FSMContext):
    is_valid, error_msg = validate_contacts(message.text)
    if not is_valid:
        await message.answer(
            f"❌ {error_msg}\nПопробуйте, пожалуйста, еще раз или нажмите «Не указывать»:",
            reply_markup=get_skip_inline_kb()
        )
        return
    data = await state.get_data()
    users_sheet.append_row([
        datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
        message.from_user.id,
        data['name'],
        data['organization'],
        message.text.strip(),
        data['org_type'],
        "Да"
    ])
    user_data = {
        "name": data['name'],
        "org": data['organization'],
        "org_type": data['org_type']
    }
    log_event("REGISTER", user_data, "Новая регистрация пользователя")
    await clear_state_safely(message.from_user.id, state)
    await message.answer("✅ Отлично, регистрация завершена! Теперь вы можете направлять запросы о покупке драгоценных металлов.")

# --- Подача предложения о покупке ---
@dp.callback_query(Form.offer_metal, lambda call: call.data in ["metal_gold", "metal_silver"])
async def process_offer_metal_cb(callback: types.CallbackQuery, state: FSMContext):
    if not is_working_day_and_hours():
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer("❌ Предложения принимаются только в рабочие дни (Пн–Пт, кроме праздников) и с 09:00 до 18:00 по Москве.")
        await state.clear()
        return
    metal = "Золото" if callback.data == "metal_gold" else "Серебро"
    # --- Проверка лимита ---
    user_id = callback.from_user.id
    count = offers_today_count(user_id, metal)
    if count >= 2:
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer(
            f"❌ Вы уже отправили 2 предложения по металлу {metal} сегодня. Новое предложение можно будет отправить завтра."
        )
        await state.clear()
        return
    # --- /конец проверки ---
    await callback.message.edit_reply_markup(reply_markup=None)
    await state.update_data(metal=metal)
    await state.set_state(Form.offer_quantity)
    await callback.message.answer(
        "Введите, пожалуйста, массу партии в кг (например: <code>100</code>):",
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "start_offer")
async def callback_start_offer(callback: types.CallbackQuery, state: FSMContext):
    if not is_offer_allowed():
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer("Подача предложений временно недоступна.")
        return
    # --- Ограничение по времени ---
    if not is_working_day_and_hours():
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer("❌ Предложения принимаются только в рабочие дни (Пн–Пт, кроме праздников) и с 09:00 до 18:00 по Москве.")
        await state.clear()
        return
    # --- конец проверки ---
    if not is_registered(callback.from_user.id):
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer("❌ Сначала пройдите регистрацию!", reply_markup=get_reg_inline_kb())
        return
    user_data = get_user(callback.from_user.id)
    if not user_data:
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer("❌ Ошибка: данные пользователя не найдены!")
        return
    await callback.message.edit_reply_markup(reply_markup=None)
    await state.set_state(Form.offer_metal)
    await callback.message.answer(
        "Выберите, пожалуйста, металл:",
        reply_markup=get_metals_inline_kb()
    )

@dp.callback_query(F.data == "cancel_offer")
async def cancel_offer_cb(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_reply_markup(reply_markup=None)
    await clear_state_safely(callback.from_user.id, state)
    await callback.message.answer(
        "❌ Очень жаль, что вы отказались предоставить предложение. Хорошего Вам дня"
    )

@dp.message(Form.offer_quantity)
async def process_offer_quantity(message: types.Message, state: FSMContext):
    try:
        quantity = float(message.text.replace(",", "."))
    except ValueError:
        await message.answer(
            "❌ Введите, пожалуйста, положительное число. Например: <code>100</code>",
            parse_mode="HTML"
        )
        return
    if quantity < 10 or quantity > 10000:
        await message.answer(
            "❌ Количество должно быть не меньше 10 и не больше 10 000 кг. Попробуйте снова:",
            parse_mode="HTML"
        )
        return

    await state.update_data(quantity=quantity)
    await state.set_state(Form.offer_quote)
    await message.answer(
        "Введите, пожалуйста, котировку в % (в случае премии число без знаков, например, <code>1.5</code> , а в случае дисконта с минусом <code>-0.5</code>):",
        parse_mode="HTML"
    )

@dp.message(Form.offer_quote)
async def process_offer_quote(message: types.Message, state: FSMContext):
    is_valid, error_msg = validate_quote(message.text)
    if not is_valid:
        await message.answer(
            f"❌ {error_msg}\nПопробуйте еще раз:"
        )
        return
    quote = float(message.text.replace(",", "."))
    data = await state.get_data()
    user_data = get_user(message.from_user.id)
    if not user_data:
        await message.answer("❌ Ошибка: данные пользователя не найдены!")
        await state.clear()
        return
    today = datetime.now().date()
    metal = data['metal']  # используем металл из state!
    all_offers = offers_sheet.get_all_records()
    user_offers_today = []
    for row in all_offers:
        try:
            if (
                str(row.get("ID Telegram", "")) == str(message.from_user.id)
                and str(row.get("Металл", "")) == metal  # сравнение по металлу
            ):
                date_str = str(row.get("Дата", "")).strip()  # или "Дата и время"
                if date_str:
                    row_date = datetime.strptime(date_str.split()[0], "%d.%m.%Y").date()
                    if row_date == today:
                        user_offers_today.append(row)
        except Exception:
            continue
    if len(user_offers_today) >= 2:
        await message.answer(f"❌ Вы уже отправили 2 предложения по металлу {metal} сегодня. Новое предложение можно будет отправить завтра.")
        await state.clear()
        return

    records = users_sheet.get_all_records()
    contacts = "Не указаны"
    for user in records:
        if str(user["ID Telegram"]) == str(message.from_user.id):
            contacts = user.get("Контакты", "Не указаны")
            break
    offers_sheet.append_row([
        message.from_user.id,
        user_data["name"],
        user_data["org"],
        user_data["org_type"],
        datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
        data['metal'],
        data['quantity'],
        quote
    ])
    log_event("OFFER", user_data, 
              f"Металл: {data['metal']} | Масса: {data['quantity']}кг | Котировка: {quote}%")
    await state.clear()
    await message.answer(
        f"✅ Спасибо! Ваше предложение принято:\n"
        f"• Металл: {data['metal']}\n"
        f"• Масса: {data['quantity']} кг\n"
        f"• Котировка: {quote}%"
    )
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=f"📨 Новое предложение о покупке:\n"
                 f"• От: {user_data['org']} ({user_data['name']})\n"
                 f"• Контакты: {contacts}\n"
                 f"• Металл: {data['metal']}\n"
                 f"• Масса: {data['quantity']} кг\n"
                 f"• Котировка: {quote}%",
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка при отправке в группу: {e}")

# --- Ответ на уведомление (котировки), только gold/silver, никаких сообщений в группу! ---
@dp.callback_query(Form.quote_metal, lambda call: call.data in ["metal_gold", "metal_silver"])
async def process_quote_metal_cb(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if data.get("timeout"):
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer("⌛ Время для предоставления котировок вышло!")
        return
    await callback.message.edit_reply_markup(reply_markup=None)
    await state.update_data(last_inline_msg_id=callback.message.message_id)
    metal = "Золото" if callback.data == "metal_gold" else "Серебро"
    await state.update_data(metal=metal)
    await state.set_state(Form.quote_value)
    await callback.message.answer(
        "Введите, пожалуйста, котировку в % (в случае премии число без знаков, например, <code>1.5</code>, а в случае дисконта с минусом <code>-0.5</code>):",
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "send_quotes")
async def callback_send_quotes(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if data.get("timeout"):
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer("⌛ Время для предоставления котировок вышло!")
        return
    await callback.message.edit_reply_markup(reply_markup=None)
    await state.update_data(last_inline_msg_id=callback.message.message_id)
    await callback.message.answer(
        "Выберите, пожалуйста, первый металл для предоставления котировок. Котировку по второму металлу можно будет не отправлять.",
        reply_markup=get_metals_inline_kb(with_cancel=False)
    )
    await state.set_state(Form.quote_metal)

@dp.callback_query(F.data == "decline_quotes")
async def callback_decline_quotes(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if data.get("timeout"):
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer("⌛ Время для предоставления котировок вышло!")
        return
    if callback.from_user.id in active_timers:
        active_timers[callback.from_user.id].cancel()
        del active_timers[callback.from_user.id]
    if not record_decline(callback.from_user.id):
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer("❌ Ошибка при обработке запроса")
        return
    await callback.message.edit_reply_markup(reply_markup=None)
    await clear_state_safely(callback.from_user.id, state)
    await callback.message.answer(
        "Очень жаль! Желаем Вам хорошего дня!"
    )

@dp.message(Form.quote_value)
async def process_quote_value(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if data.get("timeout"):
        await message.answer("⌛ Время для предоставления котировок вышло!")
        return
    await state.update_data(last_inline_msg_id=message.message_id)
    is_valid, error_msg = validate_quote(message.text)
    if not is_valid:
        await message.answer(f"❌ {error_msg}\nПопробуйте еще раз:")
        return
    quote = float(message.text.replace(",", "."))
    user_data = get_user(message.from_user.id)
    current_metal = data['metal']
    if user_data:
        log_event("QUOTE", user_data, f"Металл: {current_metal} | {quote}%")
    sheet = gold_sheet if current_metal == "Золото" else silver_sheet
    sheet.append_row([
        message.from_user.id,
        user_data["name"],
        user_data["org"],
        user_data["org_type"],
        datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
        quote
    ])
    await state.update_data(quote_value=quote)
    if 'second_metal' not in data:
        second_metal = "Серебро" if current_metal == "Золото" else "Золото"
        await state.update_data(second_metal=second_metal)
        msg = await message.answer(
            f"✅ Спасибо, котировка на {current_metal} сохранена!\n"
            f"Хотите отправить котировку на {second_metal}?",
            reply_markup=get_yes_no_inline_kb()
        )
        await state.update_data(last_inline_msg_id=msg.message_id)
        await state.set_state(Form.quote_second_metal)
    else:
        await message.answer("✅ Спасибо! Обе котировки сохранены! Хорошего Вам дня!")
        await clear_state_safely(message.from_user.id, state)

@dp.callback_query(F.data == "yes_second_metal")
async def yes_second_metal_cb(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if data.get("timeout"):
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer("⌛ Время для предоставления котировок вышло!")
        return
    await callback.message.edit_reply_markup(reply_markup=None)
    await state.update_data(last_inline_msg_id=callback.message.message_id)
    second_metal = data.get('second_metal')
    await callback.message.answer(
        f"Введите котировку для {second_metal} в %:",
        parse_mode="HTML"
    )
    await state.update_data(metal=second_metal)
    await state.set_state(Form.quote_value)

@dp.callback_query(F.data == "no_second_metal")
async def no_second_metal_cb(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if data.get("timeout"):
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer("⌛ Время для предоставления котировок вышло!")
        return
    await callback.message.edit_reply_markup(reply_markup=None)
    second_metal = data.get('second_metal')
    user_data = get_user(callback.from_user.id)
    if user_data:
        log_event("QUOTE", user_data, f"Отказ от предоставления котировки для {second_metal}")
    sheet = gold_sheet if second_metal == "Золото" else silver_sheet
    sheet.append_row([
        callback.from_user.id,
        user_data["name"],
        user_data["org"],
        user_data["org_type"],
        datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
        "Отказ от предоставления"
    ])
    await callback.message.answer(
        "Спасибо за предоставленную котировку! Желаем хорошего дня!"
    )
    await clear_state_safely(callback.from_user.id, state)

async def on_startup(bot: Bot):
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Вебхук удален, старые сообщения пропущены")
    logger.info("Подключение к Telegram API успешно")
    if not os.path.exists("bot.log"):
        with open("bot.log", "w") as f:
            f.write("")
    try:
        test_data = users_sheet.get_all_records()
        log_event("SYSTEM", None, f"Подключение к Google Sheets успешно | Пользователей: {len(test_data)}")
    except Exception as e:
        log_event("ERROR", None, f"Ошибка доступа к Google Sheets: {e}")
        return
    log_event("SYSTEM", None, "Бот успешно запущен")

async def health_check():
    while True:
        logger.info("Бот жив…")
        await asyncio.sleep(5 * 60)

async def main():
    await on_startup(bot)
    asyncio.create_task(health_check())
    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
    scheduler.add_job(
        send_scheduled_notifications,
        trigger=CronTrigger(minute="*"),
    )
    scheduler.start()
    await dp.start_polling(bot, skip_updates=True)

if __name__ == '__main__':
    asyncio.run(main())