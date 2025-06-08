import re
from datetime import datetime, timedelta
import gspread
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
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

def setup_logging():
    logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    stream=sys.stdout  # <-- Это важно!
    )
    # Отключаем лишние логи
    logging.getLogger('aiogram').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    logging.getLogger('apscheduler').setLevel(logging.WARNING)

# Вызываем настройку логирования при импорте
setup_logging()
logger = logging.getLogger(__name__)

def log_event(event_type: str, user_data: dict = None, details: str = ""):
    org_info = ""
    if user_data:
        org_info = f" | Организация: {user_data.get('org', 'N/A')} ({user_data.get('name', 'N/A')})"
    logger.info(f"{event_type}{org_info} | {details}")

# --- Константы клавиатур ---
REG_KB = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Регистрация")]],
    resize_keyboard=True
)

MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="📨 Направить предложение о покупке")]],
    resize_keyboard=True
)

METALS_KB_WITH_CANCEL = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Золото"), KeyboardButton(text="Серебро")],
        [KeyboardButton(text="❌ Отмена")]
    ],
    resize_keyboard=True
)

METALS_KB_NO_CANCEL = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Золото"), KeyboardButton(text="Серебро")]
    ],
    resize_keyboard=True
)

CANCEL_ONLY_KB = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="❌ Отмена")]],
    resize_keyboard=True
)

SKIP_KB = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Не указывать")]],
    resize_keyboard=True
)

NOTIFICATION_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📈 Отправить котировки")],
        [KeyboardButton(text="🚫 Не отправлять котировки")]
    ],
    resize_keyboard=True
)

ORG_TYPE_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Банк РФ")],
        [KeyboardButton(text="Организация в РФ")],
        [KeyboardButton(text="Организация из ЕАЭС")],
        [KeyboardButton(text="Организация вне ЕАЭС")]
    ],
    resize_keyboard=True
)

# --- Настройки ---
TOKEN = "7776660810:AAE4YZm4JkZYsUdWZcngEdwz0SajINcgTas"
GOOGLE_SHEET_NAME = "Данные из бота"
CREDENTIALS_FILE = "credentials.json"
SHEET_NAME = "Пользователи"
chat_id = '-4787764944'
active_timers = {}
MAX_MESSAGE_AGE = timedelta(minutes=2)
NOTIFICATION_COLUMN = 7

# --- Подключение к Google Sheets ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
gc = gspread.authorize(credentials)
users_sheet = gc.open(GOOGLE_SHEET_NAME).worksheet(SHEET_NAME)
offers_sheet = gc.open(GOOGLE_SHEET_NAME).worksheet("Предложения о покупке")
requests_sheet = gc.open(GOOGLE_SHEET_NAME).worksheet("Запрос")
gold_sheet = gc.open(GOOGLE_SHEET_NAME).worksheet("Золото")
silver_sheet = gc.open(GOOGLE_SHEET_NAME).worksheet("Серебро")

# --- Состояния FSM ---
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

# --- Инициализация бота ---
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# NEW: Мидлварь для проверки возраста сообщений
@dp.update.middleware()
async def check_message_age_middleware(handler, event, data):
    if isinstance(event, types.Message):
        message_time = event.date.replace(tzinfo=None)
        if (datetime.now() - message_time) > MAX_MESSAGE_AGE:
            print(f"Пропущено устаревшее сообщение от {event.from_user.id}")
            return
    return await handler(event, data)

# --- Вспомогательные функции ---
def get_user(user_id: int):
    """Получает данные пользователя из таблицы по ID Telegram"""
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
    """Проверяет, зарегистрирован ли пользователь"""
    return get_user(user_id) is not None
    
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

        if data.get('deadline') == deadline:
            if not user_data:
                return
                
            timestamp = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
            
            if 'quote_value' not in data:
                gold_sheet.append_row([user_id, user_data["name"], user_data["org"], user_data["org_type"], timestamp, "Время вышло"])
                silver_sheet.append_row([user_id, user_data["name"], user_data["org"], user_data["org_type"], timestamp, "Время вышло"])
                log_event("QUOTE", user_data, "Время вышло | Не предоставлены котировки")
            
            elif 'quote_value' in data and 'second_metal' not in data:
                second_metal = "Серебро" if data['metal'] == "Золото" else "Золото"
                if second_metal == "Золото":
                    gold_sheet.append_row([user_id, user_data["name"], user_data["org"], user_data["org_type"], timestamp, "Время вышло"])
                else:
                    silver_sheet.append_row([user_id, user_data["name"], user_data["org"], user_data["org_type"], timestamp, "Время вышло"])
                log_event("QUOTE", user_data, f"Время вышло | Предоставлена только котировка для {data['metal']}")
            
            elif 'second_metal' in data and 'second_quote' not in data:
                second_metal = data['second_metal']
                if second_metal == "Золото":
                    gold_sheet.append_row([user_id, user_data["name"], user_data["org"], user_data["org_type"], timestamp, "Время вышло"])
                else:
                    silver_sheet.append_row([user_id, user_data["name"], user_data["org"], user_data["org_type"], timestamp, "Время вышло"])
                log_event("QUOTE", user_data, f"Время вышло | Не предоставлена котировка для {second_metal}")
            
            await bot.send_message(
                chat_id=user_id,
                text="⌛ Время вышло!",
                reply_markup=MAIN_KB
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
    """Записывает отказ пользователя в таблицы"""
    user_data = get_user(user_id)
    if not user_data:
        return False
    
    timestamp = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    
    # Запись в лист "Золото"
    gold_sheet.append_row([
        user_id,
        user_data["name"],
        user_data["org"],
        user_data["org_type"],
        timestamp,
        "Отказ от предоставления"
    ])
    
    # Запись в лист "Серебро"
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
    """Безопасная очистка состояния с отменой таймера"""
    try:
        if user_id in active_timers:
            active_timers[user_id].cancel()
            del active_timers[user_id]
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка при очистке состояния для {user_id}: {e}")
        raise  # Пробрасываем исключение дальше для диагностики   

def record_quote(user_id: int, metal: str, quote: float):
    """Записывает котировку в соответствующий лист"""
    user_data = get_user(user_id)
    if not user_data:
        return False
    
    sheet = gold_sheet if metal == "Золото" else silver_sheet
    timestamp = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    
    sheet.append_row([
        user_id,
        user_data["name"],
        user_data["org"],
        user_data["org_type"],
        timestamp,
        quote
    ])
    
    return True

# --- Валидация данных ---
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

        # Логика для нахождения ближайшего уведомления
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
                response_time = int(record.get("Время ответа", 15))
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
                            text=f"{record['Текст уведомления'].strip()}\n\n⏱ На предоставление котировок даётся {response_time} минут",
                            reply_markup=NOTIFICATION_KB
                        )
                    except Exception as e:
                        log_event("ERROR", None, f"Ошибка отправки user_id={user_id}: {e}")
    except Exception as e:
        log_event("ERROR", None, f"Ошибка рассылки: {e}")

# --- Обработчики регистрации ---

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if is_registered(message.from_user.id):
        await message.answer("Главное меню:", reply_markup=MAIN_KB)
    else:
        await message.answer(
            "Добрый день! Для регистрации нажмите кнопку Регистрация:",
            reply_markup=REG_KB
        )

@dp.message(lambda message: message.text == "Регистрация")
async def start_registration(message: types.Message, state: FSMContext):
    if is_registered(message.from_user.id):
        await message.answer("Вы уже зарегистрированы!", reply_markup=MAIN_KB)
        return
    await state.set_state(Form.name)
    await message.answer(
        "Введите, пожалуйста, Ваше имя (только буквы и дефис, 3-25 символов):",
        reply_markup=types.ReplyKeyboardRemove()
    )

@dp.message(Form.name)
async def process_name(message: types.Message, state: FSMContext):
    is_valid, error_msg = validate_name(message.text)
    if not is_valid:
        await message.answer(f"❌ {error_msg}\nПопробуйте еще раз:", reply_markup=types.ReplyKeyboardRemove())
        return
    await state.update_data(name=message.text.strip())
    await state.set_state(Form.organization)
    await message.answer(
        "Спасибо! Теперь введите, пожалуйста, название Вашей организации (3-25 символов, можно использовать цифры и знаки препинания):",
        reply_markup=types.ReplyKeyboardRemove()
    )


@dp.message(Form.organization)
async def process_org(message: types.Message, state: FSMContext):
    is_valid, error_msg = validate_org(message.text)
    if not is_valid:
        await message.answer(f"❌ {error_msg}\nПопробуйте еще раз:", reply_markup=types.ReplyKeyboardRemove())
        return
    
    await state.update_data(organization=message.text.strip())
    await state.set_state(Form.org_type)  # NEW: переходим к выбору типа организации
    await message.answer(
        "Теперь выберите тип Вашей организации:",
        reply_markup=ORG_TYPE_KB
    )

@dp.message(Form.org_type)
async def process_org_type(message: types.Message, state: FSMContext):
    org_types = ["Банк РФ", "Организация в РФ", "Организация из ЕАЭС", "Организация вне ЕАЭС"]
    
    if message.text not in org_types:
        await message.answer("❌ Пожалуйста, выберите тип Вашей организации из предложенных вариантов!")
        return
    
    await state.update_data(org_type=message.text)
    await state.set_state(Form.contacts)
    await message.answer(
        "Оставьте, пожалуйста, Ваши контакты (телефон/почта) или нажмите «Не указывать»:",
        reply_markup=SKIP_KB  
    )

@dp.message(Form.contacts)
# В process_contacts (регистрация)
async def process_contacts(message: types.Message, state: FSMContext):
  try:
    if message.text == "Не указывать":
        contacts = "Не указано"
    else:
        is_valid, error_msg = validate_contacts(message.text)
        if not is_valid:
            await message.answer(
                f"❌ {error_msg}\nПопробуйте, пожалуйста, еще раз или нажмите «Не указывать»:",
                reply_markup=SKIP_KB
            )
            return
        contacts = message.text.strip()

    data = await state.get_data()
    users_sheet.append_row([
        datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
        message.from_user.id,
        data['name'],
        data['organization'],
        contacts,
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
    await message.answer("✅ Отлично, регистрация завершена! Теперь вы можете направлять запросы о покупке драгоценных металлов и, по желанию, предоставлять котировки! Желаем Вам хорошего дня!", reply_markup=MAIN_KB)
  except Exception as e:
        logger.error(f"Ошибка в process_contacts: {e}")
        await message.answer("❌ Произошла ошибка при регистрации. Пожалуйста, попробуйте ещё раз.")
# --- Обработчики предложений ---

@dp.message(lambda message: message.text == "📨 Направить предложение о покупке")
async def start_offer(message: types.Message, state: FSMContext):
    if not is_registered(message.from_user.id):
        await message.answer("❌ Сначала пройдите регистрацию!", reply_markup=REG_KB)
        return
    
    user_data = get_user(message.from_user.id)
    if not user_data:
        await message.answer("❌ Ошибка: данные пользователя не найдены!")
        return
    
    await state.set_state(Form.offer_metal)
    await message.answer(
        "Выберите, пожалуйста, металл:",
        reply_markup=METALS_KB_WITH_CANCEL
    )

async def cancel_offer(message: types.Message, state: FSMContext):
    await clear_state_safely(message.from_user.id, state)
    await message.answer(
        "❌ Очень жаль, что вы отказались предоставить предложение. Хорошего Вам дня",
        reply_markup=MAIN_KB
    )

@dp.message(Form.offer_metal)
async def process_offer_metal(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await cancel_offer(message, state)
        return
        
    if message.text not in ["Золото", "Серебро"]:
        await message.answer("❌ Выберите, пожалуйста, вариант из кнопок", reply_markup=METALS_KB)
        return
    
    await state.update_data(metal=message.text)
    await state.set_state(Form.offer_quantity)
    await message.answer(
        "Введите, пожалуйста, массу партии в кг (например: <code>100</code>):",
        reply_markup=types.ReplyKeyboardRemove(),
        parse_mode="HTML"
    )

@dp.message(Form.offer_quantity)
async def process_offer_quantity(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await cancel_offer(message, state)
        return
    
    try:
        quantity = float(message.text.replace(",", "."))
        if quantity <= 0:
            raise ValueError
    except ValueError:
        await message.answer(
            "❌ Введите, пожалуйста, положительное число. Например: <code>3.5</code>",
            reply_markup=types.ReplyKeyboardRemove(),
            parse_mode="HTML"
        )
        return
    
    await state.update_data(quantity=quantity)
    await state.set_state(Form.offer_quote)
    await message.answer(
        "Введите, пожалуйста, котировку в % (в случае премии число без знаков, например, <code>1.5</code> , а в случае дисконта число со знаком - : <code>-0.5</code>):",
        reply_markup=types.ReplyKeyboardRemove(),
        parse_mode="HTML"
    )

@dp.message(Form.offer_quote)
async def process_offer_quote(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await cancel_offer(message, state)
        return
    
    is_valid, error_msg = validate_quote(message.text)
    if not is_valid:
        await message.answer(
            f"❌ {error_msg}\nПопробуйте еще раз:",
            reply_markup=types.ReplyKeyboardRemove()
        )
        return
    
    quote = float(message.text.replace(",", "."))
    data = await state.get_data()
    user_data = get_user(message.from_user.id)
    
    if not user_data:
        await message.answer("❌ Ошибка: данные пользователя не найдены!")
        await state.clear()
        return
    
    # Получаем контакты пользователя из Google Sheets
    records = users_sheet.get_all_records()
    contacts = "Не указаны"
    for user in records:
        if str(user["ID Telegram"]) == str(message.from_user.id):
            contacts = user.get("Контакты", "Не указаны")
            break
    
    # Запись в таблицу
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
        f"• Котировка: {quote}%",
        reply_markup=MAIN_KB
    )
    
    # Отправка уведомления в группу
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

@dp.message(lambda message: message.text == "📈 Отправить котировки")
async def start_quotes(message: types.Message, state: FSMContext):
    # Проверяем время
    if await check_session_expired(
        chat_id=message.chat.id,
        user_id=message.from_user.id
    ):
        await message.answer("⌛ Время для предоставления котировок вышло!", reply_markup=MAIN_KB)
        return
    
    await message.answer(
        "Выберите, пожалуйста, первый металл для предоставления котировок. Котировку по второму металлу можно будет предоставить на следующем этапе:",
        reply_markup=METALS_KB_NO_CANCEL
    )
    await state.set_state(Form.quote_metal)

@dp.message(lambda message: message.text == "🚫 Не отправлять котировки")
async def handle_decline(message: types.Message, state: FSMContext):
    # Отменяем таймер, если он есть
    if message.from_user.id in active_timers:
        active_timers[message.from_user.id].cancel()
        del active_timers[message.from_user.id]
    
    # Записываем отказ в таблицы
    if not record_decline(message.from_user.id):
        await message.answer("❌ Ошибка при обработке запроса")
        return
    
    # Очищаем состояние
    await clear_state_safely(message.from_user.id, state)
    
    await message.answer(
        "Очень жаль! Желаем Вам хорошего дня!",
        reply_markup=MAIN_KB
    )
    
@dp.message(Form.quote_metal)
async def process_quote_metal(message: types.Message, state: FSMContext):
    if await check_session_expired(message.chat.id, message.from_user.id):
        await message.answer("⌛ Время для предоставления котировок вышло!", reply_markup=MAIN_KB)
        return
        
    if message.text not in ["Золото", "Серебро"]:  # Убрали проверку на "Отмену"
        await message.answer("❌ Выберите, пожалуйста, вариант из кнопок!", reply_markup=METALS_KB_NO_CANCEL)
        return
        
    if message.text not in ["Золото", "Серебро"]:
        await message.answer("❌ Выберите, пожалуйста, вариант из кнопок!", reply_markup=METALS_KB)
        return
    
    await state.update_data(metal=message.text)
    await message.answer(
        "Введите, пожалуйста, котировку в % (в случае премии число без знаков, например, <code>1.5</code>, а в случае дисконта число со знаком - : <code>-0.5</code>):",
        reply_markup=types.ReplyKeyboardRemove(),
        parse_mode="HTML"
    )
    await state.set_state(Form.quote_value)  # Используем новое состояние

@dp.message(Form.quote_value)
async def process_quote_value(message: types.Message, state: FSMContext):
    if await check_session_expired(chat_id=message.chat.id, user_id=message.from_user.id):
        await message.answer("⌛ Время для предоставления котировок вышло!", reply_markup=MAIN_KB)
        return
    
    if message.text == "❌ Отмена":
        await cancel_offer(message, state)
        return
    
    is_valid, error_msg = validate_quote(message.text)
    if not is_valid:
        await message.answer(f"❌ {error_msg}\nПопробуйте еще раз:", reply_markup=types.ReplyKeyboardRemove())
        return
    
    quote = float(message.text.replace(",", "."))
    data = await state.get_data()
    user_data = get_user(message.from_user.id)
    
    current_metal = data['metal']
    if user_data:
        log_event("QUOTE", user_data, f"Металл: {current_metal} | {quote}%")
    
    if current_metal == "Золото":
        gold_sheet.append_row([
            message.from_user.id,
            user_data["name"],
            user_data["org"],
            user_data["org_type"],
            datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
            quote
        ])
        await state.update_data(gold_recorded=True)
    else:
        silver_sheet.append_row([
            message.from_user.id,
            user_data["name"],
            user_data["org"],
            user_data["org_type"],
            datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
            quote
        ])
        await state.update_data(silver_recorded=True)
    
    await state.update_data(quote_value=quote)
    
    if 'second_metal' not in data:
        second_metal = "Серебро" if current_metal == "Золото" else "Золото"
        await state.update_data(second_metal=second_metal)
        
        await message.answer(
            f"✅ Спасибо, котировка на {current_metal} сохранена!\n"
            f"Хотите отправить котировку на {second_metal}?",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="Да"), KeyboardButton(text="Нет")]
                ],
                resize_keyboard=True
            )
        )
        await state.set_state(Form.quote_second_metal)
    else:
        await message.answer("✅ Спасибо! Обе котировки сохранены! Хорошего Вам дня!", reply_markup=MAIN_KB)
        await clear_state_safely(message.from_user.id, state)

@dp.message(Form.quote_second_metal)
async def process_second_metal(message: types.Message, state: FSMContext):
    if await check_session_expired(chat_id=message.chat.id, user_id=message.from_user.id):
        await message.answer("⌛ Время для предоставления котировок вышло!", reply_markup=MAIN_KB)
        return
    
    data = await state.get_data()
    second_metal = data.get('second_metal')
    user_data = get_user(message.from_user.id)
    
    if message.text == "Нет":
        if user_data:
            log_event("QUOTE", user_data, f"Отказ от предоставления котировки для {second_metal}")
        
        if second_metal == "Золото":
            gold_sheet.append_row([
                message.from_user.id,
                user_data["name"],
                user_data["org"],
                user_data["org_type"],
                datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
                "Отказ от предоставления"
            ])
        else:
            silver_sheet.append_row([
                message.from_user.id,
                user_data["name"],
                user_data["org"],
                user_data["org_type"],
                datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
                "Отказ от предоставления"
            ])
        
        await message.answer(
            "Спасибо за предоставленную котировку! Желаем хорошего дня!",
            reply_markup=MAIN_KB
        )
        await clear_state_safely(message.from_user.id, state)
        return
    
    if message.text != "Да":
        await message.answer("Пожалуйста, используйте кнопки для ответа")
        return
    
    await message.answer(
        f"Введите котировку для {second_metal} в %:",
        reply_markup=types.ReplyKeyboardRemove(),
        parse_mode="HTML"
    )
    await state.update_data(metal=second_metal)
    await state.set_state(Form.quote_value)
    
# --- Запуск ---
async def on_startup(bot: Bot):
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Вебхук удален, старые сообщения пропущены")
    logger.info("Подключение к Telegram API успешно")
    
      # Проверка наличия файла логов
    if not os.path.exists("bot.log"):
        with open("bot.log", "w") as f:
            f.write("")
    
    # Проверка подключения к Google Sheets
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
        await asyncio.sleep(5 * 60)  # каждые 5 минут

async def main():
    await on_startup(bot)

    # Запускаем health_check
    asyncio.create_task(health_check())

    # Инициализируем планировщик
    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
    scheduler.add_job(
        send_scheduled_notifications,
        trigger=CronTrigger(minute="*"),
    )
    scheduler.start()

    await dp.start_polling(bot, skip_updates=True)

if __name__ == '__main__':
    asyncio.run(main())