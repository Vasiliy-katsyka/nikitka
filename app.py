import asyncio
import asyncpg
import time
import telebot
import threading
from pyrogram import Client, idle
from pyrogram.errors import FloodWait, RPCError # Импортируем RPCError для обработки ошибок API Pyrogram
from pyrogram.types import Gift
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from telebot.apihelper import ApiTelegramException # Импортируем для обработки ошибок Telebot API
from typing import List, Dict

import os # Импортируем модуль os
from dotenv import load_dotenv # Импортируем функцию load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

# --- Конфигурация ---
# Учётные данные клиент-аккаунта (Pyrogram)
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_NAME = "autobuy_session"

interval = 0.5

# Учётные данные бота (Telebot)
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = 5146625949

# ID для уведомлений и база данных
DB_URL = "postgresql://neondb_owner:npg_RYq9IyZz7FeV@ep-soft-firefly-ab979h0g-pooler.eu-west-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
# ВАЖНО: Убедитесь, что этот ID корректен!
# Для каналов/групп ID начинается с -100.
# Вы можете получить ID, переслав сообщение из канала/группы боту @JsonDumpBot и посмотрев chat.id
NOTIFICATION_CHANNEL_ID = -1002433007679 

# --- Глобальные переменные для взаимодействия потоков ---
main_async_loop = None
db_pool = None
pyrogram_client_instance = None # Глобальная ссылка на клиент Pyrogram

# --- Асинхронные функции для работы с БД (используются Pyrogram и Telebot) ---
async def init_db(pool):
    """Инициализирует базу данных и создаёт таблицы, если они не существуют."""
    async with pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS known_gifts (
                gift_id BIGINT PRIMARY KEY,
                price INTEGER NOT NULL
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS telegram_users (
                user_id BIGINT PRIMARY KEY,
                star_balance BIGINT NOT NULL DEFAULT 0
            )
        ''')
        user_count = await conn.fetchval('SELECT COUNT(*) FROM telegram_users')
        if user_count == 0:
            await conn.execute(
                "INSERT INTO telegram_users (user_id, star_balance) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                ADMIN_USER_ID, 0
            )
        print("[DB] База данных успешно инициализирована.")

async def get_all_users(pool):
    async with pool.acquire() as conn:
        return await conn.fetch('SELECT user_id, star_balance FROM telegram_users')

async def get_known_gift_ids(pool) -> List[int]:
    async with pool.acquire() as conn:
        rows = await conn.fetch('SELECT gift_id FROM known_gifts')
        return [row['gift_id'] for row in rows]

async def add_new_gifts_to_db(pool, gifts: List[Gift]):
    if not gifts: return
    async with pool.acquire() as conn:
        await conn.executemany(
            'INSERT INTO known_gifts (gift_id, price) VALUES ($1, $2) ON CONFLICT DO NOTHING',
            [(g.id, g.price) for g in gifts]
        )
    print(f"[DB] Добавлено {len(gifts)} новых подарков в базу данных.")

async def set_user_balance(pool, user_id, new_balance):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO telegram_users (user_id, star_balance) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET star_balance = $2",
            user_id, new_balance
        )

async def add_new_user(pool, user_id):
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO telegram_users (user_id, star_balance) VALUES ($1, 0) ON CONFLICT (user_id) DO NOTHING", user_id)


# --- Настройка и обработчики бота (Telebot - Синхронный) ---
bot = telebot.TeleBot(BOT_TOKEN, threaded=False)

def run_db_coro(coro):
    """
    МОСТ (Sync -> Async): Безопасно запускает асинхронную DB функцию
    из синхронного потока Telebot, используя главный asyncio loop.
    """
    if main_async_loop and main_async_loop.is_running():
        future = asyncio.run_coroutine_threadsafe(coro, main_async_loop)
        return future.result() # Ждём результат
    else:
        print("[WARNING] main_async_loop не запущен или недоступен. Не удалось выполнить DB операцию из Telebot.")
        return None

@bot.message_handler(commands=['start'])
def start_handler(message):
    bot.reply_to(message, "Бот для мониторинга подарков активен. Используйте /status для проверки.")

@bot.message_handler(commands=['status'])
def status_handler(message):
    bot.reply_to(message, "Бот запущен и отслеживает новые подарки.")

@bot.message_handler(commands=['users'], func=lambda message: message.from_user.id == ADMIN_USER_ID)
def users_handler(message):
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("Добавить пользователя", callback_data="add_user"))
    keyboard.add(InlineKeyboardButton("Задать баланс", callback_data="set_balance_start"))
    bot.send_message(message.chat.id, "Управление пользователями:", reply_markup=keyboard)

@bot.callback_query_handler(func=lambda call: call.data == "add_user")
def cq_add_user(call):
    msg = bot.edit_message_text("Пожалуйста, отправьте ID нового пользователя Telegram.", call.message.chat.id, call.message.message_id)
    bot.register_next_step_handler(msg, process_new_user_id)

def process_new_user_id(message):
    try:
        new_user_id = int(message.text)
        run_db_coro(add_new_user(db_pool, new_user_id))
        bot.reply_to(message, f"Пользователь `{new_user_id}` успешно добавлен с балансом 0.", parse_mode="Markdown")
    except ValueError:
        bot.reply_to(message, "Неверный ID. Пожалуйста, отправьте числовой ID. Попробуйте снова через /users.")

@bot.callback_query_handler(func=lambda call: call.data == "set_balance_start")
def cq_set_balance_start(call):
    users = run_db_coro(get_all_users(db_pool))
    if not users:
        bot.edit_message_text("В базе данных нет пользователей. Сначала добавьте.", call.message.chat.id, call.message.message_id)
        return
    keyboard = InlineKeyboardMarkup(row_width=1)
    for user in users:
        keyboard.add(InlineKeyboardButton(f"Пользователь: {user['user_id']}", callback_data=f"select_user_{user['user_id']}"))
    bot.edit_message_text("Выберите пользователя, чтобы задать его баланс:", call.message.chat.id, call.message.message_id, reply_markup=keyboard)

@bot.callback_query_handler(func=lambda call: call.data.startswith("select_user_"))
def cq_select_user_for_balance(call):
    target_user_id = int(call.data.split("_")[2])
    msg = bot.edit_message_text(f"Пожалуйста, отправьте новый баланс в 'звёздах' для пользователя `{target_user_id}`.", call.message.chat.id, call.message.message_id, parse_mode="Markdown")
    bot.register_next_step_handler(msg, process_new_balance, target_user_id)

def process_new_balance(message, target_user_id):
    try:
        new_balance = int(message.text)
        run_db_coro(set_user_balance(db_pool, target_user_id, new_balance))
        bot.reply_to(message, f"Баланс для пользователя `{target_user_id}` обновлён на `{new_balance}` звёзд.", parse_mode="Markdown")
    except ValueError:
        bot.reply_to(message, "Неверный баланс. Пожалуйста, отправьте число. Попробуйте снова через /users.")


# --- Основная логика мониторинга (Pyrogram - Асинхронный) ---

async def create_and_execute_greedy_purchase_plan(app: Client, user_id: int, user_balance: int, new_gifts: List[Gift]):
    """Рассчитывает и выполняет оптимальный план покупок по "жадному" алгоритму."""
    if not new_gifts: return
    
    gifts_by_priority = sorted(new_gifts, key=lambda g: g.price, reverse=True)
    purchase_plan: Dict[int, int] = {g.id: 0 for g in gifts_by_priority}
    working_balance = user_balance
    total_cost = 0

    for gift in gifts_by_priority:
        if working_balance < gift.price: continue
        quantity_to_buy = working_balance // gift.price
        purchase_plan[gift.id] = quantity_to_buy
        cost_for_this_gift = quantity_to_buy * gift.price
        working_balance -= cost_for_this_gift
        total_cost += cost_for_this_gift

    print(f"\n[Pyrogram] --- План покупок для {user_id} (Баланс: {user_balance}). Общая стоимость: {total_cost} ---")
    
    purchase_cost_this_run = 0
    for gift in gifts_by_priority:
        quantity_to_execute = purchase_plan.get(gift.id, 0)
        if quantity_to_execute == 0: continue
            
        print(f"[Pyrogram] Покупаю {quantity_to_execute} шт. подарка {gift.id} (стоимость {gift.price})...")
        for i in range(quantity_to_execute):
            try:
                await app.send_gift(chat_id=user_id, gift_id=gift.id)
                purchase_cost_this_run += gift.price
                print(f"  > Успешно отправлен подарок {gift.id} ({i+1}/{quantity_to_execute})")
                await asyncio.sleep(0.5) # Небольшая задержка между отправками
            except FloodWait as e:
                print(f"  > FloodWait при отправке подарка {gift.id}. Ожидание: {e.value} сек.")
                await asyncio.sleep(e.value)
                # Попробуем отправить ещё раз после FloodWait
                try:
                    await app.send_gift(chat_id=user_id, gift_id=gift.id)
                    purchase_cost_this_run += gift.price
                    print(f"  > Успешно отправлен подарок {gift.id} после FloodWait ({i+1}/{quantity_to_execute})")
                    await asyncio.sleep(0.5)
                except Exception as retry_e:
                    print(f"  > Повторная попытка отправить подарок {gift.id} после FloodWait не удалась: {retry_e}")
                    break # Прекращаем для этого типа подарка
            except RPCError as e: # Catch Pyrogram specific API errors
                if e.code == 400:
                    if "USER_BOT_BLOCKED" in e.message:
                        print(f"  > Ошибка: Пользователь {user_id} заблокировал клиентский аккаунт. Не могу отправить подарок.")
                    elif "USER_IS_AN_ADMIN" in e.message:
                        print(f"  > Ошибка: Невозможно отправить подарок администратору {user_id} (возможно, сам себе?).")
                    elif "CHAT_WRITE_FORBIDDEN" in e.message:
                        print(f"  > Ошибка: Клиентский аккаунт не может писать в чат {user_id}.")
                    else:
                        print(f"  > Ошибка Pyrogram RPC (code 400) при отправке подарка {gift.id} для {user_id}: {e}")
                else:
                    print(f"  > Ошибка Pyrogram RPC при отправке подарка {gift.id} для {user_id}: {e}")
                break # Прекращаем попытки для этого типа подарка и пользователя
            except Exception as e:
                print(f"  > Неизвестная ошибка при отправке подарка {gift.id} для {user_id}: {e}")
                break # Прекращаем попытки для этого типа подарка

    final_balance = user_balance - purchase_cost_this_run
    await set_user_balance(db_pool, user_id, final_balance)
    print(f"[Pyrogram] Все покупки для {user_id} завершены. Новый баланс в БД: {final_balance}")


async def monitor_gifts(app: Client):
    """Основной цикл мониторинга клиент-аккаунта."""
    print("[Pyrogram] Клиент запущен. Начинаю мониторинг подарков...")
    last_log_time = time.time()
    gift_found_this_hour = False
    
    while True:
        try:
            current_gifts = await app.get_available_gifts()
            known_ids = await get_known_gift_ids(db_pool)
            new_gifts = [g for g in current_gifts if g.id not in known_ids and g.price > 0]

            if new_gifts:
                gift_found_this_hour = True
                print(f"[Pyrogram] НАЙДЕНО {len(new_gifts)} НОВЫХ ПОДАРКОВ!")
                await add_new_gifts_to_db(db_pool, new_gifts)

                # --- МОСТ (Async -> Sync): Уведомление через Telebot ---
                text_parts = ["🔥 Найдены новые подарки!"]
                for gift in new_gifts:
                    text_parts.append(f"  🎁 - {gift.price} ★")
                message_text = "\n".join(text_parts)
                
                all_users = await get_all_users(db_pool)
                recipient_ids = {user['user_id'] for user in all_users}
                recipient_ids.add(NOTIFICATION_CHANNEL_ID)
                
                print(f"[Pyrogram] Запускаю рассылку уведомлений через Telebot для {len(recipient_ids)} получателей...")
                for chat_id in recipient_ids:
                    try:
                        # Используем run_in_executor для запуска синхронной функции Telebot
                        await main_async_loop.run_in_executor(
                            None, bot.send_message, chat_id, message_text
                        )
                        print(f"  > Уведомление успешно отправлено в чат {chat_id}")
                    except ApiTelegramException as e:
                        # Конкретная ошибка API Telegram (например, chat not found, bot blocked)
                        print(f"  > Ошибка Telebot API при отправке уведомления в чат {chat_id}: {e}")
                    except Exception as e:
                        # Другие, непредвиденные ошибки при отправке уведомления
                        print(f"  > Неизвестная ошибка при отправке уведомления в чат {chat_id}: {e}")


                # Запускаем логику покупки подарков для каждого пользователя
                for user in all_users:
                    if user['star_balance'] > 0:
                        await create_and_execute_greedy_purchase_plan(
                            app, user['user_id'], user['star_balance'], new_gifts
                        )

            # Логгирование "всё спокойно" раз в час
            if time.time() - last_log_time > 3600:
                if not gift_found_this_hour:
                    print("[Pyrogram] Новых подарков за последний час не найдено. Продолжаю мониторинг.")
                last_log_time = time.time()
                gift_found_this_hour = False

            await asyncio.sleep(interval) # Интервал проверки

        except FloodWait as e:
            print(f"[Pyrogram] Превышен лимит запросов в основном цикле мониторинга. Ожидание: {e.value} сек.")
            await asyncio.sleep(e.value)
        except RPCError as e: # Ловим ошибки API Pyrogram
            print(f"[Pyrogram] Ошибка Pyrogram API в основном цикле мониторинга ({e.code}, {e.message}). Повторная попытка через 15 сек.")
            await asyncio.sleep(15)
        except asyncio.CancelledError:
            print("[Pyrogram] Задача мониторинга подарков отменена.")
            break # Выход из цикла while
        except Exception as e:
            print(f"[Pyrogram] В основном цикле мониторинга произошла критическая ошибка: {e}. Повторная попытка через 15 сек.")
            await asyncio.sleep(15)


async def async_main():
    """
    Главная асинхронная функция для запуска Pyrogram клиента и DB.
    Управляет жизненным циклом клиента и фоновых задач.
    """
    global main_async_loop, db_pool, pyrogram_client_instance
    main_async_loop = asyncio.get_running_loop()

    db_pool = await asyncpg.create_pool(dsn=DB_URL)
    await init_db(db_pool)

    pyrogram_client_instance = Client(SESSION_NAME, api_id=API_ID, api_hash=API_HASH)
    monitor_task = None # Инициализируем переменную для задачи мониторинга

    try:
        await pyrogram_client_instance.start()
        
        # Создаем задачу мониторинга и сохраняем на нее ссылку
        monitor_task = asyncio.create_task(monitor_gifts(pyrogram_client_instance))
        
        print("[Pyrogram] Клиент запущен и работает в режиме ожидания. Нажмите Ctrl+C для выхода.")
        await idle() # Блокирует до получения сигнала завершения

    finally:
        print("\n[Pyrogram] Получен сигнал на завершение...")
        # Шаг 1: Отменяем нашу пользовательскую задачу мониторинга
        if monitor_task and not monitor_task.done():
            print("[Pyrogram] Отмена задачи мониторинга подарков...")
            monitor_task.cancel()
            try:
                # Ожидаем завершения отмены, с таймаутом на случай зависания
                await asyncio.wait_for(monitor_task, timeout=10.0) # Увеличил таймаут для graceful shutdown
            except asyncio.CancelledError:
                print("[Pyrogram] Задача мониторинга успешно отменена.")
            except asyncio.TimeoutError:
                print("[Pyrogram] Задача мониторинга не завершилась в течение таймаута после отмены.")
            except Exception as e:
                print(f"[Pyrogram] Ошибка при ожидании отмены задачи мониторинга: {e}")

        # Шаг 2: Даем Pyrogram немного времени, прежде чем пытаться его остановить
        # Это может помочь избежать Race Condition, дав внутренним задачам Pyrogram
        # шанс обработать отмену или завершить текущие операции.
        await asyncio.sleep(0.5) # Увеличил задержку

        # Шаг 3: Останавливаем Pyrogram клиент
        if pyrogram_client_instance and pyrogram_client_instance.is_connected:
            try:
                await pyrogram_client_instance.stop()
                print("[Pyrogram] Клиент остановлен.")
            except Exception as e:
                print(f"[Pyrogram] Ошибка при остановке клиента Pyrogram: {e}")
        else:
            print("[Pyrogram] Клиент уже был отключен или не инициализирован.")
        
        # Шаг 4: Закрываем пул соединений с базой данных
        if db_pool:
            try:
                await db_pool.close()
                print("[DB] Соединение с базой данных закрыто.")
            except Exception as e:
                print(f"[DB] Ошибка при закрытии пула базы данных: {e}")


if __name__ == "__main__":
    print("[Main] Запуск Telegram бота (Telebot) в отдельном потоке...")
    telebot_thread = threading.Thread(target=bot.polling, kwargs={"none_stop": True})
    telebot_thread.daemon = True # Делаем поток демоном, чтобы он завершился при выходе основной программы
    telebot_thread.start()
    
    print("[Main] Запуск основного приложения (Pyrogram & DB)...")
    try:
        asyncio.run(async_main())
    except (KeyboardInterrupt, SystemExit):
        # async_main's finally block will handle cleanup (including DB close)
        pass 
    finally:
        print("[Main] Программа полностью завершена.")
