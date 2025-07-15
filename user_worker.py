# ==========================================
#  user_worker.py
#  Фоновый сервис (Background Worker) для Render.com
#  Отвечает за постоянный мониторинг и покупку подарков.
# ==========================================

import asyncio
import asyncpg
import time
import os
from pyrogram import Client
from pyrogram.types import Gift
from typing import List, Dict

# --- Конфигурация из переменных окружения ---
# Эти значения будут установлены на панели управления Render

# Учётные данные клиент-аккаунта (вашего личного аккаунта)
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "my_telegram_session")

# Учётные данные бота (нужны для отправки уведомлений)
BOT_TOKEN = os.getenv("BOT_TOKEN")

# ID для уведомлений и адрес базы данных
DATABASE_URL = os.getenv("DATABASE_URL")
NOTIFICATION_CHANNEL_ID = int(os.getenv("NOTIFICATION_CHANNEL_ID", 0))

# --- Функции для работы с базой данных ---

async def init_db(pool):
    """Инициализирует базу данных и создаёт таблицы, если они не существуют."""
    async with pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS known_gifts (
                gift_id BIGINT PRIMARY KEY,
                star_count INTEGER NOT NULL
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS telegram_users (
                user_id BIGINT PRIMARY KEY,
                star_balance BIGINT NOT NULL DEFAULT 0
            )
        ''')
        print("База данных успешно инициализирована.")

async def get_all_users(pool):
    """Получает всех пользователей и их балансы из БД."""
    async with pool.acquire() as conn:
        return await conn.fetch('SELECT user_id, star_balance FROM telegram_users')

async def get_known_gift_ids(pool) -> List[int]:
    """Получает все ID известных подарков из базы данных."""
    async with pool.acquire() as conn:
        rows = await conn.fetch('SELECT gift_id FROM known_gifts')
        return [row['gift_id'] for row in rows]

async def add_new_gifts_to_db(pool, gifts: List[Gift]):
    """Добавляет новые подарки в базу данных."""
    if not gifts: return
    async with pool.acquire() as conn:
        await conn.executemany(
            'INSERT INTO known_gifts (gift_id, star_count) VALUES ($1, $2) ON CONFLICT DO NOTHING',
            [(g.id, g.star_count) for g in gifts]
        )
        print(f"Добавлено {len(gifts)} новых подарков в базу данных.")

async def set_user_balance(pool, user_id, new_balance):
    """Устанавливает баланс пользователя в базе данных."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE telegram_users SET star_balance = $1 WHERE user_id = $2",
            new_balance, user_id
        )

# --- Основная логика ---

async def notify_new_gifts_broadcast(bot_client: Client, new_gifts: List[Gift], db_pool):
    """Формирует сообщение и рассылает его всем пользователям из БД и в канал."""
    if not new_gifts: return
    
    text_parts = ["new gifts:"]
    # Сортируем подарки по цене для красивого вывода
    for gift in sorted(new_gifts, key=lambda g: g.star_count, reverse=True):
        text_parts.append(f"  🎁 - {gift.star_count} ★")
    
    message_text = "\n".join(text_parts)
    
    all_users = await get_all_users(db_pool)
    recipient_ids = {user['user_id'] for user in all_users}
    recipient_ids.add(NOTIFICATION_CHANNEL_ID)
    
    print(f"Начинаю рассылку уведомлений о новых подарках для {len(recipient_ids)} получателей...")
    for chat_id in recipient_ids:
        try:
            await bot_client.send_message(chat_id, message_text)
            await asyncio.sleep(0.1)
        except Exception as e:
            print(f"Не удалось отправить уведомление в чат {chat_id}: {e}")
    print("Рассылка завершена.")

async def create_and_execute_greedy_purchase_plan(user_client: Client, user_id: int, user_balance: int, new_gifts: List[Gift], db_pool):
    """Рассчитывает и выполняет оптимальный план покупок по "жадному" алгоритму."""
    if not new_gifts or user_balance <= 0: return
    
    gifts_by_priority = sorted(new_gifts, key=lambda g: g.star_count, reverse=True)
    purchase_plan: Dict[int, int] = {g.id: 0 for g in gifts_by_priority}
    working_balance = user_balance
    total_cost = 0

    for gift in gifts_by_priority:
        if working_balance < gift.star_count:
            continue
        quantity_to_buy = working_balance // gift.star_count
        purchase_plan[gift.id] = quantity_to_buy
        cost_for_this_gift = quantity_to_buy * gift.star_count
        working_balance -= cost_for_this_gift
        total_cost += cost_for_this_gift

    if total_cost == 0:
        print(f"Для пользователя {user_id} (Баланс: {user_balance}) нет доступных для покупки подарков.")
        return

    print(f"\n--- План покупок для пользователя {user_id} (Баланс: {user_balance}) ---")
    for gift in gifts_by_priority:
        if purchase_plan.get(gift.id, 0) > 0:
            print(f"- Подарок {gift.id} (стоимость {gift.star_count}): купить {purchase_plan[gift.id]} шт.")
    print(f"Общая стоимость: {total_cost} звёзд. Расчётный остаток: {user_balance - total_cost}")
    print("-------------------------------------------------")
    
    for gift in gifts_by_priority:
        quantity_to_execute = purchase_plan.get(gift.id, 0)
        if quantity_to_execute == 0: continue
            
        print(f"Покупаю {quantity_to_execute} шт. подарка {gift.id} для пользователя {user_id}...")
        for i in range(quantity_to_execute):
            try:
                await user_client.send_gift(chat_id=user_id, gift_id=gift.id)
                print(f"  > Успешно отправлен подарок {gift.id} ({i+1}/{quantity_to_execute})")
                await asyncio.sleep(0.4) # Задержка для избежания флуда
            except Exception as e:
                print(f"  > Ошибка при отправке подарка {gift.id}: {e}")
                total_cost -= gift.star_count * (quantity_to_execute - i)
                break
    
    final_balance = user_balance - total_cost
    await set_user_balance(db_pool, user_id, final_balance)
    print(f"Все покупки для пользователя {user_id} завершены. Новый баланс в БД: {final_balance}")

async def main_worker():
    """Основной цикл мониторинга."""
    if not all([API_ID, API_HASH, SESSION_NAME, BOT_TOKEN, DATABASE_URL]):
        print("Ошибка: Не все переменные окружения установлены! Воркер не может запуститься.")
        return

    print("Запуск фонового воркера...")
    db_pool = await asyncpg.create_pool(dsn=DATABASE_URL)
    await init_db(db_pool)
    
    # Инициализируем оба клиента
    user_client = Client(SESSION_NAME, api_id=API_ID, api_hash=API_HASH)
    bot_client = Client("bot_sender_instance", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH)

    await user_client.start()
    await bot_client.start()
    print("Клиент-аккаунт и экземпляр бота для отправки запущены.")

    last_log_time = time.time()
    gift_found_this_hour = False

    while True:
        try:
            current_gifts = await user_client.get_available_gifts()
            known_ids = await get_known_gift_ids(db_pool)
            new_gifts = [g for g in current_gifts if g.id not in known_ids and g.star_count > 0]

            if new_gifts:
                gift_found_this_hour = True
                await add_new_gifts_to_db(db_pool, new_gifts)
                await notify_new_gifts_broadcast(bot_client, new_gifts, db_pool)
                users_to_gift = await get_all_users(db_pool)
                for user in users_to_gift:
                    await create_and_execute_greedy_purchase_plan(
                        user_client=user_client,
                        user_id=user['user_id'],
                        user_balance=user['star_balance'],
                        new_gifts=new_gifts,
                        db_pool=db_pool
                    )
            
            if time.time() - last_log_time > 3600:
                if not gift_found_this_hour:
                    print(f"[{time.ctime()}] Новых подарков за последний час не найдено. Мониторинг продолжается...")
                last_log_time = time.time()
                gift_found_this_hour = False

            await asyncio.sleep(2.0) # Задержка между проверками

        except Exception as e:
            print(f"В основном цикле воркера произошла ошибка: {e}")
            await asyncio.sleep(15)

if __name__ == "__main__":
    asyncio.run(main_worker())
