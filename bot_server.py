# ==========================================
#  bot_server.py (v2 - Kurigram fix)
# ==========================================

import asyncio
import asyncpg
import os
import uvloop
from kurigram import Client, filters, types # <--- ИЗМЕНЕНИЕ
from aiohttp import web

# --- Конфигурация (без изменений) ---
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", 0))
SERVER_URL = os.getenv("RENDER_EXTERNAL_URL") 
PORT = int(os.getenv("PORT", 8080))
WEBHOOK_PATH = f"/{BOT_TOKEN}"
WEBHOOK_URL = f"{SERVER_URL}{WEBHOOK_PATH}"

# --- Объекты (без изменений) ---
bot = Client("gift_bot_instance", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
user_conversations = {}

# --- Функции для работы с БД (без изменений) ---
async def set_user_balance(pool, user_id, new_balance):
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO telegram_users (user_id, star_balance) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET star_balance = $2", user_id, new_balance)

async def add_new_user(pool, user_id):
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO telegram_users (user_id, star_balance) VALUES ($1, 0) ON CONFLICT (user_id) DO NOTHING", user_id)

async def get_all_users(pool):
    async with pool.acquire() as conn:
        return await conn.fetch('SELECT user_id, star_balance FROM telegram_users')

# --- Обработчики команд бота (без изменений, кроме импорта types) ---

@bot.on_message(filters.command("start"))
async def start_handler(_, message: types.Message):
    await message.reply("Бот для мониторинга подарков активен. Используйте /status для проверки.")

@bot.on_message(filters.command("status"))
async def status_handler(_, message: types.Message):
    await message.reply("Веб-сервер бота запущен и готов принимать команды.")

@bot.on_message(filters.command("users") & filters.user(ADMIN_USER_ID))
async def users_handler(_, message: types.Message):
    keyboard = types.InlineKeyboardMarkup([
        [types.InlineKeyboardButton("Добавить пользователя", callback_data="add_user")],
        [types.InlineKeyboardButton("Задать баланс", callback_data="set_balance_start")]
    ])
    await message.reply("Управление пользователями:", reply_markup=keyboard)

@bot.on_callback_query(filters.regex("^add_user$"))
async def cq_add_user(_, cq: types.CallbackQuery):
    user_conversations[cq.from_user.id] = "awaiting_new_user_id"
    await cq.message.edit("Пожалуйста, отправьте ID нового пользователя Telegram.")

@bot.on_callback_query(filters.regex("^set_balance_start$"))
async def cq_set_balance_start(client, cq: types.CallbackQuery):
    users = await get_all_users(client.db_pool)
    buttons = [[types.InlineKeyboardButton(f"Пользователь: {user['user_id']}", callback_data=f"select_user_{user['user_id']}")] for user in users]
    if not buttons:
        await cq.message.edit("В базе данных нет пользователей. Сначала добавьте пользователя.")
        return
    await cq.message.edit("Выберите пользователя, чтобы задать его баланс:", reply_markup=types.InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex(r"^select_user_(\d+)$"))
async def cq_select_user_for_balance(_, cq: types.CallbackQuery):
    selected_user_id = int(cq.data.split("_")[2])
    user_conversations[cq.from_user.id] = f"awaiting_balance_{selected_user_id}"
    await cq.message.edit(f"Пожалуйста, отправьте новый баланс в 'звёздах' для пользователя `{selected_user_id}`.")

@bot.on_message(filters.private & filters.user(ADMIN_USER_ID))
async def conversation_handler(client, message: types.Message):
    state = user_conversations.pop(message.from_user.id, None)
    if not state: return
    if state == "awaiting_new_user_id":
        try:
            new_user_id = int(message.text)
            await add_new_user(client.db_pool, new_user_id)
            await message.reply(f"Пользователь `{new_user_id}` успешно добавлен с балансом 0.")
        except ValueError:
            await message.reply("Неверный ID. Пожалуйста, отправьте числовой ID пользователя.")
            user_conversations[message.from_user.id] = state
    elif state.startswith("awaiting_balance_"):
        try:
            target_user_id = int(state.split("_")[2])
            new_balance = int(message.text)
            await set_user_balance(client.db_pool, target_user_id, new_balance)
            await message.reply(f"Баланс для пользователя `{target_user_id}` обновлён на `{new_balance}` звёзд.")
        except ValueError:
            await message.reply("Неверный баланс. Пожалуйста, отправьте число.")
            user_conversations[message.from_user.id] = state

# --- Логика веб-сервера (без изменений) ---

async def on_startup(app):
    if not all([BOT_TOKEN, SERVER_URL]):
        print("Ошибка: BOT_TOKEN или SERVER_URL не установлены. Веб-сервис не может запуститься.")
        return
    print("Запуск веб-сервиса бота...")
    app['db_pool'] = await asyncpg.create_pool(dsn=DATABASE_URL)
    bot.db_pool = app['db_pool']
    await bot.start()
    try:
        await bot.set_webhook(url=WEBHOOK_URL, allowed_updates=[]) # allowed_updates=[] - чтобы не получать лишние апдейты
        print(f"Вебхук успешно установлен по адресу: {WEBHOOK_URL}")
    except Exception as e:
        print(f"Ошибка установки вебхука: {e}")

async def on_shutdown(app):
    print("Остановка веб-сервиса бота...")
    if bot.is_initialized:
        await bot.stop()
    await app['db_pool'].close()

async def webhook_handler(request):
    try:
        update_json = await request.json()
        await bot.feed_update(update_json)
    except Exception as e:
        print(f"Ошибка обработки вебхука: {e}")
    return web.Response(status=200)

async def main_bot_server():
    uvloop.install()
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, webhook_handler)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    web.run_app(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    asyncio.run(main_bot_server())
