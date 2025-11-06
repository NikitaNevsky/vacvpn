import os
import asyncio
import httpx
import signal
import sys
from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder, WebAppInfo
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Проверка окружения
logger.info("=" * 50)
logger.info("🤖 BOT STARTUP CHECK")
logger.info("=" * 50)
logger.info(f"Python: {sys.version}")
logger.info(f"Directory: {os.getcwd()}")
logger.info(f"TOKEN: {'✅ SET' if os.getenv('TOKEN') else '❌ MISSING'}")

if not os.getenv('TOKEN'):
    logger.error("❌ CRITICAL: TOKEN environment variable is missing!")
    sys.exit(1)

# Получаем переменные окружения
TOKEN = os.getenv("TOKEN")
SUPPORT_NICK = os.getenv("SUPPORT_NICK", "@vacvpn_support")
TG_CHANNEL = os.getenv("TG_CHANNEL", "@vac_vpn")

# URL API и веб-приложения - используем Railway URL
RAILWAY_STATIC_URL = os.getenv("RAILWAY_STATIC_URL")
if RAILWAY_STATIC_URL:
    # Используем тот же URL что и для API
    API_BASE_URL = f"https://{RAILWAY_STATIC_URL}"
    WEB_APP_URL = f"https://{RAILWAY_STATIC_URL}"  # ВАЖНО: тот же URL!
else:
    API_BASE_URL = "http://localhost:8443"
    WEB_APP_URL = "http://localhost:8443"

BOT_USERNAME = os.getenv("BOT_USERNAME", "vaaaac_bot")

logger.info("🚀 Бот запускается на Railway...")
logger.info(f"🌐 API сервер: {API_BASE_URL}")
logger.info(f"🌐 Веб-приложение: {WEB_APP_URL}")

# Настройка бота
bot = Bot(
    token=TOKEN, 
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

async def make_api_request(endpoint: str, method: str = "GET", json_data: dict = None, params: dict = None):
    """Упрощенная функция для запросов к API"""
    try:
        # Используем тот же URL что и веб-сервер
        RAILWAY_STATIC_URL = os.getenv("RAILWAY_STATIC_URL")
        if RAILWAY_STATIC_URL:
            API_BASE_URL = f"https://{RAILWAY_STATIC_URL}"
        else:
            API_BASE_URL = "http://localhost:8443"
            
        url = f"{API_BASE_URL}{endpoint}"
        timeout_config = httpx.Timeout(30.0, connect=10.0)
        
        async with httpx.AsyncClient(timeout=timeout_config) as client:
            if method.upper() == "GET":
                response = await client.get(url, params=params)
            elif method.upper() == "POST":
                response = await client.post(url, json=json_data)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"API returned status {response.status_code} for {url}")
                return {"error": f"API error: {response.status_code}"}
                
    except Exception as e:
        logger.error(f"API request error for {endpoint}: {e}")
        return {"error": f"Connection error: {str(e)}"}

async def get_user_info(user_id: int):
    """Получает информацию о пользователе через API"""
    return await make_api_request("/user-data", "GET", params={"user_id": str(user_id)})

async def create_user(user_data: dict):
    """Создает пользователя через API"""
    return await make_api_request("/init-user", "POST", json_data=user_data)

async def get_vless_config(user_id: int):
    """Получает VLESS конфигурацию через API"""
    return await make_api_request("/get-vless-config", "GET", params={"user_id": str(user_id)})

async def send_referral_notification(referrer_id: int, referred_user):
    """Отправляет уведомление рефереру о новом реферале"""
    try:
        referred_username = f"@{referred_user.username}" if referred_user.username else referred_user.first_name
        
        message = (
            f"🎉 <b>У вас новый реферал!</b>\n\n"
            f"👤 Пользователь: {referred_username}\n"
            f"💰 <b>Бонус 50₽ уже начислен на ваш баланс!</b>\n\n"
            f"Продолжайте приглашать друзей и зарабатывать больше! 🚀"
        )
        
        await bot.send_message(
            chat_id=referrer_id,
            text=message
        )
        logger.info(f"✅ Уведомление отправлено рефереру {referrer_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Не удалось отправить уведомление рефереру {referrer_id}: {e}")
        return False

# Клавиатуры
def get_main_keyboard():
    """Главное меню как на первом скрине - только одна кнопка VAC VPN"""
    builder = ReplyKeyboardBuilder()
    builder.row(
        types.KeyboardButton(text="VAC VPN")
    )
    builder.row(
        types.KeyboardButton(text="/invite"),
        types.KeyboardButton(text="/help")
    )
    builder.row(
        types.KeyboardButton(text="/privacy")
    )
    return builder.as_markup(resize_keyboard=True)

def get_vac_vpn_menu_keyboard():
    """Подменю VAC VPN как на втором скрине"""
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="🔐 Личный кабинет", callback_data="personal_cabinet"),
        types.InlineKeyboardButton(text="🛠️ Техподдержка", callback_data="support")
    )
    builder.row(
        types.InlineKeyboardButton(text="👥 Рефералка", callback_data="referral"),
        types.InlineKeyboardButton(text="🌐 Веб-кабинет", callback_data="web_cabinet")
    )
    builder.row(
        types.InlineKeyboardButton(text="🔧 VLESS Конфиг", callback_data="vless_config")
    )
    return builder.as_markup()

def get_cabinet_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(
            text="📲 Открыть веб-кабинет",
            web_app=WebAppInfo(url=WEB_APP_URL)
        )
    )
    builder.row(
        types.InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh_cabinet"),
        types.InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_vac_menu")
    )
    return builder.as_markup()

def get_ref_keyboard(user_id: int):
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(
            text="🤜🤛 Поделиться ссылкой",
            url=f"https://t.me/share/url?url=https://t.me/{BOT_USERNAME}?start=ref_{user_id}"
        )
    )
    builder.row(
        types.InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh_refs"),
        types.InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_vac_menu")
    )
    return builder.as_markup()

def get_support_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(
            text="📞 Написать в поддержку",
            url=f"https://t.me/{SUPPORT_NICK[1:]}"
        )
    )
    builder.row(
        types.InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_vac_menu")
    )
    return builder.as_markup()

def get_vless_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh_vless"),
        types.InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_vac_menu")
    )
    return builder.as_markup()

def get_web_cabinet_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(
            text="📲 Открыть веб-кабинет",
            web_app=WebAppInfo(url=WEB_APP_URL)
        )
    )
    builder.row(
        types.InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_vac_menu")
    )
    return builder.as_markup()

# Текстовые сообщения
def get_welcome_message(user_name: str, is_referral: bool = False):
    message = f"""
<b>Добро пожаловать в VAC VPN, {user_name}!</b>

🚀 Получите безопасный и быстрый доступ к интернету с нашей VPN-службой.

📊 <b>Основные возможности:</b>
• 🔒 Защита ваших данных
• 🌐 Обход блокировок
• 🚀 Высокая скорость
• 📱 Работа на всех устройствах

💳 <b>Оплата подписки:</b>
Для покупки подписки перейдите в веб-кабинет через меню бота.
"""
    if is_referral:
        message += "\n🎉 <b>Вы зарегистрировались по реферальной ссылке! Бонус 100₽ уже начислен на ваш баланс!</b>"
    
    message += "\n\n👫 <b>Пригласите друга и получите бонус!</b>"
    
    return message

async def get_cabinet_message(user_id: int):
    """Получает информацию о кабинете через API"""
    user_data = await get_user_info(user_id)
    
    if user_data.get('error'):
        return f"""
<b>Личный кабинет VAC VPN</b>

❌ Ошибка загрузки данных: {user_data['error']}

💡 Попробуйте обновить данные или обратитесь в поддержку.
"""
    
    balance = user_data.get('balance', 0)
    has_subscription = user_data.get('has_subscription', False)
    subscription_days = user_data.get('subscription_days', 0)
    
    status_text = "✅ Активна" if has_subscription else "❌ Неактивна"
    
    if has_subscription:
        subscription_info = f"{subscription_days} дней осталось"
    else:
        subscription_info = "нет активной подписки"
    
    # Получаем реферальную статистику
    referral_stats = user_data.get('referral_stats', {})
    total_referrals = referral_stats.get('total_referrals', 0)
    total_bonus_money = referral_stats.get('total_bonus_money', 0)
    
    return f"""
<b>Личный кабинет VAC VPN</b>

💰 Баланс: <b>{balance}₽</b>
📅 Статус подписки: <b>{status_text}</b>
⏰ Осталось дней: <b>{subscription_info}</b>

👥 Реферальная статистика:
• Приглашено друзей: <b>{total_referrals}</b>
• Получено бонусов: <b>{total_bonus_money}₽</b>

💡 Для покупки подписки используйте веб-кабинет.
"""

def get_ref_message(user_id: int):
    return f"""
<b>Реферальная программа VAC VPN</b>

Пригласите друга по вашей ссылке:
<code>https://t.me/{BOT_USERNAME}?start=ref_{user_id}</code>

🎁 <b>Бонус за приглашение:</b>
• Вы получаете <b>50₽</b> на баланс
• Ваш друг получает <b>100₽</b> на баланс
• Бонусы начисляются сразу после регистрации!

💡 Делитесь ссылкой и получайте бонусы!
"""

def get_support_message():
    return f"""
<b>Техническая поддержка VAC VPN</b>

Если у вас возникли вопросы или проблемы:

📞 Telegram: {SUPPORT_NICK}
📢 Наш канал: {TG_CHANNEL}

⏰ Время ответа: обычно в течение 1-2 часов
"""

async def get_vless_message(user_id: int):
    """Получает VLESS конфигурацию через API"""
    vless_data = await get_vless_config(user_id)
    
    if vless_data.get('error'):
        return f"""
<b>VLESS Конфигурация</b>

❌ Ошибка: {vless_data['error']}

💡 Для получения конфигурации необходима активная подпискa.
"""
    
    if not vless_data.get('configs'):
        return """
<b>VLESS Конфигурация</b>

❌ Конфигурация не найдена.

💡 Для получения конфигурации необходима активная подписка.
"""
    
    message = "<b>🔧 VLESS Конфигурация</b>\n\n"
    
    for config_data in vless_data['configs']:
        config = config_data['config']
        vless_link = config_data['vless_link']
        
        message += f"""
<strong>{config['name']}</strong>
<code>{vless_link}</code>

📱 <b>Для подключения:</b>
1. Скопируйте ссылку выше
2. Вставьте в ваше VPN-приложение
3. Импортируйте конфигурацию

💡 <b>Рекомендуемые приложения:</b>
• Android: V2RayNG
• iOS: Shadowrocket
• Windows: V2RayN
• macOS: V2RayU
"""
    
    return message

def get_vac_vpn_menu_message():
    return """
<b>VAC VPN - Главное меню</b>

Выберите нужный раздел:

🔐 <b>Личный кабинет</b> - информация о балансе и подписке
🛠️ <b>Техподдержка</b> - помощь по любым вопросам  
👥 <b>Рефералка</b> - приглашайте друзей и получайте бонусы
🌐 <b>Веб-кабинет</b> - покупка подписки и управление аккаунтом
🔧 <b>VLESS Конфиг</b> - настройки для подключения к VPN
"""

# Обработчики команд
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user = message.from_user
    args = message.text.split()
    is_referral = False
    referrer_id = None

    # Обработка реферальной ссылки
    if len(args) > 1 and args[1].startswith('ref_'):
        try:
            referrer_id = args[1][4:]
            if referrer_id.isdigit() and int(referrer_id) != user.id:
                is_referral = True
                logger.info(f"🎯 Реферальная регистрация: {user.id} от {referrer_id}")
        except:
            pass

    # Создаем/обновляем пользователя в API
    user_create_result = await create_user({
        "user_id": str(user.id),
        "username": user.username or "",
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
        "start_param": args[1] if len(args) > 1 else ""
    })

    logger.info(f"User create result: {user_create_result}")

    # Отправляем уведомление рефереру
    if is_referral and referrer_id:
        await send_referral_notification(int(referrer_id), user)

    await message.answer(
        text=get_welcome_message(user.first_name, is_referral),
        reply_markup=get_main_keyboard()
    )

@dp.message(Command("cabinet"))
async def cmd_cabinet(message: types.Message):
    user_id = message.from_user.id
    cabinet_text = await get_cabinet_message(user_id)
    await message.answer(cabinet_text, reply_markup=get_cabinet_keyboard())

@dp.message(Command("referral"))
async def cmd_referral(message: types.Message):
    user_id = message.from_user.id
    await message.answer(get_ref_message(user_id), reply_markup=get_ref_keyboard(user_id))

@dp.message(Command("support"))
async def cmd_support(message: types.Message):
    await message.answer(get_support_message(), reply_markup=get_support_keyboard())

@dp.message(Command("vless"))
async def cmd_vless(message: types.Message):
    user_id = message.from_user.id
    vless_text = await get_vless_message(user_id)
    await message.answer(vless_text, reply_markup=get_vless_keyboard(), disable_web_page_preview=True)

# Главная кнопка VAC VPN - открывает подменю
@dp.message(F.text == "VAC VPN")
async def vac_vpn_handler(message: types.Message):
    await message.answer(
        get_vac_vpn_menu_message(),
        reply_markup=get_vac_vpn_menu_keyboard()
    )

# Обработчики callback-кнопок подменю VAC VPN
@dp.callback_query(F.data == "personal_cabinet")
async def personal_cabinet_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    cabinet_text = await get_cabinet_message(user_id)
    await callback.message.edit_text(cabinet_text, reply_markup=get_cabinet_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "support")
async def support_handler(callback: types.CallbackQuery):
    await callback.message.edit_text(get_support_message(), reply_markup=get_support_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "referral")
async def referral_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    await callback.message.edit_text(get_ref_message(user_id), reply_markup=get_ref_keyboard(user_id))
    await callback.answer()

@dp.callback_query(F.data == "web_cabinet")
async def web_cabinet_handler(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🌐 <b>Веб-кабинет VAC VPN</b>\n\n"
        "Для покупки подписки и управления аккаунтом откройте веб-кабинет:",
        reply_markup=get_web_cabinet_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "vless_config")
async def vless_config_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    vless_text = await get_vless_message(user_id)
    await callback.message.edit_text(vless_text, reply_markup=get_vless_keyboard(), disable_web_page_preview=True)
    await callback.answer()

# Обработчики кнопок назад
@dp.callback_query(F.data == "back_to_vac_menu")
async def back_to_vac_menu_handler(callback: types.CallbackQuery):
    await callback.message.edit_text(
        get_vac_vpn_menu_message(),
        reply_markup=get_vac_vpn_menu_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "back_to_menu")
async def back_to_menu_handler(callback: types.CallbackQuery):
    await callback.message.delete()
    await callback.message.answer(
        "Главное меню VAC VPN",
        reply_markup=get_main_keyboard()
    )
    await callback.answer()

# Обработчики обновления данных
@dp.callback_query(F.data == "refresh_cabinet")
async def refresh_cabinet_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    cabinet_text = await get_cabinet_message(user_id)
    
    try:
        await callback.message.edit_text(cabinet_text, reply_markup=get_cabinet_keyboard())
        await callback.answer("✅ Данные обновлены")
    except Exception as e:
        await callback.message.answer(cabinet_text, reply_markup=get_cabinet_keyboard())
        await callback.answer("✅ Данные обновлены")

@dp.callback_query(F.data == "refresh_refs")
async def refresh_refs_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    new_ref_message = get_ref_message(user_id)
    
    try:
        await callback.message.edit_text(new_ref_message, reply_markup=get_ref_keyboard(user_id))
        await callback.answer("✅ Статистика обновлена")
    except Exception as e:
        await callback.message.answer(new_ref_message, reply_markup=get_ref_keyboard(user_id))
        await callback.answer("✅ Статистика обновлены")

@dp.callback_query(F.data == "refresh_vless")
async def refresh_vless_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    vless_text = await get_vless_message(user_id)
    
    try:
        await callback.message.edit_text(vless_text, reply_markup=get_vless_keyboard(), disable_web_page_preview=True)
        await callback.answer("✅ Конфигурация обновлена")
    except Exception as e:
        await callback.message.answer(vless_text, reply_markup=get_vless_keyboard(), disable_web_page_preview=True)
        await callback.answer("✅ Конфигурация обновлена")

# Обработка ошибок
@dp.errors()
async def errors_handler(update: types.Update, exception: Exception):
    logger.error(f"Ошибка при обработке обновления {update}: {exception}")
    return True

# Запуск бота
async def main():
    logger.info("🤖 Бот VAC VPN запускается...")
    logger.info(f"🌐 API сервер: {API_BASE_URL}")
    logger.info(f"🌐 Веб-приложение: {WEB_APP_URL}")
    
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"❌ Ошибка запуска бота: {e}")
    finally:
        await bot.session.close()

# Обработка graceful shutdown
def signal_handler(signum, frame):
    logger.info("🛑 Received shutdown signal, stopping bot...")
    asyncio.create_task(shutdown())

async def shutdown():
    await bot.session.close()
    sys.exit(0)

if __name__ == "__main__":
    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    asyncio.run(main())
