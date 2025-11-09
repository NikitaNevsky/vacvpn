from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
import os
import logging
import asyncio
from datetime import datetime, timedelta
import threading
import subprocess
import sys
import uuid
import httpx
import firebase_admin
from firebase_admin import credentials, firestore
from pydantic import BaseModel
import re
import json
import urllib.parse
from typing import List, Optional
from PIL import Image, ImageDraw, ImageFont
import io
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import hashlib
import hmac

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('vpn_service.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="VAC VPN API",
    description="Complete VAC VPN Service with API and Web Interface",
    version="2.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Монтируем статические файлы
os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Конфигурация серверов
XRAY_SERVERS = {
    "London": {
        "url": "http://45.134.13.189:8001",
        "api_key": "d67764b644f977a3edd4a6fb3cee00f1b89a406c1a86a662f490e797b7ea2367",  
        "display_name": "London",
        "api_url": "http://45.134.13.189:8002",  
        "reality_pbk": "Mue7dfZz2BXeu_p4u2moigD8243gmcnO5ohEjLzGYR0",
        "ssh_host": "45.134.13.189"
    },
    "Netherlands": {
        "url": "http://103.75.126.91:8001",  
        "api_key": "6e6fb03c83484749d7bf1d3ca0d130fbfee3854c4a8ce84fc8aabfeaa2c19fd1",  
        "display_name": "Netherlands",
        "api_url": "http://103.75.126.91:8002",  
        "reality_pbk": "biUkzZNhzbhq_b8jcw2_xbpyAQPojjG_icyZ_syWdm8",
        "ssh_host": "103.75.126.91"
    }
}

VLESS_SERVERS = [
    {
        "id": "London", 
        "name": "London",
        "address": "45.134.13.189",
        "port": 2053,
        "sni": "www.google.com",
        "reality_pbk": "Mue7dfZz2BXeu_p4u2moigD8243gmcnO5ohEjLzGYR0",
        "short_id": "abcd1234",
        "flow": "xtls-rprx-vision",
        "security": "reality"
    }
]

# Тарифы
TARIFFS = {
    "1month": {
        "name": "1 Месяц",
        "price": 150.0,
        "days": 30
    },
    "1year": {
        "name": "1 Год",
        "price": 1300.0,
        "days": 365
    }
}

# Реферальная система
REFERRAL_BONUS_REFERRER = 50.0
REFERRAL_BONUS_REFERRED = 100.0

# Инициализация Firebase
try:
    if not firebase_admin._apps:
        logger.info("🚀 Initializing Firebase for Railway")
        
        firebase_config = {
            "type": "service_account",
            "project_id": os.getenv("FIREBASE_PROJECT_ID"),
            "private_key_id": os.getenv("FIREBASE_PRIVATE_KEY_ID"),
            "private_key": os.getenv("FIREBASE_PRIVATE_KEY", "").replace('\\n', '\n'),
            "client_email": os.getenv("FIREBASE_CLIENT_EMAIL"),
            "client_id": os.getenv("FIREBASE_CLIENT_ID"),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": os.getenv("FIREBASE_CLIENT_X509_CERT_URL"),
            "universe_domain": "googleapis.com"
        }
        
        required_fields = ["project_id", "private_key", "client_email"]
        for field in required_fields:
            if not firebase_config.get(field):
                raise ValueError(f"Missing required Firebase config field: {field}")
        
        cred = credentials.Certificate(firebase_config)
        firebase_admin.initialize_app(cred)
    
    db = firestore.client()
    logger.info("✅ Firebase initialized successfully")
    
except Exception as e:
    logger.error(f"❌ Firebase initialization failed: {str(e)}")
    db = None

# Модели данных
class PaymentRequest(BaseModel):
    user_id: str
    amount: float
    tariff: str = "1month"
    payment_type: str = "tariff"

class ActivateTariffRequest(BaseModel):
    user_id: str
    tariff: str
    payment_method: str = "yookassa"
    selected_server: str = None

class AddBalanceRequest(BaseModel):
    user_id: str
    amount: float
    payment_method: str = "yookassa"

class InitUserRequest(BaseModel):
    user_id: str
    username: str = ""
    first_name: str = ""
    last_name: str = ""
    start_param: str = None

class VlessConfigRequest(BaseModel):
    user_id: str
    server_id: str = None

class BuyWithBalanceRequest(BaseModel):
    user_id: str
    tariff_id: str
    tariff_price: float
    tariff_days: int
    selected_server: str = None

class SaveVlessKeyRequest(BaseModel):
    user_id: str
    server_id: str
    vless_key: str
    config_data: dict

# Утилиты
def ensure_logo_exists():
    """Обеспечивает что логотип доступен в статической директории"""
    try:
        original_logo = "Airbrush-Image-Enhancer-1753455007914.png"
        static_logo = "static/Airbrush-Image-Enhancer-1753455007914.png"
        
        os.makedirs("static", exist_ok=True)
        
        if os.path.exists(original_logo) and not os.path.exists(static_logo):
            import shutil
            shutil.copy2(original_logo, static_logo)
            logger.info(f"✅ Logo copied to static directory: {static_logo}")
        elif os.path.exists(static_logo):
            logger.info(f"✅ Logo already exists in static directory: {static_logo}")
        else:
            logger.warning("⚠️ Original logo file not found, creating placeholder")
            create_placeholder_logo()
            
    except Exception as e:
        logger.error(f"❌ Error ensuring logo exists: {e}")
        create_placeholder_logo()

def create_placeholder_logo():
    """Создает placeholder логотип если основной не найден"""
    try:
        logo_path = "static/Airbrush-Image-Enhancer-1753455007914.png"
        
        img = Image.new('RGB', (120, 120), color='#121212')
        d = ImageDraw.Draw(img)
        
        d.ellipse([10, 10, 110, 110], fill='#B0CB1F')
        
        try:
            font = ImageFont.truetype("arial.ttf", 16)
        except:
            try:
                font = ImageFont.truetype("arialbd.ttf", 16)
            except:
                font = ImageFont.load_default()
        
        d.text((60, 40), "VAC", fill='#121212', font=font, anchor="mm")
        d.text((60, 70), "VPN", fill='#121212', font=font, anchor="mm")
        
        img.save(logo_path, "PNG")
        logger.info("✅ Placeholder logo created successfully")
        
    except Exception as e:
        logger.error(f"❌ Error creating placeholder logo: {e}")

def generate_user_uuid():
    """Генерация уникального UUID для пользователя"""
    return str(uuid.uuid4())

def verify_yookassa_signature(body: bytes, signature: str) -> bool:
    """Проверка подписи уведомления от ЮКассы"""
    try:
        secret_key = os.getenv("YOOKASSA_SECRET_KEY", "")
        if not secret_key:
            logger.warning("⚠️ YOOKASSA_SECRET_KEY not set, skipping signature verification")
            return True
            
        digest = hmac.new(
            secret_key.encode(),
            body,
            hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(digest, signature)
    except Exception as e:
        logger.error(f"❌ Error verifying signature: {e}")
        return False

# Функции работы с Firebase
def get_user(user_id: str):
    if not db: 
        return None
    try:
        doc = db.collection('users').document(user_id).get()
        return doc.to_dict() if doc.exists else None
    except Exception as e:
        logger.error(f"❌ Error getting user: {e}")
        return None

def update_user_balance(user_id: str, amount: float):
    if not db: 
        return False
    try:
        user_ref = db.collection('users').document(user_id)
        
        @firestore.transactional
        def update_balance_transaction(transaction, user_ref, amount):
            user_doc = user_ref.get(transaction=transaction)
            if not user_doc.exists:
                return False
                
            user_data = user_doc.to_dict()
            current_balance = user_data.get('balance', 0.0)
            new_balance = current_balance + amount
            
            transaction.update(user_ref, {
                'balance': new_balance,
                'updated_at': firestore.SERVER_TIMESTAMP
            })
            return True
        
        transaction = db.transaction()
        success = update_balance_transaction(transaction, user_ref, amount)
        
        if success:
            logger.info(f"💰 Balance updated for user {user_id}: +{amount}₽")
            return True
        else:
            logger.error(f"❌ Failed to update balance for user {user_id}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error updating balance: {e}")
        return False

async def ensure_user_uuid(user_id: str, server_id: str = None) -> str:
    """Гарантирует что у пользователя есть UUID и он добавлен в Xray"""
    if not db:
        raise Exception("Database not connected")
    
    try:
        user_ref = db.collection('users').document(user_id)
        
        @firestore.transactional
        def ensure_uuid_transaction(transaction, user_ref, user_id):
            user_doc = user_ref.get(transaction=transaction)
            if not user_doc.exists:
                raise Exception("User not found")
            
            user_data = user_doc.to_dict()
            vless_uuid = user_data.get('vless_uuid')
            
            if not vless_uuid:
                vless_uuid = generate_user_uuid()
                logger.info(f"🆕 Generating new UUID for user {user_id}: {vless_uuid}")
                transaction.update(user_ref, {
                    'vless_uuid': vless_uuid,
                    'updated_at': firestore.SERVER_TIMESTAMP
                })
            
            return vless_uuid
        
        transaction = db.transaction()
        vless_uuid = ensure_uuid_transaction(transaction, user_ref, user_id)
        
        # Асинхронно добавляем в Xray
        servers_to_add = [server_id] if server_id else list(XRAY_SERVERS.keys())
        asyncio.create_task(fast_add_to_xray(vless_uuid, servers_to_add))
        
        return vless_uuid
        
    except Exception as e:
        logger.error(f"❌ Error ensuring user UUID: {e}")
        raise

async def fast_add_to_xray(user_uuid: str, servers_to_add):
    """Быстрое добавление в Xray без блокировки основного потока"""
    try:
        for server_name in servers_to_add:
            if server_name in XRAY_SERVERS:
                try:
                    async with httpx.AsyncClient(timeout=10.0) as client:
                        response = await client.post(
                            f"{XRAY_SERVERS[server_name]['url']}/user",
                            headers={
                                "X-API-Key": XRAY_SERVERS[server_name]["api_key"],
                                "Content-Type": "application/json"
                            },
                            json={"uuid": user_uuid},
                            timeout=10.0
                        )
                        
                        if response.status_code == 200:
                            logger.info(f"✅ User {user_uuid} added to {server_name}")
                        else:
                            logger.warning(f"⚠️ Failed to add user to {server_name}: {response.status_code}")
                            
                except Exception as e:
                    logger.warning(f"⚠️ Fast add failed for {server_name}: {e}")
                    
    except Exception as e:
        logger.error(f"❌ Error in fast_add_to_xray: {e}")

async def check_user_in_xray(user_uuid: str, server_id: str = None) -> bool:
    """Проверить есть ли пользователь в Xray"""
    try:
        if server_id and server_id in XRAY_SERVERS:
            servers_to_check = [(server_id, XRAY_SERVERS[server_id])]
        else:
            servers_to_check = list(XRAY_SERVERS.items())
        
        for server_name, server_config in servers_to_check:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    response = await client.get(
                        f"{server_config['url']}/user/{user_uuid}",
                        headers={"X-API-Key": server_config["api_key"]},
                        timeout=5.0
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('exists'):
                            return True
            except Exception:
                continue
        
        return False
            
    except Exception as e:
        logger.error(f"❌ [XRAY CHECK] Exception: {str(e)}")
        return False

async def add_user_to_xray_server(server_id: str, user_id: str, user_uuid: str) -> bool:
    """Добавить пользователя на сервер Xray через прямой API вызов"""
    try:
        if server_id not in XRAY_SERVERS:
            logger.error(f"❌ Unknown server: {server_id}")
            return False
        
        server_config = XRAY_SERVERS[server_id]
        
        logger.info(f"🚀 Adding user {user_id} to {server_id}")
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(
                f"{server_config['url']}/user",
                headers={
                    "X-API-Key": server_config["api_key"],
                    "Content-Type": "application/json"
                },
                json={"uuid": user_uuid}
            )
            
            if response.status_code == 200:
                logger.info(f"✅ User {user_id} successfully added to {server_id}")
                return True
            else:
                logger.error(f"❌ API call failed for {server_id}: {response.status_code} - {response.text}")
                return False
                
    except Exception as e:
        logger.error(f"❌ Error calling Xray API for {server_id}: {e}")
        return False

def add_referral_bonus_immediately(referrer_id: str, referred_id: str):
    if not db: 
        return False
    
    try:
        # Обновляем баланс реферера
        update_user_balance(referrer_id, REFERRAL_BONUS_REFERRER)
        # Обновляем баланс приглашенного
        update_user_balance(referred_id, REFERRAL_BONUS_REFERRED)
        
        # Сохраняем запись о реферале
        referral_id = f"{referrer_id}_{referred_id}"
        db.collection('referrals').document(referral_id).set({
            'referrer_id': referrer_id,
            'referred_id': referred_id,
            'referrer_bonus': REFERRAL_BONUS_REFERRER,
            'referred_bonus': REFERRAL_BONUS_REFERRED,
            'bonus_paid': True,
            'created_at': firestore.SERVER_TIMESTAMP
        })
        
        logger.info(f"✅ Referral bonuses applied: {referrer_id} -> {referred_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error adding immediate referral bonus: {e}")
        return False

def save_vless_key_to_db(user_id: str, server_id: str, vless_key: str, config_data: dict):
    """Сохраняет VLESS ключ пользователя в базу данных"""
    if not db:
        return False
    
    try:
        vless_key_id = f"{user_id}_{server_id}"
        
        vless_data = {
            'user_id': user_id,
            'server_id': server_id,
            'vless_key': vless_key,
            'config_data': config_data,
            'created_at': firestore.SERVER_TIMESTAMP,
            'updated_at': firestore.SERVER_TIMESTAMP,
            'is_active': True
        }
        
        db.collection('vless_keys').document(vless_key_id).set(vless_data)
        logger.info(f"✅ VLESS key saved for user {user_id} on server {server_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving VLESS key to DB: {e}")
        return False

def get_user_vless_keys(user_id: str):
    """Получает все VLESS ключи пользователя из базы данных"""
    if not db:
        return []
    
    try:
        vless_keys_ref = db.collection('vless_keys').where('user_id', '==', user_id)
        vless_keys = vless_keys_ref.stream()
        
        keys_list = []
        for key_doc in vless_keys:
            key_data = key_doc.to_dict()
            keys_list.append(key_data)
        
        return keys_list
        
    except Exception as e:
        logger.error(f"❌ Error getting VLESS keys: {e}")
        return []

def update_vless_key_status(user_id: str, server_id: str, is_active: bool):
    """Обновляет статус VLESS ключа"""
    if not db:
        return False
    
    try:
        vless_key_id = f"{user_id}_{server_id}"
        
        db.collection('vless_keys').document(vless_key_id).update({
            'is_active': is_active,
            'updated_at': firestore.SERVER_TIMESTAMP
        })
        
        logger.info(f"✅ VLESS key status updated for user {user_id} on server {server_id}: {is_active}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error updating VLESS key status: {e}")
        return False

def create_user_vless_configs(user_id: str, vless_uuid: str, server_id: str = None) -> List[dict]:
    """Создает VLESS конфигурации для пользователя и сохраняет в БД"""
    
    configs = []
    servers_to_process = []
    
    if server_id:
        for server in VLESS_SERVERS:
            if server["id"] == server_id:
                servers_to_process = [server]
                break
        if not servers_to_process:
            servers_to_process = VLESS_SERVERS
    else:
        servers_to_process = VLESS_SERVERS
    
    for server in servers_to_process:
        address = server["address"]
        port = server["port"]
        security = server["security"]
        sni = server.get("sni", "")
        reality_pbk = server.get("reality_pbk", "")
        short_id = server.get("short_id", "")
        flow = server.get("flow", "")
        
        if security == "reality":
            clean_sni = sni.replace(":443", "") if sni else ""
            vless_link = (
                f"vless://{vless_uuid}@{address}:{port}?"
                f"type=tcp&"
                f"security=reality&"
                f"flow={flow}&"
                f"pbk={reality_pbk}&"
                f"fp=chrome&"
                f"sni={clean_sni}&"
                f"sid={short_id}#"
                f"VAC-VPN-{user_id}-{server['id']}"
            )
        else:
            vless_link = (
                f"vless://{vless_uuid}@{address}:{port}?"
                f"encryption=none&"
                f"type=tcp&"
                f"security=none#"
                f"VAC-VPN-{user_id}-{server['id']}"
            )
        
        config = {
            "name": f"{server['name']} - {user_id}",
            "protocol": "vless",
            "uuid": vless_uuid,
            "server": address,
            "port": port,
            "security": security,
            "type": "tcp",
            "remark": f"VAC VPN - {user_id} - {server['name']}",
            "user_id": user_id,
            "server_id": server["id"]
        }
        
        if security == "reality":
            config.update({
                "reality_pbk": reality_pbk,
                "sni": sni.replace(":443", "") if sni else "",
                "short_id": short_id,
                "flow": flow,
                "fingerprint": "chrome"
            })
        else:
            config.update({
                "encryption": "none"
            })
        
        encoded_vless_link = urllib.parse.quote(vless_link)
        
        config_data = {
            "vless_link": vless_link,
            "config": config,
            "qr_code": f"https://api.qrserver.com/v1/create-qr-code/?size=200x200&data={encoded_vless_link}",
            "server_name": server["name"],
            "server_id": server["id"]
        }
        
        save_vless_key_to_db(user_id, server["id"], vless_link, config)
        
        configs.append(config_data)
    
    return configs

def process_subscription_days(user_id: str) -> bool:
    """Обработка дней подписки с удалением из Xray при окончании"""
    if not db:
        return False
    
    try:
        user = get_user(user_id)
        if not user:
            return False
            
        has_subscription = user.get('has_subscription', False)
        subscription_days = user.get('subscription_days', 0)
        vless_uuid = user.get('vless_uuid')
        last_check = user.get('last_subscription_check')
        today = datetime.now().date()
        
        if not has_subscription or subscription_days <= 0:
            return True
            
        if not last_check:
            db.collection('users').document(user_id).update({
                'last_subscription_check': today.isoformat()
            })
            return True
        else:
            try:
                last_date = datetime.fromisoformat(last_check.replace('Z', '+00:00')).date()
                days_passed = (today - last_date).days
                
                if days_passed > 0:
                    new_days = max(0, subscription_days - days_passed)
                    
                    update_data = {
                        'subscription_days': new_days,
                        'last_subscription_check': today.isoformat()
                    }
                    
                    if new_days == 0:
                        update_data['has_subscription'] = False
                        update_data['subscription_end'] = datetime.now().isoformat()
                        if vless_uuid:
                            # Асинхронно удаляем из Xray
                            asyncio.create_task(remove_user_from_xray(vless_uuid))
                            # Деактивируем ключи
                            user_vless_keys = get_user_vless_keys(user_id)
                            for key_data in user_vless_keys:
                                update_vless_key_status(user_id, key_data['server_id'], False)
                    
                    db.collection('users').document(user_id).update(update_data)
                    logger.info(f"📅 Subscription updated for user {user_id}: {new_days} days left")
                    
            except Exception as e:
                logger.error(f"❌ Error processing subscription days: {e}")
        
        return True
            
    except Exception as e:
        logger.error(f"❌ Error processing subscription: {e}")
        return False

async def remove_user_from_xray(user_uuid: str):
    """Удалить пользователя из всех Xray серверов"""
    try:
        for server_name, server_config in XRAY_SERVERS.items():
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await client.delete(
                        f"{server_config['url']}/user/{user_uuid}",
                        headers={"X-API-Key": server_config["api_key"]},
                        timeout=10.0
                    )
                    
                    if response.status_code == 200:
                        logger.info(f"✅ User {user_uuid} removed from {server_name}")
                    else:
                        logger.warning(f"⚠️ Failed to remove user from {server_name}: {response.status_code}")
                        
            except Exception as e:
                logger.warning(f"⚠️ Error removing user from {server_name}: {e}")
                
    except Exception as e:
        logger.error(f"❌ Error in remove_user_from_xray: {e}")

async def check_all_subscriptions():
    """Автоматическая проверка всех подписок"""
    if not db:
        return []
    
    try:
        users_ref = db.collection('users').where('has_subscription', '==', True)
        users = users_ref.stream()
        
        expired_users = []
        
        for user_doc in users:
            user_data = user_doc.to_dict()
            user_id = user_data.get('user_id')
            
            success = process_subscription_days(user_id)
            
            if success:
                user_updated = get_user(user_id)
                if not user_updated.get('has_subscription', False):
                    expired_users.append(user_id)
        
        if expired_users:
            logger.info(f"📅 Subscription check completed: {len(expired_users)} users expired")
        
        return expired_users
        
    except Exception as e:
        logger.error(f"❌ Error checking subscriptions: {e}")
        return []

def start_subscription_checker():
    """Запуск периодической проверки подписок"""
    try:
        scheduler = BackgroundScheduler()
        scheduler.add_job(
            check_all_subscriptions,
            'interval',
            hours=6,
            id='subscription_check'
        )
        scheduler.start()
        logger.info("✅ Subscription checker started")
    except Exception as e:
        logger.error(f"❌ Error starting subscription checker: {e}")

def save_payment(payment_id: str, user_id: str, amount: float, tariff: str, payment_type: str = "tariff", payment_method: str = "yookassa", selected_server: str = None):
    if not db: 
        return
    try:
        payment_data = {
            'payment_id': payment_id,
            'user_id': user_id,
            'amount': amount,
            'tariff': tariff,
            'status': 'pending',
            'payment_type': payment_type,
            'payment_method': payment_method,
            'created_at': firestore.SERVER_TIMESTAMP,
            'yookassa_id': None
        }
        
        if selected_server:
            payment_data['selected_server'] = selected_server
        
        db.collection('payments').document(payment_id).set(payment_data)
        logger.info(f"💰 Payment saved: {payment_id} for user {user_id}")
    except Exception as e:
        logger.error(f"❌ Error saving payment: {e}")

def update_payment_status(payment_id: str, status: str, yookassa_id: str = None):
    if not db: 
        return
    try:
        update_data = {
            'status': status,
            'updated_at': firestore.SERVER_TIMESTAMP
        }
        if yookassa_id:
            update_data['yookassa_id'] = yookassa_id
            
        if status == 'succeeded':
            update_data['confirmed_at'] = firestore.SERVER_TIMESTAMP
        
        db.collection('payments').document(payment_id).update(update_data)
        logger.info(f"💰 Payment status updated: {payment_id} -> {status}")
    except Exception as e:
        logger.error(f"❌ Error updating payment status: {e}")

def get_payment(payment_id: str):
    if not db: 
        return None
    try:
        doc = db.collection('payments').document(payment_id).get()
        return doc.to_dict() if doc.exists else None
    except Exception as e:
        logger.error(f"❌ Error getting payment: {e}")
        return None

def get_referrals(referrer_id: str):
    if not db: 
        return []
    try:
        referrals = db.collection('referrals').where('referrer_id', '==', referrer_id).stream()
        return [ref.to_dict() for ref in referrals]
    except Exception as e:
        logger.error(f"❌ Error getting referrals: {e}")
        return []

def extract_referrer_id(start_param: str) -> str:
    if not start_param:
        return None
    
    if start_param.startswith('ref_'):
        referrer_id = start_param.replace('ref_', '')
        return referrer_id
    
    if start_param.isdigit():
        return start_param
    
    patterns = [
        r'ref_(\d+)',
        r'ref(\d+)',  
        r'referral_(\d+)',
        r'referral(\d+)',
        r'startapp_(\d+)',
        r'startapp(\d+)',
        r'(\d{8,})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, start_param)
        if match:
            referrer_id = match.group(1)
            return referrer_id
    
    return start_param

async def update_subscription_days(user_id: str, additional_days: int, server_id: str = None) -> bool:
    """Обновление дней подписки с ГАРАНТИРОВАННЫМ добавлением в Xray"""
    if not db: 
        return False
    try:
        user_ref = db.collection('users').document(user_id)
        
        @firestore.transactional
        def update_subscription_transaction(transaction, user_ref, additional_days, server_id):
            user_doc = user_ref.get(transaction=transaction)
            if not user_doc.exists:
                return False, None
                
            user_data = user_doc.to_dict()
            current_days = user_data.get('subscription_days', 0)
            new_days = current_days + additional_days
            
            has_subscription = user_data.get('has_subscription', False)
            if not has_subscription and additional_days > 0:
                has_subscription = True
            
            update_data = {
                'subscription_days': new_days,
                'has_subscription': has_subscription,
                'updated_at': firestore.SERVER_TIMESTAMP,
                'last_subscription_check': datetime.now().date().isoformat()
            }
            
            # Записываем начало подписки, если это новая подписка
            if has_subscription and not user_data.get('subscription_start'):
                update_data['subscription_start'] = datetime.now().isoformat()
            
            # Рассчитываем дату окончания подписки
            if has_subscription:
                subscription_end = datetime.now() + timedelta(days=new_days)
                update_data['subscription_end'] = subscription_end.isoformat()
            
            # Генерируем UUID если нужно
            vless_uuid = user_data.get('vless_uuid')
            if has_subscription and not vless_uuid:
                vless_uuid = generate_user_uuid()
                update_data['vless_uuid'] = vless_uuid
            
            transaction.update(user_ref, update_data)
            return True, vless_uuid or update_data.get('vless_uuid')
        
        transaction = db.transaction()
        success, vless_uuid = update_subscription_transaction(transaction, user_ref, additional_days, server_id)
        
        if success and vless_uuid:
            # Асинхронно добавляем в Xray
            servers_to_add = [server_id] if server_id else list(XRAY_SERVERS.keys())
            asyncio.create_task(fast_add_to_xray(vless_uuid, servers_to_add))
            
            logger.info(f"✅ Subscription updated for user {user_id}: +{additional_days} days, UUID: {vless_uuid}")
            return True
        else:
            logger.error(f"❌ Failed to update subscription for user {user_id}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error updating subscription days: {e}")
        return False

def save_referral_link(user_id: str, referral_link: str):
    """Сохраняет реферальную ссылку пользователя"""
    if not db:
        return False
    
    try:
        user_ref = db.collection('users').document(user_id)
        user_ref.update({
            'referral_link': referral_link,
            'updated_at': firestore.SERVER_TIMESTAMP
        })
        logger.info(f"✅ Referral link saved for user {user_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Error saving referral link: {e}")
        return False

def get_referral_link(user_id: str) -> str:
    """Получает реферальную ссылку пользователя"""
    if not db:
        return None
    
    try:
        user = get_user(user_id)
        if user:
            return user.get('referral_link')
        return None
    except Exception as e:
        logger.error(f"❌ Error getting referral link: {e}")
        return None

def generate_referral_link(user_id: str) -> str:
    """Генерирует реферальную ссылку для пользователя"""
    return f"https://t.me/vaaaac_bot?start=ref_{user_id}"

# Функция для запуска бота в отдельном процессе
def run_bot():
    """Запуск бота в отдельном процессе"""
    try:
        logger.info("🤖 Starting Telegram bot in separate process...")
        subprocess.run([sys.executable, "bot.py"], check=True)
    except Exception as e:
        logger.error(f"❌ Bot execution error: {e}")

@app.on_event("startup")
async def startup_event():
    """Действия при запуске приложения"""
    logger.info("🚀 VAC VPN Server starting up...")
    
    ensure_logo_exists()
    start_subscription_checker()
    
    # Проверяем обязательные переменные окружения
    required_env_vars = ['FIREBASE_PROJECT_ID', 'FIREBASE_PRIVATE_KEY', 'FIREBASE_CLIENT_EMAIL', 'SHOP_ID', 'API_KEY']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.warning(f"⚠️ Missing environment variables: {missing_vars}")
    else:
        logger.info("✅ All required environment variables are set")
    
    logger.info("🔄 Starting Telegram bot automatically...")
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
    logger.info("✅ Telegram bot started successfully")

# WEBHOOK для ЮКассы
@app.post("/yookassa-webhook")
async def yookassa_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Webhook для уведомлений от ЮКассы
    """
    try:
        body = await request.body()
        signature = request.headers.get('Yookassa-Signature')
        
        # Проверяем подпись если секретный ключ установлен
        if not verify_yookassa_signature(body, signature):
            logger.warning("⚠️ Invalid Yookassa signature")
            return JSONResponse(status_code=401, content={"error": "Invalid signature"})
        
        data = json.loads(body.decode('utf-8'))
        logger.info(f"🔄 Yookassa webhook received: {data.get('event')}")
        
        event = data.get('event')
        if event == 'payment.succeeded':
            payment_object = data.get('object', {})
            payment_id = payment_object.get('id')
            metadata = payment_object.get('metadata', {})
            
            user_id = metadata.get('user_id')
            payment_type = metadata.get('payment_type')
            tariff = metadata.get('tariff')
            amount = float(payment_object['amount']['value'])
            
            logger.info(f"💰 Payment succeeded: {payment_id}, user: {user_id}, type: {payment_type}")
            
            # Фоновая обработка платежа
            background_tasks.add_task(process_successful_payment, payment_id, user_id, payment_type, tariff, amount, metadata)
            
            return {"success": True, "message": "Webhook processed"}
        
        logger.info(f"📨 Yookassa event ignored: {event}")
        return {"success": True, "message": "Event ignored"}
        
    except Exception as e:
        logger.error(f"❌ Yookassa webhook error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

async def process_successful_payment(payment_id: str, user_id: str, payment_type: str, tariff: str, amount: float, metadata: dict):
    """Обработка успешного платежа"""
    try:
        # Находим payment в базе по yookassa_id
        payments_ref = db.collection('payments')
        query = payments_ref.where('yookassa_id', '==', payment_id).limit(1)
        payments = query.stream()
        
        payment_doc = None
        local_payment_id = None
        
        for doc in payments:
            payment_doc = doc
            payment_data = doc.to_dict()
            local_payment_id = payment_data.get('payment_id')
            break
        
        if payment_doc and local_payment_id:
            payment_data = payment_doc.to_dict()
            selected_server = payment_data.get('selected_server')
            
            # Обновляем статус платежа
            update_payment_status(local_payment_id, 'succeeded', payment_id)
            
            if payment_type == 'tariff':
                # Активируем подписку
                tariff_days = TARIFFS[tariff]["days"]
                success = await update_subscription_days(user_id, tariff_days, selected_server)
                
                if success:
                    logger.info(f"✅ Tariff activated via webhook: user {user_id}, {tariff_days} days on server {selected_server}")
                    
                    # Применяем реферальный бонус если есть
                    user = get_user(user_id)
                    if user and user.get('referred_by'):
                        referrer_id = user['referred_by']
                        referral_id = f"{referrer_id}_{user_id}"
                        
                        referral_exists = db.collection('referrals').document(referral_id).get().exists
                        
                        if not referral_exists:
                            add_referral_bonus_immediately(referrer_id, user_id)
                            logger.info(f"💰 Referral bonus applied via webhook for {user_id}")
                else:
                    logger.error(f"❌ Failed to activate tariff via webhook for user {user_id}")
            
            elif payment_type == 'balance':
                # Пополняем баланс
                success = update_user_balance(user_id, amount)
                if success:
                    logger.info(f"✅ Balance updated via webhook: user {user_id}, +{amount}₽")
                else:
                    logger.error(f"❌ Failed to update balance via webhook for user {user_id}")
        
        else:
            logger.warning(f"⚠️ Payment not found in database: {payment_id}, creating new record")
            
            # Создаем запись о платеже если не нашли
            local_payment_id = str(uuid.uuid4())
            save_payment(local_payment_id, user_id, amount, tariff, payment_type, "yookassa", metadata.get('selected_server'))
            update_payment_status(local_payment_id, 'succeeded', payment_id)
            
            # Обрабатываем платеж
            if payment_type == 'tariff':
                tariff_days = TARIFFS[tariff]["days"]
                await update_subscription_days(user_id, tariff_days, metadata.get('selected_server'))
            elif payment_type == 'balance':
                update_user_balance(user_id, amount)
            
        logger.info(f"✅ Payment processing completed: {payment_id}")
            
    except Exception as e:
        logger.error(f"❌ Error processing successful payment: {e}")

# API ЭНДПОИНТЫ
@app.get("/")
async def root():
    if os.path.exists("index.html"):
        with open("index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    
    return {
        "message": "VAC VPN API is running", 
        "status": "ok",
        "firebase": "connected" if db else "disconnected",
        "available_servers": len(VLESS_SERVERS),
        "environment": "production",
        "timestamp": datetime.now().isoformat(),
        "version": "2.0.0"
    }

@app.get("/health")
async def health_check():
    # Проверяем все системы
    systems = {
        "firebase": db is not None,
        "yookassa": bool(os.getenv("SHOP_ID") and os.getenv("API_KEY")),
        "servers": len(XRAY_SERVERS) > 0
    }
    
    status = "healthy" if all(systems.values()) else "degraded"
    
    return {
        "status": status,
        "timestamp": datetime.now().isoformat(),
        "service": "VAC VPN API",
        "systems": systems,
        "environment": "production",
        "version": "2.0.0"
    }

@app.get("/servers")
async def get_available_servers():
    return {
        "success": True,
        "servers": VLESS_SERVERS
    }

@app.get("/debug-servers")
async def debug_servers():
    """Проверка статуса всех серверов"""
    results = {}
    for server_name, server_config in XRAY_SERVERS.items():
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f"{server_config['url']}/health",
                    timeout=10.0
                )
                results[server_name] = {
                    "status": response.status_code,
                    "url": server_config['url'],
                    "healthy": response.status_code == 200,
                    "response_time": f"{response.elapsed.total_seconds():.2f}s"
                }
        except Exception as e:
            results[server_name] = {
                "error": str(e),
                "url": server_config['url'],
                "healthy": False
            }
    return results

@app.post("/init-user")
async def init_user(request: InitUserRequest):
    try:
        if not db:
            return JSONResponse(status_code=500, content={"error": "Database not connected"})
        
        if not request.user_id or request.user_id == 'unknown':
            return JSONResponse(status_code=400, content={"error": "Invalid user ID"})
        
        logger.info(f"🔐 INIT USER: {request.user_id}, name: {request.first_name}, username: {request.username}")
        logger.info(f"📱 Start param: {request.start_param}")
        
        referrer_id = None
        is_referral = False
        bonus_applied = False
        
        # Обрабатываем реферальную систему из start_param
        if request.start_param:
            logger.info(f"🎯 Processing start_param: {request.start_param}")
            referrer_id = extract_referrer_id(request.start_param)
            
            if referrer_id and referrer_id != request.user_id:
                logger.info(f"👥 Referrer detected: {referrer_id}")
                
                referrer = get_user(referrer_id)
                
                if referrer:
                    referral_id = f"{referrer_id}_{request.user_id}"
                    referral_exists = db.collection('referrals').document(referral_id).get().exists
                    
                    if not referral_exists:
                        is_referral = True
                        bonus_result = add_referral_bonus_immediately(referrer_id, request.user_id)
                        if bonus_result:
                            bonus_applied = True
                            logger.info(f"💰 Referral bonus applied: {referrer_id} -> {request.user_id}")
                else:
                    logger.warning(f"⚠️ Referrer {referrer_id} not found in database")
        
        user_ref = db.collection('users').document(request.user_id)
        user_doc = user_ref.get()
        
        if not user_doc.exists:
            user_data = {
                'user_id': request.user_id,
                'username': request.username,
                'first_name': request.first_name,
                'last_name': request.last_name,
                'balance': REFERRAL_BONUS_REFERRED if bonus_applied else 0.0,
                'has_subscription': False,
                'subscription_days': 0,
                'subscription_start': None,
                'subscription_end': None,
                'vless_uuid': None,
                'preferred_server': None,
                'last_subscription_check': datetime.now().date().isoformat(),
                'created_at': firestore.SERVER_TIMESTAMP,
                'start_param': request.start_param,
                'referrer_id': referrer_id if is_referral else None
            }
            
            if is_referral and referrer_id:
                user_data['referred_by'] = referrer_id
            
            # Генерируем реферальную ссылку
            referral_link = generate_referral_link(request.user_id)
            user_data['referral_link'] = referral_link
            
            user_ref.set(user_data)
            
            logger.info(f"✅ NEW USER CREATED: {request.user_id} (referral: {is_referral}, bonus: {bonus_applied})")
            
            return {
                "success": True, 
                "message": "User created",
                "user_id": request.user_id,
                "is_referral": is_referral,
                "bonus_applied": bonus_applied,
                "referral_link": referral_link,
                "referrer_id": referrer_id
            }
        else:
            user_data = user_doc.to_dict()
            has_referrer = user_data.get('referred_by') is not None
            
            # Обновляем реферальную ссылку если её нет
            if not user_data.get('referral_link'):
                referral_link = generate_referral_link(request.user_id)
                save_referral_link(request.user_id, referral_link)
            else:
                referral_link = user_data.get('referral_link')
            
            logger.info(f"✅ EXISTING USER LOADED: {request.user_id} (has_referrer: {has_referrer})")
            
            return {
                "success": True, 
                "message": "User already exists", 
                "user_id": request.user_id,
                "is_referral": has_referrer,
                "bonus_applied": False,
                "referral_link": referral_link
            }
            
    except Exception as e:
        logger.error(f"❌ Error initializing user: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/user-data")
async def get_user_info(user_id: str):
    try:
        if not db:
            return JSONResponse(status_code=500, content={"error": "Database not connected"})
        
        if not user_id or user_id == 'unknown':
            return JSONResponse(status_code=400, content={"error": "Invalid user ID"})
            
        # Асинхронная проверка подписки
        asyncio.create_task(process_subscription_days_async(user_id))
            
        user = get_user(user_id)
        if not user:
            return {
                "user_id": user_id,
                "balance": 0,
                "has_subscription": False,
                "subscription_days": 0,
                "vless_uuid": None,
                "preferred_server": None,
                "subscription_start": None,
                "subscription_end": None,
                "referral_link": None
            }
        
        has_subscription = user.get('has_subscription', False)
        subscription_days = user.get('subscription_days', 0)
        vless_uuid = user.get('vless_uuid')
        balance = user.get('balance', 0.0)
        preferred_server = user.get('preferred_server')
        subscription_start = user.get('subscription_start')
        subscription_end = user.get('subscription_end')
        referral_link = user.get('referral_link')
        
        vless_keys = get_user_vless_keys(user_id)
        
        referrals = get_referrals(user_id)
        referral_count = len(referrals)
        total_bonus_money = sum([ref.get('referrer_bonus', 0) for ref in referrals])
        
        return {
            "user_id": user_id,
            "balance": balance,
            "has_subscription": has_subscription,
            "subscription_days": subscription_days,
            "vless_uuid": vless_uuid,
            "preferred_server": preferred_server,
            "subscription_start": subscription_start,
            "subscription_end": subscription_end,
            "referral_link": referral_link,
            "vless_keys": vless_keys,
            "referral_stats": {
                "total_referrals": referral_count,
                "total_bonus_money": total_bonus_money,
                "referrer_bonus": REFERRAL_BONUS_REFERRER,
                "referred_bonus": REFERRAL_BONUS_REFERRED
            },
            "available_servers": VLESS_SERVERS
        }
        
    except Exception as e:
        logger.error(f"❌ Error in get_user_info: {e}")
        return JSONResponse(status_code=500, content={"error": f"Error getting user info: {str(e)}"})

async def process_subscription_days_async(user_id: str):
    """Асинхронная обработка дней подписки"""
    try:
        process_subscription_days(user_id)
    except Exception as e:
        logger.error(f"❌ Error in async subscription processing: {e}")

@app.post("/add-balance")
async def add_balance(request: AddBalanceRequest):
    try:
        if not db:
            return JSONResponse(status_code=500, content={"error": "Database not connected"})
            
        user = get_user(request.user_id)
        if not user:
            return JSONResponse(status_code=404, content={"error": "User not found"})
        
        if request.amount < 10:
            return JSONResponse(status_code=400, content={"error": "Минимальная сумма пополнения 10₽"})
        
        if request.amount > 50000:
            return JSONResponse(status_code=400, content={"error": "Максимальная сумма пополнения 50,000₽"})
        
        if request.payment_method == "yookassa":
            SHOP_ID = os.getenv("SHOP_ID")
            API_KEY = os.getenv("API_KEY")
            
            if not SHOP_ID or not API_KEY:
                return JSONResponse(status_code=500, content={"error": "Payment gateway not configured"})
            
            payment_id = str(uuid.uuid4())
            save_payment(payment_id, request.user_id, request.amount, "balance", "balance", "yookassa")
            
            yookassa_data = {
                "amount": {"value": f"{request.amount:.2f}", "currency": "RUB"},
                "confirmation": {"type": "redirect", "return_url": "https://t.me/vaaaac_bot"},
                "capture": True,
                "description": f"Пополнение баланса VAC VPN на {request.amount}₽",
                "metadata": {
                    "payment_id": payment_id,
                    "user_id": request.user_id,
                    "payment_type": "balance",
                    "amount": request.amount
                },
                "save_payment_method": False
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    "https://api.yookassa.ru/v3/payments",
                    auth=(SHOP_ID, API_KEY),
                    headers={
                        "Content-Type": "application/json",
                        "Idempotence-Key": payment_id
                    },
                    json=yookassa_data,
                    timeout=30.0
                )
            
            if response.status_code in [200, 201]:
                payment_data = response.json()
                update_payment_status(payment_id, "pending", payment_data.get("id"))
                
                return {
                    "success": True,
                    "payment_id": payment_id,
                    "payment_url": payment_data["confirmation"]["confirmation_url"],
                    "amount": request.amount,
                    "status": "pending",
                    "message": f"Перейдите по ссылке для пополнения баланса на {request.amount}₽"
                }
            else:
                logger.error(f"❌ Yookassa API error: {response.status_code} - {response.text}")
                return JSONResponse(status_code=500, content={"error": f"Payment gateway error: {response.status_code}"})
        else:
            return JSONResponse(status_code=400, content={"error": "Invalid payment method"})
        
    except Exception as e:
        logger.error(f"❌ Error adding balance: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/activate-tariff")
async def activate_tariff(request: ActivateTariffRequest):
    try:
        if not db:
            return JSONResponse(status_code=500, content={"error": "Database not connected"})
            
        user = get_user(request.user_id)
        if not user:
            return JSONResponse(status_code=404, content={"error": "User not found"})
        
        if request.tariff not in TARIFFS:
            return JSONResponse(status_code=400, content={"error": "Invalid tariff"})
            
        tariff_data = TARIFFS[request.tariff]
        tariff_price = tariff_data["price"]
        tariff_days = tariff_data["days"]
        
        selected_server = request.selected_server or user.get('preferred_server') or "London"
        
        if request.payment_method == "balance":
            user_balance = user.get('balance', 0.0)
            
            if user_balance < tariff_price:
                return JSONResponse(status_code=400, content={"error": f"Недостаточно средств на балансе. Необходимо: {tariff_price}₽, доступно: {user_balance}₽"})
            
            payment_id = str(uuid.uuid4())
            save_payment(payment_id, request.user_id, tariff_price, request.tariff, "tariff", "balance", selected_server)
            
            update_user_balance(request.user_id, -tariff_price)
            
            success = await update_subscription_days(request.user_id, tariff_days, selected_server)
            
            if not success:
                return JSONResponse(status_code=500, content={"error": "Ошибка активации подписки"})
            
            if user.get('referred_by'):
                referrer_id = user['referred_by']
                referral_id = f"{referrer_id}_{request.user_id}"
                
                referral_exists = db.collection('referrals').document(referral_id).get().exists
                
                if not referral_exists:
                    add_referral_bonus_immediately(referrer_id, request.user_id)
            
            update_payment_status(payment_id, "succeeded")
            
            return {
                "success": True,
                "payment_id": payment_id,
                "amount": tariff_price,
                "days": tariff_days,
                "selected_server": selected_server,
                "status": "succeeded",
                "message": f"Подписка успешно активирована с баланса на сервере {selected_server}!"
            }
        
        elif request.payment_method == "yookassa":
            SHOP_ID = os.getenv("SHOP_ID")
            API_KEY = os.getenv("API_KEY")
            
            if not SHOP_ID or not API_KEY:
                return JSONResponse(status_code=500, content={"error": "Payment gateway not configured"})
            
            payment_id = str(uuid.uuid4())
            save_payment(payment_id, request.user_id, tariff_price, request.tariff, "tariff", "yookassa", selected_server)
            
            yookassa_data = {
                "amount": {"value": f"{tariff_price:.2f}", "currency": "RUB"},
                "confirmation": {"type": "redirect", "return_url": "https://t.me/vaaaac_bot"},
                "capture": True,
                "description": f"Покупка подписки {tariff_data['name']} - VAC VPN (Сервер: {selected_server})",
                "metadata": {
                    "payment_id": payment_id,
                    "user_id": request.user_id,
                    "tariff": request.tariff,
                    "payment_type": "tariff",
                    "tariff_days": tariff_days,
                    "selected_server": selected_server
                },
                "save_payment_method": False
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    "https://api.yookassa.ru/v3/payments",
                    auth=(SHOP_ID, API_KEY),
                    headers={
                        "Content-Type": "application/json",
                        "Idempotence-Key": payment_id
                    },
                    json=yookassa_data,
                    timeout=30.0
                )
            
            if response.status_code in [200, 201]:
                payment_data = response.json()
                update_payment_status(payment_id, "pending", payment_data.get("id"))
                
                return {
                    "success": True,
                    "payment_id": payment_id,
                    "payment_url": payment_data["confirmation"]["confirmation_url"],
                    "amount": tariff_price,
                    "days": tariff_days,
                    "selected_server": selected_server,
                    "status": "pending",
                    "message": f"Перейдите по ссылке для оплаты подписки на сервере {selected_server}"
                }
            else:
                logger.error(f"❌ Yookassa API error: {response.status_code} - {response.text}")
                return JSONResponse(status_code=500, content={"error": f"Payment gateway error: {response.status_code}"})
        
        else:
            return JSONResponse(status_code=400, content={"error": "Invalid payment method"})
        
    except Exception as e:
        logger.error(f"❌ Error activating tariff: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/buy-with-balance")
async def buy_with_balance(request: BuyWithBalanceRequest):
    try:
        if not db:
            return JSONResponse(status_code=500, content={"error": "Database not connected"})
        
        user = get_user(request.user_id)
        if not user:
            return JSONResponse(status_code=404, content={"error": "User not found"})
        
        selected_server = request.selected_server or "London"
        
        user_balance = user.get('balance', 0.0)
        
        if user_balance < request.tariff_price:
            return JSONResponse(status_code=400, content={
                "success": False,
                "error": f"Недостаточно средств на балансе. На вашем балансе {user_balance}₽, а требуется {request.tariff_price}₽"
            })
        
        payment_id = str(uuid.uuid4())
        save_payment(payment_id, request.user_id, request.tariff_price, request.tariff_id, "tariff", "balance", selected_server)
        
        update_user_balance(request.user_id, -request.tariff_price)
        
        success = await update_subscription_days(request.user_id, request.tariff_days, selected_server)
        
        if not success:
            return JSONResponse(status_code=500, content={"error": "Ошибка активации подписки"})
        
        if user.get('referred_by'):
            referrer_id = user['referred_by']
            referral_id = f"{referrer_id}_{request.user_id}"
            
            referral_exists = db.collection('referrals').document(referral_id).get().exists
            
            if not referral_exists:
                add_referral_bonus_immediately(referrer_id, request.user_id)
        
        update_payment_status(payment_id, "succeeded")
        
        return {
            "success": True,
            "payment_id": payment_id,
            "amount": request.tariff_price,
            "days": request.tariff_days,
            "selected_server": selected_server,
            "status": "succeeded",
            "message": f"Подписка успешно активирована с баланса на сервере {selected_server}!"
        }
        
    except Exception as e:
        logger.error(f"❌ Error in buy-with-balance: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/payment-status")
async def check_payment(payment_id: str, user_id: str):
    try:
        if not db:
            return JSONResponse(status_code=500, content={"error": "Database not connected"})
            
        if not payment_id or payment_id == 'undefined':
            return JSONResponse(status_code=400, content={"error": "Invalid payment ID"})
            
        payment = get_payment(payment_id)
        if not payment:
            return JSONResponse(status_code=404, content={"error": "Payment not found"})
        
        actual_user_id = user_id if user_id != 'undefined' else payment.get('user_id')
        
        if not actual_user_id or actual_user_id == 'undefined':
            return JSONResponse(status_code=400, content={"error": "Invalid user ID"})
        
        if payment['status'] == 'succeeded':
            if payment['payment_type'] == 'balance':
                return {
                    "success": True,
                    "status": "succeeded",
                    "payment_id": payment_id,
                    "amount": payment['amount'],
                    "balance_added": payment['amount']
                }
            else:
                return {
                    "success": True,
                    "status": "succeeded",
                    "payment_id": payment_id,
                    "amount": payment['amount'],
                    "selected_server": payment.get('selected_server')
                }
        
        if payment.get('payment_method') == 'yookassa':
            yookassa_id = payment.get('yookassa_id')
            if yookassa_id:
                SHOP_ID = os.getenv("SHOP_ID")
                API_KEY = os.getenv("API_KEY")
                
                if not SHOP_ID or not API_KEY:
                    return JSONResponse(status_code=500, content={"error": "Payment gateway not configured"})
                
                async with httpx.AsyncClient() as client:
                    response = await client.get(
                        f"https://api.yookassa.ru/v3/payments/{yookassa_id}",
                        auth=(SHOP_ID, API_KEY),
                        timeout=30.0
                    )
                    
                    if response.status_code == 200:
                        yookassa_data = response.json()
                        status = yookassa_data.get('status')
                        
                        update_payment_status(payment_id, status, yookassa_id)
                        
                        if status == 'succeeded':
                            if payment['payment_type'] == 'balance':
                                amount = payment['amount']
                                success = update_user_balance(actual_user_id, amount)
                                
                                if success:
                                    return {
                                        "success": True,
                                        "status": status,
                                        "payment_id": payment_id,
                                        "amount": amount,
                                        "balance_added": amount,
                                        "message": f"Баланс успешно пополнен на {amount}₽!"
                                    }
                                else:
                                    return JSONResponse(status_code=500, content={"error": "Ошибка пополнения баланса"})
                            
                            tariff_user_id = payment.get('user_id', actual_user_id)
                            tariff = payment['tariff']
                            tariff_days = TARIFFS[tariff]["days"]
                            selected_server = payment.get('selected_server')
                            
                            success = await update_subscription_days(tariff_user_id, tariff_days, selected_server)
                            
                            if not success:
                                return JSONResponse(status_code=500, content={"error": "Failed to activate subscription"})
                            
                            user = get_user(tariff_user_id)
                            if user and user.get('referred_by'):
                                referrer_id = user['referred_by']
                                referral_id = f"{referrer_id}_{tariff_user_id}"
                                
                                referral_exists = db.collection('referrals').document(referral_id).get().exists
                                
                                if not referral_exists:
                                    add_referral_bonus_immediately(referrer_id, tariff_user_id)
                            
                            return {
                                "success": True,
                                "status": status,
                                "payment_id": payment_id,
                                "amount": payment['amount'],
                                "days_added": tariff_days,
                                "selected_server": selected_server
                            }
        
        return {
            "success": True,
            "status": payment['status'],
            "payment_id": payment_id
        }
        
    except Exception as e:
        logger.error(f"❌ Error checking payment: {e}")
        return JSONResponse(status_code=500, content={"error": f"Error checking payment: {str(e)}"})

@app.get("/get-vless-config")
async def get_vless_config(user_id: str, server_id: str = None):
    try:
        if not db:
            return JSONResponse(status_code=500, content={"error": "Database not connected"})
            
        # Асинхронная проверка подписки
        asyncio.create_task(process_subscription_days_async(user_id))
            
        user = get_user(user_id)
        if not user:
            return JSONResponse(status_code=404, content={"error": "User not found"})
        
        if not user.get('has_subscription', False):
            return JSONResponse(status_code=400, content={"error": "No active subscription"})
        
        # Получаем UUID
        vless_uuid = await ensure_user_uuid(user_id, server_id)
        
        # Создаем конфиги
        configs = create_user_vless_configs(user_id, vless_uuid, server_id)
        
        return {
            "success": True,
            "user_id": user_id,
            "vless_uuid": vless_uuid,
            "has_subscription": True,
            "subscription_days": user.get('subscription_days', 0),
            "selected_server": server_id or "all",
            "configs": configs,
            "config_ready": True,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting VLESS config: {e}")
        return JSONResponse(status_code=500, content={"error": f"Error getting VLESS config: {str(e)}"})

# Дополнительные endpoints для диагностики и управления
@app.post("/manual-check-payment")
async def manual_check_payment(payment_id: str):
    """Ручная проверка статуса платежа"""
    try:
        payment = get_payment(payment_id)
        if not payment:
            return {"error": "Payment not found"}
        
        yookassa_id = payment.get('yookassa_id')
        if not yookassa_id:
            return {"error": "No Yookassa ID"}
        
        SHOP_ID = os.getenv("SHOP_ID")
        API_KEY = os.getenv("API_KEY")
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"https://api.yookassa.ru/v3/payments/{yookassa_id}",
                auth=(SHOP_ID, API_KEY),
                timeout=30.0
            )
            
            if response.status_code == 200:
                yookassa_data = response.json()
                status = yookassa_data.get('status')
                
                if status == 'succeeded':
                    # Обрабатываем успешный платеж
                    user_id = payment.get('user_id')
                    payment_type = payment.get('payment_type')
                    
                    if payment_type == 'tariff':
                        tariff = payment.get('tariff')
                        tariff_days = TARIFFS[tariff]["days"]
                        selected_server = payment.get('selected_server')
                        
                        success = await update_subscription_days(user_id, tariff_days, selected_server)
                        if success:
                            update_payment_status(payment_id, 'succeeded')
                            return {"success": True, "message": "Tariff activated"}
                    
                    elif payment_type == 'balance':
                        amount = payment.get('amount')
                        success = update_user_balance(user_id, amount)
                        if success:
                            update_payment_status(payment_id, 'succeeded')
                            return {"success": True, "message": "Balance updated"}
                
                return {"status": status}
            
            return {"error": f"Yookassa API error: {response.status_code}"}
            
    except Exception as e:
        return {"error": str(e)}

@app.post("/emergency-add-to-xray")
async def emergency_add_to_xray(user_id: str):
    """Экстренное добавление пользователя во все Xray серверы"""
    try:
        user = get_user(user_id)
        if not user:
            return JSONResponse(status_code=404, content={"error": "User not found"})
        
        vless_uuid = user.get('vless_uuid')
        if not vless_uuid:
            return JSONResponse(status_code=400, content={"error": "User has no UUID"})
        
        success_count = 0
        for server_name, server_config in XRAY_SERVERS.items():
            try:
                success = await add_user_to_xray_server(server_name, user_id, vless_uuid)
                if success:
                    success_count += 1
            except Exception as e:
                logger.error(f"❌ Emergency add failed for {server_name}: {e}")
        
        user_vless_keys = get_user_vless_keys(user_id)
        for key_data in user_vless_keys:
            update_vless_key_status(user_id, key_data['server_id'], True)
        
        return {
            "success": True,
            "message": f"User {user_id} emergency added to {success_count} servers",
            "servers_added": success_count,
            "keys_activated": len(user_vless_keys)
        }
            
    except Exception as e:
        logger.error(f"❌ Error in emergency-add-to-xray: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/get-referral-link")
async def get_referral_link_endpoint(user_id: str):
    """Получить реферальную ссылку пользователя"""
    try:
        if not db:
            return JSONResponse(status_code=500, content={"error": "Database not connected"})
        
        user = get_user(user_id)
        if not user:
            return JSONResponse(status_code=404, content={"error": "User not found"})
        
        referral_link = user.get('referral_link')
        if not referral_link:
            # Если ссылки нет, генерируем и сохраняем её
            referral_link = generate_referral_link(user_id)
            save_referral_link(user_id, referral_link)
        
        return {
            "success": True,
            "user_id": user_id,
            "referral_link": referral_link
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting referral link: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/referral-stats")
async def get_referral_stats(user_id: str):
    """Получить статистику по рефералам"""
    try:
        if not db:
            return JSONResponse(status_code=500, content={"error": "Database not connected"})
        
        referrals = get_referrals(user_id)
        referral_count = len(referrals)
        total_bonus_money = sum([ref.get('referrer_bonus', 0) for ref in referrals])
        
        return {
            "success": True,
            "user_id": user_id,
            "referral_count": referral_count,
            "total_bonus_money": total_bonus_money,
            "referrals": referrals
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting referral stats: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/debug-user/{user_id}")
async def debug_user(user_id: str):
    """Диагностика пользователя"""
    try:
        user = get_user(user_id)
        if not user:
            return {"error": "User not found"}
        
        payments_ref = db.collection('payments').where('user_id', '==', user_id)
        payments = payments_ref.stream()
        payment_list = [payment.to_dict() for payment in payments]
        
        vless_keys = get_user_vless_keys(user_id)
        
        xray_status = {}
        if user.get('vless_uuid'):
            for server_name in XRAY_SERVERS.keys():
                xray_status[server_name] = await check_user_in_xray(user['vless_uuid'], server_name)
        
        return {
            "user": user,
            "payments": payment_list,
            "vless_keys": vless_keys,
            "xray_status": xray_status,
            "subscription_active": user.get('has_subscription', False) and user.get('subscription_days', 0) > 0
        }
        
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
