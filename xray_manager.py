class XrayManager:
    def __init__(self):
        self.script_path = "/usr/local/bin/add_vpn_user"
        
    async def add_user(self, email: str, uuid_str: str = None) -> bool:
        """Добавляет пользователя через скрипт"""
        try:
            logger.info(f"🔄 Adding user via script: {email}")
            
            # На Railway используем прямое редактирование конфига
            return await self.add_user_direct(email, uuid_str)
                
        except Exception as e:
            logger.error(f"❌ Error adding user via script: {e}")
            return False
    
    async def add_user_direct(self, email: str, uuid_str: str = None) -> bool:
        """Добавляет пользователя напрямую в конфиг"""
        try:
            logger.info(f"🔄 Adding user directly to config: {email}")
            
            if not uuid_str:
                uuid_str = str(uuid.uuid4())
            
            config_path = "/usr/local/etc/xray/config.json"
            
            # Читаем конфиг
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            new_user = {
                "id": uuid_str,
                "email": email,
                "flow": ""
            }
            
            # Добавляем пользователя в первый inbound
            config['inbounds'][0]['settings']['clients'].append(new_user)
            
            # Сохраняем конфиг
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)
            
            # На Railway перезапускаем Xray через pkill + запуск в фоне
            await self.restart_xray()
            
            logger.info(f"✅ User {email} successfully added directly to config")
            return True, uuid_str
            
        except Exception as e:
            logger.error(f"❌ Error adding user directly: {e}")
            return False, None
    
    async def restart_xray(self):
        """Перезапускает Xray на Railway"""
        try:
            # Ищем процесс Xray и убиваем его
            subprocess.run(["pkill", "-f", "xray"], capture_output=True)
            
            # Ждем немного
            await asyncio.sleep(2)
            
            # Запускаем Xray в фоне
            subprocess.Popen([
                "/usr/local/bin/xray", "run", "-config", "/usr/local/etc/xray/config.json"
            ])
            
            logger.info("✅ Xray restarted")
            
        except Exception as e:
            logger.error(f"❌ Error restarting Xray: {e}")
    
    async def remove_user(self, email: str) -> bool:
        """Удаляет пользователя из конфига"""
        try:
            logger.info(f"🔄 Removing user from config: {email}")
            
            config_path = "/usr/local/etc/xray/config.json"
            
            # Читаем конфиг
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # Удаляем пользователя
            for inbound in config['inbounds']:
                if inbound.get('tag') == 'inbound-1':
                    original_count = len(inbound['settings']['clients'])
                    inbound['settings']['clients'] = [
                        client for client in inbound['settings']['clients'] 
                        if client.get('email') != email
                    ]
                    new_count = len(inbound['settings']['clients'])
                    
                    if new_count < original_count:
                        logger.info(f"✅ Removed user {email} from config")
                    break
            
            # Сохраняем конфиг
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)
            
            # Перезапускаем Xray
            await self.restart_xray()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error removing user: {e}")
            return False
