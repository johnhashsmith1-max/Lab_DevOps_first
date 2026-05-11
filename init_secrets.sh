#!/bin/sh
# Ждем пару секунд, чтобы Vault точно успел поднять API
sleep 3

# Включаем KV хранилище второй версии (стандарт для Vault)
vault secrets enable -path=secret kv-v2

# Записываем секреты
vault kv put secret/db_credentials username=admin password=supersecret_pass

echo "Секреты успешно загружены в Vault!"