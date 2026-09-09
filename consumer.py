import os

# --- БЛОКИРОВКА СИСТЕМНЫХ ПРОКСИ (закомментировано для тестов без Vault) ---
# for key in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
#     os.environ.pop(key, None)

import time
import json
from kafka import KafkaConsumer
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# import requests
# import hvac
# import socket

# --- ПРИНУДИТЕЛЬНЫЙ IPv4 DNS (ЗАЩИТА ОТ БАГОВ DOCKER/GLIBC) ---
# old_getaddrinfo = socket.getaddrinfo
# def new_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
#     return old_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
# socket.getaddrinfo = new_getaddrinfo
# -------------------------------------------------------------

# def get_vault_db_credentials():
#     print("Consumer: Ожидание 6 секунд для инициализации секретов Vault...")
#     time.sleep(6)
#
#     vault_url = os.getenv('VAULT_ADDR', 'http://vault:8200')
#     vault_token = os.getenv('VAULT_TOKEN', 'myroot')
#
#     session = requests.Session()
#     session.trust_env = False
#
#     for attempt in range(10):
#         try:
#             print(f"Consumer: Обращение в Vault ({vault_url}), попытка {attempt + 1}/10...")
#             client = hvac.Client(url=vault_url, token=vault_token, session=session)
#             response = client.secrets.kv.v2.read_secret_version(
#                 path='db_credentials', raise_on_deleted_version=True
#             )
#             print("Consumer: Секреты успешно получены!")
#             return response['data']['data']
#         except Exception as e:
#             print(f"Consumer: Секреты еще не готовы (ошибка: {e}). Повтор через 3 сек...")
#             time.sleep(3)
#     raise Exception("Не удалось получить секреты из Vault")

# --- ВРЕМЕННЫЙ ХАРДКОД КРЕДЕНШНАЛОВ (Без Vault) ---
# credentials = get_vault_db_credentials()
# DB_USER = credentials['username']
# DB_PASS = credentials['password']

DB_USER = os.getenv("POSTGRES_USER", "myuser")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "mypassword")
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "mydb")

SQLALCHEMY_DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class PredictionResult(Base):
    __tablename__ = "predictions"
    id = Column(Integer, primary_key=True, index=True)
    news_text = Column(String, index=True)
    prediction_label = Column(String)
    probability = Column(Float)

Base.metadata.create_all(bind=engine)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")

def start_consumer():
    print(f"Consumer: Подключение к Kafka ({KAFKA_BROKER})...")
    time.sleep(2)

    consumer = KafkaConsumer(
        'predictions_topic',
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='db_writer_group',
        auto_offset_reset='earliest'
    )
    print("Consumer: Успешно подключен. Ожидание сообщений...")

    db = SessionLocal()
    try:
        for message in consumer:
            data = message.value
            print(f"Consumer: Получено сообщение -> {data}")

            db_record = PredictionResult(
                news_text=data["text"],
                prediction_label=data["label"],
                probability=data["probability"]
            )
            db.add(db_record)
            db.commit()
            print("Consumer: SUCCESS_DB_WRITE")
    finally:
        db.close()

if __name__ == "__main__":
    start_consumer()