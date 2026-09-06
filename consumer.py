import os
import time
import json
import hvac
import socket
import requests
from kafka import KafkaConsumer
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


# --- 1. Получение секретов из Vault ---
def get_vault_db_credentials():
    vault_host = 'vault'
    vault_port = '8200'
    vault_token = os.getenv('VAULT_TOKEN', 'myroot')

    # Принудительно резолвим IPv4 (обход бага IPv6 DNS в связке Debian/requests/Docker)
    vault_ip = vault_host
    for attempt in range(5):
        try:
            vault_ip = socket.gethostbyname(vault_host)
            print(f"Consumer: IP Vault успешно определен -> {vault_ip}")
            break
        except socket.error as e:
            print(f"Consumer: Ожидание DNS Docker... ({e})")
            time.sleep(2)

    vault_url = f"http://{vault_ip}:{vault_port}"

    session = requests.Session()
    session.trust_env = False

    client = hvac.Client(url=vault_url, token=vault_token, session=session)

    for attempt in range(5):
        try:
            print(f"Consumer: Обращение в Vault ({vault_url}), попытка {attempt + 1}/5...")
            response = client.secrets.kv.v2.read_secret_version(
                path='db_credentials', raise_on_deleted_version=True
            )
            print("Consumer: Секреты успешно получены!")
            return response['data']['data']
        except Exception as e:
            print(f"Consumer: Ошибка подключения к Vault: {e}")
            time.sleep(3)
    raise Exception("Не удалось получить секреты из Vault")


# --- 2. Настройка БД ---
credentials = get_vault_db_credentials()
DB_USER = credentials['username']
DB_PASS = credentials['password']
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

# --- 3. Настройка Kafka Consumer ---
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