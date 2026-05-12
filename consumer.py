import os
import time
import json
import hvac
from kafka import KafkaConsumer
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


# --- 1. Получение секретов из Vault ---
def get_vault_db_credentials():
    print("Consumer: Ждем 10 секунд для стабилизации внутренней сети Docker...")
    time.sleep(10)
    print("Consumer: Обращаемся в Vault за секретами БД...")

    vault_url = os.getenv('VAULT_ADDR', 'http://vault:8200')
    vault_token = os.getenv('VAULT_TOKEN', 'myroot')

    for attempt in range(5):
        try:
            # ВАЖНО: Инициализируем клиент ЗДЕСЬ, внутри цикла.
            # Это принудительно сбрасывает кэш DNS при каждой попытке!
            client = hvac.Client(url=vault_url, token=vault_token)

            response = client.secrets.kv.v2.read_secret_version(
                path='db_credentials', raise_on_deleted_version=True
            )
            print("Consumer: Секреты успешно получены!")
            return response['data']['data']
        except Exception as e:
            print(f"Consumer: Ошибка сети/DNS Vault (попытка {attempt + 1}/5): {e}")
            time.sleep(5)  # Пауза между попытками
    raise Exception("Не удалось получить секреты из Vault")


# --- 2. Настройка БД ---
credentials = get_vault_db_credentials()
DB_USER = credentials['username']
DB_PASS = credentials['password']
DB_HOST = os.getenv("DB_HOST", "lab3_db")
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
    print(f"Consumer: Подключение к Kafka по адресу {KAFKA_BROKER}...")
    # Даем Kafka время на полную инициализацию
    time.sleep(10)

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

            # Запись в БД
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