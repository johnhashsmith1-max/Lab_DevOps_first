import os
import time
import hvac
from fastapi import FastAPI, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session


# --- 1. Получение секретов из Vault с Retry-логикой ---
def get_vault_db_credentials():
    print("Обращаемся в Vault за секретами БД...")
    vault_url = os.getenv('VAULT_ADDR', 'http://vault:8200')
    vault_token = os.getenv('VAULT_TOKEN', 'myroot')

    client = hvac.Client(url=vault_url, token=vault_token)

    # Добавляем цикл из 5 попыток для защиты от плавающих ошибок DNS
    for attempt in range(5):
        try:
            # raise_on_deleted_version=True убирает желтый DeprecationWarning из логов
            response = client.secrets.kv.v2.read_secret_version(
                path='db_credentials',
                raise_on_deleted_version=True
            )
            print("Секреты успешно получены!")
            return response['data']['data']
        except Exception as e:
            print(f"Ошибка сети/DNS (попытка {attempt + 1}/5): {e}")
            time.sleep(2)

    raise Exception("Не удалось получить секреты из Vault после 5 попыток")


# Получаем учетные данные из Vault
credentials = get_vault_db_credentials()
DB_USER = credentials['username']
DB_PASS = credentials['password']
# --------------------------------------------------

# 2. Хост, порт и имя БД оставляем в переменных окружения
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "fakenews_db")

# Формируем URL для подключения
SQLALCHEMY_DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# 3. Настраиваем SQLAlchemy
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# 4. Описываем таблицу в БД
class PredictionResult(Base):
    __tablename__ = "predictions"
    id = Column(Integer, primary_key=True, index=True)
    news_text = Column(String, index=True)
    prediction_label = Column(String)
    probability = Column(Float)


# Создаем таблицы
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Fake News Detector API")


# Зависимость для получения сессии БД
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class NewsRequest(BaseModel):
    text: str


# 5. Наполняем БД валидационными данными при старте
@app.on_event("startup")
def startup_populate_db():
    db = SessionLocal()
    if db.query(PredictionResult).count() == 0:
        sample_data = PredictionResult(
            news_text="Sample validated news for testing",
            prediction_label="Real",
            probability=0.99
        )
        db.add(sample_data)
        db.commit()
    db.close()


# 6. Эндпоинт предсказания
@app.post("/predict")
def predict_news(request: NewsRequest, db: Session = Depends(get_db)):
    label = "Fake"
    prob = 0.85

    db_record = PredictionResult(news_text=request.text, prediction_label=label, probability=prob)
    db.add(db_record)
    db.commit()
    db.refresh(db_record)

    return {"text": request.text, "label": label, "probability": prob, "db_id": db_record.id}