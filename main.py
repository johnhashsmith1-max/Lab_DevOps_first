import os
from fastapi import FastAPI, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

# 1. Читаем доступы из переменных окружения
DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "supersecret_pass")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "fakenews_db")

SQLALCHEMY_DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# 2. Настраиваем SQLAlchemy
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 3. Описываем таблицу в БД
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

# 4. Наполняем БД валидационными данными при старте
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


# 5. Эндпоинт предсказания и взаимодействия с источником данных
@app.post("/predict")
def predict_news(request: NewsRequest, db: Session = Depends(get_db)):
    # Здесь логика твоей модели (заглушка для примера)
    # prediction = model.predict([request.text])
    label = "Fake"  # результат работы модели
    prob = 0.85

    # Отправка результатов модели в базу данных
    db_record = PredictionResult(news_text=request.text, prediction_label=label, probability=prob)
    db.add(db_record)
    db.commit()
    db.refresh(db_record)

    return {"text": request.text, "label": label, "probability": prob, "db_id": db_record.id}