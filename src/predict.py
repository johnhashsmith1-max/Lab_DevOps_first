import os
import json
from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer

app = FastAPI(title="Fake News Detector API (Kafka Producer)")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
producer = None

@app.on_event("startup")
def startup_event():
    global producer
    print(f"API: Подключение к Kafka ({KAFKA_BROKER})...")
    import time
    for _ in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("API: Успешно подключен к Kafka!")
            break
        except Exception as e:
            print(f"API: Ошибка подключения к Kafka, повторяем... {e}")
            time.sleep(3)

class NewsRequest(BaseModel):
    text: str

@app.post("/predict")
def predict_news(request: NewsRequest):
    # Здесь должна быть твоя логика предсказания модели
    # Оставляем заглушку или вставь сюда свой реальный инференс
    label = "Fake News"
    prob_fake = 0.8915

    msg = {
        "text": request.text,
        "label": label,
        "probability": prob_fake
    }

    if producer:
        producer.send('predictions_topic', value=msg)
        producer.flush() 

    return {"status": "queued", "message": "Prediction sent to Kafka", "data": msg}