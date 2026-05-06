from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import uvicorn

# Инициализация приложения
app = FastAPI(title="Fake News Detector API")

# Загрузка обученной модели при старте сервера
model = joblib.load('experiments/model.pkl')


# Схема входящих данных (ожидаем текст новости)
class NewsItem(BaseModel):
    text: str


# Создаем метод API
@app.post("/predict")
def predict_news(item: NewsItem):
    # Делаем предсказание. Модель ожидает список, поэтому оборачиваем текст в []
    prediction = model.predict([item.text])[0]
    probability = model.predict_proba([item.text])[0]

    # Расшифровываем результат (0 - Фейк, 1 - Правда)
    result = "True News" if prediction == 1 else "Fake News"

    return {
        "prediction": result,
        "confidence_fake": round(float(probability[0]), 4),
        "confidence_true": round(float(probability[1]), 4)
    }


if __name__ == "__main__":
    # Запуск локального веб-сервера
    uvicorn.run(app, host="0.0.0.0", port=8000)