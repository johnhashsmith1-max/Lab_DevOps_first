from fastapi.testclient import TestClient
from main import app

# Создаем тестовый клиент на основе нашего FastAPI приложения
client = TestClient(app)


def test_predict_endpoint_fake():
    # Имитируем отправку POST-запроса на наш эндпоинт
    response = client.post(
        "/predict",
        json={"text": "BREAKING: Aliens have landed in New York and are giving out free pizza!"}
    )

    # Проверяем, что сервер ответил статусом 200 (ОК)
    assert response.status_code == 200

    # Получаем ответ в формате JSON (словарь)
    data = response.json()

    # Проверяем, что в ответе есть нужные нам ключи
    assert "prediction" in data
    assert "confidence_fake" in data
    assert "confidence_true" in data

    # Проверяем типы данных, чтобы убедиться, что API работает корректно
    assert isinstance(data["prediction"], str)
    assert isinstance(data["confidence_fake"], float)


def test_predict_endpoint_empty():
    # Проверяем, как API справляется с пустым текстом
    response = client.post(
        "/predict",
        json={"text": ""}
    )
    assert response.status_code == 200
    assert "prediction" in response.json()