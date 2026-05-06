import pandas as pd
import configparser
import joblib
import os
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score, classification_report

def train_model():
    # 1. Читаем конфигурацию из config.ini
    config = configparser.ConfigParser()
    config.read('config.ini')

    max_features = int(config['hyperparameters']['max_features'])
    c_param = float(config['hyperparameters']['C'])
    random_state = int(config['hyperparameters']['random_state'])

    print("Загрузка данных...")
    df = pd.read_csv('data/processed/news_data.csv')

    X = df['text']
    y = df['label']

    # Разбиваем данные на обучающую (80%) и тестовую (20%) выборки
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=random_state)

    print("Создание пайплайна и обучение модели...")
    # Pipeline объединяет процесс перевода текста в числа (Tfidf) и саму модель (LogisticRegression)
    pipeline = Pipeline([
        ('tfidf', TfidfVectorizer(max_features=max_features, stop_words='english')),
        ('clf', LogisticRegression(C=c_param, random_state=random_state, max_iter=200))
    ])

    # Обучаем модель
    pipeline.fit(X_train, y_train)

    print("Оценка модели...")
    predictions = pipeline.predict(X_test)
    acc = accuracy_score(y_test, predictions)
    print(f"Точность (Accuracy): {acc:.4f}")
    print(classification_report(y_test, predictions))

    # Сохраняем готовую модель в папку experiments
    os.makedirs('experiments', exist_ok=True)
    joblib.dump(pipeline, 'experiments/model.pkl')
    print("Модель успешно сохранена в experiments/model.pkl")

if __name__ == "__main__":
    train_model()