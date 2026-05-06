"""
train.py — обучение модели классификации новостей
"""
import pandas as pd
import joblib
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, accuracy_score
import logging
import configparser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_config(config_path: str = "config.ini") -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


def train_model(data_path: str = "data/processed/news_data.csv",
                config_path: str = "config.ini"):
    """Обучает модель и сохраняет артефакты"""
    # Загрузка конфигурации
    config = load_config(config_path)

    # Параметры
    test_size = config.getfloat('model', 'test_size', fallback=0.25)
    max_features = config.getint('vectorizer', 'max_features', fallback=5000)
    random_state = config.getint('model', 'random_state', fallback=42)

    # Загрузка данных
    logger.info(f"Loading data from {data_path}")
    df = pd.read_csv(data_path)

    # Подготовка признаков и целевой переменной
    X = df['text_clean'].fillna('')
    y = df['label']

    # Разделение
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )

    # Векторизация
    logger.info(f"Vectorizing text with TF-IDF (max_features={max_features})")
    vectorizer = TfidfVectorizer(max_features=max_features, ngram_range=(1, 2))
    X_train_vec = vectorizer.fit_transform(X_train)
    X_test_vec = vectorizer.transform(X_test)

    # Обучение модели
    logger.info("Training Logistic Regression model")
    model = LogisticRegression(
        max_iter=config.getint('model', 'max_iter', fallback=1000),
        random_state=random_state,
        class_weight='balanced'
    )
    model.fit(X_train_vec, y_train)

    # Оценка
    y_pred = model.predict(X_test_vec)
    accuracy = accuracy_score(y_test, y_pred)
    logger.info(f"\nTest Accuracy: {accuracy:.4f}")
    logger.info(f"\nClassification Report:\n{classification_report(y_test, y_pred)}")

    # Сохранение артефактов
    joblib.dump(model, 'models/model.pkl')
    joblib.dump(vectorizer, 'models/vectorizer.pkl')
    logger.info("Model and vectorizer saved to models/")

    return {
        'accuracy': accuracy,
        'report': classification_report(y_test, y_pred, output_dict=True)
    }


if __name__ == "__main__":
    metrics = train_model()
    print(f"\nFinal Accuracy: {metrics['accuracy']:.4f}")