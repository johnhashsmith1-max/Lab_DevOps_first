"""
data_processing.py — модуль для загрузки и предобработки данных
"""
import pandas as pd
import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class NewsDataProcessor:
    def __init__(self, raw_dir: str = "data/raw", processed_dir: str = "data/processed"):
        self.raw_dir = Path(raw_dir)
        self.processed_dir = Path(processed_dir)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        nltk.download('stopwords', quiet=True)

    def load_data(self) -> pd.DataFrame:
        """Загружает и объединяет fake/true датасеты"""
        fake = pd.read_csv(self.raw_dir / "fake.csv")
        true = pd.read_csv(self.raw_dir / "true.csv")

        fake['label'] = 0
        true['label'] = 1

        df = pd.concat([fake, true], ignore_index=True)
        logger.info(f"Loaded {len(df)} samples, label distribution: {df['label'].value_counts().to_dict()}")
        return df

    def preprocess_text(self, text: str) -> str:
        """Базовая предобработка текста"""
        if pd.isna(text):
            return ""
        text = text.lower()
        text = re.sub(r'[^a-zA-Z\s]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()

        stop_words = set(stopwords.words('english'))
        words = [w for w in text.split() if w not in stop_words]

        stemmer = PorterStemmer()
        words = [stemmer.stem(w) for w in words]
        return ' '.join(words)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Применяет предобработку ко всему датасету"""
        df = df.copy()
        df['text_clean'] = df['text'].apply(self.preprocess_text)
        df['title_clean'] = df['title'].apply(self.preprocess_text)
        logger.info("Text preprocessing completed")
        return df

    def save(self, df: pd.DataFrame, filename: str = "news_data.csv"):
        """Сохраняет обработанный датасет"""
        path = self.processed_dir / filename
        df.to_csv(path, index=False)
        logger.info(f"Saved processed data to {path}")
        return path