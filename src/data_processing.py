import pandas as pd
import os

def load_and_process_data(fake_path, true_path, output_path):
    print("Загрузка данных...")
    # Загружаем датасеты
    fake_df = pd.read_csv(fake_path)
    true_df = pd.read_csv(true_path)

    # Добавляем целевую переменную (label): 0 - фейк, 1 - реальная новость
    fake_df['label'] = 0
    true_df['label'] = 1

    # Объединяем в один датафрейм
    print("Объединение данных...")
    df = pd.concat([fake_df, true_df], ignore_index=True)

    # Для классификации нам хватит заголовка и самого текста. Объединим их.
    df['text'] = df['title'] + " " + df['text']
    
    # Оставляем только нужные колонки
    df = df[['text', 'label']]

    # Перемешиваем строки, чтобы фейки и правда шли вперемешку, и сбрасываем индексы
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)

    # Сохраняем результат
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Данные успешно обработаны и сохранены в {output_path}")

if __name__ == "__main__":
    # Пути относительно корневой папки проекта
    FAKE_DATA_PATH = 'data/raw/fake.csv'
    TRUE_DATA_PATH = 'data/raw/true.csv'
    OUTPUT_PATH = 'data/processed/news_data.csv'
    
    load_and_process_data(FAKE_DATA_PATH, TRUE_DATA_PATH, OUTPUT_PATH)