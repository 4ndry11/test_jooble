#Імморт необхідних бібліотек
import sys
import os
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Завантаження змінних оточення з .env файлу
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def connect_to_db():
    """
    Створити SQLAlchemy engine для підключення до PostgreSQL
    Використовуйте environment variables для параметрів підключення
    Поверніть engine об'єкт
    Обробіть помилки підключення
    """
    try:
        # Читання параметрів з environment variables
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')

        # Перевірка наявності обов'язкових параметрів
        if not all([db_host, db_name, db_user, db_password]):
            raise ValueError("Не всі обов'язкові environment variables встановлені")

        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

        engine = create_engine(connection_string)

        # Тестове підключення
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        print("Підключено до бази даних успішно")
        return engine

    except ValueError as ve:
        print(f"Помилка конфігурації: {ve}")
        sys.exit(1)
    except SQLAlchemyError as e:
        print(f"Помилка підключення до бази даних: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Невідома помилка при підключенні: {e}")
        sys.exit(1)


def extract_books(engine, cutoff_date):
    """
    Витягнути книги з таблиці books де last_updated >= cutoff_date
    
    Параметри:
    - engine: SQLAlchemy engine
    - cutoff_date: рядок в форматі 'YYYY-MM-DD'
    
    Поверніть: pandas DataFrame з колонками:
    book_id, title, price, genre, stock_quantity, last_updated
    
    Виведіть кількість знайдених записів
    """
    try:
        # SQL запит для витягуванння даних
        query = """
        SELECT book_id, title, price, genre, stock_quantity, last_updated
        FROM books
        WHERE last_updated >= :cutoff_date
        ORDER BY last_updated DESC
        """

        # Виконання запиту
        df = pd.read_sql_query(
            sql=text(query),
            con=engine,
            params={'cutoff_date': cutoff_date}
        )

        # Перевірка чи є дані
        if df.empty:
            print("Нових книг для обробки за вказану дату не знайдено. Роботу завершено.")
            sys.exit(0)

        print(f"Витягнуто {len(df)} записів з таблиці books")
        return df

    except SQLAlchemyError as e:
        print(f"Помилка SQL запиту при витягуванні даних: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Невідома помилка при витягуванні даних: {e}")
        sys.exit(1)


def transform_data(df):
    """
    Трансформувати дані згідно бізнес-правил:
    
    1. Створити колонку 'original_price' (копія 'price')
    2. Округлити 'price' до 1 знака після коми та зберегти як нову колонку з назвою 'rounded_price'
    3. Створити 'price_category':
       - 'budget' якщо rounded_price < 500
       - 'premium' якщо rounded_price >= 500
    4. Видалити оригінальну колонку 'price'
    
    Поверніть: трансформований DataFrame
    Виведіть кількість оброблених записів
    """
    try:
        # Створення копії для трансформації
        transformed_df = df.copy()

        # 1. Створення колонки 'original_price'(копія 'price')
        transformed_df['original_price'] = transformed_df['price']

        # 2. Округлення ціни до 1 знака після коми
        transformed_df['rounded_price'] = transformed_df['price'].round(1)

        # 3. Створення категорії ціни
        transformed_df['price_category'] = transformed_df['rounded_price'].apply(
            lambda x: 'budget' if x < 500 else 'premium'
        )

        # 4. Видалення оригінальної колонки 'price'
        transformed_df = transformed_df.drop('price', axis=1)

        # Видалення колонок stock_quantity та last_updated (не потрібні в processed таблиці)
        transformed_df = transformed_df.drop(['stock_quantity', 'last_updated'], axis=1)

        print(f"Трансформовано {len(transformed_df)} записів")
        return transformed_df

    except Exception as e:
        print(f"Помилка при трансформації даних: {e}")
        sys.exit(1)


def load_data(df, engine):
    """
    Зберегти оброблені дані в таблицю books_processed

    Використовуйте df.to_sql() з параметрами:
    - if_exists='append' (додавати до існуючих даних)
    - index=False (не зберігати індекс DataFrame)
    - chunksize=1000 (пакетна обробка)

    Виведіть кількість збережених записів
    Обробіть помилки збереження
    """
    try:
        # Збереження даних в таблицю books_processed
        df.to_sql(
            name='books_processed',
            con=engine,
            if_exists='append',
            index=False,
            chunksize=1000
        )

        print(f"Збережено {len(df)} записів в books_processed")

    except SQLAlchemyError as e:
        print(f"Помилка SQL при збереженні даних: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Невідома помилка при збереженні даних: {e}")
        sys.exit(1)


def validate_date_format(date_string):
    """
    Валідація формату дати YYYY-MM-DD
    """
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def main():
    """
    Головна функція:
    1. Перевірити аргументи командного рядка (має бути рівно 1 - дата)
    2. Валідувати формат дати (YYYY-MM-DD)
    3. Викликати всі ETL функції в правильному порядку
    4. Обробити помилки та вивести підсумкову статистику
    """
    # Перевірка аргументів командного рядка
    if len(sys.argv) != 2:
        print("Використання: python books_etl.py YYYY-MM-DD")
        print("Приклад: python books_etl.py 2025-01-01")
        sys.exit(1)

    cutoff_date = sys.argv[1]

    # Валідація формату дати
    if not validate_date_format(cutoff_date):
        print(f"Невірний формат дати: {cutoff_date}")
        print("Використовуйте формат YYYY-MM-DD")
        print("Приклад: 2025-01-01")
        sys.exit(1)

    try:
        print(f"Початок ETL процесу для дати: {cutoff_date}")

        # ETL Pipeline
        # 1. Connect
        engine = connect_to_db()

        # 2. Extract
        books_df = extract_books(engine, cutoff_date)

        # 3. Transform
        transformed_df = transform_data(books_df)

        # 4. Load
        load_data(transformed_df, engine)

        print("ETL процес завершено успішно")

    except Exception as e:
        print(f"Помилка в ETL процесі: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()