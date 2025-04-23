from sqlalchemy import create_engine
import logging
import pandas as pd

# Слой извлечения данных
def extract_data(query, source_conn_string, batch_size=100000):
    """
    Извлекает данные из PostgreSQL.
    :param query: SQL-запрос для извлечения данных.
    :param source_conn_string: Строка подключения к PostgreSQL.
    :return: DataFrame с данными.
    """
    try:
        engine = create_engine(source_conn_string)
        logging.info("Подключение к PostgreSQL установлено.")

        # Читаем данные чанками и собираем в список
        chunks = []
        for chunk in pd.read_sql(query, engine, chunksize=batch_size):
            chunks.append(chunk)
            logging.info(f"Выгружено {len(chunk)} строк (всего: {sum(len(c) for c in chunks)})")

        # Объединяем все чанки в один DataFrame
        full_df = pd.concat(chunks, ignore_index=True)
        logging.info(f"Итоговый размер данных: {len(full_df)} строк.")
        
        return full_df

    except Exception as e:
        logging.error(f"Ошибка при извлечении данных: {e}")
        raise