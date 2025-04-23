import pandas as pd
from sqlalchemy import create_engine
import logging

def load_data(df, table_name, target_conn_string, chunksize=10000, if_exists='replace'):
    """
    Загружает данные в SQL Server 2022.
    :param df: DataFrame с данными.
    :param table_name: Имя целевой таблицы.
    :param target_conn_string: Строка подключения к SQL Server.
    :param chunksize: Количество строк для загрузки за один запрос.
    :param if_exists: 'replace' - перезаписать таблицу, 'append' - добавить данные.
    """
    try:
        # Создаем подключение к SQL Server
        engine = create_engine(target_conn_string)
        logging.info("Подключение к SQL Server установлено.")

        # Загружаем данные порциями
        df.to_sql(table_name, engine, if_exists=if_exists, index=False, chunksize=chunksize)
        logging.info(f"Данные успешно загружены в таблицу {table_name}.")

    except Exception as e:
        logging.error(f"Ошибка при загрузке данных: {e}")
        raise


def save_to_csv(df, file_path, index=False):
    """
    Сохраняет DataFrame в CSV-файл.
    :param df: DataFrame с данными.
    :param file_path: Путь к файлу (например, 'output.csv').
    :param index: Сохранять ли индекс (по умолчанию False).
    """
    try:
        # Сохранение данных в CSV
        df.to_csv(file_path, index=index, encoding='utf-8-sig')  # encoding для поддержки кириллицы
        print(f"Данные успешно сохранены в файл: {file_path}")
    except Exception as e:
        print(f"Ошибка при сохранении данных в CSV: {e}")
        raise
