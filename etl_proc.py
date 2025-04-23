import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
import logging
import sql_scripts as sql
from datetime import datetime
from extract import extract_data
import transform as tr
import load as l

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Конфигурация подключений
POSTGRES_CONN_STRING = 'postgresql+psycopg2://login:pass@10.1.1.37:5432/med'
SQLSERVER_CONN_STRING = 'mssql+pyodbc://user:password@:1433/mydatabase?driver=ODBC+Driver+17+for+SQL+Server'
# SQLSERVER_CONN_STRING = (
#     "mssql+pyodbc://login:pass@localhost:1433/MedData"
#     "driver=ODBC+Driver+17+for+SQL+Server"
# )

# Основной ETL-процесс
def etl_process():
    """
    Основной ETL-процесс:
    1. Извлечение данных из PostgreSQL.
    2. Преобразование данных.
    3. Выгрузка данных в в csv файлах 
    4. Загрузка данных в SQL Server. (В разработке)
    """
    
    # Извлечение данных
    try:
        # Шаг 1: Извлечение данных
        raw_data_patient = extract_data(sql.query_get_patient, POSTGRES_CONN_STRING)
        raw_data_stac_visit = extract_data(sql.stac_query_get_visit, POSTGRES_CONN_STRING)
        raw_data_amb_visit = extract_data(sql.amb_query_get_visit, POSTGRES_CONN_STRING)
        raw_data_dol = extract_data(sql.query_get_dolznost, POSTGRES_CONN_STRING)
        raw_data_doc = extract_data(sql.query_get_doctor, POSTGRES_CONN_STRING)
        raw_data_diap = extract_data(sql.amb_query_get_diagn_pat, POSTGRES_CONN_STRING)
        raw_data_po = extract_data(sql.stac_query_get_PO_visit , POSTGRES_CONN_STRING)
        logging.info("Выгрузка данных успешно завершена.")
    except Exception as e:
        logging.error(f"Ошибка в ETL-процессе: {e}")

    # Преобразование
    try:
        new_data_doc = tr.process_doc_data(raw_data_doc)
        new_data_patient = tr.process_patient_data(raw_data_patient)
        new_data_stac_visit = tr.process_hospital_visits(raw_data_stac_visit, new_data_patient)
        new_data_diap = tr.process_diagnoses_data(raw_data_diap, new_data_patient)
        new_data_visit = tr.process_visits_data(raw_data_amb_visit, new_data_patient)
        new_data_po = tr.process_po_visit_data(raw_data_po, new_data_patient)

    except Exception as e:
        logging.error(f"Ошибка в ETL-процессе - преобразование: {e}")

    # Выгрузка в csv
    try:
        l.save_to_csv(new_data_patient, 'Patient_f.csv')
        l.save_to_csv(new_data_visit, "AMB visit.csv")
        l.save_to_csv(raw_data_dol, "dols.csv")
        l.save_to_csv(raw_data_doc, "doc.csv")
        l.save_to_csv(new_data_diap, "AMB diap.csv")
        l.save_to_csv(new_data_stac_visit, "STAC visit.csv")
        l.save_to_csv(new_data_doc, "doc.csv")
        l.save_to_csv(new_data_po, "po_visit.csv")
    except Exception as e:
        logging.error(f"Ошибка в ETL-процессе - csv: {e}")
        
        logging.info("ETL-процесс успешно завершен.")

# Запуск ETL-процесса
if __name__ == "__main__":
    etl_process()