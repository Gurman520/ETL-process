import pandas as pd
import numpy as np
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_invalid_dates(df, date_columns):
    """
    Обрабатывает некорректные даты:
    1. Преобразует строки в datetime.
    2. Исправляет даты с годом < 1900 или > текущего года.
    3. Заменяет даты-заглушки (30.12.1899) на NaT.
    """
    current_year = datetime.now().year
    
    for col in date_columns:
        # Преобразование в datetime с учетом формата ДД.ММ.ГГГГ
        df[col] = pd.to_datetime(df[col], format='%d.%m.%Y', errors='coerce')
        
        # Замена дат с некорректным годом
        mask = (df[col].dt.year < 1900) | (df[col].dt.year > current_year)
        df[col] = df[col].mask(mask, pd.NaT)
        
        # Удаление даты-заглушки 30.12.1899
        df[col] = df[col].replace(pd.Timestamp('1899-12-30'), pd.NaT)
    
    return df

# Пример использования для диагнозов
def process_diagnoses_data(df):
    date_columns = ['reg_dat', 'confirm_dat', 'end_dat']
    return fix_invalid_dates(df, date_columns)




def process_patient_data(df):
    """
    Обрабатывает данные пациентов:
    1. Преобразует даты в формат datetime.
    2. Очищает некорректные даты рождения (больше текущей даты).
    3. Вычисляет возраст пациента (целое число).
    """
    try:
        # Преобразуем даты
        df['birthdate'] = pd.to_datetime(df['birthdate'], errors='coerce')
        df['death_dat'] = pd.to_datetime(df['death_dat'], errors='coerce')

        # Очистка некорректных дат рождения
        current_date = datetime.now()
        invalid_birthdates = df['birthdate'] > current_date
        if invalid_birthdates.any():
            logger.warning(f"Найдено {invalid_birthdates.sum()} строк с датой рождения больше текущей даты. Они будут очищены.")
            df.loc[invalid_birthdates, 'birthdate'] = pd.NaT

        # Точный расчет возраста (в годах)
        def calculate_age(row):
            if pd.isnull(row['birthdate']):  # Если дата рождения отсутствует
                return None
            end_date = row['death_dat'] if pd.notnull(row['death_dat']) else current_date
            return end_date.year - row['birthdate'].year - (
                (end_date.month, end_date.day) < (row['birthdate'].month, row['birthdate'].day)
            )

        # Применяем функцию расчета возраста
        df['age'] = df.apply(calculate_age, axis=1)
        
        # Приводим возраст к целому числу (если значение не NaN)
        df['age'] = df['age'].astype(pd.Int64Dtype())  # Поддержка целых чисел с NaN

        return df

    except Exception as e:
        logger.error(f"Ошибка при обработке данных пациентов: {e}")
        raise


def process_visits_data(df_visits, df_patients):
    """
    Обрабатывает данные о посещениях:
    1. Проверяет корректность даты посещения.
    2. Удаляет строки с некорректными датами.
    3. Проверяет, что у каждого посещения есть корректный patientid.
    4. Удаляет строки, где patientid отсутствует или не соответствует пациентам.
    5. Удаляет строки с дубликатами keyid.
    """
    try:
        # Проверка на пустые данные
        if df_visits.empty or df_patients.empty:
            logger.warning("Один из DataFrame пуст. Проверка невозможна.")
            return df_visits

        # Удаление строк с дубликатами keyid
        df_visits = df_visits.drop_duplicates(subset=['keyid'], keep='first')
        logger.info(f"Удалено {len(df_visits) - len(df_visits.drop_duplicates(subset=['keyid']))} дубликатов keyid.")

        # Преобразуем дату посещения в datetime
        df_visits['dat'] = pd.to_datetime(df_visits['dat'], errors='coerce')

        # Очистка некорректных дат (заглушки или будущие даты)
        current_date = datetime.now()
        invalid_dates = (df_visits['dat'] > current_date) | (df_visits['dat'].isna())
        if invalid_dates.any():
            logger.warning(f"Найдено {invalid_dates.sum()} строк с некорректной датой посещения. Они будут удалены.")
            df_visits = df_visits[~invalid_dates]

        # Проверка, что patientid не пустой
        missing_patientid = df_visits['patientid'].isna()
        if missing_patientid.any():
            logger.warning(f"Найдено {missing_patientid.sum()} строк с отсутствующим patientid. Они будут удалены.")
            df_visits = df_visits[~missing_patientid]

        # Проверка, что patientid существует в таблице пациентов
        valid_patient_ids = df_patients['keyid'].unique()
        invalid_patients = ~df_visits['patientid'].isin(valid_patient_ids)
        if invalid_patients.any():
            logger.warning(f"Найдено {invalid_patients.sum()} строк с некорректным patientid. Они будут удалены.")
            df_visits = df_visits[~invalid_patients]

        # Преобразуем doctorid в int
        df_visits['doctorid'] = df_visits['doctorid'].astype(pd.Int64Dtype())

        return df_visits

    except Exception as e:
        logger.error(f"Ошибка при обработке данных о посещениях: {e}")
        raise


def process_diagnoses_data(df_diagnoses, df_patients):
    """
    Обрабатывает данные о диагнозах:
    1. Преобразует даты (reg_dat, confirm_dat, end_dat) в datetime.
    2. Очищает даты-заглушки (заменяет на NaT).
    3. Удаляет строки с будущими датами.
    4. Проверяет, что patient_id существует в таблице пациентов.
    5. Удаляет строки с некорректным или отсутствующим patient_id.
    6. Преобразует столбцы reg_by, confirm_by, end_by в тип int.
    """
    try:
        # Проверка на пустые данные
        if df_diagnoses.empty or df_patients.empty:
            logger.warning("Один из DataFrame пуст. Проверка невозможна.")
            return df_diagnoses

        # Преобразуем даты в datetime
        date_columns = ['reg_dat', 'confirm_dat', 'end_dat']
        for col in date_columns:
            df_diagnoses[col] = pd.to_datetime(df_diagnoses[col], format='%d.%m.%Y', errors='coerce')

        # Очистка дат-заглушек (например, 30.12.1899)
        date_columns = ['reg_dat', 'confirm_dat', 'end_dat']
        for col in date_columns:
            df_diagnoses[col] = df_diagnoses[col].replace(pd.Timestamp('1899-12-30'), pd.NaT)
            df_diagnoses[col] = df_diagnoses[col].replace(pd.Timestamp('1899-12-25'), pd.NaT)
            df_diagnoses[col] = df_diagnoses[col].replace(pd.Timestamp('1899-12-26'), pd.NaT)

        # Удаляем строки с будущими датами
        current_date = datetime.now()
        future_dates = (
            (df_diagnoses['reg_dat'] > current_date) |
            (df_diagnoses['confirm_dat'] > current_date) |
            (df_diagnoses['end_dat'] > current_date)
        )
        if future_dates.any():
            logger.warning(f"Найдено {future_dates.sum()} строк с будущими датами. Они будут удалены.")
            df_diagnoses = df_diagnoses[~future_dates]

        # Проверка, что patient_id не пустой
        missing_patientid = df_diagnoses['patient_id'].isna()
        if missing_patientid.any():
            logger.warning(f"Найдено {missing_patientid.sum()} строк с отсутствующим patient_id. Они будут удалены.")
            df_diagnoses = df_diagnoses[~missing_patientid]

        # Проверка, что patient_id существует в таблице пациентов
        valid_patient_ids = df_patients['keyid'].unique()
        invalid_patients = ~df_diagnoses['patient_id'].isin(valid_patient_ids)
        if invalid_patients.any():
            logger.warning(f"Найдено {invalid_patients.sum()} строк с некорректным patient_id. Они будут удалены.")
            df_diagnoses = df_diagnoses[~invalid_patients]

        # Преобразуем столбцы в int
        int_columns = ['reg_by', 'confirm_by', 'end_by']
        for col in int_columns:
            df_diagnoses[col] = pd.to_numeric(df_diagnoses[col], errors='coerce').astype(pd.Int64Dtype())
            invalid_count = df_diagnoses[col].isna().sum()
            if invalid_count > 0:
                logger.warning(f"Найдено {invalid_count} некорректных значений в столбце '{col}'. Они заменены на NaN.")

        return df_diagnoses

    except Exception as e:
        logger.error(f"Ошибка при обработке данных о диагнозах: {e}")
        raise


def process_hospital_visits(df_visits, df_patients):
    """
    Обрабатывает данные о стационарных визитах:
    1. Проверяет корректность дат (dat, dat1).
    2. Очищает некорректные даты (заглушки, кривые значения).
    3. Удаляет строки с будущими датами.
    4. Проверяет, что patientid существует в таблице пациентов.
    5. Удаляет строки с некорректным или отсутствующим patientid.
    """
    try:
        # Проверка на пустые данные
        if df_visits.empty or df_patients.empty:
            logger.warning("Один из DataFrame пуст. Проверка невозможна.")
            return df_visits

        # Преобразуем даты в datetime
        date_columns = ['dat', 'dat1']
        for col in date_columns:
            df_visits[col] = pd.to_datetime(df_visits[col], errors='coerce')

        # Удаляем строки с будущими датами
        current_date = datetime.now()
        future_dates = (
            (df_visits['dat'] > current_date) |
            (df_visits['dat1'] > current_date)
        )
        if future_dates.any():
            logger.warning(f"Найдено {future_dates.sum()} строк с будущими датами. Они будут удалены.")
            df_visits = df_visits[~future_dates]

        # Проверка, что patientid не пустой
        missing_patientid = df_visits['patientid'].isna()
        if missing_patientid.any():
            logger.warning(f"Найдено {missing_patientid.sum()} строк с отсутствующим patientid. Они будут удалены.")
            df_visits = df_visits[~missing_patientid]

        # Проверка, что patientid существует в таблице пациентов
        valid_patient_ids = df_patients['keyid'].unique()
        invalid_patients = ~df_visits['patientid'].isin(valid_patient_ids)
        if invalid_patients.any():
            logger.warning(f"Найдено {invalid_patients.sum()} строк с некорректным patientid. Они будут удалены.")
            df_visits = df_visits[~invalid_patients]

        return df_visits

    except Exception as e:
        logger.error(f"Ошибка при обработке данных о стационарных визитах: {e}")
        raise


def process_doc_data(df):
    """
    Преобразует последний столбец DataFrame в тип int.
    Некорректные значения заменяются на NaN.
    """
    try:
        # Получаем имя последнего столбца
        last_column = df.columns[-1]
        
        # Преобразуем столбец в int, заменяя некорректные значения на NaN
        df[last_column] = pd.to_numeric(df[last_column], errors='coerce').astype(pd.StringDtype())
        
        # Логируем количество некорректных значений
        invalid_count = df[last_column].isna().sum()
        if invalid_count > 0:
            logger.warning(f"Найдено {invalid_count} некорректных значений в столбце '{last_column}'. Они заменены на NaN.")
        
        return df

    except Exception as e:
        logger.error(f"Ошибка при обработке данных: {e}")
        raise


def process_po_visit_data(df_visits, df_patients):
    """
    Обрабатывает данные о визитах:
    1. Удаляет строки без ссылки на пациента или с несуществующими пациентами
    2. Очищает даты от заглушек (заменяет на NaT)
    3. Удаляет строки с будущими датами
    4. Проверяет корректность интервала дат (DAT_ST <= DAT_FIN)
    
    Параметры:
        df_visits (pd.DataFrame): Данные о визитах
        df_patients (pd.DataFrame): Данные о пациентах (должен содержать столбец 'keyid')
    
    Возвращает:
        pd.DataFrame: Очищенный DataFrame с визитами
    """
    try:
        # Проверка на пустые данные
        if df_visits.empty or df_patients.empty:
            logger.warning("Один из DataFrame пуст. Обработка невозможна.")
            return df_visits

        # Сохраняем исходное количество строк для логов
        initial_count = len(df_visits)

        # 1. Удаление строк без ссылки на пациента
        df_visits = df_visits[df_visits['pat'].notna()]
        removed_missing_pat = initial_count - len(df_visits)
        if removed_missing_pat > 0:
            logger.warning(f"Удалено {removed_missing_pat} строк без ссылки на пациента.")

        # 2. Удаление строк с несуществующими пациентами
        valid_patient_ids = df_patients['keyid'].unique()
        before_pat_check = len(df_visits)
        df_visits = df_visits[df_visits['pat'].isin(valid_patient_ids)]
        removed_invalid_pat = before_pat_check - len(df_visits)
        if removed_invalid_pat > 0:
            logger.warning(f"Удалено {removed_invalid_pat} строк с несуществующими пациентами.")

        # 3. Преобразование дат в datetime
        date_columns = ['dat_st', 'dat_fin']
        for col in date_columns:
            df_visits[col] = pd.to_datetime(df_visits[col], errors='coerce')

        # 4. Очистка дат-заглушек (например, 30.12.1899)
        df_visits[date_columns] = df_visits[date_columns].replace(pd.Timestamp('1899-12-30'), pd.NaT)

        # 5. Удаление строк с будущими датами
        current_date = datetime.now()
        future_dates_mask = (df_visits['dat_st'] > current_date) | (df_visits['dat_fin'] > current_date)
        if future_dates_mask.any():
            logger.warning(f"Найдено {future_dates_mask.sum()} строк с будущими датами. Они будут удалены.")
            df_visits = df_visits[~future_dates_mask]

        # 6. Проверка корректности интервала дат (DAT_ST <= DAT_FIN)
        valid_dates_mask = df_visits[date_columns].notna().all(axis=1)
        invalid_interval_mask = (df_visits['dat_st'] > df_visits['dat_fin']) & valid_dates_mask
        if invalid_interval_mask.any():
            logger.warning(f"Найдено {invalid_interval_mask.sum()} строк с некорректным интервалом дат (DAT_ST > DAT_FIN). Они будут удалены.")
            df_visits = df_visits[~invalid_interval_mask]

        # 7. Удаление строк, где обе даты NaT (полностью некорректные записи)
        both_dates_invalid = df_visits[date_columns].isna().all(axis=1)
        if both_dates_invalid.any():
            logger.warning(f"Удалено {both_dates_invalid.sum()} строк с полностью некорректными датами.")
            df_visits = df_visits[~both_dates_invalid]

        logger.info(f"После обработки осталось {len(df_visits)} корректных записей из {initial_count}.")
        return df_visits

    except Exception as e:
        logger.error(f"Ошибка при обработке данных: {str(e)}")
        raise

