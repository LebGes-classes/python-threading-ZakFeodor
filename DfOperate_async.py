import pandas as pd
import numpy as np
from datetime import datetime
import asyncio


class DfOperate:
    """Класс для обработки и анализа данных об оборудовании (DataFrame)."""

    def __init__(self, df: pd.DataFrame) -> None:
        """Инициализация класса и предварительная обработка дат.

        Копирует исходный DataFrame и преобразует столбцы с датами
        в стандартный формат datetime для дальнейшей работы.

        Args:
            df (pd.DataFrame): Исходный DataFrame с данными.
        """

        self.df = df.copy()
        self.df['warranty_until'] = pd.to_datetime(self.df['warranty_until'], format='mixed')
        self.df['last_calibration_date'] = pd.to_datetime(self.df['last_calibration_date'], format='mixed')
        self.df['install_date'] = pd.to_datetime(self.df['install_date'], format='mixed')

    async def get_tables_by_warranty_status(self) -> dict:
        """Группирует данные по статусу гарантии оборудования.

        Разделяет оборудование на категории в зависимости от того,
        сколько времени осталось до истечения гарантии относительно текущей даты.

        Returns:
            dict: Словарь, где ключи это текстовые статусы гарантии, а значения это соответствующие им DataFrame.
        """

        df_res = self.df.copy()
        today = pd.Timestamp.today()

        conditions = [
            df_res['warranty_until'] < today,
            df_res['warranty_until'] < (today + pd.DateOffset(months=1)),
            df_res['warranty_until'] < (today + pd.DateOffset(months=6)),
            df_res['warranty_until'] > (today + pd.DateOffset(months=6))
        ]
        choices = [
            'Гарантия истекла',
            'Истечет менее чем через месяц',
            'Истечет менее чем через полгода',
            'Истечет более чем через полгода'
        ]
        df_res['warranty_status'] = np.select(conditions, choices, default='Нет данных')

        warranty_dict = {}

        for status_name, group_df in df_res.groupby('warranty_status'):
            warranty_dict[status_name] = group_df.reset_index(drop=True)

        return warranty_dict

    async def find_problem_clinics(self) -> pd.DataFrame:
        """Определяет самые проблемные клиники по количеству поломок.

        Суммирует заявленные проблемы и поломки за последние 12 месяцев
        по каждой клинике и сортирует список по убыванию проблем.

        Returns:
            pd.DataFrame: DataFrame с названиями клиник и общим количеством проблем.
        """

        self.df['total_problems'] = self.df['issues_reported_12mo'].fillna(0) + self.df['failure_count_12mo'].fillna(0)
        clinics_problems = self.df.groupby('clinic_name')['total_problems'].sum().sort_values(ascending=False)

        clinics_problems = clinics_problems.reset_index()

        return clinics_problems

    async def get_calibration_statuses(self) -> pd.DataFrame:
        """Определяет статус калибровки оборудования.

        Проверяет наличие даты калибровки, корректность дат и срок давности калибровки.

        Returns:
            pd.DataFrame: Таблица со столбцами clinic_name, device_id и calibration_status.
        """

        df_res = self.df.copy()
        today = pd.Timestamp.today()

        conditions = [df_res['last_calibration_date'].isna(),
                      df_res['last_calibration_date'] < df_res['install_date'],
                      df_res['last_calibration_date'] < (today + pd.DateOffset(years=1))
        ]
        choices = [
            'Нет данных',
            'Неправильная дата калибровки',
            'Требуется калибровка'
        ]

        df_res['calibration_status'] = np.select(conditions, choices, default='Калибровка в норме')

        return df_res[['clinic_name', 'device_id', 'calibration_status']]

    async def create_pivot_table(self) -> pd.DataFrame:
        """Создает сводную таблицу по моделям оборудования.

        Подсчитывает количество каждой модели оборудования в разрезе
        конкретных клиник и городов.

        Returns:
            pd.DataFrame: Сводная таблица с агрегированными данными.
        """

        pivot_df = pd.pivot_table(
            self.df,
            values='device_id',
            index=['clinic_name', 'city'],
            columns='model',
            aggfunc='count',
            fill_value=0
        ).reset_index()

        return pivot_df
