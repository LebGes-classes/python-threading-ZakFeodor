import pandas as pd
from DfOperate_threads import DfOperate
import time
import threading
from queue import Queue


def excel_processing(file_name):
    """Считывание файл Excel, обработка его и возврат результирующей таблицы

    Args:
        file_name: имя файла для считывания
    """
    
    try:
        df = pd.read_excel(file_name)
    except Exception as e:

        print(f'Ошибка при считывании файла {file_name}: {e}')

    status_mapping = {
        'OK': 'operational',
        'op': 'operational',
        'broken': 'faulty',
        'planned_installation': 'planned_installation',
        'maintenance_scheduled': 'maintenance_scheduled',
        'operational': 'operational',
        'faulty': 'faulty'
    }

    df['status'] = df['status'].astype(str).str.lower().str.strip()
    df['status'] = df['status'].map(status_mapping).fillna(df['status'])

    data = DfOperate(df)
    q_warranty = Queue()
    q_problems = Queue()
    q_calibration = Queue()
    q_pivot = Queue()

    t1 = threading.Thread(target=data.get_tables_by_warranty_status, args=(q_warranty,))
    t2 = threading.Thread(target=data.find_problem_clinics, args=(q_problems,))
    t3 = threading.Thread(target=data.get_calibration_statuses, args=(q_calibration,))
    t4 = threading.Thread(target=data.create_pivot_table, args=(q_pivot,))

    t1.start()
    t2.start()
    t3.start()
    t4.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()

    table_filter_warranty = q_warranty.get()
    problem_clinics = q_problems.get()
    calibration_statuses = q_calibration.get()
    pivot_table_clinics = q_pivot.get()
    with pd.ExcelWriter(f'{file_name[:-5]}_solution.xlsx', engine='openpyxl') as writer:
        table_filter_warranty['Гарантия истекла'].to_excel(writer, sheet_name='Гарантия истекла', index=False)
        table_filter_warranty['Истечет менее чем через месяц'].to_excel(writer,
                                                                        sheet_name='Гарантия менее месяца',
                                                                        index=False)
        table_filter_warranty['Истечет менее чем через полгода'].to_excel(writer,
                                                                          sheet_name='Гарантия менее полугода',
                                                                          index=False)
        table_filter_warranty['Истечет более чем через полгода'].to_excel(writer,
                                                                          sheet_name='Гарантия более полугода',
                                                                          index=False)
        problem_clinics.to_excel(writer, sheet_name='Наиболее проблемные клиники', index=False)
        calibration_statuses.to_excel(writer, sheet_name='Статусы калибровки', index=False)
        pivot_table_clinics.to_excel(writer, sheet_name='Сводная таблица по клиникам', index=False)


def main():
    """Функция выполнения задач с синхронной записью

    Returns:
        float: Время работы многопоточно выполняемого кода
    """

    start_time = time.time()
    t1 = threading.Thread(target=excel_processing, args=('medical_diagnostic_devices_1.xlsx',))
    t2 = threading.Thread(target=excel_processing, args=('medical_diagnostic_devices_2.xlsx',))
    t3 = threading.Thread(target=excel_processing, args=('medical_diagnostic_devices_3.xlsx',))
    t4 = threading.Thread(target=excel_processing, args=('medical_diagnostic_devices_4.xlsx',))
    t5 = threading.Thread(target=excel_processing, args=('medical_diagnostic_devices_5.xlsx',))
    t6 = threading.Thread(target=excel_processing, args=('medical_diagnostic_devices_6.xlsx',))
    t7 = threading.Thread(target=excel_processing, args=('medical_diagnostic_devices_7.xlsx',))
    t8 = threading.Thread(target=excel_processing, args=('medical_diagnostic_devices_8.xlsx',))
    t9 = threading.Thread(target=excel_processing, args=('medical_diagnostic_devices_9.xlsx',))
    t10 = threading.Thread(target=excel_processing, args=('medical_diagnostic_devices_10.xlsx',))

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()
    t8.start()
    t9.start()
    t10.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    t6.join()
    t7.join()
    t8.join()
    t9.join()
    t10.join()

    return f'Время работы многопоточно выполняемого кода = {time.time()-start_time}'


if __name__ == '__main__':
    print(main())
