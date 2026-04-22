#!/usr/bin/env python3
"""
Скрипт для начального заполнения справочных таблиц в хранилище данных.
Использует simulation.config.yaml для параметров складов.
"""

import yaml
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import sys
import os

def seed_warehouses(engine, config):
    """Заполняет dim_warehouses на основе конфига."""
    warehouses = config['simulation']['warehouses']
    rows = []
    for wh in warehouses:
        rows.append({
            'warehouse_id': wh['id'],
            'warehouse_name': f"Склад #{wh['id']} ({wh['client_type']})",
            'total_capacity': wh['capacity'],
            'timezone': 'Europe/Moscow',
            'client_type': wh['client_type'],
            'is_active': True
        })
    df = pd.DataFrame(rows)
    df.to_sql('dim_warehouses', engine, schema='bronze', if_exists='append', index=False)
    print(f"Добавлено {len(df)} записей в bronze.dim_warehouses")

def seed_clients(engine, config):
    """Создаёт уникальных клиентов на основе client_type."""
    warehouses = config['simulation']['warehouses']
    client_types = set(wh['client_type'] for wh in warehouses)
    rows = []
    for idx, ctype in enumerate(client_types, 1):
        rows.append({
            'client_id': idx,
            'client_name': f"Клиент {ctype.capitalize()}",
            'industry': ctype,
            'contract_start': config['simulation']['start_date'],
            'contract_end': None
        })
    df = pd.DataFrame(rows)
    df.to_sql('dim_clients', engine, schema='bronze', if_exists='append', index=False)
    print(f"Добавлено {len(df)} записей в bronze.dim_clients")

def seed_warehouse_clients(engine, config):
    """Связывает склады с клиентами (многие ко многим)."""
    # Получаем client_id из только что созданной таблицы
    clients_df = pd.read_sql("SELECT client_id, industry FROM bronze.dim_clients", engine)
    client_map = dict(zip(clients_df['industry'], clients_df['client_id']))

    warehouses = config['simulation']['warehouses']
    rows = []
    for wh in warehouses:
        rows.append({
            'warehouse_id': wh['id'],
            'client_id': client_map[wh['client_type']],
            'valid_from': config['simulation']['start_date'],
            'valid_to': None
        })
    df = pd.DataFrame(rows)
    df.to_sql('ref_warehouse_clients', engine, schema='silver', if_exists='append', index=False)
    print(f"Добавлено {len(df)} записей в silver.ref_warehouse_clients")

def generate_calendar(start_date, end_date):
    """Генерирует DataFrame календаря с праздниками и событиями."""
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    df = pd.DataFrame({'date_id': dates})
    df['year'] = df['date_id'].dt.year
    df['month'] = df['date_id'].dt.month
    df['day'] = df['date_id'].dt.day
    df['day_of_week'] = df['date_id'].dt.weekday
    df['is_weekend'] = df['day_of_week'].isin([5, 6])

    # Праздники
    holidays = []
    for year in df['year'].unique():
        for d in range(1, 11):
            holidays.append((datetime(year, 1, d), 'NY', 'New Year'))
        holidays.append((datetime(year, 2, 23), 'Defender', 'Defender of Fatherland'))
        holidays.append((datetime(year, 3, 8), 'WomensDay', 'Womens Day'))
        holidays.append((datetime(year, 5, 1), 'LabourDay', 'Labour Day'))
        holidays.append((datetime(year, 5, 9), 'VictoryDay', 'Victory Day'))
        holidays.append((datetime(year, 11, 4), 'UnityDay', 'Unity Day'))
        # Чёрная пятница
        nov = pd.date_range(start=f'{year}-11-01', end=f'{year}-11-30')
        fridays = nov[nov.weekday == 4]
        if len(fridays) > 0:
            bf = fridays[-1]
            holidays.append((bf, 'BlackFriday', 'Black Friday'))

    holiday_df = pd.DataFrame(holidays, columns=['date_id', 'event_type', 'holiday_name'])
    holiday_df['is_holiday'] = True

    # Объединяем по date_id (уже datetime)
    df = df.merge(holiday_df, on='date_id', how='left')
    df['is_holiday'] = df['is_holiday'].fillna(False)
    df['holiday_name'] = df['holiday_name'].fillna('')
    df['event_type'] = df['event_type'].fillna('')

    # Дни до Чёрной пятницы
    df['days_to_black_friday'] = pd.NA
    bf_dates = df[df['event_type'] == 'BlackFriday']['date_id'].tolist()
    for bf in bf_dates:
        mask = df['date_id'] <= bf
        diff = (bf - df.loc[mask, 'date_id']).dt.days
        df.loc[mask, 'days_to_black_friday'] = diff

    # Приводим date_id к date для совместимости с SQL
    df['date_id'] = df['date_id'].dt.date
    return df

def seed_calendar(engine, config):
    """Заполняет silver.dim_calendar."""
    start = pd.to_datetime(config['simulation']['start_date']).date()
    end = pd.to_datetime(config['simulation']['end_date']).date()
    df = generate_calendar(start, end)
    df.to_sql('dim_calendar', engine, schema='silver', if_exists='append', index=False)
    print(f"Добавлено {len(df)} записей в silver.dim_calendar")

def main():
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = 'simulation.config.yaml'

    if not os.path.exists(config_path):
        print(f"Конфигурационный файл {config_path} не найден.")
        sys.exit(1)

    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    output = config['output']
    if output.get('type') != 'database':
        print("Этот скрипт работает только с типом вывода 'database'.")
        sys.exit(1)

    uri = output.get('uri')
    if not uri:
        print("Не указан uri в конфиге.")
        sys.exit(1)

    engine = create_engine(uri)

    # Очищаем таблицы перед заполнением (опционально)
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE bronze.dim_warehouses CASCADE"))
        conn.execute(text("TRUNCATE bronze.dim_clients CASCADE"))
        conn.execute(text("TRUNCATE silver.dim_calendar CASCADE"))
        conn.commit()

    seed_warehouses(engine, config)
    seed_clients(engine, config)
    seed_warehouse_clients(engine, config)
    seed_calendar(engine, config)

    print("Все справочники успешно заполнены.")

if __name__ == '__main__':
    main()