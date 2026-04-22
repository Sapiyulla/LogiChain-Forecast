#!/usr/bin/env python3
"""
Генератор синтетических данных для проекта LogiChain Forecast.
Генерирует сырые данные, имитирующие записи из OLTP/WMS системы.
Конфигурация задаётся в simulation.config.yaml.
"""

import yaml
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from uuid import uuid4
import sys
import os
from sqlalchemy import create_engine, text

random.seed(42)
np.random.seed(42)


class WarehouseDataGenerator:
    def __init__(self, config_path: str):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        self.sim = self.config['simulation']
        self.dirty = self.config['dirty_data']
        self.noise = self.config['noise']
        self.output = self.config['output']

        self.start_date = pd.to_datetime(self.sim['start_date'])
        self.end_date = pd.to_datetime(self.sim['end_date'])
        self.max_freq_minutes = self.sim['max_frequency_minutes']
        self.warehouses = self.sim['warehouses']

        self.holidays = self._generate_holidays()
        self.active_drifts = {}

        self.prev_occupancy: dict[int, float | None] = {wh['id']: None for wh in self.warehouses}
        self.prev_ts: dict[int, pd.Timestamp | None] = {wh['id']: None for wh in self.warehouses}

    def _generate_holidays(self):
        """Возвращает словарь: дата -> (is_holiday, event_type) для внутреннего использования."""
        holidays = {}
        years = range(self.start_date.year, self.end_date.year + 1)
        for year in years:
            # Новый год: 1-10 января
            for d in range(1, 11):
                try:
                    dt = datetime(year, 1, d).date()
                    holidays[dt] = (True, 'NY')
                except:
                    pass
            # 23 февраля
            dt = datetime(year, 2, 23).date()
            holidays[dt] = (True, 'Defender')
            # 8 марта
            dt = datetime(year, 3, 8).date()
            holidays[dt] = (True, 'WomensDay')
            # Майские: 1 и 9
            dt1 = datetime(year, 5, 1).date()
            dt9 = datetime(year, 5, 9).date()
            holidays[dt1] = (True, 'LabourDay')
            holidays[dt9] = (True, 'VictoryDay')
            # 4 ноября
            dt = datetime(year, 11, 4).date()
            holidays[dt] = (True, 'UnityDay')
            # Чёрная пятница - последняя пятница ноября
            nov = pd.date_range(start=f'{year}-11-01', end=f'{year}-11-30')
            fridays = [d for d in nov if d.weekday() == 4]
            if fridays:
                bf = fridays[-1].date()
                holidays[bf] = (True, 'BlackFriday')
        return holidays

    def _base_signal(self, ts: pd.Timestamp, wh: dict) -> float:
        """Базовое значение занятости без шума и грязи (внутреннее)."""
        # Тренд
        years_passed = (ts - self.start_date).days / 365.25
        growth_factor = 1 + wh['yearly_growth'] * years_passed
        base = wh['capacity'] * wh['base_occupancy'] * growth_factor

        # Внутринедельная сезонность
        dow = ts.weekday()
        if dow < 4:
            weekly_factor = 1.0 + 0.1 * np.sin(np.pi * dow / 4)
        elif dow == 4:
            weekly_factor = 0.9
        else:
            weekly_factor = 0.7

        # Месячная сезонность
        day_of_month = ts.day
        month_end = pd.Timestamp(year=ts.year, month=ts.month, day=1) + pd.offsets.MonthEnd(1)
        days_to_end = (month_end - ts).days
        if days_to_end < 3:
            monthly_factor = 0.8
        elif day_of_month <= 3:
            monthly_factor = 1.15
        else:
            monthly_factor = 1.0

        # Внутрисуточная
        hour = ts.hour
        if 0 <= hour < 8:
            hourly_factor = 0.85
        elif 8 <= hour < 20:
            hourly_factor = 1.1
        else:
            hourly_factor = 0.95

        # Праздники
        date = ts.date()
        holiday_factor = 1.0
        if date in self.holidays:
            _, event_type = self.holidays[date]
            if event_type in ('NY', 'LabourDay', 'VictoryDay'):
                holiday_factor = 0.5
            elif event_type == 'BlackFriday':
                holiday_factor = 1.4
            else:
                holiday_factor = 0.8

        value = base * weekly_factor * monthly_factor * hourly_factor * holiday_factor
        return min(value, wh['capacity'])

    def _add_noise(self, value: float) -> float:
        noise_std = self.noise['white_noise_std'] * value
        noise = np.random.normal(0, noise_std)
        return max(0, value + noise)

    def _generate_volumes_24h(self, prev_occ: float, curr_occ: float, ts: pd.Timestamp, wh: dict):
        """
        Генерирует объёмы приёмки и отгрузки за последние 24 часа.
        В реальной системе эти счётчики обновляются скользящим окном.
        """
        delta = curr_occ - prev_occ
        hour = ts.hour
        if 8 <= hour < 20:
            activity_scale = 1.0
        else:
            activity_scale = 0.2

        # Базовые объёмы за 24 часа зависят от ёмкости и загрузки
        base_turnover = wh['capacity'] * 0.05 * activity_scale  # 5% ёмкости в день

        # Учитываем изменение занятости
        if delta > 0:
            receiving = base_turnover + delta * (0.7 + 0.2 * random.random())
            shipping = base_turnover - delta * (0.3 + 0.2 * random.random())
        else:
            shipping = base_turnover + abs(delta) * (0.7 + 0.2 * random.random())
            receiving = base_turnover - abs(delta) * (0.3 + 0.2 * random.random())

        receiving = max(0, int(receiving))
        shipping = max(0, int(shipping))
        return receiving, shipping

    def _apply_dirty_flags(self, value: float, wh: dict) -> tuple[float | None, str]:
        """Применяет искажения к значению occupied_pallets и возвращает флаг качества."""
        if not self.dirty['enabled']:
            return value, 'OK'

        flags = []
        if value is not None and random.random() < self.dirty['null_probability']:
            return None, 'NULL'
        if value is not None and random.random() < self.dirty['negative_probability']:
            value = -abs(value) * random.uniform(0.1, 0.5)
            flags.append('NEGATIVE')
        if value is not None and random.random() < self.dirty['over_capacity_probability']:
            value = wh['capacity'] * (1 + random.uniform(0.05, 0.2))
            flags.append('OVER')

        flag = '|'.join(flags) if flags else 'OK'
        return value, flag

    def _next_timestamp(self, current_ts: pd.Timestamp, wh_id: int) -> pd.Timestamp:
        """
        Генерирует следующий timestamp со случайным интервалом от 1 секунды до max_frequency_minutes минут.
        Учитывает возможную задержку данных (lag).
        """
        # Случайный интервал в секундах: от 1 до max_freq_minutes * 60
        max_seconds = self.max_freq_minutes * 60
        seconds = random.randint(1, max_seconds)
        next_ts = current_ts + timedelta(seconds=seconds)

        # Эффект запаздывания (lag)
        prev = self.prev_ts.get(wh_id)
        if prev is not None and random.random() < self.dirty.get('lag_probability', 0):
            # Откатываем время назад или делаем минимальный сдвиг
            lag_seconds = random.randint(1, max(1, max_seconds // 2))
            next_ts = prev + timedelta(seconds=lag_seconds)

        if next_ts > self.end_date:
            next_ts = self.end_date
        return next_ts

    def generate(self) -> pd.DataFrame:
        """Основной цикл генерации данных с переменным шагом."""
        all_rows = []
        duplicate_candidates = []

        current_ts = {wh['id']: self.start_date for wh in self.warehouses}
        active = {wh['id']: True for wh in self.warehouses}

        total_expected = 0
        for wh in self.warehouses:
            total_seconds = (self.end_date - self.start_date).total_seconds()
            avg_interval = (1 + self.max_freq_minutes * 60) / 2
            total_expected += total_seconds / avg_interval
        print(f"Ожидаемое количество записей: ~{int(total_expected)}")
        print("Генерация данных...")

        while any(active.values()):
            for wh in self.warehouses:
                wh_id = wh['id']
                if not active[wh_id]:
                    continue

                ts = current_ts[wh_id]
                if ts >= self.end_date:
                    active[wh_id] = False
                    continue

                # Пропуск записи (gap)
                if random.random() < self.dirty.get('gap_probability', 0):
                    current_ts[wh_id] = self._next_timestamp(ts, wh_id)
                    continue

                # Генерируем базовое значение
                base_val = self._base_signal(ts, wh)

                # Применяем дрейф, если активен
                if wh_id in self.active_drifts:
                    drift_info = self.active_drifts[wh_id]
                    if ts <= drift_info['end_time']:
                        hours_passed = (ts - drift_info['start_time']).total_seconds() / 3600
                        base_val += drift_info['slope'] * hours_passed
                        base_val = max(0, base_val)
                    else:
                        del self.active_drifts[wh_id]

                # Шум
                noisy_val = self._add_noise(base_val)

                # Объёмы за 24 часа
                prev_val = self.prev_occupancy[wh_id]
                if prev_val is not None:
                    receiving, shipping = self._generate_volumes_24h(prev_val, noisy_val, ts, wh)
                else:
                    receiving, shipping = 0, 0

                # Применяем грязные флаги
                final_val, dq_flag = self._apply_dirty_flags(noisy_val, wh)

                # Создаём запись (только сырые поля)
                row = {
                    'timestamp': ts,
                    'warehouse_id': wh_id,
                    'occupied_pallets': final_val,
                    'receiving_volume_24h': receiving,
                    'shipping_volume_24h': shipping,
                    'wms_batch_id': str(uuid4())
                }

                all_rows.append(row)

                # Обновляем предыдущее состояние
                if final_val is not None:
                    self.prev_occupancy[wh_id] = final_val
                self.prev_ts[wh_id] = ts

                # Дубликат?
                if random.random() < self.dirty.get('duplicate_probability', 0):
                    dup = row.copy()
                    dup['wms_batch_id'] = str(uuid4())
                    if dup['occupied_pallets'] is not None:
                        dup['occupied_pallets'] *= random.uniform(0.98, 1.02)
                    duplicate_candidates.append(dup)

                # Активация дрейфа
                if random.random() < self.dirty.get('drift_probability', 0) and wh_id not in self.active_drifts:
                    drift_hours = random.randint(2, 8)
                    slope = base_val * random.uniform(-0.02, 0.02)
                    self.active_drifts[wh_id] = {
                        'start_time': ts,
                        'end_time': ts + timedelta(hours=drift_hours),
                        'slope': slope
                    }

                # Следующий timestamp
                current_ts[wh_id] = self._next_timestamp(ts, wh_id)

        # Добавляем дубликаты
        all_rows.extend(duplicate_candidates)

        df = pd.DataFrame(all_rows)
        df = df.sort_values(['timestamp', 'warehouse_id']).reset_index(drop=True)

        print(f"Сгенерировано записей: {len(df)}")
        return df

    def save(self, df: pd.DataFrame):
        """Сохраняет DataFrame в соответствии с output конфигом."""
        output_type = self.output.get('type', 'file')
        if output_type == 'file':
            path = self.output.get('path', 'raw_warehouse_occupancy.csv')
            df.to_csv(path, index=False, encoding='utf-8')
            print(f"Данные сохранены в CSV: {path}")
        elif output_type == 'database':
            uri = self.output.get('uri')
            if not uri:
                raise ValueError("Для типа 'database' необходимо указать 'uri' в конфиге.")
            engine = create_engine(uri)
            table_name = self.output.get('table', 'raw_warehouse_occupancy')
            schema_name = self.output.get('schema', 'public')

            with engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
                conn.commit()

            df.to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)
            print(f"Данные записаны в таблицу '{schema_name}.{table_name}'")
        else:
            raise ValueError(f"Неизвестный тип вывода: {output_type}")


def main():
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = 'simulation.config.yaml'

    if not os.path.exists(config_path):
        print(f"Конфигурационный файл {config_path} не найден.")
        sys.exit(1)

    generator = WarehouseDataGenerator(config_path)
    df = generator.generate()
    generator.save(df)


if __name__ == '__main__':
    main()