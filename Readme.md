# LogiChain Forecast

> К сожалению проект не доведен до бета-версии. Автор активно работает над курсовой работой (в рамках университетской дисциплины) в сфере DE.
> Активные проекты можно рассмотреть в [Sapiyulla`s repositories](https://github.com/Sapiyulla?tab=repositories) по последним коммитам.

[![Python](https://img.shields.io/badge/Python-3.14-blue?logo=python)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-3.2.0-017CEE?logo=apacheairflow)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.11.7-FF694B?logo=dbt)](https://www.getdbt.com/)
[![LightGBM](https://img.shields.io/badge/LightGBM-4.3-yellow?logo=lightgbm)](https://lightgbm.readthedocs.io/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.28-FF4B4B?logo=streamlit)](https://streamlit.io/)
[![Docker](https://img.shields.io/badge/Docker-28.3.2-2496ED?logo=docker)](https://www.docker.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14-4169E1?logo=postgresql)](https://www.postgresql.org/)

**Программное средство для прогнозирования загрузки складских площадей (3PL-логистика)**

Проект демонстрирует полный цикл Data Engineering: от генерации синтетических данных до построения прогнозной модели и визуализации результатов. Реализован с использованием Modern Data Stack.

---

## 📌 Особенности

- **Полноценный ETL-пайплайн** на Apache Airflow с еженедельным обновлением данных.
- **Трансформация данных через dbt** с документированием и версионированием SQL‑логики.
- **Прогнозирование временного ряда** на основе LightGBM с инжинирингом признаков.
- **Интерактивный дашборд** на Streamlit с графиками и метриками качества.
- **Автоматическая генерация PDF‑отчётов** для менеджмента.
- **Полная контейнеризация** — запуск одной командой `docker compose up`.

---

## 🧱 Архитектура

![arch](./Documents/LogiChain%20Forecast%20[arch.].png)

---

## 📁 Структура репозитория

```
logichain-forecast/
├── airflow/                    # Airflow DAGs и конфигурация
│   ├── dags/
│   │   └── warehouse_forecast_dag.py
│   └── Dockerfile
├── dbt_project/                # dbt‑проект
│   ├── models/
│   │   ├── staging/
│   │   ├── analytics/
│   │   └── schema.yml
│   ├── dbt_project.yml
│   └── profiles.yml
├── generator/                  # Генерация синтетики
│   ├── generate_historical.py
│   └── generate_weekly.py
├── ml/                         # ML‑модуль
│   ├── train_model.py
│   └── predict.py
├── dashboard/                  # Streamlit + PDF
│   ├── app.py
│   └── report_generator.py
├── docker-compose.yml
├── .env.example
├── pyproject.toml
└── README.md
```

---

## ⚙️ Стек технологий

| Компонент | Инструменты |
|-----------|-------------|
| **Язык** | Python 3.14+ |
| **Оркестрация** | Apache Airflow |
| **Трансформации** | dbt-core 1.7 + dbt-postgres |
| **Хранилище** | PostgreSQL 14 |
| **ML** | LightGBM, Scikit-learn |
| **Визуализация** | Streamlit, Plotly |
| **Отчёты** | ReportLab |
| **Инфраструктура** | Docker, Docker Compose |

---

## 🚀 Быстрый старт

### Предварительные требования

- Docker 28.3.2+
- Docker Compose v2.38.2-desktop.1+
- Python 3.14

### Установка и запуск

1. **Клонируйте репозиторий**
   ```bash
   git clone https://github.com/yourusername/LogiChain-Forecast.git
   cd logichain-forecast
   ```

2. **Настройте переменные окружения**
   ```bash
   cp .env.example .env
   # При необходимости отредактируйте .env
   ```

3. **Запустите все сервисы**
   ```bash
   docker compose up -d
   ```

4. **Сгенерируйте первоначальные исторические данные** (3 года)
   ```bash
   docker compose exec airflow-webserver python /opt/airflow/generator/generate_historical.py
   ```

5. **Запустите инициализацию dbt и первую загрузку**
   ```bash
   docker compose exec airflow-webserver dbt deps --project-dir /opt/airflow/dbt_project
   docker compose exec airflow-webserver dbt run --project-dir /opt/airflow/dbt_project
   ```

### Доступ к компонентам

| Сервис | URL | Учётные данные |
|--------|-----|----------------|
| **Airflow** | http://localhost:8080 | `airflow` / `airflow` |
| **Streamlit Dashboard** | http://localhost:8501 | — |
| **PostgreSQL** | `localhost:5432` | `postgres` / `postgres` |

---

## 📊 Использование

### 1. Запуск еженедельного обновления

В интерфейсе Airflow:
- Перейдите в DAG `warehouse_forecast`.
- Нажмите **Trigger DAG** (молния) для ручного запуска.

По расписанию DAG автоматически выполняется каждую пятницу в 18:00.

**Что происходит при запуске:**
1. Генерируется 1000–2000 новых записей за прошедшую неделю.
2. Данные загружаются в PostgreSQL (слой Raw).
3. Выполняются dbt‑модели (очистка, интерполяция, расчёт признаков).
4. Переобучается модель LightGBM.
5. Создаётся PDF‑отчёт.

### 2. Просмотр прогноза

Откройте Streamlit Dashboard: http://localhost:8501

На дашборде доступны:
- График исторических данных и прогноза на 28 дней.
- Таблица с прогнозными значениями на следующую неделю.
- Метрики качества модели (MAE, MAPE).
- Кнопка **«Сгенерировать отчёт»** для скачивания PDF.

### 3. Документация данных (dbt docs)

Сгенерируйте и откройте документацию:
```bash
docker compose exec airflow-webserver dbt docs generate --project-dir /opt/airflow/dbt_project
docker compose exec airflow-webserver dbt docs serve --project-dir /opt/airflow/dbt_project --port 8081
```
Документация будет доступна по адресу http://localhost:8081.

---

## 📈 Оценка качества модели

В процессе работы модели рассчитываются метрики на отложенной тестовой выборке (последние 20% данных):

- **MAE** (Mean Absolute Error) — средняя абсолютная ошибка в паллето-местах.
- **MAPE** (Mean Absolute Percentage Error) — средняя абсолютная процентная ошибка.

Целевые показатели: **MAPE < 15%**. Метрики отображаются на дашборде и логируются в Airflow.

---

## 🧪 Тестирование

Для проверки работоспособности выполните:

```bash
# Запуск всех контейнеров
docker compose up -d
sleep 30

# Проверка статуса сервисов
docker compose ps

# Просмотр логов Airflow
docker compose logs airflow-scheduler

# Ручной запуск DAG (через CLI)
docker compose exec airflow-webserver airflow dags trigger warehouse_forecast
```

---

## 📸 Скриншоты

*Здесь будут размещены скриншоты интерфейса после запуска*

| Airflow DAG | Streamlit Dashboard | PDF Report |
|-------------|--------------------|------------|
| ![Airflow](docs/airflow.png) | ![Dashboard](docs/dashboard.png) | ![PDF](docs/report.png) |

---

## 📝 Лицензия

MIT License — подробности в файле [LICENSE](LICENSE).

---

## 👤 Автор

**Sapiyulla**  
Студент 2 курса Факультета информатики и информационных технологий  
[GitHub](https://github.com/Sapiyulla)