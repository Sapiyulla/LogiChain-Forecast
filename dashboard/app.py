# dashboard/app.py
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os

st.set_page_config(page_title="LogiChain Forecast", layout="wide")

st.title("📦 LogiChain Forecast")
st.markdown("Прогнозирование загрузки складских площадей")

# Подключение к БД
DB_HOST = os.getenv("WAREHOUSE_DB_HOST", "warehouse")
DB_PORT = os.getenv("WAREHOUSE_DB_PORT", "5432")
DB_NAME = os.getenv("WAREHOUSE_DB_NAME", "warehouse")
DB_USER = os.getenv("WAREHOUSE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("WAREHOUSE_DB_PASSWORD", "postgres")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    engine = create_engine(DATABASE_URL)
    conn = engine.connect()
    st.success("✅ Подключение к базе данных установлено")
except Exception as e:
    st.error(f"❌ Ошибка подключения к БД: {e}")

# Заглушка для графика
st.subheader("Прогноз загрузки склада")
st.info("Данные прогноза появятся после первого запуска Airflow DAG")

# Кнопка генерации отчёта (пока не активна)
if st.button("📄 Сгенерировать PDF-отчёт"):
    st.warning("Функция в разработке")