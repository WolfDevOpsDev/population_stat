import streamlit as st
import pandas as pd
import requests
import pycountry
import plotly.express as px
import psycopg2
import os
import time

# --- НАЛАШТУВАННЯ ---
st.set_page_config(page_title="Population Dashboard", layout="wide", page_icon="🌍")


# --- КЛАС: РОБОТА З БАЗОЮ ДАНИХ ---
class DatabaseManager:
    def __init__(self):
        self.host = os.getenv("DB_HOST", "localhost")
        self.database = os.getenv("DB_NAME", "population_db")
        self.user = os.getenv("DB_USER", "user")
        self.password = os.getenv("DB_PASS", "password")
        self.conn = None
        self._connect()

    def _connect(self):
        """Встановлення з'єднання з ретраями"""
        for i in range(5):
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    database=self.database,
                    user=self.user,
                    password=self.password
                )
                return
            except Exception as e:
                time.sleep(2)
        st.error("❌ Не вдалося підключитися до бази даних.")

    def init_db(self):
        """Створення таблиці"""
        query = """
                CREATE TABLE IF NOT EXISTS countries \
                ( \
                    id \
                    SERIAL \
                    PRIMARY \
                    KEY, \
                    name \
                    VARCHAR \
                ( \
                    255 \
                ),
                    cca2 VARCHAR \
                ( \
                    10 \
                ),
                    cca3 VARCHAR \
                ( \
                    10 \
                ),
                    region VARCHAR \
                ( \
                    100 \
                ),
                    population BIGINT,
                    area FLOAT
                    );
                TRUNCATE TABLE countries; \
                """
        with self.conn.cursor() as cur:
            cur.execute(query)
            self.conn.commit()

    def save_data(self, df: pd.DataFrame):
        """Збереження DataFrame у базу"""
        self.init_db()
        query = """
                INSERT INTO countries (name, cca2, cca3, region, population, area)
                VALUES (%s, %s, %s, %s, %s, %s) \
                """
        data_tuples = list(df.itertuples(index=False, name=None))

        with self.conn.cursor() as cur:
            # executemany для швидкості
            cur.executemany(query, data_tuples)
            self.conn.commit()

    def get_all_data(self):
        """Отримати всі дані для карти (SELECT *)"""
        query = "SELECT name, cca2, cca3, region, population, area FROM countries"
        return pd.read_sql_query(query, self.conn)

    def get_aggregated_stats(self):
        """
        Агрегація даних одним SQL запитом згідно ТЗ:
        Регіон, Загальне, Найбільша (країна/поп), Найменша (країна/поп)
        """
        query = """
                WITH ranked AS (SELECT region, \
                                       name, \
                                       population, \
                                       SUM(population) OVER (PARTITION BY region) as total_pop, ROW_NUMBER() OVER (PARTITION BY region ORDER BY population DESC) as rank_desc, ROW_NUMBER() OVER (PARTITION BY region ORDER BY population ASC) as rank_asc \
                                FROM countries \
                                WHERE region IS NOT NULL \
                                  AND region != ''
                    )
                SELECT region                                           as "Регіон", \
                       total_pop                                        as "Загальне населення", \
                       MAX(CASE WHEN rank_desc = 1 THEN name END)       as "Найбільша країна", \
                       MAX(CASE WHEN rank_desc = 1 THEN population END) as "Населення (max)", \
                       MAX(CASE WHEN rank_asc = 1 THEN name END)        as "Найменша країна", \
                       MAX(CASE WHEN rank_asc = 1 THEN population END)  as "Населення (min)"
                FROM ranked
                GROUP BY region, total_pop
                ORDER BY total_pop DESC; \
                """
        return pd.read_sql_query(query, self.conn)


# --- ФУНКЦІЇ API (ETL) ---
def fetch_api_data():
    """Отримує дані з RestCountries API"""
    url = "https://restcountries.com/v3.1/all?fields=name,cca2,cca3,population,region,area"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        countries_list = []
        for item in data:
            countries_list.append((
                item.get("name", {}).get("common", "N/A"),
                item.get("cca2", ""),
                item.get("cca3", ""),
                item.get("region", "Other"),
                item.get("population", 0),
                item.get("area", 0)
            ))

        # Повертаємо DataFrame, який відповідає структурі таблиці БД
        return pd.DataFrame(countries_list, columns=["name", "cca2", "cca3", "region", "population", "area"])
    except Exception as e:
        st.error(f"Error fetching API: {e}")
        return pd.DataFrame()


def get_population_world_bank(country_code):
    """Отримує історичні дані для однієї країни (World Bank)"""
    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/SP.POP.TOTL"
    params = {"format": "json", "date": "1990:2023", "per_page": 50}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if len(data) > 1 and isinstance(data[1], list):
            rows = []
            for entry in data[1]:
                if entry["value"] is not None:
                    rows.append({
                        "country": entry["country"]["value"],
                        "year": int(entry["date"]),
                        "population": entry["value"],
                    })
            return pd.DataFrame(rows).sort_values("year")
        return None
    except Exception as e:
        st.error(f"World Bank API error: {e}")
        return None


# --- UI LOGIC ---

def main():
    st.sidebar.title("🛠️ Керування даними")

    # Ініціалізація БД
    db = DatabaseManager()

    # Кнопка оновлення бази
    if st.sidebar.button("🔄 Оновити базу (API -> DB)"):
        with st.spinner("Завантаження з RestCountries..."):
            df_api = fetch_api_data()

        if not df_api.empty:
            with st.spinner("Збереження в PostgreSQL..."):
                try:
                    db.save_data(df_api)
                    st.sidebar.success(f"✅ Успішно оновлено! ({len(df_api)} країн)")
                    st.cache_data.clear()  # Чистимо кеш, бо дані змінились
                except Exception as e:
                    st.sidebar.error(f"Помилка запису в БД: {e}")

    # Основний контент
    st.title("🌍 Глобальна статистика населення (DB Version)")
    st.markdown("Дані зберігаються в **PostgreSQL**. Карта будується на основі бази.")

    tab1, tab2 = st.tabs(["🗺️ Карта світу та Звіт", "🔍 Детальна історія країни"])

    # ==================== TAB 1: DB DATA ====================
    with tab1:
        # Читаємо дані з БД для карти
        try:
            df_db = db.get_all_data()
        except Exception:
            df_db = pd.DataFrame()

        if df_db.empty:
            st.warning("⚠️ База даних порожня. Натисніть 'Оновити базу' в сайдбарі.")
        else:
            col1, col2 = st.columns([3, 1])

            with col1:
                st.subheader("Карта населення (з БД)")
                fig_map = px.choropleth(
                    df_db,
                    locations="cca3",
                    color="population",
                    hover_name="name",
                    hover_data=["region"],
                    color_continuous_scale=px.colors.sequential.Plasma,
                    projection="natural earth",
                    title="Населення світу"
                )
                fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
                st.plotly_chart(fig_map, use_container_width=True)

            with col2:
                st.subheader("Топ-10 (з БД)")
                top_10 = df_db.sort_values(by="population", ascending=False).head(10)
                st.dataframe(
                    top_10[["name", "population"]].style.format({"population": "{:,}"}),
                    hide_index=True,
                    use_container_width=True
                )

            st.divider()

            # Агрегований звіт SQL
            st.subheader("📊 Агрегований звіт по регіонах (SQL Aggregation)")
            df_agg = db.get_aggregated_stats()
            if not df_agg.empty:
                st.dataframe(
                    df_agg.style.format({
                        "Загальне населення": "{:,}",
                        "Населення (max)": "{:,}",
                        "Населення (min)": "{:,}"
                    }),
                    use_container_width=True
                )

    # ==================== TAB 2: HISTORY (API) ====================
    with tab2:
        st.header("Історичний аналіз")

        # Список країн беремо з БД (якщо є), інакше порожній
        if not df_db.empty:
            country_options = dict(zip(df_db["name"], df_db["cca2"]))

            # Спробуємо знайти Україну за замовчуванням
            default_idx = 0
            keys_list = list(country_options.keys())
            if "Ukraine" in keys_list:
                default_idx = keys_list.index("Ukraine")

            selected_country_name = st.selectbox(
                "Оберіть країну:",
                keys_list,
                index=default_idx
            )
            selected_code = country_options[selected_country_name]

            if st.button("Отримати графік (World Bank)"):
                with st.spinner(f"Запит до World Bank для {selected_country_name}..."):
                    df_history = get_population_world_bank(selected_code)

                if df_history is not None:
                    fig_line = px.line(
                        df_history, x="year", y="population", markers=True,
                        title=f"Динаміка: {selected_country_name}"
                    )
                    st.plotly_chart(fig_line, use_container_width=True)
                else:
                    st.warning("Немає даних у World Bank.")
        else:
            st.info("Завантажте дані в базу (Tab 1), щоб вибрати країну.")


if __name__ == "__main__":
    main()