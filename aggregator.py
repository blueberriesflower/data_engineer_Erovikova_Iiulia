import os
import pandas as pd
import sys
from dateutil import parser
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()


def get_engine():
    return create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )


def get_available_date(engine):
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
            SELECT MIN(created_at::date), MAX(created_at::date)
            FROM user_logs
        """
            )
        )
        min_date, max_date = result.fetchone()
        return min_date, max_date


def aggregate_data(start_input, end_input):
    try:

        start_date = parser.parse(start_input).date()
        end_date = parser.parse(end_input).date()

        engine = get_engine()
        min_date, max_date = get_available_date(engine)

        if min_date and max_date:
            original_start = start_date
            original_end = end_date
            if start_date < min_date:
                start_date = min_date
            if end_date > max_date:
                end_date = max_date
            if start_date > end_date:
                print("Указанный диапазон не пересекается с данными в БД.")
                return
            if (start_date, end_date) != (original_start, original_end):
                print(
                    f"Диапазон скорректирован с {original_start}–{original_end} на {start_date}–{end_date}"
                )

        query = """
            WITH daily_metrics AS (
                SELECT 
                    l.created_at::date as day,
                    COUNT(*) FILTER (WHERE t.name = 'registration' AND l.status = 'success') as new_accounts,
                    COUNT(*) FILTER (WHERE t.name = 'write_message') as total_messages,
                    COUNT(*) FILTER (WHERE t.name = 'write_message' AND l.user_id IS NULL) as anon_messages,
                    COUNT(*) FILTER (WHERE t.name = 'create_topic' AND l.status = 'success') as topics_created_today,
                    COUNT(*) FILTER (WHERE t.name = 'delete_topic' AND l.status = 'success') as topics_deleted_today
                FROM user_logs l
                JOIN action_types t ON l.action_id = t.id
                GROUP BY 1
                ),
                cumulative AS (
                    SELECT *,
                        SUM(topics_created_today - topics_deleted_today) OVER (ORDER BY day) AS total_topics_at_end_of_day
                    FROM daily_metrics
                ),
                inventory AS (
                    SELECT *,
                        ROUND((topics_created_today::float / NULLIF(LAG(total_topics_at_end_of_day) OVER (ORDER BY day), 0) * 100)::numeric, 2) AS topic_growth_percent
                    FROM cumulative
                )
                
                SELECT 
                    day as "day",
                    new_accounts as "new_accounts",
                    ROUND(
                        (anon_messages::float / NULLIF(total_messages, 0) * 100)::numeric, 2) as "anon_messages_percent",
                    total_messages as "total_messages",
                    topic_growth_percent as "topic_growth_percent"
                FROM inventory
                WHERE day BETWEEN :start_date AND :end_date
                ORDER BY day;
                """

        engine = get_engine()
        with engine.connect() as conn:
            print(f"Анализ периода: {start_date} — {end_date}")
            df = pd.read_sql_query(
                sql=text(query),
                con=conn,
                params={"start_date": start_date, "end_date": end_date},
            )

            if df.empty:
                print("Данные за этот период отсутствуют.")
                return

            folder_path = "./reports"
            os.makedirs(folder_path, exist_ok=True)
            filename = os.path.join(
                folder_path, f"report_{start_date}_{end_date}.csv")
            df.to_csv(filename, index=False)

            print(f"Отчет создан: {filename}")
            print("Превью отчета:")
            print(df.head())

    except Exception as e:
        print(f"Ошибка: {e}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Введите обе даты")
    else:
        aggregate_data(sys.argv[1], sys.argv[2])
