import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dateutil import parser
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    return create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )

def aggregate_data(start_input, end_input):
    try:
        start_date = parser.parse(start_input).date()
        end_date = parser.parse(end_input).date()

        query = """
        WITH daily_stats AS (
            SELECT 
                DATE(l.created_at) AS day,
                COUNT(DISTINCT CASE WHEN t.name = 'registration' AND l.user_id IS NOT NULL THEN l.user_id END) AS new_accounts,
                COUNT(CASE WHEN t.name = 'write_message' THEN 1 END) AS total_messages,
                COUNT(CASE WHEN t.name = 'write_message' AND l.user_id IS NULL THEN 1 END) AS anon_messages,
                COUNT(CASE WHEN t.name = 'create_topic' AND l.status = 'success' THEN 1 END) AS topics_created_today
            FROM user_logs l
            JOIN action_types t ON l.action_id = t.id
            WHERE l.created_at >= :start_date
            AND l.created_at < :end_date + interval '1 day'
            GROUP BY DATE(l.created_at)
        ),
        cumulative_topics AS (
            SELECT 
                day,
                new_accounts,
                total_messages,
                anon_messages,
                topics_created_today,
                SUM(topics_created_today) OVER (ORDER BY day) AS total_topics
            FROM daily_stats
        )
        SELECT 
            day,
            new_accounts,
            total_messages,
            CASE 
                WHEN total_messages > 0 THEN ROUND((anon_messages::numeric / total_messages) * 100, 2)
                ELSE 0 
            END AS anon_messages_pct,
            CASE 
                WHEN LAG(total_topics) OVER (ORDER BY day) > 0 
                THEN ROUND(((total_topics::numeric - LAG(total_topics) OVER (ORDER BY day)) 
                     / LAG(total_topics) OVER (ORDER BY day)) * 100, 2)
                ELSE 0 
            END AS topic_growth_pct
        FROM cumulative_topics
        ORDER BY day;
        """

        engine = get_engine()
        with engine.connect() as conn:
            print(f"Анализ периода: {start_date} — {end_date}")
            df = pd.read_sql_query(
                sql=text(query),
                con=conn,
                params={"start_date": start_date, "end_date": end_date}
            )

            if df.empty:
                print("Данные за этот период отсутствуют.")
                return

            folder_path = './reports'
            os.makedirs(folder_path, exist_ok=True)
            filename = os.path.join(folder_path, f'report_{start_date}_{end_date}.csv')
            df.to_csv(filename, index=False)

            print(f"Отчет создан: {filename}")
            print("Превью отчета:")
            print(df.head())

    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Использование: python aggregator.py '01-01-2026' '2026/02/01'")
    else:
        aggregate_data(sys.argv[1], sys.argv[2])