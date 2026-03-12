import os
import sys
import pandas as pd
import psycopg2
from dateutil import parser
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )

def aggregate_data(start_input, end_input):
    try:
        start_date = parser.parse(start_input).date()
        end_date = parser.parse(end_input).date()
        
        query = f"""
        WITH daily_stats AS (
            SELECT 
                l.created_at::date AS day,
                COUNT(l.id) FILTER (WHERE t.name = 'registration') AS new_accounts,
                COUNT(l.id) FILTER (WHERE t.name = 'write_message') AS total_messages,
                COUNT(l.id) FILTER (WHERE t.name = 'write_message' AND l.user_id IS NULL) AS anon_messages,
                COUNT(l.id) FILTER (WHERE t.name = 'create_topic' AND l.status = 'success') AS topics_created
            FROM user_logs l
            JOIN action_types t ON l.action_id = t.id
            WHERE l.created_at::date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY l.created_at::date
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
                WHEN LAG(topics_created) OVER (ORDER BY day) > 0 
                THEN ROUND(((topics_created::numeric - LAG(topics_created) OVER (ORDER BY day)) 
                     / LAG(topics_created) OVER (ORDER BY day)) * 100, 2)
                ELSE 0 
            END AS topic_growth_pct
        FROM daily_stats
        ORDER BY day;
        """

        with get_db_connection() as conn:
            print(f"Анализ периода: {start_date} — {end_date}")
            df = pd.read_sql_query(query, conn)

            if df.empty:
                print("Данные за этот период отсутствуют.")
                return

            filename = f"report_{start_date}_{end_date}.csv"
            df.to_csv(filename, index=False)
            print(f"Отчет создан: {filename}")
            print(f"Превью отчета:")
            print(df.head())

    except Exception as e:
        print(f"Ошибка: {e}.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Использование: python aggregator.py '01-01-2026' '2026/02/01'")
    else:
        aggregate_data(sys.argv[1], sys.argv[2])