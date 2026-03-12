import os
import random
import psycopg2
from datetime import datetime, timedelta
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


def generate_data():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name, id FROM action_types")
            actions = dict(cur.fetchall())

            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)

            current_day = start_date
            while current_day <= end_date:
                for action_name, action_id in actions.items():
                    count = random.randint(5, 15)
                    
                    for i in range(count):
                        user_id = random.randint(1, 500)
                        status = 'success'
                        object_id = random.randint(1000, 9999)
                        
                        timestamp = current_day.replace(
                            hour=random.randint(0, 23),
                            minute=random.randint(0, 59)
                        )

                        if action_name == 'create_topic' and i < 2:
                            user_id = None
                            status = 'error'

                        if action_name == 'write_message' and random.random() > 0.5:
                            user_id = None

                        # Вставка данных
                        cur.execute("""
                            INSERT INTO user_logs (user_id, action_id, object_id, status, created_at)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (user_id, action_id, object_id, status, timestamp))

                print(f"Завершен день: {current_day.date()}")
                current_day += timedelta(days=1)


if __name__ == "__main__":
    generate_data()