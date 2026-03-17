import os
import random
import psycopg2
from psycopg2.extras import execute_values
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

# Коэффициенты активности по дням недели (0=пн, 1=вт, 2=ср, 3=чт, 4=пт, 5=сб, 6=вс)
# Получены из анализа комментариев опросов на сайте VL.ru (см. analysis.ipynb)
WEEKDAY_COEFFICIENTS = [1.7, 2.2, 1.0, 4.8, 3.7, 2.2, 2.6]

def generate_data():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name, id FROM action_types")
            actions = dict(cur.fetchall())

            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)

            current_day = start_date
            while current_day <= end_date:
                weekday = current_day.weekday()
                coeff = WEEKDAY_COEFFICIENTS[weekday]

                # Список для пакетной вставки всех записей за день
                day_batch = []

                for action_name, action_id in actions.items():
                    base_count = random.randint(5, 15)
                    # Масштабируем с учётом дня недели и округляем
                    count = int(round(base_count * coeff))
                    if count < 5:
                        count = 5

                    for i in range(count):
                        # Генерация времени внутри дня (случайный час и минута)
                        timestamp = current_day.replace(
                            hour=random.randint(0, 23),
                            minute=random.randint(0, 59)
                        )

                        user_id = random.randint(1, 5000)
                        status = 'success'
                        object_id = random.randint(1000, 9999)

                        if action_name == 'create_topic' and i < 2:
                            user_id = None
                            status = 'error'
                        elif action_name == 'write_message':
                            # Примерно 50% сообщений от анонимных пользователей
                            if random.random() > 0.5:
                                user_id = None
                            if random.random() < 0.05:  # 5% ошибок
                                status = 'error'
                        else:
                            # Для остальных действий с вероятностью 10% добавляем ошибку
                            if random.random() < 0.10:
                                status = 'error'

                        day_batch.append((user_id, action_id, object_id, status, timestamp))

                # Пакетная вставка всех записей за день
                execute_values(cur, """
                    INSERT INTO user_logs (user_id, action_id, object_id, status, created_at)
                    VALUES %s
                """, day_batch)
                conn.commit()  # Фиксируем день

                print(f"Завершен день: {current_day.date()} (записей: {len(day_batch)})")
                current_day += timedelta(days=1)


if __name__ == "__main__":
    generate_data()