import os
import random
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

def get_engine():
    return create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )

# Коэффициенты активности по дням недели (0=пн, 1=вт, 2=ср, 3=чт, 4=пт, 5=сб, 6=вс)
# Получены из анализа комментариев опросов на сайте VL.ru (см. analysis.ipynb)
WEEKDAY_COEFFICIENTS = [1.7, 2.2, 1.0, 4.8, 3.7, 2.2, 2.6]


def generate_data():
    engine = get_engine()
    with engine.raw_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name, id FROM action_types")
            actions = dict(cur.fetchall())

            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)

            # Множество для хранения активных топиков (object_id)
            existing_topic_ids = set()

            current_day = start_date
            while current_day <= end_date:
                weekday = current_day.weekday()
                coeff = WEEKDAY_COEFFICIENTS[weekday]

                day_batch = []

                for action_name, action_id in actions.items():
                    base_count = random.randint(5, 15)
                    count = int(round(base_count * coeff))
                    if count < 5:
                        count = 5

                    for i in range(count):
                        timestamp = current_day.replace(
                            hour=random.randint(0, 23),
                            minute=random.randint(0, 59)
                        )

                        # Значения по умолчанию
                        user_id = random.randint(1, 5000)
                        status = 'success'
                        object_id = random.randint(1000, 9999)

                        # --- Обработка действий, связанных с топиками ---
                        if action_name == 'create_topic':
                            # Генерируем уникальный object_id для нового топика
                            new_id = random.randint(1000, 99999)  # увеличим диапазон, чтобы избежать коллизий
                            while new_id in existing_topic_ids:
                                new_id = random.randint(1000, 99999)
                            object_id = new_id

                            # Имитация ошибок (как в исходном коде)
                            if i < 2:  # первые две записи — ошибка
                                status = 'error'
                                user_id = None

                            if status == 'success':
                                existing_topic_ids.add(object_id)

                        elif action_name == 'delete_topic':
                            # Удаляем только если есть что удалять
                            if existing_topic_ids:
                                object_id = random.choice(list(existing_topic_ids))
                                # 10% ошибок (как для других действий)
                                if random.random() < 0.10:
                                    status = 'error'
                                else:
                                    status = 'success'

                                if status == 'success':
                                    existing_topic_ids.remove(object_id)
                            else:
                                # Нет активных топиков — пропускаем удаление
                                continue

                        elif action_name == 'write_message':
                            # Оставляем как было (можно позже привязать к существующим топикам)
                            prob = random.gauss(0.5, 0.05)
                            prob = max(0.4, min(0.6, prob))
                            if random.random() < prob:
                                user_id = None
                            else:
                                user_id = random.randint(1, 5000)

                            if random.random() < 0.05:
                                status = 'error'

                        else:
                            # Остальные действия (registration и т.д.)
                            if random.random() < 0.10:
                                status = 'error'

                        day_batch.append((user_id, action_id, object_id, status, timestamp))

                # Пакетная вставка
                execute_values(cur, """
                    INSERT INTO user_logs (user_id, action_id, object_id, status, created_at)
                    VALUES %s
                """, day_batch)
                conn.commit()

                print(f"Завершен день: {current_day.date()} (записей: {len(day_batch)}, активных топиков: {len(existing_topic_ids)})")
                current_day += timedelta(days=1)

if __name__ == "__main__":
    generate_data()