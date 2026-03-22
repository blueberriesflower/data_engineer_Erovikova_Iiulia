import os
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )


MIN_ACTIONS_PER_TYPE = 5
ERROR_PROBABILITY = 0.10
ANON_PROBABILITY = 0.5
REQUIRED_CREATE_TOPIC_ERRORS_PER_DAY = 2


# Коэффициенты активности по дням недели (0=пн, 1=вт, 2=ср, 3=чт, 4=пт, 5=сб, 6=вс)
# Получены из анализа комментариев опросов на сайте VL.ru (см. analysis.ipynb)
WEEKDAY_COEFFICIENTS = [1.7, 2.2, 1.0, 4.8, 3.7, 2.2, 2.6]


def generate_data():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT name, id FROM action_types")
    actions = dict(cur.fetchall())

    create_id = actions.get('create_topic')

    total_topics = 0

    end_date = datetime.now().replace(hour=0, minute=0, second=0)
    current_day = end_date - timedelta(days=30)

    while current_day <= end_date:
        weekday = current_day.weekday()
        coeff = WEEKDAY_COEFFICIENTS[weekday]
        day_batch = []

        create_count = int((random.randint(20, 40)) * coeff)
        successful_creates_today = 0

        for i in range(create_count):
            timestamp = current_day + \
                timedelta(hours=random.randint(0, 23),
                          minutes=random.randint(0, 59))
            user_id = random.randint(1, 1000)
            status = 'success'

            if i < REQUIRED_CREATE_TOPIC_ERRORS_PER_DAY:
                status = 'error'
                user_id = None
            else:
                successful_creates_today += 1

            day_batch.append(
                (user_id, create_id, random.randint(100, 9999), status, timestamp))

        max_deletions = total_topics + successful_creates_today
        deletions_count = 0

        for action_name, action_id in actions.items():
            count = int(max(MIN_ACTIONS_PER_TYPE,
                        random.randint(5, 15)) * coeff)

            if action_name == 'create_topic':
                continue

            if action_name == 'delete_topic':
                if max_deletions >= 5:
                    count = random.randint(5, max_deletions)
                else:
                    count = max_deletions
                deletions_count = count
            else:
                count = int(max(MIN_ACTIONS_PER_TYPE,
                            random.randint(5, 15)) * coeff)

            for i in range(count):
                timestamp = current_day + timedelta(
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59)
                )

                user_id = random.randint(1, 1000)
                object_id = random.randint(100, 9999)
                status = 'success'

                if action_name == 'write_message':
                    if random.random() < ANON_PROBABILITY:
                        user_id = None

                elif action_name == 'first_visit':
                    user_id = None

                elif random.random() < 0.05:
                    status = 'error'

                day_batch.append(
                    (user_id, action_id, object_id, status, timestamp))

        execute_values(cur, """
            INSERT INTO user_logs (user_id, action_id, object_id, status, created_at)
            VALUES %s
        """, day_batch)

        total_topics = total_topics + successful_creates_today - deletions_count

        print(f"Заполнен день: {current_day.date()}")
        current_day += timedelta(days=1)

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    generate_data()
