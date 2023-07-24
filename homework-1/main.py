"""Скрипт для заполнения данными таблиц в БД Postgres."""

import psycopg2

conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='maria')
try:
    with conn:
        with conn.cursor() as cur:

            with open('north_data/customers_data.csv', 'r') as csvfile:
                next(csvfile)
                cur.copy_expert("COPY customers FROM STDIN WITH csv", csvfile)

            with open('north_data/employees_data.csv', 'r') as csvfile:
                next(csvfile)

                cur.copy_expert("COPY employees FROM STDIN WITH csv", csvfile)

            with open('north_data/orders_data.csv', 'r') as csvfile:
                next(csvfile)
                cur.copy_expert("COPY orders FROM STDIN WITH csv", csvfile)

            cur.execute("SELECT * from customers")
            cur.execute("SELECT * from employees")
            cur.execute("SELECT * from orders")


finally:
    conn.close()


