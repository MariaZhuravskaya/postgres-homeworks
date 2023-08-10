import json

import psycopg2

from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'my_new_db'

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur)
                print(f"FOREIGN KEY успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params: dict, db_name: str) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f'DROP DATABASE {db_name}')
    cur.execute(f'CREATE DATABASE {db_name}')

    conn.close()
    conn = psycopg2.connect(dbname=db_name, **params)

    conn.commit()
    conn.close()

def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла fill_db.sql для заполнения БД данными."""
    with open(script_file, 'r') as file:
        respons = file.read()
        cur.execute(respons)

def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""

    cur.execute("""
    CREATE TABLE suppliers (
    supplier_id SERIAL PRIMARY KEY,
    company_name VARCHAR(50),
    contact VARCHAR(150),
    city VARCHAR(50),
    country VARCHAR(50),
    phone VARCHAR(50),
    products TEXT
    )
    """)



def get_suppliers_data(json_file: str):
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""

    with open(json_file, 'r', encoding='utf-8') as file:
        respons = json.load(file)

        for s in respons:
            lst_suppliers = []
            d = {'company_name': s['company_name'],
                 'contact': s['contact'],
                 'phone': s['phone'],
                 'products': s['products'],
                 'city': s['address'].split(';')[3],
                 'country': s['address'].split(';')[0]
                 }
            lst_suppliers.append(d)
        return lst_suppliers


def insert_suppliers_data(cur, suppliers) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    for s in suppliers:
        company_name = s['company_name']
        contact = s['contact']
        phone = s['phone']
        products = s['products']
        city = s['city']
        country = s['country']
        cur.execute(
            """
            INSERT INTO suppliers(company_name, contact, phone, products, city, country)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING supplier_id
            """,
            (company_name, contact, phone, products, city, country)
                    )


def add_foreign_keys(cur) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""

    cur.execute("""
        ALTER TABLE products ADD supplier_id SMALLINT
        """)
    cur.execute("""
        ALTER TABLE products ADD CONSTRAINT fk_supplier_id FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
        """)



if __name__ == '__main__':
    main()
