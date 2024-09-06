import psycopg2
from psycopg2.sql import SQL, Identifier


def create_db(conn):
    """Создание таблиц"""
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE phone_numbers;
            DROP TABLE clients_info;
            """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS clients_info(
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(60) NOT NULL,
            last_name VARCHAR(60) NOT NULL,
            email VARCHAR(60) NOT NULL UNIQUE
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS phone_numbers(
            id SERIAL PRIMARY KEY,
            phone_number VARCHAR(20),
            client_id INTEGER NOT NULL REFERENCES clients_info(id)
        );
        """)

        print("База данных создана.")

        conn.commit()


def add_client(conn, first_name, last_name, email, phones=None):
    """Добавление клиента в БД"""
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO clients_info(first_name, last_name, email)
        VALUES(%s, %s, %s);
        """, (first_name, last_name, email))

        cur.execute("""
        SELECT id from clients_info
        WHERE email = %s;
        """, (email,))
        client_id = cur.fetchone()

        if phones is not None:
            for phone in phones:
                cur.execute("""
                INSERT INTO phone_numbers(phone_number, client_id)
                VALUES(%s, %s);
                """, (phone, client_id))

        print(f"{first_name} {last_name} добавлен.")

        conn.commit()


def add_phone(conn, client_id, phone):
    """Добавление номера телефона"""
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id from clients_info;
        """)
        clients = cur.fetchall()

        if (client_id,) in clients:
            cur.execute("""
            INSERT INTO phone_numbers(phone_number, client_id)
            VALUES(%s, %s);
            """, (phone, client_id))
        else:
            print('Такого клиента не существует')

        print(f'Телефон {phone} добавлен.')

        conn.commit()


def change_client(conn, client_id, first_name=None, last_name=None, email=None, old_phone=None, new_phone=None):
    """Изменение данных клиента."""
    new_info = {"first_name": first_name, 'last_name': last_name, 'email': email}

    with conn.cursor() as cur:
        for key, value in new_info.items():
            if key:
                cur.execute(SQL("UPDATE clients_info SET {}=%s WHERE id=%s;").format(Identifier(key)),
                            (value, client_id))
        if old_phone and new_phone:
            cur.execute("UPDATE phone_numbers SET phone_number=%s WHERE client_id=%s AND phone_number=%s;",
                        (new_phone, client_id, old_phone))

        print('Данные клиента изменены.')

        conn.commit()


def delete_phone(conn, client_id, phone):
    """Удаление номера телефона."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM clients_info;")
        clients = cur.fetchall()

        cur.execute("SELECT phone_number FROM phone_numbers;")
        phones = cur.fetchall()

        if (client_id,) in clients:
            if (phone,) in phones:
                cur.execute("""DELETE FROM phone_numbers
                            WHERE client_id=%s AND phone_number=%s;""", (client_id, phone))
            else:
                print('Такого номера телефона нет у этого клиента')
        else:
            print('Такого клиента не существует')

        print(f'Телефон {phone} удален.')

        conn.commit()


with psycopg2.connect(database="clients_db", user="postgres", password="postgres") as conn:
    create_db(conn)

    add_client(conn, 'Иван', 'Иванов', 'ivan@mail.ru')
    add_client(conn, 'Петр', 'Петров', 'petr@mail.ru', phones=('79999', '98888', '39999'))

    add_phone(conn, 1, '764646')

    change_client(conn, client_id=1, first_name='Ваня', last_name='Ванечкин', email='Vanya@gmail.com',
                  old_phone='764646', new_phone='99999')

    delete_phone(conn, 2, '79999')
conn.close()
