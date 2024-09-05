import psycopg2


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

        if phones is None:
            cur.execute("""
            INSERT INTO phone_numbers(phone_number, client_id)
            VALUES(%s, %s);
            """, (None, client_id))
        else:
            for phone in phones:
                cur.execute("""
                INSERT INTO phone_numbers(phone_number, client_id)
                VALUES(%s, %s);
                """, (phone, client_id))

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

        conn.commit()


with psycopg2.connect(database="clients_db", user="postgres", password="postgres") as conn:
    create_db(conn)

    add_client(conn, 'Иван', 'Иванов', 'ivan@mail.ru')
    add_client(conn, 'Петр', 'Петров', 'petr@mail.ru', phones=('79999', '98888', '39999'))

    add_phone(conn, 1, '764646',)
conn.close()
