import psycopg2

DB_CONFIG = {
    "host": "localhost", 
    "database": "dwh",
    "user": "dwh_user",
    "password": "dwh_password",
    "port": 5433,         
}


def test_db_connection_and_insert():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        CREATE TEMP TABLE test_table (
            id INT,
            name TEXT
        )
    """)

    cur.execute("INSERT INTO test_table VALUES (1, 'test')")

    cur.execute("SELECT * FROM test_table")
    result = cur.fetchone()

    conn.commit()
    cur.close()
    conn.close()

    assert result == (1, "test")