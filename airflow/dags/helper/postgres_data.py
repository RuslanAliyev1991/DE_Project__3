from airflow.providers.postgres.hooks.postgres import PostgresHook
import time


def execute_postgres_crud():
    # 1. Airflow-da təyin etdiyin Connection ID vasitəsilə bağlantı yaradırıq
    pg_hook = PostgresHook(postgres_conn_id="POSTGRES_CONNECTION")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        # Cədvəlin mövcudluğundan əmin oluruq
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS my_test (
                id          SERIAL PRIMARY KEY,
                name        VARCHAR(20)    NOT NULL,
                created_at  TIMESTAMP      NOT NULL DEFAULT NOW(),
                updated_at  TIMESTAMP      NOT NULL DEFAULT NOW()
            );
        """
        )

        # -------------------------------------------------------------
        # 2. Üç fərqli sətir INSERT edirik
        # -------------------------------------------------------------
        insert_query = "INSERT INTO my_test (name) VALUES (%s) RETURNING id;"

        cursor.execute(insert_query, ("Ruslan",))
        id1 = cursor.fetchone()[0]
        time.sleep(3)

        cursor.execute(insert_query, ("cavid",))
        id2 = cursor.fetchone()[0]
        time.sleep(2)

        cursor.execute(insert_query, ("samir",))
        id3 = cursor.fetchone()[0]
        time.sleep(3)

        print(f"--> 3 sətir uğurla əlavə olundu. ID-lər: {id1}, {id2}, {id3}")

        # -------------------------------------------------------------
        # 3. İkinci sətri UPDATE edirik
        # -------------------------------------------------------------
        update_query = """
            UPDATE my_test 
            SET name = %s, updated_at = CLOCK_TIMESTAMP() 
            WHERE id = %s;
        """
        cursor.execute(update_query, ("cavid updated", id2))
        time.sleep(3)
        print(f"--> ID={id2} olan sətir yeniləndi (UPDATE).")

        # -------------------------------------------------------------
        # 4. Üçüncü sətri DELETE edirik
        # -------------------------------------------------------------
        delete_query = "DELETE FROM my_test WHERE id = %s;"
        cursor.execute(delete_query, (id3,))
        print(f"--> ID={id3} olan sətir silindi (DELETE).")

        # -------------------------------------------------------------
        # 5. Dəyişiklikləri bazada təsdiqləyirik (Commit)
        # -------------------------------------------------------------
        conn.commit()
        print("--> Bütün əməliyyatlar uğurla bazaya yazıldı (COMMITTED).")

    except Exception as e:
        # Xəta baş verərsə əməliyyatları geri qaytarırıq
        conn.rollback()
        print(f"Xəta baş verdi: {e}")
        raise e

    finally:
        # Bağlantıları bağlayırıq
        cursor.close()
        conn.close()