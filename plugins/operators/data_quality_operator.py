from airflow.models import BaseOperator
import psycopg2


class DataQualityOperator(BaseOperator):

    def __init__(
        self,
        table: str,
        pk: list,
        check_nulls: bool = True,
        check_duplicates: bool = True,
        expected_columns: list = None,
        db_config: dict = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.table = table
        self.pk = pk
        self.check_nulls = check_nulls
        self.check_duplicates = check_duplicates
        self.expected_columns = expected_columns
        self.db_config = db_config or {
            "host": "postgres-dwh",
            "database": "dwh",
            "user": "dwh_user",
            "password": "dwh_password",
            "port": 5432,
        }


    # CONNECT
    def _connect(self):
        return psycopg2.connect(**self.db_config)

    # NULL CHECK       
    def _check_nulls(self, cur):
        null_conditions = " OR ".join([f"{col} IS NULL" for col in self.pk])

        query = f"""
            SELECT *
            FROM {self.table}
            WHERE {null_conditions}
            LIMIT 1
        """

        cur.execute(query)

        if cur.fetchone():
            raise ValueError(f"❌ NULL check failed on PK columns in {self.table}")

    # DUPLICATES CHECK
    def _check_duplicates(self, cur):
        group_by_cols = ", ".join(self.pk)

        cur.execute(f"""
            SELECT {group_by_cols}, COUNT(*)
            FROM {self.table}
            GROUP BY {group_by_cols}
            HAVING COUNT(*) > 1
        """)

        duplicates = cur.fetchall()

        if duplicates:
            raise ValueError(
                f"❌ Duplicate keys found in {self.table}: {duplicates[:5]}"
            )

    # SCHEMA CHECK
    def _check_schema(self, cur):
        if not self.expected_columns:
            return

        cur.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """, (self.table.split(".")[-1],))

        actual_columns = {row[0] for row in cur.fetchall()}
        expected_columns = set(self.expected_columns)

        missing = expected_columns - actual_columns

        if missing:
            raise ValueError(
                f"❌ Schema mismatch in {self.table}. Missing: {missing}"
            )

    # EXECUTE
    def execute(self, context):
        conn = self._connect()
        cur = conn.cursor()

        self.log.info(f"Running data quality checks on {self.table}")

        if self.check_nulls:
            self._check_nulls(cur)

        if self.check_duplicates:
            self._check_duplicates(cur)

        if self.expected_columns:
            self._check_schema(cur)

        cur.close()
        conn.close()

        self.log.info(f" Data quality passed for {self.table}")