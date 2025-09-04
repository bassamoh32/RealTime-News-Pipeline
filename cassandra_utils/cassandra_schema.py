class CassandraSchema:
    CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {0}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }};
    """

    CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS {0} (
            new_id TEXT,
            title TEXT,
            description TEXT,
            url TEXT,
            publishedAt TIMESTAMP,
            source TEXT,
            category TEXT,
            PRIMARY KEY ((category), publishedAt, new_id)
        );
    """

    @staticmethod
    def get_insert_query(table: str):
        return f"""
            INSERT INTO {table} (
                new_id, title, description, url,
                publishedAt, source, category
            ) VALUES (%s, %s, %s, %s, %s, %s, %s);
        """