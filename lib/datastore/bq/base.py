from datastore import DataStoreManager
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class BaseManager:
    def __init__(self):
        pass

    def run_query(self, query):
        """
        executes the given query
        """
        with DataStoreManager.engine.connect() as connection:
             connection.execute(query)

    def read(self, query):
        """ """
        return pd.read_gbq(query, credentials=DataStoreManager.credentials).to_dict(
            "records"
        )

    def drop_table(self, table_name):
        """ """
        if self.table_exists(table_name):
            self.run_query(f"DROP TABLE {table_name}")
        else:
            logger.info(f"{table_name} does not exists")
            return

    def read_table(self, table_name):
        """ """
        if self.table_exists(table_name):
            return self.read(f"SELECT * FROM {table_name}")
        else:
            logger.info(f"{table_name} does not exists")
            return []

    def table_exists(self, table_name):
        """ """
        dataset, table = table_name.split(".")
        query = f"""
            SELECT COUNT(*) as cnt
            FROM {dataset}.__TABLES__
            WHERE table_id = '{table}' 
        """
        records = self.read(query)
        return records[0]["cnt"] == 1

    def create_and_append(self, rows, table_name, if_exists='append'):
        """ """
        if rows:
            df_data = pd.DataFrame(rows)
            df_data.to_gbq(
                f"{table_name}",
                if_exists=if_exists,
                credentials=DataStoreManager.credentials,
            )

    def update(self, rows, table_name, matching_cols=[]):
        """ """
        if not rows:
            return

        if not self.table_exists(table_name):
            logger.info(f"table {table_name} does not exists. creating...")
            self.create_and_append(rows, table_name)
            return

        logger.info(f"updating table {table_name}...")
        dataset, table = table_name.split(".")
        tmp_table = f"{dataset}.tmp_{table}"
        self.create_and_append(rows, tmp_table, if_exists='replace')

        # Construct the matching condition
        matching_condition = " AND ".join(f"T.{col} = S.{col}" for col in matching_cols)

        # Construct the columns to be updated/inserted
        all_cols = list(rows[0].keys())
        update_cols = list(set(all_cols).difference(matching_cols))

        # Construct the MERGE statement
        merge_statement = f"""
            MERGE {table_name} AS T
            USING {tmp_table} AS S
            ON {matching_condition}
            WHEN MATCHED THEN
            UPDATE SET
                {', '.join(f'T.{col} = S.{col}' for col in update_cols)}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(all_cols)})
                VALUES ({', '.join(f'S.{col}' for col in all_cols)})
        """
        self.run_query(merge_statement)

        # DROP the tmp table
        self.drop_table(tmp_table)
