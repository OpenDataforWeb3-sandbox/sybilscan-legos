from datastore import DataStoreManager
from datastore.bq.base import BaseManager
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class JobManager(BaseManager):
    """
    Class for managing job sync status
    """

    def __init__(self):
        """ """
        self.jobs_table = f"{DataStoreManager.db}.jobs"


    def get_last_synced_block(self, job_id):
        """
        Returns the last synced block
        """
        try:
            query = f"""
            SELECT MAX(end_block) AS last_synced_block
            FROM {self.jobs_table}
            WHERE job_id  = '{job_id}'
            """
            job_records = self.read(query)
            return None if job_records[0]['last_synced_block'] is pd.NA else job_records[0]['last_synced_block']
        except Exception as e:
            logger.info('get_last_synced_block failed')
            logger.info(str(e))
            return None

    def clear(self):
        """ """
        self.drop_table(self.jobs_table)

    def remove_job_state(self, job_id):
        """ """
        try:
            query = f"DELETE FROM {self.jobs_table} WHERE job_id='{job_id}'"
            self.run_query(query)
        except Exception as e:
            if self.table_exists(self.jobs_table):
                raise Exception(e)

    def update_sync_status(self, job_id, start_block, end_block):
        """
        Persists the contract information
        """
        rows = [{'job_id': job_id, 'start_block': start_block, 'end_block': end_block}]
        self.create_and_append(rows=rows, table_name=self.jobs_table, if_exists='append')
        