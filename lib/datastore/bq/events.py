from datastore import DataStoreManager
from datastore.bq.base import BaseManager
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class EventManager(BaseManager):
    """
    A class for managing events data
    """

    def __init__(self):
        """ """
        self.table_prefix = "events_"

    def get_event_tables(self):
        """
        Returns all the event tables for a given contract
        """

        query = f"""
            SELECT table_id
            FROM {DataStoreManager.db}.__TABLES__
            WHERE starts_with(table_id, '{self.table_prefix}')
        """

        return self.read(query)

    def clear(self, address):
        """ """

        for event in self.get_event_tables():
            query = f"""
                DELETE FROM 
                {DataStoreManager.db}.{event['table_id']}
                WHERE address = '{address}'
            """
            self.run_query(query)

    def update_events(self, event_key, events):
        """
        Appends the event data to corresponding tables
        """
        self.create_and_append(
            rows=events,
            table_name=f"{DataStoreManager.db}.{self.table_prefix}{event_key}",
            if_exists="append",
        )

    def get_events(self, contracts, events, start_block, end_block):
        """ """

        event_data = []
        for event_name in events:
            for contract in contracts:
                contract_type = contract["contract_type"]
                table_name = f"{DataStoreManager.db}.{self.table_prefix}{contract['chain']}_{contract['contract_type']}_{event_name}"
                query = f"""
                    SELECT *
                    FROM {table_name}
                    WHERE blockNumber >= {start_block}
                    AND blockNumber <= {end_block}
                    AND address = '{contract['address']}'
                """
                event_data += self.read(query)

        return sorted(
            event_data,
            key=lambda x: (x["blockNumber"], x["transactionIndex"], x["logIndex"]),
        )
