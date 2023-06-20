from datastore.bq.base import BaseManager
from datastore import DataStoreManager
import pandas as pd
import json


class AppManager(BaseManager):
    """
    Manages CRUD for application tables
        - grant_metrics
    #TODO: add chain
    """

    def __init__(self):
        """ """
        self.app_metrics_table = f"{DataStoreManager.db}.application_metrics"
        self.wallet_metrics_table = f"{DataStoreManager.db}.wallet_metrics"

    def clear_metrics_state(self):
        """ """
        self.drop_table(self.app_metrics_table)
        self.drop_table(self.wallet_metrics_table)

    def get_wallet_metrics(self):
        """ """
        wallet_metrics = self.read_table(self.wallet_metrics_table)
        for w in wallet_metrics:
            w["tags"] = json.loads(w["tags"])
        return wallet_metrics

    def get_application_metrics(self):
        """ """
        return self.read_table(self.app_metrics_table)

    def update_application_metrics(self, app_metrics):
        """ """
        self.update(
            rows=app_metrics,
            table_name=self.app_metrics_table,
            matching_cols=["round_address", "app_index"],
        )

    def update_wallet_metrics(self, wallet_metrics):
        """ """
        self.update(
            rows=wallet_metrics,
            table_name=self.wallet_metrics_table,
            matching_cols=["wallet"]
        )
