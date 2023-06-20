from datastore.bq.base import BaseManager
from datastore import DataStoreManager
import pandas as pd
import json

class TokenManager(BaseManager):
    """
    Manages CRUD for tables
        - tokens
    """

    def __init__(self):
        """ """
        self.tokens_table = f"{DataStoreManager.db}.tokens"

    def get_tokens(self):
        """
        Returns all the token mappings
        """
        tokens = self.read_table(self.tokens_table)
        return {t["token_address"]: t["token_symbol"] for t in tokens}

    def clear(self):
        """ """
        self.drop_table(self.tokens_table)

    def update_tokens(self, tokens):
        """
        Persists the contract information
        """
        self.update(
            rows=tokens, table_name=self.tokens_table, matching_cols=["token_address"]
        )
