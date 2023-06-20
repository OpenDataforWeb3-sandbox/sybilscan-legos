from datastore import DataStoreManager
from datastore.bq.base import BaseManager
import pandas as pd
import json


class ContractManager(BaseManager):
    """
    Manages CRUD for tables
        - contracts
        - contract_abis
    """

    def __init__(self):
        """ """
        self.contract_table = f"{DataStoreManager.db}.contracts"
        self.contract_abi_table = f"{DataStoreManager.db}.contract_abis"

    def get_contracts(self, chain=None):
        """
        Returns all the active contracts
        """
        query = f"""
            SELECT 
                c.*,
                a.abi
            FROM {self.contract_table} c 
            LEFT JOIN {self.contract_abi_table} a
            ON c.contract_type = a.contract_type
        """

        if chain is not None:
            query += f"""
                WHERE chain = '{chain}'
            """

        contracts = self.read(query)

        # convert abi string representation to json
        for c in contracts:
            c["abi"] = json.loads(c["abi"])

        return contracts

    def get_abis(self, contract_type=None):
        """
        Returns ABI information
        """
        query = f"""
            SELECT 
                *
            FROM {self.contract_abi_table}
        """

        if contract_type is not None:
            query += f"""
                WHERE contract_type = '{contract_type}'
            """

        abis = self.read(query)

        # convert abi string representation to json
        for a in abis:
            a["abi"] = json.loads(a["abi"])

        return abis

    def get_chains(self):
        """
        Returns chain supported by the protocol
        """
        query = f"""
            SELECT DISTINCT chain
            FROM {self.contract_table}
        """
        return self.read(query)

    def clear(self):
        """ """
        self.drop_table(self.contract_table)
        self.drop_table(self.contract_abi_table)

    def update_contracts(self, contracts):
        """
        Persists the contract information
        """
        self.update(
            rows=contracts,
            table_name=f"{self.contract_table}",
            matching_cols=["chain", "address"],
        )

    def update_abis(self, contract_abis):
        """
        Persists the contract information
        """
        self.update(
            rows=contract_abis,
            table_name=f"{self.contract_abi_table}",
            matching_cols=["contract_type"],
        )
