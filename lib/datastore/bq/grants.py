from datastore.bq.base import BaseManager
from datastore.bq.contracts import ContractManager
from datastore import DataStoreManager


class GrantManager(BaseManager):
    """
    Bq class for managing rounds data
        - programs
        - rounds
        - rounds_applications
        - votes
    """

    def __init__(self):
        """ """
        self.rounds_table = f"{DataStoreManager.db}.rounds"
        self.applications_table = f"{DataStoreManager.db}.round_applications"
        self.programs_table = f"{DataStoreManager.db}.programs"
        self.votes_table = f"{DataStoreManager.db}.votes"

    def clear_rounds_state(self):
        """ """
        self.drop_table(self.rounds_table)

    def clear_applications_state(self):
        """ """
        self.drop_table(self.applications_table)

    def clear_programs_state(self):
        """ """
        self.drop_table(self.programs_table)

    def clear_votes_state(self):
        """ """
        self.drop_table(self.votes_table)

    def get_round_contracts(self, chain, owned_by=None):
        """
        Returns all the round contracts
        """

        try:
            query = f"""
                SELECT *
                FROM {self.rounds_table}
                WHERE chain = '{chain}'
            """
            return self.read(query)
        except Exception as e:
            if self.table_exists(self.rounds_table):
                raise Exception(e)
            return []

    def get_round_applications(self, chain):
        """ """
        try:
            query = f"""
                SELECT *
                FROM {self.applications_table}
                WHERE chain = '{chain}'
            """
            return self.read(query)
        except Exception as e:
            if self.table_exists(self.applications_table):
                raise Exception(e)
            return []

    def get_round_contracts_with_abi(self, chain):
        """ """
        round_contracts = self.get_round_contracts(chain)
        contract_abi = ContractManager().get_abis(contract_type="round_implementation")[0]

        contracts = []
        for c in round_contracts:
            contracts.append(
                {
                    "address": c["round_address"],
                    "chain": c["chain"],
                    "contract_type": "round_implementation",
                    "abi": contract_abi["abi"],
                }
            )

        return contracts

    def get_voting_contracts_with_abi(self, chain, round_address):
        """ """
        round_contracts = self.get_round_contracts(chain)
        contract_abi = ContractManager().get_abis(contract_type="voting_strategy")[0]

        return [
            {
                "address": c["voting_strategy"],
                "chain": c["chain"],
                "contract_type": "voting_strategy",
                "abi": contract_abi["abi"],
            }
            for c in round_contracts
            if c["round_address"] == round_address
        ]

    def get_grant_wallets(self, chain):
        """ """
        query = f"""
        SELECT 
            DISTINCT LOWER(recipient) AS wallets
        FROM 
            sybilscan.datastore.round_applications
        WHERE
            chain = '{chain}'
        """
        return self.read(query)
        

    def get_voter_wallets(self,chain):
        """ """
        query = f"""
        SELECT 
            DISTINCT LOWER(voter) AS wallets
        FROM 
            sybilscan.datastore.votes
        WHERE
            chain = '{chain}'
        """
        return self.read(query)

    def get_round_applications_with_votes(self,chain):
        """ """
        query = f"""
        WITH votes_count AS (
            SELECT 
                LOWER(v.round_address) AS round_address,
                v.app_index,
                COUNT(*) AS num_votes,
                COUNT(DISTINCT voter) AS num_contributors,
                SUM(CASE WHEN v.token = 'ETH' THEN v.amount
                ELSE 0
                END) AS amount_contributed_eth,
                SUM(CASE WHEN v.token = 'DAI' THEN v.amount
                ELSE 0
                END) AS amount_contributed_dai
            FROM datastore.votes v
            WHERE v.chain ='{chain}'
            GROUP BY 
                round_address, 
                app_index
        )
        SELECT 
            ra.round_address,
            ra.app_index,
            ra.project_id,
            ra.recipient,
            ra.title,
            ra.project_twitter, 
            vc.num_votes,
            vc.num_contributors,
            vc.amount_contributed_eth,
            vc.amount_contributed_dai
        FROM votes_count vc
        INNER JOIN datastore.round_applications ra
            ON LOWER(vc.round_address) = LOWER(ra.round_address)
            AND ra.app_index = vc.app_index
        """
        return self.read(query)


    def get_votes(self,chain):
        """ """
        query = f"""
        SELECT 
            round_address,
            app_index,
            voter,
            token,
            amount
        FROM datastore.votes
        """
        return self.read(query)

    def update_rounds(self, rounds):
        """
        Persists the contract information
        """
        self.update(
            rows=rounds,
            table_name=self.rounds_table,
            matching_cols=["round_address", "chain"],
        )

    def update_applications(self, round_applications):
        """ """
        self.update(
            rows=round_applications,
            table_name=self.applications_table,
            matching_cols=["round_address", "app_index", "chain"],
        )

    def update_programs(self, programs):
        """ """
        self.update(
            rows=programs,
            table_name=self.programs_table,
            matching_cols=["program_address", "chain"],
        )

    def update_votes(self, votes):
        """ """
        self.create_and_append(
            rows=votes, table_name=self.votes_table, if_exists="append"
        )
