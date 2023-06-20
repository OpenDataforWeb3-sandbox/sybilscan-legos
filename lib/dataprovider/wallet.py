import config
import logging
import pandas as pd
import sys
from shroomdk import ShroomDK
from utils.common import split
from utils.web3 import get_keccak_hash
import uuid
import os

logger = logging.getLogger(__name__)

class WalletInsights:
    """
    A class to provide insights about wallet
    """

    def __init__(self, api_key):
        """ """
        self.chunk_size = 10000
        self.flipside = ShroomDK(api_key)

    def run_query(self, query):
        """
        function to run a query and retrieve the records using flipside sdk
        """

        #with open('flipquery_' + str(uuid.uuid4()), "w") as file:
        #    file.write(query)
        #return []
        response = self.flipside.query(query,timeout_minutes=15)
        logger.info(f'{response.run_stats.record_count} fetched in {response.run_stats.elapsed_seconds} seconds ')
        return response.records

    def erc20_assets_count_gte(self, wallets, value):
        """
        counts the unique assets of given wallet address and return Y/N if it matches the criteria
        """

        logger.info("executing query: {}".format(sys._getframe().f_code.co_name))
        user_erc20_assets = []

        for wallets_chunk in split(wallets, self.chunk_size):

            wallets_concatenated= ",".join(["'" + w.lower() + "'" for w in wallets_chunk])
            query_num_assets = """
            SELECT
                user_address AS wallet,
                COUNT(DISTINCT contract_address) AS num_assets
            FROM
                ethereum.core.fact_token_balances
            WHERE
                user_address IN (%s)
            GROUP BY
                user_address
                HAVING num_assets >= %d
            """ % ( wallets_concatenated, value )

            # query_name = f"erc20_assets_count_gte_{get_keccak_hash(wallets_chunk)}"
            # query_result = f"{query_name}_result"
            # if os.path.exists(query_result):
            #     pass
            # else:
            #     with open(query_name, "w") as file:
            #         file.write(query)
            #     return []

            query_records = self.run_query(query_num_assets)
            user_erc20_assets +=  query_records if query_records != None else []

        return user_erc20_assets


    def eth_transactions_value_lte(self, wallets, value):
        """
        counts the unique assets of given wallet address and return Y/N if it matches the criteria
        """
        logger.info("executing query: {}".format(sys._getframe().f_code.co_name))

        wallets_with_less_avg_value = []
        for wallets_chunk in split(wallets, self.chunk_size):

            wallets_concatenated= ",".join(["'" + w.lower() + "'" for w in wallets_chunk])
            query_avg_tx_value = """
            SELECT
                from_address AS wallet
            FROM
                ethereum.core.fact_transactions
            WHERE
                from_address IN (%s)
            GROUP BY
                from_address
            HAVING 
                AVG(eth_value) < %f
            """ % ( wallets_concatenated, value )

            # query_name = f"eth_transactions_value_lte_{get_keccak_hash(wallets_chunk)}"
            # query_result = f"{query_name}_result"
            # if os.path.exists(query_result):
            #     pass
            # else:
            #     with open(query_name, "w") as file:
            #         file.write(query)
            #     return []

            query_records = self.run_query(query_avg_tx_value)
            wallets_with_less_avg_value +=  query_records if query_records != None else []

        return wallets_with_less_avg_value


    def eth_wallet_age_gte(self, wallets, value):
        """
        check the age of given wallet address and return Y/N if it matches the criteria
        """
        
        logger.info("executing query: {}".format(sys._getframe().f_code.co_name))

        wallet_age = []
        for wallets_chunk in split(wallets, self.chunk_size):

            wallets_concatenated= ",".join(["'" + w.lower() + "'" for w in wallets_chunk])
            query_wallet_age = """
            SELECT
                from_address AS wallet
            FROM
                ethereum.core.fact_transactions
            WHERE
                from_address IN (%s)
            GROUP BY
                from_address
            HAVING
                MIN(block_timestamp) < CURRENT_DATE - INTERVAL '%d YEARS'
            """ % ( wallets_concatenated, value )

            # query_name = f"eth_wallet_age_gte_{get_keccak_hash(wallets_chunk)}"
            # query_result = f"{query_name}_result"
            # if os.path.exists(query_result):
            #     pass
            # else:
            #     with open(query_name, "w") as file:
            #         file.write(query)
            #     return []

            query_records = self.run_query(query_wallet_age)
            wallet_age +=  query_records if query_records != None else []

        return wallet_age


    def interact_with_contracts_call_from_wallet(self, wallets, contracts):
        """
        check if the wallets have interacted with the given address
        """

        logger.info("executing query: {}".format(sys._getframe().f_code.co_name))
        contracts_concatenated = ",".join(["'" + c.lower() + "'" for c in contracts])

        interacted_wallets = []
        for wallets_chunk in split(wallets, self.chunk_size):

            wallets_concatenated= ",".join(["'" + w.lower() + "'" for w in wallets_chunk])
            query_contract_interactions_from_wallet = """
            SELECT
                DISTINCT from_address AS wallet
            FROM
                ethereum.core.fact_transactions
            WHERE
                from_address IN (%s)
                AND to_address IN (%s)
            """ % ( wallets_concatenated, contracts_concatenated)

            # query_name = f"iwc_cfw_{get_keccak_hash(wallets_chunk)}"
            # query_result = f"{query_name}_result"
            # if os.path.exists(query_result):
            #     pass
            # else:
            #     with open(query_name, "w") as file:
            #         file.write(query)
            #     return []

            query_records = self.run_query(query_contract_interactions_from_wallet)
            interacted_wallets +=  query_records if query_records != None else []

        return interacted_wallets

    def interact_with_contracts_call_to_wallet_eth(self, wallets, contracts):
        """
        check if the wallets have interacted with the given address
        """

        logger.info("executing query: {}".format(sys._getframe().f_code.co_name))
        contracts_concatenated = ",".join(["'" + c.lower() + "'" for c in contracts])

        interacted_wallets = []
        for wallets_chunk in split(wallets, self.chunk_size):

            wallets_concatenated= ",".join(["'" + w.lower() + "'" for w in wallets_chunk])
            query_contract_interactions_from_wallet = """
            SELECT
                DISTINCT eth_to_address AS wallet
            FROM
                ethereum.core.ez_eth_transfers
            WHERE
                eth_from_address IN ( %s )
                AND eth_to_address IN (%s )
            """ % ( contracts_concatenated, wallets_concatenated )

            # query_name = f"iwc_ctw_{get_keccak_hash(wallets_chunk)}"
            # query_result = f"{query_name}_result"
            # if os.path.exists(query_result):
            #     pass
            # else:
            #     with open(query_name, "w") as file:
            #         file.write(query)
            #     return []

            query_records = self.run_query(query_contract_interactions_from_wallet)
            interacted_wallets +=  query_records if query_records != None else []

        return interacted_wallets

    def interact_with_contracts_call_to_wallet_erc20(self, wallets, contracts):
        """
        check if the wallets have interacted with the given address
        """

        logger.info("executing query: {}".format(sys._getframe().f_code.co_name))
        contracts_concatenated = ",".join(["'" + c.lower() + "'" for c in contracts])

        interacted_wallets = []
        for wallets_chunk in split(wallets, self.chunk_size):

            wallets_concatenated= ",".join(["'" + w.lower() + "'" for w in wallets_chunk])
            query_contract_interactions = """
            SELECT
                to_address AS wallet
            FROM
                ethereum.core.ez_token_transfers
            WHERE
                from_address IN (%s)
                AND to_address IN (%s) 
                
            """ % ( contracts_concatenated, wallets_concatenated )


            # query_name = f"iwc_ctw_e20_{get_keccak_hash(wallets_chunk)}"
            # query_result = f"{query_name}_result"
            # if os.path.exists(query_result):
            #     pass
            # else:
            #     with open(query_name, "w") as file:
            #         file.write(query)
            #     return []

            query_records = self.run_query(query_contract_interactions)
            interacted_wallets +=  query_records if query_records != None else []

        return interacted_wallets


    def interact_with_contracts(self, wallets, value):
        """
        check if the wallets have interacted with the given address
        """

        logger.info("executing query: {}".format(sys._getframe().f_code.co_name))
        interacted_wallets = self.interact_with_contracts_call_from_wallet(wallets, value)
        interacted_wallets += self.interact_with_contracts_call_to_wallet_eth(wallets, value)
        interacted_wallets += self.interact_with_contracts_call_to_wallet_erc20(wallets, value)

        return interacted_wallets
