import config
import logging
import os

from utils.common import load_json
from utils.web3 import get_web3_rpc_provider
from db import job_manager, contract_manager, event_manager, token_manager
from web3 import Web3

logger = logging.getLogger(__name__)


class BaseBlockJob:
    """
    Base class for jobs that parses on chain data
    """

    def __init__(self, job_id, chain, dependent_jobids = [], batch_size=100000):
        """
        Sets job parameters
        """

        self.job_id = job_id
        self.web3_provider = get_web3_rpc_provider(config.RPC_PROVIDER_URI[chain], chain)
        self.chain = chain
        self.dependent_jobids = dependent_jobids
        self.batch_size = batch_size

        # SETUP contract
        self.contracts = contract_manager.get_contracts(chain)
        self.tokens = token_manager.get_tokens()
        self.abis = {item['contract_type']: item['abi'] for item in  contract_manager.get_abis()}

    def get_from_block(self):
        """ """
        return min([c['start_block'] for c in self.contracts])

    def get_to_block(self):
        """ """
        if self.dependent_jobids:
            return min([job_manager.get_last_synced_block(j) for j in self.dependent_jobids])
        else:
            return self.web3_provider.get_finalized_block()

    def get_block_range(self):
        """ """
        last_synced_block = job_manager.get_last_synced_block(self.job_id)
        start_block = (
            last_synced_block + 1
            if last_synced_block is not None
            else self.get_from_block()
        )
        end_block = self.get_to_block()

        return start_block, end_block

    def update_state(self, updates):
        """
        This needs to be handled in the child class
        """
        raise Exception('Implement update_store_state in child class')

    def reset(self):
        """
        """
        job_manager.remove_job_state(self.job_id)
        self.reset_state()

    def reset_state(self, updates):
        """
        This needs to be handled in the child class to remove specific state
        """
        raise Exception('Implement reset_state in child class')

    def run(self):
        """
        Triggers the job as per given batch size & maintains job sync information
        """

        start_block, end_block = self.get_block_range()
        logger.info(f'{self.chain}:{self.job_id}:Base Job processing data from {start_block}-{end_block}')

        if start_block > end_block:
            return

        batch_start_block = start_block
        for batch_start_block in range(start_block, end_block + 1, self.batch_size):
            batch_end_block = min(batch_start_block + self.batch_size - 1, end_block)
            logger.info(f'{self.chain}:{self.job_id}:Job processing data in batch from {batch_start_block}-{batch_end_block}:{end_block}')
            updates = self.process_data(batch_start_block, batch_end_block)
            self.update_state(updates)
            job_manager.update_sync_status(self.job_id, batch_start_block, batch_end_block)
