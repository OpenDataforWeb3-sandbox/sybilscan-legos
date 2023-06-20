import config
import logging
import os
from db import event_manager
from base_job import BaseBlockJob

logger = logging.getLogger(__name__)


class EventsExtractor(BaseBlockJob):
    """
    Extracts Allo Events and Saves it to DB
    """

    def __init__(self, job_id, chain, contracts, dependent_jobids=[]):
        """
        Sets job parameters
        """
        #SET the contract details whose events needs to be extracted
        self.target_contracts = contracts
        super().__init__(job_id=job_id, chain=chain, dependent_jobids=dependent_jobids, batch_size=1000000)

    def update_state(self, event_updates):
        """
        updates data to DB
        """
        for event_info in event_updates:
            event_key = event_info[0]
            events = event_info[1]
            event_manager.update_events(event_key, events)

    def reset_state(self):
        """ """
        for c in self.target_contracts:
            event_manager.clear(c['address'])

    def process_data(self, start_block, end_block):
        """
        Index the events for the Project Registry
        """

        logs = self.web3_provider.get_logs(
            self.target_contracts, start_block, end_block
        )
        logger.info(f"{len(logs)} allo logs extracted")

        event_updates = {}
        for log in logs:
            if log is not None:
                contract_type = [c['contract_type'] for c in self.target_contracts if c['address']==log['address']][0]
                event_key = f'{self.chain}_{contract_type}_{log["eventName"]}'
                event_updates.setdefault(event_key, []).append(log)

        return list(event_updates.items())
