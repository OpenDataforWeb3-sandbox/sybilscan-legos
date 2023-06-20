import logging
import json
from base_job import BaseBlockJob
from db import grant_manager, event_manager
from utils.ipfs import get_json_from_ipfs
from utils.common import timestamp_to_utc_string, parse_methods

logger = logging.getLogger(__name__)

class VotingParser(BaseBlockJob):
    """
    Class to parse round contract events
    """

    def __init__(self, job_id, chain, contracts, dependent_jobids):
        """
        Sets job parameters
        """
        super().__init__(
            job_id=job_id,
            chain=chain,
            dependent_jobids=dependent_jobids,
            batch_size=1000000,
        )

        # SET the supported events
        self.events = parse_methods(self, pattern='parse_')

        # READ the round factory contract info
        self.target_contracts = contracts

        #SET the state of this instance
        self.votes = []

    def update_state(self, state_updates):
        """
        updates data to DB
        """
        grant_manager.update_votes(state_updates)

    def reset_state(self):
        """ """
        #grant_manager.clear_votes_state()
        pass

    def process_data(self, start_block, end_block):
        """
        Index the events for the Project Registry
        """

        #SET the state for this run
        self.votes = []

        event_logs = event_manager.get_events(
            self.target_contracts,
            self.events,
            start_block,
            end_block,
        )

        for event in event_logs:
            event_handler = getattr(self, "parse_{}".format(event["eventName"]))
            event_handler(event)

        return self.votes

    def parse_Voted(self, event):
        """ 
        handler for voting strategy contract event Voted
        """
        
        self.votes.append({
            'chain': self.chain,
            'round_address': event['roundAddress'].lower(),
            'app_index': int(event['applicationIndex']),
            'voter': event['voter'].lower(),
            'token': self.tokens[event['token'].lower()],
            'amount': int(event['amount'])/1e18,
            'recipient': event['grantAddress'].lower(),
            'last_updated_block': event['blockNumber']
        })
