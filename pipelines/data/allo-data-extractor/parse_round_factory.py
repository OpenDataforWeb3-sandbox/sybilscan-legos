import logging
import json
from base_job import BaseBlockJob
from db import grant_manager, event_manager
from utils.ipfs import get_json_from_ipfs
from utils.common import timestamp_to_utc_string, parse_methods

logger = logging.getLogger(__name__)

class RoundFactoryParser(BaseBlockJob):
    """
    Class to parse round factory contract events
    """

    def __init__(self, job_id, chain, dependent_jobids):
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
        self.target_contracts = [
            c
            for c in self.contracts
            if c["contract_type"] == "round_factory" and c["chain"] == chain
        ]

        # SET the state
        self.rounds = []

    def update_state(self, rounds):
        """
        updates data to DB
        """
        grant_manager.update_rounds(rounds)

    def reset_state(self):
        """ """
        grant_manager.clear_rounds_state()

    def process_data(self, start_block, end_block):
        """
        Index the events for the Project Registry
        """

        # SET the state for this run
        self.rounds = []

        event_logs = event_manager.get_events(
            self.target_contracts,
            self.events,
            start_block,
            end_block,
        )

        for event in event_logs:
            event_handler = getattr(self, "parse_{}".format(event["eventName"]))
            event_handler(event)    

        new_rounds = [r for r in self.rounds if r['last_updated_block']>= start_block]

        return new_rounds

    def parse_RoundCreated(self, event):
        """ """

        round_contract = self.web3_provider.bind_contract(
            event["roundAddress"],
            self.abis['round_implementation'],
        )

        token = round_contract.functions.token().call(block_identifier=event["blockNumber"])
        application_start_time = round_contract.functions.applicationsStartTime().call(block_identifier=event["blockNumber"])
        application_end_time = round_contract.functions.applicationsEndTime().call(block_identifier=event["blockNumber"])
        round_start_time = round_contract.functions.roundStartTime().call(block_identifier=event["blockNumber"])
        round_end_time = round_contract.functions.roundEndTime().call(block_identifier=event["blockNumber"])
        round_metaptr = round_contract.functions.roundMetaPtr().call(block_identifier=event["blockNumber"])
        program_contract = event["ownedBy"]
        payout_strategy = round_contract.functions.payoutStrategy().call(block_identifier=event["blockNumber"])
        voting_strategy = round_contract.functions.votingStrategy().call(block_identifier=event["blockNumber"])

        match_amount = round_contract.functions.matchAmount().call(block_identifier=event["blockNumber"]);
        fee_percentage = round_contract.functions.roundFeePercentage().call(block_identifier=event["blockNumber"]);
        fee_address = round_contract.functions.roundFeeAddress().call(block_identifier=event["blockNumber"])

        metaptr_protocol = round_metaptr[0]
        if metaptr_protocol != 1:
            raise ValueError('Protocol not implemented')
        metaptr_pointer = round_metaptr[1]
        round_info = get_json_from_ipfs(metaptr_pointer)

        new_round = {
            'round_name': round_info['name'] if round_info is not None and 'name' in round_info else '',
            'round_address': event["roundAddress"].lower(),
            'chain': self.chain,
            'token': self.tokens[token.lower()],
            'application_start_time': timestamp_to_utc_string(application_start_time),
            'application_end_time': timestamp_to_utc_string(application_end_time),
            'round_start_time': timestamp_to_utc_string(round_start_time),
            'round_end_time': timestamp_to_utc_string(round_end_time),
            'program_contract': program_contract.lower(),
            'payout_strategy': payout_strategy.lower(),
            'voting_strategy': voting_strategy.lower(),
            'match_amount': int(match_amount)/1e18,
            'fee_percentage': fee_percentage * 1.0,
            'fee_address': fee_address,
            'round_info': json.dumps(round_info),
            'last_updated_block': event['blockNumber']
        }

        self.rounds.append(new_round)
