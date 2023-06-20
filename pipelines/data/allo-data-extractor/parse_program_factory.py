import logging
import json
from base_job import BaseBlockJob
from db import grant_manager, event_manager
from utils.ipfs import get_json_from_ipfs
from utils.common import parse_methods

logger = logging.getLogger(__name__)

class ProgramFactoryParser(BaseBlockJob):
    """
    Class to parse program factory contract events
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

        # READ the program factory contract info
        self.target_contracts = [
            c
            for c in self.contracts
            if c["contract_type"] == "program_factory" and c["chain"] == chain
        ]

        # SET the state
        self.programs = []

    def update_state(self, programs):
        """
        updates data to DB
        """
        grant_manager.update_programs(programs)

    def reset_state(self):
        """ """
        grant_manager.clear_programs_state()

    def process_data(self, start_block, end_block):
        """
        Parses ProgramFactory contract to extract information about new programs
        """
        #SET the state for this run
        self.programs = []

        event_logs = event_manager.get_events(
            self.target_contracts,
            self.events,
            start_block,
            end_block,
        )

        for event in event_logs:
            event_handler = getattr(self, "parse_{}".format(event["eventName"]))
            event_handler(event)
            
        new_programs = [p for p in self.programs if p['last_updated_block']>= start_block]

        return new_programs

    def parse_ProgramCreated(self, event):
        """ """
        program_contract = self.web3_provider.bind_contract(
            event["programContractAddress"],
            self.abis['program_implementation'],
        )
        meta_ptr = program_contract.functions.metaPtr().call(
            block_identifier=event["blockNumber"]
        )
        
        metaptr_protocol = meta_ptr[0]
        if metaptr_protocol != 1:
            raise ValueError('Protocol not implemented')
        metaptr_pointer = meta_ptr[1]
        program_info = get_json_from_ipfs(metaptr_pointer)

        new_program = {
            'program_name': program_info['name'] if 'name' in program_info else '',
            'program_address': event["programContractAddress"].lower(),
            'chain' : self.chain,
            'program_info': json.dumps(program_info),
            'last_updated_block': event['blockNumber']
        }

        self.programs.append(new_program)