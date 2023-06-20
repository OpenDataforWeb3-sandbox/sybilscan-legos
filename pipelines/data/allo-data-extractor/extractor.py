import time
import fire
import logging
import json
import os

from events_extractor import EventsExtractor
from db import contract_manager, token_manager, grant_manager
from utils.common import load_json, list_files_ending_with
from parse_program_factory import ProgramFactoryParser
from parse_round_factory import RoundFactoryParser
from parse_round_contract import RoundParser
from parse_voting_contract import VotingParser
from config import CONTRACTS_ABI_PATH, CONTRACTS_INFO_PATH, TOKENS_INFO_PATH

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def load_contract_data_from_config():
    """
    Returns contracts info from json
    """

    # Load json file
    with open(CONTRACTS_INFO_PATH, "r") as f:
        contract_info_data = f.read()
    contract_info = json.loads(contract_info_data)

    # Create Contracts object
    contracts = [
        {
            "address": contract["address"].lower(),
            "chain": contract["chain"],
            "contract_type": contract_type,
            "start_block": contract["start_block"],
        }
        for contract_type, contract_val in contract_info.items()
        for contract in contract_val
    ]

    abis = [
        {
            "contract_type": file_name.split("/")[-1].split(".")[0],
            "abi": json.dumps(load_json(file_name)),
        }
        for file_name in list_files_ending_with(CONTRACTS_ABI_PATH, "json")
    ]

    contract_manager.update_contracts(contracts)
    contract_manager.update_abis(abis)


def load_token_data_from_config():
    """
    Returns contracts info from json
    """

    # Load json file
    with open(TOKENS_INFO_PATH, "r") as f:
        token_info_data = f.read()
    token_info = json.loads(token_info_data)

    # Create Contracts object
    tokens = [
        {"token_address": token_address.lower(), "token_symbol": token_symbol}
        for token_address, token_symbol in token_info.items()
    ]

    token_manager.clear()
    token_manager.update_tokens(tokens)


def run_allo_events_extractor(chain):
    """ """
    contracts = contract_manager.get_contracts(chain)
    event_extractor_job_id = f"allo_events_{chain}"
    extractor = EventsExtractor(
        job_id=event_extractor_job_id, chain=chain, contracts=contracts
    )
    extractor.run()
    return event_extractor_job_id


def run_program_factory_parser(chain, dependent_jobids):
    """ """
    program_factory_parser_job_id = f"program_factory_events_parser_{chain}"
    program_factory_parser = ProgramFactoryParser(
        job_id=program_factory_parser_job_id,
        chain=chain,
        dependent_jobids=dependent_jobids,
    )

    program_factory_parser.run()
    return program_factory_parser_job_id


def run_round_factory_parser(chain, dependent_jobids):
    """ """
    round_factory_parser_job_id = f"round_factory_events_parser_{chain}"
    round_factory_parser = RoundFactoryParser(
        job_id=round_factory_parser_job_id,
        chain=chain,
        dependent_jobids=dependent_jobids,
    )
    round_factory_parser.run()
    return round_factory_parser_job_id


def run_round_events_extractor(chain, round_contract, dependent_jobids):
    """ """
    round_events_job_id = f"round_events_{chain}_{round_contract['address']}"
    round_events_extractor = EventsExtractor(
        job_id=round_events_job_id,
        chain=chain,
        contracts=[round_contract],
        dependent_jobids=dependent_jobids,
    )
    round_events_extractor.run()
    return round_events_job_id


def run_round_events_parser(chain, round_contract, dependent_jobids):
    """ """
    round_parser_job_id = f"round_parser_{chain}_{round_contract['address']}"
    round_events_parser = RoundParser(
        job_id=round_parser_job_id,
        chain=chain,
        contracts=[round_contract],
        dependent_jobids=dependent_jobids,
    )
    round_events_parser.run()
    return round_parser_job_id


def run_voting_events_extractor(chain, round_contract, dependent_jobids):
    """ """
    voting_events_job_id = f"voting_events_{chain}_{round_contract['address']}"
    voting_contracts = grant_manager.get_voting_contracts_with_abi(
        chain, round_contract["address"]
    )
    voting_events_extractor = EventsExtractor(
        job_id=voting_events_job_id,
        chain=chain,
        contracts=voting_contracts,
        dependent_jobids=dependent_jobids,
    )
    voting_events_extractor.run()
    return voting_events_job_id


def run_voting_events_parser(chain, round_contract, dependent_jobids):
    """ """
    voting_parser_job_id = f"voting_parser_{chain}_{round_contract['address']}"
    voting_contracts = grant_manager.get_voting_contracts_with_abi(
        chain, round_contract["address"]
    )
    voting_events_parser = VotingParser(
        job_id=voting_parser_job_id,
        chain=chain,
        contracts=voting_contracts,
        dependent_jobids=dependent_jobids,
    )
    voting_events_parser.reset()
    voting_events_parser.run()
    return voting_parser_job_id


def run_extractor():
    """
    Extracts the Allo Protocol data to the given datastore
        - Google Bigquery (supported)
    """

    #load_contract_data_from_config()
    #load_token_data_from_config()

    for c in contract_manager.get_chains():

        chain = c["chain"]

        if chain != "ethereum":
            continue

        # GET all programs and rounds information
        #evt_job_id = run_allo_events_extractor(chain)
        #pf_job_id = run_program_factory_parser(chain, dependent_jobids=[evt_job_id])
        #rf_job_id = run_round_factory_parser(chain, dependent_jobids=[pf_job_id])

        # For each round, retrieve application information and voting information
        round_contracts = grant_manager.get_round_contracts_with_abi(chain)

        for i in range(len(round_contracts)):

            round_contract = round_contracts[i]
            logger.info(
                f"processing data for round {round_contract['address']} : {i}\{len(round_contracts)}"
            )

            """
            r_evt_job_id = run_round_events_extractor(
                chain, round_contract, dependent_jobids=[rf_job_id]
            )
            
            
            r_parser_job_id = run_round_events_parser(
                chain, round_contract, dependent_jobids=[r_evt_job_id]
            )
            
            v_evt_job_id = run_voting_events_extractor(
                chain, round_contract, dependent_jobids=[r_parser_job_id]
            )
            """
            v_evt_job_id = f"voting_events_{chain}_{round_contract['address']}"
            v_parser_job_id = run_voting_events_parser(
                chain, round_contract, dependent_jobids=[v_evt_job_id]
            )
            

        logger.info("job completed")


if __name__ == "__main__":
    fire.Fire(run_extractor)
