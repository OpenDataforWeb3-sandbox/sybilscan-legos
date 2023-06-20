from web3 import Web3, HTTPProvider
import logging
from web3.datastructures import AttributeDict
import re

logger = logging.getLogger("RPCProvider")


class RPCProvider:
    def __init__(self, rpc_provider_uri, contracts=None, chain="ethereum"):
        """
        Initialize the RPC provider
        """
        if chain == "ethereum":
            self.num_blocks_to_finalize = 6
        self.web3 = self._get_web3(rpc_provider_uri)


    def _get_web3(self, rpc_provider_uri):
        """ """
        for i in range(3):
            web3 = Web3(Web3.HTTPProvider(rpc_provider_uri))
            if web3.is_connected():
                return web3
            logger.info("web3 request failed, retrying")
            time.sleep(60)

        raise ValueError("Unable to establish connection with {}".format(rpc_provider_uri))

    def _get_event_signature(self, event):
        """ """
        args = []
        for input in event.get("inputs", []):
            if "components" in input:
                args.append(
                    "("
                    + ",".join(component["type"] for component in input["components"])
                    + ")"
                )
            else:
                args.append(input["type"])

        event_signature = event["name"] + "(" + ",".join(args) + ")"
        return event_signature

    def _get_event_parser(self, address, abi):
        """
        Extracts all the events from abi and generates event parser object
        """
        contract = self.web3.eth.contract(
            address=self.web3.to_checksum_address(address), abi=abi
        )
        event_parser = {}

        for item in abi:
            if item["type"] == "event":

                event_hash = Web3.keccak(text=self._get_event_signature(item)).hex()
                event_parser[event_hash] = {
                    "name": item["name"],
                    "parser": contract.events[item["name"]](),
                }

        return event_parser

    def _get_parsed_log(self, event_parser, log):
        """ """

        event_hash = log.topics[0].hex()
        contract_address = log.address.lower()

        curr_event_parser = None
        if (
            contract_address in event_parser
            and event_hash in event_parser[contract_address]
        ):
            curr_event_parser = event_parser[contract_address][event_hash]
        else:
            logger.info(
                f"event {event_hash} in contract {contract_address} not configured for parsing. check abi"
            )
            return None

        processed_log = curr_event_parser["parser"].process_log(log)

        # CONVERT types
        parsed_log = processed_log.args.__dict__
        expanded_items = []
        
        #TODO: a better way
        for arg_name, arg_value in dict(parsed_log).items():
            if isinstance(arg_value, bytes):
                parsed_log[arg_name] = arg_value.hex()
            if isinstance(arg_value, int):
                parsed_log[arg_name] = str(arg_value)
            if isinstance(arg_value, AttributeDict):
                for k,v in arg_value.__dict__.items():
                    parsed_log[arg_name+"_"+k] = v
                del parsed_log[arg_name] #
        
        # ADD additional info
        parsed_log["transactionHash"] = processed_log["transactionHash"].hex()
        parsed_log["transactionIndex"] = str(processed_log["transactionIndex"])
        parsed_log["logIndex"] = str(processed_log["logIndex"])
        parsed_log["blockNumber"] = processed_log["blockNumber"]
        parsed_log["eventName"] = curr_event_parser["name"]
        parsed_log["address"] = contract_address

        return parsed_log

    def get_logs(self, contracts, start_block, end_block):
        """
        retrieve logs for the given range
        if fails, reduce the block range size
        """

        # Initialize the event parser to parse the events
        event_parser = {
            c["address"]: self._get_event_parser(c["address"], c["abi"])
            for c in contracts
        }

        addresses = [self.web3.to_checksum_address(c["address"]) for c in contracts]

        parsed_logs = []

        retry_attempt = 0
        batch_start_block = start_block
        batch_size = end_block - start_block + 1

        while batch_start_block <= end_block:

            batch_end_block = min(batch_start_block + batch_size - 1, end_block)

            events = {
                "fromBlock": batch_start_block,
                "toBlock": batch_end_block,
                "address": addresses,
            }

            try:
                raw_logs = self.web3.eth.get_logs(events)
                parsed_logs += [
                    self._get_parsed_log(event_parser, log) for log in raw_logs
                ]

            except ValueError as e:

                if retry_attempt > 3:
                    raise ValueError("get logs failed: max retry exceeded")

                # this is applicable only for alchemy
                block_range_pattern = r"\[(0x[a-fA-F\d]+), (0x[a-fA-F\d]+)\]"
                match = re.search(block_range_pattern, str(e))

                if match:
                    batch_size = int(match.group(2), 16) - int(match.group(1), 16)
                    logger.info(f'batch_size reduced to {batch_size}')

                retry_attempt += 1
                continue

            batch_start_block = batch_end_block + 1
            batch_size = batch_size * 2
            retry_attempt = 0

        return parsed_logs

    """
    def get_events(self, address, abi):  
        contract = self.web3.eth.contract(address=address, abi=abi)
        #print(contract.events)
    """

    def bind_contract(self, address, abi):
        """
        returns the contract object
        """
        contract = self.web3.eth.contract(address=address, abi=abi)
        return contract

    def get_finalized_block(self):
        """
        Returns the latest finalized block for the given chain
        """
        return self.web3.eth.get_block_number() - self.num_blocks_to_finalize
