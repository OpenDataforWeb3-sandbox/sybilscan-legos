import requests
import logging
import json
import time
import random

from itertools import cycle
logger = logging.getLogger(__name__)


IPFS_GATEWAYS = ["https://gateway.pinata.cloud", "https://cloudflare-ipfs.com"]


def get_json_from_ipfs(file_hash):
    """
    reads ipfs using one of the gateways
    returns json data
    """
    random.shuffle(IPFS_GATEWAYS)
    gateway_iterator = cycle(IPFS_GATEWAYS)

    for i in range(5):
        try:
            file_path = "{}/ipfs/{}".format(next(gateway_iterator), file_hash)
            logger.info(file_path)

            response = requests.get(file_path)

            if response.ok:
                return response.json()

        except Exception as e:
            pass
        
        logger.info('ipfs request failed. retrying')
        time.sleep(i*5)

    return {}
