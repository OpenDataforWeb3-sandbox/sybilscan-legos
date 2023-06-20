from dataprovider.rpc import RPCProvider
from web3 import Web3

def get_web3_rpc_provider(provider_uri, chain):
    """
    """
    if chain == 'ethereum':
         return RPCProvider(provider_uri, chain)
    else:
        raise ValueError('web3 provider not configured for {}'.format(chain))

def get_keccak_hash(addresses):
    """ """
    return Web3.keccak(text=''.join(sorted(addresses))).hex()