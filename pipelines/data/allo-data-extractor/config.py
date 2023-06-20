import os

#ENV VARIABLES
DB_CONNECTION_URI = os.getenv('DB_CONNECTION_URI','')
DB_CONNECTION_API_CREDENTIALS = os.getenv('DB_CONNECTION_API_CREDENTIALS','') #applicable only for bigquery connection
DB_CONNECTION_DB = 'datastore'

RPC_PROVIDER_URI = {
    'ethereum' : os.getenv('ETHEREUM_RPC_PROVIDER_URI','')
}

CONTRACTS_INFO_PATH = 'files/contracts.json'
TOKENS_INFO_PATH = 'files/tokens.json'
CONTRACTS_ABI_PATH = 'files/abi'