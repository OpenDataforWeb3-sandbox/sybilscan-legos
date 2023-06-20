from datastore.bq.contracts import ContractManager
from datastore.bq.events import EventManager
from datastore.bq.grants import GrantManager
from datastore.bq.jobs import JobManager
from datastore.bq.tokens import TokenManager

from datastore import DataStoreManager
from config import DB_CONNECTION_URI, DB_CONNECTION_API_CREDENTIALS, DB_CONNECTION_DB

# Setup data store to load the extracted data
DataStoreManager.initialize(
    db_connection_uri=DB_CONNECTION_URI,
    db_api_credentials_json_path=DB_CONNECTION_API_CREDENTIALS,
    db=DB_CONNECTION_DB,
)

contract_manager = ContractManager()
event_manager = EventManager()
job_manager = JobManager()
token_manager = TokenManager()
grant_manager = GrantManager()