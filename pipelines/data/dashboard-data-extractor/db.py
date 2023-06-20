from datastore.bq.app import AppManager
from datastore.bq.grants import GrantManager
from datastore.sb.dashboard import DashboardAppManager
from datastore import DataStoreManager
from config import DB_CONNECTION_URI, DB_CONNECTION_API_CREDENTIALS, DB_CONNECTION_DB,SUPABASE_KEY, SUPABASE_URL

# Setup data store to load the extracted data
DataStoreManager.initialize(
    db_connection_uri=DB_CONNECTION_URI,
    db_api_credentials_json_path=DB_CONNECTION_API_CREDENTIALS,
    db=DB_CONNECTION_DB,
)

app_manager = AppManager()
grant_manager = GrantManager()
dashboard_manager = DashboardAppManager(supabase_key=SUPABASE_KEY, supabase_url=SUPABASE_URL)