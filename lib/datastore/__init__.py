from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import logging
from google.oauth2 import service_account

logger = logging.getLogger(__name__)


class DataStoreManager:
    """
    Initializes the Database Engine & Session
    """

    engine = None
    credentials = None
    db=None

    @staticmethod
    def initialize(**store_args):
        """
        Initializes the Database Store
        """

        if "db_connection_uri" in store_args:

            if "bigquery" in store_args["db_connection_uri"]:
                DataStoreManager.engine = create_engine(
                    store_args["db_connection_uri"],
                    credentials_path=store_args["db_api_credentials_json_path"],
                )
                # required for pandas gbq
                DataStoreManager.credentials = (
                    service_account.Credentials.from_service_account_file(
                        store_args["db_api_credentials_json_path"]
                    )
                )

                DataStoreManager.db = store_args["db"]
            else:
                raise ValueError("DB not supported")

        else:
            raise ValueError("Data store connection info missing")
