import os
from supabase import create_client
from utils.common import split
import logging

logger = logging.getLogger(__name__)

class DashboardAppManager:
    def __init__(self, supabase_key, supabase_url):
        """ """
        self.supabase_client = create_client(supabase_url, supabase_key)
        self.chunk_size = 10000  # to avoid timeout

    def add_entries_to_projects(self, projects):
        """ """
        data = self.supabase_client.table("projects").upsert(projects).execute()
        logger.info("{} records inserted/updated".format(len(data.data)))

    def add_entries_to_contributions(self, contributions):
        """ """
        for contributions_chunk in split(contributions, self.chunk_size):
            data = (
                self.supabase_client.table("contributions")
                .upsert(contributions_chunk)
                .execute()
            )
            logger.info("{} records inserted/updated".format(len(data.data)))

    def add_entries_to_contributor_wallets(self,contributor_wallets):
        """ """
        for contributor_wallets_chunk in split(contributor_wallets, self.chunk_size):

            data = (
                self.supabase_client.table("contributor_wallets")
                .upsert(contributor_wallets_chunk)
                .execute()
            )
            logger.info("{} records inserted/updated".format(len(data.data)))
