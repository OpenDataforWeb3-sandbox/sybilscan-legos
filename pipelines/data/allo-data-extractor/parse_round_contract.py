import logging
import json
from base_job import BaseBlockJob
from db import grant_manager, event_manager
from utils.ipfs import get_json_from_ipfs
from utils.common import timestamp_to_utc_string, parse_methods


logger = logging.getLogger(__name__)


class RoundParser(BaseBlockJob):
    """
    Class to parse round contract events
    """

    def __init__(self, job_id, chain, contracts, dependent_jobids):
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

        # READ the round factory contract info
        self.target_contracts = contracts

        #SET the status mapping
        self.status = {
            0: 'pending',
            1: 'approved',
            2: 'rejected',
            3: 'cancelled'
        }

        #SET the state of this instance
        self.applications = []

    def get_application(self, round_address, app_index):
        """ """
        application = [a for a in self.applications if a['round_address']==round_address and a['app_index']==app_index]
        return application[0] if application else None

    def update_state(self, state_updates):
        """
        updates data to DB
        """
        grant_manager.update_applications(state_updates)

    def reset_state(self):
        """ """
        #grant_manager.clear_applications_state()
        pass

    def process_data(self, start_block, end_block):
        """
        Index the events for the Project Registry
        """

        #SET the state for this run
        self.applications = grant_manager.get_round_applications(self.chain)

        event_logs = event_manager.get_events(
            self.target_contracts,
            self.events,
            start_block,
            end_block,
        )

        for event in event_logs:
            event_handler = getattr(self, "parse_{}".format(event["eventName"]))
            event_handler(event)

        #PUSH the state updates on this run
        application_updates = [a for a in self.applications if a['last_updated_block']>= start_block]

        return application_updates

    def parse_NewProjectApplication(self, event):
        """ 
        handler for round contract event NewProjectApplication
        """
        round_address = event['address'].lower()
        project_id = event['projectID']
        app_index = int(event['applicationIndex'])
        metaptr_protocol = event['applicationMetaPtr_protocol']
        metaptr_pointer = event['applicationMetaPtr_pointer']

        if metaptr_protocol != 1:
            raise ValueError('Protocol not implemented')

        application_info = get_json_from_ipfs(metaptr_pointer)
        recipient = application_info.get('application',{}).get('recipient','').lower()
        title = application_info.get('application',{}).get('project',{}).get('title','')
        description = application_info.get('application',{}).get('project',{}).get('description','')
        website = application_info.get('application',{}).get('project',{}).get('website','')
        project_twitter = application_info.get('application',{}).get('project',{}).get('projectTwitter','')
        logo_img = application_info.get('application',{}).get('project',{}).get('logoImg','')
        banner_img = application_info.get('application',{}).get('project',{}).get('bannerImg','')
        project_github = application_info.get('application',{}).get('project',{}).get('projectGithub','')

        new_application = {
            'chain': self.chain,
            'round_address': round_address,
            'app_index': app_index,
            'project_id': project_id,
            'recipient': recipient,
            'title': title,
            'description': description,
            'website': website,
            'project_twitter': project_twitter,
            'logo_img': logo_img,
            'banner_img': banner_img,
            'project_github': project_github,
            'status': 'pending',
            #'application_info': json.dumps(application_info),
            'last_updated_block': event['blockNumber']
        }

        self.applications.append(new_application)

    def parse_ApplicationStatusesUpdated(self,event):
        """ """

        round_address = event['address'].lower()
        row_index = int(event['index'])
        status_bitmap = int(event['status'])

        state_updates = []
        applications_per_row = 128
        for i in range(applications_per_row):
            app_index = row_index + i
            status_index = (status_bitmap >> (i * 2)) & 3
            application = self.get_application(round_address, app_index)
            if application:
                application['status'] = self.status[status_index]
                application['last_updated_block'] = event['blockNumber']
