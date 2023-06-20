from db import dashboard_manager, grant_manager, app_manager
import json
import time
from datetime import datetime
from utils.common import timestamp_to_utc_string
import uuid

class DashboardDataExtractor:
    def __init__(self):
        pass

    def extract_projects_data(self):

        applications = grant_manager.get_round_applications_with_votes(chain="ethereum")
        application_metrics = app_manager.get_application_metrics()

        projects = []
        for a in applications:
            metrics = [
                m
                for m in application_metrics
                if m["round_address"] == a["round_address"]
                and m["app_index"] == a["app_index"]
            ]
            project = {}
            project["id"] = a["round_address"] + str(a["app_index"])
            project["project"] = a["project_id"]
            project["created_at"] = timestamp_to_utc_string(time.time())
            project["updated_at"] = timestamp_to_utc_string(time.time())
            project["round_id"] = a["round_address"]
            project["wallet_address"] = a["recipient"]
            project["title"] = a["title"]
            project["profile_pic"] = ""
            project["banner_pic"] = ""
            project["gitcoin_url"] = ""
            project["num_votes"] = a["num_votes"]
            project["num_contributors"] = a["num_contributors"]
            project["total_amount_contributed_usd"] = (
                a["amount_contributed_eth"] * 1797.89 + a["amount_contributed_dai"] * 1
            )
            project["status"] = "APPROVED"

            if metrics:
                metrics = metrics[0]
                project["risk_score"] = metrics["risk_score"]
                ui_tags = []
                tags = json.loads(metrics["tags"])
                for t_n, t_v in tags.items():
                    if t_v:
                        ui_tags.append(t_n + " ✅")
                    else:
                        ui_tags.append(t_n + " ❌")
                project["tags"] = {"tags": ui_tags}
                projects.append(project)

        dashboard_manager.add_entries_to_projects(projects)

    def extract_contributions_data(self):
        """
        "id",
        "project_id",
        "wallet_id",
        "amount_contributed_usd"
        """
        votes = grant_manager.get_votes(chain='ethereum')
        contributions = []
        for v in votes:
            contributions.append({
                "id": str(uuid.uuid4()),
                "project_id": v["round_address"] + str(v["app_index"]),
                "wallet_id": v['voter'],
                "amount_contributed_usd": v['amount'] * 1 if v["token"] == "DAI" else v['amount'] * 1797.89 ,
                "created_at": timestamp_to_utc_string(time.time()),
                "updated_at": timestamp_to_utc_string(time.time())
            })
        dashboard_manager.add_entries_to_contributions(contributions)


    def extract_contributor_wallets_data(self):
        wallet_metrics = app_manager.get_wallet_metrics()

        contributor_wallets = []
        for w in wallet_metrics:
            c_wallet = {}
            c_wallet["id"] = w['wallet']
            c_wallet["risk_score"] = w["risk_score"]
            c_wallet["created_at"] = timestamp_to_utc_string(time.time())
            c_wallet["updated_at"] = timestamp_to_utc_string(time.time())
  
            ui_tags = []
            
            for t_n, t_v in w["tags"].items():
                if t_v:
                    ui_tags.append(t_n + " ✅")
                else:
                    ui_tags.append(t_n + " ❌")
            c_wallet["tags"] = {"tags": ui_tags}
            contributor_wallets.append(c_wallet)

        dashboard_manager.add_entries_to_contributor_wallets(contributor_wallets)

    def run(self):

        self.extract_projects_data()
        self.extract_contributor_wallets_data()
        self.extract_contributions_data()
