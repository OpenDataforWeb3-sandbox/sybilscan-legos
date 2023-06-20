import argparse
import logging
from utils.common import read_yaml
import numpy as np
import pandas as pd
import os
import importlib
from dataprovider.wallet import WalletInsights
from config import FLIPSIDE_API_KEY, TAG_CONFIG
import json

logger = logging.getLogger(__name__)


class Tagger:
    """
    A class to tag wallets based on the configured rules
    """

    def __init__(self):
        """ """
        # Initialize account to query on wallet insights
        self.wallet_insights = WalletInsights(api_key=FLIPSIDE_API_KEY)
        # Initialize default tags
        self.tags = read_yaml(TAG_CONFIG)["tags"]

    def run_step(self, step, wallets):
        """
        calls the corresponding module for each step
        """
        step_handler = getattr(self.wallet_insights, step["call"])
        step_params = {"wallets": wallets, "value": step["params"]["value"]}
        result = step_handler(**step_params)
        return self.check_wallets(wallets, result)
        #return [True for w in wallets]

    def transform_data(self, wallets):
        """ """
        result = []

        for w in wallets:
            risk_score = 0
            risk_score += 10 if w["Farmer"] else 0
            risk_score += 10 if w["MoneyMixer"] else 0
            risk_score += 10 if not w["OnChainHistory"] else 0
            wallets_mod = {}
            wallets_mod["wallet"] = w["wallet"]
            wallets_mod["risk_score"] = risk_score
            wallets_mod["tags"] = json.dumps(
                {
                    "Farmer": w["Farmer"],
                    "MoneyMixer": w["MoneyMixer"],
                    "OnChainHistory": w["OnChainHistory"],
                }
            )
            result.append(wallets_mod)

        return result

    def tag(self, wallets, tag_names=[]):
        """ """

        tags = [t for t in self.tags if not tag_names or t["name"] in tag_names]

        tag_results = {}

        for tag in tags:

            step_results = [
                True for i in range(len(wallets))
            ]  # assume True to start with
            for step in tag["steps"]:
                step_result = self.run_step(step, wallets)
                step_results = np.logical_and(step_results, step_result)

            tag_results[tag["name"]] = step_results

        df_result = pd.DataFrame(tag_results)
        df_result["wallet"] = wallets
        df_result.to_csv('data.csv')
        return self.transform_data(df_result.to_dict("records"))

    def check_wallets(self, wallets, results):
        """
        This function takes in a query and a list of wallets as input,
        and returns a list of boolean values indicating whether each wallet in the input list is present in the query results.

        Args:
        query (str) : The query to be run
        wallets (list) : A list of wallets to check against the query results

        Returns:
        list : A list of boolean values indicating whether each wallet in the input list is present in the query results.
        """

        matched_wallets = [w["wallet"] for w in results] if results is not None else []
        result = [w in matched_wallets for w in wallets]
        return result
