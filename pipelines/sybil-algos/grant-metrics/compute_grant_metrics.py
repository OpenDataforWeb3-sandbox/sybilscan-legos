import logging
import numpy as np
import pandas as pd
import os
import importlib
from db import app_manager
import json

from utils.twitter import TwitterAPI
from config import (
    TWITTER_API_KEY,
    TWITTER_API_KEY_SECRET,
    TWITTER_ACCESS_TOKEN,
    TWITTER_ACCESS_TOKEN_SECRET,
)

logger = logging.getLogger(__name__)


class GrantMetricsCalculator:
    """
    A class to tag grant applications based on the rules
    """

    def __init__(self):
        """ """
        self.rules = []
        self.twitter_api = TwitterAPI(
            TWITTER_API_KEY,
            TWITTER_API_KEY_SECRET,
            TWITTER_ACCESS_TOKEN,
            TWITTER_ACCESS_TOKEN_SECRET,
        )
        self.wallet_metrics = {r["wallet"]: r for r in app_manager.get_wallet_metrics()}

    def metric_TwitterVotesImbalance(self, applications):
        """
        calls the corresponding module for each step
        """
        i=0
        for a in applications:
            
            
            votes_count = a.get("num_votes", 0)
            followers_count = self.twitter_api.get_followers_count(a["project_twitter"])
            print(f"{i}/{len(applications)} : {a['title']}:{followers_count}")
            i=i+1
            if followers_count:
                votes_follower_ratio = votes_count / (followers_count + 1)
                a["TwitterVotesImbalance"] = votes_follower_ratio > 1
            else:
                a["TwitterVotesImbalance"] = False

    def metric_Farmer(self, applications):
        """
        """
        for a in applications:
            a["Farmer"] = (
                self.wallet_metrics[a["recipient"]]["tags"]["Farmer"]
                if a["recipient"] in self.wallet_metrics
                else None
            )

    def metric_MoneyMixer(self, applications):
        """
        """
        for a in applications:
            a["MoneyMixer"] = (
                self.wallet_metrics[a["recipient"]]["tags"]["MoneyMixer"]
                if a["recipient"] in self.wallet_metrics
                else None
            )

    def metric_OnChainHistory(self, applications):
        """
        """
        for a in applications:
            a["OnChainHistory"] = (
                self.wallet_metrics[a["recipient"]]["tags"]["OnChainHistory"]
                if a["recipient"] in self.wallet_metrics
                else None
            )

    def compute_risk_score(self, applications):
        """ """
        for a in applications:
            risk_score = 0
            risk_score += 10 if a['Farmer'] else 0
            risk_score += 10 if a['MoneyMixer'] else 0
            risk_score += 10 if not a['OnChainHistory'] else 0
            risk_score += 10 if a['TwitterVotesImbalance'] else 0
            a["risk_score"] = risk_score


    def compute(self, applications):
        """ """

        rules = ["TwitterVotesImbalance", 'Farmer', 'OnChainHistory', 'MoneyMixer']

        for r in rules:
            metric_fn = getattr(self, f"metric_{r}")
            metric_fn(applications)

        self.compute_risk_score(applications)

        return [
            {
                "round_address": a["round_address"],
                "app_index": a["app_index"],
                "tags" : json.dumps({
                    "Farmer": a["Farmer"],
                    "MoneyMixer": a["MoneyMixer"],
                    "OnChainHistory": a["OnChainHistory"],
                    "TwitterVotesImbalance": a["TwitterVotesImbalance"]
                }),
                "risk_score": a["risk_score"]
            }
            for a in applications
        ]
