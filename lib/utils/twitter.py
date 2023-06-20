import tweepy
import config
import os
import logging

logger = logging.getLogger(__name__)

class TwitterAPI:
    def __init__(
        self,
        twitter_api_key,
        twitter_api_secret_key,
        twitter_access_token,
        twitter_access_token_secret,
    ):
        """ """
        auth = tweepy.OAuth1UserHandler(
            twitter_api_key,
            twitter_api_secret_key,
            twitter_access_token,
            twitter_access_token_secret,
        )
        self.api = tweepy.API(auth)

    def get_followers_count(self,screen_name):
        """ """
        try:
            screen_name = screen_name.split("/")[-1]
            user = self.api.get_user(screen_name=screen_name)
            return user.followers_count
        except Exception as e:
            logger.info(e)
            return None

    def get_user_info(self, screen_name):
        """ """
        try:
            screen_name = screen_name.split("/")[-1]
            user = self.api.get_user(screen_name=screen_name)
            return user
        except Exception as e:
            return None
