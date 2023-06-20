import os

#ENV VARIABLES
DB_CONNECTION_URI = os.getenv('DB_CONNECTION_URI','')
DB_CONNECTION_API_CREDENTIALS = os.getenv('DB_CONNECTION_API_CREDENTIALS','') #applicable only for bigquery connection
DB_CONNECTION_DB = 'datastore'
FLIPSIDE_API_KEY = os.getenv('FLIPSIDE_API_KEY','')
TAG_CONFIG = './tag.yaml'