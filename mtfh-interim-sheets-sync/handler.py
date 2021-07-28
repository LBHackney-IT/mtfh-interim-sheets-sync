import datetime
import logging
import os

from utils.data_load_utils import read_db

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

__DOMAIN = os.getenv("UH_DOMAIN")
__SERVER = os.getenv("UH_SERVER")
__DATABASE = os.getenv("UH_DATABASE")
__USERNAME = __DOMAIN + '\\' + os.getenv("UH_USERNAME")
__PASSWORD = os.getenv("UH_PASSWORD")

__ASSETS_QUERY_FILE = "queries/interim_process_assets.sql"
__TENURES_QUERY_FILE = "queries/interim_process_tenures.sql"

__DYNAMODB_PERSONS_ENTITY = "Persons"
__DYNAMICS_CONTACTS_ENTITY = "ContactDetails"
__DYNAMODB_TENURE_ENTITY = "TenureInformation"
__DYNAMODB_ACTIVITY_ENTITY = "ActivityHistory"


def run(event, context):
    current_time = datetime.datetime.now().time()
    name = context.function_name
    logger.info("Your cron function " + name + " ran at " + str(current_time))
    logger.info("database: " + __DATABASE)
    assets = read_db(__SERVER, __USERNAME, __PASSWORD, __DATABASE,
                     open(__ASSETS_QUERY_FILE, 'r').read())

    payment_ref = read_db(__SERVER, __USERNAME, __PASSWORD, __DATABASE,
                          open(__TENURES_QUERY_FILE, 'r').read())
    logger.info("Asset: " + str(assets[0]))
    logger.info("Payment ref: " + str(payment_ref[0]))
