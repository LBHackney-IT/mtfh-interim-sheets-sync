import re
from typing import Dict
import logging
import os

from utils.data_load_utils import read_db
from utils.transform_interim_sheets import format_date, create_hashed_id
from utils.transform_interim_sheets import transform_tenure, merge_person_dynamodb_interim
from utils.dynamodb_utils import query_dynamodb_by_id, load_dict_to_dynamodb
from utils.google_sheets_utils import read_google_sheets
from utils.transform_activity import person_migrated_activity, contact_details_migrated_activity

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

__DOMAIN = os.getenv("UH_DOMAIN")
__SERVER = os.getenv("UH_SERVER")
__DATABASE = os.getenv("UH_DATABASE")
__USERNAME = __DOMAIN + '\\' + os.getenv("UH_USERNAME")
__PASSWORD = os.getenv("UH_PASSWORD")

__TENANCIES_SPREADSHEET_ID = os.getenv("TENANCIES_SPREADSHEET_ID")
__LEASEHOLDS_SPREADSHEET_ID = os.getenv("LEASEHOLDS_SPREADSHEET_ID")

__ASSETS_QUERY_FILE = "queries/interim_process_assets.sql"
__TENURES_QUERY_FILE = "queries/interim_process_tenures.sql"

__DYNAMODB_PERSONS_ENTITY = "Persons"
__DYNAMICS_CONTACTS_ENTITY = "ContactDetails"
__DYNAMODB_TENURE_ENTITY = "TenureInformation"
__DYNAMODB_ACTIVITY_ENTITY = "ActivityHistory"


def process_interim_data(all_tenures: [Dict], assets: [Dict]):
    """
    Process google sheets data.

    :param all_tenures: List of all tenures from the interim google sheets.
    :param assets: List of all the assets
    :return:
    """
    for tenure in all_tenures:
        print("processing tenure: " + tenure['Payment Ref'].strip())
        if tenure['UH Ref'].strip() in ('', 'New Assignment', 'New Build', 'New RTB'):
            transformed_people, transformed_phones, transformed_tenure = transform_tenure(tenure, assets)

            result_tenure = query_dynamodb_by_id('id', [transformed_tenure['id']],
                                                 __DYNAMODB_PERSONS_ENTITY)
            if len(result_tenure) == 0:
                print("tenure does not exist")
                for person in transformed_people:
                    result_person = query_dynamodb_by_id('id', [person['id']],
                                                         __DYNAMODB_PERSONS_ENTITY)
                    if len(result_person) > 0:
                        merged_person = merge_person_dynamodb_interim(result_person[0], person)
                        # load_dict_to_dynamodb(merged_person, __DYNAMODB_PERSONS_ENTITY)
                        print("person found")
                    else:
                        print("person not found")
                        # load_dict_to_dynamodb(person, __DYNAMODB_PERSONS_ENTITY)
                    person_activity = person_migrated_activity(person)
                    # load_dict_to_dynamodb(person_activity, __DYNAMODB_ACTIVITY_ENTITY)
                print("creating tenure")
                if transformed_tenure != {}:
                    print("creating tenure 2")
                    # load_dict_to_dynamodb(transformed_tenure, __DYNAMODB_TENURE_ENTITY)

                for phone in transformed_phones:
                    result_person = query_dynamodb_by_id('id', [phone['targetId']],
                                                         __DYNAMODB_PERSONS_ENTITY)
                    if len(result_person) > 0:
                        # load_dict_to_dynamodb(phone, __DYNAMICS_CONTACTS_ENTITY)
                        phone_activity = contact_details_migrated_activity(phone)
                        # load_dict_to_dynamodb(phone_activity, __DYNAMODB_ACTIVITY_ENTITY)
                    else:
                        print("phone: person not found: " + phone['targetId'])


def update_household_members_tenure_end_date(household_members: [Dict], tenure_id: str,
                                             end_date: str):
    """
    Updates tenure end date in the tenures stored in the person object.

    :param household_members: List of household members from the tenure object.
    :param tenure_id: The tenure that needs to be updated.
    :param end_date: The end date to apply to the tenure.
    :return:
    """
    for person in household_members:
        result_person = query_dynamodb_by_id('id', [person['id']],
                                             __DYNAMODB_PERSONS_ENTITY)
        if len(result_person) > 0:
            for person_tenure in result_person[0]['tenures']:
                if person_tenure['id'] == tenure_id:
                    person_tenure['endDate'] = format_date(end_date)
            # load_dict_to_dynamodb(result_person[0], __DYNAMODB_PERSONS_ENTITY)


def update_former_tenure_end_date(former_tenures: [Dict]):
    """
    Process former tenures tab in the tenancies spreadsheets by closing the tenures.

    :param former_tenures: List of former tenures from the interim google sheets.
    :return:
    """
    for tenure in former_tenures:
        if tenure['UH Ref'].strip() != '':
            tenure_id = create_hashed_id(tenure['UH Ref'])
        else:
            tenure_id = create_hashed_id(tenure['Payment Ref'])
        result_tenure = query_dynamodb_by_id('id', [tenure_id], __DYNAMODB_TENURE_ENTITY)
        if len(result_tenure) > 0 and not re.search('[a-zA-Z]', tenure['Void Date']):
            print("tenure end date changed")
            result_tenure[0]['endOfTenureDate'] = format_date(tenure['Void Date'])
            # load_dict_to_dynamodb(result_tenure[0], __DYNAMODB_TENURE_ENTITY)
            update_household_members_tenure_end_date(result_tenure[0]['householdMembers'],
                                                     tenure_id, tenure['Void Date'])


def run(event, context):
    """
    Main entry point of Lambda function.
    :param event:
    :param context:
    :return:
    """
    assets = read_db(__SERVER, __USERNAME, __PASSWORD, __DATABASE,
                     open(__ASSETS_QUERY_FILE, 'r').read())

    logger.info("spreadsheet tenancies 2021/04 upto now")
    all_tenancies_range_name = 'Weekly Payments!A1:BY20567'
    all_tenancies = read_google_sheets(__TENANCIES_SPREADSHEET_ID, all_tenancies_range_name)
    process_interim_data(all_tenancies, assets)

    logger.info("Former tenancies 2021/04 upto now")
    former_tenancies_range_name = 'Former Tenants!A1:BU202'
    former_tenancies = read_google_sheets(__TENANCIES_SPREADSHEET_ID, former_tenancies_range_name)
    update_former_tenure_end_date(former_tenancies)

    logger.info("spreadsheet new leaseholds")
    all_leaseholds_range_name = 'New Assignment / RTB!A1:P139'
    all_leaseholds = read_google_sheets(__LEASEHOLDS_SPREADSHEET_ID, all_leaseholds_range_name)
    for leasehold in all_leaseholds:
        leasehold['Date of Birth'] = ''
        leasehold['Home Tel'] = ''
        leasehold['Mobile'] = ''
        leasehold['Property Ref'] = leasehold.pop('Property No')
        leasehold['Tenancy Type'] = leasehold.pop('Tenancy')
        leasehold['Tenancy Start Date'] = leasehold.pop('Assignment / RTB Date')
        leasehold['UH Ref'] = leasehold.pop('UH Rent Acct')
    process_interim_data(all_leaseholds, assets)

    logger.info("spreadsheet new builds")
    all_leaseholds_range_name = 'New Build!A1:Q26'
    all_leaseholds = read_google_sheets(__LEASEHOLDS_SPREADSHEET_ID, all_leaseholds_range_name)
    for leasehold in all_leaseholds:
        leasehold['Date of Birth'] = ''
        leasehold['Home Tel'] = ''
        leasehold['Mobile'] = ''
        leasehold['Property Ref'] = leasehold.pop('Property No')
        leasehold['Tenancy Type'] = leasehold.pop('Tenancy')
        leasehold['Tenancy Start Date'] = leasehold.pop('Date of New Build')
        leasehold['UH Ref'] = leasehold.pop('UH Rent Acct')
    process_interim_data(all_leaseholds, assets)
