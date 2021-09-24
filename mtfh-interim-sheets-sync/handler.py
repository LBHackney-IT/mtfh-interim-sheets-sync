"""
Main entry point of the lambda function.
"""
import re
from typing import Dict
import logging
import os
import boto3

from utils.data_load_utils import read_db
from utils.transform_interim_sheets import format_date, create_hashed_id
from utils.transform_interim_sheets import transform_tenure, merge_person_dynamodb_interim
from utils.dynamodb_utils import query_dynamodb_by_id, load_dict_to_dynamodb
from utils.google_sheets_utils import read_google_sheets
from utils.transform_activity import person_migrated_activity, contact_details_migrated_activity, \
                                     tenure_migrated_activity, tenure_people_migrated_activity
from utils.transform_interim_asset import transform_asset

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

__DOMAIN = os.getenv("UH_DOMAIN")
__SERVER = os.getenv("UH_SERVER")
__DATABASE = os.getenv("UH_DATABASE")
__USERNAME = __DOMAIN + '\\' + os.getenv("UH_USERNAME")
__PASSWORD = os.getenv("UH_PASSWORD")

__TENANCIES_SPREADSHEET_ID = os.getenv("TENANCIES_SPREADSHEET_ID")
__LEASEHOLDS_SPREADSHEET_ID = os.getenv("LEASEHOLDS_SPREADSHEET_ID")
__ASSETS_SPREADSHEET_ID = os.getenv("ASSETS_SPREADSHEET_ID")
__MISSING_TENURES_SPREADSHEET_ID = os.getenv("MISSING_TENURES_SPREADSHEET_ID")

__ASSETS_QUERY_FILE = "queries/interim_process_assets.sql"

__DYNAMODB_PERSONS_ENTITY = "Persons"
__DYNAMICS_CONTACTS_ENTITY = "ContactDetails"
__DYNAMODB_TENURE_ENTITY = "TenureInformation"
__DYNAMODB_ACTIVITY_ENTITY = "ActivityHistory"
__DYNAMODB_ASSET_ENTITY = "Assets"

payment_ref_property_ref_fix = {
    '228011997': '00090269',
    '228011998': '00090280',
    '228011999': '00090282',
    '228012000': '00090110',
    '228012001': '00090270',
    '228013057': '00090274',
    '228013008': '00090135',
    '228013027': '00090302',
    '228013034': '00090275',
    '228013035': '00090272',
    '228013036': '00090281',
    '228013049': '00090188',
    '228013056': '00090322',
    '228013216': '00090321',
    '228013217': '00090316',
    '228013335': '00090317',
    '228013336': '00090319',
    '228013337': '00090320'
}

new_changes_payment_reference_mapping = {
    '1913901402': '0123376/01',
    '1916723502': '0112512/01',
    '4065013508': '030182/01',
    '4350034704': '031743/01',
    '5602052606': '039277/01',
    '7312011106': '049324/01',
    '8533001210': '060527/01',
    '1924859402': '030201/01',
    '3376005810': '025273/01',
    '1931626402': '0121968/01',
    '1931660402': '0125032/01',
    '5674080606': '039563/01',
    '1330093304': '001774/01',
    '1990062504': '0117712/01',
    '3376039104': '025350/01',
    '1939208402': ''
}


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
            transformed_people, transformed_phones, transformed_tenure = transform_tenure(tenure,
                                                                                          assets)

            if transformed_tenure != {}:
                result_tenure = query_dynamodb_by_id('id', [transformed_tenure['id']],
                                                     __DYNAMODB_TENURE_ENTITY)
                if len(result_tenure) == 0:
                    print("tenure does not exist")
                    for person in transformed_people:
                        result_person = query_dynamodb_by_id('id', [person['id']],
                                                             __DYNAMODB_PERSONS_ENTITY)
                        if len(result_person) > 0:
                            load_dict_to_dynamodb(merge_person_dynamodb_interim(
                                result_person[0], person), __DYNAMODB_PERSONS_ENTITY)
                            print("person found")
                        else:
                            print("person not found")
                            load_dict_to_dynamodb(person, __DYNAMODB_PERSONS_ENTITY)
                        load_dict_to_dynamodb(person_migrated_activity(person),
                                              __DYNAMODB_ACTIVITY_ENTITY)
                    print("creating tenure")
                    if transformed_tenure != {}:
                        result_asset = query_dynamodb_by_id('id', [transformed_tenure['tenuredAsset']['id']],
                                                            __DYNAMODB_ASSET_ENTITY)
                        if len(result_asset) > 0 and (result_asset[0]['tenure'] == {} or
                                                      transformed_tenure['startOfTenureDate'] > result_asset[0]['tenure']['startOfTenureDate']):
                            result_asset[0]['tenure']['id'] = transformed_tenure['id']
                            result_asset[0]['tenure']['startOfTenureDate'] = transformed_tenure['startOfTenureDate']
                            result_asset[0]['tenure']['endOfTenureDate'] = transformed_tenure['endOfTenureDate']
                            result_asset[0]['tenure']['type'] = transformed_tenure['tenureType']['description']
                            result_asset[0]['tenure']['paymentReference'] = transformed_tenure['paymentReference']
                            load_dict_to_dynamodb(result_asset[0], __DYNAMODB_ASSET_ENTITY)
                        load_dict_to_dynamodb(transformed_tenure, __DYNAMODB_TENURE_ENTITY)
                        load_dict_to_dynamodb(tenure_migrated_activity(transformed_tenure),
                                              __DYNAMODB_ACTIVITY_ENTITY)
                        # for tenure_person_activity in tenure_people_migrated_activity(
                        #         transformed_tenure):
                        #     load_dict_to_dynamodb(tenure_person_activity,
                        #                           __DYNAMODB_ACTIVITY_ENTITY)

                    for phone in transformed_phones:
                        result_person = query_dynamodb_by_id('id', [phone['targetId']],
                                                             __DYNAMODB_PERSONS_ENTITY)
                        if len(result_person) > 0:
                            load_dict_to_dynamodb(phone, __DYNAMICS_CONTACTS_ENTITY)
                            phone_activity = contact_details_migrated_activity(phone)
                            load_dict_to_dynamodb(phone_activity, __DYNAMODB_ACTIVITY_ENTITY)
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
            load_dict_to_dynamodb(result_person[0], __DYNAMODB_PERSONS_ENTITY)


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
            load_dict_to_dynamodb(result_tenure[0], __DYNAMODB_TENURE_ENTITY)
            update_household_members_tenure_end_date(result_tenure[0]['householdMembers'],
                                                     tenure_id, tenure['Void Date'])
            result_asset = query_dynamodb_by_id('id', [result_tenure[0]['tenuredAsset']['id']],
                                                __DYNAMODB_ASSET_ENTITY)
            if len(result_asset) > 0 and result_tenure[0]['id'] == result_asset[0]['tenure']['id']:
                result_asset[0]['tenure']['endOfTenureDate'] = result_tenure[0]['endOfTenureDate']
                load_dict_to_dynamodb(result_asset[0], __DYNAMODB_ASSET_ENTITY)


def run(event, context):
    """
    Main entry point of Lambda function.
    :param event:
    :param context:
    :return:
    """
    assets = read_db(__SERVER, __USERNAME, __PASSWORD, __DATABASE,
                     open(__ASSETS_QUERY_FILE, 'r').read())

    logger.info("spreadsheet assets")
    assets_range_name = 'New Build properties!A1:L300'
    all_assets = read_google_sheets(__ASSETS_SPREADSHEET_ID, assets_range_name)
    for asset in all_assets:
        tenure_res = query_dynamodb_by_id('id', [create_hashed_id(asset['Payment Ref'])], __DYNAMODB_TENURE_ENTITY)
        if len(tenure_res) > 0:
            tenure = {
                'id': tenure_res[0]['id'],
                'paymentReference': tenure_res[0]['paymentReference'],
                'type': tenure_res[0]['tenureType']['description'],
                'startOfTenureDate': tenure_res[0]['startOfTenureDate'],
                'endOfTenureDate': tenure_res[0]['endOfTenureDate']
            }
        else:
            tenure = {}
        transformed_asset = transform_asset(asset, tenure)
        load_dict_to_dynamodb(transformed_asset, __DYNAMODB_ASSET_ENTITY)
        assets.append({
            'prop_ref': transformed_asset['assetId'],
            'property_llpg_ref': "",
            'property_full_address': transformed_asset['assetAddress']['addressLine1'] + ', ' +
                                     transformed_asset['assetAddress']['postCode'],
            'asset_type': transformed_asset['assetType']
        })

    logger.info("spreadsheet tenancies 2021/04 upto now")
    all_tenancies_range_name = 'Weekly Payments!A1:BY22000'
    all_tenancies = read_google_sheets(__TENANCIES_SPREADSHEET_ID, all_tenancies_range_name)
    process_interim_data(all_tenancies, assets)

    logger.info("Former tenancies 2021/04 upto now")
    former_tenancies_range_name = 'Former Tenants!A1:BU1000'
    former_tenancies = read_google_sheets(__TENANCIES_SPREADSHEET_ID, former_tenancies_range_name)
    update_former_tenure_end_date(former_tenancies)

    logger.info("spreadsheet new leaseholds")
    all_leaseholds_range_name = 'New Assignment / RTB!A1:P1000'
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
    all_leaseholds_range_name = 'New Build!A1:Q200'
    all_leaseholds = read_google_sheets(__LEASEHOLDS_SPREADSHEET_ID, all_leaseholds_range_name)
    all_leaseholds_new = []
    for leasehold in all_leaseholds:
        if leasehold['Tenant'].strip() not in ('Countryside Partnerships', ''):
            if leasehold['Payment Ref'] in payment_ref_property_ref_fix:
                leasehold['Property No'] = payment_ref_property_ref_fix[leasehold['Payment Ref']]
            leasehold['Date of Birth'] = ''
            leasehold['Home Tel'] = ''
            leasehold['Mobile'] = ''
            leasehold['Property Ref'] = leasehold.pop('Property No')
            leasehold['Tenancy Type'] = leasehold.pop('Tenancy')
            if 'Date of New Build' in leasehold:
                leasehold['Tenancy Start Date'] = leasehold.pop('Date of New Build')
            else:
                leasehold['Tenancy Start Date'] = ""
            leasehold['UH Ref'] = leasehold.pop('UH Rent Acct')
            all_leaseholds_new.append(leasehold)
    process_interim_data(all_leaseholds_new, assets)

    logger.info("tenure changes spreadsheet")
    changes_spreadsheet_id = '19Q9z1IckmrwWx1l6cGXoUeEBsDTNuW-of1OM3099x6I'
    changes_range_name = 'Sheet1!A1:M1500'
    all_changes = read_google_sheets(changes_spreadsheet_id, changes_range_name)
    for change in all_changes:
        if change['Payment Ref'] in new_changes_payment_reference_mapping:
            change['UH Ref'] = new_changes_payment_reference_mapping[change['Payment Ref']]

        if change['UH Ref'].strip() != '':
            tenure_id = create_hashed_id(change['UH Ref'])
        else:
            tenure_id = create_hashed_id(change['Payment Ref'])

        if change['Type of change'].strip().lower() in ('new let', 'let & void after cyber attack'):
            tenure = query_dynamodb_by_id('id', [tenure_id], 'TenureInformation')
            if len(tenure) == 0:
                change['Date of Birth'] = ''
                change['Home Tel'] = ''
                change['Mobile'] = ''
                if change['Tenancy Type'] == 'IT':
                    change['Tenancy Type'] = 'Introductory'
                elif change['Tenancy Type'] == 'Decant Rent Free Lic':
                    change['Tenancy Type'] = 'Temp Decant'
                process_interim_data([change], assets)
        elif change['Type of change'].strip().lower() in ('new void', 'rtb', 'let & void after cyber attack'):
            tenure = query_dynamodb_by_id('id', [tenure_id], 'TenureInformation')
            if len(tenure) == 0:
                print("problem: tenure not found for 'new void': " + change['Payment Ref'])
            else:
                if tenure[0]['endOfTenureDate'] is None and change['Void Date'] not in ('Pre Cyber Attack?', 'Non-Possessed'):
                    update_former_tenure_end_date([change])

    logger.info("Mehdi - Missing tenures spreadsheet")
    missing_tenures_range_name = 'New Build!A1:G200'
    missing_tenures = read_google_sheets(__MISSING_TENURES_SPREADSHEET_ID, missing_tenures_range_name)
    for missing_tenure in missing_tenures:
        missing_tenure['Date of Birth'] = ''
        missing_tenure['Home Tel'] = ''
        missing_tenure['Mobile'] = ''
        missing_tenure['Property Ref'] = missing_tenure.pop('Property No')
        missing_tenure['Tenancy Type'] = missing_tenure.pop('Tenancy')
        if 'Date of New Build' in missing_tenure:
            missing_tenure['Tenancy Start Date'] = missing_tenure.pop('Date of New Build')
        else:
            missing_tenure['Tenancy Start Date'] = ""
        missing_tenure['UH Ref'] = missing_tenure.pop('UH Rent Acct')
    process_interim_data(missing_tenures, assets)

    logger.info("reprocess spreadsheet new builds")
    for asset in all_assets:
        tenure_res = query_dynamodb_by_id('id', [create_hashed_id(asset['Payment Ref'])], __DYNAMODB_TENURE_ENTITY)
        if len(tenure_res) > 0:
            tenure = {
                'id': tenure_res[0]['id'],
                'paymentReference': tenure_res[0]['paymentReference'],
                'type': tenure_res[0]['tenureType']['description'],
                'startOfTenureDate': tenure_res[0]['startOfTenureDate'],
                'endOfTenureDate': tenure_res[0]['endOfTenureDate']
            }
        else:
            tenure = {}
        transformed_asset = transform_asset(asset, tenure)
        load_dict_to_dynamodb(transformed_asset, __DYNAMODB_ASSET_ENTITY)

    lambda_client = boto3.client('lambda')
    person_lambda_payload = """{
        "dynamoTable": "Persons",
        "indexNodeHost": "https://vpc-housing-search-api-es-cggwz5gia7iqw6kxw64ytgrmr4.eu-west-2.es.amazonaws.com",
        "indexName": "persons"
    }"""
    lambda_client.invoke(FunctionName='mtfh-dynamodb-elasticsearch-indexing-production', InvocationType='Event',
                         Payload=person_lambda_payload)

    tenure_lambda_payload = """{
        "dynamoTable": "TenureInformation",
        "indexNodeHost": "https://vpc-housing-search-api-es-cggwz5gia7iqw6kxw64ytgrmr4.eu-west-2.es.amazonaws.com",
        "indexName": "tenures"
    }"""
    lambda_client.invoke(FunctionName='mtfh-dynamodb-elasticsearch-indexing-production', InvocationType='Event',
                         Payload=tenure_lambda_payload)

    asset_lambda_payload = """{
        "dynamoTable": "Assets",
        "indexNodeHost": "https://vpc-housing-search-api-es-cggwz5gia7iqw6kxw64ytgrmr4.eu-west-2.es.amazonaws.com",
        "indexName": "assets"
    }"""
    lambda_client.invoke(FunctionName='mtfh-dynamodb-elasticsearch-indexing-production', InvocationType='Event',
                         Payload=asset_lambda_payload)
