"""
This module exposes functions to transform data from interim sheets to match
the target format in swagger.
"""
from datetime import datetime
from typing import Dict
import uuid
import hashlib

tenure_type_name_to_code = {
    "Secure": "SEC",
    "Non-Secure": "NON",
    "Mesne Profit Ac": "MPA",
    "Introductory": "INT",
    "Temp Decant": "DEC",
    "Asylum Seeker": "ASY",
    "Leasehold (RTB)": "LEA",
    "Freehold (Serv)": "FRS",
    "Shared Owners": "SHO",
    "Private Sale LH": "SPS",
    "": ""
}


def create_hashed_id(value: str) -> str:
    """
    Create the UUID MD5 hashed ID from a string, which is used in all the migration to keep the link
    between the different entities.

    :param value: String to hash.
    :return: MD5 hashed UUID.
    """
    return str(uuid.UUID(hashlib.md5(value.strip().encode()).hexdigest()))


def create_hashed_id_without_strip(value: str) -> str:
    """
    Create the UUID MD5 hashed ID from a string without removing whitespaces,
    which is used in all the migration to keep the link between the different entities.

    :param value: String to hash.
    :return: MD5 hashed UUID.
    """
    return str(uuid.UUID(hashlib.md5(value.encode()).hexdigest()))


def name_starts_with_title(name: str) -> bool:
    """
    Check if the input string starts with a title or not.

    :param name: Input string that represents a full name.
    :return: A boolean that is True if the input string starts with a title.
    """
    return (name.lower().startswith('mr ') or name.lower().startswith('ms ')
            or name.lower().startswith('miss ')
            or name.lower().startswith('mrs '))


def format_date(date: str) -> str:
    """
    Reformat a string date from the format DD.MM.YYYY or DD/MM/YYYY to YYYY-MM-DD.

    :param date: Input date to reformat.
    :return: Reformatted date as a string.
    """
    if '.' in date:
        transformed_date = str(datetime.strptime(date, '%d.%m.%Y').date()) if date != '' \
            else '1900-01-01'
    elif '09/12 2020' in date:
        transformed_date = str(datetime.strptime(date, '%d/%m %Y').date()) if date != '' \
            else '1900-01-01'
    else:
        transformed_date = str(datetime.strptime(date, '%d/%m/%Y').date()) if date != '' \
            else '1900-01-01'
    return transformed_date


def get_person_tenure_type(tenure_type: str) -> str:
    """
    Get the Person tenure type from the tenure type.

    :param tenure_type: Tenure type to be used to define the person type.
    :return: The person tenure type.
    """
    if tenure_type in ('Freehold', 'Freehold (Serv)'):
        person_tenure_type = 'Freeholder'
    elif tenure_type in ('Leasehold (RTB)', 'RS Landlord'):
        person_tenure_type = 'Leaseholder'
    else:
        person_tenure_type = 'Tenant'
    return person_tenure_type


def merge_person_dynamodb_interim(dynamodb_person: Dict, interim_person: Dict) -> Dict:
    """
    Add new tenure from the interim person created from google sheets into
    the person available in DynamoDB

    :param dynamodb_person: The person document available in dynamoDB.
    :param interim_person: The person document created from the interim google sheet.
    :return: The dynamoDB document enriched with the tenure from the interim spreadsheet.
    """
    result_person = dynamodb_person.copy()
    tenures = [tenure for tenure in result_person['tenures'] if tenure['id'] ==
               interim_person['tenures'][0]['id']]
    if len(tenures) == 0:
        result_person['tenures'].append(interim_person['tenures'][0])
    return result_person


def get_asset_details(assets: [Dict], property_ref: str) -> Dict:
    """
    Search for an asset and return its details, otherwise return empty values.

    :param assets: List of assets from UH.
    :param property_ref: The asset ID from tenure.
    :return: Dict of asset ID, asset UPRN, asset reference, asset address and asset type.
    """
    tenure_asset = [asset for asset in assets if asset['prop_ref'].strip() == property_ref.strip()]
    if len(tenure_asset) > 0 and tenure_asset[0]['prop_ref'] != '' \
            and tenure_asset[0]['prop_ref'] is not None:
        asset_full_address = tenure_asset[0]['property_full_address'].strip()
        uprn = tenure_asset[0]['property_llpg_ref'].strip()
        asset_id = create_hashed_id_without_strip(tenure_asset[0]['prop_ref'])
        property_ref = tenure_asset[0]['prop_ref'].strip()
        asset_type = tenure_asset[0]['asset_type']
    else:
        asset_full_address = ''
        uprn = ''
        asset_id = '00000000-0000-0000-0000-000000000000'
        property_ref = ''
        asset_type = ''
    return {
        'asset_id': asset_id,
        'uprn': uprn,
        'property_ref': property_ref,
        'asset_full_address': asset_full_address,
        'asset_type': asset_type
    }


def get_list_of_tenants(tenants: str) -> []:
    """
    Transform a text with potentially many people to a list of people.

    :param tenants: Text variable with potentially many people names.
    :return: An array of people.
    """
    final_list_of_people = []
    for person in tenants.strip().split(' & '):
        if ' and ' in person:
            final_list_of_people += person.split(' and ')
        elif ',' in person:
            final_list_of_people += person.split(',')
        else:
            final_list_of_people.append(person)
    return final_list_of_people


def transform_tenure(tenure: Dict, assets: [Dict]) -> ([Dict], [Dict], Dict):
    """
    Transform a new tenure from the interim google sheet into the target tenure,
    person and phone models.

    :param tenure: The new tenure record from the interim google sheet.
    :param assets: List of assets from UH.
    :return: List of responsible people, their contact details and the tenure object.
    """
    asset_details = get_asset_details(assets, tenure['Property Ref'].strip())
    dob = format_date(tenure['Date of Birth'])

    transformed_people = []
    transformed_people_for_tenure = []
    transformed_phones = []
    for name in get_list_of_tenants(tenure['Tenant']):
        if 'limited' not in name.lower() and 'ltd' not in name.lower() \
                and 'TBG (Open Door)' not in name:
            if name_starts_with_title(name):
                title = name.split(' ')[0]
                firstname = " ".join(name.split(' ')[1:-1])
                surname = name.split(' ')[-1]
            else:
                title = ""
                firstname = " ".join(name.split(' ')[0:-1])
                surname = name.split(' ')[-1]
            transformed_people.append({
                'id': create_hashed_id(surname.lower().strip() + firstname.lower().strip() + dob),
                'preferredTitle': title,
                'title': title,
                'preferredFirstName': firstname,
                'firstName': firstname,
                'preferredMiddleName': "",
                'middleName': "",
                'preferredSurname': surname,
                'surname': surname,
                'placeOfBirth': "",
                'dateOfBirth': dob,
                'personTypes': ['Tenant'],
                'tenures': [{
                    'id': create_hashed_id(tenure['Payment Ref']),
                    'paymentReference': tenure['Payment Ref'],
                    'type': tenure['Tenancy Type'].strip(),
                    'startDate': format_date(tenure['Tenancy Start Date']),
                    'endDate': None,
                    'assetFullAddress': asset_details['asset_full_address'],
                    'uprn': asset_details['uprn'],
                    'propertyReference': asset_details['property_ref'],
                    'assetId': asset_details['asset_id']
                }],
                'lastModified': str(datetime.now().isoformat())
            })
            transformed_people_for_tenure.append({
                'id': create_hashed_id(surname.lower().strip() + firstname.lower().strip() + dob),
                'type': 'person',
                'fullName': firstname + " " + surname,
                'isResponsible': True,
                'dateOfBirth': dob,
                'personTenureType': get_person_tenure_type(tenure['Tenancy Type'])
            })

            for phone in tenure['Home Tel'].split('/') + tenure['Mobile'].split('/'):
                if phone.strip() != '':
                    transformed_phones.append({
                        'id': create_hashed_id(phone),
                        'targetId': create_hashed_id(surname.lower().strip()
                                                     + firstname.lower().strip() + dob),
                        'targetType': 'person',
                        'contactInformation': {
                            'contactType': 'phone',
                            'subType': 'mobile' if phone.strip().startswith('07') else 'landline',
                            'value': phone.strip(),
                            'description': '',
                            'addressExtended': None
                        },
                        'sourceServiceArea': {
                            'area': 'Housing',
                            'isDefault': True
                        },
                        'recordValidUntil': None,
                        'isActive': True,
                        'createdBy': {
                            'createdAt': str(datetime.now().isoformat()),
                            'fullName': 'Import',
                            'emailAddress': ''
                        },
                        'lastModified': str(datetime.now().isoformat())
                    })
    transformed_tenure = {} if len(transformed_people_for_tenure) == 0 else {
        'id': create_hashed_id(tenure['Payment Ref']),
        'paymentReference': tenure['Payment Ref'],
        'householdMembers': transformed_people_for_tenure,
        'tenuredAsset': {
            'id': asset_details['asset_id'],
            'fullAddress': asset_details['asset_full_address'],
            'uprn': asset_details['uprn'],
            'type': asset_details['asset_type']
        },
        'charges': None,
        'startOfTenureDate': format_date(tenure['Tenancy Start Date']),
        'endOfTenureDate': None,
        'tenureType': {
            'code': tenure_type_name_to_code[tenure['Tenancy Type'].strip()],
            'description': tenure['Tenancy Type'].strip()
        },
        'terminated': {
            'isTerminated': False,
            'reasonForTermination': ""
        },
        'successionDate': "1900-01-01",
        'evictionDate': "1900-01-01",
        'potentialEndDate': "1900-01-01",
        "notices": [
            {
                'type': "",
                'servedDate': "1900-01-01",
                'expiryDate': "1900-01-01",
                'effectiveDate': "1900-01-01",
                'endDate': None
            }
        ],
        'legacyReferences': [
            {
                'name': 'uh_tag_ref',
                'value': ""
            },
            {
                'name': 'u_saff_tenancy',
                'value': ""
            }
        ],
        'isMutualExchange': False,
        'informHousingBenefitsForChanges': False,
        'isSublet': False,
        'subletEndDate': "1900-01-01"
    }
    return transformed_people, transformed_phones, transformed_tenure
