"""
This module exposes functions to handle activity data.
"""
from typing import Dict
import uuid
import hashlib
from datetime import datetime


def person_migrated_activity(person: Dict) -> Dict:
    """
    Creating an activity history event for migrated person.

    :param person: Transformed person object.
    :return: Activity History event object.
    """
    return {
        "id": str(uuid.UUID(hashlib.md5(("migrate" + "person" + person['id'])
                                        .strip().encode()).hexdigest())),
        "type": "migrate",
        "targetType": "person",
        "targetId": person['id'],
        "createdAt": datetime.now().isoformat(timespec="seconds"),
        "timeToLiveForRecord": 0,
        "oldData": None,
        "newData": None,
        "authorDetails": {
            'fullName': 'Import',
            'emailAddress': ''
        }
    }


def contact_details_migrated_activity(contact_details: Dict) -> Dict:
    """
    Creating an activity history event for migrated contact details.

    :param contact_details: Transformed contact details object.
    :return: Activity History event object.
    """
    return {
        "id": str(uuid.UUID(hashlib.md5(("migrate" + "contactDetails" + contact_details['id'] +
                                         contact_details['targetId']).strip().encode())
                            .hexdigest())),
        "type": "migrate",
        "targetType": "contactDetails",
        "targetId": contact_details['targetId'],
        "createdAt": datetime.now().isoformat(timespec="seconds"),
        "timeToLiveForRecord": 0,
        "oldData": None,
        "newData": {
            "id": contact_details['id'],
            "value": contact_details['contactInformation']['value']
        },
        "authorDetails": {
            'fullName': 'Import',
            'emailAddress': ''
        }
    }


def tenure_migrated_activity(tenure: Dict) -> Dict:
    """
    Creating an activity history event for migrated tenures.

    :param tenure: Transformed tenure object.
    :return: Activity History event object.
    """
    return {
        "id": str(uuid.UUID(hashlib.md5(("migrate" + "tenure" + tenure['id']).strip().encode())
                            .hexdigest())),
        "type": "migrate",
        "targetType": "tenure",
        "targetId": tenure['id'],
        "createdAt": datetime.now().isoformat(timespec="seconds"),
        "timeToLiveForRecord": 0,
        "oldData": None,
        "newData": None,
        "authorDetails": {
            'fullName': 'Import',
            'emailAddress': ''
        }
    }


def tenure_people_migrated_activity(tenure: Dict) -> [Dict]:
    """
    Creating a list of activity history events for migrated tenure people.

    :param tenure: Transformed tenure object.
    :return: List of Activity History events.
    """
    list_of_tenure_persons_activities = []
    for person in tenure['householdMembers']:
        list_of_tenure_persons_activities.append(
            {
                "id": str(uuid.UUID(hashlib.md5(("migrate" + "tenure" + tenure['id'] +
                                                 person['id']).strip().encode()).hexdigest())),
                "type": "migrate",
                "targetType": "tenure",
                "targetId": tenure['id'],
                "createdAt": datetime.now().isoformat(timespec="seconds"),
                "timeToLiveForRecord": 0,
                "oldData": None,
                "newData": {
                    "fullName": person['fullName'],
                    "personTenureType": person['personTenureType'],
                    "dateOfBirth": person['dateOfBirth']
                },
                "authorDetails": {
                    "fullName": "Admin",
                    "emailAddress": "mtfh.admin@hackney.gov.uk"
                }
            })
    return list_of_tenure_persons_activities
