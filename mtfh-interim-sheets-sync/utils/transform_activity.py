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
            'id': '11111111-1111-1111-1111-111111111111',
            'fullName': 'Admin',
            'emailAddress': 'mtfh.admin@hackney.gov.uk'
        }
    }
