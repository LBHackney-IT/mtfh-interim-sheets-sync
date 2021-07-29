"""
Module exposing some high level utils such as a progress bar, cleansing functions...
"""
from datetime import datetime
import uuid
import hashlib


def create_hashed_id(value: str) -> str:
    """
    Create the UUID MD5 hashed ID from a string, which is used in all the migration to keep the link
    between the different entities.

    :param value: String to hash.
    :return: MD5 hashed UUID.
    """
    return str(uuid.UUID(hashlib.md5(value.strip().encode()).hexdigest()))


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
