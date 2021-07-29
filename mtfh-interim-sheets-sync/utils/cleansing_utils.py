"""
Module exposing some high level utils such as a progress bar, cleansing functions...
"""
from datetime import datetime
import uuid
import hashlib


def print_progress_bar(iteration: int, total: int, prefix: str = '', suffix: str = '',
                       length: int = 100):
    """
    A function that prints a progress bar.
    :param iteration: The iteration of the process out of the total number of iterations.
    :param total: The total number of iterations in the process.
    :param prefix: A text to show at the start of the progress bar line.
    :param suffix: A text to show at the end of the progress bar line.
    :param length: Length of the progress bar.
    :return:
    """
    fill = '█'
    print_end = "\r"
    decimals = 1
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    progress_bar = fill * filled_length + '-' * (length - filled_length)
    print(f'\r{prefix} |{progress_bar}| {percent}% {suffix}', end=print_end)
    # Print New Line on Complete
    if iteration == total:
        print()


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
