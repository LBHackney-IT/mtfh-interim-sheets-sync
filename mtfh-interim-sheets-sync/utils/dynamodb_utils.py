"""
This module exposes functions that allows devs to interact with DynamoDB.
"""
from typing import Dict
import boto3
from boto3.dynamodb.conditions import Key

__AWS_REGION = "eu-west-2"


def load_dict_to_dynamodb(dict_to_write: Dict, table: str, dynamodb=None):
    """
    This function allows to load a dict into a DynamoDB table.

    :param dict_to_write: Dict to load into DynamoDB.
    :param table: The table where to load data.
    :param dynamodb: DynamoDB client that can be initiated outside of this function.
    :return:
    """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', region_name=__AWS_REGION)
    table = dynamodb.Table(table)
    table.put_item(Item=dict_to_write)


def query_dynamodb_by_id(key: str, values: [str], table: str, dynamodb=None) -> [Dict]:
    """
    This function allows to read data from a DynamoDB table by ID.
    :param key: The dynamoDB table's hash key field name.
    :param values: The list of ID that will be used to query dynamoDB.
    :param table: The dynamoDB table from where to read data.
    :param dynamodb: DynamoDB client that can be initiated outside of this function.
    :return: A list of dicts of the data extracted from dynamoDB table.
    """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', region_name=__AWS_REGION)

    table = dynamodb.Table(table)
    results = []

    for value in values:
        response = table.query(KeyConditionExpression=Key(key).eq(value))
        if len(response['Items']) > 0:
            results.append(response['Items'][0])
    return results
