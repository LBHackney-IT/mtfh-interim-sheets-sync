"""
This module exposes functions to transform asset data from interim sheets to match
the target format in swagger.
"""
from typing import Dict
import uuid
import hashlib


def transform_asset(interim_asset: Dict, asset_tenure: Dict) -> Dict:
    """
    Transform interim asset data into target asset entity.

    :param interim_asset: Interim asset data from spreadsheet.
    :param asset_tenure: The tenure linked to this asset.
    :return: Target asset data
    """
    assetType = ""
    if "Type" in interim_asset:
        assetType = interim_asset['Type'] or 'Dwelling'
    else:
        assetType = 'Dwelling'

    print('Asset Type')
    print(assetType)

    prop_ref = interim_asset['Property Ref'].zfill(8)
    return {
        'id': str(uuid.UUID(hashlib.md5(prop_ref.encode()).hexdigest())),
        'assetId': prop_ref.strip(),
        'assetType': assetType,
        'assetLocation': None,
        'assetAddress': {
            'uprn': interim_asset['uprn'],
            'addressLine1': interim_asset['Address Line 1'],
            'addressLine2': interim_asset['Address Line 2'],
            'addressLine3': interim_asset['Address Line 3'],
            'addressLine4': "",
            'postCode': interim_asset['Post Code'],
            'postPreamble': ""
        },
        'assetManagement': None,
        'assetCharacteristics': None,
        'tenure': asset_tenure,
        'rootAsset': "ROOT",
        'parentAssetIds': "ROOT"
    }
