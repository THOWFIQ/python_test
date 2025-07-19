from flask import request, jsonify
import requests
import httpx
import json
import os
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from graphqlQueries import *

# from constants.constants import get_configdetails

# Get the absolute path to the config file
configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))

# Load the JSON
with open(configABSpath, 'r') as file:
    configPath = json.load(file)


FID     = configPath['Linkage_DAO']
# FOID    = configPath['FM_Order_EMEA_APJ']
# SOPATH  = configPath['SO_Header_EMEA_APJ']
# WOID    = configPath['WO_Details_EMEA_APJ']
# FFBOM   = configPath['FM_BOM_EMEA_APJ']
FOID    = configPath['FM_Order_DAO']
SOPATH  = configPath['SO_Header_DAO']
WOID    = configPath['WO_Details_DAO']
FFBOM   = configPath['FM_BOM_DAO']

#Define field categories
PRIMARY_FIELDS = {
    "Sales_Order_id",
    "wo_id",
    "Sales_order_ref",
    "Fullfillment Id",
    "foid",
    "order_date",
    "Order create_date",
    "Manifest ID"
}

SECONDARY_FIELDS = {
    "ISMULTIPACK",
    "BUID",
    "Facility"
}

def post_api(URL, query, variables):
    if variables:
        response = httpx.post(SOPATH, json = {"query": query, "variables": variables}, verify=False)
    else:
        response = httpx.post(URL, json = {"query": query}, verify=False)
    print(response.json())
    print("\n")
    return response.json()

def transform_keys(data):
    def convert(key):
        parts = key.split("_")
        return ''.join(part.capitalize() for part in parts)
    return {convert(k): v for k, v in data.items()}

def fileldValidation(filters,format_type,region):
    primary_in_filters = []
    secondary_in_filters = []

    # Step 2: Categorize fields in the filters
    for field in filters:
        if field in PRIMARY_FIELDS:
            primary_in_filters.append(field)
        elif field in SECONDARY_FIELDS:
            secondary_in_filters.append(field)

    # Step 3: If no primary fields found, raise error
    if not primary_in_filters:
        return {
            "status": "error",
            "message": "At least one primary field is required in filters."
        }

    # # Step 4: Call query using matched primary fields and region
    # primary_filters = {key: filters[key] for key in primary_in_filters}
    # result = get_data_by_fields(primary_filters, region)
pass

if __name__ == "__main__":
    region = "EMEA"
    format_type='grid' #grid/export
    filters = {
            "Sales_Order_id": "1004543337,1004543337,1004543337,1004543337",
            "foid": "FO999999,1004543337,1004543337,1004543337,1004543337",
            "Fullfillment_Id": "262135,262135,262135,262135,262135,262135",
            "wo_id": "7360928459,7360928459,7360928459,7360928459,7360928459",
            "Sales_order_ref": "REF123456",
            "Order_create_date": "2025-07-15",
            "ISMULTIPACK": "Yes",
            "BUID": "202",
            "Facility": "WH_BANGALORE",
            "Manifest_ID": "MANI0001",
            "order_date": "2025-07-15"
            }
    result = fileldValidation(filters=filters,format_type=format_type,region=region)
