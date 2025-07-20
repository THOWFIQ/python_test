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

# Load config
configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

FID     = configPath['Linkage_DAO']
FOID    = configPath['FM_Order_DAO']
SOPATH  = configPath['SO_Header_DAO']
WOID    = configPath['WO_Details_DAO']
FFBOM   = configPath['FM_BOM_DAO']

PRIMARY_FIELDS = {
    "Sales_Order_id",
    "wo_id",
    "Fullfillment Id",
    "foid",
    "order_date"
}

SECONDARY_FIELDS = {
    "ISMULTIPACK",
    "BUID",
    "Facility"
}

def post_api(URL, query, variables):
    try:
        if variables:
            response = httpx.post(URL, json={"query": query, "variables": variables}, verify=False)
        else:
            response = httpx.post(URL, json={"query": query}, verify=False)
        return response.json()
    except Exception as e:
        print(f"Exception in post_api: {e}")
        return {"error": str(e)}

def combined_salesorder_fetch(so_id):
    combined_salesorder_data  = {'data': {}}

    soi = {"salesorderIds": [so_id]}
    soaorder_query = fetch_soaorder_query()
    soaorder = post_api(URL=SOPATH, query=soaorder_query, variables=soi)
    if soaorder and soaorder.get('data'):
        combined_salesorder_data['data']['getSoheaderBySoids'] = soaorder['data'].get('getSoheaderBySoids', [])

    salesorder_query = fetch_salesorder_query(so_id)
    salesorder = post_api(URL=FID, query=salesorder_query, variables=None)
    if salesorder and salesorder.get('data'):
        combined_salesorder_data['data']['getBySalesorderids'] = salesorder['data'].get('getBySalesorderids', [])

    return combined_salesorder_data

def combined_fulfillment_fetch(fulfillment_id):
    combined_fullfillment_data = {'data': {}}

    variables = {"fulfillment_id": fulfillment_id}

    fulfillment_query = fetch_fulfillment_query()
    fulfillment_data = post_api(URL=SOPATH, query=fulfillment_query, variables=variables)
    if fulfillment_data and fulfillment_data.get('data'):
        combined_fullfillment_data['data']['getFulfillmentsById'] = fulfillment_data['data'].get('getFulfillmentsById', {})

    sofulfillment_query = fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id)
    sofulfillment_data = post_api(URL=SOPATH, query=sofulfillment_query, variables=variables)
    if sofulfillment_data and sofulfillment_data.get('data'):
        combined_fullfillment_data['data']['getFulfillmentsBysofulfillmentid'] = sofulfillment_data['data'].get('getFulfillmentsBysofulfillmentid', {})

    directship_query = fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id)
    directship_data = post_api(URL=FOID, query=directship_query, variables=variables)
    if directship_data and directship_data.get('data'):
        combined_fullfillment_data['data']['getAllFulfillmentHeadersSoidFulfillmentid'] = directship_data['data'].get('getAllFulfillmentHeadersSoidFulfillmentid', {})

    fbom_query = fetch_getFbomBySoFulfillmentid_query(fulfillment_id)
    fbom_data = post_api(URL=FFBOM, query=fbom_query, variables=variables)
    if fbom_data and fbom_data.get('data'):
        combined_fullfillment_data['data']['getFbomBySoFulfillmentid'] = fbom_data['data'].get('getFbomBySoFulfillmentid', {})

    return combined_fullfillment_data

def fileldValidation(filters, format_type, region):
    data_row_export = {}
    primary_in_filters = []
    secondary_in_filters = []

    for field in filters:
        if field in PRIMARY_FIELDS:
            primary_in_filters.append(field)
        elif field in SECONDARY_FIELDS:
            secondary_in_filters.append(field)

    if not primary_in_filters:
        return {
            "status": "error",
            "message": "At least one primary field is required in filters."
        }

    primary_filters = {key: filters[key] for key in primary_in_filters}
    secondary_filters = {key: filters[key] for key in secondary_in_filters}
    result_map = {}

    if 'Sales_Order_id' in primary_filters:
        so_ids = list(set(x.strip() for x in primary_filters['Sales_Order_id'].split(',') if x.strip()))
        salesorder_results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(combined_salesorder_fetch, soid) for soid in so_ids]
            for future in as_completed(futures):
                try:
                    res = future.result()
                    if res:
                        salesorder_results.append(res)
                except Exception as e:
                    print(f"Error in SalesOrder Id fetch: {e}")
        result_map['Sales_Order_id'] = salesorder_results

    if 'Fullfillment Id' in primary_filters:
        ff_ids = list(set(x.strip() for x in primary_filters['Fullfillment Id'].split(',') if x.strip()))
        fullfillment_results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(combined_fulfillment_fetch, fid) for fid in ff_ids]
            for future in as_completed(futures):
                try:
                    res = future.result()
                    if res:
                        fullfillment_results.append(res)
                except Exception as e:
                    print(f"Error in Fullfillment Id fetch: {e}")
        result_map['Fullfillment Id'] = fullfillment_results

    print("Sales Order Result Count:", len(result_map.get('Sales_Order_id', [])))
    print("Fulfillment Result Count:", len(result_map.get('Fullfillment Id', [])))

    return {
        "status": "success",
        "message": "Validation and fetch completed.",
        "result_summary": {key: f"{len(val)} response(s)" for key, val in result_map.items()}
    }

if __name__ == "__main__":
    region = "EMEA"
    format_type = 'grid'
    filters = {
        "Sales_Order_id": "1004543337,1004543337,1004543337,1004543337",
        "foid": "7336030653629440001",
        "Fullfillment Id": "262135,262136",
        "wo_id": "7360928459,7360928460,7360970693",
        "Sales_order_ref": "REF123456",
        "Order_create_date": "2025-07-15",
        "ISMULTIPACK": "Yes",
        "BUID": "202",
        "Facility": "WH_BANGALORE",
        "Manifest_ID": "MANI0001",
        "order_date": "2025-07-15"
    }

    result = fileldValidation(filters=filters, format_type=format_type, region=region)
    print(json.dumps(result, indent=2))
