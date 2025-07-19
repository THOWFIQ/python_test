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

# Define field categories
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

# Common POST call
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

# Reusable threaded fetcher
def threaded_fetch(query_func, var_key, values_list, url):
    query = query_func()
    results = []

    def call_api(val):
        variables = {var_key: [val]}
        return post_api(URL=url, query=query, variables=variables)

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_value = {executor.submit(call_api, val): val for val in values_list}
        for future in as_completed(future_to_value):
            val = future_to_value[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Error for {var_key} = {val}: {e}")
    return results

# Full logic
def fileldValidation(filters, format_type, region):
    primary_in_filters = []
    secondary_in_filters = []

    # Step 2: Categorize fields
    for field in filters:
        if field in PRIMARY_FIELDS:
            primary_in_filters.append(field)
        elif field in SECONDARY_FIELDS:
            secondary_in_filters.append(field)

    # Step 3: Validation
    if not primary_in_filters:
        return {
            "status": "error",
            "message": "At least one primary field is required in filters."
        }

    primary_filters = {key: filters[key] for key in primary_in_filters}
    secondary_filters = {key: filters[key] for key in secondary_in_filters}

    result_map = {}

    # Step 4: Threaded API calls for each supported field
    if 'Sales_Order_id' in primary_filters:
        so_ids = list(set(x.strip() for x in primary_filters['Sales_Order_id'].split(',') if x.strip()))
        result_map['Sales_Order_id'] = threaded_fetch(fetch_salesorder_query, "salesorderIds", so_ids, SOPATH)

    if 'foid' in primary_filters:
        foids = list(set(x.strip() for x in primary_filters['foid'].split(',') if x.strip()))
        result_map['foid'] = threaded_fetch(fetch_foid_query, "foids", foids, FOID)

    if 'wo_id' in primary_filters:
        woids = list(set(x.strip() for x in primary_filters['wo_id'].split(',') if x.strip()))
        result_map['wo_id'] = threaded_fetch(fetch_workOrderId_query, "woIds", woids, WOID)
        here i need to call 
        fetch_workOrderId_query
        fetch_getByWorkorderids_query

    if 'Fullfillment Id' in primary_filters:
        ffs = list(set(x.strip() for x in primary_filters['Fullfillment Id'].split(',') if x.strip()))

        ( here i need to call 
        
        combined_data = {'data': {}}
    variables = {"fulfillment_id": fulfillment_id}

    fulfillment_query = fetch_fulfillment_query()
    fulfillment_data = post_api(URL=SOPATH, query=fulfillment_query, variables=variables)
    if fulfillment_data and fulfillment_data.get('data'):
        combined_data['data']['getFulfillmentsById'] = fulfillment_data['data']['getFulfillmentsById']

    sofulfillment_query = fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id)
    sofulfillment_data = post_api(URL=SOPATH, query=sofulfillment_query, variables=None)
    if sofulfillment_data and sofulfillment_data.get('data'):
        combined_data['data']['getFulfillmentsBysofulfillmentid'] = sofulfillment_data['data']['getFulfillmentsBysofulfillmentid']

    directship_query = fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id)
    directship_data = post_api(URL=FOID, query=directship_query, variables=None)
    if directship_data and directship_data.get('data'):
        combined_data['data']['getAllFulfillmentHeadersSoidFulfillmentid'] = directship_data['data']['getAllFulfillmentHeadersSoidFulfillmentid']

    fbom_query = fetch_getFbomBySoFulfillmentid_query(fulfillment_id)
    fbom_data = post_api(URL=FFBOM, query=fbom_query, variables=None)
    if fbom_data and fbom_data.get('data'):
        combined_data['data']['getFbomBySoFulfillmentid'] = fbom_data['data']['getFbomBySoFulfillmentid']

    return combined_data
    
    )
        result_map['Fullfillment Id'] = threaded_fetch(fetch_fm_bom_query, "ffIds", ffs, FFBOM)

    # if 'order_date' in primary_filters:
    #     dates = list(set(x.strip() for x in primary_filters['order_date'].split(',') if x.strip()))
    #     result_map['order_date'] = threaded_fetch(fetch_getOrderDate_query, "orderDates", dates, SOPATH)

    # Debug: Print all results
    for key, res in result_map.items():
        print(f"\nThreaded results for {key}:")
        for r in res:
            print(json.dumps(r, indent=2))

    return {
        "status": "success",
        "message": "Validation and fetch completed.",
        "result_summary": {key: f"{len(val)} response(s)" for key, val in result_map.items()}
    }

# Runner
if __name__ == "__main__":
    region = "EMEA"
    format_type = 'grid'
    filters = {
        "Sales_Order_id": "1004543337,1004543337,1004543337,1004543337",
        "foid": "FO999999,1004543337,1004543337,1004543337,1004543337",
        "Fullfillment Id": "262135,262135,262135,262135,262135,262135",
        "wo_id": "7360928459,7360928459,7360928459,7360928459,7360928459",
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
