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
        print(f"\n--- QUERY TO {URL} ---\n{query}\nVARIABLES:\n{variables}\n")
        response = httpx.post(URL, json={"query": query, "variables": variables}, verify=False)
        return response.json()
    except Exception as e:
        print(f"Exception in post_api: {e}")
        return {"error": str(e)}

def threaded_fetch(query_func_with_param, var_key, values_list, url):
    results = []

    def call_api(val):
        query = query_func_with_param(val)
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

def combined_fulfillment_fetch(fulfillment_id):
    combined_data = {'data': {}}

    fulfillment_query = fetch_fulfillment_query(fulfillment_id)
    fulfillment_data = post_api(URL=SOPATH, query=fulfillment_query, variables={"fulfillment_id": fulfillment_id})
    if fulfillment_data and fulfillment_data.get('data'):
        combined_data['data']['getFulfillmentsById'] = fulfillment_data['data']['getFulfillmentsById']

    sofulfillment_query = fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id)
    sofulfillment_data = post_api(URL=SOPATH, query=sofulfillment_query, variables={"sofulfillmentid": fulfillment_id})
    if sofulfillment_data and sofulfillment_data.get('data'):
        combined_data['data']['getFulfillmentsBysofulfillmentid'] = sofulfillment_data['data']['getFulfillmentsBysofulfillmentid']

    directship_query = fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id)
    directship_data = post_api(URL=FOID, query=directship_query, variables={"soid": "dummy", "fulfillmentid": fulfillment_id})
    if directship_data and directship_data.get('data'):
        combined_data['data']['getAllFulfillmentHeadersSoidFulfillmentid'] = directship_data['data']['getAllFulfillmentHeadersSoidFulfillmentid']

    fbom_query = fetch_getFbomBySoFulfillmentid_query(fulfillment_id)
    fbom_data = post_api(URL=FFBOM, query=fbom_query, variables={"sofulfillmentid": fulfillment_id})
    if fbom_data and fbom_data.get('data'):
        combined_data['data']['getFbomBySoFulfillmentid'] = fbom_data['data']['getFbomBySoFulfillmentid']

    return combined_data

def fileldValidation(filters, format_type, region):
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
    export_rows = []

    if 'Fullfillment Id' in primary_filters:
        ff_ids = list(set(x.strip() for x in primary_filters['Fullfillment Id'].split(',') if x.strip()))
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(combined_fulfillment_fetch, fid) for fid in ff_ids]
            for future in as_completed(futures):
                try:
                    combined_data = future.result()
                    header = combined_data.get("data", {}).get("getFulfillmentsById", {})
                    address = combined_data.get("data", {}).get("getFulfillmentsBysofulfillmentid", {}).get("address", [{}])[0]
                    fulfillment = combined_data.get("data", {}).get("getFulfillmentsBysofulfillmentid", {})
                    forderline = combined_data.get("data", {}).get("getAllFulfillmentHeadersSoidFulfillmentid", [{}])[0]

                    row = {
                        "BUID": header.get("buid", ""),
                        "PP Date": header.get("ppDate", ""),
                        "Sales Order Id": header.get("salesOrderId", ""),
                        "Fulfillment Id": header.get("fulfillmentId", ""),
                        "Region Code": header.get("region", ""),
                        "FoId": forderline.get("foId", ""),
                        "System Qty": fulfillment.get("systemQty", ""),
                        "Ship By Date": fulfillment.get("shipByDate", ""),
                        "LOB": fulfillment.get("salesOrderLines", [{}])[0].get("lob", ""),
                        "Ship From Facility": forderline.get("shipFromFacility", ""),
                        "Ship To Facility": forderline.get("shipToFacility", ""),
                        "Tax Regstrn Num": address.get("taxRegstrnNum", ""),
                        "Address Line1": address.get("addressLine1", ""),
                        "Postal Code": address.get("postalCode", ""),
                        "State Code": address.get("stateCode", ""),
                        "City Code": address.get("cityCode", ""),
                        "Customer Num": address.get("customerNum", ""),
                        "Customer NameExt": address.get("customerNameExt", ""),
                        "Country": address.get("country", ""),
                        "Create Date": address.get("createDate", ""),
                        "Ship Code": fulfillment.get("shipCode", ""),
                        "Must Arrive By Date": fulfillment.get("mustArriveByDate", ""),
                        "Update Date": fulfillment.get("updateDate", ""),
                        "Merge Type": fulfillment.get("mergeType", ""),
                        "Manifest Date": fulfillment.get("manifestDate", ""),
                        "Revised Delivery Date": fulfillment.get("revisedDeliveryDate", ""),
                        "Delivery City": fulfillment.get("deliveryCity", ""),
                        "Source System Id": fulfillment.get("sourceSystemId", ""),
                        "IsDirect Ship": fulfillment.get("isDirectShip", ""),
                        "SSC": fulfillment.get("ssc", ""),
                        "OIC Id": fulfillment.get("oicId", ""),
                        "Order Date": header.get("orderDate", ""),
                        "SN Number": None,
                        "wo_ids": []
                    }
                    export_rows.append(row)
                except Exception as e:
                    print(f"Error building export row: {e}")

    return {
        "status": "success",
        "message": "Validation and export data build completed.",
        "rows": export_rows
    }

if __name__ == "__main__":
    region = "EMEA"
    format_type = 'grid'
    filters = {
        "Sales_Order_id": "1004543337,1004543337",
        "foid": "FO999999,1004543337",
        "Fullfillment Id": "262135,262136",
        "wo_id": "7360928459,7360928460",
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
