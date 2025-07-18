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

# Load configuration
configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

FID     = configPath['Linkage_DAO']
FOID    = configPath['FM_Order_DAO']
SOPATH  = configPath['SO_Header_DAO']
WOID    = configPath['WO_Details_DAO']
FFBOM   = configPath['FM_BOM_DAO']


def post_api(URL, query, variables):
    if variables:
        response = httpx.post(URL, json={"query": query, "variables": variables}, verify=False)
    else:
        response = httpx.post(URL, json={"query": query}, verify=False)
    return response.json()


def fetch_all_data(salesorderid):
    try:
        data_list = getbySalesOrderIDs(salesorderid=salesorderid, format_type='export')
        for row in data_list:
            # Enrich with Facility and other secondary fields if available
            for wo in row.get("wo_ids", []):
                row["Facility"] = wo.get("Facility", "")
                row["ISMULTIPACK"] = wo.get("Ismultipack", "")
                row["Manifest ID"] = row.get("Manifest Date", "")
        return data_list
    except Exception as e:
        print(f"Error in fetch_all_data for {salesorderid}: {e}")
        return []


def filter_combination(data, filters):
    def match(record):
        for key, value in filters.items():
            filter_vals = [v.strip() for v in value.split(',') if v.strip()]
            record_val = record.get(key)

            if record_val is None:
                return False

            if isinstance(record_val, str):
                if record_val not in filter_vals:
                    return False

            elif isinstance(record_val, list):
                if not any(str(item) in filter_vals for item in record_val):
                    return False

            else:
                if str(record_val) not in filter_vals:
                    return False
        return True

    return [record for record in data if match(record)]


def format_output(filtered_data, format_type, region):
    if format_type == "export":
        return json.dumps(filtered_data)
    elif format_type == "grid":
        return json.dumps(tablestructural(data=filtered_data, IsPrimary=region))
    else:
        return json.dumps({"error": "Invalid format type. Use 'export' or 'grid'."})


def getFilteredSalesOrders(filters, format_type, region):
    salesorder_ids = filters.get("Sales Order Id")
    salesorder_list = []

    if salesorder_ids:
        salesorder_list = [x.strip() for x in salesorder_ids.split(',') if x.strip()]
    else:
        return {"error": "Sales Order Id is required to start fetch process."}

    all_data = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_all_data, soid) for soid in salesorder_list]
        for future in as_completed(futures):
            result = future.result()
            all_data.extend(result)

    filtered = filter_combination(all_data, filters)

    return format_output(filtered, format_type, region)


# Example for local testing:
# if __name__ == '__main__':
#     filters = {
#         "Sales Order Id": "123456,2323433543",
#         "FoId": "F72348",
#         "Facility": "3647gjhsgbfhs",
#         "Fulfillment Id": "123456,2323433543"
#     }
#     format_type = 'grid'
#     region = 'DAO'
#     output = getFilteredSalesOrders(filters, format_type, region)
#     print(output)
