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

# Load Config
configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

# API Paths
FID = configPath['Linkage_DAO']
FOID = configPath['FM_Order_DAO']
SOPATH = configPath['SO_Header_DAO']
WOID = configPath['WO_Details_DAO']
FFBOM = configPath['FM_BOM_DAO']

def post_api(URL, query, variables=None):
    response = httpx.post(URL, json={"query": query, "variables": variables} if variables else {"query": query}, verify=False)
    return response.json()

def flatten_dict(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            if all(isinstance(i, dict) for i in v):
                for i, elem in enumerate(v):
                    items.extend(flatten_dict(elem, f"{new_key}[{i}]", sep=sep).items())
            else:
                for i, elem in enumerate(v):
                    items.append((f"{new_key}[{i}]", elem))
        else:
            items.append((new_key, v))
    return dict(items)

def get_by_combination(filters: dict, region: str, format_type: str = "export"):
    data = []

    if sales_order_id := filters.get("Sales_Order_id"):
        query = fetch_salesorder_query(sales_order_id)
        response = post_api(SOPATH, query)
        if response and response.get("data"):
            data.append(response["data"])

    if wo_id := filters.get("wo_id"):
        query = fetch_workOrderId_query(wo_id)
        response = post_api(WOID, query)
        if response and response.get("data"):
            data.append(response["data"])

    if fulfillment_id := filters.get("Fullfillment Id"):
        query = fetch_fulfillment_query()
        response = post_api(SOPATH, query, {"fulfillment_id": fulfillment_id})
        if response and response.get("data"):
            data.append(response["data"])

    if foid := filters.get("foid"):
        query = fetch_foid_query(foid)
        response = post_api(FOID, query)
        if response and response.get("data"):
            data.append(response["data"])

    if manifest_id := filters.get("Manifest ID"):
        query = fetch_getAsn_query(manifest_id)
        response = post_api(FID, query)
        if response and response.get("data"):
            data.append(response["data"])

    if sn_number := filters.get("SN Number"):
        query = fetch_getAsnbySn_query(sn_number)
        response = post_api(FID, query)
        if response and response.get("data"):
            data.append(response["data"])

    if order_date := filters.get("order_date"):
        try:
            from_date, to_date = order_date.split(" to ")
            query = fetch_getOrderDate_query(from_date.strip(), to_date.strip())
            response = post_api(SOPATH, query)
            if response and response.get("data"):
                data.append(response["data"])
        except Exception as e:
            print("Invalid order_date range format:", e)

    flat_data = []
    for record in data:
        flat = flatten_dict(record)
        flat_data.append(flat)

    flat_data = [{k: ("" if v is None else v) for k, v in row.items()} for row in flat_data]
    if format_type == "export":
        return json.dumps(flat_data, indent=2)
    elif format_type == "grid":
        return tablestructural(data=flat_data, IsPrimary=region)
    else:
        return {"error": "Invalid format type"}

def apply_filters(data_list, filters):
    if not filters:
        return data_list

    def match(record):
        for key, value in filters.items():
            values = [v.strip() for v in value.split(',')]
            if str(record.get(key, '')).strip() not in values:
                return False
        return True

    return [item for item in data_list if match(item)]

def getbySalesOrderIDs(salesorderid, format_type):
    from .data_collector import collect_data
    return collect_data(salesorderid, format_type)

def tablestructural(data, IsPrimary):
    unique_keys = list({key for row in data for key in row.keys()})
    columns = [
        {
            "checked": True if key in ["BUID", "PP Date", "Sales Order Id", "Fulfillment Id", "System Qty", "Ship By Date", "LOB", "Ship From Facility", "Ship To Facility"] else False,
            "group": "ID" if "Id" in key or "Num" in key else "Date" if "Date" in key else "Address" if "Address" in key else "Facility" if "Facility" in key else "Code" if "Code" in key else "Flag" if "Is" in key else "Other",
            "isPrimary": True if key in ["BUID", "PP Date", "Sales Order Id", "Fulfillment Id", "System Qty", "Ship By Date", "LOB", "Ship From Facility", "Ship To Facility"] else False,
            "sortBy": "ascending",
            "value": key
        } for key in unique_keys
    ]

    data_rows = [{"columns": [{"value": row.get(col["value"], "")} for col in columns]} for row in data]

    return {
        "columns": columns,
        "data": data_rows,
        "logs": {
            "urls": [],
            "time": []
        }
    }

def getbySalesOrderID(salesorderid, format_type, region, filters=None):
    if filters:
        return get_by_combination(filters, region, format_type)

    total_output = []
    def fetch_order(so_id):
        try:
            return getbySalesOrderIDs(salesorderid=so_id, format_type=format_type)
        except Exception as e:
            print(f"Error for {so_id}: {e}")
            return []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_order, sid) for sid in salesorderid]
        for future in as_completed(futures):
            total_output.extend(future.result())

    total_output = [{k: ("" if v is None else v) for k, v in row.items()} for row in total_output]
    total_output = apply_filters(total_output, filters)

    if format_type == "export":
        return json.dumps(total_output, indent=2)
    elif format_type == "grid":
        table_grid_output = tablestructural(data=total_output, IsPrimary=region)
        return table_grid_output
    else:
        return {"error": "Invalid format type"}

if __name__ == "__main__":
    output = getbySalesOrderID(
        salesorderid=["1004452326", "1004543337"],
        format_type="export",
        region="EMEA",
        filters={
            "Fullfillment Id": "262135",
            "wo_id": "7360928459"
        }
    )
    print("Export Output")
    print(output)
