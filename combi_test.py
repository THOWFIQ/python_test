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

    def match_optional_fields(entry):
        for key, expected in filters.items():
            if key == "ISMULTIPACK":
                if not any(line.get("ismultipack") == expected for line in entry.get("woLines", [])):
                    return False
            elif key == "BUID" and entry.get("buid") != expected:
                return False
            elif key == "Facility":
                facilities = (entry.get("shipFromFacility"), entry.get("shipToFacility"))
                if expected not in facilities:
                    return False
            elif key == "Order create_date":
                if entry.get("createDate") != expected:
                    return False
            elif key == "Sales_order_ref":
                if entry.get("soHeaderRef") != expected:
                    return False
            elif key == "FullfillmentID":
                if entry.get("fulfillmentId") != expected:
                    return False
            elif key == "WorkOrderID":
                if entry.get("woId") != expected:
                    return False
        return True

    flat_data = []
    for item in data:
        if isinstance(item, dict):
            for key, val in item.items():
                if isinstance(val, list):
                    for rec in val:
                        if match_optional_fields(rec):
                            flat_data.append(rec)
                elif isinstance(val, dict):
                    if match_optional_fields(val):
                        flat_data.append(val)

    return flat_data

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
    combined_data = fetch_and_clean(salesorderid)
    if not combined_data:
        return []

    soheader = combined_data["data"]["getSoheaderBySoids"][0]
    result = combined_data["data"]["getBySalesorderids"]["result"][0]
    fulfillment = combined_data["data"]["getFulfillmentsById"][0]["fulfillments"][0]
    forderline = combined_data["data"]["getAllFulfillmentHeadersByFoId"][0]["forderline"][0]
    getFulfillmentsBysofulfillmentid = combined_data["data"]["getFulfillmentsBysofulfillmentid"][0]["fulfillments"][0]
    sourceSystemId = combined_data["data"]["getFulfillmentsBysofulfillmentid"][0]["sourceSystemId"]
    isDirectShip = combined_data["data"]["getAllFulfillmentHeadersSoidFulfillmentid"][0]["isDirectShip"]
    ssc = combined_data["data"]["getFbomBySoFulfillmentid"][0]["ssc"]

    wo_ids = [wo for wo in result["workOrders"]]
    base = {
        "BUID": soheader["buid"],
        "PP Date": soheader["ppDate"],
        "Sales Order Id": result["salesOrder"]["salesOrderId"],
        "Fulfillment Id": fulfillment.get("fulfillmentId"),
        "Region Code": result["salesOrder"]["region"],
        "FoId": result["fulfillmentOrders"][0]["foId"],
        "System Qty": fulfillment["systemQty"],
        "Ship By Date": fulfillment["shipByDate"],
        "LOB": fulfillment["salesOrderLines"][0]["lob"],
        "Ship From Facility": forderline["shipFromFacility"],
        "Ship To Facility": forderline["shipToFacility"],
        "Tax Regstrn Num": getFulfillmentsBysofulfillmentid["address"][0]["taxRegstrnNum"],
        "Address Line1": getFulfillmentsBysofulfillmentid["address"][0]["addressLine1"],
        "Postal Code": getFulfillmentsBysofulfillmentid["address"][0]["postalCode"],
        "State Code": getFulfillmentsBysofulfillmentid["address"][0]["stateCode"],
        "City Code": getFulfillmentsBysofulfillmentid["address"][0]["cityCode"],
        "Customer Num": getFulfillmentsBysofulfillmentid["address"][0]["customerNum"],
        "Customer Name Ext": getFulfillmentsBysofulfillmentid["address"][0]["customerNameExt"],
        "Country": getFulfillmentsBysofulfillmentid["address"][0]["country"],
        "Create Date": getFulfillmentsBysofulfillmentid["address"][0]["createDate"],
        "Ship Code": getFulfillmentsBysofulfillmentid["shipCode"],
        "Must Arrive By Date": getFulfillmentsBysofulfillmentid["mustArriveByDate"],
        "Update Date": getFulfillmentsBysofulfillmentid["updateDate"],
        "Merge Type": getFulfillmentsBysofulfillmentid["mergeType"],
        "Manifest Date": getFulfillmentsBysofulfillmentid["manifestDate"],
        "Revised Delivery Date": getFulfillmentsBysofulfillmentid["revisedDeliveryDate"],
        "Delivery City": getFulfillmentsBysofulfillmentid["deliveryCity"],
        "Source System Id": sourceSystemId,
        "IsDirect Ship": isDirectShip,
        "SSC": ssc,
        "OIC Id": getFulfillmentsBysofulfillmentid["oicId"],
        "Order Date": soheader["orderDate"]
    }

    flat_list = []
    for wo in wo_ids:
        sn_numbers = wo.get("SN Number", [])
        flat_wo = {k: v for k, v in wo.items() if k != "SN Number"}
        if sn_numbers:
            for sn in sn_numbers:
                flat_list.append({**base, **flat_wo, "SN Number": sn})
        else:
            flat_list.append({**base, **flat_wo, "SN Number": None})

    return flat_list

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
        print(json.dumps(total_output, indent=2))
        return json.dumps(total_output, indent=2)
    elif format_type == "grid":
        table_grid_output = tablestructural(data=total_output, IsPrimary=region)
        print(json.dumps(table_grid_output, indent=2))
        return table_grid_output
    else:
        return {"error": "Invalid format type"}

if __name__ == "__main__":
    output = getbySalesOrderID(
        salesorderid=["1004452326", "1004543337"],
        format_type="grid",
        region="EMEA",
        filters={
            "FullfillmentID": "262135",
            "WorkOrderID": "7360928459"
        }
    )
