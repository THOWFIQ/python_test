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


def fetch_and_clean(salesorderIds):
    combined_data = {'data': {}}
    soi = {"salesorderIds": [salesorderIds]}

    soaorder = post_api(SOPATH, fetch_soaorder_query(), soi)
    if soaorder and soaorder.get('data'):
        combined_data['data']['getSoheaderBySoids'] = soaorder['data']['getSoheaderBySoids']

    salesorder = post_api(FID, fetch_salesorder_query(salesorderIds), None)
    if salesorder and salesorder.get('data'):
        combined_data['data']['getBySalesorderids'] = salesorder['data']['getBySalesorderids']

    result_list = salesorder.get("data", {}).get("getBySalesorderids", {}).get("result", [])
    if not result_list:
        return None

    work_orders = result_list[0]["workOrders"]
    for i in work_orders:
        woId = i["woId"]
        getWorkOrderById = post_api(WOID, fetch_workOrderId_query(woId), None)
        getByWorkorderids = post_api(FID, fetch_getByWorkorderids_query(woId), None)

        sn_numbers = []
        if getByWorkorderids and getByWorkorderids.get('data'):
            sn_numbers = [
                sn.get("snNumber") 
                for sn in getByWorkorderids["data"]["getByWorkorderids"]["result"][0]["asnNumbers"]
                if sn.get("snNumber") is not None
            ]

        if getWorkOrderById and getWorkOrderById.get('data'):
            wo_detail = getWorkOrderById["data"]["getWorkOrderById"][0]
            flattened_wo = {
                "Vendor Work Order Num": wo_detail["woId"],
                "Channel Status Code": wo_detail["channelStatusCode"],
                "Ismultipack": wo_detail["woLines"][0].get("ismultipack"),
                "Ship Mode": wo_detail["shipMode"],
                "Is Otm Enabled": wo_detail["isOtmEnabled"],
                "SN Number": sn_numbers
            }
            for i, wo in enumerate(work_orders):
                if wo.get("woId") == wo_detail["woId"]:
                    work_orders[i] = flattened_wo.copy()

    fulfillment_id = None
    fulfillment_raw = result_list[0].get("fulfillment")
    if isinstance(fulfillment_raw, dict):
        fulfillment_id = fulfillment_raw.get("fulfillmentId")
    elif isinstance(fulfillment_raw, list) and fulfillment_raw:
        fulfillment_id = fulfillment_raw[0].get("fulfillmentId")

    if fulfillment_id:
        fulfillment_data = post_api(SOPATH, fetch_fulfillment_query(), {"fulfillment_id": fulfillment_id})
        if fulfillment_data and fulfillment_data.get('data'):
            combined_data['data']['getFulfillmentsById'] = fulfillment_data['data']['getFulfillmentsById']

        combined_data['data']['getFulfillmentsBysofulfillmentid'] = post_api(
            SOPATH, fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id), None
        )['data']['getFulfillmentsBysofulfillmentid']

        combined_data['data']['getAllFulfillmentHeadersSoidFulfillmentid'] = post_api(
            FOID, fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id), None
        )['data']['getAllFulfillmentHeadersSoidFulfillmentid']

        combined_data['data']['getFbomBySoFulfillmentid'] = post_api(
            FFBOM, fetch_getFbomBySoFulfillmentid_query(fulfillment_id), None
        )['data']['getFbomBySoFulfillmentid']

    fulfillment_orders = result_list[0].get("fulfillmentOrders", [])
    if fulfillment_orders and fulfillment_orders[0].get("foId"):
        fo_id = fulfillment_orders[0]["foId"]
        foid_output = post_api(FOID, fetch_foid_query(fo_id), None)
        if foid_output and foid_output.get('data'):
            combined_data['data']['getAllFulfillmentHeadersByFoId'] = foid_output['data']['getAllFulfillmentHeadersByFoId']

    return combined_data


def getbySalesOrderIDs(salesorderid, format_type):
    data_row_export = {}
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


def getbySalesOrderID(salesorderid, format_type, region, filters=None):
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
        return tablestructural(data=total_output, IsPrimary=region)
    else:
        return {"error": "Invalid format type"}

output = getbySalesOrderID(
    salesorderid=["1004452326", "1004543337"],
    format_type="export",
    region="EMEA",
    filters={
        "Sales Order Id": "1004452326,1004543337",
        "FoId": "F72348",
        "Facility": "3647gjhsgbfhs"
    }
)

print(output)

