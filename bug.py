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

FID    = configPath['Linkage_DAO']
FOID   = configPath['FM_Order_DAO']
SOPATH = configPath['SO_Header_DAO']
WOID   = configPath['WO_Details_DAO']
FFBOM  = configPath['FM_BOM_DAO']

def post_api(URL, query, variables):
    try:
        if variables:
            response = httpx.post(SOPATH, json={"query": query, "variables": variables}, verify=False)
        else:
            response = httpx.post(URL, json={"query": query}, verify=False)
        return response.json()
    except Exception as e:
        print(f"API error: {e}")
        return {}

def safe_get(data, keys, default=None):
    for key in keys:
        if isinstance(data, list):
            if not data:
                return default
            data = data[0]
        if isinstance(data, dict):
            data = data.get(key)
            if data is None:
                return default
        else:
            return default
    return data

def fetch_and_clean(salesorderIds):
    combined_data = {'data': {}}
    soi = {"salesorderIds": [salesorderIds]}
    soaorder_query = fetch_soaorder_query()
    soaorder = post_api(SOPATH, soaorder_query, soi)
    if soaorder and soaorder.get('data'):
        combined_data['data']['getSoheaderBySoids'] = soaorder['data']['getSoheaderBySoids']

    salesorder_query = fetch_salesorder_query(salesorderIds)
    salesorder = post_api(FID, salesorder_query, None)
    if salesorder and salesorder.get('data'):
        combined_data['data']['getBySalesorderids'] = salesorder['data']['getBySalesorderids']

    result_list = salesorder.get("data", {}).get("getBySalesorderids", {}).get("result", [])
    if not result_list:
        return None

    work_orders = result_list[0].get("workOrders", [])
    for wo in work_orders:
        woId = wo.get("woId")
        if not woId:
            continue

        workOrderId_query = fetch_workOrderId_query(woId)
        getWorkOrderById = post_api(WOID, workOrderId_query, None)

        getByWorkorderids_query = fetch_getByWorkorderids_query(woId)
        getByWorkorderids = post_api(FID, getByWorkorderids_query, None)

        sn_numbers = [
            sn.get("snNumber")
            for sn in getByWorkorderids.get("data", {}).get("getByWorkorderids", {}).get("result", [])[0].get("asnNumbers", [])
            if sn.get("snNumber")
        ] if getByWorkorderids.get("data") else []

        if getWorkOrderById.get('data'):
            wo_detail = getWorkOrderById["data"].get("getWorkOrderById", [{}])[0]
            flat_wo = {
                "Vendor Work Order Num": wo_detail.get("woId"),
                "Channel Status Code": wo_detail.get("channelStatusCode"),
                "Ismultipack": safe_get(wo_detail, ["woLines", 0, "ismultipack"]),
                "Ship Mode": wo_detail.get("shipMode"),
                "Is Otm Enabled": wo_detail.get("isOtmEnabled"),
                "SN Number": sn_numbers
            }
            for i, w in enumerate(work_orders):
                if w.get("woId") == wo_detail.get("woId"):
                    work_orders[i] = flat_wo.copy()

    fulfillment_raw = result_list[0].get("fulfillment")
    fulfillment_id = (
        fulfillment_raw[0].get("fulfillmentId") if isinstance(fulfillment_raw, list) and fulfillment_raw else
        fulfillment_raw.get("fulfillmentId") if isinstance(fulfillment_raw, dict) else None
    )
    if not fulfillment_id:
        return combined_data

    fulfillment_query = fetch_fulfillment_query()
    fulfillment = post_api(SOPATH, fulfillment_query, {"fulfillment_id": fulfillment_id})
    if fulfillment and fulfillment.get('data'):
        combined_data['data']['getFulfillmentsById'] = fulfillment['data']['getFulfillmentsById']

    sofulfill_query = fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id)
    sofulfill = post_api(SOPATH, sofulfill_query, None)
    if sofulfill.get('data'):
        combined_data['data']['getFulfillmentsBysofulfillmentid'] = sofulfill['data']['getFulfillmentsBysofulfillmentid']

    header_query = fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id)
    header = post_api(FOID, header_query, None)
    if header.get('data'):
        combined_data['data']['getAllFulfillmentHeadersSoidFulfillmentid'] = header['data']['getAllFulfillmentHeadersSoidFulfillmentid']

    fbom_query = fetch_getFbomBySoFulfillmentid_query(fulfillment_id)
    fbom = post_api(FFBOM, fbom_query, None)
    if fbom.get('data'):
        combined_data['data']['getFbomBySoFulfillmentid'] = fbom['data']['getFbomBySoFulfillmentid']

    fulfillment_orders = result_list[0].get("fulfillmentOrders", [])
    if fulfillment_orders and fulfillment_orders[0].get("foId"):
        fo_id = fulfillment_orders[0].get("foId")
        foid_query = fetch_foid_query(fo_id)
        foid_output = post_api(FOID, foid_query, None)
        if foid_output.get('data'):
            combined_data['data']['getAllFulfillmentHeadersByFoId'] = foid_output['data']['getAllFulfillmentHeadersByFoId']

    return combined_data

def getbySalesOrderIDs(salesorderid, format_type):
    combined_data = fetch_and_clean(salesorderIds=salesorderid)
    if not combined_data:
        return []

    soheader = safe_get(combined_data, ["data", "getSoheaderBySoids", 0], {})
    result = safe_get(combined_data, ["data", "getBySalesorderids", "result", 0], {})

    fulfillment_raw = result.get("fulfillment")
    fulfillment_id = (
        fulfillment_raw[0].get("fulfillmentId") if isinstance(fulfillment_raw, list) and fulfillment_raw else
        fulfillment_raw.get("fulfillmentId") if isinstance(fulfillment_raw, dict) else None
    )

    fulfillment = safe_get(combined_data, ["data", "getFulfillmentsById", 0, "fulfillments", 0], {})
    forderline = safe_get(combined_data, ["data", "getAllFulfillmentHeadersByFoId", 0, "forderline", 0], {})
    sofulfill = safe_get(combined_data, ["data", "getFulfillmentsBysofulfillmentid", 0, "fulfillments", 0], {})
    sourceSystemId = safe_get(combined_data, ["data", "getFulfillmentsBysofulfillmentid", 0, "sourceSystemId"])
    isDirectShip = safe_get(combined_data, ["data", "getAllFulfillmentHeadersSoidFulfillmentid", 0, "isDirectShip"])
    ssc = safe_get(combined_data, ["data", "getFbomBySoFulfillmentid", 0, "ssc"])

    wo_ids = result.get("workOrders", [])

    base = {
        "BUID": soheader.get("buid"),
        "PP Date": soheader.get("ppDate"),
        "Sales Order Id": result.get("salesOrder", {}).get("salesOrderId"),
        "Fulfillment Id": fulfillment_id,
        "Region Code": result.get("salesOrder", {}).get("region"),
        "FoId": safe_get(result, ["fulfillmentOrders", 0, "foId"]),
        "System Qty": fulfillment.get("systemQty"),
        "Ship By Date": fulfillment.get("shipByDate"),
        "LOB": safe_get(fulfillment, ["salesOrderLines", 0, "lob"]),
        "Ship From Facility": forderline.get("shipFromFacility"),
        "Ship To Facility": forderline.get("shipToFacility"),
        "Tax Regstrn Num": safe_get(sofulfill, ["address", 0, "taxRegstrnNum"]),
        "Address Line1": safe_get(sofulfill, ["address", 0, "addressLine1"]),
        "Postal Code": safe_get(sofulfill, ["address", 0, "postalCode"]),
        "State Code": safe_get(sofulfill, ["address", 0, "stateCode"]),
        "City Code": safe_get(sofulfill, ["address", 0, "cityCode"]),
        "Customer Num": safe_get(sofulfill, ["address", 0, "customerNum"]),
        "Customer Name Ext": safe_get(sofulfill, ["address", 0, "customerNameExt"]),
        "Country": safe_get(sofulfill, ["address", 0, "country"]),
        "Create Date": safe_get(sofulfill, ["address", 0, "createDate"]),
        "Ship Code": sofulfill.get("shipCode"),
        "Must Arrive By Date": sofulfill.get("mustArriveByDate"),
        "Update Date": sofulfill.get("updateDate"),
        "Merge Type": sofulfill.get("mergeType"),
        "Manifest Date": sofulfill.get("manifestDate"),
        "Revised Delivery Date": sofulfill.get("revisedDeliveryDate"),
        "Delivery City": sofulfill.get("deliveryCity"),
        "Source System Id": sourceSystemId,
        "Is Direct Ship": isDirectShip,
        "SSC": ssc,
        "OIC ID": sofulfill.get("oicId"),
        "Order Date": soheader.get("orderDate")
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

def getbyOrderDateRange(orderFromDate, orderToDate, format_type, region):
    total_output = []
    query = fetch_getOrderDate_query(orderFromDate, orderToDate)
    response = post_api(SOPATH, query=query, variables=None)
    records = response.get("data", {}).get("getOrdersByDate", {}).get("result", [])
    if not records:
        return {"message": "No records for date range."}

    def fetch_by_id(record):
        try:
            salesorderid = record.get("salesOrderId")
            return getbySalesOrderIDs(salesorderid, format_type)
        except Exception as e:
            print(f"Error fetching order ID {record.get('salesOrderId')}: {e}")
            return []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_by_id, r) for r in records]
        for future in as_completed(futures):
            try:
                result = future.result()
                if isinstance(result, list):
                    total_output.extend(result)
            except Exception as e:
                print(f"Thread error: {e}")

    if format_type == "export":
        return json.dumps(total_output, indent=2)
    elif format_type == "grid":
        return json.dumps(tablestructural(total_output, IsPrimary=region), indent=2)
    else:
        return {"error": "Invalid format type"}
