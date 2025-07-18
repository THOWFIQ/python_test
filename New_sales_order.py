from flask import request, jsonify
import httpx
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from graphqlQueries import *

# Load global config once
CONFIG = load_config()

def post_api(URL, query, variables=None):
    response = httpx.post(URL, json={"query": query, "variables": variables} if variables else {"query": query}, verify=False)
    return response.json()

def fetch_and_clean(salesorder_id, region):
    combined_data = {'data': {}}
    
    # Region-specific endpoints
    FID = get_path(region, "FID", CONFIG)
    FOID = get_path(region, "FOID", CONFIG)
    SOPATH = get_path(region, "SOPATH", CONFIG)
    WOID = get_path(region, "WOID", CONFIG)
    FFBOM = get_path(region, "FFBOM", CONFIG)

    soi = {"salesorderIds": [salesorder_id]}

    # Fetch SO Header
    so_header_resp = post_api(SOPATH, fetch_soaorder_query(), soi)
    combined_data['data']['getSoheaderBySoids'] = so_header_resp['data'].get('getSoheaderBySoids', [])

    # Fetch Sales Order
    salesorder_resp = post_api(FID, fetch_salesorder_query(salesorder_id))
    result = salesorder_resp['data'].get('getBySalesorderids', {}).get('result', [])[0]
    combined_data['data']['getBySalesorderids'] = salesorder_resp['data'].get('getBySalesorderids', {})

    # Enrich Work Orders
    for wo in result.get("workOrders", []):
        wo_id = wo["woId"]
        wo_detail = post_api(WOID, fetch_workOrderId_query(wo_id))
        wo_enriched = wo_detail["data"]["getWorkOrderById"][0]

        sn_resp = post_api(FID, fetch_getByWorkorderids_query(wo_id))
        sn_numbers = sn_resp["data"]["getByWorkorderids"]["result"][0]["asnNumbers"]
        sn_list = [sn["snNumber"] for sn in sn_numbers if sn.get("snNumber")]

        wo.update({
            "Vendor Work Order Num": wo_enriched["woId"],
            "Channel Status Code": wo_enriched["channelStatusCode"],
            "Ismultipack": wo_enriched["woLines"][0].get("ismultipack"),
            "Ship Mode": wo_enriched["shipMode"],
            "Is Otm Enabled": wo_enriched["isOtmEnabled"],
            "SN Number": sn_list
        })

    # Fulfillment-related
    fulfillment = result.get("fulfillment")
    fulfillment_id = None
    if isinstance(fulfillment, dict):
        fulfillment_id = fulfillment.get("fulfillmentId")
    elif isinstance(fulfillment, list) and fulfillment:
        fulfillment_id = fulfillment[0].get("fulfillmentId")

    if fulfillment_id:
        combined_data["data"]["getFulfillmentsById"] = post_api(SOPATH, fetch_fulfillment_query(), {"fulfillment_id": fulfillment_id})["data"]["getFulfillmentsById"]
        combined_data["data"]["getFulfillmentsBysofulfillmentid"] = post_api(SOPATH, fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id))["data"]["getFulfillmentsBysofulfillmentid"]
        combined_data["data"]["getAllFulfillmentHeadersSoidFulfillmentid"] = post_api(FOID, fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id))["data"]["getAllFulfillmentHeadersSoidFulfillmentid"]
        combined_data["data"]["getFbomBySoFulfillmentid"] = post_api(FFBOM, fetch_getFbomBySoFulfillmentid_query(fulfillment_id))["data"]["getFbomBySoFulfillmentid"]

    # FOID
    fulfillment_orders = result.get("fulfillmentOrders", [])
    if fulfillment_orders:
        foid = fulfillment_orders[0]["foId"]
        fo_output = post_api(FOID, fetch_foid_query(foid))
        combined_data["data"]["getAllFulfillmentHeadersByFoId"] = fo_output["data"]["getAllFulfillmentHeadersByFoId"]

    return combined_data

def flatten_data(data, format_type, region):
    soheader = data["data"]["getSoheaderBySoids"][0]
    result = data["data"]["getBySalesorderids"]["result"][0]

    fulfillment_raw = result.get("fulfillment")
    fulfillment_id = fulfillment_raw["fulfillmentId"] if isinstance(fulfillment_raw, dict) else fulfillment_raw[0]["fulfillmentId"]

    fulfillment = data["data"]["getFulfillmentsById"][0]["fulfillments"][0]
    forderline = data["data"]["getAllFulfillmentHeadersByFoId"][0]["forderline"][0]
    getFulfillmentsByso = data["data"]["getFulfillmentsBysofulfillmentid"][0]["fulfillments"][0]
    sourceSystemId = data["data"]["getFulfillmentsBysofulfillmentid"][0]["sourceSystemId"]
    isDirectShip = data["data"]["getAllFulfillmentHeadersSoidFulfillmentid"][0]["isDirectShip"]
    ssc = data["data"]["getFbomBySoFulfillmentid"][0]["ssc"]

    base_row = {
        "BUID": soheader["buid"],
        "PP Date": soheader["ppDate"],
        "Sales Order Id": result["salesOrder"]["salesOrderId"],
        "Fulfillment Id": fulfillment_id,
        "Region Code": result["salesOrder"]["region"],
        "FoId": result["fulfillmentOrders"][0]["foId"],
        "System Qty": fulfillment["systemQty"],
        "Ship By Date": fulfillment["shipByDate"],
        "LOB": fulfillment["salesOrderLines"][0]["lob"],
        "Ship From Facility": forderline["shipFromFacility"],
        "Ship To Facility": forderline["shipToFacility"],
        "Tax Regstrn Num": getFulfillmentsByso["address"][0]["taxRegstrnNum"],
        "Address Line1": getFulfillmentsByso["address"][0]["addressLine1"],
        "Postal Code": getFulfillmentsByso["address"][0]["postalCode"],
        "State Code": getFulfillmentsByso["address"][0]["stateCode"],
        "City Code": getFulfillmentsByso["address"][0]["cityCode"],
        "Customer Num": getFulfillmentsByso["address"][0]["customerNum"],
        "Customer Name Ext": getFulfillmentsByso["address"][0]["customerNameExt"],
        "Country": getFulfillmentsByso["address"][0]["country"],
        "Create Date": getFulfillmentsByso["address"][0]["createDate"],
        "Ship Code": getFulfillmentsByso["shipCode"],
        "Must Arrive By Date": getFulfillmentsByso["mustArriveByDate"],
        "Update Date": getFulfillmentsByso["updateDate"],
        "Merge Type": getFulfillmentsByso["mergeType"],
        "Manifest Date": getFulfillmentsByso["manifestDate"],
        "Revised Delivery Date": getFulfillmentsByso["revisedDeliveryDate"],
        "Delivery City": getFulfillmentsByso["deliveryCity"],
        "Source System Id": sourceSystemId,
        "IsDirect Ship": isDirectShip,
        "SSC": ssc,
        "OIC Id": getFulfillmentsByso["oicId"],
        "Order Date": soheader["orderDate"]
    }

    flat_output = []
    for wo in result["workOrders"]:
        wo_copy = {**base_row, **{k: v for k, v in wo.items() if k != "SN Number"}}
        sn_list = wo.get("SN Number", [])
        if sn_list:
            for sn in sn_list:
                flat_output.append({**wo_copy, "SN Number": sn})
        else:
            flat_output.append({**wo_copy, "SN Number": None})

    if format_type == "export":
        return flat_output
    elif format_type == "grid":
        return tablestructural(flat_output, region)
    else:
        return {"error": "Invalid format type"}

def getbySalesOrderID(salesorderids, format_type, region):
    if not salesorderids:
        return {"error": "SalesOrderIds required"}
    if format_type not in ["export", "grid"]:
        return {"error": "Invalid format type"}
    if not region:
        return {"error": "Region is required"}

    total_output = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_and_clean, sid, region): sid for sid in salesorderids}
        for future in as_completed(futures):
            try:
                data = future.result()
                flat = flatten_data(data, format_type, region)
                if isinstance(flat, list):
                    total_output.extend(flat)
                elif isinstance(flat, dict) and "columns" in flat:
                    total_output.extend(flat.get("data", []))
            except Exception as e:
                print(f"Error processing {futures[future]}: {e}")

    return json.dumps(total_output, indent=2) if format_type == "export" else json.dumps(tablestructural(total_output, region), indent=2)

# Example usage:
if __name__ == "__main__":
    print(getbySalesOrderID(["1004452326"], "grid", "DAO"))





from flask import request, jsonify
import httpx
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from graphqlQueries import *

# Load global config once
CONFIG = load_config()

def post_api(URL, query, variables=None):
    response = httpx.post(URL, json={"query": query, "variables": variables} if variables else {"query": query}, verify=False)
    return response.json()

def replace_none_with_empty(data):
    if isinstance(data, dict):
        return {k: replace_none_with_empty(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [replace_none_with_empty(item) for item in data]
    elif data is None:
        return ""
    else:
        return data

def fetch_and_clean(salesorder_id, region):
    combined_data = {'data': {}}

    # Region-specific endpoints
    FID = get_path(region, "FID", CONFIG)
    FOID = get_path(region, "FOID", CONFIG)
    SOPATH = get_path(region, "SOPATH", CONFIG)
    WOID = get_path(region, "WOID", CONFIG)
    FFBOM = get_path(region, "FFBOM", CONFIG)

    soi = {"salesorderIds": [salesorder_id]}

    # Fetch SO Header
    so_header_resp = post_api(SOPATH, fetch_soaorder_query(), soi)
    combined_data['data']['getSoheaderBySoids'] = so_header_resp['data'].get('getSoheaderBySoids', [])

    # Fetch Sales Order
    salesorder_resp = post_api(FID, fetch_salesorder_query(salesorder_id))
    result = salesorder_resp['data'].get('getBySalesorderids', {}).get('result', [])[0]
    combined_data['data']['getBySalesorderids'] = salesorder_resp['data'].get('getBySalesorderids', {})

    # Enrich Work Orders
    for wo in result.get("workOrders", []):
        wo_id = wo["woId"]
        wo_detail = post_api(WOID, fetch_workOrderId_query(wo_id))
        wo_enriched = wo_detail["data"]["getWorkOrderById"][0]

        sn_resp = post_api(FID, fetch_getByWorkorderids_query(wo_id))
        sn_numbers = sn_resp["data"]["getByWorkorderids"]["result"][0]["asnNumbers"]
        sn_list = [sn["snNumber"] for sn in sn_numbers if sn.get("snNumber")]

        wo.update({
            "Vendor Work Order Num": wo_enriched["woId"],
            "Channel Status Code": wo_enriched["channelStatusCode"],
            "Ismultipack": wo_enriched["woLines"][0].get("ismultipack"),
            "Ship Mode": wo_enriched["shipMode"],
            "Is Otm Enabled": wo_enriched["isOtmEnabled"],
            "SN Number": sn_list
        })

    # Fulfillment-related
    fulfillment = result.get("fulfillment")
    fulfillment_id = None
    if isinstance(fulfillment, dict):
        fulfillment_id = fulfillment.get("fulfillmentId")
    elif isinstance(fulfillment, list) and fulfillment:
        fulfillment_id = fulfillment[0].get("fulfillmentId")

    if fulfillment_id:
        combined_data["data"]["getFulfillmentsById"] = post_api(SOPATH, fetch_fulfillment_query(), {"fulfillment_id": fulfillment_id})["data"]["getFulfillmentsById"]
        combined_data["data"]["getFulfillmentsBysofulfillmentid"] = post_api(SOPATH, fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id))["data"]["getFulfillmentsBysofulfillmentid"]
        combined_data["data"]["getAllFulfillmentHeadersSoidFulfillmentid"] = post_api(FOID, fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id))["data"]["getAllFulfillmentHeadersSoidFulfillmentid"]
        combined_data["data"]["getFbomBySoFulfillmentid"] = post_api(FFBOM, fetch_getFbomBySoFulfillmentid_query(fulfillment_id))["data"]["getFbomBySoFulfillmentid"]

    # FOID
    fulfillment_orders = result.get("fulfillmentOrders", [])
    if fulfillment_orders:
        foid = fulfillment_orders[0]["foId"]
        fo_output = post_api(FOID, fetch_foid_query(foid))
        combined_data["data"]["getAllFulfillmentHeadersByFoId"] = fo_output["data"]["getAllFulfillmentHeadersByFoId"]

    return combined_data

def flatten_data(data, format_type, region):
    soheader = data["data"]["getSoheaderBySoids"][0]
    result = data["data"]["getBySalesorderids"]["result"][0]

    fulfillment_raw = result.get("fulfillment")
    fulfillment_id = fulfillment_raw["fulfillmentId"] if isinstance(fulfillment_raw, dict) else fulfillment_raw[0]["fulfillmentId"]

    fulfillment = data["data"]["getFulfillmentsById"][0]["fulfillments"][0]
    forderline = data["data"]["getAllFulfillmentHeadersByFoId"][0]["forderline"][0]
    getFulfillmentsByso = data["data"]["getFulfillmentsBysofulfillmentid"][0]["fulfillments"][0]
    sourceSystemId = data["data"]["getFulfillmentsBysofulfillmentid"][0]["sourceSystemId"]
    isDirectShip = data["data"]["getAllFulfillmentHeadersSoidFulfillmentid"][0]["isDirectShip"]
    ssc = data["data"]["getFbomBySoFulfillmentid"][0]["ssc"]

    base_row = {
        "BUID": soheader["buid"],
        "PP Date": soheader["ppDate"],
        "Sales Order Id": result["salesOrder"]["salesOrderId"],
        "Fulfillment Id": fulfillment_id,
        "Region Code": result["salesOrder"]["region"],
        "FoId": result["fulfillmentOrders"][0]["foId"],
        "System Qty": fulfillment["systemQty"],
        "Ship By Date": fulfillment["shipByDate"],
        "LOB": fulfillment["salesOrderLines"][0]["lob"],
        "Ship From Facility": forderline["shipFromFacility"],
        "Ship To Facility": forderline["shipToFacility"],
        "Tax Regstrn Num": getFulfillmentsByso["address"][0]["taxRegstrnNum"],
        "Address Line1": getFulfillmentsByso["address"][0]["addressLine1"],
        "Postal Code": getFulfillmentsByso["address"][0]["postalCode"],
        "State Code": getFulfillmentsByso["address"][0]["stateCode"],
        "City Code": getFulfillmentsByso["address"][0]["cityCode"],
        "Customer Num": getFulfillmentsByso["address"][0]["customerNum"],
        "Customer Name Ext": getFulfillmentsByso["address"][0]["customerNameExt"],
        "Country": getFulfillmentsByso["address"][0]["country"],
        "Create Date": getFulfillmentsByso["address"][0]["createDate"],
        "Ship Code": getFulfillmentsByso["shipCode"],
        "Must Arrive By Date": getFulfillmentsByso["mustArriveByDate"],
        "Update Date": getFulfillmentsByso["updateDate"],
        "Merge Type": getFulfillmentsByso["mergeType"],
        "Manifest Date": getFulfillmentsByso["manifestDate"],
        "Revised Delivery Date": getFulfillmentsByso["revisedDeliveryDate"],
        "Delivery City": getFulfillmentsByso["deliveryCity"],
        "Source System Id": sourceSystemId,
        "IsDirect Ship": isDirectShip,
        "SSC": ssc,
        "OIC Id": getFulfillmentsByso["oicId"],
        "Order Date": soheader["orderDate"]
    }

    flat_output = []
    for wo in result["workOrders"]:
        wo_copy = {**base_row, **{k: v for k, v in wo.items() if k != "SN Number"}}
        sn_list = wo.get("SN Number", [])
        if sn_list:
            for sn in sn_list:
                flat_output.append({**wo_copy, "SN Number": sn})
        else:
            flat_output.append({**wo_copy, "SN Number": None})

    clean_output = replace_none_with_empty(flat_output)

    if format_type == "export":
        return clean_output
    elif format_type == "grid":
        return tablestructural(clean_output, region)
    else:
        return {"error": "Invalid format type"}

def getbySalesOrderID(salesorderids, format_type, region):
    if not salesorderids:
        return {"error": "SalesOrderIds required"}
    if format_type not in ["export", "grid"]:
        return {"error": "Invalid format type"}
    if not region:
        return {"error": "Region is required"}

    total_output = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_and_clean, sid, region): sid for sid in salesorderids}
        for future in as_completed(futures):
            try:
                data = future.result()
                flat = flatten_data(data, format_type, region)
                if isinstance(flat, list):
                    total_output.extend(flat)
                elif isinstance(flat, dict) and "columns" in flat:
                    total_output.extend(flat.get("data", []))
            except Exception as e:
                print(f"Error processing {futures[future]}: {e}")

    return json.dumps(total_output, indent=2) if format_type == "export" else json.dumps(tablestructural(total_output, region), indent=2)

# Example usage
if __name__ == "__main__":
    print(getbySalesOrderID(["1004452326"], "grid", "DAO"))
