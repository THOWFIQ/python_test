from flask import request, jsonify
import httpx
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from graphqlQueries import *

# ---- Load config ----
def load_config():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, "config", "config_ge4.json")
    with open(config_path, "r") as f:
        return json.load(f)

CONFIG = load_config()

# ---- Helper: Replace None with empty string ----
def sanitize(data):
    if isinstance(data, dict):
        return {k: sanitize(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize(i) for i in data]
    elif data is None:
        return ""
    return data

# ---- GraphQL POST helper ----
def post_api(URL, query, variables=None):
    try:
        response = httpx.post(URL, json={"query": query, "variables": variables} if variables else {"query": query}, verify=False)
        return response.json()
    except Exception as e:
        print(f"[ERROR] Failed request to {URL}: {e}")
        return {}

# ---- Fetch & Clean all required data ----
def fetch_and_clean(salesorder_id, region):
    combined_data = {'data': {}}
    soi = {"salesorderIds": [salesorder_id]}

    # Region-specific URLs
    FID = get_path(region, "FID", CONFIG)
    FOID = get_path(region, "FOID", CONFIG)
    SOPATH = get_path(region, "SOPATH", CONFIG)
    WOID = get_path(region, "WOID", CONFIG)
    FFBOM = get_path(region, "FFBOM", CONFIG)

    # SO Header
    so_header_resp = post_api(SOPATH, fetch_soaorder_query(), soi)
    so_header = so_header_resp.get('data', {}).get('getSoheaderBySoids', [])
    if not so_header:
        print(f"[WARNING] No SO header found for {salesorder_id}")
        return None
    combined_data['data']['getSoheaderBySoids'] = so_header

    # SO data
    salesorder_resp = post_api(FID, fetch_salesorder_query(salesorder_id))
    sales_result = salesorder_resp.get('data', {}).get('getBySalesorderids', {}).get('result', [])
    if not sales_result:
        print(f"[WARNING] No SalesOrder found for {salesorder_id}")
        return None
    result = sales_result[0]
    combined_data['data']['getBySalesorderids'] = salesorder_resp['data'].get('getBySalesorderids', {})

    # WorkOrders
    for wo in result.get("workOrders", []):
        wo_id = wo["woId"]
        wo_detail = post_api(WOID, fetch_workOrderId_query(wo_id))
        wo_enriched = wo_detail.get("data", {}).get("getWorkOrderById", [{}])[0]

        sn_resp = post_api(FID, fetch_getByWorkorderids_query(wo_id))
        sn_numbers = sn_resp.get("data", {}).get("getByWorkorderids", {}).get("result", [{}])[0].get("asnNumbers", [])
        sn_list = [sn.get("snNumber", "") for sn in sn_numbers if sn.get("snNumber")]

        wo.update({
            "Vendor Work Order Num": wo_enriched.get("woId", ""),
            "Channel Status Code": wo_enriched.get("channelStatusCode", ""),
            "Ismultipack": wo_enriched.get("woLines", [{}])[0].get("ismultipack", ""),
            "Ship Mode": wo_enriched.get("shipMode", ""),
            "Is Otm Enabled": wo_enriched.get("isOtmEnabled", ""),
            "SN Number": sn_list
        })

    # Fulfillment
    fulfillment = result.get("fulfillment")
    fulfillment_id = ""
    if isinstance(fulfillment, dict):
        fulfillment_id = fulfillment.get("fulfillmentId")
    elif isinstance(fulfillment, list) and fulfillment:
        fulfillment_id = fulfillment[0].get("fulfillmentId")

    if fulfillment_id:
        combined_data["data"]["getFulfillmentsById"] = post_api(SOPATH, fetch_fulfillment_query(), {"fulfillment_id": fulfillment_id}).get("data", {}).get("getFulfillmentsById", [])
        combined_data["data"]["getFulfillmentsBysofulfillmentid"] = post_api(SOPATH, fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id)).get("data", {}).get("getFulfillmentsBysofulfillmentid", [])
        combined_data["data"]["getAllFulfillmentHeadersSoidFulfillmentid"] = post_api(FOID, fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id)).get("data", {}).get("getAllFulfillmentHeadersSoidFulfillmentid", [])
        combined_data["data"]["getFbomBySoFulfillmentid"] = post_api(FFBOM, fetch_getFbomBySoFulfillmentid_query(fulfillment_id)).get("data", {}).get("getFbomBySoFulfillmentid", [])

    # FOID
    fulfillment_orders = result.get("fulfillmentOrders", [])
    if fulfillment_orders:
        foid = fulfillment_orders[0].get("foId")
        fo_output = post_api(FOID, fetch_foid_query(foid))
        combined_data["data"]["getAllFulfillmentHeadersByFoId"] = fo_output.get("data", {}).get("getAllFulfillmentHeadersByFoId", [])

    return combined_data

# ---- Flatten structured data ----
def flatten_data(data, format_type, region):
    try:
        soheader = data["data"]["getSoheaderBySoids"][0]
        result = data["data"]["getBySalesorderids"]["result"][0]

        fulfillment_raw = result.get("fulfillment")
        fulfillment_id = fulfillment_raw["fulfillmentId"] if isinstance(fulfillment_raw, dict) else fulfillment_raw[0]["fulfillmentId"]

        fulfillment = data["data"]["getFulfillmentsById"][0]["fulfillments"][0]
        forderline = data["data"]["getAllFulfillmentHeadersByFoId"][0]["forderline"][0]
        getFulfillmentsByso = data["data"]["getFulfillmentsBysofulfillmentid"][0]["fulfillments"][0]
        sourceSystemId = data["data"]["getFulfillmentsBysofulfillmentid"][0].get("sourceSystemId", "")
        isDirectShip = data["data"]["getAllFulfillmentHeadersSoidFulfillmentid"][0].get("isDirectShip", "")
        ssc = data["data"]["getFbomBySoFulfillmentid"][0].get("ssc", "")

        base_row = {
            "BUID": soheader.get("buid", ""),
            "PP Date": soheader.get("ppDate", ""),
            "Sales Order Id": result["salesOrder"].get("salesOrderId", ""),
            "Fulfillment Id": fulfillment_id,
            "Region Code": result["salesOrder"].get("region", ""),
            "FoId": result["fulfillmentOrders"][0].get("foId", ""),
            "System Qty": fulfillment.get("systemQty", ""),
            "Ship By Date": fulfillment.get("shipByDate", ""),
            "LOB": fulfillment["salesOrderLines"][0].get("lob", ""),
            "Ship From Facility": forderline.get("shipFromFacility", ""),
            "Ship To Facility": forderline.get("shipToFacility", ""),
            "Tax Regstrn Num": getFulfillmentsByso["address"][0].get("taxRegstrnNum", ""),
            "Address Line1": getFulfillmentsByso["address"][0].get("addressLine1", ""),
            "Postal Code": getFulfillmentsByso["address"][0].get("postalCode", ""),
            "State Code": getFulfillmentsByso["address"][0].get("stateCode", ""),
            "City Code": getFulfillmentsByso["address"][0].get("cityCode", ""),
            "Customer Num": getFulfillmentsByso["address"][0].get("customerNum", ""),
            "Customer Name Ext": getFulfillmentsByso["address"][0].get("customerNameExt", ""),
            "Country": getFulfillmentsByso["address"][0].get("country", ""),
            "Create Date": getFulfillmentsByso["address"][0].get("createDate", ""),
            "Ship Code": getFulfillmentsByso.get("shipCode", ""),
            "Must Arrive By Date": getFulfillmentsByso.get("mustArriveByDate", ""),
            "Update Date": getFulfillmentsByso.get("updateDate", ""),
            "Merge Type": getFulfillmentsByso.get("mergeType", ""),
            "Manifest Date": getFulfillmentsByso.get("manifestDate", ""),
            "Revised Delivery Date": getFulfillmentsByso.get("revisedDeliveryDate", ""),
            "Delivery City": getFulfillmentsByso.get("deliveryCity", ""),
            "Source System Id": sourceSystemId,
            "IsDirect Ship": isDirectShip,
            "SSC": ssc,
            "OIC Id": getFulfillmentsByso.get("oicId", ""),
            "Order Date": soheader.get("orderDate", "")
        }

        flat_output = []
        for wo in result.get("workOrders", []):
            wo_copy = {**base_row, **{k: v for k, v in wo.items() if k != "SN Number"}}
            sn_list = wo.get("SN Number", [])
            if sn_list:
                for sn in sn_list:
                    flat_output.append(sanitize({**wo_copy, "SN Number": sn}))
            else:
                flat_output.append(sanitize({**wo_copy, "SN Number": ""}))

        return tablestructural(flat_output, region) if format_type == "grid" else flat_output

    except Exception as e:
        print(f"[ERROR] flatten_data: {e}")
        return []

# ---- Main API logic ----
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
            sid = futures[future]
            try:
                data = future.result()
                if data is None:
                    continue
                flat = flatten_data(data, format_type, region)
                if isinstance(flat, list):
                    total_output.extend(flat)
                elif isinstance(flat, dict) and "columns" in flat:
                    total_output.extend(flat.get("data", []))
            except Exception as e:
                print(f"[ERROR] processing {sid}: {e}")

    return json.dumps(total_output, indent=2) if format_type == "export" else json.dumps(tablestructural(total_output, region), indent=2)

# ---- Local test runner ----
if __name__ == "__main__":
    print(getbySalesOrderID(["1004452326", "1004543337"], "grid", "DAO"))
