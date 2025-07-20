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
    "Sales_Order_id", "wo_id", "Fullfillment Id", "foid", "order_date"
}

SECONDARY_FIELDS = {
    "ISMULTIPACK", "BUID", "Facility"
}

def post_api(URL, query, variables):
    try:
        response = httpx.post(URL, json={"query": query, "variables": variables} if variables else {"query": query}, verify=False)
        return response.json()
    except Exception as e:
        print(f"Exception in post_api: {e}")
        return {"error": str(e)}

def combined_salesorder_fetch(so_id):
    combined_salesorder_data = {'data': {}}
    soi = {"salesorderIds": [so_id]}

    try:
        soaorder = post_api(SOPATH, fetch_soaorder_query(), soi)
        combined_salesorder_data['data']['getSoheaderBySoids'] = soaorder.get('data', {}).get('getSoheaderBySoids', [])
    except Exception as e:
        print(f"[ERROR] getSoheaderBySoids failed for {so_id}: {e}")
        combined_salesorder_data['data']['getSoheaderBySoids'] = []

    try:
        salesorder = post_api(FID, fetch_salesorder_query(so_id), None)
        combined_salesorder_data['data']['getBySalesorderids'] = salesorder.get('data', {}).get('getBySalesorderids', [])
    except Exception as e:
        print(f"[ERROR] getBySalesorderids failed for {so_id}: {e}")
        combined_salesorder_data['data']['getBySalesorderids'] = []

    return combined_salesorder_data

def combined_foid_fetch(fo_id):
    combined_foid_data = {'data': {}}
    foid_query = fetch_foid_query(fo_id)
    foid_output = post_api(FOID, foid_query, None)

    if foid_output and foid_output.get('data'):
        combined_foid_data['data']['getAllFulfillmentHeadersByFoId'] = foid_output['data'].get('getAllFulfillmentHeadersByFoId', [])

    return combined_foid_data

def combined_woid_fetch(wo_id):
    flattened_wo_dic = {}
    sn_numbers = []

    try:
        wo_query = fetch_workOrderId_query(wo_id)
        wo_data = post_api(WOID, wo_query, None)
        wo_detail_list = wo_data.get('data', {}).get('getWorkOrderById', [])

        if not wo_detail_list:
            return {}

        wo_detail = wo_detail_list[0]

        flattened_wo_dic["Vendor Work Order Num"] = wo_detail.get("woId")
        flattened_wo_dic["Channel Status Code"] = wo_detail.get("channelStatusCode")
        flattened_wo_dic["Ismultipack"] = wo_detail.get("woLines", [{}])[0].get("ismultipack")
        flattened_wo_dic["Ship Mode"] = wo_detail.get("shipMode")
        flattened_wo_dic["Is Otm Enabled"] = wo_detail.get("isOtmEnabled")

        sn_data = post_api(FID, fetch_getByWorkorderids_query(wo_id), None)
        sn_numbers = [
            sn.get("snNumber") for sn in sn_data.get("data", {}).get("getByWorkorderids", {}).get("result", [{}])[0].get("asnNumbers", [])
            if sn.get("snNumber")
        ]

        flattened_wo_dic["SN Number"] = sn_numbers

    except Exception as e:
        print(f"[ERROR] Fetching WOID {wo_id}: {e}")

    return flattened_wo_dic

def combined_fulfillment_fetch(fulfillment_id):
    combined = {'data': {}}
    vars = {"fulfillment_id": fulfillment_id}

    combined['data']['getFulfillmentsById'] = post_api(SOPATH, fetch_fulfillment_query(), vars).get("data", {}).get("getFulfillmentsById", [])
    combined['data']['getFulfillmentsBysofulfillmentid'] = post_api(SOPATH, fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id), vars).get("data", {}).get("getFulfillmentsBysofulfillmentid", [])
    combined['data']['getAllFulfillmentHeadersSoidFulfillmentid'] = post_api(FOID, fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id), vars).get("data", {}).get("getAllFulfillmentHeadersSoidFulfillmentid", [])
    combined['data']['getFbomBySoFulfillmentid'] = post_api(FFBOM, fetch_getFbomBySoFulfillmentid_query(fulfillment_id), vars).get("data", {}).get("getFbomBySoFulfillmentid", [])

    return combined

def validate_secondary_filters(row, filters):
    if not filters:
        return True
    for key, expected in filters.items():
        actual = str(row.get(key, "")).lower()
        if actual != str(expected).lower():
            return False
    return True

def OutputFormat(result_map, format_type=None, secondary_filters=None):
    flat_list = []

    sales_orders = result_map.get("Sales_Order_id", [])
    fulfillments = result_map.get("Fullfillment Id", [])
    wo_ids = result_map.get("wo_id", [])
    foid_data = result_map.get("foid", [])

    for so_index, so_entry in enumerate(sales_orders):
        try:
            so_data = so_entry.get("data", {})
            get_soheaders = so_data.get("getSoheaderBySoids", [])
            get_salesorders = so_data.get("getBySalesorderids", [])

            if not get_soheaders or not get_salesorders:
                print(f"[WARN] Missing SO headers or sales orders at row {so_index}")
                continue

            soheader = get_soheaders[0]
            salesorder = get_salesorders[0]

            fulfillment_data = fulfillments[so_index].get("data", {}) if so_index < len(fulfillments) else {}
            fulfillment = fulfillment_data.get("getFulfillmentsById", [{}])[0]
            sofulfillment = fulfillment_data.get("getFulfillmentsBysofulfillmentid", [{}])[0]

            forderline = (fulfillment.get("salesOrderLines") or [{}])[0]
            address = (sofulfillment.get("address") or [{}])[0]

            wo_data = wo_ids[so_index] if so_index < len(wo_ids) else {}

            base_row = {
                "BUID": soheader.get("buid"),
                "PP Date": soheader.get("ppDate"),
                "Sales Order Id": salesorder.get("salesOrderId"),
                "Fulfillment Id": fulfillment.get("fulfillmentId"),
                "Region Code": salesorder.get("region"),
                "FoId": foid_data[0]["data"].get("getAllFulfillmentHeadersByFoId", [{}])[0].get("foId") if foid_data else None,
                "System Qty": fulfillment.get("systemQty"),
                "Ship By Date": fulfillment.get("shipByDate"),
                "LOB": forderline.get("lob"),
                "Ship From Facility": forderline.get("shipFromFacility"),
                "Ship To Facility": forderline.get("shipToFacility"),
                "Tax Regstrn Num": address.get("taxRegstrnNum"),
                "Address Line1": address.get("addressLine1"),
                "Postal Code": address.get("postalCode"),
                "State Code": address.get("stateCode"),
                "City Code": address.get("cityCode"),
                "Customer Num": address.get("customerNum"),
                "Customer Name Ext": address.get("customerNameExt"),
                "Country": address.get("country"),
                "Create Date": address.get("createDate"),
                "Ship Code": sofulfillment.get("shipCode"),
                "Must Arrive By Date": sofulfillment.get("mustArriveByDate"),
                "Update Date": sofulfillment.get("updateDate"),
                "Merge Type": sofulfillment.get("mergeType"),
                "Manifest Date": sofulfillment.get("manifestDate"),
                "Revised Delivery Date": sofulfillment.get("revisedDeliveryDate"),
                "Delivery City": sofulfillment.get("deliveryCity"),
                "Source System Id": sofulfillment.get("sourceSystemId"),
                "IsDirect Ship": sofulfillment.get("isDirectShip"),
                "SSC": sofulfillment.get("ssc"),
                "OIC Id": sofulfillment.get("oicId"),
                "Order Date": soheader.get("orderDate")
            }

            if isinstance(wo_data, dict):
                sn_numbers = wo_data.get("SN Number", [])
                wo_clean = {k: v for k, v in wo_data.items() if k != "SN Number"}
                if sn_numbers:
                    for sn in sn_numbers:
                        row = {**base_row, **wo_clean, "SN Number": sn}
                        if validate_secondary_filters(row, secondary_filters):
                            flat_list.append(row)
                else:
                    row = {**base_row, **wo_clean, "SN Number": None}
                    if validate_secondary_filters(row, secondary_filters):
                        flat_list.append(row)

        except Exception as e:
            print(f"[ERROR] formatting row {so_index}: {e}")
            traceback.print_exc()

    if format_type == "export":
        return flat_list

    elif format_type == "grid":
        desired_order = [
            'BUID', 'PP Date', 'Sales Order Id', 'Fulfillment Id', 'Region Code', 'FoId', 'System Qty', 'Ship By Date',
            'LOB', 'Ship From Facility', 'Ship To Facility', 'Tax Regstrn Num', 'Address Line1', 'Postal Code',
            'State Code', 'City Code', 'Customer Num', 'Customer Name Ext', 'Country', 'Create Date', 'Ship Code',
            'Must Arrive By Date', 'Update Date', 'Merge Type', 'Manifest Date', 'Revised Delivery Date',
            'Delivery City', 'Source System Id', 'IsDirect Ship', 'SSC', 'Vendor Work Order Num',
            'Channel Status Code', 'Ismultipack', 'Ship Mode', 'Is Otm Enabled', 'SN Number', 'OIC Id', 'Order Date'
        ]
        return [{"columns": [{"value": row.get(key, "")} for key in desired_order]} for row in flat_list]

    return {"error": "Format type is not part of grid/export"}

def fileldValidation(filters, format_type, region):
    result_map = {}
    primary_filters = {k: v for k, v in filters.items() if k in PRIMARY_FIELDS}
    secondary_filters = {k: v for k, v in filters.items() if k in SECONDARY_FIELDS}

    if not primary_filters:
        return {"status": "error", "message": "At least one primary field is required in filters."}

    if "Sales_Order_id" in primary_filters:
        soids = list({x.strip() for x in primary_filters["Sales_Order_id"].split(",")})
        with ThreadPoolExecutor(max_workers=5) as executor:
            result_map["Sales_Order_id"] = [f.result() for f in as_completed([executor.submit(combined_salesorder_fetch, x) for x in soids])]

    if "foid" in primary_filters:
        foids = list({x.strip() for x in primary_filters["foid"].split(",")})
        with ThreadPoolExecutor(max_workers=5) as executor:
            result_map["foid"] = [f.result() for f in as_completed([executor.submit(combined_foid_fetch, x) for x in foids])]

    if "wo_id" in primary_filters:
        woids = list({x.strip() for x in primary_filters["wo_id"].split(",")})
        with ThreadPoolExecutor(max_workers=5) as executor:
            result_map["wo_id"] = [f.result() for f in as_completed([executor.submit(combined_woid_fetch, x) for x in woids])]

    if "Fullfillment Id" in primary_filters:
        fids = list({x.strip() for x in primary_filters["Fullfillment Id"].split(",")})
        with ThreadPoolExecutor(max_workers=5) as executor:
            result_map["Fullfillment Id"] = [f.result() for f in as_completed([executor.submit(combined_fulfillment_fetch, x) for x in fids])]

    formatted = OutputFormat(result_map, format_type=format_type, secondary_filters=secondary_filters)
    return {
        "status": "success",
        "message": "Validation and fetch completed.",
        "result_summary": {k: f"{len(v)} response(s)" for k, v in result_map.items()},
        "data": formatted
    }

if __name__ == "__main__":
    filters = {
        "Sales_Order_id": "1004543337,483713",
        "foid": "7336030653629440001",
        "Fullfillment Id": "262136",
        "wo_id": "7360928459",
        "ISMULTIPACK": "Yes",
        "BUID": "202",
        "Facility": "WH_BANGALORE"
    }
    result = fileldValidation(filters, format_type='grid', region="EMEA")
    print(json.dumps(result, indent=2))
