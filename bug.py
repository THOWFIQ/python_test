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

PRIMARY_FIELDS = {"Sales_Order_id", "wo_id", "Fullfillment Id", "foid", "order_date"}
SECONDARY_FIELDS = {"ISMULTIPACK", "BUID", "Facility"}

def post_api(URL, query, variables):
    try:
        response = httpx.post(URL, json={"query": query, "variables": variables} if variables else {"query": query}, verify=False)
        return response.json()
    except Exception as e:
        print(f"Exception in post_api: {e}")
        return {"error": str(e)}

def combined_salesorder_fetch(so_id):
    combined_data = {'data': {}}
    soi = {"salesorderIds": [so_id]}
    try:
        combined_data['data']['getSoheaderBySoids'] = post_api(SOPATH, fetch_soaorder_query(), soi)['data'].get('getSoheaderBySoids', [])
    except:
        combined_data['data']['getSoheaderBySoids'] = []
    try:
        combined_data['data']['getBySalesorderids'] = post_api(FID, fetch_salesorder_query(so_id), None)['data'].get('getBySalesorderids', [])
    except:
        combined_data['data']['getBySalesorderids'] = []
    return combined_data

def combined_foid_fetch(fo_id):
    foid_query = fetch_foid_query(fo_id)
    output = post_api(FOID, foid_query, None)
    return {'data': {'getAllFulfillmentHeadersByFoId': output.get('data', {}).get('getAllFulfillmentHeadersByFoId', [])}}

def combined_woid_fetch(wo_id):
    wo_query = fetch_workOrderId_query(wo_id)
    sn_query = fetch_getByWorkorderids_query(wo_id)
    wo_data = post_api(WOID, wo_query, None)
    sn_data = post_api(FID, sn_query, None)

    wo_detail = wo_data.get('data', {}).get('getWorkOrderById', [{}])[0]
    sn_numbers = [sn['snNumber'] for sn in sn_data.get('data', {}).get('getByWorkorderids', {}).get('result', [{}])[0].get('asnNumbers', []) if sn.get('snNumber')]

    return {
        "Vendor Work Order Num": wo_detail.get("woId"),
        "Channel Status Code": wo_detail.get("channelStatusCode"),
        "Ismultipack": wo_detail.get("woLines", [{}])[0].get("ismultipack"),
        "Ship Mode": wo_detail.get("shipMode"),
        "Is Otm Enabled": wo_detail.get("isOtmEnabled"),
        "SN Number": sn_numbers
    }

def combined_fulfillment_fetch(fid):
    result = {'data': {}}
    v = {"fulfillment_id": fid}
    result['data']['getFulfillmentsById'] = post_api(SOPATH, fetch_fulfillment_query(), v).get('data', {}).get('getFulfillmentsById', [])
    result['data']['getFulfillmentsBysofulfillmentid'] = post_api(SOPATH, fetch_getFulfillmentsBysofulfillmentid_query(fid), v).get('data', {}).get('getFulfillmentsBysofulfillmentid', [])
    result['data']['getAllFulfillmentHeadersSoidFulfillmentid'] = post_api(FOID, fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fid), v).get('data', {}).get('getAllFulfillmentHeadersSoidFulfillmentid', {})
    result['data']['getFbomBySoFulfillmentid'] = post_api(FFBOM, fetch_getFbomBySoFulfillmentid_query(fid), v).get('data', {}).get('getFbomBySoFulfillmentid', {})
    return result

def validate_secondary_filters(row, filters):
    if not filters:
        return True
    for k, v in filters.items():
        if str(row.get(k, '')).lower() != str(v).lower():
            return False
    return True

def OutputFormat(result_map, format_type=None, secondary_filters=None):
    flat_list = []
    sales_orders = result_map.get("Sales_Order_id", [])
    fulfillments = result_map.get("Fullfillment Id", [])
    wo_ids = result_map.get("wo_id", [])
    foid_data = result_map.get("foid", [])

    for i, so_entry in enumerate(sales_orders):
        try:
            so_data = so_entry.get("data", {})
            get_soheaders = so_data.get("getSoheaderBySoids", [])
            get_salesorders = so_data.get("getBySalesorderids", [])
            if not get_soheaders or not get_salesorders:
                print(f"[WARN] Missing SO data at row {i}")
                continue
            soheader = get_soheaders[0]
            salesorder = get_salesorders[0]

            fulfillment_entry = fulfillments[i] if i < len(fulfillments) else {"data": {}}
            fdata = fulfillment_entry.get("data", {})
            fulfillment = fdata.get("getFulfillmentsById", [{}])[0] if isinstance(fdata.get("getFulfillmentsById"), list) else {}
            sofulfillment = fdata.get("getFulfillmentsBysofulfillmentid", [{}])[0] if isinstance(fdata.get("getFulfillmentsBysofulfillmentid"), list) else {}
            forderline = (fulfillment.get("salesOrderLines") or [{}])[0]
            address = (sofulfillment.get("address") or [{}])[0]

            wo_data = wo_ids[i] if i < len(wo_ids) else []

            base = {
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
                "Order Date": soheader.get("orderDate"),
            }

            for wo in wo_data if isinstance(wo_data, list) else []:
                if not isinstance(wo, dict):
                    print(f"[WARN] Invalid wo entry: {wo}")
                    continue
                sn_numbers = wo.get("SN Number", [])
                wo_clean = {k: v for k, v in wo.items() if k != "SN Number"}
                if sn_numbers:
                    for sn in sn_numbers:
                        row = {**base, **wo_clean, "SN Number": sn}
                        if validate_secondary_filters(row, secondary_filters):
                            flat_list.append(row)
                else:
                    row = {**base, **wo_clean, "SN Number": None}
                    if validate_secondary_filters(row, secondary_filters):
                        flat_list.append(row)

        except Exception as e:
            print(f"[ERROR] formatting row {i}: {e}")
            traceback.print_exc()

    if format_type == "export":
        return flat_list
    elif format_type == "grid":
        desired_order = ["BUID", "PP Date", "Sales Order Id", "Fulfillment Id", "Region Code", "FoId", "System Qty",
                         "Ship By Date", "LOB", "Ship From Facility", "Ship To Facility", "Tax Regstrn Num",
                         "Address Line1", "Postal Code", "State Code", "City Code", "Customer Num",
                         "Customer Name Ext", "Country", "Create Date", "Ship Code", "Must Arrive By Date",
                         "Update Date", "Merge Type", "Manifest Date", "Revised Delivery Date", "Delivery City",
                         "Source System Id", "IsDirect Ship", "SSC", "Vendor Work Order Num", "Channel Status Code",
                         "Ismultipack", "Ship Mode", "Is Otm Enabled", "SN Number", "OIC Id", "Order Date"]

        return [{"columns": [{"value": row.get(key, '')} for key in desired_order]} for row in flat_list]
    return {"error": "Format type is not part of grid/export"}

def fileldValidation(filters, format_type, region):
    result_map = {}
    primary_filters = {k: v for k, v in filters.items() if k in PRIMARY_FIELDS}
    secondary_filters = {k: v for k, v in filters.items() if k in SECONDARY_FIELDS}

    if not primary_filters:
        return {"status": "error", "message": "At least one primary field is required."}

    with ThreadPoolExecutor(max_workers=5) as executor:
        if 'Sales_Order_id' in primary_filters:
            futures = [executor.submit(combined_salesorder_fetch, soid.strip()) for soid in primary_filters['Sales_Order_id'].split(',')]
            result_map['Sales_Order_id'] = [f.result() for f in as_completed(futures)]

        if 'foid' in primary_filters:
            futures = [executor.submit(combined_foid_fetch, foid.strip()) for foid in primary_filters['foid'].split(',')]
            result_map['foid'] = [f.result() for f in as_completed(futures)]

        if 'wo_id' in primary_filters:
            futures = [executor.submit(combined_woid_fetch, woid.strip()) for woid in primary_filters['wo_id'].split(',')]
            result_map['wo_id'] = [f.result() for f in as_completed(futures)]

        if 'Fullfillment Id' in primary_filters:
            futures = [executor.submit(combined_fulfillment_fetch, fid.strip()) for fid in primary_filters['Fullfillment Id'].split(',')]
            result_map['Fullfillment Id'] = [f.result() for f in as_completed(futures)]

    return {
        "status": "success",
        "message": "Validation and fetch completed.",
        "result_summary": {k: f"{len(v)} response(s)" for k, v in result_map.items()},
        "data": OutputFormat(result_map, format_type=format_type, secondary_filters=secondary_filters)
    }

if __name__ == "__main__":
    filters = {
        "Sales_Order_id": "1004543337,483713,416695",
        "foid": "7336030653629440001",
        "Fullfillment Id": "262136,262135",
        "wo_id": "7360928459,7360928460,7360970693",
        "ISMULTIPACK": "Yes",
        "BUID": "202",
        "Facility": "WH_BANGALORE"
    }
    result = fileldValidation(filters=filters, format_type='grid', region='EMEA')
    print(json.dumps(result, indent=2))
