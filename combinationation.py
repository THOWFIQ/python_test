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


def getbySalesOrderIDs(salesorderid, format_type):
    data_row_export = {}
    soi = {"salesorderIds": [salesorderid]}
    combined_data = {'data': {}}

    soaorder = post_api(URL=SOPATH, query=fetch_soaorder_query(), variables=soi)
    if soaorder and soaorder.get('data'):
        combined_data['data']['getSoheaderBySoids'] = soaorder['data']['getSoheaderBySoids']

    salesorder = post_api(URL=FID, query=fetch_salesorder_query(salesorderid), variables=None)
    if salesorder and salesorder.get('data'):
        combined_data['data']['getBySalesorderids'] = salesorder['data']['getBySalesorderids']

    result = combined_data['data']['getBySalesorderids']['result'][0]
    soheader = combined_data['data']['getSoheaderBySoids'][0]
    work_orders = result.get('workOrders', [])
    for i in work_orders:
        workOrderId = i['woId']
        wo_data = post_api(URL=WOID, query=fetch_workOrderId_query(workOrderId), variables=None)
        sn_data = post_api(URL=FID, query=fetch_getByWorkorderids_query(workOrderId), variables=None)

        sn_numbers = [sn.get("snNumber") for sn in sn_data['data']['getByWorkorderids']['result'][0]['asnNumbers'] if sn.get("snNumber") is not None] if sn_data.get('data') else []

        if wo_data.get('data'):
            wo_detail = wo_data['data']['getWorkOrderById'][0]
            for i, wo in enumerate(work_orders):
                if wo.get("woId") == wo_detail["woId"]:
                    work_orders[i] = {
                        "Vendor Work Order Num": wo_detail["woId"],
                        "Channel Status Code": wo_detail["channelStatusCode"],
                        "Ismultipack": wo_detail["woLines"][0].get("ismultipack"),
                        "Ship Mode": wo_detail["shipMode"],
                        "Is Otm Enabled": wo_detail["isOtmEnabled"],
                        "SN Number": sn_numbers,
                        "Facility": wo_detail.get("shipToFacility", "")
                    }

    fulfillment_id = None
    fulfillment_raw = result.get("fulfillment")
    if isinstance(fulfillment_raw, dict):
        fulfillment_id = fulfillment_raw.get("fulfillmentId")
    elif isinstance(fulfillment_raw, list) and fulfillment_raw:
        fulfillment_id = fulfillment_raw[0].get("fulfillmentId")

    if fulfillment_id:
        combined_data['data']['getFulfillmentsById'] = post_api(URL=SOPATH, query=fetch_fulfillment_query(), variables={"fulfillment_id": fulfillment_id})['data']['getFulfillmentsById']
        combined_data['data']['getFulfillmentsBysofulfillmentid'] = post_api(URL=SOPATH, query=fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id), variables=None)['data']['getFulfillmentsBysofulfillmentid']
        combined_data['data']['getAllFulfillmentHeadersSoidFulfillmentid'] = post_api(URL=FOID, query=fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id), variables=None)['data']['getAllFulfillmentHeadersSoidFulfillmentid']
        combined_data['data']['getFbomBySoFulfillmentid'] = post_api(URL=FFBOM, query=fetch_getFbomBySoFulfillmentid_query(fulfillment_id), variables=None)['data']['getFbomBySoFulfillmentid']

    foid = result.get("fulfillmentOrders", [{}])[0].get("foId")
    if foid:
        combined_data['data']['getAllFulfillmentHeadersByFoId'] = post_api(URL=FOID, query=fetch_foid_query(foid), variables=None)['data']['getAllFulfillmentHeadersByFoId']

    fulfillment = combined_data['data']['getFulfillmentsById'][0]['fulfillments'][0]
    forderline = combined_data['data']['getAllFulfillmentHeadersByFoId'][0]['forderline'][0]
    getFulfillmentsBysofulfillmentid = combined_data['data']['getFulfillmentsBysofulfillmentid'][0]['fulfillments'][0]
    sourceSystemId = combined_data['data']['getFulfillmentsBysofulfillmentid'][0]['sourceSystemId']
    getAllFulfillmentHeadersSoidFulfillmentid = combined_data['data']['getAllFulfillmentHeadersSoidFulfillmentid'][0]
    isDirectShip = getAllFulfillmentHeadersSoidFulfillmentid["isDirectShip"]
    ssc = combined_data['data']['getFbomBySoFulfillmentid'][0]['ssc']

    data_row_export["BUID"] = soheader["buid"]
    data_row_export["PP Date"] = soheader["ppDate"]
    data_row_export["Sales Order Id"] = result["salesOrder"]["salesOrderId"]
    data_row_export["Fulfillment Id"] = fulfillment_id
    data_row_export["Region Code"] = result["salesOrder"]["region"]
    data_row_export["wo_ids"] = work_orders
    data_row_export["FoId"] = foid
    data_row_export["System Qty"] = fulfillment["systemQty"]
    data_row_export["Ship By Date"] = fulfillment["shipByDate"]
    data_row_export["LOB"] = fulfillment["salesOrderLines"][0]["lob"]
    data_row_export["Ship From Facility"] = forderline["shipFromFacility"]
    data_row_export["Ship To Facility"] = forderline["shipToFacility"]
    data_row_export["Tax Regstrn Num"] = getFulfillmentsBysofulfillmentid["address"][0]["taxRegstrnNum"]
    data_row_export["Address Line1"] = getFulfillmentsBysofulfillmentid["address"][0]["addressLine1"]
    data_row_export["Postal Code"] = getFulfillmentsBysofulfillmentid["address"][0]["postalCode"]
    data_row_export["State Code"] = getFulfillmentsBysofulfillmentid["address"][0]["stateCode"]
    data_row_export["City Code"] = getFulfillmentsBysofulfillmentid["address"][0]["cityCode"]
    data_row_export["Customer Num"] = getFulfillmentsBysofulfillmentid["address"][0]["customerNum"]
    data_row_export["Customer NameExt"] = getFulfillmentsBysofulfillmentid["address"][0]["customerNameExt"]
    data_row_export["Country"] = getFulfillmentsBysofulfillmentid["address"][0]["country"]
    data_row_export["Create Date"] = getFulfillmentsBysofulfillmentid["address"][0]["createDate"]
    data_row_export["Ship Code"] = getFulfillmentsBysofulfillmentid["shipCode"]
    data_row_export["Must Arrive By Date"] = getFulfillmentsBysofulfillmentid["mustArriveByDate"]
    data_row_export["Update Date"] = getFulfillmentsBysofulfillmentid["updateDate"]
    data_row_export["Merge Type"] = getFulfillmentsBysofulfillmentid["mergeType"]
    data_row_export["Manifest Date"] = getFulfillmentsBysofulfillmentid["manifestDate"]
    data_row_export["Revised Delivery Date"] = getFulfillmentsBysofulfillmentid["revisedDeliveryDate"]
    data_row_export["Delivery City"] = getFulfillmentsBysofulfillmentid["deliveryCity"]
    data_row_export["Source System Id"] = sourceSystemId
    data_row_export["IsDirect Ship"] = isDirectShip
    data_row_export["SSC"] = ssc
    data_row_export["OIC Id"] = getFulfillmentsBysofulfillmentid["oicId"]
    data_row_export["Order Date"] = soheader["orderDate"]

    base = {k: v for k, v in data_row_export.items() if k != "wo_ids"}
    flat_list = []
    for wo in data_row_export.get("wo_ids", []):
        sn_numbers = wo.get("SN Number", [])
        if sn_numbers:
            for sn in sn_numbers:
                flat_entry = {
                    **base,
                    **{k: v for k, v in wo.items() if k != "SN Number"},
                    "SN Number": sn
                }
                flat_list.append(flat_entry)
        else:
            flat_entry = {
                **base,
                **{k: v for k, v in wo.items() if k != "SN Number"},
                "SN Number": None
            }
            flat_list.append(flat_entry)

    return flat_list


def fetch_all_data(salesorderid):
    try:
        data_list = getbySalesOrderIDs(salesorderid=salesorderid, format_type='export')
        for row in data_list:
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
