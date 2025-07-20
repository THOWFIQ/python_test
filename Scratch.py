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
    "Sales_Order_id",
    "wo_id",
    "Fullfillment Id",
    "foid",
    "order_date"
}

SECONDARY_FIELDS = {
    "ISMULTIPACK",
    "BUID",
    "Facility"
}

def post_api(URL, query, variables):
    try:
        if variables:
            response = httpx.post(URL, json={"query": query, "variables": variables}, verify=False)
        else:
            response = httpx.post(URL, json={"query": query}, verify=False)
        return response.json()
    except Exception as e:
        print(f"Exception in post_api: {e}")
        return {"error": str(e)}

def combined_salesorder_fetch(so_id):
    combined_salesorder_data  = {'data': {}}

    soi = {"salesorderIds": [so_id]}
    soaorder_query = fetch_soaorder_query()
    soaorder = post_api(URL=SOPATH, query=soaorder_query, variables=soi)
    if soaorder and soaorder.get('data'):
        combined_salesorder_data['data']['getSoheaderBySoids'] = soaorder['data'].get('getSoheaderBySoids', [])

    salesorder_query = fetch_salesorder_query(so_id)
    salesorder = post_api(URL=FID, query=salesorder_query, variables=None)
    if salesorder and salesorder.get('data'):
        combined_salesorder_data['data']['getBySalesorderids'] = salesorder['data'].get('getBySalesorderids', [])

    return combined_salesorder_data

def combined_foid_fetch(fo_id):

    combined_foid_data = {'data': {}}

    foid_query = fetch_foid_query(fo_id)

    foid_output=post_api(URL=FOID, query=foid_query, variables=None)
    if foid_output and foid_output.get('data'):
        combined_foid_data['data']['getAllFulfillmentHeadersByFoId'] = foid_output['data']['getAllFulfillmentHeadersByFoId']

    return combined_foid_data

def combined_woid_fetch(wo_id):

    flattened_wo_dic = {}

    workOrderId_query = fetch_workOrderId_query(wo_id)

    getWorkOrderById=post_api(URL=WOID, query=workOrderId_query, variables=None)
    
    getByWorkorderids_query = fetch_getByWorkorderids_query(wo_id)

    sn_numbers = []
    getByWorkorderids=post_api(URL=FID, query=getByWorkorderids_query, variables=None)
    
    if getByWorkorderids and getByWorkorderids.get('data') is not None:
        sn_numbers = [
            sn.get("snNumber") 
            for sn in getByWorkorderids["data"]["getByWorkorderids"]["result"][0]["asnNumbers"]
            if sn.get("snNumber") is not None
        ]
    
    if getWorkOrderById and getWorkOrderById.get('data') is not None:
        wo_detail = getWorkOrderById["data"]["getWorkOrderById"][0]

        flattened_wo_dic["Vendor Work Order Num"] = wo_detail["woId"],
        flattened_wo_dic["Channel Status Code"] = wo_detail["channelStatusCode"],
        flattened_wo_dic["Ismultipack"] = wo_detail["woLines"][0].get("ismultipack"),
        flattened_wo_dic["Ship Mode"] = wo_detail["shipMode"],
        flattened_wo_dic["Is Otm Enabled"] = wo_detail["isOtmEnabled"],
        flattened_wo_dic["SN Number"] = sn_numbers
    
    return flattened_wo_dic

def combined_fulfillment_fetch(fulfillment_id):
    combined_fullfillment_data = {'data': {}}

    variables = {"fulfillment_id": fulfillment_id}

    fulfillment_query = fetch_fulfillment_query()
    fulfillment_data = post_api(URL=SOPATH, query=fulfillment_query, variables=variables)
    if fulfillment_data and fulfillment_data.get('data'):
        combined_fullfillment_data['data']['getFulfillmentsById'] = fulfillment_data['data'].get('getFulfillmentsById', {})

    sofulfillment_query = fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id)
    sofulfillment_data = post_api(URL=SOPATH, query=sofulfillment_query, variables=variables)
    if sofulfillment_data and sofulfillment_data.get('data'):
        combined_fullfillment_data['data']['getFulfillmentsBysofulfillmentid'] = sofulfillment_data['data'].get('getFulfillmentsBysofulfillmentid', {})

    directship_query = fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id)
    directship_data = post_api(URL=FOID, query=directship_query, variables=variables)
    if directship_data and directship_data.get('data'):
        combined_fullfillment_data['data']['getAllFulfillmentHeadersSoidFulfillmentid'] = directship_data['data'].get('getAllFulfillmentHeadersSoidFulfillmentid', {})

    fbom_query = fetch_getFbomBySoFulfillmentid_query(fulfillment_id)
    fbom_data = post_api(URL=FFBOM, query=fbom_query, variables=variables)
    if fbom_data and fbom_data.get('data'):
        combined_fullfillment_data['data']['getFbomBySoFulfillmentid'] = fbom_data['data'].get('getFbomBySoFulfillmentid', {})

    return combined_fullfillment_data

def fileldValidation(filters, format_type, region):
    data_row_export = {}
    primary_in_filters = []
    secondary_in_filters = []

    for field in filters:
        if field in PRIMARY_FIELDS:
            primary_in_filters.append(field)
        elif field in SECONDARY_FIELDS:
            secondary_in_filters.append(field)

    if not primary_in_filters:
        return {
            "status": "error",
            "message": "At least one primary field is required in filters."
        }

    primary_filters = {key: filters[key] for key in primary_in_filters}
    secondary_filters = {key: filters[key] for key in secondary_in_filters}
    result_map = {}

    if 'Sales_Order_id' in primary_filters:
        so_ids = list(set(x.strip() for x in primary_filters['Sales_Order_id'].split(',') if x.strip()))
        salesorder_results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(combined_salesorder_fetch, soid) for soid in so_ids]
            for future in as_completed(futures):
                try:
                    res = future.result()
                    if res:
                        salesorder_results.append(res)
                except Exception as e:
                    print(f"Error in SalesOrder ID fetch: {e}")
        result_map['Sales_Order_id'] = salesorder_results

    if 'foid' in primary_filters:
        foids = list(set(x.strip() for x in primary_filters['foid'].split(',') if x.strip()))
        foid_result = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(combined_foid_fetch, foid) for foid in foids]
            for future in as_completed(futures):
                try:
                    foid_result.append(future.result())
                except Exception as e:
                    print(f"Error in FO ID fetch: {e}")
        result_map['foid'] = foid_result

    if 'wo_id' in primary_filters:
        woids = list(set(x.strip() for x in primary_filters['wo_id'].split(',') if x.strip()))
        woids_result = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(combined_woid_fetch, woid) for woid in woids]
            for future in as_completed(futures):
                try:
                    woids_result.append(future.result())
                except Exception as e:
                    print(f"Error in WO ID fetch: {e}")

        result_map['wo_id'] = woids_result

    if 'Fullfillment Id' in primary_filters:
        ff_ids = list(set(x.strip() for x in primary_filters['Fullfillment Id'].split(',') if x.strip()))
        fullfillment_results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(combined_fulfillment_fetch, fid) for fid in ff_ids]
            for future in as_completed(futures):
                try:
                    res = future.result()
                    if res:
                        fullfillment_results.append(res)
                except Exception as e:
                    print(f"Error in Fullfillment Id fetch: {e}")
        result_map['Fullfillment Id'] = fullfillment_results
        formattingData = OutputFormat(json.dumps(result_map))
        
    return {
        "status": "success",
        "message": "Validation and fetch completed.",
        "result_summary": {key: f"{len(val)} response(s)" for key, val in result_map.items()}
    }

def OutputFormat(resulData):
    print(resulData)


    # #prepare the json
    # data_row_export["BUID"]                   = soheader["buid"]
    # data_row_export["PP Date"]                = soheader["ppDate"]
    # data_row_export["Sales Order Id"]         = result["salesOrder"]["salesOrderId"]
    # data_row_export["Fulfillment Id"]         = fulfillment_id
    # data_row_export["Region Code"]            = result["salesOrder"]["region"]
    # data_row_export["wo_ids"]                 = wo_ids # Pleae Don't Change the Key name 
    # data_row_export["FoId"]                   = result["fulfillmentOrders"][0]["foId"]
    # # data_row_export["SN Number"]            = sn_numbers
    # data_row_export["System Qty"]             = fulfillment["systemQty"]
    # data_row_export["Ship By Date"]           = fulfillment["shipByDate"]
    # data_row_export["LOB"]                    = fulfillment["salesOrderLines"][0]["lob"]
    # data_row_export["Ship From Facility"]     = forderline["shipFromFacility"]
    # data_row_export["Ship To Facility"]       = forderline["shipToFacility"]
    # # data_row_export["shipByDate"]           = getFulfillmentsBysofulfillmentid["shipByDate"]
    # data_row_export["Tax Regstrn Num"]        = getFulfillmentsBysofulfillmentid["address"][0]["taxRegstrnNum"]
    # data_row_export["Address Line1"]          = getFulfillmentsBysofulfillmentid["address"][0]["addressLine1"]
    # data_row_export["Postal Code"]            = getFulfillmentsBysofulfillmentid["address"][0]["postalCode"]
    # data_row_export["State Code"]             = getFulfillmentsBysofulfillmentid["address"][0]["stateCode"]
    # data_row_export["City Code"]              = getFulfillmentsBysofulfillmentid["address"][0]["cityCode"]
    # data_row_export["Customer Num"]           = getFulfillmentsBysofulfillmentid["address"][0]["customerNum"]
    # data_row_export["Customer NameExt"]       = getFulfillmentsBysofulfillmentid["address"][0]["customerNameExt"]
    # data_row_export["Country"]                = getFulfillmentsBysofulfillmentid["address"][0]["country"]
    # data_row_export["Create Date"]            = getFulfillmentsBysofulfillmentid["address"][0]["createDate"]
    # data_row_export["Ship Code"]              = getFulfillmentsBysofulfillmentid["shipCode"]
    # data_row_export["Must Arrive By Date"]    = getFulfillmentsBysofulfillmentid["mustArriveByDate"]
    # data_row_export["Update Date"]            = getFulfillmentsBysofulfillmentid["updateDate"]
    # data_row_export["Merge Type"]             = getFulfillmentsBysofulfillmentid["mergeType"]
    # data_row_export["Manifest Date"]          = getFulfillmentsBysofulfillmentid["manifestDate"]
    # data_row_export["Revised Delivery Date"]  = getFulfillmentsBysofulfillmentid["revisedDeliveryDate"]
    # data_row_export["Delivery City"]          = getFulfillmentsBysofulfillmentid["deliveryCity"]
    # data_row_export["Source System Id"]       = sourceSystemId
    # data_row_export["IsDirect Ship"]          = isDirectShip
    # data_row_export["SSC"]                    = ssc
    # data_row_export["OIC Id"]                 = getFulfillmentsBysofulfillmentid["oicId"]
    # data_row_export["Order Date"]             = soheader["orderDate"]

    # export_out=json.dumps(data_row_export, indent=2)
    # print(export_out)
    # flat_list = []

    # # Exclude wo_ids to create the shared base fields
    # base = {k: v for k, v in data_row_export.items() if k != "wo_ids"}

    # # Iterate over each work order
    # for wo in data_row_export.get("wo_ids", []):
    #     sn_numbers = wo.get("SN Number", [])  # Handles case where list is empty or missing
       
    #     if sn_numbers:
    #         for sn in sn_numbers:
    #             flat_wo = {k: v for k, v in wo.items() if k != "SN Number"}
    #             flat_entry = {
    #                 **base,
    #                 **flat_wo,
    #                 "SN Number": sn
    #             }
    #             flat_list.append(flat_entry)
    #     else:
    #         flat_wo = {k: v for k, v in wo.items() if k != "SN Number"}
    #         flat_entry = {
    #             **base,
    #             **flat_wo,
    #             "SN Number": None
    #         }
    #         flat_list.append(flat_entry)

    # # flat_out=json.dumps(flat_list, indent=2)
    # print(json.dumps(flat_list, indent=2))
    # if format_type and format_type=="export":
    #     # export_output = json.dumps(flat_list)
    #     return flat_list
    # elif format_type and format_type=="grid":
    #     desired_order = ['BUID','PP Date','Sales Order Id','Fulfillment Id','Region Code','FoId','System Qty','Ship By Date',
    #                       'LOB','Ship From Facility','Ship To Facility','TaxRegstrn Num','Address Line1','Postal Code','State Code',
    #                       'City Code','Customer Num','Customer Name Ext','Country','Create Date','Ship Code','Must Arrive By Date',
    #                       'Update Date','Merge Type','Manifest Date','Revised Delivery Date','Delivery City','Source System Id','IsDirect Ship',
    #                       'SSC','Vendor Work Order Num','Channel Status Code','Ismultipack','Ship Mode','Is Otm Enabled',
    #                       'SN Number','OIC ID', 'Order Date']
    #     rows = []
    #     for item in flat_list:
    #         reordered_values = [item.get(key) for key in desired_order]

    #         row = {
    #             "columns": [{"value": val if val is not None else ""} for val in reordered_values]
    #         }

    #         rows.append(row)
    #     return rows
        
    # else:
    #     print("Format type is not part of grid/export")
    #     out={"error": "Format type is not part of grid/export"}
    #     return out

if __name__ == "__main__":
    region = "EMEA"
    format_type = 'grid'
    filters = {
        "Sales_Order_id": "1004543337,483713,416695",
        "foid": "7336030653629440001",
        "Fullfillment Id": "262135,262136",
        "wo_id": "7360928459,7360928460,7360970693",
        "Sales_order_ref": "REF123456",
        "Order_create_date": "2025-07-15",
        "ISMULTIPACK": "Yes",
        "BUID": "202",
        "Facility": "WH_BANGALORE",
        "Manifest_ID": "MANI0001",
        "order_date": "2025-07-15"
    }

    result = fileldValidation(filters=filters, format_type=format_type, region=region)
    print(json.dumps(result, indent=2))
