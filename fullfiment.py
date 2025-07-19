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

# from constants.constants import get_configdetails

# Get the absolute path to the config file
configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))

# Load the JSON
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

FID     = configPath['Linkage_DAO']
FOID    = configPath['FM_Order_DAO']
SOPATH  = configPath['SO_Header_DAO']
WOID    = configPath['WO_Details_DAO']
FFBOM   = configPath['FM_BOM_DAO']

def post_api(URL, query, variables):
    # print(URL, query, variables)
    if variables:
        response = httpx.post(SOPATH, json = {"query": query, "variables": variables}, verify=False)
    else:
        response = httpx.post(URL, json = {"query": query}, verify=False)
    return response.json()

def transform_keys(data):
    def convert(key):
        parts = key.split("_")
        return ''.join(part.capitalize() for part in parts)
    return {convert(k): v for k, v in data.items()}
   
def fetch_and_clean_by_fulfillment(fulfillment_id):
    combined_data = {'data': {}}
    variables = {"fulfillment_id": fulfillment_id}

    fulfillment_query = fetch_fulfillment_query()
    fulfillment_data = post_api(URL=SOPATH, query=fulfillment_query, variables=variables)
    if fulfillment_data and fulfillment_data.get('data'):
        combined_data['data']['getFulfillmentsById'] = fulfillment_data['data']['getFulfillmentsById']

    sofulfillment_query = fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id)
    sofulfillment_data = post_api(URL=SOPATH, query=sofulfillment_query, variables=None)
    if sofulfillment_data and sofulfillment_data.get('data'):
        combined_data['data']['getFulfillmentsBysofulfillmentid'] = sofulfillment_data['data']['getFulfillmentsBysofulfillmentid']

    directship_query = fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id)
    directship_data = post_api(URL=FOID, query=directship_query, variables=None)
    if directship_data and directship_data.get('data'):
        combined_data['data']['getAllFulfillmentHeadersSoidFulfillmentid'] = directship_data['data']['getAllFulfillmentHeadersSoidFulfillmentid']

    fbom_query = fetch_getFbomBySoFulfillmentid_query(fulfillment_id)
    fbom_data = post_api(URL=FFBOM, query=fbom_query, variables=None)
    if fbom_data and fbom_data.get('data'):
        combined_data['data']['getFbomBySoFulfillmentid'] = fbom_data['data']['getFbomBySoFulfillmentid']

    return combined_data

def getbyFulfillmentIDs(fulfillmentid, format_type):
    
    combined_data = fetch_and_clean_by_fulfillment(fulfillmentid)

    if not combined_data:
        return []

    fulfillment = combined_data["data"]["getFulfillmentsById"][0]["fulfillments"][0]
    getFulfillmentsBysofulfillmentid = combined_data["data"]["getFulfillmentsBysofulfillmentid"][0]["fulfillments"][0]
    sourceSystemId = combined_data["data"]["getFulfillmentsBysofulfillmentid"][0]["sourceSystemId"]
    isDirectShip = combined_data["data"]["getAllFulfillmentHeadersSoidFulfillmentid"][0]["isDirectShip"]
    ssc = combined_data["data"]["getFbomBySoFulfillmentid"][0]["ssc"]

    soriD = combined_data["data"]["getFulfillmentsById"][0]["salesOrderId"]

    if soriD:
        soi = {"salesorderIds": [soriD]}
        soaorder_query = fetch_soaorder_query()

        soaorder=post_api(URL=SOPATH, query=soaorder_query, variables=soi)
        if soaorder and soaorder.get('data'):
            combined_data['data']['getSoheaderBySoids'] = soaorder['data']['getSoheaderBySoids']    

    getByFulfillmentids_query = fetch_getByFulfillmentids_query(fulfillmentid)

    FID_result =post_api(URL=FID, query=getByFulfillmentids_query, variables=None)
    # print(f" foid data : {FID_result}")
    
    if FID_result and FID_result.get('data'):
        combined_data['data']['getByFulfillmentids'] = FID_result['data']['getByFulfillmentids']

    fo_id = combined_data['data']['getByFulfillmentids']['result'][0]["fulfillmentOrders"][0]["foId"]
    
    foid_query = fetch_foid_query(fo_id)        

    foid_output=post_api(URL=FOID, query=foid_query, variables=None)
    
    if foid_output and foid_output.get('data'):
        combined_data['data']['getAllFulfillmentHeadersByFoId'] = foid_output['data']['getAllFulfillmentHeadersByFoId']
    
    workorder_query = fetch_salesorder_query(soriD)

    Work_result = post_api(URL=FID, query=workorder_query, variables=None)

    if Work_result and Work_result.get('data'):
        combined_data['data']['getBySalesorderids'] = Work_result['data']['getBySalesorderids']    

    work_orders = combined_data["data"]["getBySalesorderids"]["result"][0]["workOrders"]
    for i in work_orders:
        workOrderId = i["woId"]

        workOrderId_query = fetch_workOrderId_query(workOrderId)

        getWorkOrderById=post_api(URL=WOID, query=workOrderId_query, variables=None)
       
        getByWorkorderids_query = fetch_getByWorkorderids_query(workOrderId)

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
            flattened_wo = {
                "Vendor Work Order Num": wo_detail["woId"],
                "Channel Status Code": wo_detail["channelStatusCode"],
                "Ismultipack": wo_detail["woLines"][0].get("ismultipack"),  # just the first line's value
                "Ship Mode": wo_detail["shipMode"],
                "Is Otm Enabled": wo_detail["isOtmEnabled"],
                "SN Number": sn_numbers
            }
            
            for i, wo in enumerate(work_orders):
                if wo.get("woId") == wo_detail["woId"]:
                    work_orders[i] = flattened_wo.copy()

    # print(f"work order data : {work_orders}")

    # result_list = salesorder.get("data", {}).get("getBySalesorderids", {}).get("result", [])
    wo_ids = [wo for wo in work_orders]
    # print(f"wid : {wo_ids}")
    # sn_numbers = [sn["snNumber"] for sn in result["asnNumbers"] if sn["snNumber"]]

    data_row_export = {
        "BUID": combined_data["data"]["getFulfillmentsById"][0]["buid"],
        "Sales Order Id": soriD,
        "Region Code": combined_data["data"]["getFulfillmentsById"][0]["region"],
        "PP Date": combined_data['data']['getSoheaderBySoids'][0]["ppDate"],
        "Fulfillment Id": fulfillmentid,
        "System Qty": fulfillment["systemQty"],
        "Ship By Date": fulfillment["shipByDate"],
        "LOB": fulfillment["salesOrderLines"][0]["lob"],
        "Ship From Facility": combined_data["data"]["getAllFulfillmentHeadersByFoId"][0]["forderline"][0]['shipFromFacility'],
        "Ship To Facility": combined_data["data"]["getAllFulfillmentHeadersByFoId"][0]["forderline"][0]['shipToFacility'],
        "FOID": fo_id,
        "wo_ids": work_orders,
        "Tax Regstrn Num": getFulfillmentsBysofulfillmentid["address"][0]["taxRegstrnNum"],
        "Address Line1": getFulfillmentsBysofulfillmentid["address"][0]["addressLine1"],
        "Postal Code": getFulfillmentsBysofulfillmentid["address"][0]["postalCode"],
        "State Code": getFulfillmentsBysofulfillmentid["address"][0]["stateCode"],
        "City Code": getFulfillmentsBysofulfillmentid["address"][0]["cityCode"],
        "Customer Num": getFulfillmentsBysofulfillmentid["address"][0]["customerNum"],
        "Customer NameExt": getFulfillmentsBysofulfillmentid["address"][0]["customerNameExt"],
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
        "Order Date": combined_data['data']['getSoheaderBySoids'][0]["orderDate"]
    }
    print(data_row_export)
    flat_list = []

    # Exclude wo_ids to create the shared base fields
    base = {k: v for k, v in data_row_export.items() if k != "wo_ids"}

    # Iterate over each work order
    for wo in data_row_export.get("wo_ids", []):
        sn_numbers = wo.get("SN Number", [])  # Handles case where list is empty or missing
       
        if sn_numbers:
            for sn in sn_numbers:
                flat_wo = {k: v for k, v in wo.items() if k != "SN Number"}
                flat_entry = {
                    **base,
                    **flat_wo,
                    "SN Number": sn
                }
                flat_list.append(flat_entry)
        else:
            flat_wo = {k: v for k, v in wo.items() if k != "SN Number"}
            flat_entry = {
                **base,
                **flat_wo,
                "SN Number": None
            }
            flat_list.append(flat_entry)

    # flat_out=json.dumps(flat_list, indent=2)
    print(json.dumps(flat_list, indent=2))

    if format_type and format_type=="export":
        # export_output = json.dumps(flat_list)
        return flat_list
    elif format_type and format_type=="grid":
        desired_order = ['BUID','PP Date','Sales Order Id','Fulfillment Id','Region Code','FoId','System Qty','Ship By Date',
                          'LOB','Ship From Facility','Ship To Facility','TaxRegstrn Num','Address Line1','Postal Code','State Code',
                          'City Code','Customer Num','Customer Name Ext','Country','Create Date','Ship Code','Must Arrive By Date',
                          'Update Date','Merge Type','Manifest Date','Revised Delivery Date','Delivery City','Source System Id','IsDirect Ship',
                          'SSC','Vendor Work Order Num','Channel Status Code','Ismultipack','Ship Mode','Is Otm Enabled',
                          'SN Number','OIC ID', 'Order Date']
        rows = []
        for item in flat_list:
            reordered_values = [item.get(key) for key in desired_order]

            row = {
                "columns": [{"value": val if val is not None else ""} for val in reordered_values]
            }

            rows.append(row)
        return rows
        
    else:
        print("Format type is not part of grid/export")
        out={"error": "Format type is not part of grid/export"}
        return out
    
def getbyFulfillmentID(fulfillmentids, format_type, region):
    total_output = []

    if not fulfillmentids:
        return {"error": "FulfillmentId is required"}

    if not format_type or format_type not in ["export", "grid"]:
        return {"error": "Format type must be 'export' or 'grid'"}

    if not region:
        return {"error": "Region is required"}

    def fetch_fulfillment(fid):
        try:
            print("Fetching:", fid)
            return getbyFulfillmentIDs(fid, format_type)
        except Exception as e:
            print(f"Error fetching {fid}: {e}")
            return []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_fulfillment, fid) for fid in fulfillmentids]
        for future in as_completed(futures):
            try:
                common_output = future.result()
                total_output.extend(common_output)
            except Exception as e:
                print(f"Exception in future: {e}")

    total_output = [{k: ("" if v is None else v) for k, v in item.items()} for item in total_output]

    if format_type == "export":
        print(json.dumps(total_output, indent=2))
        return json.dumps(total_output)

    elif format_type == "grid":
        table_grid_output = tablestructural(data=total_output, IsPrimary=region)
        print(json.dumps(table_grid_output, indent=2))
        return json.dumps(table_grid_output)

    else:
        return {"error": "Format type is not part of grid/export"}

if __name__ == "__main__":
    fulfillmentIds=["262135"]
    region = "EMEA"
    # fulfillmentIds=["262135", "271698"]
    # cleaned = fetch_and_clean()
    # print(cleaned)
    format_type='grid' #grid/export
    getbyFulfillmentID(fulfillmentids=fulfillmentIds,format_type=format_type,region=region)
