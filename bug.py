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
    combined_salesorder_data = {'data': {}}

    # Prepare input variables
    soi = {"salesorderIds": [so_id]}

    # First Query - getSoheaderBySoids
    try:
        soaorder_query = fetch_soaorder_query()
        soaorder = post_api(URL=SOPATH, query=soaorder_query, variables=soi)
        soheaders = soaorder.get('data', {}).get('getSoheaderBySoids', [])

        # Ensure it's a list
        if isinstance(soheaders, dict):
            soheaders = [soheaders]
        elif not isinstance(soheaders, list):
            soheaders = []

        combined_salesorder_data['data']['getSoheaderBySoids'] = soheaders
    except Exception as e:
        print(f"[ERROR] getSoheaderBySoids failed for {so_id}: {e}")
        combined_salesorder_data['data']['getSoheaderBySoids'] = []

    # Second Query - getBySalesorderids
    try:
        salesorder_query = fetch_salesorder_query(so_id)
        salesorder = post_api(URL=FID, query=salesorder_query, variables=None)
        soorders = salesorder.get('data', {}).get('getBySalesorderids', [])

        # Ensure it's a list
        if isinstance(soorders, dict):
            soorders = [soorders]
        elif not isinstance(soorders, list):
            soorders = []

        combined_salesorder_data['data']['getBySalesorderids'] = soorders
    except Exception as e:
        print(f"[ERROR] getBySalesorderids failed for {so_id}: {e}")
        combined_salesorder_data['data']['getBySalesorderids'] = []

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
    # print(json.dumps(result_map,indent=2))
    formattingData = OutputFormat(result_map, format_type=format_type)

    return {
        "status": "success",
        "message": "Validation and fetch completed.",
        "result_summary": {key: f"{len(val)} response(s)" for key, val in result_map.items()},
        "data": formattingData
    }

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

            # Fulfillment
            fulfillment_entry = fulfillments[so_index] if so_index < len(fulfillments) else {"data": {}}
            fulfillment_data = fulfillment_entry.get("data", {})

            fulfillment_raw = fulfillment_data.get("getFulfillmentsById", {})
            fulfillment = (
                fulfillment_raw[0] if isinstance(fulfillment_raw, list) and fulfillment_raw else
                fulfillment_raw if isinstance(fulfillment_raw, dict) else {}
            )

            sofulfillment_raw = fulfillment_data.get("getFulfillmentsBysofulfillmentid", {})
            sofulfillment = (
                sofulfillment_raw[0] if isinstance(sofulfillment_raw, list) and sofulfillment_raw else
                sofulfillment_raw if isinstance(sofulfillment_raw, dict) else {}
            )

            forderline = (fulfillment.get("salesOrderLines") or [{}])[0] if isinstance(fulfillment, dict) else {}
            address = (sofulfillment.get("address") or [{}])[0] if isinstance(sofulfillment, dict) else {}

            wo_data = wo_ids[so_index] if so_index < len(wo_ids) else []

            data_row_export = {
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
                "wo_ids": wo_data,
            }

            base = {k: v for k, v in data_row_export.items() if k != "wo_ids"}

            for wo in wo_data:
                if not isinstance(wo, dict):
                    print(f"[WARN] Skipping invalid wo entry at row {so_index}: {wo}")
                    continue

                sn_numbers = wo.get("SN Number", [])
                wo_clean = {k: v for k, v in wo.items() if k != "SN Number"}

                if sn_numbers:
                    for sn in sn_numbers:
                        row = {**base, **wo_clean, "SN Number": sn}
                        if not validate_secondary_filters(row, secondary_filters):
                            continue
                        flat_list.append(row)
                else:
                    row = {**base, **wo_clean, "SN Number": None}
                    if not validate_secondary_filters(row, secondary_filters):
                        continue
                    flat_list.append(row)

        except Exception as e:
            print(f"[ERROR] formatting row {so_index}: {e}")
            import traceback
            traceback.print_exc()

    if format_type == "export":
        return flat_list

    elif format_type == "grid":
        desired_order = [
            'BUID','PP Date','Sales Order Id','Fulfillment Id','Region Code','FoId','System Qty','Ship By Date',
            'LOB','Ship From Facility','Ship To Facility','Tax Regstrn Num','Address Line1','Postal Code','State Code',
            'City Code','Customer Num','Customer Name Ext','Country','Create Date','Ship Code','Must Arrive By Date',
            'Update Date','Merge Type','Manifest Date','Revised Delivery Date','Delivery City','Source System Id','IsDirect Ship',
            'SSC','Vendor Work Order Num','Channel Status Code','Ismultipack','Ship Mode','Is Otm Enabled',
            'SN Number','OIC Id', 'Order Date'
        ]

        rows = []
        for item in flat_list:
            row = {
                "columns": [{"value": item.get(key, "")} for key in desired_order]
            }
            rows.append(row)

        return rows

    else:
        return {"error": "Format type is not part of grid/export"}


def validate_secondary_filters(row, filters):
    if not filters:
        return True
    for key, expected_val in filters.items():
        actual_val = str(row.get(key, "")).lower()
        if actual_val != str(expected_val).lower():
            return False
    return True
              

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
        "Fullfillment Id": "262136,262135",
        "wo_id": "7360928459,7360928460,7360970693",
        "Sales_order_ref": "7331634580634656768",
        "Order_create_date": "2025-07-15",
        "ISMULTIPACK": "Yes",
        "BUID": "202",
        "Facility": "WH_BANGALORE",
        "Manifest_ID": "MANI0001",
        "order_date": "2025-07-15"
    }

    result = fileldValidation(filters=filters, format_type=format_type, region=region)
    print(json.dumps(result, indent=2))
