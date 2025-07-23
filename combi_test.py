from flask import request, jsonify
import requests
import httpx
import json
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from graphqlQueries import *

configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

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

combined_salesorder_data  = {'data': {}}
combined_fullfillment_data = {'data': {}}
combined_foid_data = {'data': {}}
combined_wo_data = {'data': {}}

CollectedValue = {
    "sales":False,
    "FullFil":False,
    "Work" : False,
    "Fo":False
}

def getPath(region):
    if region == "EMEA":
        path = {
            "FID": configPath['Linkage_EMEA'],
            "FOID": configPath['FM_Order_EMEA_APJ'],
            "SOPATH": configPath['SO_Header_EMEA_APJ'],
            "WOID": configPath['WO_Details_EMEA_APJ'],
            "FFBOM": configPath['FM_BOM_EMEA_APJ']
        }
        return path
    elif region == "APJ":
        path = {
            "FID": configPath['Linkage_APJ'],
            "FOID": configPath['FM_Order_EMEA_APJ'],
            "SOPATH": configPath['SO_Header_EMEA_APJ'],
            "WOID": configPath['WO_Details_EMEA_APJ'],
            "FFBOM": configPath['FM_BOM_EMEA_APJ']
        }
        return path
    elif region == "DAO":
        path = {
            "FID": configPath['Linkage_DAO'],
            "FOID": configPath['FM_Order_DAO'],
            "SOPATH": configPath['SO_Header_DAO'],
            "WOID": configPath['WO_Details_DAO'],
            "FFBOM": configPath['FM_BOM_DAO']
        }
        return path
    else:
        return "Path Not Found"

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

def combined_salesorder_fetch(so_id,region,filters,CollectedValue):
    path = getPath(region)

    soi = {"salesorderIds": [so_id]}
    if so_id is not None:
        soaorder_query = fetch_soaorder_query()
        soaorder = post_api(URL=path['SOPATH'], query=soaorder_query, variables=soi)
        if soaorder and soaorder.get('data'):
            combined_salesorder_data['data']['getSoheaderBySoids'] = soaorder['data'].get('getSoheaderBySoids', [])

        salesorder_query = fetch_salesorder_query(so_id)
        salesorder = post_api(URL=path['FID'], query=salesorder_query, variables=None)
        if salesorder and salesorder.get('data'):
            combined_salesorder_data['data']['getBySalesorderids'] = salesorder['data'].get('getBySalesorderids', [])
    
    return combined_salesorder_data

def combined_fulfillment_fetch(fulfillment_id,region,filters,CollectedValue):
    path = getPath(region)

    ffQid = {"fulfillment_id": fulfillment_id}
    
    fulfillment_query = fetch_fulfillment_query()
    fulfillment_data = post_api(URL=path['SOPATH'], query=fulfillment_query, variables=ffQid)
    if fulfillment_data and fulfillment_data.get('data'):
        combined_fullfillment_data['data']['getFulfillmentsById'] = fulfillment_data['data'].get('getFulfillmentsById', {})
    salesOrderID = combined_fullfillment_data['data']['getFulfillmentsById'][0]['salesOrderId']
    
    sofulfillment_query = fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id)
    sofulfillment_data = post_api(URL=path['SOPATH'], query=sofulfillment_query, variables=ffQid)
    if sofulfillment_data and sofulfillment_data.get('data'):
        combined_fullfillment_data['data']['getFulfillmentsBysofulfillmentid'] = sofulfillment_data['data'].get('getFulfillmentsBysofulfillmentid', {})
    
    directship_query = fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id)
    directship_data = post_api(URL=path['FOID'], query=directship_query, variables=ffQid)
    if directship_data and directship_data.get('data'):
        combined_fullfillment_data['data']['getAllFulfillmentHeadersSoidFulfillmentid'] = directship_data['data'].get('getAllFulfillmentHeadersSoidFulfillmentid', {})
    
    fbom_query = fetch_getFbomBySoFulfillmentid_query(fulfillment_id)
    fbom_data = post_api(URL=path['FFBOM'], query=fbom_query, variables=ffQid)
    if fbom_data and fbom_data.get('data'):
        combined_fullfillment_data['data']['getFbomBySoFulfillmentid'] = fbom_data['data'].get('getFbomBySoFulfillmentid', {})
    
    ffoid_query = fetch_salesorder_query(salesOrderID)
    ffoidData = post_api(URL=path['FID'], query=ffoid_query, variables=None)
    if ffoidData and ffoidData.get('data'):
        combined_fullfillment_data['data']['getBySalesorderids'] = ffoidData['data'].get('getBySalesorderids', [])
   
    return combined_fullfillment_data

def combined_foid_fetch(fo_id,region,filters,CollectedValue):
    path = getPath(region)

    foid_query = fetch_foid_query(fo_id)

    foid_output = post_api(URL=path['FOID'], query=foid_query, variables=None)
    
    if foid_output and foid_output.get('data'):
        combined_foid_data['data']['getAllFulfillmentHeadersByFoId'] = foid_output['data']['getAllFulfillmentHeadersByFoId']

    fulfillment_id = combined_foid_data['data']['getAllFulfillmentHeadersByFoId'][0]['fulfillmentId']

    if fulfillment_id is not None:
        fulfillment_query = fetch_getByFulfillmentids_query(fulfillment_id)
        
        fulfillment_data = post_api(URL=path['FID'], query=fulfillment_query, variables=None)
        
        if fulfillment_data and fulfillment_data.get('data'):
            combined_foid_data['data']['getAllFulfillmentHeadersByFoId'] = fulfillment_data['data'].get('getByFulfillmentids', [])

    return combined_foid_data

def combined_woid_fetch(wo_id,region,filters,CollectedValue):
    path = getPath(region)

    wo_query = fetch_getByWorkorderids_query(wo_id)

    wo_data = post_api(URL=path['FID'], query=wo_query, variables=None)
    
    if wo_data and wo_data.get('data'):
        combined_wo_data['data']['getByWorkorderids'] = wo_data['data'].get('getByWorkorderids', {})

    return combined_wo_data
    
def threadFunction(functionName,ids,region,filters,CollectedValue):
    result = []
    with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(functionName, id, region,filters,CollectedValue) for id in ids]
            for future in as_completed(futures):                  
                try:
                    res = future.result()
                    if res:
                        result.append(res)
                except Exception as e:
                    print(f"Error in Thread Function fetch: {e}")
    return result

def fieldValidation(filters, format_type, region):
    
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
    
    if 'Sales_Order_id' in primary_filters and CollectedValue['sales'] == False:
        
        so_ids = list(set(x.strip() for x in primary_filters['Sales_Order_id'].split(',') if x.strip()))
       
        threadRes = threadFunction(combined_salesorder_fetch,so_ids,region,filters,CollectedValue)
        result_map['Sales_Order_id'] = threadRes
        CollectedValue['sales'] = True
        
        for salesData in result_map['Sales_Order_id']:            
            fillvalidation = salesData['data']['getBySalesorderids']['result'][0]['fulfillment'][0]['fulfillmentId']
            foidvalidation = salesData['data']['getBySalesorderids']['result'][0]['fulfillmentOrders'][0]['foId']
            woidvalidation = salesData['data']['getBySalesorderids']['result'][0]['workOrders'][0]['woId']
                 
            if 'Fullfillment Id' in filters:
                if fillvalidation in [filters['Fullfillment Id']] :
                    for value in list(filters['Fullfillment Id']):
                        if value == fillvalidation:                            
                            threadRes = threadFunction(combined_fulfillment_fetch,[fillvalidation],region,filters,CollectedValue)
                            result_map['Fullfillment Id'] = threadRes  
                            CollectedValue['FullFil'] = True                  
                else:
                    threadRes = threadFunction(combined_fulfillment_fetch,[fillvalidation],region,filters,CollectedValue)
                    result_map['Fullfillment Id'] = threadRes
                    CollectedValue['FullFil'] = True
            else:
                threadRes = threadFunction(combined_fulfillment_fetch,[fillvalidation],region,filters,CollectedValue)
                result_map['Fullfillment Id'] = threadRes
                CollectedValue['FullFil'] = True

            if 'foid' in filters:
                if foidvalidation in [filters['foid']] :
                    for value in list(filters['foid']):
                        if value == foidvalidation:                            
                            threadRes = threadFunction(combined_combined_foid_fetchfulfillment_fetch,[foidvalidation],region,filters,CollectedValue)
                            result_map['foid'] = threadRes
                            CollectedValue['Fo'] = True                    
                else:
                    threadRes = threadFunction(combined_foid_fetch,[foidvalidation],region,filters,CollectedValue)
                    result_map['foid'] = threadRes
                    CollectedValue['Fo'] = True 
            else:
                threadRes = threadFunction(combined_foid_fetch,[foidvalidation],region,filters,CollectedValue)
                result_map['foid'] = threadRes
                CollectedValue['Fo'] = True 

            if 'wo_id' in filters:
                if woidvalidation in [filters['wo_id']] :
                    for value in list(filters['wo_id']):
                        if value == woidvalidation:                            
                            threadRes = threadFunction(combined_woid_fetch,[woidvalidation],region,filters,CollectedValue)
                            result_map['wo_id'] = threadRes 
                            CollectedValue['work'] = True                 
                else:
                    threadRes = threadFunction(combined_woid_fetch,[woidvalidation],region,filters,CollectedValue)
                    result_map['wo_id'] = threadRes
                    CollectedValue['work'] = True
            else:
                threadRes = threadFunction(combined_woid_fetch,[woidvalidation],region,filters,CollectedValue)
                result_map['wo_id'] = threadRes  
                CollectedValue['work'] = True           
        
    if 'Fullfillment Id' in primary_filters and CollectedValue['FullFil'] == False:
        
        fil_ids = list(set(x.strip() for x in primary_filters['Fullfillment Id'].split(',') if x.strip()))
        
        threadRes = threadFunction(combined_fulfillment_fetch,fil_ids,region,filters,CollectedValue)
        result_map['Fullfillment Id'] = threadRes

        CollectedValue['FullFil'] = True
        
        for FullfilData in result_map['Fullfillment Id']:
            salesvalidation = FullfilData['data']['getFulfillmentsById'][0]['salesOrderId']
            foidvalidation = FullfilData['data']['getBySalesorderids']['result'][0]['fulfillmentOrders'][0]['foId']
            woidvalidation = FullfilData['data']['getBySalesorderids']['result'][0]['workOrders'][0]['woId']
            
            if 'Sales_Order_id' in filters:
                if salesvalidation in [filters['Sales_Order_id']] :
                    for value in list(filters['Sales_Order_id']):
                        if value == salesvalidation:                            
                            threadRes = threadFunction(combined_salesorder_fetch,[salesvalidation],region,filters,CollectedValue)
                            result_map['Sales_Order_id'] = threadRes  
                            CollectedValue['sales'] = True                  
                else:
                    threadRes = threadFunction(combined_salesorder_fetch,[salesvalidation],region,filters,CollectedValue)
                    result_map['Sales_Order_id'] = threadRes
                    CollectedValue['sales'] = True
            else:
                threadRes = threadFunction(combined_salesorder_fetch,[salesvalidation],region,filters,CollectedValue)
                result_map['Sales_Order_id'] = threadRes
                CollectedValue['sales'] = True

            if 'foid' in filters:
                if foidvalidation in [filters['foid']] :
                    for value in list(filters['foid']):
                        if value == foidvalidation:                            
                            threadRes = threadFunction(combined_foid_fetch,[foidvalidation],region,filters,CollectedValue)
                            result_map['foid'] = threadRes
                            CollectedValue['Fo'] = True                    
                else:
                    threadRes = threadFunction(combined_foid_fetch,[foidvalidation],region,filters,CollectedValue)
                    result_map['foid'] = threadRes
                    CollectedValue['Fo'] = True 
            else:
                threadRes = threadFunction(combined_foid_fetch,[foidvalidation],region,filters,CollectedValue)
                result_map['foid'] = threadRes
                CollectedValue['Fo'] = True 

            if 'wo_id' in filters:
                if woidvalidation in [filters['wo_id']] :
                    for value in list(filters['wo_id']):
                        if value == woidvalidation:                            
                            threadRes = threadFunction(combined_woid_fetch,[woidvalidation],region,filters,CollectedValue)
                            result_map['wo_id'] = threadRes 
                            CollectedValue['work'] = True                 
                else:
                    threadRes = threadFunction(combined_woid_fetch,[woidvalidation],region,filters,CollectedValue)
                    result_map['wo_id'] = threadRes
                    CollectedValue['work'] = True
            else:
                threadRes = threadFunction(combined_woid_fetch,[woidvalidation],region,filters,CollectedValue)
                result_map['wo_id'] = threadRes  
                CollectedValue['work'] = True         
            
    if 'foid' in primary_filters and CollectedValue['Fo'] == False:
        
        fo_ids = list(set(x.strip() for x in primary_filters['foid'].split(',') if x.strip()))
        
        threadRes = threadFunction(combined_foid_fetch,fo_ids,region,filters,CollectedValue)
        result_map['foid'] = threadRes
        CollectedValue['Fo'] = True 
          
        for FOData in result_map['foid']:            
            salesvalidation = FOData['data']['getAllFulfillmentHeadersByFoId']['result'][0]['salesOrder']['salesOrderId']
            fillvalidation = FOData['data']['getAllFulfillmentHeadersByFoId']['result'][0]['fulfillment']['fulfillmentId']
            woidvalidation = FOData['data']['getAllFulfillmentHeadersByFoId']['result'][0]['workOrders'][0]['woId']
            
            if 'Sales_Order_id' in filters:
                if salesvalidation in [filters['Sales_Order_id']] :
                    for value in list(filters['Sales_Order_id']):
                        if value == salesvalidation:                            
                            threadRes = threadFunction(combined_salesorder_fetch,[salesvalidation],region,filters,CollectedValue)
                            result_map['Sales_Order_id'] = threadRes  
                            CollectedValue['FullFil'] = True                  
                else:
                    threadRes = threadFunction(combined_salesorder_fetch,[salesvalidation],region,filters,CollectedValue)
                    result_map['Sales_Order_id'] = threadRes
                    CollectedValue['FullFil'] = True
            else:
                threadRes = threadFunction(combined_salesorder_fetch,[salesvalidation],region,filters,CollectedValue)
                result_map['Sales_Order_id'] = threadRes
                CollectedValue['FullFil'] = True

            if 'Fullfillment Id' in filters:
                if fillvalidation in [filters['Fullfillment Id']] :
                    for value in list(filters['Fullfillment Id']):
                        if value == fillvalidation:                            
                            threadRes = threadFunction(combined_fulfillment_fetch,[fillvalidation],region,filters,CollectedValue)
                            result_map['Fullfillment Id'] = threadRes  
                            CollectedValue['FullFil'] = True                  
                else:
                    threadRes = threadFunction(combined_fulfillment_fetch,[fillvalidation],region,filters,CollectedValue)
                    result_map['Fullfillment Id'] = threadRes
                    CollectedValue['FullFil'] = True
            else:
                threadRes = threadFunction(combined_fulfillment_fetch,[fillvalidation],region,filters,CollectedValue)
                result_map['Fullfillment Id'] = threadRes
                CollectedValue['FullFil'] = True

            if 'wo_id' in filters:
                if woidvalidation in [filters['wo_id']] :
                    for value in list(filters['wo_id']):
                        if value == woidvalidation:                            
                            threadRes = threadFunction(combined_woid_fetch,[woidvalidation],region,filters,CollectedValue)
                            result_map['wo_id'] = threadRes 
                            CollectedValue['work'] = True                 
                else:
                    threadRes = threadFunction(combined_woid_fetch,[woidvalidation],region,filters,CollectedValue)
                    result_map['wo_id'] = threadRes
                    CollectedValue['work'] = True
            else:
                threadRes = threadFunction(combined_woid_fetch,[woidvalidation],region,filters,CollectedValue)
                result_map['wo_id'] = threadRes  
                CollectedValue['work'] = True         

    if 'wo_id' in primary_filters and CollectedValue['Work'] == False:
        woids = list(set(x.strip() for x in primary_filters['wo_id'].split(',') if x.strip()))
        
        threadRes = threadFunction(combined_woid_fetch,woids,region,filters,CollectedValue)
        result_map['WOID'] = threadRes
        CollectedValue['work'] = True
        
        for WOData in result_map['WOID']:
            salesvalidation = WOData['data']['getByWorkorderids']['result'][0]['salesOrder']['salesOrderId']
            fillvalidation = WOData['data']['getByWorkorderids']['result'][0]['fulfillment']['fulfillmentId']
            foidvalidation = WOData['data']['getByWorkorderids']['result'][0]['fulfillmentOrders'][0]['foId']

            if 'Sales_Order_id' in filters:
                if salesvalidation in [filters['Sales_Order_id']] :
                    for value in list(filters['Sales_Order_id']):
                        if value == salesvalidation:                            
                            threadRes = threadFunction(combined_salesorder_fetch,[salesvalidation],region,filters,CollectedValue)
                            result_map['Sales_Order_id'] = threadRes  
                            CollectedValue['FullFil'] = True                  
                else:
                    threadRes = threadFunction(combined_salesorder_fetch,[salesvalidation],region,filters,CollectedValue)
                    result_map['Sales_Order_id'] = threadRes
                    CollectedValue['FullFil'] = True
            else:
                threadRes = threadFunction(combined_salesorder_fetch,[salesvalidation],region,filters,CollectedValue)
                result_map['Sales_Order_id'] = threadRes
                CollectedValue['FullFil'] = True

            if 'Fullfillment Id' in filters:
                if fillvalidation in [filters['Fullfillment Id']] :
                    for value in list(filters['Fullfillment Id']):
                        if value == fillvalidation:                            
                            threadRes = threadFunction(combined_fulfillment_fetch,[fillvalidation],region,filters,CollectedValue)
                            result_map['Fullfillment Id'] = threadRes  
                            CollectedValue['FullFil'] = True                  
                else:
                    threadRes = threadFunction(combined_fulfillment_fetch,[fillvalidation],region,filters,CollectedValue)
                    result_map['Fullfillment Id'] = threadRes
                    CollectedValue['FullFil'] = True
            else:
                threadRes = threadFunction(combined_fulfillment_fetch,[fillvalidation],region,filters,CollectedValue)
                result_map['Fullfillment Id'] = threadRes
                CollectedValue['FullFil'] = True

            if 'foid' in filters:
                if foidvalidation in [filters['foid']] :
                    for value in list(filters['foid']):
                        if value == foidvalidation:                            
                            threadRes = threadFunction(combined_foid_fetch,[foidvalidation],region,filters,CollectedValue)
                            result_map['foid'] = threadRes
                            CollectedValue['Fo'] = True                    
                else:
                    threadRes = threadFunction(combined_foid_fetch,[foidvalidation],region,filters,CollectedValue)
                    result_map['foid'] = threadRes
                    CollectedValue['Fo'] = True 
            else:
                threadRes = threadFunction(combined_foid_fetch,[foidvalidation],region,filters,CollectedValue)
                result_map['foid'] = threadRes
                CollectedValue['Fo'] = True 

    # print(json.dumps(result_map, indent=2))
    return result_map

def OutputFormat(result_map, format_type=None):
    flat_list = []

    sales_orders = result_map.get("Sales_Order_id", [])
    fulfillments = result_map.get("Fullfillment Id", [])
    wo_ids = result_map.get("wo_id", [])
    foid_data = result_map.get("foid", [])

    for so_index, so_entry in enumerate(sales_orders):
        try:
            if not isinstance(so_entry, dict):
                continue

            so_data = so_entry.get("data", {})
            
            get_soheaders = so_data.get("getSoheaderBySoids", [])
            get_salesorders = so_data.get("getBySalesorderids", {})
            
            if not get_soheaders or not get_salesorders:
                continue

            soheader = get_soheaders[0] if isinstance(get_soheaders, list) and get_soheaders else {}
            
            salesorder = get_salesorders.get("result",[{}])[0]
            
            fulfillment = {}
            sofulfillment = {}
            forderline = {}
            address = {}

            if so_index < len(fulfillments):
                fulfillment_entry = fulfillments[so_index]
                if isinstance(fulfillment_entry, dict):
                    fulfillment_data = fulfillment_entry.get("data", {})
                    
                    f_raw = fulfillment_data.get("getFulfillmentsById",[])
                    s_raw = fulfillment_data.get("getFulfillmentsBysofulfillmentid",[])
                    
                    fulfillment = f_raw[0] if isinstance(f_raw, list) and f_raw else f_raw or {}
                    sofulfillment = s_raw[0] if isinstance(s_raw, list) and s_raw else s_raw or {}
            wo_data = []
            if so_index < len(wo_ids):
                wo_entry = wo_ids[so_index]
                if isinstance(wo_entry, str):
                    try:
                        wo_data = json.loads(wo_entry)
                    except:
                        wo_data = []
                elif isinstance(wo_entry, dict):
                    wo_data = [wo_entry]
                elif isinstance(wo_entry, list):
                    wo_data = wo_entry

            foid_entry = None
            if isinstance(foid_data, list) and so_index < len(foid_data):
                entry = foid_data[so_index]
                foid_entry = entry.get("data", {}).get("getAllFulfillmentHeadersByFoId", [{}])

            base_row = {
                "BUID": soheader.get("buid"),
                "PP Date": soheader.get("ppDate"),
                "Sales Order Id": soheader.get("salesOrderId"),
                "Fulfillment Id": salesorder['fulfillment'][0]['fulfillmentId'],
                "Region Code": salesorder['salesOrder']['region'],
                "FoId": salesorder['fulfillmentOrders'][0]['foId'],
                "System Qty": f_raw[0]['fulfillments'][0]['systemQty'],
                "Ship By Date": f_raw[0]['fulfillments'][0]['shipByDate'],
                "LOB": f_raw[0]['fulfillments'][0]['salesOrderLines'][0]['lob'],
                "Ship From Facility": "",
                "Ship To Facility": "",
                "Tax Regstrn Num": sofulfillment['fulfillments'][0]['address'][0]['taxRegstrnNum'],
                "Address Line1": sofulfillment['fulfillments'][0]['address'][0]['addressLine1'],
                "Postal Code": sofulfillment['fulfillments'][0]['address'][0]['postalCode'],
                "State Code": sofulfillment['fulfillments'][0]['address'][0]['stateCode'],
                "City Code": sofulfillment['fulfillments'][0]['address'][0]['cityCode'],
                "Customer Num": sofulfillment['fulfillments'][0]['address'][0]['customerNum'],
                "Customer Name Ext": sofulfillment['fulfillments'][0]['address'][0]['customerNameExt'],
                "Country": sofulfillment['fulfillments'][0]['address'][0]['country'],
                "Create Date": sofulfillment['fulfillments'][0]['address'][0]['createDate'],
                "Ship Code": sofulfillment['fulfillments'][0]['shipCode'],
                "Must Arrive By Date": sofulfillment['fulfillments'][0]["mustArriveByDate"],
                "Update Date": sofulfillment['fulfillments'][0]["updateDate"],
                "Merge Type": sofulfillment['fulfillments'][0]["mergeType"],
                "Manifest Date": sofulfillment['fulfillments'][0]["manifestDate"],
                "Revised Delivery Date": sofulfillment['fulfillments'][0]["revisedDeliveryDate"],
                "Delivery City": sofulfillment['fulfillments'][0]["deliveryCity"],
                "Source System Id": sofulfillment["sourceSystemId"],
                "IsDirect Ship": "",
                "SSC": "",
                "OIC Id": sofulfillment['fulfillments'][0]["oicId"],
                "Order Date": soheader.get("orderDate")
            }

            # if not wo_data:
            #     flat_list.append({**base_row, "SN Number": None})
            # else:
            #     for wo in wo_data:
            #         sn_list = wo.get("SN Number", [])
            #         wo_filtered = {k: v for k, v in wo.items() if k != "SN Number"}
            #         if sn_list:
            #             for sn in sn_list:
            #                 flat_list.append({**base_row, **wo_filtered, "SN Number": sn})
            #         else:
            #             flat_list.append({**base_row, **wo_filtered, "SN Number": None})
        
        except Exception as e:
            print(f"[ERROR] formatting row {so_index}: {e}")
            traceback.print_exc()
            continue
   
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

    return {"error": "Format type must be either 'grid' or 'export'"}


# if __name__ == "__main__":
#     region = "DAO"
#     format_type = 'export'
#     filters = {
#         "Sales_Order_id": "1004543337,483713,416695",
#         # "Sales_Order_id": "8040047674",
#         # "foid": "7329527909391728641",
#         # "Fullfillment Id": "543376,532626",
#         # "wo_id": "7329527968633573377",
#         # "Sales_order_ref": "7331634580634656768",
#         # "Order_create_date": "2025-07-15",
#         # "ISMULTIPACK": "Yes",
#         # "BUID": "202",
#         # "Facility": "WH_BANGALORE",
#         # "Manifest_ID": "MANI0001",
#         # "order_date": "2025-07-15"
#     }
   
#     result = fieldValidation(filters=filters, format_type=format_type, region=region)

#     Final_Response = OutputFormat(result,format_type=format_type)
