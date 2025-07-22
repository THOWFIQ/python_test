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
from combinationFunctions import *

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
    
    if fulfillment_id is not None:
        fulfillment_query = fetch_getByFulfillmentids_query(fulfillment_id)
        
        fulfillment_data = post_api(URL=path['FID'], query=fulfillment_query, variables=None)
        
        if fulfillment_data and fulfillment_data.get('data'):
            combined_fullfillment_data['data']['getByFulfillmentids'] = fulfillment_data['data'].get('getByFulfillmentids', [])
        

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

    # print(json.dumps(combined_foid_data, indent=2))
    return combined_foid_data

def combined_woid_fetch(wo_id,region,filters,CollectedValue):
    path = getPath(region)

    wo_query = fetch_getByWorkorderids_query(wo_id)

    wo_data = post_api(URL=path['FID'], query=wo_query, variables=None)
    
    if wo_data and wo_data.get('data'):
        combined_wo_data['data']['getByWorkorderids'] = wo_data['data'].get('getByWorkorderids', {})

    # print(json.dumps(combined_wo_data, indent=2))
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
            salesvalidation = FullfilData['data']['getByFulfillmentids']['result'][0]['salesOrder']['salesOrderId']
            foidvalidation = FullfilData['data']['getByFulfillmentids']['result'][0]['fulfillmentOrders'][0]['foId']
            woidvalidation = FullfilData['data']['getByFulfillmentids']['result'][0]['workOrders'][0]['woId']
            
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
        # print(json.dumps(combined_foid_data,indent=2))
        
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

    DataFormatation = OutputFormat(result_map,format_type=None)
    # print(json.dumps(DataFormatation, indent=2))
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
                print(f"[WARN] sales_orders[{so_index}] is not a dict.")
                continue

            so_data = so_entry.get("data", {})
            get_soheaders = so_data.get("getSoheaderBySoids", [])
            get_salesorders = so_data.get("getBySalesorderids", [])

            if not get_soheaders or not get_salesorders:
                print(f"[WARN] Missing SO headers or sales orders at row {so_index}")
                continue

            soheader = get_soheaders[0] if isinstance(get_soheaders, list) else {}
            salesorder = get_salesorders[0] if isinstance(get_salesorders, list) else {}

            fulfillment = {}
            sofulfillment = {}
            forderline = {}
            address = {}
           
            if so_index < len(fulfillments):
                fulfillment_entry = fulfillments[so_index]
                if isinstance(fulfillment_entry, dict):
                    fulfillment_data = fulfillment_entry.get("data", {})
                    f_raw = fulfillment_data.get("getFulfillmentsById")
                    s_raw = fulfillment_data.get("getFulfillmentsBysofulfillmentid")

                    # Handle if 'getFulfillmentsById' is a list
                    if isinstance(f_raw, list):
                        fulfillment = f_raw[0] if f_raw else {}
                    elif isinstance(f_raw, dict):
                        fulfillment = f_raw

                    if isinstance(s_raw, list):
                        sofulfillment = s_raw[0] if s_raw else {}
                    elif isinstance(s_raw, dict):
                        sofulfillment = s_raw

                    forderline = (fulfillment.get("salesOrderLines") or [{}])[0]
                    address = (sofulfillment.get("address") or [{}])[0]

            if so_index < len(wo_ids):
                wo_data = wo_ids[so_index]
                if isinstance(wo_data, str):
                    wo_data = json.loads(wo_data)
            else:
                wo_data = []

            foid_entry = None
            if foid_data and isinstance(foid_data, dict):
                foid_entry = foid_data.get("data", {}).get("getAllFulfillmentHeadersByFoId", [{}])[0]

            data_row_export = {
                "BUID": soheader.get("buid"),
                "PP Date": soheader.get("ppDate"),
                "Sales Order Id": soheader.get("salesOrderId"),
                "Fulfillment Id": fulfillment.get("fulfillmentId"),
                "Region Code": salesorder.get("region"),
                "FoId": foid_entry.get("foId") if foid_entry else None,
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
                "wo_ids": wo_ids,
            }

            base = {k: v for k, v in data_row_export.items() if k != "wo_ids"}

            for wo in wo_data:
                if isinstance(wo, str):
                    try:
                        wo = json.loads(wo)
                    except:
                        continue
                if not isinstance(wo, dict):
                    continue

                sn_numbers = wo.get("SN Number", [])
                wo_clean = {k: v for k, v in wo.items() if k != "SN Number"}

                if sn_numbers and isinstance(sn_numbers, list):
                    for sn in sn_numbers:
                        row = {**base, **wo_clean, "SN Number": sn}
                        flat_list.append(row)
                else:
                    row = {**base, **wo_clean, "SN Number": None}
                    flat_list.append(row)

        except Exception as e:
            print(f"[ERROR] formatting row {so_index}: {e}")
            import traceback
            traceback.print_exc()
            continue
    print(flat_list)
    exit()
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
        return {"error": "Format type must be either 'grid' or 'export'"}
               

    # flat_out=json.dumps(flat_list, indent=2)
    print(json.dumps(flat_list, indent=2))
    exit()
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


if __name__ == "__main__":
    region = "DAO"
    format_type = 'export'
    filters = {
        # "Sales_Order_id": "1004543337,483713,416695",
        # "Sales_Order_id": "8040047674",
        "foid": "7329527909391728641",
        # "Fullfillment Id": "543376,532626",
        # "wo_id": "7329527968633573377",
        # "Sales_order_ref": "7331634580634656768",
        # "Order_create_date": "2025-07-15",
        # "ISMULTIPACK": "Yes",
        # "BUID": "202",
        # "Facility": "WH_BANGALORE",
        # "Manifest_ID": "MANI0001",
        # "order_date": "2025-07-15"
    }
   
    result = fieldValidation(filters=filters, format_type=format_type, region=region)
    # print(json.dumps(result, indent=2))
