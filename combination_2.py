import os
import sys
import json
import httpx
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from graphqlQueries import *

configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

FID     = configPath['Linkage_DAO']
FOID    = configPath['FM_Order_DAO']
SOPATH  = configPath['SO_Header_DAO']
WOID    = configPath['WO_Details_DAO']
FFBOM   = configPath['FM_BOM_DAO']
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
    "Sales_Order_id", "wo_id", "Fullfillment Id", "foid", "order_date"
}
SECONDARY_FIELDS = {
    "ISMULTIPACK", "BUID", "Facility"
}

combined_salesorder_data  = {'data': {}}
combined_fullfillment_data = {'data': {}}
combined_foid_data = {'data': {}}
combined_wo_data = {'data': {}}

CollectedValue = {
    "sales": False,
    "FullFil": False,
    "Work": False,
    "Fo": False
}

def getPath(region):
    try:
        if region == "EMEA":
            return {
                "FID": configPath['Linkage_EMEA'],
                "FOID": configPath['FM_Order_EMEA_APJ'],
                "SOPATH": configPath['SO_Header_EMEA_APJ'],
                "WOID": configPath['WO_Details_EMEA_APJ'],
                "FFBOM": configPath['FM_BOM_EMEA_APJ']
            }
        elif region == "APJ":
            return {
                "FID": configPath['Linkage_APJ'],
                "FOID": configPath['FM_Order_EMEA_APJ'],
                "SOPATH": configPath['SO_Header_EMEA_APJ'],
                "WOID": configPath['WO_Details_EMEA_APJ'],
                "FFBOM": configPath['FM_BOM_EMEA_APJ']
            }
        elif region == "DAO":
            return {
                "FID": configPath['Linkage_DAO'],
                "FOID": configPath['FM_Order_DAO'],
                "SOPATH": configPath['SO_Header_DAO'],
                "WOID": configPath['WO_Details_DAO'],
                "FFBOM": configPath['FM_BOM_DAO']
            }
    except Exception as e:
        print(f"[ERROR] getPath failed: {e}")
        traceback.print_exc()
        return {}

def post_api(URL, query, variables):
    try:
        if variables:
            response = httpx.post(URL, json={"query": query, "variables": variables}, verify=False)
        else:
            response = httpx.post(URL, json={"query": query}, verify=False)
        return response.json()
    except Exception as e:
        print(f"[ERROR] post_api failed: {e}")
        traceback.print_exc()
        return {"error": str(e)}

def combined_salesorder_fetch(so_id, region, filters, CollectedValue):
    try:
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
    except Exception as e:
        print(f"Error in combined_salesorder_fetch: {e}")
        return {}

def combined_fulfillment_fetch(fulfillment_id, region, filters, CollectedValue):
    try:
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
    except Exception as e:
        print(f"Error in combined_fulfillment_fetch: {e}")
        return {}

def combined_foid_fetch(fo_id, region, filters, CollectedValue):
    try:
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
    except Exception as e:
        print(f"Error in combined_foid_fetch: {e}")
        return {}

def combined_woid_fetch(wo_id, region, filters, CollectedValue):
    try:
        path = getPath(region)

        wo_query = fetch_getByWorkorderids_query(wo_id)
        wo_data = post_api(URL=path['FID'], query=wo_query, variables=None)
        if wo_data and wo_data.get('data'):
            combined_wo_data['data']['getByWorkorderids'] = wo_data['data'].get('getByWorkorderids', {})

        return combined_wo_data
    except Exception as e:
        print(f"Error in combined_woid_fetch: {e}")
        return {}
def fieldValidation(filters, format_type, region):
    data_row_export = {}
    primary_in_filters = []
    secondary_in_filters = []

    try:
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

        # ---------- Sales Order Block ----------
        if 'Sales_Order_id' in primary_filters and not CollectedValue['sales']:
            try:
                so_ids = list(set(x.strip() for x in primary_filters['Sales_Order_id'].split(',') if x.strip()))
                threadRes = threadFunction(combined_salesorder_fetch, so_ids, region, filters, CollectedValue)
                result_map['Sales_Order_id'] = threadRes
                CollectedValue['sales'] = True

                for salesData in threadRes:
                    try:
                        fill = salesData['data']['getBySalesorderids']['result'][0]['fulfillment'][0]['fulfillmentId']
                        foid = salesData['data']['getBySalesorderids']['result'][0]['fulfillmentOrders'][0]['foId']
                        woid = salesData['data']['getBySalesorderids']['result'][0]['workOrders'][0]['woId']
                    except Exception as e:
                        print(f"[ERROR] Parsing salesData: {e}")
                        continue

                    # Fulfillment
                    try:
                        if 'Fullfillment Id' in filters and fill in [filters['Fullfillment Id']]:
                            threadRes = threadFunction(combined_fulfillment_fetch, [fill], region, filters, CollectedValue)
                            result_map['Fullfillment Id'] = threadRes
                            CollectedValue['FullFil'] = True
                        elif 'Fullfillment Id' not in filters:
                            threadRes = threadFunction(combined_fulfillment_fetch, [fill], region, filters, CollectedValue)
                            result_map['Fullfillment Id'] = threadRes
                            CollectedValue['FullFil'] = True
                    except Exception as e:
                        print(f"[ERROR] Fulfillment Thread from Sales: {e}")

                    # FOID
                    try:
                        if 'foid' in filters and foid in [filters['foid']]:
                            threadRes = threadFunction(combined_combined_foid_fetchfulfillment_fetch, [foid], region, filters, CollectedValue)
                            result_map['foid'] = threadRes
                        else:
                            threadRes = threadFunction(combined_foid_fetch, [foid], region, filters, CollectedValue)
                            result_map['foid'] = threadRes
                        CollectedValue['Fo'] = True
                    except Exception as e:
                        print(f"[ERROR] FOID Thread from Sales: {e}")

                    # WOID
                    try:
                        if 'wo_id' in filters and woid in [filters['wo_id']]:
                            threadRes = threadFunction(combined_woid_fetch, [woid], region, filters, CollectedValue)
                            result_map['wo_id'] = threadRes
                        else:
                            threadRes = threadFunction(combined_woid_fetch, [woid], region, filters, CollectedValue)
                            result_map['wo_id'] = threadRes
                        CollectedValue['work'] = True
                    except Exception as e:
                        print(f"[ERROR] WOID Thread from Sales: {e}")
            except Exception as e:
                print(f"[ERROR] Sales_Order_id thread block: {e}")

        # ---------- Fulfillment Block ----------
        if 'Fullfillment Id' in primary_filters and not CollectedValue['FullFil']:
            try:
                fil_ids = list(set(x.strip() for x in primary_filters['Fullfillment Id'].split(',') if x.strip()))
                threadRes = threadFunction(combined_fulfillment_fetch, fil_ids, region, filters, CollectedValue)
                result_map['Fullfillment Id'] = threadRes
                CollectedValue['FullFil'] = True

                for fullfilData in threadRes:
                    try:
                        sales = fullfilData['data']['getFulfillmentsById'][0]['salesOrderId']
                        foid = fullfilData['data']['getBySalesorderids']['result'][0]['fulfillmentOrders'][0]['foId']
                        woid = fullfilData['data']['getBySalesorderids']['result'][0]['workOrders'][0]['woId']
                    except Exception as e:
                        print(f"[ERROR] Parsing fullfilData: {e}")
                        continue

                    try:
                        if 'Sales_Order_id' not in filters or sales in [filters['Sales_Order_id']]:
                            threadRes = threadFunction(combined_salesorder_fetch, [sales], region, filters, CollectedValue)
                            result_map['Sales_Order_id'] = threadRes
                            CollectedValue['sales'] = True
                    except Exception as e:
                        print(f"[ERROR] Sales from Fulfillment: {e}")

                    try:
                        threadRes = threadFunction(combined_foid_fetch, [foid], region, filters, CollectedValue)
                        result_map['foid'] = threadRes
                        CollectedValue['Fo'] = True
                    except Exception as e:
                        print(f"[ERROR] FOID from Fulfillment: {e}")

                    try:
                        threadRes = threadFunction(combined_woid_fetch, [woid], region, filters, CollectedValue)
                        result_map['wo_id'] = threadRes
                        CollectedValue['work'] = True
                    except Exception as e:
                        print(f"[ERROR] WOID from Fulfillment: {e}")
            except Exception as e:
                print(f"[ERROR] Fulfillment thread block: {e}")

        # ---------- FOID Block ----------
        if 'foid' in primary_filters and not CollectedValue['Fo']:
            try:
                fo_ids = list(set(x.strip() for x in primary_filters['foid'].split(',') if x.strip()))
                threadRes = threadFunction(combined_foid_fetch, fo_ids, region, filters, CollectedValue)
                result_map['foid'] = threadRes
                CollectedValue['Fo'] = True

                for FOData in threadRes:
                    try:
                        sales = FOData['data']['getAllFulfillmentHeadersByFoId']['result'][0]['salesOrder']['salesOrderId']
                        fill = FOData['data']['getAllFulfillmentHeadersByFoId']['result'][0]['fulfillment']['fulfillmentId']
                        woid = FOData['data']['getAllFulfillmentHeadersByFoId']['result'][0]['workOrders'][0]['woId']
                    except Exception as e:
                        print(f"[ERROR] Parsing FOData: {e}")
                        continue

                    try:
                        threadRes = threadFunction(combined_salesorder_fetch, [sales], region, filters, CollectedValue)
                        result_map['Sales_Order_id'] = threadRes
                        CollectedValue['sales'] = True
                    except Exception as e:
                        print(f"[ERROR] Sales from FOID: {e}")

                    try:
                        threadRes = threadFunction(combined_fulfillment_fetch, [fill], region, filters, CollectedValue)
                        result_map['Fullfillment Id'] = threadRes
                        CollectedValue['FullFil'] = True
                    except Exception as e:
                        print(f"[ERROR] Fulfillment from FOID: {e}")

                    try:
                        threadRes = threadFunction(combined_woid_fetch, [woid], region, filters, CollectedValue)
                        result_map['wo_id'] = threadRes
                        CollectedValue['work'] = True
                    except Exception as e:
                        print(f"[ERROR] WOID from FOID: {e}")
            except Exception as e:
                print(f"[ERROR] FOID thread block: {e}")

        # ---------- WOID Block ----------
        if 'wo_id' in primary_filters and not CollectedValue['work']:
            try:
                wo_ids = list(set(x.strip() for x in primary_filters['wo_id'].split(',') if x.strip()))
                threadRes = threadFunction(combined_woid_fetch, wo_ids, region, filters, CollectedValue)
                result_map['WOID'] = threadRes
                CollectedValue['work'] = True

                for WOData in threadRes:
                    try:
                        sales = WOData['data']['getByWorkorderids']['result'][0]['salesOrder']['salesOrderId']
                        fill = WOData['data']['getByWorkorderids']['result'][0]['fulfillment']['fulfillmentId']
                        foid = WOData['data']['getByWorkorderids']['result'][0]['fulfillmentOrders'][0]['foId']
                    except Exception as e:
                        print(f"[ERROR] Parsing WOData: {e}")
                        continue

                    try:
                        threadRes = threadFunction(combined_salesorder_fetch, [sales], region, filters, CollectedValue)
                        result_map['Sales_Order_id'] = threadRes
                        CollectedValue['sales'] = True
                    except Exception as e:
                        print(f"[ERROR] Sales from WOID: {e}")

                    try:
                        threadRes = threadFunction(combined_fulfillment_fetch, [fill], region, filters, CollectedValue)
                        result_map['Fullfillment Id'] = threadRes
                        CollectedValue['FullFil'] = True
                    except Exception as e:
                        print(f"[ERROR] Fulfillment from WOID: {e}")

                    try:
                        threadRes = threadFunction(combined_foid_fetch, [foid], region, filters, CollectedValue)
                        result_map['foid'] = threadRes
                        CollectedValue['Fo'] = True
                    except Exception as e:
                        print(f"[ERROR] FOID from WOID: {e}")
            except Exception as e:
                print(f"[ERROR] WOID thread block: {e}")

        return result_map

    except Exception as e:
        print(f"[FATAL ERROR] in fieldValidation: {e}")
        return {"status": "error", "message": "Unexpected failure during validation"}

def threadFunction(functionName, ids, region, filters, CollectedValue):
    result = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(functionName, id, region, filters, CollectedValue) for id in ids]
        for future in as_completed(futures):
            try:
                res = future.result()
                if res:
                    result.append(res)
            except Exception as e:
                print(f"[ERROR] Thread Function fetch failed: {e}")
    return result

def OutputFormat(result_map, format_type=None):
    try:
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

                soheader = get_soheaders[0] if isinstance(get_soheaders, list) else {}
                salesorder = get_salesorders.get("result", [{}])[0]

                fulfillment, sofulfillment = {}, {}
                if so_index < len(fulfillments):
                    fulfillment_entry = fulfillments[so_index]
                    if isinstance(fulfillment_entry, dict):
                        fulfillment_data = fulfillment_entry.get("data", {})
                        f_raw = fulfillment_data.get("getFulfillmentsById", [])
                        s_raw = fulfillment_data.get("getFulfillmentsBysofulfillmentid", [])
                        fulfillment = f_raw[0] if f_raw else {}
                        sofulfillment = s_raw[0] if s_raw else {}

                base_row = {
                    "BUID": soheader.get("buid"),
                    "PP Date": soheader.get("ppDate"),
                    "Sales Order Id": soheader.get("salesOrderId"),
                    "Fulfillment Id": salesorder['fulfillment'][0]['fulfillmentId'] if salesorder.get('fulfillment') else "",
                    "Region Code": salesorder.get('salesOrder', {}).get('region', ""),
                    "FoId": salesorder['fulfillmentOrders'][0]['foId'] if salesorder.get('fulfillmentOrders') else "",
                    "System Qty": fulfillment.get('fulfillments', [{}])[0].get('systemQty', ""),
                    "Ship By Date": fulfillment.get('fulfillments', [{}])[0].get('shipByDate', ""),
                    "LOB": fulfillment.get('fulfillments', [{}])[0].get('salesOrderLines', [{}])[0].get('lob', ""),
                    "Ship From Facility": "",
                    "Ship To Facility": "",
                    "Tax Regstrn Num": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('taxRegstrnNum', ""),
                    "Address Line1": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('addressLine1', ""),
                    "Postal Code": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('postalCode', ""),
                    "State Code": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('stateCode', ""),
                    "City Code": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('cityCode', ""),
                    "Customer Num": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('customerNum', ""),
                    "Customer Name Ext": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('customerNameExt', ""),
                    "Country": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('country', ""),
                    "Create Date": sofulfillment.get('fulfillments', [{}])[0].get('address', [{}])[0].get('createDate', ""),
                    "Ship Code": sofulfillment.get('fulfillments', [{}])[0].get("shipCode", ""),
                    "Must Arrive By Date": sofulfillment.get('fulfillments', [{}])[0].get("mustArriveByDate", ""),
                    "Update Date": sofulfillment.get('fulfillments', [{}])[0].get("updateDate", ""),
                    "Merge Type": sofulfillment.get('fulfillments', [{}])[0].get("mergeType", ""),
                    "Manifest Date": sofulfillment.get('fulfillments', [{}])[0].get("manifestDate", ""),
                    "Revised Delivery Date": sofulfillment.get('fulfillments', [{}])[0].get("revisedDeliveryDate", ""),
                    "Delivery City": sofulfillment.get('fulfillments', [{}])[0].get("deliveryCity", ""),
                    "Source System Id": sofulfillment.get("sourceSystemId", ""),
                    "IsDirect Ship": "",
                    "SSC": "",
                    "OIC Id": sofulfillment.get('fulfillments', [{}])[0].get("oicId", ""),
                    "Order Date": soheader.get("orderDate", "")
                }
                flat_list.append(base_row)
            except Exception as inner_e:
                print(f"[ERROR] OutputFormat inner loop at index {so_index}: {inner_e}")
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
            return [{"columns": [{"value": item.get(k, "")} for k in desired_order]} for item in flat_list]

        return {"error": "Format type must be either 'grid' or 'export'"}
    except Exception as e:
        print(f"[ERROR] OutputFormat failed: {e}")
        traceback.print_exc()
        return {"error": str(e)}

# You can similarly wrap `fieldValidation` and other main endpoints if used inside a Flask route

def post_api(URL, query, variables):
    if variables:
        response = httpx.post(URL, json={"query": query, "variables": variables}, verify=False)
    else:
        response = httpx.post(URL, json={"query": query}, verify=False)
    return response.json()

def fetch_combination_data(filters):
    combined_data = {'data': {}}

    so_id = filters.get("Sales_Order_id")
    foid = filters.get("foid")
    fulfillment_id = filters.get("Fullfillment_Id")
    wo_id = filters.get("wo_id")

    if not so_id:
        return {"error": "Sales_Order_id is required"}

    variables = {"salesorderIds": [so_id]}
    soaorder_query = fetch_soaorder_query()
    soaorder = post_api(URL=SOPATH, query=soaorder_query, variables=variables)
    if soaorder and soaorder.get('data'):
        combined_data['data']['getSoheaderBySoids'] = soaorder['data']['getSoheaderBySoids']

    salesorder_query = fetch_salesorder_query(so_id)
    salesorder = post_api(URL=FID, query=salesorder_query, variables=None)
    if salesorder and salesorder.get('data'):
        combined_data['data']['getBySalesorderids'] = salesorder['data']['getBySalesorderids']

    result = combined_data['data']['getBySalesorderids']['result'][0]
    soheader = combined_data['data']['getSoheaderBySoids'][0]
    fulfillment = {}
    getFulfillmentsByso = {}
    forderline = {}
    sourceSystemId = ""
    isDirectShip = False
    ssc = ""

    if fulfillment_id:
        fulfillment_query = fetch_fulfillment_query()
        fulfillments = post_api(URL=SOPATH, query=fulfillment_query, variables={"fulfillment_id": fulfillment_id})
        if fulfillments.get("data"):
            fulfillment = fulfillments["data"]["getFulfillmentsById"][0]["fulfillments"][0]
            combined_data['data']['getFulfillmentsById'] = fulfillments['data']['getFulfillmentsById']

        getFulfillmentsByso_query = fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id)
        sofulfillments = post_api(URL=SOPATH, query=getFulfillmentsByso_query, variables=None)
        if sofulfillments.get("data"):
            getFulfillmentsByso = sofulfillments["data"]["getFulfillmentsBysofulfillmentid"][0]
            sourceSystemId = getFulfillmentsByso.get("sourceSystemId", "")
            combined_data['data']['getFulfillmentsBysofulfillmentid'] = getFulfillmentsByso

        getAllFulfillmentHeaders_query = fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id)
        fulfillment_headers = post_api(URL=FOID, query=getAllFulfillmentHeaders_query, variables=None)
        if fulfillment_headers.get("data"):
            isDirectShip = fulfillment_headers['data']['getAllFulfillmentHeadersSoidFulfillmentid'][0]['isDirectShip']

        getFbom_query = fetch_getFbomBySoFulfillmentid_query(fulfillment_id)
        fbom = post_api(URL=FFBOM, query=getFbom_query, variables=None)
        if fbom.get("data"):
            ssc = fbom["data"]["getFbomBySoFulfillmentid"][0]["ssc"]

    if foid:
        foid_query = fetch_foid_query(foid)
        foid_output = post_api(URL=FOID, query=foid_query, variables=None)
        if foid_output.get("data"):
            forderline = foid_output["data"]["getAllFulfillmentHeadersByFoId"][0]["forderline"][0]

    base_row = {
        "BUID": soheader.get("buid"),
        "PP Date": soheader.get("ppDate"),
        "Sales Order Id": result["salesOrder"].get("salesOrderId"),
        "Fulfillment Id": fulfillment_id,
        "Region Code": result["salesOrder"].get("region"),
        "FoId": foid,
        "System Qty": fulfillment.get("systemQty"),
        "Ship By Date": fulfillment.get("shipByDate"),
        "LOB": fulfillment.get("salesOrderLines", [{}])[0].get("lob"),
        "Ship From Facility": forderline.get("shipFromFacility"),
        "Ship To Facility": forderline.get("shipToFacility"),
        "Tax Regstrn Num": getFulfillmentsByso.get("address", [{}])[0].get("taxRegstrnNum"),
        "Address Line1": getFulfillmentsByso.get("address", [{}])[0].get("addressLine1"),
        "Postal Code": getFulfillmentsByso.get("address", [{}])[0].get("postalCode"),
        "State Code": getFulfillmentsByso.get("address", [{}])[0].get("stateCode"),
        "City Code": getFulfillmentsByso.get("address", [{}])[0].get("cityCode"),
        "Customer Num": getFulfillmentsByso.get("address", [{}])[0].get("customerNum"),
        "Customer Name Ext": getFulfillmentsByso.get("address", [{}])[0].get("customerNameExt"),
        "Country": getFulfillmentsByso.get("address", [{}])[0].get("country"),
        "Create Date": getFulfillmentsByso.get("address", [{}])[0].get("createDate"),
        "Ship Code": getFulfillmentsByso.get("shipCode"),
        "Must Arrive By Date": getFulfillmentsByso.get("mustArriveByDate"),
        "Update Date": getFulfillmentsByso.get("updateDate"),
        "Merge Type": getFulfillmentsByso.get("mergeType"),
        "Manifest Date": getFulfillmentsByso.get("manifestDate"),
        "Revised Delivery Date": getFulfillmentsByso.get("revisedDeliveryDate"),
        "Delivery City": getFulfillmentsByso.get("deliveryCity"),
        "Source System Id": sourceSystemId,
        "IsDirect Ship": isDirectShip,
        "SSC": ssc,
        "OIC Id": getFulfillmentsByso.get("oicId"),
        "Order Date": soheader.get("orderDate"),
        "Work Order Id": wo_id,
        "Sales Order Ref": filters.get("Sales_order_ref"),
        "Order Create Date": filters.get("Order_create_date"),
        "Ismultipack": filters.get("ISMULTIPACK"),
        "Facility": filters.get("Facility"),
        "Manifest ID": filters.get("Manifest_ID")
    }

    return base_row

def fetch_multiple_combination_data(filters_list):
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_combination_data, f) for f in filters_list]
        for future in as_completed(futures):
            try:
                res = future.result()
                results.append(res)
            except Exception as e:
                results.append({"error": str(e)})
    return results

def flatten_table(data_rows):
    table_structure = {
        "columns": [{"value": key} for key in data_rows[0].keys()],
        "data": data_rows
    }
    return table_structure

if __name__ == "__main__":
    sample_filters = [
        {
            "Sales_Order_id": "1004543337",
            "foid": "FO999999",
            "Fullfillment_Id": "262135",
            "wo_id": "7360928459",
            "Sales_order_ref": "REF123456",
            "Order_create_date": "2025-07-15",
            "ISMULTIPACK": "Yes",
            "BUID": "202",
            "Facility": "WH_BANGALORE",
            "Manifest_ID": "MANI0001"
        }
    ]

    output = fetch_multiple_combination_data(sample_filters)
    flat_table = flatten_table(output)

    import json
    print(json.dumps(flat_table, indent=2))
