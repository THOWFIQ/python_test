import requests
import httpx
import json
import os
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from graphqlQueries import *

# Load Config
configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

# API Paths
FID = configPath['Linkage_DAO']
FOID = configPath['FM_Order_DAO']
SOPATH = configPath['SO_Header_DAO']
WOID = configPath['WO_Details_DAO']
FFBOM = configPath['FM_BOM_DAO']

def post_api(URL, query, variables=None):
    response = httpx.post(URL, json={"query": query, "variables": variables} if variables else {"query": query}, verify=False)
    return response.json()

def fetch_and_clean(salesorderid):
    try:
        queries = {
            "getSoheaderBySoids": (SOPATH, fetch_soaorder_query()),
            "getBySalesorderids": (FOID, fetch_salesorder_query(salesorderid)),
            "getFulfillmentsById": (FOID, fetch_fulfillment_query()),
            "getAllFulfillmentHeadersByFoId": (FOID, fetch_foid_query(salesorderid)),
            "getFulfillmentsBysofulfillmentid": (FID, fetch_getFulfillmentsBysofulfillmentid_query(salesorderid)),
            "getAllFulfillmentHeadersSoidFulfillmentid": (FOID, fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(salesorderid)),
            "getFbomBySoFulfillmentid": (FFBOM, fetch_getFbomBySoFulfillmentid_query(salesorderid))
        }

        combined_result = {"data": {}}
        for key, (url, query) in queries.items():
            response = post_api(url, query)
            if response and "data" in response:
                combined_result["data"][key] = response["data"].get(key, list(response["data"].values())[0])
            else:
                combined_result["data"][key] = {}

        return combined_result
    except Exception as e:
        print("Error in fetch_and_clean:", e)
        traceback.print_exc()
        return None

def getbySalesOrderIDs(salesorderid, format_type):
    combined_data = fetch_and_clean(salesorderid)
    if not combined_data:
        return []

    soheader = combined_data["data"].get("getSoheaderBySoids", [{}])[0]
    result = combined_data["data"].get("getBySalesorderids", {}).get("result", [{}])[0]
    fulfillment = combined_data["data"].get("getFulfillmentsById", {}).get("fulfillments", [{}])[0]
    forderline = combined_data["data"].get("getAllFulfillmentHeadersByFoId", {}).get("forderline", [{}])[0]
    getFulfillmentsBysofulfillmentid = combined_data["data"].get("getFulfillmentsBysofulfillmentid", {}).get("fulfillments", [{}])[0]
    sourceSystemId = combined_data["data"].get("getFulfillmentsBysofulfillmentid", {}).get("sourceSystemId")
    isDirectShip = combined_data["data"].get("getAllFulfillmentHeadersSoidFulfillmentid", {}).get("isDirectShip")
    ssc = combined_data["data"].get("getFbomBySoFulfillmentid", {}).get("ssc")

    wo_ids = result.get("workOrders", [])
    base = {
        "BUID": soheader.get("buid"),
        "PP Date": soheader.get("ppDate"),
        "Sales Order Id": result.get("salesOrder", {}).get("salesOrderId"),
        "Fulfillment Id": fulfillment.get("fulfillmentId"),
        "Region Code": result.get("salesOrder", {}).get("region"),
        "FoId": result.get("fulfillmentOrders", [{}])[0].get("foId"),
        "System Qty": fulfillment.get("systemQty"),
        "Ship By Date": fulfillment.get("shipByDate"),
        "LOB": fulfillment.get("salesOrderLines", [{}])[0].get("lob"),
        "Ship From Facility": forderline.get("shipFromFacility"),
        "Ship To Facility": forderline.get("shipToFacility"),
        "Tax Regstrn Num": getFulfillmentsBysofulfillmentid.get("address", [{}])[0].get("taxRegstrnNum"),
        "Address Line1": getFulfillmentsBysofulfillmentid.get("address", [{}])[0].get("addressLine1"),
        "Postal Code": getFulfillmentsBysofulfillmentid.get("address", [{}])[0].get("postalCode"),
        "State Code": getFulfillmentsBysofulfillmentid.get("address", [{}])[0].get("stateCode"),
        "City Code": getFulfillmentsBysofulfillmentid.get("address", [{}])[0].get("cityCode"),
        "Customer Num": getFulfillmentsBysofulfillmentid.get("address", [{}])[0].get("customerNum"),
        "Customer Name Ext": getFulfillmentsBysofulfillmentid.get("address", [{}])[0].get("customerNameExt"),
        "Country": getFulfillmentsBysofulfillmentid.get("address", [{}])[0].get("country"),
        "Create Date": getFulfillmentsBysofulfillmentid.get("address", [{}])[0].get("createDate"),
        "Ship Code": getFulfillmentsBysofulfillmentid.get("shipCode"),
        "Must Arrive By Date": getFulfillmentsBysofulfillmentid.get("mustArriveByDate"),
        "Update Date": getFulfillmentsBysofulfillmentid.get("updateDate"),
        "Merge Type": getFulfillmentsBysofulfillmentid.get("mergeType"),
        "Manifest Date": getFulfillmentsBysofulfillmentid.get("manifestDate"),
        "Revised Delivery Date": getFulfillmentsBysofulfillmentid.get("revisedDeliveryDate"),
        "Delivery City": getFulfillmentsBysofulfillmentid.get("deliveryCity"),
        "Source System Id": sourceSystemId,
        "IsDirect Ship": isDirectShip,
        "SSC": ssc,
        "OIC Id": getFulfillmentsBysofulfillmentid.get("oicId"),
        "Order Date": soheader.get("orderDate")
    }

    flat_list = []
    for wo in wo_ids:
        sn_numbers = wo.get("SN Number", [])
        flat_wo = {k: v for k, v in wo.items() if k != "SN Number"}
        if sn_numbers:
            for sn in sn_numbers:
                flat_list.append({**base, **flat_wo, "SN Number": sn})
        else:
            flat_list.append({**base, **flat_wo, "SN Number": None})

    return flat_list
