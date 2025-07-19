import os
import sys
import json
import httpx
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add path to import GraphQL queries (assume graphqlQueries.py is in the same folder)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from graphqlQueries import *

# Load config file
configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

FID     = configPath['Linkage_DAO']
FOID    = configPath['FM_Order_DAO']
SOPATH  = configPath['SO_Header_DAO']
WOID    = configPath['WO_Details_DAO']
FFBOM   = configPath['FM_BOM_DAO']

def post_api(URL, query, variables):
    response = httpx.post(URL, json={"query": query, "variables": variables} if variables else {"query": query}, verify=False)
    return response.json()

def fetch_combination_data(filters):
    combined_data = {'data': {}}
    
    so_id = filters.get("Sales_Order_id")
    foid = filters.get("foid")
    fulfillment_id = filters.get("Fullfillment_Id")
    wo_id = filters.get("wo_id")

    if not so_id:
        return {"error": "Sales_Order_id is required"}

    # SO Header Data
    variables = {"salesorderIds": [so_id]}
    soaorder_query = fetch_soaorder_query()
    soaorder = post_api(URL=SOPATH, query=soaorder_query, variables=variables)
    if soaorder and soaorder.get('data'):
        combined_data['data']['getSoheaderBySoids'] = soaorder['data']['getSoheaderBySoids']

    # Sales Order
    salesorder_query = fetch_salesorder_query(so_id)
    salesorder = post_api(URL=FID, query=salesorder_query, variables=None)
    if salesorder and salesorder.get('data'):
        combined_data['data']['getBySalesorderids'] = salesorder['data']['getBySalesorderids']

    result = combined_data['data']['getBySalesorderids']['result'][0]
    soheader = combined_data['data']['getSoheaderBySoids'][0]

    # Optional fetches
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

        sofulfillments_query = fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id)
        sofulfillments = post_api(URL=SOPATH, query=sofulfillments_query, variables=None)
        if sofulfillments.get("data"):
            getFulfillmentsByso = sofulfillments["data"]["getFulfillmentsBysofulfillmentid"][0]
            sourceSystemId = getFulfillmentsByso.get("sourceSystemId", "")

        fulfillment_headers_query = fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id)
        fulfillment_headers = post_api(URL=FOID, query=fulfillment_headers_query, variables=None)
        if fulfillment_headers.get("data"):
            isDirectShip = fulfillment_headers['data']['getAllFulfillmentHeadersSoidFulfillmentid'][0]['isDirectShip']

        fbom_query = fetch_getFbomBySoFulfillmentid_query(fulfillment_id)
        fbom = post_api(URL=FFBOM, query=fbom_query, variables=None)
        if fbom.get("data"):
            ssc = fbom["data"]["getFbomBySoFulfillmentid"][0]["ssc"]

    if foid:
        foid_query = fetch_foid_query(foid)
        foid_output = post_api(URL=FOID, query=foid_query, variables=None)
        if foid_output.get("data"):
            forderline = foid_output["data"]["getAllFulfillmentHeadersByFoId"][0]["forderline"][0]

    # Construct the base row
    return {
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

def fetch_multiple_combination_data(filters_list):
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_combination_data, f) for f in filters_list]
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                results.append({"error": str(e)})
    return results

def flatten_table(data_rows):
    column_meta = {
        "BUID": ("ID", True),
        "PP Date": ("Date", True),
        "Sales Order Id": ("ID", True),
        "Fulfillment Id": ("ID", True),
        "Region Code": ("Code", False),
        "FoId": ("ID", False),
        "System Qty": ("Other", True),
        "Ship By Date": ("Date", True),
        "LOB": ("Other", True),
        "Ship From Facility": ("Facility", True),
        "Ship To Facility": ("Facility", True),
        "Tax Regstrn Num": ("Other", False),
        "Address Line1": ("Address", False),
        "Postal Code": ("Address", False),
        "State Code": ("Code", False),
        "City Code": ("Address", False),
        "Customer Num": ("Other", False),
        "Customer Name Ext": ("Other", False),
        "Country": ("Address", True),
        "Create Date": ("Date", False),
        "Ship Code": ("Code", False),
        "Must Arrive By Date": ("Date", False),
        "Update Date": ("Date", False),
        "Merge Type": ("Type", False),
        "Manifest Date": ("Date", False),
        "Revised Delivery Date": ("Date", False),
        "Delivery City": ("Address", False),
        "Source System Id": ("ID", False),
        "IsDirect Ship": ("Flag", False),
        "SSC": ("Other", False),
        "OIC Id": ("ID", False),
        "Order Date": ("Date", True),
        "Work Order Id": ("ID", True),
        "Sales Order Ref": ("ID", True),
        "Order Create Date": ("Date", True),
        "Ismultipack": ("Flag", False),
        "Facility": ("Facility", False),
        "Manifest ID": ("ID", True)
    }

    if not data_rows:
        return {"columns": [], "data": []}

    columns = [
        {
            "value": key,
            "sortBy": "ascending",
            "isPrimary": column_meta.get(key, ("Other", False))[1],
            "group": column_meta.get(key, ("Other", False))[0],
            "checked": column_meta.get(key, ("Other", False))[1]
        }
        for key in data_rows[0]
    ]

    return {"columns": columns, "data": data_rows}

if __name__ == "__main__":
    sample_filters = [
        {
            "Sales_Order_id": "SO123456789",
            "foid": "FO999999",
            "Fullfillment_Id": "FULFILL123",
            "wo_id": "WO321654",
            "Sales_order_ref": "REF123456",
            "Order_create_date": "2025-07-15",
            "ISMULTIPACK": "Yes",
            "Facility": "WH_BANGALORE",
            "Manifest_ID": "MANI0001"
        }
    ]

    output = fetch_multiple_combination_data(sample_filters)
    flat_table = flatten_table(output)
    print(json.dumps(flat_table, indent=2))
