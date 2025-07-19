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
