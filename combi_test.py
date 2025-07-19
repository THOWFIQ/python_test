import os
import sys
import json
import httpx
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from graphqlQueries import *

# Load config
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(config_path, 'r') as file:
    config = json.load(file)

FID = config['Linkage_DAO']
FOID = config['FM_Order_DAO']
SOPATH = config['SO_Header_DAO']
WOID = config['WO_Details_DAO']
FFBOM = config['FM_BOM_DAO']


def post_api(url, query, variables=None):
    response = httpx.post(url, json={"query": query, "variables": variables} if variables else {"query": query}, verify=False)
    return response.json()


def fetch_combination_data(filters):
    combined_data = {"data": {}}
    so_id = filters.get("Sales_Order_id")
    foid = filters.get("foid")
    fulfillment_id = filters.get("Fullfillment_Id")
    wo_id = filters.get("wo_id")

    if not so_id:
        return {"error": "Sales_Order_id is required"}

    # Sales order header
    so_header_query = fetch_soaorder_query()
    so_header = post_api(SOPATH, so_header_query, {"salesorderIds": [so_id]})
    if so_header and so_header.get("data"):
        combined_data["data"]["getSoheaderBySoids"] = so_header["data"]["getSoheaderBySoids"]

    # Sales order linkage
    so_link_query = fetch_salesorder_query(so_id)
    so_link = post_api(FID, so_link_query)
    if so_link and so_link.get("data"):
        combined_data["data"]["getBySalesorderids"] = so_link["data"]["getBySalesorderids"]

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
        fulfillments = post_api(SOPATH, fulfillment_query, {"fulfillment_id": fulfillment_id})
        if fulfillments.get("data"):
            fulfillment = fulfillments["data"]["getFulfillmentsById"][0]["fulfillments"][0]
            combined_data['data']['getFulfillmentsById'] = fulfillments['data']['getFulfillmentsById']

        getFulfillmentsByso_query = fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id)
        sofulfillments = post_api(SOPATH, getFulfillmentsByso_query)
        if sofulfillments.get("data"):
            getFulfillmentsByso = sofulfillments["data"]["getFulfillmentsBysofulfillmentid"][0]
            sourceSystemId = getFulfillmentsByso.get("sourceSystemId", "")

        getAllFulfillmentHeaders_query = fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id)
        fulfillment_headers = post_api(FOID, getAllFulfillmentHeaders_query)
        if fulfillment_headers.get("data"):
            isDirectShip = fulfillment_headers['data']['getAllFulfillmentHeadersSoidFulfillmentid'][0]['isDirectShip']

        getFbom_query = fetch_getFbomBySoFulfillmentid_query(fulfillment_id)
        fbom = post_api(FFBOM, getFbom_query)
        if fbom.get("data"):
            ssc = fbom["data"]["getFbomBySoFulfillmentid"][0]["ssc"]

    if foid:
        foid_query = fetch_foid_query(foid)
        foid_output = post_api(FOID, foid_query)
        if foid_output.get("data"):
            forderline = foid_output["data"]["getAllFulfillmentHeadersByFoId"][0]["forderline"][0]

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


def tablestructural(data, IsPrimary):
    column_meta = {
        "BUID": ("ID", True), "PP Date": ("Date", True), "Sales Order Id": ("ID", True),
        "Fulfillment Id": ("ID", True), "Region Code": ("Code", False), "FoId": ("ID", False),
        "System Qty": ("Other", True), "Ship By Date": ("Date", True), "LOB": ("Other", True),
        "Ship From Facility": ("Facility", True), "Ship To Facility": ("Facility", True),
        "Tax Regstrn Num": ("Other", False), "Address Line1": ("Address", False),
        "Postal Code": ("Address", False), "State Code": ("Code", False), "City Code": ("Address", False),
        "Customer Num": ("Other", False), "Customer Name Ext": ("Other", False),
        "Country": ("Address", True), "Create Date": ("Date", False), "Ship Code": ("Code", False),
        "Must Arrive By Date": ("Date", False), "Update Date": ("Date", False),
        "Merge Type": ("Type", False), "Manifest Date": ("Date", False),
        "Revised Delivery Date": ("Date", False), "Delivery City": ("Address", False),
        "Source System Id": ("ID", False), "IsDirect Ship": ("Flag", False), "SSC": ("Other", False),
        "OIC Id": ("ID", False), "Order Date": ("Date", True), "Work Order Id": ("ID", True),
        "Sales Order Ref": ("ID", True), "Order Create Date": ("Date", True),
        "Ismultipack": ("Flag", False), "Facility": ("Facility", False), "Manifest ID": ("ID", True)
    }

    if not data:
        return {"columns": [], "data": []}

    columns = []
    for key in data[0].keys():
        group, is_primary = column_meta.get(key, ("Other", False))
        columns.append({
            "value": key,
            "sortBy": "ascending",
            "isPrimary": is_primary,
            "group": group,
            "checked": is_primary
        })

    return {"columns": columns, "data": data}


# Entry point
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

    format_type = "grid"  # or "export"
    region = "APJ"

    total_output = fetch_multiple_combination_data(sample_filters)

    # Format response
    if format_type == "export":
        print(json.dumps(total_output, indent=2))
        result = json.dumps(total_output)
    elif format_type == "grid":
        table_grid_output = tablestructural(data=total_output, IsPrimary=region)
        print(json.dumps(table_grid_output, indent=2))
        result = json.dumps(table_grid_output)
    else:
        result = {"error": "Format type is not part of grid/export"}

    # Optional return or further processing
