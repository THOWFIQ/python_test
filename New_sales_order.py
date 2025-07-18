import json
import os
import sys
import httpx
from concurrent.futures import ThreadPoolExecutor, as_completed
from graphqlQueries import *

# Config
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(config_path, 'r') as file:
    config = json.load(file)

# Region-based path helper
def get_path(region, key):
    region = region.upper()
    key = key.upper()
    mapping = {
        "FID": f"Linkage_{region}",
        "FOID": f"FM_Order_{region if region != 'DAO' else 'DAO'}",
        "SOPATH": f"SO_Header_{region if region != 'DAO' else 'DAO'}",
        "WOID": f"WO_Details_{region if region != 'DAO' else 'DAO'}",
        "FFBOM": f"FM_BOM_{region if region != 'DAO' else 'DAO'}"
    }
    return config.get(mapping.get(key, ""))

# POST request to GraphQL endpoint
def post_api(url, query, variables=None):
    response = httpx.post(url, json={"query": query, "variables": variables} if variables else {"query": query}, verify=False)
    return response.json()

# Main function for one SO ID
def getbySalesOrderIDs(salesorderid, format_type, region):
    region = region.upper()
    sopath = get_path(region, "SOPATH")
    fid = get_path(region, "FID")
    woid = get_path(region, "WOID")
    ffbom = get_path(region, "FFBOM")
    foid = get_path(region, "FOID")

    combined_data = {"data": {}}
    sales_order_input = {"salesorderIds": [salesorderid]}

    # SO Header
    soaorder = post_api(sopath, fetch_soaorder_query(), sales_order_input)
    combined_data["data"]["getSoheaderBySoids"] = soaorder["data"]["getSoheaderBySoids"]

    # Sales Order Base
    salesorder = post_api(fid, fetch_salesorder_query(salesorderid))
    result = salesorder["data"]["getBySalesorderids"]["result"][0]
    combined_data["data"]["getBySalesorderids"] = salesorder["data"]["getBySalesorderids"]

    # Work Order details
    work_orders = result["workOrders"]
    for wo in work_orders:
        wo_id = wo["woId"]

        wo_detail = post_api(woid, fetch_workOrderId_query(wo_id))
        asn_detail = post_api(fid, fetch_getByWorkorderids_query(wo_id))

        sn_numbers = [
            sn["snNumber"]
            for sn in asn_detail["data"]["getByWorkorderids"]["result"][0]["asnNumbers"]
            if sn.get("snNumber")
        ]

        wo_info = wo_detail["data"]["getWorkOrderById"][0]
        flattened_wo = {
            "Vendor Work Order Num": wo_info["woId"],
            "Channel Status Code": wo_info["channelStatusCode"],
            "Ismultipack": wo_info["woLines"][0].get("ismultipack"),
            "Ship Mode": wo_info["shipMode"],
            "Is Otm Enabled": wo_info["isOtmEnabled"],
            "SN Number": sn_numbers
        }

        for i, item in enumerate(work_orders):
            if item["woId"] == wo_info["woId"]:
                work_orders[i] = flattened_wo

    # Fulfillment Section
    fulfillment_id = result.get("fulfillment", {}).get("fulfillmentId") or (
        result.get("fulfillment", [{}])[0].get("fulfillmentId") if isinstance(result.get("fulfillment"), list) else None
    )

    if fulfillment_id:
        combined_data["data"]["getFulfillmentsById"] = post_api(sopath, fetch_fulfillment_query(), {"fulfillment_id": fulfillment_id})["data"]["getFulfillmentsById"]
        combined_data["data"]["getFulfillmentsBysofulfillmentid"] = post_api(sopath, fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id))["data"]["getFulfillmentsBysofulfillmentid"]
        combined_data["data"]["getAllFulfillmentHeadersSoidFulfillmentid"] = post_api(foid, fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id))["data"]["getAllFulfillmentHeadersSoidFulfillmentid"]
        combined_data["data"]["getFbomBySoFulfillmentid"] = post_api(ffbom, fetch_getFbomBySoFulfillmentid_query(fulfillment_id))["data"]["getFbomBySoFulfillmentid"]

    # FOID
    if result["fulfillmentOrders"]:
        fo_id = result["fulfillmentOrders"][0]["foId"]
        combined_data["data"]["getAllFulfillmentHeadersByFoId"] = post_api(foid, fetch_foid_query(fo_id))["data"]["getAllFulfillmentHeadersByFoId"]

    return build_output(combined_data, format_type, region)

# Flatten and format final output
def build_output(combined_data, format_type, region):
    soheader = combined_data["data"]["getSoheaderBySoids"][0]
    result = combined_data["data"]["getBySalesorderids"]["result"][0]
    fulfillment_id = result["fulfillment"]["fulfillmentId"]
    fulfillment = combined_data["data"]["getFulfillmentsById"][0]["fulfillments"][0]
    forderline = combined_data["data"]["getAllFulfillmentHeadersByFoId"][0]["forderline"][0]
    sofulfill = combined_data["data"]["getFulfillmentsBysofulfillmentid"][0]["fulfillments"][0]
    sourceSystemId = combined_data["data"]["getFulfillmentsBysofulfillmentid"][0]["sourceSystemId"]
    isDirectShip = combined_data["data"]["getAllFulfillmentHeadersSoidFulfillmentid"][0]["isDirectShip"]
    ssc = combined_data["data"]["getFbomBySoFulfillmentid"][0]["ssc"]

    base = {
        "BUID": soheader["buid"],
        "PP Date": soheader["ppDate"],
        "Sales Order Id": result["salesOrder"]["salesOrderId"],
        "Fulfillment Id": fulfillment_id,
        "Region Code": result["salesOrder"]["region"],
        "FoId": result["fulfillmentOrders"][0]["foId"],
        "System Qty": fulfillment["systemQty"],
        "Ship By Date": fulfillment["shipByDate"],
        "LOB": fulfillment["salesOrderLines"][0]["lob"],
        "Ship From Facility": forderline["shipFromFacility"],
        "Ship To Facility": forderline["shipToFacility"],
        "Tax Regstrn Num": sofulfill["address"][0]["taxRegstrnNum"],
        "Address Line1": sofulfill["address"][0]["addressLine1"],
        "Postal Code": sofulfill["address"][0]["postalCode"],
        "State Code": sofulfill["address"][0]["stateCode"],
        "City Code": sofulfill["address"][0]["cityCode"],
        "Customer Num": sofulfill["address"][0]["customerNum"],
        "Customer Name Ext": sofulfill["address"][0]["customerNameExt"],
        "Country": sofulfill["address"][0]["country"],
        "Create Date": sofulfill["address"][0]["createDate"],
        "Ship Code": sofulfill["shipCode"],
        "Must Arrive By Date": sofulfill["mustArriveByDate"],
        "Update Date": sofulfill["updateDate"],
        "Merge Type": sofulfill["mergeType"],
        "Manifest Date": sofulfill["manifestDate"],
        "Revised Delivery Date": sofulfill["revisedDeliveryDate"],
        "Delivery City": sofulfill["deliveryCity"],
        "Source System Id": sourceSystemId,
        "Is Direct Ship": isDirectShip,
        "SSC": ssc,
        "OIC ID": sofulfill["oicId"],
        "Order Date": soheader["orderDate"]
    }

    flat_output = []
    for wo in result["workOrders"]:
        sn_list = wo.get("SN Number", [])
        wo_data = {k: v for k, v in wo.items() if k != "SN Number"}

        if sn_list:
            for sn in sn_list:
                flat_output.append({**base, **wo_data, "SN Number": sn})
        else:
            flat_output.append({**base, **wo_data, "SN Number": None})

    if format_type == "export":
        return json.dumps(flat_output)
    elif format_type == "grid":
        return json.dumps(tablestructural(data=flat_output, IsPrimary=region))
    else:
        return {"error": "Format type must be 'grid' or 'export'"}

# Main function for multiple SO IDs
def getbySalesOrderID(salesorderid, format_type, region):
    output = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(getbySalesOrderIDs, soid, format_type, region) for soid in salesorderid]
        for future in as_completed(futures):
            result = future.result()
            if isinstance(result, str):
                output.extend(json.loads(result))
            elif isinstance(result, dict) and "data" in result:
                output.append(result)

    if format_type == "export":
        return json.dumps(output)
    elif format_type == "grid":
        return json.dumps(tablestructural(data=output, IsPrimary=region))
    else:
        return {"error": "Invalid format type"}
