from flask import Flask, request, jsonify
import nest_asyncio
import asyncio
import aiohttp
import requests
from dataclasses import dataclass,asdict
from typing import List, Dict, Optional
import traceback
import time
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from graphqlQueries import *

configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

# Define dataclasses
@dataclass
class ASNNumber:
    shipFrom: str
    shipTo: str
    snNumber: str
    sourceManifestId: str
    sourceManifestStatus: str
    shipDate: str
    shipFromVendorId: str

@dataclass
class Fulfillment:
    fulfillmentId: str
    oicId: str
    fulfillmentStatus: str
    sourceSystemStatus: str

@dataclass
class FulfillmentOrder:
    foId: str

@dataclass
class SalesOrder:
    buid: str
    region: str
    salesOrderId: str
    createDate: str
    shipFromVendorId: Optional[str] = None
    sourceManifestId: Optional[str] = None

@dataclass
class SalesOrderIdData:
    salesOrderId: str

@dataclass
class FulfillmentIdData:
    fulfillmentId: str

@dataclass
class WorkOrder:
    woId: str
    woType: str
    channelStatusCode: str
    woStatusCode: str

@dataclass
class FulfillmentRecord:
    salesOrderId: SalesOrder
    workOrders: List[WorkOrder]
    fulfillment: Fulfillment
    fulfillmentOrders: List[FulfillmentOrder]
    asnNumbers: List[ASNNumber]

@dataclass
class SalesRecord:
    salesOrderId: SalesOrder
    workOrders: List[WorkOrder]
    fulfillment: List[Fulfillment]
    fulfillmentOrders: List[FulfillmentOrder]
    asnNumbers: List[ASNNumber]
    vendorInfo: Optional[Dict] = None

@dataclass
class WORecord:
    salesOrderId: SalesOrder
    workOrders: List[WorkOrder]
    fulfillment: Fulfillment
    fulfillmentOrders: List[FulfillmentOrder]
    asnNumbers: List[ASNNumber]

@dataclass
class FORecord:
    salesOrderId: SalesOrder
    fulfillment: Fulfillment
    fulfillmentOrders: List[FulfillmentOrder]
    asnNumbers: List[ASNNumber]

@dataclass
class OrderDateRecord:
    salesOrderId: SalesOrderIdData
    fulfillmentId: FulfillmentIdData

def mainfunction(filters, format_type, region):
    payload = {}
    path = getPath(region)  
    records = []

    if "from" in filters and "to" in filters:
        url = path['SOPATH']
        
        payload = {
            "query": fetch_getOrderDate_query(
                filters["from"],
                filters["to"],
                filters.get("fulfillmentSts", ""),
                filters.get("sourceSystemSts", "")
            )
        }
       
        response = requests.post(url, json=payload, verify=False)
        data = response.json()
        if "errors" in data:
            return jsonify({"error": data["errors"]}), 200
        result = data.get("data", {}).get("getOrdersByDate", {})
        for entry in result.get("result", []):
            record = OrderDateRecord(
                salesOrderId=SalesOrderIdData(entry.get("salesOrderId", {})),
                fulfillmentId=FulfillmentIdData(entry.get("fulfillmentId", {}))
            )
            records.append(record)

    # if "Sales_Order_id" in filters:
    #     url = path['FID']
    #     sales_ids = list(map(str.strip, filters["Sales_Order_id"].split(",")))
    #     for batch in chunk_list(sales_ids, 50):
    #         payload = {
    #             "query": fetch_salesorderf_query(json.dumps(batch))
    #         }
    #         response = requests.post(url, json=payload, verify=False)
    #         data = response.json()
    #         if "errors" in data:
    #             return jsonify({"error": data["errors"]}), 200
    #         result = data.get("data", {}).get("getBySalesorderids", {})

    #         for entry in result.get("result", []):
    #             record = SalesRecord(
    #                 asnNumbers=[ASNNumber(**asn) for asn in entry.get("asnNumbers", [])],
    #                 fulfillment=[Fulfillment(**ff) for ff in entry.get("fulfillment", [])],
    #                 fulfillmentOrders=[FulfillmentOrder(**fo) for fo in entry.get("fulfillmentOrders", [])],
    #                 salesOrderId=SalesOrder(**entry.get("salesOrder", {})),
    #                 workOrders=[WorkOrder(**wo) for wo in entry.get("workOrders", [])]
    #             )
    #             records.append(record)

    if "Sales_Order_id" in filters:
        url = path['FID']
        sales_ids = list(map(str.strip, filters["Sales_Order_id"].split(",")))
        
        for batch in chunk_list(sales_ids, 50):
            payload = {
                "query": fetch_salesorderf_query(json.dumps(batch))
            }
            response = requests.post(url, json=payload, verify=False)
            data = response.json()

            if "errors" in data:
                return jsonify({"error": data["errors"]}), 200

            result = data.get("data", {}).get("getBySalesorderids", {})

            for entry in result.get("result", []):
                record = SalesRecord(
                    asnNumbers=[ASNNumber(**asn) for asn in entry.get("asnNumbers", [])],
                    fulfillment=[Fulfillment(**ff) for ff in entry.get("fulfillment", [])],
                    fulfillmentOrders=[FulfillmentOrder(**fo) for fo in entry.get("fulfillmentOrders", [])],
                    salesOrderId=SalesOrder(**entry.get("salesOrder", {})),
                    workOrders=[WorkOrder(**wo) for wo in entry.get("workOrders", [])]
                )
                
                # 🔹 Extract vendor-related IDs                
                asn_list = entry.get("asnNumbers", [])
                first_asn = asn_list[0] if asn_list else {}

                ship_from_vendor_id = first_asn.get("shipFromVendorId", "")
                source_manifest_id = first_asn.get("sourceManifestId", "")

                # print("i'm here ")
                # print(f"from vendor id : {ship_from_vendor_id}")
                # print(f"source_manifest_id : {source_manifest_id}")
                # exit()
                if ship_from_vendor_id and source_manifest_id:
                    # 🔹 Call ASNODM
                    asn_url = path['ASNODM']
                    asn_payload = {
                        "query": fetch_AsnOrderByID_query(
                            json.dumps(ship_from_vendor_id),
                            json.dumps(source_manifest_id)
                        )
                    }
                    asn_response = requests.post(asn_url, json=asn_payload, verify=False)                    
                    asn_data = asn_response.json()

                    ship_to_vendor_id = (
                        asn_data.get("data", {})
                                .get("getAsnHeaderById", [{}])[0]
                                .get("shipToVendorId")
                    )

                    if ship_to_vendor_id:
                        vendor_url = path['VENDOR']
                        vendor_payload = {
                            "query": fetch_isCFI_query(json.dumps(ship_to_vendor_id))
                        }
                        vendor_response = requests.post(vendor_url, json=vendor_payload, verify=False)
                        vendor_data = vendor_response.json()

                        record.vendorInfo = (
                            vendor_data.get("data", {})
                            .get("getVendormasterByVendorid", [{}])[0]
                            .get("isCfi")
                            )                    
                records.append(record)

    if "Fullfillment Id" in filters:
        url = path['FID']
        fulfillment_ids = list(map(str.strip, filters["Fullfillment Id"].split(",")))
        for batch in chunk_list(fulfillment_ids, 50):
            payload = {
                "query": fetch_getByFulfillmentids_query(json.dumps(batch))
            }
            response = requests.post(url, json=payload, verify=False)
            data = response.json()
            if "errors" in data:
                return jsonify({"error": data["errors"]}), 200
            result = data.get("data", {}).get("getByFulfillmentids", {})
            for entry in result.get("result", []):
                record = FulfillmentRecord(
                    asnNumbers=[ASNNumber(**asn) for asn in entry.get("asnNumbers", [])],
                    fulfillment=Fulfillment(**entry.get("fulfillment", {})),
                    fulfillmentOrders=[FulfillmentOrder(**fo) for fo in entry.get("fulfillmentOrders", [])],
                    salesOrderId=SalesOrder(**entry.get("salesOrder", {})),
                    workOrders=[WorkOrder(**wo) for wo in entry.get("workOrders", [])]
                )
                records.append(record)

    if "foid" in filters:
        url = path['FID']
        fo_ids = list(map(str.strip, filters["foid"].split(",")))
        for batch in chunk_list(fo_ids, 50):
            payload = {
                "query": fetch_getByFoid_query(json.dumps(batch))
            }
            response = requests.post(url, json=payload, verify=False)
            data = response.json()
            if "errors" in data:
                return jsonify({"error": data["errors"]}), 200
            result = data.get("data", {}).get("getByFoid", {})
            for entry in result.get("result", []):
                record = FulfillmentOrderRecord(
                    fulfillmentOrders=[FulfillmentOrder(**fo) for fo in entry.get("fulfillmentOrders", [])],
                    salesOrderId=SalesOrder(**entry.get("salesOrder", {})),
                    fulfillment=[Fulfillment(**ff) for ff in entry.get("fulfillment", [])],
                    workOrders=[WorkOrder(**wo) for wo in entry.get("workOrders", [])]
                )
                records.append(record)

    if "wo_id" in filters:
        url = path['FID']
        wo_ids = list(map(str.strip, filters["wo_id"].split(",")))
        for batch in chunk_list(wo_ids, 50):
            payload = {
                "query": fetch_getByWoId_query(json.dumps(batch))
            }
            response = requests.post(url, json=payload, verify=False)
            data = response.json()
            if "errors" in data:
                return jsonify({"error": data["errors"]}), 200
            result = data.get("data", {}).get("getByWoId", {})
            for entry in result.get("result", []):
                record = WorkOrderRecord(
                    workOrders=[WorkOrder(**wo) for wo in entry.get("workOrders", [])],
                    salesOrderId=SalesOrder(**entry.get("salesOrder", {})),
                    fulfillment=[Fulfillment(**ff) for ff in entry.get("fulfillment", [])],
                    fulfillmentOrders=[FulfillmentOrder(**fo) for fo in entry.get("fulfillmentOrders", [])]
                )
                records.append(record)

    graphql_request = []
    countReqNo = 0
    for obj in records:
        countReqNo += 1

        if obj.salesOrderId and obj.salesOrderId.salesOrderId:
            graphql_request.append({
                "url": path['FID'],
                "query": fetch_salesorder_query(json.dumps(obj.salesOrderId.salesOrderId))
            })
            print(f"[{countReqNo}] Sales Order: {obj.salesOrderId.salesOrderId}")

        fulfillment_id = None
        if hasattr(obj, "fulfillmentId"):
            fulfillment_id = getattr(obj.fulfillmentId, "fulfillmentId", obj.fulfillmentId)
        elif hasattr(obj, "fulfillment") and obj.fulfillment:
            if isinstance(obj.fulfillment, Fulfillment):
                fulfillment_id = obj.fulfillment.fulfillmentId
            elif isinstance(obj.fulfillment, list) and isinstance(obj.fulfillment[0], Fulfillment):
                fulfillment_id = obj.fulfillment[0].fulfillmentId

        if fulfillment_id:
            graphql_request.append({
                "url": path['SOPATH'],
                "query": fetch_fulfillmentf_query(json.dumps(fulfillment_id),
                                                  json.dumps(obj.salesOrderId.salesOrderId))
            })
            graphql_request.append({
                "url": path['FID'],
                "query": fetch_getByFulfillmentids_query(json.dumps(fulfillment_id))
            })
            print(f"[{countReqNo}] Fulfillment ID: {fulfillment_id}")

    results = asyncio.run(run_all(graphql_request))
    return results

async def fetch_graphql(session, url, query):
    async with session.post(url, json={"query": query}) as response:
        return await response.json()

async def run_all(graphql_request):

    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_graphql(session, req["url"], req["query"])
            for req in graphql_request
        ]
        results = await asyncio.gather(*tasks)

        for i, result in enumerate(results, 1):
            print(f"\n--- Result {i} ---")
            #print(result)
        # Run the async function in Jupyter
        return results

def OutputFormat(result_map, format_type=None, region=None):
    print(json.dumps(result_map,indent=2))
    exit()
    try:
        # Extract relevant blocks
        salesorders = list(map(
            lambda item: item["data"]["getBySalesorderids"]["result"][0],
            filter(lambda item: "getBySalesorderids" in item.get("data", {}), result_map)
        ))

        fulfillments_by_id = list(map(
            lambda item: item["data"]["getFulfillmentsById"][0],
            filter(lambda item: "getFulfillmentsById" in item.get("data", {}), result_map)
        ))

        salesheaders_by_ids = list(map(
            lambda item: item["data"]["getSoheaderBySoids"][0],
            filter(lambda item: "getSoheaderBySoids" in item.get("data", {}), result_map)
        ))

        # Combine all entries
        flat_list = list(map(lambda idx: {
            "BUID": safe_get(salesorders[idx], ['salesOrder', 'buid']),
            "PP Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'statusDate'])) if safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']) == "PP" else "",
            "IP Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'statusDate'])) if safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']) == "IP" else "",
            "MN Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'statusDate'])) if safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']) == "MN" else "",
            "SC Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'statusDate'])) if safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']) == "SC" else "",
            "CFI Flag": "",
            "Agreement ID": safe_get(salesheaders_by_ids[idx], ['agreementId']),
            "Amount": safe_get(salesheaders_by_ids[idx], ['totalPrice']),
            "Currency Code": safe_get(salesheaders_by_ids[idx], ['currency']),
            "Customer Po Number": safe_get(salesheaders_by_ids[idx], ['poNumber']),
            "Dp ID": safe_get(salesheaders_by_ids[idx], ['dpid']),
            "Location Number": safe_get(salesheaders_by_ids[idx], ['locationNum']),
            "Order Age": safe_get(salesheaders_by_ids[idx], ['orderDate']),
            "Order Amount usd": safe_get(salesheaders_by_ids[idx], ['rateUsdTransactional']),
            "Order Update Date": safe_get(salesheaders_by_ids[idx], ['updateDate']),
            "Rate Usd Transactional": safe_get(salesheaders_by_ids[idx], ['rateUsdTransactional']),
            "Sales Rep Name": safe_get(salesheaders_by_ids[idx], ['salesrep', 0, 'salesRepName']),
            "Shipping Country": safe_get(salesheaders_by_ids[idx], ['address', 0, 'country']),
            "Source System Status": safe_get(fulfillments_by_id[idx], ['sourceSystemId']),
            "Tie Number": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'soLineNum']),
            "Si Number": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'siNumber']),
            "Req Ship Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'shipCode']),
            "Reassigned Ip Date": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']),
            "RDD": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'revisedDeliveryDate']),
            "Product Lob": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'lob']),
            "Payment Term Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'paymentTerm']),
            "Ofs Status Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']),
            "Ofs Status": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'fulfillmentStsCode']),
            "Fulfillment Status": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'fulfillmentStsCode']),
            "DomsStatus": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']),
            "Company Name": safe_get(salesheaders_by_ids[idx], ['address', 0, 'companyName']),
            "Contact Type": safe_get(salesheaders_by_ids[idx], ['address', 0, 'contact', 0, 'contactType']),
            "Special Instruction ID": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'specialinstructions', 0, 'specialInstructionId' ]),
            "Special Instruction Type": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'specialinstructions', 0, 'specialInstructionType' ]),
            "Shipping City Code": safe_get(salesheaders_by_ids[idx], ['address', 0, 'cityCode']),
            "City": safe_get(salesheaders_by_ids[idx], ['address', 0, 'city']),
            "First Name": safe_get(salesheaders_by_ids[idx], ['address', 0, 'firstName']),
            "Last Name": safe_get(salesheaders_by_ids[idx], ['address', 0, 'lastName']),
            "State Code": safe_get(salesheaders_by_ids[idx], ['address', 0, 'stateCode']),
            "Address Line1": safe_get(salesheaders_by_ids[idx], ['address', 0, 'addressLine1']),
            "Address Line2": safe_get(salesheaders_by_ids[idx], ['address', 0, 'addressLine2']),
            "Phone Number": safe_get(salesheaders_by_ids[idx], ['address', 0, 'phone', 0, 'phoneNumber']),
            "Postal Code": safe_get(salesheaders_by_ids[idx], ['address', 0, 'postalCode']),
            "Sales Order ID": safe_get(salesorders[idx], ['salesOrder', 'salesOrderId']),
            "Fulfillment ID": safe_get(salesorders[idx], ['fulfillment', 0, 'fulfillmentId']),
            "Region Code": safe_get(salesorders[idx], ['salesOrder', 'region']),
            "FO ID": safe_get(salesorders[idx], ['fulfillmentOrders', 0, 'foId']),
            "WO ID": safe_get(salesorders[idx], ['workOrders', 0, 'woId']),
            "System Qty": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'systemQty']),
            "Ship By Date": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'shipByDate']),
            "LOB": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'lob']),
            "Ship From Facility": safe_get(salesorders[idx], ['asnNumbers', 0, 'shipFrom']),
            "Ship To Facility": safe_get(salesorders[idx], ['asnNumbers', 0, 'shipTo']),
            "Facility": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'facility']),
            "ASN Number":safe_get(salesorders[idx], ['asnNumbers', 0, 'snNumber']),
            "Tax Regstrn Num": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'taxRegstrnNum']),
            "State Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'stateCode']),
            "City Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'cityCode']),
            "Customer Num": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'customerNum']),
            "Customer Name Ext": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'customerNameExt']),
            "Country": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'country']),
            "Create Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'createDate'])),
            "Ship Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'shipCode']),
            "Must Arrive By Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'mustArriveByDate'])),
            "Update Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'updateDate'])),
            "Merge Type": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'mergeType']),
            "Manifest Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'manifestDate'])),
            "Revised Delivery Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'revisedDeliveryDate'])),
            "Delivery City": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'deliveryCity']),
            "Source System ID": safe_get(fulfillments_by_id[idx], ['sourceSystemId']),
            "OIC ID": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'oicId']),
            "Order Date": dateFormation(safe_get(salesorders[idx], ['salesOrder', 'createDate']))
        }, range(min(len(salesorders), len(fulfillments_by_id), len(salesheaders_by_ids)))))

        if not flat_list:
            return {"Error Message": "No Data Found"}

        if len(flat_list) > 0:
          if format_type == "export":
              return flat_list

          elif format_type == "grid":
              
              desired_order = [
                    "BUID","PP Date","IP Date","MN Date","SC Date","CFI Flag","Agreement ID","Amount","Currency Code",
                    "Customer Po Number","Dp ID","Location Number","Order Age","Order Amount usd","Order Update Date",
                    "Rate Usd Transactional","Sales Rep Name","Shipping Country","Source System Status","Tie Number",
                    "Si Number","Req Ship Code","Reassigned Ip Date","RDD","Product Lob","Payment Term Code","Ofs Status Code",
                    "Ofs Status","Fulfillment Status","DomsStatus","Company Name","Contact Type", "Special Instruction ID", "Special Instruction Type", "Shipping City Code",
                    "City","First Name","Last Name","State Code","Address Line1","Address Line2","Phone Number","Postal Code",
                    "Sales Order ID","Fulfillment ID","Region Code","FO ID","WO ID","System Qty","Ship By Date","LOB",
                    "Ship From Facility","Ship To Facility","Facility","ASN Number","Tax Regstrn Num","State Code","City Code",
                    "Customer Num","Customer Name Ext","Country","Create Date","Ship Code","Must Arrive By Date","Update Date",
                    "Merge Type","Manifest Date","Revised Delivery Date","Delivery City","Source System ID","OIC ID","Order Date"
                    ]
              rows = []
              for item in flat_list:
                  reordered_values = [item.get(key) for key in desired_order]
                 
                  row = {
                      "columns": [{"value": val if val is not None else ""} for val in reordered_values]
                  }

                  rows.append(row)
                  table_grid_output = tablestructural(rows, region) if rows else []
              
              return table_grid_output
        else:            
            Error_Message = {"Error Message": "No Data Found"}
            return Error_Message

        return {"error": "Format type must be either 'grid' or 'export'"}            

    except Exception as e:
        print(f"[ERROR] OutputFormat failed: {e}")
        traceback.print_exc()
        return {"error": str(e)}

def safe_get(data, path, default=""):
    try:
        for key in path:
            if isinstance(key, int):
                data = data[key]
            else:
                data = data.get(key, {})
        return str(data) if data != {} else default
    except (IndexError, KeyError, TypeError):
        return str(default)

def dateFormation(unformatedDate):
    if unformatedDate not in [None, "", "null"]:
        return unformatedDate.split('.')[0]
    else:
        return ""

def chunk_list(data_list, chunk_size):
    """Split list into chunks of given size."""
    for i in range(0, len(data_list), chunk_size):
        yield data_list[i:i + chunk_size]

def getPath(region):
    try:
        if region == "EMEA":
            return {
                "FID": configPath['Linkage_EMEA'],
                "FOID": configPath['FM_Order_EMEA_APJ'],
                "SOPATH": configPath['SO_Header_EMEA_APJ'],
                "WOID": configPath['WO_Details_EMEA_APJ'],
                "FFBOM": configPath['FM_BOM_EMEA_APJ'],
                "ASNODM": configPath['ASNODM_EMEA'],
                "VENDOR": configPath['Vendor_Master_Data_URL_EMEA']
            }
        elif region == "APJ":
            return {
                "FID": configPath['Linkage_APJ'],
                "FOID": configPath['FM_Order_EMEA_APJ'],
                "SOPATH": configPath['SO_Header_EMEA_APJ'],
                "WOID": configPath['WO_Details_EMEA_APJ'],
                "FFBOM": configPath['FM_BOM_EMEA_APJ'],
                "ASNODM": configPath['ASNODM_APJ'],
                "VENDOR": configPath['Vendor_Master_Data_URL_APJ']
            }
        elif region in ["DAO","AMER","LA"]:
            return {
                "FID": configPath['Linkage_DAO'],
                "FOID": configPath['FM_Order_DAO'],
                "SOPATH": configPath['SO_Header_DAO'],
                "WOID": configPath['WO_Details_DAO'],
                "FFBOM": configPath['FM_BOM_DAO'],
                "ASNODM": configPath['ASNODM_DAO'],
                "VENDOR": configPath['Vendor_Master_Data_URL_DAO']
            }
    except Exception as e:
        print(f"[ERROR] getPath failed: {e}")
        traceback.print_exc()
        return {}

if __name__ == '__main__':
    app.run(debug=True)
