from flask import Flask, request, jsonify
import nest_asyncio
import asyncio
import aiohttp
import requests
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
from functools import reduce
import traceback
import time
import os
import sys
import json

# Ensure nested event loop safe (Flask / notebooks)
nest_asyncio.apply()

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from graphqlQueries import *

configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

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
    vendorsiteid: Optional[Dict] = None 


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
class WorkOrderRecord:
    workOrders: List[WorkOrder]
    salesOrderId: SalesOrder
    fulfillment: Fulfillment
    fulfillmentOrders: List[FulfillmentOrder]
    asnNumbers: List[ASNNumber]
    vendorsiteid: Optional[Dict] = None

@dataclass
class FulfillmentOrderRecord:
    fulfillmentOrders: List[FulfillmentOrder]
    salesOrderId: SalesOrder
    fulfillment: List[Fulfillment]
    workOrders: List[WorkOrder]

@dataclass
class OrderDateRecord:
    salesOrderId: SalesOrderIdData
    fulfillmentId: FulfillmentIdData

SequenceValue = []
def mainfunction(filters, format_type, region):
    payload = {}
    path = getPath(region)
    records = []
    sodata = []
    fildata = []
    fodata = []
    salesOrderidsByOrderDate = []
     
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
            salesOrderidsByOrderDate.append(record.salesOrderId)
    
    salesorder_key = None
    if "Sales_Order_id" in filters:
        salesorder_key = "Sales_Order_id"
    elif salesOrderidsByOrderDate is not None and len(salesOrderidsByOrderDate) > 0:
        salesorder_key = "Sales_Order_id"
    
    if 'Sales_Order_id' in filters:
        uniqueSales_ids = ",".join(sorted(set(filters['Sales_Order_id'].split(','))))
        filters['Sales_Order_id'] = uniqueSales_ids

    if salesorder_key:
        url = path['FID']
        if filters.get(salesorder_key):
            sales_ids = list(map(str.strip, filters[salesorder_key].split(",")))
        if salesOrderidsByOrderDate is not None and len(salesOrderidsByOrderDate) > 0:
            sales_ids = [s.salesOrderId for s in salesOrderidsByOrderDate]

        for batch in chunk_list(sales_ids, 50):
            payload = {"query": fetch_salesorderf_query(json.dumps(batch))}
            response = requests.post(url, json=payload, verify=False)
            data = response.json()
            if "errors" in data:
                return jsonify({"error": data["errors"]}), 200
            result = data.get("data", {}).get("getBySalesorderids", {})
            FullFillmentInitial = result.get("result", [{}])[0].get("fulfillment", [])
            WorkOrderInitial = result.get("result", [{}])[0].get("workOrders", [])
                      
            for entry in result.get("result", []):
                salesid     = entry.get("salesOrder", {}).get("salesOrderId")
                woiid       = entry.get("workOrders", [])
                ffiid       = entry.get("fulfillment", [])
                f0id        = entry.get("fulfillmentOrders", [])
                asn_list    = entry.get("asnNumbers", [])
                first_asn   = asn_list[0] if asn_list else {}
                ship_from_vendor_id = first_asn.get("shipFromVendorId", "")
                source_manifest_id = first_asn.get("sourceManifestId", "")                              
               
                if ship_from_vendor_id and source_manifest_id:
                    asn_url = path['ASNODM']
                    asn_payload = {
                        "query": fetch_AsnOrderByID_query(
                            json.dumps(ship_from_vendor_id),
                            json.dumps(source_manifest_id)
                        )
                    }
                    asn_response = requests.post(asn_url, json=asn_payload, verify=False)
                    asn_data = None
                    try:
                        if asn_response.status_code == 200:
                            asn_data = asn_response.json()
                    except Exception as e:
                        print("Error parsing ASN response JSON:", e)

                    asn_header = [{}]
                    if isinstance(asn_data, dict):
                        data_block = asn_data.get("data")
                        if isinstance(data_block, dict):
                            asn_header = data_block.get("getAsnHeaderById", [{}])
                            
                    if isinstance(asn_header, list) and asn_header and isinstance(asn_header[0], dict):
                        filters["vendorsiteid"] = asn_header[0].get("shipToVendorSiteId")
                    else:
                        print("ASN header missing.")

                if len(woiid) >0:
                    sales_order_ids = filters["Sales_Order_id"].split(",")
                    for wdata in woiid:
                        sodata.append(wdata['woId'])
                        if salesid in sales_order_ids:
                            sales_order_ids.remove(salesid)
                            
                    filters["Sales_Order_id"] = ",".join(sorted(set(sales_order_ids)))
                    filters["wo_id"] = ",".join(sorted(set(sodata)))
                
                if len(ffiid) >0:
                    sales_order_ids = filters["Sales_Order_id"].split(",")
                    for fdata in ffiid:
                        fildata.append(fdata['fulfillmentId'])
                        if salesid in sales_order_ids:
                            sales_order_ids.remove(salesid)
                   
                    filters["Sales_Order_id"] = ",".join(sorted(set(sales_order_ids)))
                   
                    existing_values = set(filters.get("Fullfillment Id", "").split(",")) if filters.get("Fullfillment Id") else set()
                    new_values = set(fildata)
                    combined_values = existing_values.union(new_values)
                    filters["Fullfillment Id"] = ",".join(sorted(combined_values))

                if len(f0id) >0:
                    sales_order_ids = filters["Sales_Order_id"].split(",")
                    for foiddata in f0id:
                        fodata.append(foiddata['foId'])
                        if salesid in sales_order_ids:
                            sales_order_ids.remove(salesid)
                   
                    filters["Sales_Order_id"] = ",".join(sorted(set(sales_order_ids)))
                   
                    existing_values = set(filters.get("foid", "").split(",")) if filters.get("foid") else set()
                    new_values = set(fodata)
                    combined_values = existing_values.union(new_values)
                    filters["foid"] = ",".join(sorted(combined_values))
                
    if "wo_id" in filters or len(sodata)>0:
        url = path['FID']
        if sodata:
            filters["wo_id"] = ",".join(sodata)
        wo_ids = list(map(str.strip, filters["wo_id"].split(",")))
        for batch in chunk_list(wo_ids, 50):
            payload = {"query": fetch_getByWorkorderids_query(json.dumps(batch))}
            response = requests.post(url, json=payload, verify=False)            
            data = response.json()
            if "errors" in data:
                return jsonify({"error": data["errors"]}), 200
            result = data.get("data", {}).get("getByWorkorderids", {})
            for entry in result.get("result", []):
                if "Fullfillment Id" in filters:
                    ffiid = [entry.get("fulfillment", {})]
                    if len(ffiid) >0:
                        Fullfillmnt_ids = filters["Fullfillment Id"].split(",")
                        for fdata in ffiid:
                            fid = fdata.get('fulfillmentId')
                            if fid in fildata:
                                fildata.remove(fid)
                                Fullfillmnt_ids = [x for x in Fullfillmnt_ids if x != fid]

                        filters["Fullfillment Id"] = ",".join(Fullfillmnt_ids)

                record = WorkOrderRecord(
                    workOrders=WorkOrder(**entry.get("workOrder", {})),
                    salesOrderId=SalesOrder(**entry.get("salesOrder", {})),
                    fulfillment=Fulfillment(**entry.get("fulfillment", {})),
                    fulfillmentOrders=[FulfillmentOrder(**fo) for fo in entry.get("fulfillmentOrders", [])],
                    asnNumbers=[ASNNumber(**asn) for asn in entry.get("asnNumbers", [])]
                )
                records.append(record)
   
    fulfillment_key = None
    if "Fullfillment Id" in filters:
        fulfillment_key = "Fullfillment Id"
    elif "Fulfillment Id" in filters:
        fulfillment_key = "Fulfillment Id"
    
    if fulfillment_key and filters[fulfillment_key] != "":
        url = path['FID']
        if filters.get(fulfillment_key):
            fulfillment_ids = list(map(str.strip, filters[fulfillment_key].split(",")))
        
        for batch in chunk_list(fulfillment_ids, 50):
            payload = {"query": fetch_getByFulfillmentids_query(json.dumps(batch))}
            response = requests.post(url, json=payload, verify=False)
            data = response.json()
            if "errors" in data:
                return jsonify({"error": data["errors"]}), 200
            
            result = data.get("data", {}).get("getByFulfillmentids", {})
            for entry in result.get("result", []):                
                ffiid = [entry.get("fulfillment", {})]
                if len(ffiid) >0:
                    Fullfillmnt_ids = filters["Fullfillment Id"].split(",")
                    for fdata in ffiid:
                        fid = fdata.get('fulfillmentId')
                        if fid in fildata:
                            fildata.remove(fid)
                        if fid in Fullfillmnt_ids:
                            Fullfillmnt_ids = [x for x in Fullfillmnt_ids if x != fid]
                        
                    filters["Fullfillment Id"] = ",".join(sorted(set(Fullfillmnt_ids)))
                
                record = FulfillmentRecord(
                    asnNumbers=[ASNNumber(**asn) for asn in entry.get("asnNumbers", [])],
                    fulfillment=Fulfillment(**entry.get("fulfillment", {})),
                    fulfillmentOrders=[FulfillmentOrder(**fo) for fo in entry.get("fulfillmentOrders", [])],
                    salesOrderId=SalesOrder(**entry.get("salesOrder", {})),
                    workOrders=[WorkOrder(**wo) for wo in entry.get("workOrders", [])]
                )
                records.append(record)
    
    if "foid" in filters:
        url = path['FOID']
        fo_ids = list(map(str.strip, filters["foid"].split(",")))
        for batch in chunk_list(fo_ids, 50):
            payload = {"query": fetch_foid_query(json.dumps(batch))}
            response = requests.post(url, json=payload, verify=False)
            data = response.json()
            if "errors" in data:
                return jsonify({"error": data["errors"]}), 200
            result = data.get("data", {}).get("getAllFulfillmentHeadersByFoId", {})            
            for entry in result:             
                fooid = [entry]
                if len(fooid) >0:
                    FO_ids = filters["foid"].split(",")
                    for fodata in ffiid:
                        foid = fodata.get('foid')
                        if foid in fodata:
                            fildata.remove(fid)
                        if foid in FO_ids:
                            FO_ids = [x for x in FO_ids if x != foid]
                        
                    filters["foid"] = ",".join(sorted(set(FO_ids)))

    graphql_request = []
    countReqNo = 0   
    for obj in records:
        sequeneAppend = {}
        countReqNo += 1        
        if hasattr(obj, "salesOrderId") and obj.salesOrderId.salesOrderId:
            graphql_request.append({
                "url": path['FID'],
                "query": fetch_salesorder_query(json.dumps(obj.salesOrderId.salesOrderId))
            })            
            sequeneAppend["salesOrderId"] = obj.salesOrderId.salesOrderId

        if hasattr(obj, "fulfillment"):
            if isinstance(obj.fulfillment, list):
                for fulfillment in obj.fulfillment:
                    fulfillment_id = getattr(fulfillment, "fulfillmentId", None)
                    if fulfillment_id:
                        sales_order_id = getattr(obj.salesOrderId, "salesOrderId", obj.salesOrderId) \
                            if hasattr(obj, "salesOrderId") else getattr(obj.salesOrder, "salesOrderId", None)

                        graphql_request.append({
                            "url": path['SOPATH'],
                            "query": fetch_fulfillmentf_query(json.dumps(fulfillment_id),
                                                            json.dumps(sales_order_id))
                        })
                        graphql_request.append({
                            "url": path['FID'],
                            "query": fetch_getByFulfillmentids_query(json.dumps(fulfillment_id))
                        })
                        sequeneAppend = {"fulfillment":fulfillment_id}
            else:
                fulfillment_id = getattr(obj.fulfillment, "fulfillmentId", None)
                if fulfillment_id:
                    sales_order_id = getattr(obj.salesOrderId, "salesOrderId", obj.salesOrderId) \
                        if hasattr(obj, "salesOrderId") else getattr(obj.salesOrder, "salesOrderId", None)

                    graphql_request.append({
                        "url": path['SOPATH'],
                        "query": fetch_fulfillmentf_query(json.dumps(fulfillment_id),
                                                        json.dumps(sales_order_id))
                    })
                    graphql_request.append({
                        "url": path['FID'],
                        "query": fetch_getByFulfillmentids_query(json.dumps(fulfillment_id))
                    })
                    sequeneAppend["fulfillment"] = fulfillment_id
        
        if hasattr(obj, "asnNumbers") and obj.asnNumbers:
            first_asn = obj.asnNumbers[0]
            ship_from_vendor_id = first_asn.shipFromVendorId
            source_manifest_id = first_asn.sourceManifestId

            if ship_from_vendor_id and source_manifest_id:
                graphql_request.append({
                    "url": path['ASNODM'],
                    "query": fetch_AsnOrderByID_query(
                        json.dumps(ship_from_vendor_id),
                        json.dumps(source_manifest_id))
                })

        if hasattr(obj, "asnNumbers") and obj.asnNumbers:
            first_asn = obj.asnNumbers[0]
            ship_from_vendor_id = first_asn.shipFromVendorId
            source_manifest_id = first_asn.sourceManifestId

            if ship_from_vendor_id and source_manifest_id:
                graphql_request.append({
                    "url": path['ASNODM'],
                    "query": fetch_AsnDetailById_query(
                        json.dumps(region),
                        json.dumps(ship_from_vendor_id),
                        json.dumps(source_manifest_id))
                })
       
        if 'vendorsiteid' in filters and filters["vendorsiteid"]:
            graphql_request.append({
                "url": path['VENDOR'],
                "query": fetch_isCFI_query(json.dumps(filters["vendorsiteid"]))
            })

        if hasattr(obj, "workOrders") and obj.workOrders:            
            if not isinstance(obj.workOrders, list):
                obj.workOrders = [obj.workOrders]
            for workid in obj.workOrders:
                graphql_request.append({
                    "url": path['WOID'],
                    "query": fetch_workOrderId_query(json.dumps(workid.woId))
                })
                sequeneAppend["WorkOrderID"] = workid.woId

        if hasattr(obj, "fulfillmentOrders") and obj.fulfillmentOrders:
            if isinstance(obj.fulfillmentOrders, list):
                objj = obj.fulfillmentOrders[0]
                obj.foId = [objj]               
                for FOid in obj.foId:
                    graphql_request.append({
                        "url": path['FOID'],
                        "query": fetch_foid_query(json.dumps(FOid.foId))
                    })                
                    sequeneAppend["FOID"] = FOid.foId

        SequenceValue.append(sequeneAppend)
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
            return results

def OutputFormat(result_map, format_type=None, region=None): 
    try:
        salesorders = [
            item["data"]["getBySalesorderids"]["result"][0]
            for item in result_map
            if isinstance(item.get("data"), dict)
            and "getBySalesorderids" in item["data"]
            and isinstance(item["data"]["getBySalesorderids"].get("result"), list)
            and item["data"]["getBySalesorderids"]["result"]
        ]

        fulfillments_by_id = [
            item["data"]["getFulfillmentsById"][0]
            for item in result_map
            if isinstance(item.get("data"), dict)
            and "getFulfillmentsById" in item["data"]
            and isinstance(item["data"]["getFulfillmentsById"], list)
            and item["data"]["getFulfillmentsById"]
        ]
        
        salesheaders_by_ids = [
            item["data"]["getSoheaderBySoids"][0]
            for item in result_map
            if isinstance(item.get("data"), dict)
            and "getSoheaderBySoids" in item["data"]
            and isinstance(item["data"]["getSoheaderBySoids"], list)
            and item["data"]["getSoheaderBySoids"]
        ]
        
        VendormasterByVendor = [
            item["data"]["getVendormasterByVendorsiteid"][0]
            for item in result_map
            if isinstance(item.get("data"), dict)
            and "getVendormasterByVendorsiteid" in item["data"]
            and isinstance(item["data"]["getVendormasterByVendorsiteid"], list)
            and item["data"]["getVendormasterByVendorsiteid"]
        ]

        ASNheaderByID = [
            item["data"]["getAsnHeaderById"][0]
            for item in result_map
            if isinstance(item.get("data"), dict)
            and "getAsnHeaderById" in item["data"]
            and isinstance(item["data"]["getAsnHeaderById"], list)
            and item["data"]["getAsnHeaderById"]
        ]

        WorkOrderByID = [
            item["data"]["getWorkOrderById"][0]
            for item in result_map
            if isinstance(item.get("data"), dict)
            and "getWorkOrderById" in item["data"]
            and isinstance(item["data"]["getWorkOrderById"], list)
            and item["data"]["getWorkOrderById"]
        ]
       
        ASNDetailById = [
            item["data"]["getAsnDetailById"]
            for item in result_map
            if isinstance(item.get("data"), dict)
            and "getAsnDetailById" in item["data"]
            and isinstance(item["data"]["getAsnDetailById"], dict)
        ]
        
        def listify(x):
            if x is None:
                return []
            if isinstance(x, list):
                return x
            return [x]

        def pick_address_by_type(salesheader_entry: Dict, contact_type: str) -> Dict:
            
            addresses = salesheader_entry.get("address", [])
            addresses = listify(addresses)
            for addr in addresses:
                contacts = listify(addr.get("contact", []))
                for c in contacts:
                    if isinstance(c, dict) and c.get("contactType") == contact_type:
                        return addr
            return {}

        def get_install_instruction2_id(fulfillment_entry: Dict) -> str:
           
            fulfills = listify(fulfillment_entry.get("fulfillments", []))
            if not fulfills:
                return ""
            lines = listify(fulfills[0].get("salesOrderLines", []))
            for line in lines:
                instrs = listify(line.get("specialinstructions", []))
                for instr in instrs:
                    if instr.get("specialInstructionType") == "INSTALL_INSTR2":
                        return str(instr.get("specialInstructionId", ""))
            return ""

        N = min(len(salesorders), len(fulfillments_by_id), len(salesheaders_by_ids))
        flat_list = []
        for idx in range(N):
            SequenseSalesID = safe_get(salesorders[idx], ['salesOrder', 'salesOrderId'])
            Sequencerecord = next((item for item in SequenceValue if item["salesOrderId"] == SequenseSalesID), None)
            shipping_addr = pick_address_by_type(salesheaders_by_ids[idx], "SHIPPING")
            billing_addr = pick_address_by_type(salesheaders_by_ids[idx], "BILLING")

            ASN, Destination, Origin, Way_Bill_Number, ship_mode = "", "", "", "", ""
            ActualShipCode, OrderVolWt, PPID, SvcTag, TargetDeliveryDate, TotalBoxCount, TotalGrossWeight, TotalVolumetricWeight,as_shipped_ppid,make_man_dtls,make_man_ppids = "", "", "", "", "", "", "", "", "", "", ""
            DellBlanketPoNum, IsLastLeg, ShipFromMcid, WoOtmEnable, WoShipMode,wo_lines,has_software,MakeWoAckValue,McidValue,WO_ID,ismultipack  = "", "", "", "", "","","","","","",""
            
            if safe_get(salesorders[idx], ['asnNumbers', 0, 'sourceManifestId']) != "":
                for asheaderData in ASNheaderByID:
                    if (asheaderData.get('shipFromVendorId') == safe_get(salesorders[idx], ['asnNumbers', 0, 'shipFromVendorId'])
                        and (asheaderData.get('sourceManifestId') == safe_get(salesorders[idx], ['asnNumbers', 0, 'sourceManifestId']))):                        
                        ASN             = safe_get(asheaderData, ['sourceManifestId'])
                        Destination     = safe_get(asheaderData, ['shipToVendorSiteId'])
                        Origin          = safe_get(asheaderData, ['shipFromVendorSiteId'])
                        Way_Bill_Number = safe_get(asheaderData, ['airwayBillNum'])
                        ship_mode       = safe_get(asheaderData, ['shipMode'])

                for asdetailData in ASNDetailById:
                    if asdetailData.get('sourceManifestId') == safe_get(salesorders[idx], ['asnNumbers', 0, 'sourceManifestId']):
                        ActualShipCode        = safe_get(asdetailData,['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'shipviaCode'])
                        OrderVolWt            = safe_get(asdetailData,['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'boxVolWt'])
                        box_details           = safe_get(asdetailData, ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'woShipmentBoxDetails'])
                        PPID                  = safe_get(box_details[0], ['basePpid'])
                        SvcTag                = list(filter(
                                                        lambda tag: tag is not None,
                                                        map(
                                                            lambda detail: safe_get(detail, ['serviceTag']),
                                                            sum([
                                                                safe_get(box, ['woShipmentBoxDetails']) or []
                                                                for pallet in safe_get(asdetailData, ['manifestPallet']) or []
                                                                for shipment in safe_get(pallet, ['woShipment']) or []
                                                                for box in safe_get(shipment, ['woShipmentBox']) or []
                                                            ], [])
                                                        )
                                                    ))
                        TargetDeliveryDate    = safe_get(asdetailData,['manifestPallet', 0, 'woShipment', 0, 'estDeliveryDate'])
                        
                        shipment_boxes        = sum([safe_get(shipment, ['woShipmentBox']) or []
                                                    for pallet in safe_get(asdetailData, ['manifestPallet']) or []
                                                    for shipment in safe_get(pallet, ['woShipment']) or []
                                                ], [])

                        TotalBoxCount         = len(list(filter(lambda box: safe_get(box, ['boxRef']) is not None, shipment_boxes)))
                        TotalGrossWeight      = sum(filter(lambda wt: wt is not None, map(lambda box: safe_get(box, ['boxGrossWt'], default=0), shipment_boxes)))
                        TotalVolumetricWeight = sum(filter(lambda wt: wt is not None, map(lambda box: safe_get(box, ['boxVolWt'], default=0), shipment_boxes)))
                        as_shipped_ppid       = safe_get(box_details[0], ['asShippedPpid'])
                        make_man_dtls         = safe_get(box_details[0], ['woMakeManDtl'])
                        make_man_ppids        = list(filter(lambda ppid: ppid is not None,map(lambda detail: safe_get(detail, ['asShippedPpid']), make_man_dtls)))
            
            
            if len(safe_get(salesorders[idx], ['workOrders', 0, 'woId'])) > 0:
                if isinstance(Sequencerecord, dict):
                    if "WorkOrderID" in Sequencerecord:
                        for WorkOrderData in WorkOrderByID:
                            if WorkOrderData.get('woId') == Sequencerecord['WorkOrderID']:
                                WO_ID = safe_get(WorkOrderData, ['woId'])
                                DellBlanketPoNum = safe_get(WorkOrderData, ['dellBlanketPoNum'])
                                IsLastLeg = safe_get(WorkOrderData, ['shipToFacility'])
                                ShipFromMcid = safe_get(WorkOrderData, ['vendorSiteId'])
                                WoOtmEnable = safe_get(WorkOrderData, ['isOtmEnabled'])
                                WoShipMode = safe_get(WorkOrderData, ['shipMode'])
                                ismultipack = safe_get(WorkOrderData, ['woLines',0,"ismultipack"])
                                wo_lines = safe_get(WorkOrderData, ['woLines'])
                                has_software = any(safe_get(line, ['woLineType']) == 'SOFTWARE' for line in wo_lines)
                                MakeWoAckValue = next((dateFormation(status.get("statusDate")) for status in WorkOrderData.get("woStatusList", [])
                                                        if str(status.get("channelStatusCode")) == "3000" and WorkOrderData.get("woType") == "MAKE"),
                                                        "")
                                McidValue = (
                                                WorkOrderData.get('woShipInstr', [{}])[0].get('mergeFacility') or
                                                WorkOrderData.get('woShipInstr', [{}])[0].get('carrierHubCode', "")
                                            )
                            
                                record_to_delete = next((item for item in SequenceValue if item["WorkOrderID"] == WO_ID), None)
                                record_to_delete and SequenceValue.remove(record_to_delete)

            ship_first = shipping_addr.get("firstName", "") if shipping_addr else ""
            ship_last = shipping_addr.get("lastName", "") if shipping_addr else ""
            shipping_contact_name = (f"{ship_first} {ship_last}").strip()
            SvcTag = ",".join(SvcTag)
            
            if Sequencerecord and "fulfillment" in Sequencerecord:
                ffid = Sequencerecord['fulfillment'] if Sequencerecord.get('fulfillment') else ""
            if Sequencerecord and "FOID" in Sequencerecord:
                foid = Sequencerecord['FOID'] if Sequencerecord.get('FOID') else ""

            row = {
                "Fulfillment ID": ffid,
                "BUID": safe_get(salesorders[idx], ['salesOrder', 'buid']),
                "BillingCustomerName": billing_addr.get("companyName", "") if billing_addr else "",
                "CustomerName": shipping_addr.get("companyName", "") if shipping_addr else "",
                "InstallInstruction2": get_install_instruction2_id(fulfillments_by_id[idx]),
                "ShippingCityCode": shipping_addr.get("cityCode", "") if shipping_addr else "",
                "ShippingContactName": shipping_contact_name,
                "ShippingCustName": shipping_addr.get("companyName", "") if shipping_addr else "",
                "ShippingStateCode": shipping_addr.get("stateCode", "") if shipping_addr else "",
                "ShipToAddress1": shipping_addr.get("addressLine1", "") if shipping_addr else "",
                "ShipToAddress2": shipping_addr.get("addressLine2", "") if shipping_addr else "",
                "ShipToCompany": shipping_addr.get("companyName", "") if shipping_addr else "",
                "ShipToPhone": (listify(shipping_addr.get("phone", []))[0].get("phoneNumber", "")
                                if shipping_addr and listify(shipping_addr.get("phone", [])) else ""),
                "ShipToPostal": shipping_addr.get("postalCode", "") if shipping_addr else "",
                "PP Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'statusDate'])) if safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']) == "PP" else "",
                "IP Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'statusDate'])) if safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']) == "IP" else "",
                "MN Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'statusDate'])) if safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']) == "MN" else "",
                "SC Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'statusDate'])) if safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']) == "SC" else "",
                "CFI Flag": safe_get(VendormasterByVendor[idx], ['isCfi']) if VendormasterByVendor and 0 <= idx < len(VendormasterByVendor) else "N",
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
                "Source System Status":  safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']),
                "Tie Number": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'soLineNum']),
                "Si Number": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'siNumber']),
                "Req Ship Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'shipCode']),
                "Reassigned Ip Date": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']),
                "Product Lob": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'lob']),
                "Payment Term Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'paymentTerm']),
                "Ofs Status Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']),
                "Ofs Status": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'fulfillmentStsCode']),
                "Fulfillment Status": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'fulfillmentStsCode']),
                "DomsStatus": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'sostatus', 0, 'sourceSystemStsCode']),
                "Sales Order ID": safe_get(salesorders[idx], ['salesOrder', 'salesOrderId']),                        
                "Region Code": safe_get(salesorders[idx], ['salesOrder', 'region']),
                "FO ID": foid,
                "WO ID": WO_ID,
                "System Qty": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'systemQty']),
                "Ship By Date": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'shipByDate']),
                "LOB": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'lob']),
                "Facility": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'salesOrderLines', 0, 'facility']),
                "SN Number": safe_get(salesorders[idx], ['asnNumbers', 0, 'snNumber']),
                "Tax Regstrn Num": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'taxRegstrnNum']),
                "State Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'stateCode']),
                "City Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'cityCode']),
                "Customer Num": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'customerNum']),
                "Customer Name Ext": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'customerNameExt']),
                "Country": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'address', 0, 'country']),
                "Create Date": dateFormation(dateFormation(safe_get(salesheaders_by_ids[idx], ['createDate']))),
                "Ship Code": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'shipCode']),
                "Must Arrive By Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'mustArriveByDate'])),
                "Update Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'updateDate'])),
                "Merge Type": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'mergeType']),
                "Manifest Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'manifestDate'])),
                "Revised Delivery Date": dateFormation(safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'revisedDeliveryDate'])),
                "Delivery City": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'deliveryCity']),
                "Source System ID": safe_get(fulfillments_by_id[idx], ['sourceSystemId']),
                "OIC ID": safe_get(fulfillments_by_id[idx], ['fulfillments', 0, 'oicId']),
                "Order Date": dateFormation(safe_get(salesheaders_by_ids[idx], ['orderDate'])),
                "Actual Ship Mode":ship_mode,
                "First Leg Ship Mode":ship_mode,
                "ASN":ASN,
                "Destination":Destination,
                "Manifest ID":ASN,
                "Origin":Origin,
                "Way Bill Number":Way_Bill_Number,                   
                "Build Facility":"",
                "Dell Blanket Po Num": DellBlanketPoNum,
                "Has Software":has_software,
                "Is Last Leg": IsLastLeg,
                "Make WoAck": MakeWoAckValue,                
                "Mcid": McidValue,
                "Ship From Mcid": ShipFromMcid,
                "Ship To Mcid": IsLastLeg,
                "Wo Otm Enable": WoOtmEnable,
                "Wo Ship Mode":WoShipMode,
                "Actual Ship Code": ActualShipCode,
                "Order Vol Wt": OrderVolWt,
                "PP ID": PPID,
                "Svc Tag": SvcTag,
                "Target Delivery Date": TargetDeliveryDate,
                "Total Box Count": TotalBoxCount,
                "Total Gross Weight": TotalGrossWeight,
                "Total Volumetric Weight": TotalVolumetricWeight,
                "Order Type": safe_get(salesheaders_by_ids[idx], ['orderType']),
                "Is Multi Pack":ismultipack,
            }
            
            flat_list.append(row)
        SequenceValue.clear()
        if not flat_list:
            return {"Error Message": "No Data Found"}

        if len(flat_list) > 0:
            if format_type == "export":
                return flat_list

            elif format_type == "grid":
                desired_order = [
                    "Fulfillment ID","BUID",
                    "BillingCustomerName","CustomerName","InstallInstruction2","ShippingCityCode",
                    "ShippingContactName","ShippingCustName","ShippingStateCode",
                    "ShipToAddress1","ShipToAddress2","ShipToCompany","ShipToPhone","ShipToPostal",
                    "PP Date","IP Date","MN Date","SC Date","CFI Flag","Agreement ID","Amount","Currency Code",
                    "Customer Po Number","Dp ID","Location Number","Order Age","Order Amount usd","Order Update Date",
                    "Rate Usd Transactional","Sales Rep Name","Shipping Country","Source System Status","Tie Number",
                    "Si Number","Req Ship Code","Reassigned Ip Date","Product Lob","Payment Term Code","Ofs Status Code",
                    "Ofs Status","Fulfillment Status","DomsStatus",
                    "Sales Order ID","Region Code","FO ID","WO ID","System Qty","Ship By Date","LOB",
                    "Facility","SN Number","Tax Regstrn Num","State Code","City Code",
                    "Customer Num","Customer Name Ext","Country","Create Date","Ship Code","Must Arrive By Date","Update Date",
                    "Merge Type","Manifest Date","Revised Delivery Date","Delivery City","Source System ID","OIC ID","Order Date",
                    "Actual Ship Mode","First Leg Ship Mode","ASN","Destination","Manifest ID","Origin","Way Bill Number",
                    "Build Facility","Dell Blanket Po Num","Has Software","Is Last Leg","Make Wo Ack","Mcid","Ship From Mcid",
                    "Ship To Mcid","Wo Otm Enable","Wo Ship Mode",
                    "Actual Ship Code","Order Vol Wt","PP ID","Svc Tag","Target Delivery Date","Total Box Count","Total Gross Weight",
                    "Total Volumetric Weight","Order Type","Is Multi Pack"
                ]
                
                rows = []
                for item in flat_list:
                    reordered_values = [item.get(key, "") for key in desired_order]
                    row = {"columns": [{"value": val if val is not None else ""} for val in reordered_values]}
                    rows.append(row)
                table_grid_output = tablestructural(rows, region) if rows else []
                
                return table_grid_output

        return {"error": "Format type must be either 'grid' or 'export'"}

    except Exception as e:
        traceback.print_exc()
        return {"error": str(e)}

def safe_get(data, path, default=""):
    try:
        for key in path:
            if data is None:
                return default
            if isinstance(key, int):
                if isinstance(data, list) and 0 <= key < len(data):
                    data = data[key]
                else:
                    return default
            elif isinstance(data, dict):
                data = data.get(key)
            else:
                return default
        return data if data is not None else default
    except (IndexError, KeyError, TypeError) as e:
        print(f"safe_get error: {e}")
        return default


def dateFormation(unformatedDate):
    if unformatedDate not in [None, "", "null"]:
        return unformatedDate.split('.')[0]
    else:
        return ""

def chunk_list(data_list, chunk_size):
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
        elif region in ["DAO", "AMER", "LA"]:
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

