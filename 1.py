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

nest_asyncio.apply()

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from graphqlQueries_new import *

configABSpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
with open(configABSpath, 'r') as file:
    configPath = json.load(file)

SequenceValue = []
ValidCount  = []
ASNHeaderData = []
ASNDetailsData = []
def newmainfunction(filters, format_type, region, filtersKey):
    regionFrom = region.upper()
    path = getPath(region)

    graphql_request = []
    finalResult = []
    FulfillID = []
    
    if "Fullfillment Id" in filters:
        Fullfillment_Id_key = "Fullfillment Id"
        uniqueFullfillment_ids = ",".join(sorted(set(filters[Fullfillment_Id_key].split(','))))
        filters[Fullfillment_Id_key] = uniqueFullfillment_ids

        if filters.get(Fullfillment_Id_key):
            Fullfillment_ids = list(map(str.strip, filters[Fullfillment_Id_key].split(",")))
            for ffid_chunk in chunk_list(Fullfillment_ids, 10):
                payload = {"query": fetch_keysphereFullfillment_query(ffid_chunk)}
                response = requests.post(path['FID'], json=payload, verify=False)
                data = response.json()

                if "errors" in data:
                    continue

                result = data.get("data", {}).get("getByFulfillmentids", {})
               
                for entry in result.get("result", []):
                    SequenceValue.append(entry)
                    salesid     = entry.get("salesOrder", {}).get("salesOrderId")
                    woiid       = entry.get("workOrders", [])
                    ffiid       = [entry.get("fulfillment", {})]
                    f0id        = entry.get("fulfillmentOrders", [])
                    ASN         = entry.get("asnNumbers", [])
                    region      = entry.get('salesOrder').get('region')
                    
                    if isinstance(result, dict):
                        if regionFrom == region:
                            fullffid = ffiid[0].get("fulfillmentId")

                            if filtersKey == "Fullfillment Id":
                                ValidCount.append(fullffid)

                            if len(woiid) > 0:                               
                                work_order_ids = list(map(lambda w: w['woId'], woiid)) if woiid else []

                                existing_values = set(filters.get("wo_id", "").split(",")) if filters.get("wo_id") else set()
                                new_values = set(work_order_ids)
                                combined_values = existing_values.union(new_values)
                                filters["wo_id"] = ",".join(sorted(combined_values))

                                Full_fillment_ids = filters["Fullfillment Id"].split(",")
                                
                                if fullffid in Full_fillment_ids:
                                    Full_fillment_ids.remove(fullffid)
                                if len(Full_fillment_ids) > 0:
                                    filters["Fullfillment Id"] = ",".join(sorted(Full_fillment_ids))
                                else:
                                    filters.pop("Fullfillment Id", None)

                            if len(ASN) > 0 and not woiid:
                                for ASNResult in ASN:
                                    sourceManifestID = ASNResult.get('sourceManifestId')
                                    shipFromVendorID = ASNResult.get('shipFromVendorId')

                                    if not sourceManifestID or not shipFromVendorID:
                                        continue

                                    payload = {"query": fetch_AsnOrderByID_query(shipFromVendorID, sourceManifestID)}
                                    response = requests.post(path['ASNODM'], json=payload, verify=False)
                                    ASNheaderdata = response.json()

                                    existing_combinations = set()
                                    existing_combinations.update(
                                        (entry.get("shipFromVendorId"), entry.get("sourceManifestId")) 
                                        for entry in ASNHeaderData
                                        if entry.get("shipFromVendorId") and entry.get("sourceManifestId")
                                    )
                                    existing_combinations.update(
                                        (entry.get("shipFromVendorId"), entry.get("sourceManifestId")) 
                                        for entry in ASNDetailsData
                                        if entry.get("shipFromVendorId") and entry.get("sourceManifestId")
                                    )

                                    for ASNentry in ASNheaderdata.get('data', {}).get('getAsnHeaderById', []):
                                        current_combo = (ASNentry['shipFromVendorId'], ASNentry['sourceManifestId'])
                                        if current_combo not in existing_combinations:
                                            ASNHeader = {
                                                'FullfillmentID': fullffid,
                                                'WorkOrderID': "",
                                                'airwayBillNum': ASNentry['airwayBillNum'],
                                                'shipFromVendorSiteId': ASNentry['shipFromVendorSiteId'],
                                                'shipFromVendorId': ASNentry['shipFromVendorId'],
                                                'sourceManifestId': ASNentry['sourceManifestId'],
                                                'shipMode': ASNentry['shipMode'],
                                                'shipToVendorSiteId': ASNentry['shipToVendorSiteId']
                                            }
                                            ASNHeaderData.append(ASNHeader)
                                            existing_combinations.add(current_combo)

                                    payload = {"query": fetch_AsnDetailById_query(region, shipFromVendorID, sourceManifestID)}
                                    response = requests.post(path['ASNODM'], json=payload, verify=False)
                                    ASNheaderdetailsdata = response.json()

                                    asn_detail = ASNheaderdetailsdata.get('data', {})

                                    if asn_detail is not None:
                                        ASNDetailresult = [ASNheaderdetailsdata.get('data', {}).get('getAsnDetailById', {})]

                                        for ASNDetailentry in ASNDetailresult:
                                            current_combo = (ASNDetailentry.get('shipFromVendorId'), ASNDetailentry.get('sourceManifestId'))
                                            if current_combo not in existing_combinations:
                                                ASNDetailentry["FullfillmentID"] = fullffid
                                                ASNDetailentry["WorkOrderID"] = ""
                                                ASNDetailsData.append(ASNDetailentry)
                                                existing_combinations.add(current_combo)

    if "Sales_Order_id" in filters:
        salesOrder_key = "Sales_Order_id"
        uniqueSalesOrder_ids = ",".join(sorted(set(filters[salesOrder_key].split(','))))
        filters[salesOrder_key] = uniqueSalesOrder_ids

        if filters.get(salesOrder_key):
            salesorder_ids = list(map(str.strip, filters[salesOrder_key].split(",")))

            for soid_chunk in chunk_list(salesorder_ids,10):
                payload = {"query": fetch_keysphereSalesorder_query(soid_chunk)}
                response = requests.post(path['FID'], json=payload, verify=False)
                data = response.json()
               
                if "errors" in data:
                    continue

                result = data.get("data", {}).get("getBySalesorderids", {})
               
                for entry in result.get("result", []):
                    salesid     = entry.get("salesOrder", {}).get("salesOrderId")
                    woiid       = entry.get("workOrders", [])
                    ffiid       = entry.get("fulfillment", [])
                    f0id        = entry.get("fulfillmentOrders", [])
                    ASN         = entry.get("asnNumbers", [])
                    region      = entry.get('salesOrder').get('region')
                    
                    if isinstance(result, dict):
                        if regionFrom == region:
                            if filtersKey == "Sales_Order_id":
                                ValidCount.append(salesid)
                            fullffid = ffiid[0].get("fulfillmentId")
                            if len(woiid) > 0:
                            
                                work_order_ids = list(map(lambda w: w['woId'], woiid)) if woiid else []

                                existing_values = set(filters.get("wo_id", "").split(",")) if filters.get("wo_id") else set()
                                new_values = set(work_order_ids)
                                combined_values = existing_values.union(new_values)
                                filters["wo_id"] = ",".join(sorted(combined_values))

                            if len(ffiid) > 0:
                                ffi_ids = list(map(lambda f: f['fulfillmentId'], ffiid)) if ffiid else []
                                
                                existing_ffi = set(filters.get("Fullfillment Id", "").split(",")) if filters.get("Fullfillment Id") else set()
                                new_ffi = set(ffi_ids)
                                combined_ffi = existing_ffi.union(new_ffi)
                                if not woiid:
                                    filters["Fullfillment Id"] = ",".join(sorted(combined_ffi))
                            
                            if len(ASN) > 0 and not woiid:
                                for ASNResult in ASN:
                                    sourceManifestID = ASNResult.get('sourceManifestId')
                                    shipFromVendorID = ASNResult.get('shipFromVendorId')

                                    if not sourceManifestID or not shipFromVendorID:
                                        continue

                                    payload = {"query": fetch_AsnOrderByID_query(shipFromVendorID, sourceManifestID)}
                                    response = requests.post(path['ASNODM'], json=payload, verify=False)
                                    ASNheaderdata = response.json()

                                    existing_combinations = set()
                                    existing_combinations.update(
                                        (entry.get("shipFromVendorId"), entry.get("sourceManifestId")) 
                                        for entry in ASNHeaderData
                                        if entry.get("shipFromVendorId") and entry.get("sourceManifestId")
                                    )
                                    existing_combinations.update(
                                        (entry.get("shipFromVendorId"), entry.get("sourceManifestId")) 
                                        for entry in ASNDetailsData
                                        if entry.get("shipFromVendorId") and entry.get("sourceManifestId")
                                    )

                                    for ASNentry in ASNheaderdata.get('data', {}).get('getAsnHeaderById', []):
                                        current_combo = (ASNentry['shipFromVendorId'], ASNentry['sourceManifestId'])
                                        if current_combo not in existing_combinations:
                                            ASNHeader = {
                                                'FullfillmentID': fullffid,
                                                'WorkOrderID': "",
                                                'airwayBillNum': ASNentry['airwayBillNum'],
                                                'shipFromVendorSiteId': ASNentry['shipFromVendorSiteId'],
                                                'shipFromVendorId': ASNentry['shipFromVendorId'],
                                                'sourceManifestId': ASNentry['sourceManifestId'],
                                                'shipMode': ASNentry['shipMode'],
                                                'shipToVendorSiteId': ASNentry['shipToVendorSiteId']
                                            }
                                            ASNHeaderData.append(ASNHeader)
                                            existing_combinations.add(current_combo)

                                    payload = {"query": fetch_AsnDetailById_query(region, shipFromVendorID, sourceManifestID)}
                                    response = requests.post(path['ASNODM'], json=payload, verify=False)
                                    ASNheaderdetailsdata = response.json()

                                    asn_detail = ASNheaderdetailsdata.get('data', {})

                                    if asn_detail is not None:
                                        ASNDetailresult = [ASNheaderdetailsdata.get('data', {}).get('getAsnDetailById', {})]

                                        for ASNDetailentry in ASNDetailresult:
                                            current_combo = (ASNDetailentry.get('shipFromVendorId'), ASNDetailentry.get('sourceManifestId'))
                                            if current_combo not in existing_combinations:
                                                ASNDetailentry["FullfillmentID"] = fullffid
                                                ASNDetailentry["WorkOrderID"] = ""
                                                ASNDetailsData.append(ASNDetailentry)
                                                existing_combinations.add(current_combo)

                            filters.pop("Sales_Order_id", None)
    
    if "wo_id" in filters:
        workOrder_key = "wo_id"
        uniqueWorkOrder_ids = ",".join(sorted(set(filters[workOrder_key].split(','))))
        filters[workOrder_key] = uniqueWorkOrder_ids

        if filters.get(workOrder_key):
            workorder_ids = list(map(str.strip, filters[workOrder_key].split(",")))
            for woid_chunk in chunk_list(workorder_ids,10):
                payload = {"query": fetch_keysphereWorkorder_query(woid_chunk)}
                response = requests.post(path['FID'], json=payload, verify=False)
                data = response.json()

                if "errors" in data:
                    continue

                result = data.get("data", {}).get("getByWorkorderids", {})
                for entry in result.get("result", []):
                    SequenceValue.append(entry)
                    salesid     = entry.get("salesOrder", {}).get("salesOrderId")
                    woiid       = [entry.get("workOrder", {})]
                    ffiid       = [entry.get("fulfillment", {})]
                    f0id        = entry.get("fulfillmentOrders", [])
                    ASN         = entry.get("asnNumbers", [])
                    region      = entry.get('salesOrder').get('region')

                    if isinstance(result, dict):
                        if regionFrom == region:
                            wooiid = woiid[0].get("woId")
                            ffmmiid = ffiid[0].get("fulfillmentId")
                            if filtersKey == "wo_id":
                                ValidCount.append(wooiid)
                            if len(woiid) > 0:
                                work_order_ids = list(map(lambda w: w['woId'], woiid)) if woiid else []

                                existing_values = set(filters.get("wo_id", "").split(",")) if filters.get("wo_id") else set()
                                new_values = set(work_order_ids)
                                combined_values = existing_values.union(new_values)
                                filters["wo_id"] = ",".join(sorted(combined_values))

                                graphql_request.append({
                                        "url": path['WORKORDER'],
                                        "query": fetch_workOrder_query(work_order_ids)
                                    })                            

                            if len(ffiid) > 0:
                                ffi_ids = list(map(lambda f: f['fulfillmentId'], ffiid)) if ffiid else []
                                
                                existing_ffi = set(filters.get("ffi_id", "").split(",")) if filters.get("ffi_id") else set()
                                new_ffi = set(ffi_ids)
                                combined_ffi = existing_ffi.union(new_ffi)
                                if not woiid:
                                    filters["Fullfillment Id"] = ",".join(sorted(combined_ffi))

                                graphql_request.append({
                                        "url": path['SALESFULLFILLMENT'],
                                        "query": fetch_Fullfillment_query(ffi_ids)
                                    })
                            
                            if len(ASN) > 0:
                                for ASNResult in ASN:
                                    sourceManifestID = ASNResult.get('sourceManifestId')
                                    shipFromVendorID = ASNResult.get('shipFromVendorId')

                                    if not sourceManifestID or not shipFromVendorID:
                                        continue

                                    payload = {"query": fetch_AsnOrderByID_query(shipFromVendorID, sourceManifestID)}
                                    response = requests.post(path['ASNODM'], json=payload, verify=False)
                                    ASNheaderdata = response.json()

                                    existing_combinations = set()
                                    existing_combinations.update(
                                        (entry.get("shipFromVendorId"), entry.get("sourceManifestId")) 
                                        for entry in ASNHeaderData
                                        if entry.get("shipFromVendorId") and entry.get("sourceManifestId")
                                    )
                                    existing_combinations.update(
                                        (entry.get("shipFromVendorId"), entry.get("sourceManifestId")) 
                                        for entry in ASNDetailsData
                                        if entry.get("shipFromVendorId") and entry.get("sourceManifestId")
                                    )

                                    for ASNentry in ASNheaderdata.get('data', {}).get('getAsnHeaderById', []):
                                        current_combo = (ASNentry['shipFromVendorId'], ASNentry['sourceManifestId'])
                                        if current_combo not in existing_combinations:
                                            ASNHeader = {
                                                'FullfillmentID': ffmmiid,
                                                'WorkOrderID': wooiid,
                                                'airwayBillNum': ASNentry['airwayBillNum'],
                                                'shipFromVendorSiteId': ASNentry['shipFromVendorSiteId'],
                                                'shipFromVendorId': ASNentry['shipFromVendorId'],
                                                'sourceManifestId': ASNentry['sourceManifestId'],
                                                'shipMode': ASNentry['shipMode'],
                                                'shipToVendorSiteId': ASNentry['shipToVendorSiteId']
                                            }
                                            ASNHeaderData.append(ASNHeader)
                                            existing_combinations.add(current_combo)

                                    payload = {"query": fetch_AsnDetailById_query(region, shipFromVendorID, sourceManifestID)}
                                    response = requests.post(path['ASNODM'], json=payload, verify=False)
                                    ASNheaderdetailsdata = response.json()

                                    asn_detail = ASNheaderdetailsdata.get('data', {})

                                    if asn_detail is not None:
                                        ASNDetailresult = [ASNheaderdetailsdata.get('data', {}).get('getAsnDetailById', {})]

                                        for ASNDetailentry in ASNDetailresult:
                                            current_combo = (ASNDetailentry.get('shipFromVendorId'), ASNDetailentry.get('sourceManifestId'))
                                            if current_combo not in existing_combinations:
                                                ASNDetailentry["FullfillmentID"] = ffmmiid
                                                ASNDetailentry["WorkOrderID"] = wooiid
                                                ASNDetailsData.append(ASNDetailentry)
                                                existing_combinations.add(current_combo)
                
    if "Fullfillment Id" in filters:
        Fullfillment_Id_key = "Fullfillment Id"
        uniqueFullfillment_ids = ",".join(sorted(set(filters[Fullfillment_Id_key].split(','))))
        filters[Fullfillment_Id_key] = uniqueFullfillment_ids

        if filters.get(Fullfillment_Id_key):
            Fullfillment_ids = list(map(str.strip, filters[Fullfillment_Id_key].split(",")))
            for ffid_chunk in chunk_list(Fullfillment_ids,10):
                payload = {"query": fetch_keysphereFullfillment_query(ffid_chunk)}
                response = requests.post(path['FID'], json=payload, verify=False)
                data = response.json()

                if "errors" in data:
                    continue

                result = data.get("data", {}).get("getByFulfillmentids", {})

                for entry in result.get("result", []):
                    SequenceValue.append(entry)
                    salesid     = entry.get("salesOrder", {}).get("salesOrderId")
                    woiid       = entry.get("workOrder", [])
                    ffiid       = [entry.get("fulfillment", {})]
                    f0id        = entry.get("fulfillmentOrders", [])
                    ASN         = entry.get("asnNumbers", [])
                    region      = entry.get('salesOrder').get('region')
               
                    if isinstance(result, dict):
                        if regionFrom == region:
                            fullffid = ffiid[0].get("fulfillmentId")

                            if len(ffiid) > 0:
                                ffi_ids = list(map(lambda f: f['fulfillmentId'], ffiid)) if ffiid else []
                                
                                existing_ffi = set(filters.get("Fullfillment Id", "").split(",")) if filters.get("Fullfillment Id") else set()
                                new_ffi = set(ffi_ids)
                                combined_ffi = existing_ffi.union(new_ffi)

                                if not woiid:
                                    filters["Fullfillment Id"] = ",".join(sorted(combined_ffi))

                                graphql_request.append({
                                        "url": path['SALESFULLFILLMENT'],
                                        "query": fetch_Fullfillment_query(ffi_ids)
                                    })

                            if len(ASN) > 0 and not woiid:
                                for ASNResult in ASN:
                                    sourceManifestID = ASNResult.get('sourceManifestId')
                                    shipFromVendorID = ASNResult.get('shipFromVendorId')

                                    if not sourceManifestID or not shipFromVendorID:
                                        continue

                                    payload = {"query": fetch_AsnOrderByID_query(shipFromVendorID, sourceManifestID)}
                                    response = requests.post(path['ASNODM'], json=payload, verify=False)
                                    ASNheaderdata = response.json()

                                    existing_combinations = set()
                                    existing_combinations.update(
                                        (entry.get("shipFromVendorId"), entry.get("sourceManifestId")) 
                                        for entry in ASNHeaderData
                                        if entry.get("shipFromVendorId") and entry.get("sourceManifestId")
                                    )
                                    existing_combinations.update(
                                        (entry.get("shipFromVendorId"), entry.get("sourceManifestId")) 
                                        for entry in ASNDetailsData
                                        if entry.get("shipFromVendorId") and entry.get("sourceManifestId")
                                    )

                                    for ASNentry in ASNheaderdata.get('data', {}).get('getAsnHeaderById', []):
                                        current_combo = (ASNentry['shipFromVendorId'], ASNentry['sourceManifestId'])
                                        if current_combo not in existing_combinations:
                                            ASNHeader = {
                                                'FullfillmentID': fullffid,
                                                'WorkOrderID': "",
                                                'airwayBillNum': ASNentry['airwayBillNum'],
                                                'shipFromVendorSiteId': ASNentry['shipFromVendorSiteId'],
                                                'shipFromVendorId': ASNentry['shipFromVendorId'],
                                                'sourceManifestId': ASNentry['sourceManifestId'],
                                                'shipMode': ASNentry['shipMode'],
                                                'shipToVendorSiteId': ASNentry['shipToVendorSiteId']
                                            }
                                            ASNHeaderData.append(ASNHeader)
                                            existing_combinations.add(current_combo)

                                    payload = {"query": fetch_AsnDetailById_query(region, shipFromVendorID, sourceManifestID)}
                                    response = requests.post(path['ASNODM'], json=payload, verify=False)
                                    ASNheaderdetailsdata = response.json()

                                    asn_detail = ASNheaderdetailsdata.get('data', {})

                                    if asn_detail is not None:
                                        ASNDetailresult = [ASNheaderdetailsdata.get('data', {}).get('getAsnDetailById', {})]

                                        for ASNDetailentry in ASNDetailresult:
                                            current_combo = (ASNDetailentry.get('shipFromVendorId'), ASNDetailentry.get('sourceManifestId'))
                                            if current_combo not in existing_combinations:
                                                ASNDetailentry["FullfillmentID"] = fullffid
                                                ASNDetailentry["WorkOrderID"] = ""
                                                ASNDetailsData.append(ASNDetailentry)
                                                existing_combinations.add(current_combo)

                            filters.pop("Fullfillment Id", None)
                    
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
        return results

def newOutputFormat(result_map, format_type=None, region=None, filtersValue=None):
    try:
        flat_list = []

        FlatData = [] 
        wo_data_list = []
        final_merged_data = []
        for result in result_map:
            if result.get('data', {}) is None:
                continue
            workOrderData = result.get('data',{}).get('getWorkOrderByWoIds',[])
            ffIdData = result.get('data',{}).get('getSalesOrderByFfids',{}).get('salesOrders',[])
            
            if workOrderData:
                workorders_Data = workOrderData[0]
                wo_row = {
                    "WO_ID": safe_get(workorders_Data, ['woId']),
                    "Dell Blanket PO Num": safe_get(workorders_Data, ['dellBlanketPoNum']),
                    "Ship To Facility": safe_get(workorders_Data, ['shipToFacility']),
                    "Is Last Leg": 'Y' if safe_get(workorders_Data, ['shipToFacility']) else 'N',
                    "Ship From MCID": safe_get(workorders_Data, ['vendorSiteId']),
                    "WO OTM Enabled": safe_get(workorders_Data, ['isOtmEnabled']),
                    "WO Ship Mode": safe_get(workorders_Data, ['shipMode']),
                    "Is Multipack": safe_get(workorders_Data, ['woLines', 0, 'ismultipack']),
                    "Has Software": any(safe_get(line, ['woLineType']) == 'SOFTWARE' for line in safe_get(workorders_Data, ['woLines']) or []),
                    "Make WO Ack Date": next(
                        (dateFormation(status.get("statusDate"))
                            for status in workorders_Data.get("woStatusList", [])
                            if str(status.get("channelStatusCode")) == "3000" and workorders_Data.get("woType") == "MAKE"),
                        ""
                    ),
                    "MCID Value": (
                        safe_get(workorders_Data, ['woShipInstr', 0, "mergeFacility"]) or
                        safe_get(workorders_Data, ['woShipInstr', 0, "carrierHubCode"])
                    ),
                    "Merge Facility": safe_get(workorders_Data, ['woShipInstr', 0, "mergeFacility"])
                }
                wo_data_list.append(wo_row)
            else:
                so = ffIdData[0]

                fulfillments = safe_get(so, ['fulfillments']) or []

                workorders_Data = wo_data_list[0] if wo_data_list else []

                if isinstance(fulfillments, dict):
                    fulfillments = [fulfillments]
                fulfillment_id = safe_get(fulfillments, [0, 'fulfillmentId'])
                WorkOrderIDD = safe_get(workorders_Data, ['WO_ID'])                

                matching_asnheader_records = [
                                                asn for asn in ASNHeaderData
                                                if (str(asn.get('WorkOrderID', '')).strip() != "" and str(asn.get('WorkOrderID', '')).strip() == str(WorkOrderIDD).strip())
                                                or (str(asn.get('WorkOrderID', '')).strip() == "" and str(asn.get('FullfillmentID', '')).strip() == str(fulfillment_id).strip())
                                            ]

                matching_asn_details = [
                                        asn for asn in ASNDetailsData
                                        if (str(asn.get('WorkOrderID', '')).strip() != "" and str(asn.get('WorkOrderID', '')).strip() == str(WorkOrderIDD).strip())
                                        or (str(asn.get('WorkOrderID', '')).strip() == "" and str(asn.get('FullfillmentID', '')).strip() == str(fulfillment_id).strip())
                                    ]

                
                shipping_addr = pick_address_by_type(so, "SHIPPING")
                billing_addr = pick_address_by_type(so, "BILLING")
                shipping_phone = pick_address_by_type(fulfillments[0], "SHIPPING") if fulfillments else None
                shipping_contact_name = shipping_addr.get("fullName", "") if shipping_addr else ""

                lob_list = list(filter(
                    lambda lob: lob and lob.strip() != "",
                    map(lambda line: safe_get(line, ['lob']), safe_get(fulfillments, [0,'salesOrderLines']) or [])
                ))
                lob = ", ".join(lob_list)
                facility_list = list(filter(
                    lambda f: f and f.strip() != "",
                    map(lambda line: safe_get(line, ['facility']), safe_get(fulfillments, [0,'salesOrderLines']) or [])
                ))
                facility = ", ".join(dict.fromkeys(f.strip() for f in facility_list if f))

                def get_status_date(code):
                    status_code = safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode'])
                    if status_code == code:
                        return dateFormation(safe_get(fulfillments, [0, 'soStatus', 0, 'statusDate']))
                    return ""
                ActualShipCode,OrderVolWt,PPID,SvcTag,TargetDeliveryDate,TotalBoxCount,TotalGrossWeight,TotalVolumetricWeight = "","","","","","","",""
                ASN,Destination,Origin,Way_Bill_Number,ship_mode = "","","","",""
                if len(matching_asnheader_records) > 0:
                    for idex, matching_asn_header in enumerate(matching_asnheader_records):
                        if matching_asn_header is not None :
                            
                            ASN             = safe_get(matching_asn_header, ['sourceManifestId'])
                            Destination     = safe_get(matching_asn_header, ['shipToVendorSiteId'])
                            Origin          = safe_get(matching_asn_header, ['shipFromVendorSiteId'])
                            Way_Bill_Number = safe_get(matching_asn_header, ['airwayBillNum'])
                            ship_mode       = safe_get(matching_asn_header, ['shipMode'])
                            
                              
                        if matching_asn_details[idex] is not None :
                            ActualShipCode = safe_get(matching_asn_details[idex], ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'shipviaCode'])
                            OrderVolWt     = safe_get(matching_asn_details[idex], ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'boxVolWt'])
                            box_details    = safe_get(matching_asn_details[idex], ['manifestPallet', 0, 'woShipment', 0, 'woShipmentBox', 0, 'woShipmentBoxDetails'])
                            base_ppid = safe_get(box_details[0], ['basePpid'])
                            as_shipped_ppid_box = safe_get(box_details[0], ['asShippedPpid'])

                            wo_make_man_dtl = safe_get(box_details[0], ['woMakeManDtl'])
                            as_shipped_ppid_make = None
                            if isinstance(wo_make_man_dtl, list):
                                for item in wo_make_man_dtl:
                                    as_shipped_ppid_make = safe_get(item, ['asShippedPpid'])
                                    if as_shipped_ppid_make:
                                        break

                            PPID = base_ppid or as_shipped_ppid_box or as_shipped_ppid_make or ""

                            SvcTag                = list(filter(
                                                            lambda tag: tag is not None,
                                                            map(
                                                                lambda detail: safe_get(detail, ['serviceTag']),
                                                                sum([
                                                                    safe_get(box, ['woShipmentBoxDetails']) or []
                                                                    for pallet in safe_get(matching_asn_details[idex], ['manifestPallet']) or []
                                                                    for shipment in safe_get(pallet, ['woShipment']) or []
                                                                    for box in safe_get(shipment, ['woShipmentBox']) or []
                                                                ], [])
                                                            )
                                                        ))

                            TargetDeliveryDate    = safe_get(matching_asn_details[idex],['manifestPallet', 0, 'woShipment', 0, 'estDeliveryDate'])
                            
                            shipment_boxes        = sum([safe_get(shipment, ['woShipmentBox']) or []
                                                        for pallet in safe_get(matching_asn_details[idex], ['manifestPallet']) or []
                                                        for shipment in safe_get(pallet, ['woShipment']) or []
                                                    ], [])

                            TotalBoxCount         = len(list(filter(lambda box: safe_get(box, ['boxRef']) is not None, shipment_boxes)))
                            TotalGrossWeight      = sum(filter(lambda wt: wt is not None, map(lambda box: safe_get(box, ['boxGrossWt'], default=0), shipment_boxes)))
                            TotalVolumetricWeight = sum(filter(lambda wt: wt is not None, map(lambda box: safe_get(box, ['boxVolWt'], default=0), shipment_boxes)))
                            as_shipped_ppid       = safe_get(box_details[0], ['asShippedPpid'])
                            make_man_dtls         = safe_get(box_details[0], ['woMakeManDtl'])
                            make_man_ppids        = list(filter(lambda ppid: ppid is not None,map(lambda detail: safe_get(detail, ['asShippedPpid']), make_man_dtls)))
                            SvcTag = ", ".join(s.strip() for s in SvcTag if s.strip()) if isinstance(SvcTag, list) else str(SvcTag).strip()
                        
                        row = {
                            "Fulfillment ID": fulfillment_id,
                            "BUID": safe_get(so, ['buid']),
                            "BillingCustomerName": billing_addr.get("companyName", "") if billing_addr else "",
                            "CustomerName": shipping_addr.get("companyName", "") if shipping_addr else "",
                            "LOB": lob,
                            "Sales Order ID": safe_get(so, ['salesOrderId']),
                            "Agreement ID": safe_get(so, ['agreementId']),
                            "Amount": safe_get(so, ['totalPrice']),
                            "Currency Code": safe_get(so, ['currency']),
                            "Customer Po Number": safe_get(so, ['poNumber']),
                            "Delivery City": safe_get(fulfillments, [0, 'deliveryCity']),
                            "DOMS Status": safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode']),
                            "Dp ID": safe_get(so, ['dpid']),
                            "Fulfillment Status": safe_get(fulfillments, [0, 'soStatus', 0, 'fulfillmentStsCode']),
                            "Merge Type": safe_get(fulfillments, [0, 'mergeType']),
                            "InstallInstruction2": get_install_instruction2_id(so),
                            "PP Date": get_status_date("PP"),
                            "IP Date": get_status_date("IP"),
                            "MN Date": get_status_date("MN"),
                            "SC Date": get_status_date("SC"),
                            "Location Number": safe_get(so, ['locationNum']),
                            "OFS Status Code": safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode']),
                            "OFS Status": safe_get(fulfillments, [0, 'soStatus', 0, 'fulfillmentStsCode']),
                            "ShippingCityCode": shipping_addr.get("cityCode", "") if shipping_addr else "",
                            "ShippingContactName": shipping_contact_name,
                            "ShippingCustName": shipping_addr.get("companyName", "") if shipping_addr else "",
                            "ShippingStateCode": shipping_addr.get("stateCode", "") if shipping_addr else "",
                            "ShipToAddress1": shipping_addr.get("addressLine1", "") if shipping_addr else "",
                            "ShipToAddress2": shipping_addr.get("addressLine2", "") if shipping_addr else "",
                            "ShipToCompany": shipping_addr.get("companyName", "") if shipping_addr else "",
                            "ShipToPhone": (listify(shipping_phone.get("phone", []))[0].get("phoneNumber", "")
                                            if shipping_phone and listify(shipping_phone.get("phone", [])) else ""),
                            "ShipToPostal": shipping_addr.get("postalCode", "") if shipping_addr else "",
                            "Order Age": safe_get(so, ['orderDate']),
                            "Order Amount usd": safe_get(so, ['rateUsdTransactional']),
                            "Rate Usd Transactional": safe_get(so, ['rateUsdTransactional']),
                            "Sales Rep Name": safe_get(so, ['salesrep', 0, 'salesRepName']),
                            "Shipping Country": shipping_addr.get("country", "") if shipping_addr else "",
                            "Source System Status": safe_get(fulfillments, [0, 'soStatus', 0,'sourceSystemStsCode']),
                            "Tie Number": safe_get(fulfillments, [0, 'salesOrderLines', 0, 'soLineNum']),
                            "Si Number": safe_get(fulfillments, [0, 'salesOrderLines', 0, 'siNumber']),
                            "Req Ship Code": safe_get(fulfillments, [0, 'shipCode']),
                            "Reassigned IP Date": safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode']),
                            "Payment Term Code": safe_get(fulfillments, [0, 'paymentTerm']),
                            "Region Code": safe_get(so, ['region']),
                            "FO ID": safe_get(fulfillments, [0, 'fulfillmentOrder', 0, 'foId']),
                            "System Qty": safe_get(fulfillments, [0, 'systemQty']),
                            "Ship By Date": safe_get(fulfillments, [0, 'shipByDate']),
                            "Facility": facility,
                            "Tax Regstrn Num": safe_get(fulfillments, [0, 'address', 0, 'taxRegstrnNum']),
                            "State Code": shipping_addr.get("stateCode", "") if shipping_addr else "",
                            # "City Code": shipping_addr.get("cityCode", "") if shipping_addr else "",
                            "Customer Num": shipping_addr.get("customerNum", "") if shipping_addr else "",
                            # "Customer Name Ext": shipping_addr.get("customerNameExt", "") if shipping_addr else "",
                            "Country": shipping_addr.get("country", "") if shipping_addr else "",
                            "Ship Code": safe_get(fulfillments, [0, 'shipCode']),
                            "Must Arrive By Date": dateFormation(safe_get(fulfillments, [0, 'mustArriveByDate'])),
                            "Manifest Date": dateFormation(safe_get(fulfillments, [0, 'manifestDate'])),
                            "Revised Delivery Date": dateFormation(safe_get(fulfillments, [0, 'revisedDeliveryDate'])),
                            "Source System ID": safe_get(so, ['sourceSystemId']),
                            "OIC ID": safe_get(fulfillments, [0, 'oicId']),
                            "Order Date": dateFormation(safe_get(so, ['orderDate'])),
                            "Order Type": dateFormation(safe_get(so, ['orderType'])),
                            "Work Order ID": safe_get(workorders_Data, ['WO_ID']),
                            "Dell Blanket PO Num": safe_get(workorders_Data, ['Dell Blanket PO Num']),
                            "Ship To Facility": safe_get(workorders_Data, ['Ship To Facility']),
                            "Is Last Leg": 'Y' if safe_get(workorders_Data, ['Is Last Leg']) else 'N',
                            "Ship From MCID": safe_get(workorders_Data, ['Ship From MCID']),
                            "Ship To MCID": 'Y' if safe_get(workorders_Data, ['Is Last Leg']) else 'N',
                            "WO OTM Enabled": safe_get(workorders_Data, ['WO OTM Enabled']),
                            "WO Ship Mode": safe_get(workorders_Data, ['WO Ship Mode']),
                            "Is Multipack": safe_get(workorders_Data, ['Is Multipack']),
                            "Has Software": safe_get(workorders_Data, ['Has Software']),
                            "Make WO Ack Date": safe_get(workorders_Data, ['Make WO Ack Date']),
                            "MCID Value": safe_get(workorders_Data, ['MCID Value']),
                            "Merge Facility": safe_get(workorders_Data, ['Merge Facility']),
                            "ASN":ASN,
                            "Destination":Destination,
                            "Manifest ID":ASN,
                            "Origin":Origin,
                            "Way Bill Number":Way_Bill_Number, 
                            "Actual Ship Mode":ship_mode,
                            "Actual Ship Code": ActualShipCode,
                            "Order Vol Wt": OrderVolWt,
                            "PP ID": PPID,
                            "SVC Tag": SvcTag,
                            "Target Delivery Date": TargetDeliveryDate,
                            "Total Box Count": TotalBoxCount,
                            "Total Gross Weight": TotalGrossWeight,
                            "Total Volumetric Weight": TotalVolumetricWeight
                        }

                        flat_list.append(row)
                    
                        wo_data_list.clear()
                else:                    
                    row = {
                        "Fulfillment ID": fulfillment_id,
                        "BUID": safe_get(so, ['buid']),
                        "BillingCustomerName": billing_addr.get("companyName", "") if billing_addr else "",
                        "CustomerName": shipping_addr.get("companyName", "") if shipping_addr else "",
                        "LOB": lob,
                        "Sales Order ID": safe_get(so, ['salesOrderId']),
                        "Agreement ID": safe_get(so, ['agreementId']),
                        "Amount": safe_get(so, ['totalPrice']),
                        "Currency Code": safe_get(so, ['currency']),
                        "Customer Po Number": safe_get(so, ['poNumber']),
                        "Delivery City": safe_get(fulfillments, [0, 'deliveryCity']),
                        "DOMS Status": safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode']),
                        "Dp ID": safe_get(so, ['dpid']),
                        "Fulfillment Status": safe_get(fulfillments, [0, 'soStatus', 0, 'fulfillmentStsCode']),
                        "Merge Type": safe_get(fulfillments, [0, 'mergeType']),
                        "InstallInstruction2": get_install_instruction2_id(so),
                        "PP Date": get_status_date("PP"),
                        "IP Date": get_status_date("IP"),
                        "MN Date": get_status_date("MN"),
                        "SC Date": get_status_date("SC"),
                        "Location Number": safe_get(so, ['locationNum']),
                        "OFS Status Code": safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode']),
                        "OFS Status": safe_get(fulfillments, [0, 'soStatus', 0, 'fulfillmentStsCode']),
                        "ShippingCityCode": shipping_addr.get("cityCode", "") if shipping_addr else "",
                        "ShippingContactName": shipping_contact_name,
                        "ShippingCustName": shipping_addr.get("companyName", "") if shipping_addr else "",
                        "ShippingStateCode": shipping_addr.get("stateCode", "") if shipping_addr else "",
                        "ShipToAddress1": shipping_addr.get("addressLine1", "") if shipping_addr else "",
                        "ShipToAddress2": shipping_addr.get("addressLine2", "") if shipping_addr else "",
                        "ShipToCompany": shipping_addr.get("companyName", "") if shipping_addr else "",
                        "ShipToPhone": (listify(shipping_phone.get("phone", []))[0].get("phoneNumber", "")
                                        if shipping_phone and listify(shipping_phone.get("phone", [])) else ""),
                        "ShipToPostal": shipping_addr.get("postalCode", "") if shipping_addr else "",
                        "Order Age": safe_get(so, ['orderDate']),
                        "Order Amount usd": safe_get(so, ['rateUsdTransactional']),
                        "Rate Usd Transactional": safe_get(so, ['rateUsdTransactional']),
                        "Sales Rep Name": safe_get(so, ['salesrep', 0, 'salesRepName']),
                        "Shipping Country": shipping_addr.get("country", "") if shipping_addr else "",
                        "Source System Status": safe_get(fulfillments, [0, 'soStatus', 0,'sourceSystemStsCode']),
                        "Tie Number": safe_get(fulfillments, [0, 'salesOrderLines', 0, 'soLineNum']),
                        "Si Number": safe_get(fulfillments, [0, 'salesOrderLines', 0, 'siNumber']),
                        "Req Ship Code": safe_get(fulfillments, [0, 'shipCode']),
                        "Reassigned IP Date": safe_get(fulfillments, [0, 'soStatus', 0, 'sourceSystemStsCode']),
                        "Payment Term Code": safe_get(fulfillments, [0, 'paymentTerm']),
                        "Region Code": safe_get(so, ['region']),
                        "FO ID": safe_get(fulfillments, [0, 'fulfillmentOrder', 0, 'foId']),
                        "System Qty": safe_get(fulfillments, [0, 'systemQty']),
                        "Ship By Date": safe_get(fulfillments, [0, 'shipByDate']),
                        "Facility": facility,
                        "Tax Regstrn Num": safe_get(fulfillments, [0, 'address', 0, 'taxRegstrnNum']),
                        "State Code": shipping_addr.get("stateCode", "") if shipping_addr else "",
                        # "City Code": shipping_addr.get("cityCode", "") if shipping_addr else "",
                        "Customer Num": shipping_addr.get("customerNum", "") if shipping_addr else "",
                        # "Customer Name Ext": shipping_addr.get("customerNameExt", "") if shipping_addr else "",
                        "Country": shipping_addr.get("country", "") if shipping_addr else "",
                        "Ship Code": safe_get(fulfillments, [0, 'shipCode']),
                        "Must Arrive By Date": dateFormation(safe_get(fulfillments, [0, 'mustArriveByDate'])),
                        "Manifest Date": dateFormation(safe_get(fulfillments, [0, 'manifestDate'])),
                        "Revised Delivery Date": dateFormation(safe_get(fulfillments, [0, 'revisedDeliveryDate'])),
                        "Source System ID": safe_get(so, ['sourceSystemId']),
                        "OIC ID": safe_get(fulfillments, [0, 'oicId']),
                        "Order Date": dateFormation(safe_get(so, ['orderDate'])),
                        "Order Type": dateFormation(safe_get(so, ['orderType'])),
                        "Work Order ID": safe_get(workorders_Data, ['WO_ID']),
                        "Dell Blanket PO Num": safe_get(workorders_Data, ['Dell Blanket PO Num']),
                        "Ship To Facility": safe_get(workorders_Data, ['Ship To Facility']),
                        "Is Last Leg": 'Y' if safe_get(workorders_Data, ['Is Last Leg']) else 'N',
                        "Ship From MCID": safe_get(workorders_Data, ['Ship From MCID']),
                        "Ship To MCID": 'Y' if safe_get(workorders_Data, ['Is Last Leg']) else 'N',
                        "WO OTM Enabled": safe_get(workorders_Data, ['WO OTM Enabled']),
                        "WO Ship Mode": safe_get(workorders_Data, ['WO Ship Mode']),
                        "Is Multipack": safe_get(workorders_Data, ['Is Multipack']),
                        "Has Software": safe_get(workorders_Data, ['Has Software']),
                        "Make WO Ack Date": safe_get(workorders_Data, ['Make WO Ack Date']),
                        "MCID Value": safe_get(workorders_Data, ['MCID Value']),
                        "Merge Facility": safe_get(workorders_Data, ['Merge Facility']),
                        "ASN":ASN,
                        "Destination":Destination,
                        "Manifest ID":ASN,
                        "Origin":Origin,
                        "Way Bill Number":Way_Bill_Number, 
                        "Actual Ship Mode":ship_mode,
                        "Actual Ship Code": ActualShipCode,
                        "Order Vol Wt": OrderVolWt,
                        "PP ID": PPID,
                        "SVC Tag": SvcTag,
                        "Target Delivery Date": TargetDeliveryDate,
                        "Total Box Count": TotalBoxCount,
                        "Total Gross Weight": TotalGrossWeight,
                        "Total Volumetric Weight": TotalVolumetricWeight
                    }

                    flat_list.append(row)
                
                    wo_data_list.clear()
                continue
        if not flat_list:
            return {"error": "No Data Found"}

        if len(flat_list) > 0:
            if format_type == "export":
                if filtersValue:
                    data = []
                    count =  {"Count ": len(ValidCount)}
                    data.append(count)
                    data.append(flat_list)
                    ValidCount.clear()
                    return data

            elif format_type == "grid":
                desired_order = list(flat_list[0].keys())
                rows = []
                count =  len(ValidCount)
                for item in flat_list:
                    row = {"columns": [{"value": item.get(k, "")} for k in desired_order]}
                    rows.append(row)
                table_grid_output = tablestructural(rows, region) if rows else []
                if filtersValue:
                    table_grid_output["Count"] = count
                ValidCount.clear()
                return table_grid_output

        return flat_list
        
    except Exception as e:
        return {"error": str(e)}


def safe_get(data, keys, default=""):
    if data is None:
        return default

    for key in keys:
        if isinstance(data, dict):
            data = data.get(key, default)
        elif isinstance(data, list):
            try:
                index = int(key)
                if 0 <= index < len(data):
                    data = data[index]
                else:
                    return default
            except (ValueError, TypeError):
                return default
        else:
            return default

        if data is None:
            return default
    return data

def dateFormation(unformatedDate):
    if unformatedDate not in [None, "", "null"]:
        return unformatedDate.split('.')[0]
    else:
        return ""

def chunk_list(data_list, chunk_size):
    for i in range(0, len(data_list), chunk_size):
        yield data_list[i:i + chunk_size]

def listify(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def pick_address_by_type(so, contact_type):
    addresses = so.get("address", [])
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

def getPath(region):
    try:
        if region == "EMEA":
            return {
                "FID": configPath['Linkage_EMEA'],
                "SALESFULLFILLMENT": configPath['SALES_ORDER_EMEA'],
                "WORKORDER": configPath['WORK_ORDER_EMEA'],
                "ASNODM": configPath['ASNODM_EMEA'],
                "SOPATH": configPath['SO_Header_EMEA'],
            }
        elif region == "APJ":
            return {
                "FID": configPath['Linkage_APJ'],
                "SALESFULLFILLMENT": configPath['SALES_ORDER_APJ'],
                "WORKORDER": configPath['WORK_ORDER_APJ'],
                "ASNODM": configPath['ASNODM_APJ'],
                "SOPATH": configPath['SO_Header_APJ'],
            }
        elif region in ["DAO", "AMER", "LA"]:
            return {
                "FID": configPath['Linkage_DAO'],
                "SALESFULLFILLMENT": configPath['SALES_ORDER_DAO'],
                "WORKORDER": configPath['WORK_ORDER_DAO'],
                "ASNODM": configPath['ASNODM_DAO'],
                "SOPATH": configPath['SO_Header_DAO'],
            }
    except Exception as e:
        print(f"[ERROR] getPath failed: {e}")
        traceback.print_exc()
        return {}
