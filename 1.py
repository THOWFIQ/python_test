import requests
import json
import os
import sys
from flask import jsonify, request
from constants.constants import geturlfromconfig
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import orderProgressGraphqlQueries as gq
from OrderAssist.OrderAssist import fetch_getTransactionDataGraph, GetLinkagebyEntityData
from concurrent.futures import ThreadPoolExecutor

#  GraphQL Query Dictionary
QUERIES = {
    "SalesOrderId": {
        "query": """
        query MyQuery($sono: String!) {
          getSalesOrderHierarchy(salesorderId: $sono) {
            salesOrders {
              SONumber
              OICIDs {
                OICID
                fulfillmentIds {
                  FulfillmentId
                  fulfillmentOrders {
                    FOID
                    workOrders {
                      workOrderId
                      ASNs {
                        ASNNumber
                        SNs {
                          SNNumber
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """,
        "var_key": "sono"
    },
    "FulfillmentId": {
        "query": """
        query GetByFFid($ffid: [String!]!) {
          getByFulfillmentids(fulfillmentIds: $ffid) {
            result {
              workOrders { woId }
              fulfillment { fulfillmentId oicId }
              fulfillmentOrders {foId}
              salesOrder { salesOrderId }
              asnNumbers { sourceManifestId snNumber }
            }
          }
        }
        """,
        "var_key": "ffid"
    },
    "WOId": {
        "query": """
        query GetByWOid($woid: [String!]!) {
          getByWorkorderids(workorderIds: $woid) {
            result {
              workOrder { woId }
              fulfillment { fulfillmentId oicId }
              fulfillmentOrders {foId}
              salesOrder { salesOrderId }
              asnNumbers { sourceManifestId snNumber }
            }
          }
        }
        """,
        "var_key": "woid"
    },
    "ASN": {
        "query": """
        query GetByASN($asn: [String!]!) {
          getByAsn(asnNumbers: $asn) {
            result {
              asnNumber { sourceManifestId snNumber }
              salesOrders {
                salesOrder { salesOrderId }
                fulfillment { fulfillmentId oicId }
                fulfillmentOrders {foId}
                workOrder { woId }
              }
            }
          }
        }
        """,
        "var_key": "asn"
    },
    "SN": {
        "query": """
        query GetBySN($sn: [String!]!) {
          getAsnBySn(snNumbers: $sn) {
            result {
              snNumber
              asnNumbers { sourceManifestId }
            }
          }
        }
        """,
        "var_key": "sn"
    },
    "PreGSO": {
        "query": """
        query GetPreGSOHierarchy($sonumber: String, $region: String) {
          envelopeQuery {
            getPreGSOHierarchy(soNumber: $sonumber, region: $region) {
              salesOrders {
                soNumber
                oICIDs {
                  oICID
                  fulfillmentIds {
                    fulfillmentId
                  }
                }
              }
            }
          }
        }
        """,
        "var_key": "sonumber"
    },
    "routeplan": {
        "query": """
        query RoutePlan($woid: [String!]!) {
            getWorkordersByListOfWoid(workOrderId: $woid) {
                woId
                parentWoId
                woType
                woStatusCode
                channel
                channelStatusCode
                vendorSiteId
                shipToFacility
                shipMode
                fabd
                fsbd
                sequenceId
                estDeliveryDate
                woAttribute {
                attributeName
                attributeValue
                }
                woShipInstr {
                carrierHubCode
                shipviaCode
                shipCode
                carrierCode
                mergeFacility
                }
                woRelation {
                woRelationType
                }
                workOrderAddressHistory {
                carrierHubCode
                shipviaCode
                mergeFacility
                status
                }
            }
        }
        """,
        "var_key": "woid"
    }
}

def execute_graphql(query: str, variables: dict, url: str):
    response = requests.post(
        url,
        json={"query": query, "variables": variables},
        headers={"Content-Type": "application/json"},
        verify=False
    )
    response.raise_for_status()
    return response.json()


def extract_data(data):
    # Check if it's the new GraphQL response structure
    if "data" in data and "envelopeQuery" in data["data"]:
        try:
            hierarchy = data["data"]["envelopeQuery"]["getPreGSOHierarchy"]
            if isinstance(hierarchy, list) and hierarchy:
                sales_orders = hierarchy[0]["salesOrders"]
            else:
                sales_orders = hierarchy.get("salesOrders", [])
            result = []
            for so in sales_orders:
                so_number = so.get("soNumber")
                for oic in so.get("oICIDs", []):
                    oic_id = oic.get("oICID")
                    for ffid_obj in oic.get("fulfillmentIds", []):
                        ffid = ffid_obj.get("fulfillmentId")
                        result.append({
                            "soNumber": so_number,
                            "oicId": oic_id,
                            "fulfillmentId": ffid
                        })
            return result if result else None
        except (KeyError, TypeError, IndexError):
            return None
    else:
        # Fallback to old structure
        so_number = data.get("soNumber")
        result = []
        for oic_detail in data.get("oicDetails", []):
            if not oic_detail.get("isPreGSO", True):
                oic_id = oic_detail.get("oicId")
                for fulfillment in oic_detail.get("fulfillments", []):
                    fulfillment_id = fulfillment.get("fulfillmentId")
                    if fulfillment_id:
                        result.append({
                            "soNumber": so_number,
                            "oicId": oic_id,
                            "fulfillmentId": fulfillment_id
                        })
        return result if result else None



def fallback_fetch_entity_logic(req_json):
    region = req_json.get("region")
    # Map region to DAO/APJ/EMEA
    if region in ["AMER", "LA"]:
        region_key = 'DAO'
    elif region == "APJ":
        region_key = 'APJ'
    elif region == "EMEA":
        region_key = 'EMEA'
    else:
        region_key = 'DAO'  # default fallback

    url = geturlfromconfig('OOE', region_key)

    query = QUERIES["PreGSO"]["query"]
    variables = {"sonumber": req_json.get("entity_value"), "region": region}
    try:
        result = execute_graphql(query, variables, url)
        extracted_data = extract_data(result)
        print(json.dumps(extracted_data, indent=4))
        return jsonify(result)
    except requests.exceptions.RequestException as e:
        status = getattr(e.response, "status_code", None)
        return jsonify({"error": str(e), "status_code": status})


def fetch_entity_logic(req_json):
    entity_type = req_json.get("entity_type")
    entity_value = req_json.get("entity_value")
    region = req_json.get("region")

    if entity_type not in QUERIES:
        return jsonify({"error": f"Unsupported entity type: {entity_type}"}), 400

    if region in ["AMER", "LA"]:
        region = 'DAO'
    elif region == "APJ":
        region = 'APJ'
    elif region == "EMEA":
        region = 'EMEA'
    else:
        region = 'DAO'

    graphql_url = geturlfromconfig('Linkage', region)

    query_info = QUERIES[entity_type]
    variables = {query_info["var_key"]: entity_value} if entity_type == "SalesOrderId" else {query_info["var_key"]: [entity_value]}
    result = execute_graphql(query_info["query"], variables, graphql_url)

    if entity_type == "SN":
        try:
            asn_list = result["data"]["getAsnBySn"]["result"][0]["asnNumbers"]
            if asn_list:
                asn_value = asn_list[0]["sourceManifestId"]
                asn_query = QUERIES["ASN"]["query"]
                asn_result = execute_graphql(asn_query, {"asn": [asn_value]}, graphql_url)
                return jsonify({
                    "sn_result": result,
                    "asn_followup": asn_result
                })
        except Exception as e:
            return jsonify({
                "sn_result": result,
                "asn_followup_error": str(e)
            })

    return jsonify(result)

def route_plan_logic(req_json):
    woid = req_json.get("woid")
    region = req_json.get("region")

    if not woid:
        return jsonify({"error": "woid is required"}), 400

    if region in ["AMER", "LA"]:
        region = 'DAO'
    elif region == "APJ":
        region = 'APJ'
    elif region == "EMEA":
        region = 'EMEA'
    else:
        region = 'DAO'

    graphql_url = geturlfromconfig('Routeplan', region)

    query_info = QUERIES["routeplan"]
    query = query_info["query"]
    variables = {query_info["var_key"]: woid}

    result = execute_graphql(query, variables, graphql_url)

    return jsonify(result)

STATUS_RULES = {
    "FFID": ["PreGSOReceiver", "WorkOrderCreation"],
    "WOID": ["ProcessFOWONotification", "PublishEDI850"],
    "ASN": ["InitASNPalletConsumeProcess", "ManufacturingworkorderASNAcked"],
    "SN": ["InitSNConsumeProcess", "ManufacturingworkorderTransportCMP"]
}

def extract_entity_ids(hierarchy):
    entities = {}

    def walk(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "OICID" and value:
                    entities[value] = "OICID"
                elif key == "FulfillmentId" and value:
                    entities[value] = "FulfillmentId"
                elif key == "FOID" and value:
                    entities[value] = "FOID"
                elif key == "workOrderId" and value:
                    entities[value] = "workOrderId"
                elif key == "ASNNumber" and value:
                    entities[value] = "ASNNumber"
                elif key == "SNNumber" and value:
                    entities[value] = "SNNumber"
                walk(value)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    walk(hierarchy)
    return entities


def get_order_types_for_entity(entity_key):
    return ["OOE", "CM", "FDH-Stable"]


def get_status_logic(req_json):

    entity_type = req_json.get("entity_type")
    entity_value = req_json.get("entity_value")
    region = req_json.get("region")
    original_region = region

    if entity_type not in QUERIES:
        return jsonify({"error": f"Unsupported entity type: {entity_type}"}), 400

    if region in ["AMER", "LA"]:
        region = 'DAO'
    elif region == "APJ":
        region = 'APJ'
    elif region == "EMEA":
        region = 'EMEA'
    else:
        region = 'DAO'

    graphql_url = geturlfromconfig('Linkage', region)

    query_info = QUERIES[entity_type]
    variables = {query_info["var_key"]: entity_value} if entity_type == "SalesOrderId" else {query_info["var_key"]: [entity_value]}
    result = execute_graphql(query_info["query"], variables, graphql_url)

    hierarchy = result.get("data", {}).get("getSalesOrderHierarchy", {})
    entities = extract_entity_ids(hierarchy)

    try:
        linkage_response = GetLinkagebyEntityData(entity_value, entity_type, region)
        if isinstance(linkage_response, str):
            linked_values = [v.strip() for v in linkage_response.split(',') if v.strip()]
            for linked_value in linked_values:
                if linked_value not in entities and linked_value != entity_value:
                    entities[linked_value] = entity_type
    except Exception as e:
        print(f"GetLinkagebyEntityData FAILED for {entity_value}: {e}")

    region_key = 'DAO' if original_region in ["AMER", "LA"] else 'APJ' if original_region == "APJ" else 'EMEA' if original_region == "EMEA" else 'DAO'
    ooe_url = geturlfromconfig('OOE', region_key)
    pregso_query = QUERIES["PreGSO"]["query"]
    pregso_variables = {"sonumber": entity_value, "region": original_region}
    try:
        pregso_result = execute_graphql(pregso_query, pregso_variables, ooe_url)
        extracted_entities = extract_data(pregso_result)
        if extracted_entities:
            for item in extracted_entities:
                so_num = item.get("soNumber")
                oic_id = item.get("oicId")
                ff_id = item.get("fulfillmentId")
                if so_num and so_num not in entities:
                    entities[so_num] = "SalesOrderId"
                if oic_id and oic_id not in entities:
                    entities[oic_id] = "OICID"
                if ff_id and ff_id not in entities:
                    entities[ff_id] = "FulfillmentId"
    except Exception as e:
        print(f"PreGSO call FAILED for {entity_value}: {e}")

    def call_api(order_type, value):
        try:
            return value, fetch_getTransactionDataGraph(order_type, value, region)
        except Exception as e:
            print(f"{order_type} FAILED for {value}: {e}")
            return value, {"transactions": []}

    entity_txn_map = {}
    for value, entity_key in entities.items():
        entity_txn_map[value] = {'txn_set': set(), 'entity_key': entity_key}

    with ThreadPoolExecutor(max_workers=10) as executor:

        futures = []
        for value, entity_key in entities.items():
            order_types = get_order_types_for_entity(entity_key)
            for ot in order_types:
                futures.append(executor.submit(call_api, ot, value))

        for f in futures:
            value, resp = f.result(timeout=15)

            txn_set = entity_txn_map[value]['txn_set']

            for txn in resp.get("transactions", []):
                name = txn.get("name")
                if name:
                    txn_set.add(name)

    all_txn_names = set()
    for info in entity_txn_map.values():
        all_txn_names.update(info['txn_set'])

    entity_status = {}

    for entity_value, info in entity_txn_map.items():
        entity_key = info['entity_key']
        txn_names = all_txn_names 

        matched_entity_type = None
        status = "NO_DATA"

        rule_key = None
        if entity_key == "FulfillmentId":
            rule_key = "FFID"
        elif entity_key == "workOrderId":
            rule_key = "WOID"
        elif entity_key == "ASNNumber":
            rule_key = "ASN"
        elif entity_key == "SNNumber":
            rule_key = "SN"
        elif entity_key == "OICID":
            status = "NO_STATUS" 
            matched_entity_type = "OICID"
        else:
            status = "UNKNOWN"
            matched_entity_type = "UNKNOWN"

        if rule_key:
            required_txns = STATUS_RULES.get(rule_key, [])
            found_count = sum(txn in txn_names for txn in required_txns)

            if found_count == len(required_txns):
                status = "COMPLETED"
                matched_entity_type = rule_key
            elif found_count > 0:
                status = "IN_PROGRESS"
                matched_entity_type = rule_key
            else:
                status = "NO_DATA"
                matched_entity_type = rule_key

        entity_status[entity_value] = {
            "entityType": matched_entity_type,
            "status": status
        }

    statuses = [v["status"] for v in entity_status.values()]

    if statuses and all(s == "COMPLETED" for s in statuses):
        so_status = "COMPLETED"
    elif any(s in ["COMPLETED", "IN_PROGRESS"] for s in statuses):
        so_status = "IN_PROGRESS"
    else:
        so_status = "NO_DATA"

    def append_statuses(hierarchy, entity_status):
        def walk(obj):
            if isinstance(obj, dict):
                keys_to_add = {}
                for key, value in obj.items():
                    if key in ["FulfillmentId", "workOrderId", "ASNNumber", "SNNumber"] and value in entity_status:
                        keys_to_add["status"] = entity_status[value]["status"]
                    walk(value)
                obj.update(keys_to_add)
            elif isinstance(obj, list):
                for item in obj:
                    walk(item)
        walk(hierarchy)

    append_statuses(hierarchy, entity_status)

    for so in hierarchy.get("salesOrders", []):
        so_statuses = []
        for oic in so.get("OICIDs", []):
            oic_id = oic.get("OICID")
            oic_statuses = []
            for ff in oic.get("fulfillmentIds", []):
                ff_status = ff.get("status", "NO_DATA")
                oic_statuses.append(ff_status)
                for fo in ff.get("fulfillmentOrders", []):
                    fo_statuses = []
                    for wo in fo.get("workOrders", []):
                        wo_status = wo.get("status", "NO_DATA")
                        fo_statuses.append(wo_status)
                        for asn in wo.get("ASNs", []):
                            asn_status = asn.get("status", "NO_DATA")
                            for sn in asn.get("SNs", []):
                                sn_status = sn.get("status", "NO_DATA")
                    fo["status"] = "COMPLETED" if fo_statuses and all(s == "COMPLETED" for s in fo_statuses) else ("IN_PROGRESS" if any(s in ["COMPLETED", "IN_PROGRESS"] for s in fo_statuses) else "NO_DATA")
         
            if oic_id in entity_status and entity_status[oic_id]["status"] == "NO_STATUS":
                oic["status"] = "NO_STATUS"
            else:
                oic["status"] = "COMPLETED" if oic_statuses and all(s == "COMPLETED" for s in oic_statuses) else ("IN_PROGRESS" if any(s in ["COMPLETED", "IN_PROGRESS"] for s in oic_statuses) else "NO_DATA")
            so_statuses.append(oic["status"])
        so["status"] = "COMPLETED" if so_statuses and all(s == "COMPLETED" for s in so_statuses) else ("IN_PROGRESS" if any(s in ["COMPLETED", "IN_PROGRESS"] for s in so_statuses) else "NO_DATA")

        key_types = ["FulfillmentId", "workOrderId", "ASNNumber", "SNNumber"]
        if all(entity_status.get(k, {}).get("status") == "COMPLETED" for k in entity_status if entity_txn_map.get(k, {}).get('entity_key') in key_types):
            so["status"] = "COMPLETED"

    return jsonify(result)

def uppercase_keys(obj):
    if isinstance(obj, dict):
        return {str(k).upper(): uppercase_keys(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [uppercase_keys(x) for x in obj]
    else:
        return obj


def _ensure_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def _to_str(x, default=""):
    return default if x is None else str(x)


def _find_first(d, keys, default=None):
    if not isinstance(d, dict):
        return default
    for k in keys:
        if k in d:
            return d[k]
    return default


def _collect_all_fulfillment_ids_anywhere(u):
    out = set()

    def walk(x):
        if isinstance(x, dict):
            if "FULFILLMENTID" in x and (x["FULFILLMENTID"] or "") != "":
                out.add(_to_str(x["FULFILLMENTID"]))
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for it in x:
                walk(it)

    walk(u)
    return list(out)


def is_graphql_error(res):
    if not isinstance(res, dict):
        return True
    if res.get("errors"):
        return True
    data_val = res.get("data")
    return data_val is None


def make_minimal_canonical(fallback_so, ffids=None):
    ffids = ffids or []
    oicids_out = []
    if ffids:
        oicids_out.append({
            "FULFILLMENTIDS": [
                {"FULFILLMENTID": _to_str(fid), "FULFILLMENTORDERS": [{"WORKORDERS": []}]}
                for fid in ffids
            ]
        })

    return {
        "DATA": {
            "GETSALESORDERHIERARCHY": {
                "SALESORDERS": [
                    {
                        "SONUMBER": _to_str(fallback_so),
                        "OICIDS": oicids_out
                    }
                ]
            }
        }
    }


def is_null_hierarchy_anycase(res):
    if not isinstance(res, dict):
        return True

    u = uppercase_keys(res)
    data = u.get("DATA") or {}
    if not isinstance(data, dict):
        return True

    gsoh = data.get("GETSALESORDERHIERARCHY") or {}
    if not isinstance(gsoh, dict):
        return True

    so_list = gsoh.get("SALESORDERS") or []
    if not isinstance(so_list, list) or not so_list:
        return True

    found_valid = False
    for so in so_list:
        so = so or {}
        so_number = (so.get("SONUMBER") or "").strip()

        oicids = so.get("OICIDS") or []
        if not isinstance(oicids, list):
            oicids = []

        has_ffid = False
        for oic in oicids:
            oic = oic or {}
            ff_list = oic.get("FULFILLMENTIDS") or []
            if not isinstance(ff_list, list):
                ff_list = []
            if any(((ff or {}).get("FULFILLMENTID") or "").strip() for ff in ff_list):
                has_ffid = True
                break

        if so_number or has_ffid:
            found_valid = True
            break

    return not found_valid


def is_null_hierarchy(res):
    return is_null_hierarchy_anycase(res)


def coerce_graphql_hierarchy_to_canonical(res_u, fallback_so=None):
    data = res_u.get("DATA") or {}
    if not isinstance(data, dict):
        data = {}

    gsoh = data.get("GETSALESORDERHIERARCHY") or {}
    if not isinstance(gsoh, dict):
        gsoh = {}

    if "SALESORDERS" in gsoh and isinstance(gsoh.get("SALESORDERS"), list):
        if fallback_so:
            for so in gsoh["SALESORDERS"]:
                if isinstance(so, dict) and not (so.get("SONUMBER") or "").strip():
                    so["SONUMBER"] = str(fallback_so)
        return res_u

    sonumber = (_find_first(res_u, ["SONUMBER", "SALESORDERID", "SOID", "ORDERID"], default="") or "").strip()
    if not sonumber and fallback_so is not None:
        sonumber = str(fallback_so)

    return {
        "DATA": {
            "GETSALESORDERHIERARCHY": {
                "SALESORDERS": [
                    {"SONUMBER": sonumber, "OICIDS": []}
                ]
            }
        }
    }


def pregso_to_canonical_fallback_aware(res_any, fallback_so):
    if is_graphql_error(res_any):
        return make_minimal_canonical(fallback_so)

    res_list = _ensure_list(res_any)
    root0 = next((item for item in res_list if isinstance(item, dict)), None)
    if root0 is None:
        return make_minimal_canonical(fallback_so)

    u = uppercase_keys(root0)

    data = u.get("DATA") or {}
    if not isinstance(data, dict):
        ffids = _collect_all_fulfillment_ids_anywhere(u)
        return make_minimal_canonical(fallback_so, ffids=ffids)

    env = data.get("ENVELOPEQUERY") or {}
    if not isinstance(env, dict):
        ffids = _collect_all_fulfillment_ids_anywhere(u)
        return make_minimal_canonical(fallback_so, ffids=ffids)

    pregso_list = env.get("GETPREGSOHIERARCHY") or []
    pregso_list = _ensure_list(pregso_list)

    sales_orders_out = []

    for entry in pregso_list:
        if not isinstance(entry, dict):
            continue

        so_list = entry.get("SALESORDERS") or []
        so_list = _ensure_list(so_list)

        for so in so_list:
            if not isinstance(so, dict):
                continue

            so_number = (so.get("SONUMBER") or "").strip() or _to_str(fallback_so)

            oicids_in = so.get("OICIDS") or []
            oicids_in = _ensure_list(oicids_in)

            oicids_out = []
            for o in oicids_in:
                if not isinstance(o, dict):
                    continue

                ff_in = o.get("FULFILLMENTIDS") or []
                ff_in = _ensure_list(ff_in)

                ff_out = []
                for ff in ff_in:
                    ffid = _to_str(ff.get("FULFILLMENTID", "")) if isinstance(ff, dict) else _to_str(ff, "")
                    if not ffid:
                        continue

                    ff_out.append({
                        "FULFILLMENTID": ffid,
                        "FULFILLMENTORDERS": [{"WORKORDERS": []}]
                    })

                if ff_out:
                    oicids_out.append({"FULFILLMENTIDS": ff_out})

            sales_orders_out.append({
                "SONUMBER": so_number,
                "OICIDS": oicids_out
            })

    if sales_orders_out:
        return {
            "DATA": {
                "GETSALESORDERHIERARCHY": {
                    "SALESORDERS": sales_orders_out
                }
            }
        }

    ffids = _collect_all_fulfillment_ids_anywhere(u)
    return make_minimal_canonical(fallback_so, ffids=ffids)


def format_hierarchy_to_tree(hierarchy_responses):
    final_trees = []

    for resp in (hierarchy_responses or []):
        u = uppercase_keys(resp)

        data = u.get("DATA") or {}
        if not isinstance(data, dict):
            data = {}

        root = data.get("GETSALESORDERHIERARCHY") or {}
        if not isinstance(root, dict):
            root = {}

        sales_orders = root.get("SALESORDERS") or []
        if not isinstance(sales_orders, list):
            sales_orders = []

        for so in sales_orders:
            so = so or {}
            so_number = _to_str(so.get("SONUMBER", ""))

            so_node = {
                "id": so_number,
                "label": "Sales Order ID",
                "status": "In Progress",
                "expanded": True,
                "FFIDCount": 0,
                "WOCount": 0,
                "ASNCount": 0,
                "SNCount": 0,
                "children": []
            }

            for oic in (so.get("OICIDS") or []):
                oic = oic or {}
                for ff in (oic.get("FULFILLMENTIDS") or []):
                    ff = ff or {}
                    ffid_val = _to_str(ff.get("FULFILLMENTID", ""))

                    ff_node = {
                        "id": ffid_val,
                        "label": "Fullfillment ID",
                        "status": "In Progress",
                        "expanded": True,
                        "WOCount": 0,
                        "ASNCount": 0,
                        "SNCount": 0,
                        "children": []
                    }

                    for fo in (ff.get("FULFILLMENTORDERS") or []):
                        fo = fo or {}
                        for wo in (fo.get("WORKORDERS") or []):
                            wo = wo or {}
                            wo_id = _to_str(wo.get("WORKORDERID", ""))

                            asns = wo.get("ASNS") or []
                            if not isinstance(asns, list):
                                asns = []

                            asn_nodes = []
                            total_sn_count_for_wo = 0

                            for a in asns:
                                a = a or {}
                                asn_num = _to_str(a.get("ASNNUMBER", ""))
                                sn_list = a.get("SNS") or []
                                if not isinstance(sn_list, list):
                                    sn_list = []

                                sn_nodes = [
                                    {
                                        "id": _to_str((sn or {}).get("SNNUMBER", "")),
                                        "label": "SN Number",
                                        "status": "In Progress",
                                        "expanded": True
                                    }
                                    for sn in sn_list
                                    if _to_str((sn or {}).get("SNNUMBER", "")) != ""
                                ]

                                asn_nodes.append({
                                    "id": asn_num,
                                    "label": "ASN Number",
                                    "status": "In Progress",
                                    "expanded": True,
                                    "SNCount": len(sn_nodes),
                                    "children": sn_nodes
                                })

                                total_sn_count_for_wo += len(sn_nodes)

                            wo_node = {
                                "id": wo_id,
                                "label": "Work Order ID",
                                "status": "Completed" if asn_nodes else "In Progress",
                                "expanded": True,
                                "ASNCount": len(asn_nodes),
                                "SNCount": total_sn_count_for_wo,
                                "children": asn_nodes
                            }

                            ff_node["children"].append(wo_node)
                            ff_node["WOCount"] += 1
                            ff_node["ASNCount"] += len(asn_nodes)
                            ff_node["SNCount"] += total_sn_count_for_wo

                    if ff_node["id"] or ff_node["children"]:
                        so_node["children"].append(ff_node)
                        so_node["FFIDCount"] += 1
                        so_node["WOCount"] += ff_node["WOCount"]
                        so_node["ASNCount"] += ff_node["ASNCount"]
                        so_node["SNCount"] += ff_node["SNCount"]

            final_trees.append(so_node)

    return final_trees


def orderprogressPreGSOData(entityValue, region):
    graphql_url = geturlfromconfig("OOE", region)
    query = gq.fetch_pregso_hierarchy_query(entityValue, region)
    return execute_graphql(query, {}, graphql_url)


def OrderProgressData(entityType, entityValue, region):
   
    graphql_url = geturlfromconfig("Linkage", region)
    ids = [i.strip() for i in (entityValue or "").split(",") if i.strip()]
    responses = []

    for idval in ids:
        so = ff = wo = ""
        et = (entityType or "").lower().strip()
        if et == "salesorderid":
            so = idval
        elif et == "fulfillmentid":
            ff = idval
        elif et == "workorderid":
            wo = idval
        else:
            continue

        query = gq.fetch_salesorder_hierarchy_query(so, ff, wo)
        res = execute_graphql(query, {}, graphql_url)

        if is_graphql_error(res):
            pregso_val = orderprogressPreGSOData(idval, region)
            if is_graphql_error(pregso_val):
                responses.append(make_minimal_canonical(idval))
                continue

            pregso_canonical = pregso_to_canonical_fallback_aware(pregso_val, fallback_so=idval)
            responses.append(pregso_canonical)
            continue

        res_u = uppercase_keys(res)
        res_u_canonical = coerce_graphql_hierarchy_to_canonical(res_u, fallback_so=idval)

        if is_null_hierarchy_anycase(res_u_canonical):
            pregso_val = orderprogressPreGSOData(idval, region)
            if is_graphql_error(pregso_val):
                responses.append(make_minimal_canonical(idval))
            else:
                pregso_canonical = pregso_to_canonical_fallback_aware(pregso_val, fallback_so=idval)
                responses.append(pregso_canonical)
        else:
            responses.append(res_u_canonical)

    return format_hierarchy_to_tree(responses)
