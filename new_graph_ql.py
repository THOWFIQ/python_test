import os
import sys
import json

def load_config():
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config_ge4.json'))
    with open(config_path, "r") as f:
        return json.load(f)

def fetch_soaorder_query():
    return """
    query MyQuery($salesorderIds: [String!]!) {
    getSoheaderBySoids(salesOrderIds: $salesorderIds) {
        buid
        ppDate
        orderDate
    }
    }
    """

def fetch_fulfillment_query():
    return """
    query MyQuery($fulfillment_id: String!) {
    getFulfillmentsById(fulfillmentId: $fulfillment_id) {
        soHeaderRef
        buid
        salesOrderId
        region
        fulfillments {
        systemQty
        shipByDate
        updateDate
        salesOrderLines {
            lob
        }
        }
    }
    }
    """
def fetch_salesorder_query(salesorderIds):
    return f"""
    query MyQuery {{
      getBySalesorderids(salesorderIds: "{salesorderIds}") {{
        result {{
          asnNumbers {{
            shipDate
            shipFrom
            shipTo
            snNumber
            sourceManifestId
            sourceManifestStatus
          }}
          fulfillment {{
            fulfillmentId
            fulfillmentStatus
            oicId
            sourceSystemStatus
          }}
          fulfillmentOrders {{
            foId
          }}
          salesOrder {{
            buid
            region
            salesOrderId
          }}
          workOrders {{
            channelStatusCode
            woId
            woStatusCode
            woType
          }}
        }}
      }}
    }}
    """

def fetch_workOrderId_query(workOrderId):
    return f"""
    query MyQuery {{
        getWorkOrderById(workOrderId: "{workOrderId}") {{
        channelStatusCode
        woLines {{
            ismultipack
        }}
        shipMode
        updateDate
        isOtmEnabled
        woId
        shipToFacility
        }}
    }}
    """

def fetch_getByWorkorderids_query(workOrderId):
    return f"""
    query MyQuery {{
        getByWorkorderids(workorderIds: "{workOrderId}") {{
          result {{
            asnNumbers {{
              shipDate
              shipFrom
              shipTo
              snNumber
              sourceManifestId
              sourceManifestStatus
            }}
            fulfillment {{
              fulfillmentId
              fulfillmentStatus
              oicId
              sourceSystemStatus
            }}
            fulfillmentOrders {{
              foId
            }}
            salesOrder {{
              buid
              region
              salesOrderId
            }}
            workOrder {{
              channelStatusCode
              woId
              woStatusCode
              woType
            }}
          }}
        }}
    }}
    """

def fetch_getFulfillmentsBysofulfillmentid_query(fulfillment_id):
    return f"""
    query MyQuery {{
        getFulfillmentsBysofulfillmentid(fulfillmentId: "{fulfillment_id}") {{
        fulfillments {{
            shipByDate
            address {{
            taxRegstrnNum
            addressLine1
            postalCode
            stateCode
            cityCode
            customerNum
            customerNameExt
            country
            createDate
            }}
            oicId
            shipCode
            mustArriveByDate
            updateDate
            mergeType
            manifestDate
            revisedDeliveryDate
            deliveryCity
        }}
        sourceSystemId
        salesOrderId
        }}
    }}
    """

def fetch_getAllFulfillmentHeadersSoidFulfillmentid_query(fulfillment_id):
    return f"""
    query MyQuery {{
        getAllFulfillmentHeadersSoidFulfillmentid(fulfillmentId: "{fulfillment_id}") {{
        isDirectShip
        }}
    }}
    """

def fetch_getFbomBySoFulfillmentid_query(fulfillment_id):
    return f"""
    query MyQuery {{
        getFbomBySoFulfillmentid(fulfillmentId: "{fulfillment_id}") {{
        ssc
        }}
    }}
    """
def fetch_foid_query(fo_id):
    return f"""
    query MyQuery {{
    getAllFulfillmentHeadersByFoId(foId: "{fo_id}") {{
        foId
        forderline {{
        shipFromFacility
        shipToFacility
        }}
    }}
    }}
    """
def fetch_getByFulfillmentids_query(fulfillmentid):
    return f"""
    query MyQuery {{
      getByFulfillmentids(fulfillmentIds: "{fulfillmentid}") {{
        result {{
          fulfillment {{
            fulfillmentId
            fulfillmentStatus
            oicId
            sourceSystemStatus
          }}
          fulfillmentOrders {{
            foId
          }}
          salesOrder {{
            buid
            region
            salesOrderId
          }}
          workOrders {{
            channelStatusCode
            createDate
            woId
            woStatusCode
            woType
          }}
          asnNumbers {{
            shipDate
            shipFrom
            shipTo
            snNumber
            sourceManifestId
            sourceManifestStatus
          }}
        }}
      }}
    }}
    """

def fetch_getOrderDate_query(orderFromDate, orderToDate):
    return f"""
    query MyQuery {{
        getOrdersByDate(fromDate: "{orderFromDate}", toDate: "{orderToDate}") {{
            result {{
                fulfillmentId
                oicId
                mustArriveByDate
                salesOrderId
                deliveryCity
                manifestDate
                shipByDate
                shipCode
                updateDate
                soHeaderRef
                systemQty
            }}
        }}
    }}
    """
def fetch_getkeys_query(fulfillmentid,salesorderIds):
  return f"""
    query MyQuery {{
      getKeys(fulfillmentId: "{fulfillment_id}", salesOrderId: "{sales_order_id}") {{
        fulfillmentOrder {{
          foId
        }}
        workorder {{
          parentWoId
          woId
        }}
        salesOrderId
        fulfillmentId
      }}
    }}
  """

def fetch_getparentwoid_query(wo_ids):
  return f"""
    query MyQuery {{
      getParentwoids(woIds: "{wo_ids}") {{
        parentWoId
        woId
      }}
    }}
    """
def fetch_getAsn_query(asn_numbers):
 return f"""
    query MyQuery {{
      getByAsn(asnNumbers: "{asn_numbers}") {{
        result {{
          asnNumber {{
            shipDate
            shipFrom
            shipTo
            snNumber
            sourceManifestId
            sourceManifestStatus
          }}
          salesOrders {{
            fulfillment {{
              fulfillmentId
              fulfillmentStatus
              oicId
              sourceSystemStatus
            }}
            fulfillmentOrders {{
              foId
            }}
            salesOrder {{
              buid
              region
              salesOrderId
            }}
            workOrder {{
              channelStatusCode
              woId
              woStatusCode
              woType
            }}
          }}
        }}
      }}
    }}
    """

def fetch_getAsnbySn_query(sn_numbers)
  return f"""
    query MyQuery {{
      getAsnBySn(snNumbers: "{sn_numbers}") {{
        result {{
          asnNumbers {{
            shipDate
            shipFrom
            shipTo
            sourceManifestId
            sourceManifestStatus
          }}
          snNumber
        }}
      }}
    }}
    """


def tablestructural(data,IsPrimary):
    table_structure = {
        "columns": [            
            {"value": "BUID", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ","EMEA","DAO"], "group": "ID", "checked": IsPrimary in ["APJ","EMEA","DAO"]},
            {"value": "PP Date", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ","EMEA","DAO"], "group": "Date", "checked": IsPrimary in ["APJ","EMEA","DAO"]},
            {"value": "Sales Order Id", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ","EMEA","DAO"], "group": "ID", "checked": IsPrimary in ["APJ","EMEA","DAO"]},
            {"value": "Fulfillment Id", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ","EMEA","DAO"], "group": "ID", "checked": IsPrimary in ["APJ","EMEA","DAO"]},
            {"value": "Region Code", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Code", "checked": IsPrimary in []},
            {"value": "FoId", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
            {"value": "System Qty", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ","EMEA"], "group": "Other", "checked": IsPrimary in ["APJ","EMEA"]},
            {"value": "Ship By Date", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ","EMEA","DAO"], "group": "Date", "checked": IsPrimary in ["APJ","EMEA","DAO"]},
            {"value": "LOB", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ","EMEA"], "group": "Other", "checked": IsPrimary in ["APJ","EMEA"]},
            {"value": "Ship From Facility", "sortBy": "ascending", "isPrimary": IsPrimary in ["EMEA"], "group": "Facility", "checked": IsPrimary in ["EMEA"]},
            {"value": "Ship To Facility", "sortBy": "ascending", "isPrimary": IsPrimary in ["EMEA"], "group": "Facility", "checked": IsPrimary in ["EMEA"]},
            {"value": "Tax Regstrn Num", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Other", "checked": IsPrimary in []},
            {"value": "Address Line1", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Address", "checked": IsPrimary in []},
            {"value": "Postal Code", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Address", "checked": IsPrimary in []},
            {"value": "State Code", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Code", "checked": IsPrimary in []},
            {"value": "City Code", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Address", "checked": IsPrimary in []},
            {"value": "Customer Num", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Other", "checked": IsPrimary in []},
            {"value": "Customer Name Ext", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Other", "checked": IsPrimary in []},
            {"value": "Country", "sortBy": "ascending", "isPrimary": IsPrimary in ['APJ'], "group": "Address", "checked": IsPrimary in ['APJ']},
            {"value": "Create Date", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
            {"value": "Ship Code", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Code", "checked": IsPrimary in []},
            {"value": "Must Arrive By Date", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
            {"value": "Update Date", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
            {"value": "Merge Type", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Type", "checked": IsPrimary in []},
            {"value": "Manifest Date", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
            {"value": "Revised Delivery Date", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
            {"value": "Delivery City", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Address", "checked": IsPrimary in []},
            {"value": "Source System Id", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
            {"value": "Is Direct Ship", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Flag", "checked": IsPrimary in []},
            {"value": "SSC", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Other", "checked": IsPrimary in []},
            {"value": "Vendor Work Order Num", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
            {"value": "Channel Status Code", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Code", "checked": IsPrimary in []},
            {"value": "Ismultipack", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Flag", "checked": IsPrimary in []},
            {"value": "Ship Mode", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Mode", "checked": IsPrimary in []},
            {"value": "Is Otm Enabled", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Flag", "checked": IsPrimary in []},
            {"value": "SN Number", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
            {"value": "OIC ID", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
            {"value":"Order Date","sortBy":"ascending","isPrimary":IsPrimary in ["APJ","DAO"],"group":"Date","checked":IsPrimary in ["APJ","DAO"]}
        ],
        "data": []
    }
    table_structure["data"].extend(data)
    return table_structure

def get_path(region, path, configPath):
    region = region.upper()
    path = path.upper()

    if path == "FID":
        if region == "DAO":
            return configPath.get('Linkage_DAO')
        elif region == "APJ":
            return configPath.get('Linkage_APJ')
        elif region == "EMEA":
            return configPath.get('Linkage_EMEA')

    elif path == "FOID":
        if region == "DAO":
            return configPath.get('FM_Order_DAO')
        elif region in ("EMEA", "APJ"):
            return configPath.get('FM_Order_EMEA_APJ')

    elif path == "SOPATH":
        if region == "DAO":
            return configPath.get('SO_Header_DAO')
        elif region in ("EMEA", "APJ"):
            return configPath.get('SO_Header_EMEA_APJ')

    elif path == "WOID":
        if region == "DAO":
            return configPath.get('WO_Details_DAO')
        elif region in ("EMEA", "APJ"):
            return configPath.get('WO_Details_EMEA_APJ')

    elif path == "FFBOM":
        if region == "DAO":
            return configPath.get('FM_BOM_DAO')
        elif region in ("EMEA", "APJ"):
            return configPath.get('FM_BOM_EMEA_APJ')

    return None
