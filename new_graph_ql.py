# graphqlQueries.py

# --------------------- GraphQL Queries ---------------------

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

def fetch_salesorder_query(salesorderId):
    return f"""
    query MyQuery {{
      getBySalesorderids(salesorderIds: "{salesorderId}") {{
        result {{
          fulfillment {{
            fulfillmentId
          }}
          salesOrder {{
            salesOrderId
            buid
            region
          }}
          workOrders {{
            woId
          }}
          fulfillmentOrders {{
            foId
          }}
          asnNumbers {{
            snNumber
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
            snNumber
          }}
          fulfillment {{
            fulfillmentId
          }}
          salesOrder {{
            salesOrderId
          }}
          fulfillmentOrders {{
            foId
          }}
          workOrder {{
            woId
          }}
        }}
      }}
    }}
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
          salesOrder {{
            salesOrderId
            buid
            region
          }}
          workOrders {{
            woId
            channelStatusCode
          }}
          fulfillment {{
            oicId
          }}
          fulfillmentOrders {{
            foId
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


# --------------------- Table Structure Builder ---------------------

def tablestructural(data, IsPrimary):
    table_structure = {
        "columns": [
            {"value": "BUID", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ", "EMEA", "DAO"], "group": "ID", "checked": True},
            {"value": "PP Date", "sortBy": "ascending", "isPrimary": True, "group": "Date", "checked": True},
            {"value": "Sales Order Id", "sortBy": "ascending", "isPrimary": True, "group": "ID", "checked": True},
            {"value": "Fulfillment Id", "sortBy": "ascending", "isPrimary": True, "group": "ID", "checked": True},
            {"value": "Region Code", "sortBy": "ascending", "group": "Code", "checked": False},
            {"value": "FoId", "sortBy": "ascending", "group": "ID", "checked": False},
            {"value": "System Qty", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ", "EMEA"], "group": "Other", "checked": IsPrimary in ["APJ", "EMEA"]},
            {"value": "Ship By Date", "sortBy": "ascending", "group": "Date", "checked": True},
            {"value": "LOB", "sortBy": "ascending", "group": "Other", "checked": IsPrimary in ["APJ", "EMEA"]},
            {"value": "Ship From Facility", "sortBy": "ascending", "group": "Facility", "checked": IsPrimary == "EMEA"},
            {"value": "Ship To Facility", "sortBy": "ascending", "group": "Facility", "checked": IsPrimary == "EMEA"},
            {"value": "Tax Regstrn Num", "sortBy": "ascending", "group": "Other", "checked": False},
            {"value": "Address Line1", "sortBy": "ascending", "group": "Address", "checked": False},
            {"value": "Postal Code", "sortBy": "ascending", "group": "Address", "checked": False},
            {"value": "State Code", "sortBy": "ascending", "group": "Code", "checked": False},
            {"value": "City Code", "sortBy": "ascending", "group": "Address", "checked": False},
            {"value": "Customer Num", "sortBy": "ascending", "group": "Other", "checked": False},
            {"value": "Customer Name Ext", "sortBy": "ascending", "group": "Other", "checked": False},
            {"value": "Country", "sortBy": "ascending", "group": "Address", "checked": IsPrimary == "APJ"},
            {"value": "Create Date", "sortBy": "ascending", "group": "Date", "checked": False},
            {"value": "Ship Code", "sortBy": "ascending", "group": "Code", "checked": False},
            {"value": "Must Arrive By Date", "sortBy": "ascending", "group": "Date", "checked": False},
            {"value": "Update Date", "sortBy": "ascending", "group": "Date", "checked": False},
            {"value": "Merge Type", "sortBy": "ascending", "group": "Type", "checked": False},
            {"value": "Manifest Date", "sortBy": "ascending", "group": "Date", "checked": False},
            {"value": "Revised Delivery Date", "sortBy": "ascending", "group": "Date", "checked": False},
            {"value": "Delivery City", "sortBy": "ascending", "group": "Address", "checked": False},
            {"value": "Source System Id", "sortBy": "ascending", "group": "ID", "checked": False},
            {"value": "Is Direct Ship", "sortBy": "ascending", "group": "Flag", "checked": False},
            {"value": "SSC", "sortBy": "ascending", "group": "Other", "checked": False},
            {"value": "Vendor Work Order Num", "sortBy": "ascending", "group": "ID", "checked": False},
            {"value": "Channel Status Code", "sortBy": "ascending", "group": "Code", "checked": False},
            {"value": "Ismultipack", "sortBy": "ascending", "group": "Flag", "checked": False},
            {"value": "Ship Mode", "sortBy": "ascending", "group": "Mode", "checked": False},
            {"value": "Is Otm Enabled", "sortBy": "ascending", "group": "Flag", "checked": False},
            {"value": "SN Number", "sortBy": "ascending", "group": "ID", "checked": False},
            {"value": "OIC ID", "sortBy": "ascending", "group": "ID", "checked": False},
            {"value": "Order Date", "sortBy": "ascending", "group": "Date", "checked": IsPrimary in ["APJ", "DAO"]}
        ],
        "data": data
    }
    return table_structure
