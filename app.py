def fetch_salesOrder_query(salesorderId):
    return f"""
        query MyQuery {{
        getSalesOrderBySoids(soid: {json.dumps(salesorderId)}) {{
            salesOrders {{
            agreementId
            totalPrice
            buid
            currency
            poNumber
            dpid
            locationNum
            orderDate
            rateUsdTransactional
            orderCreateDate
            sourceSystemId
            salesOrderId
            region
            orderType
            address {{
                companyName
                cityCode
                city
                firstName
                lastName
                fullName
                country
                stateCode
                addressLine1
                addressLine2
                postalCode
                customerNum
                customerNameExt
                contact {{
                    contactType
                }}
            }}
            fulfillments {{
                deliveryCity
                mergeType
                paymentTerm
                shipCode
                systemQty
                shipByDate
                mustArriveByDate
                manifestDate
                revisedDeliveryDate
                oicId
                fulfillmentId
                address {{
                    postalCode
                    taxRegstrnNum
                    contact {{
                        contactType
                    }}
                    phone {{
                        phoneNumber
                    }}
                }}
                soStatus {{
                    sourceSystemStsCode
                    fulfillmentStsCode
                    statusDate
                }}
                salesOrderLines {{
                    soLineQty
                    lob
                    siNumber
                    soLineNum
                    facility
                    specialInstructions {{
                        specialInstructionId
                        specialInstructionType
                    }}                
                }}
                fulfillmentOrder {{
                    foId
                    fulfillmentId
                }}
            }}
            salesRep {{
                salesRepName
            }}
            }}
        }}
        }}
        """

def fetch_Fullfillment_query(fullfillmentId):
    return f"""
        query MyQuery {{
        getSalesOrderByFfids(ffid: {json.dumps(fullfillmentId)}) {{
            salesOrders {{
            agreementId
            totalPrice
            buid
            currency
            poNumber
            dpid
            locationNum
            orderDate
            rateUsdTransactional
            orderCreateDate
            sourceSystemId
            salesOrderId
            region
            orderType
            address {{
                companyName
                cityCode
                city
                firstName
                lastName
                fullName
                country
                stateCode
                addressLine1
                addressLine2
                postalCode
                customerNum
                customerNameExt
                contact {{
                contactType
                }}
            }}
            fulfillments {{
                deliveryCity
                mergeType
                paymentTerm
                shipCode
                systemQty
                shipByDate
                mustArriveByDate
                manifestDate
                revisedDeliveryDate
                oicId
                fulfillmentId
                address {{
                contact {{
                        contactType
                    }}
                phone {{
                    phoneNumber
                }}
                postalCode
                taxRegstrnNum
                }}
                soStatus {{
                sourceSystemStsCode
                fulfillmentStsCode
                statusDate
                }}
                salesOrderLines {{
                specialInstructions {{
                    specialInstructionId
                    specialInstructionType
                }}
                soLineQty
                lob
                siNumber
                soLineNum
                facility
                }}
                fulfillmentOrder {{
                    foId
                    fulfillmentId
                }}
            }}
            salesRep {{
                salesRepName
            }}
            }}
        }}
        }}
        """

def fetch_workOrder_query(workorderId):
    return f"""
        query MyQuery {{
        getWorkOrderByWoIds(woIds: {json.dumps(workorderId)}) {{
            channel
            vendorSiteId
            dellBlanketPoNum
            woLines {{
            ismultipack
            woLineType
            }}
            woType
            woId
            woStatusList {{
            channelStatusCode
            }}
            woShipInstr {{
            carrierHubCode
            mergeFacility
            }}
            shipMode
            shipToFacility
        }}
        }}
        """

def fetch_keysphereFullfillment_query(fullFillment_ids):
    return f"""
        query MyQuery {{
            getByFulfillmentids(fulfillmentIds: {json.dumps(fullFillment_ids)}) {{
                result {{
                salesOrder {{
                    region
                    salesOrderId
                }}
                workOrders {{
                    woId
                }}
                fulfillment {{
                    fulfillmentId
                }}
                fulfillmentOrders {{
                    foId
                }}
                asnNumbers {{
                    snNumber
                    sourceManifestId
                    sourceManifestStatus
                    shipTo
                    shipMode
                    shipFromVendorId
                    shipFrom
                    shipDate
                }}
                }}
            }}
        }}
        """

def fetch_keysphereSalesorder_query(salesOrder_ids):
    return f"""
        query MyQuery {{
            getBySalesorderids(salesorderIds: {json.dumps(salesOrder_ids)}) {{
                result {{
                salesOrder {{
                    region
                    salesOrderId
                }}
                workOrders {{
                    woId
                }}
                fulfillment {{
                    fulfillmentId
                }}
                fulfillmentOrders {{
                    foId
                }}
                asnNumbers {{
                    snNumber
                    sourceManifestId
                    sourceManifestStatus
                    shipTo
                    shipMode
                    shipFromVendorId
                    shipFrom
                    shipDate
                }}
                }}
            }}
        }}
        """

def fetch_keysphereWorkorder_query(workOrder_ids):
    return f"""
        query MyQuery {{
            getByWorkorderids(workorderIds: {json.dumps(workOrder_ids)}) {{
                result {{
                salesOrder {{
                    region
                    salesOrderId
                }}
                workOrders {{
                    woId
                }}
                fulfillment {{
                    fulfillmentId
                }}
                fulfillmentOrders {{
                    foId
                }}
                asnNumbers {{
                    snNumber
                    sourceManifestId
                    sourceManifestStatus
                    shipTo
                    shipMode
                    shipFromVendorId
                    shipFrom
                    shipDate
                }}
                }}
            }}
        }}
        """
