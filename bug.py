def OutputFormat(result_map, format_type=None,region=None):
  try:
      flat_list = []
      for results in result_map:
       
        try:         
          base_row = {
              "BUID": safe_get(salesorder, ['salesOrder', 'buid']),
              "PP Date": dateFormation(StatusDate) if statusCode == "PP" else "",
              "IP Date": dateFormation(StatusDate) if statusCode == "IP" else "",
              "MN Date": dateFormation(StatusDate) if statusCode == "MN" else "",
              "SC Date": dateFormation(StatusDate) if statusCode == "SC" else "",
              "CFI Flag": "",
              "Agreement Id": safe_get(salesbyids, ['agreementId']),
              "Amount": safe_get(salesbyids, ['totalPrice']),
              "Currency Code": safe_get(salesbyids, ['currency']),
              "Customer Po Number": safe_get(salesbyids, ['poNumber']),
              "Dp Id": safe_get(salesbyids, ['dpid']),
              "Location Number": safe_get(salesbyids, ['locationNum']),
              "Order Age": safe_get(salesbyids, ['orderDate']),
              "Order Amount usd": safe_get(salesbyids, ['rateUsdTransactional']),
              "Order Update Date": safe_get(salesbyids, ['updateDate']),
              "Rate Usd Transactional": safe_get(salesbyids, ['rateUsdTransactional']),
              "Sales Rep Name": salesResponse,
              "Shipping Country": safe_get(salesbyids, ['address', 0, 'country']),
              "Source System Status": safe_get(salesbyids, ['sourceSystemId']),
              "Tie Number": safe_get(fulfillmentbyid, ['fulfillments', 0, 'salesOrderLines', 0, 'soLineNum']),
              "Source System Status": statusCode,
              "Si Number": safe_get(fulfillmentbyid, ['fulfillments', 0, 'salesOrderLines', 0, 'siNumber']),
              "Req Ship Code": safe_get(fulfillmentbyid, ['fulfillments', 0, 'shipCode']),
              "Reassigned Ip Date": statusCode,
              "RDD": safe_get(fulfillmentbyid, ['fulfillments', 0, 'revisedDeliveryDate']),
              "Product Lob": safe_get(fulfillmentbyid, ['fulfillments', 0, 'salesOrderLines', 0, 'lob']),
              "Payment Term Code": safe_get(fulfillmentbyid, ['fulfillments', 0, 'paymentTerm']),
              "Ofs Status Code": statusCode,
              "Ofs Status": safe_get(fulfillmentbyid, ['fulfillments', 0, 'sostatushist', 0, 'fulfillmentStsCode']),
              "Fulfillment Status": safe_get(fulfillmentbyid, ['fulfillments', 0, 'sostatushist', 0, 'fulfillmentStsCode']),
              "DomsStatus":statusCode,
              "Company Name": safe_get(salesbyids, ['address', 0, 'companyName']),
              "Contact Type": safe_get(salesbyids, ['address', 0, 'contact', 0, 'contactType']),
              "Special Instruction Id": safe_get(fulfillmentbyid, ['fulfillments', 0, 'salesOrderLines', 0, 'specialinstructions', 0, 'specialInstructionId']),
              "Special Instruction Type": safe_get(fulfillmentbyid, ['fulfillments', 0, 'salesOrderLines', 0, 'specialinstructions', 0, 'specialInstructionType']),
              "Shipping City Code": safe_get(salesbyids, ['address', 0, 'cityCode']),
              "City": safe_get(salesbyids, ['address', 0, 'city']),
              "First Name": safe_get(salesbyids, ['address', 0, 'firstName']),
              "Last Name": safe_get(salesbyids, ['address', 0, 'lastName']),
              "State Code": safe_get(salesbyids, ['address', 0, 'stateCode']),
              "Address Line1": safe_get(salesbyids, ['address', 0, 'addressLine1']),
              "Address Line2": safe_get(salesbyids, ['address', 0, 'addressLine2']),
              "Phone Number": safe_get(salesbyids, ['address', 0, 'phone', 0, 'phoneNumber']),
              "Postal Code": safe_get(salesbyids, ['address', 0, 'postalCode']),
              "Sales Order Id": safe_get(salesorder, ['salesOrder', 'salesOrderId']),
              "Fulfillment Id": safe_get(salesorder, ['fulfillment', 0, 'fulfillmentId']),
              "Region Code": safe_get(salesorder, ['salesOrder', 'region']),
              "FoId": safe_get(salesorder, ['fulfillmentOrders', 0, 'foId']),
              "woId": safe_get(salesorder, ['workOrders', 0, 'woId']),
              "System Qty": safe_get(fulfillmentbyid, ['fulfillments', 0, 'systemQty']),
              "Ship By Date": safe_get(fulfillmentbyid, ['fulfillments', 0, 'shipByDate']),
              "LOB": safe_get(fulfillmentbyid, ['fulfillments', 0, 'salesOrderLines', 0, 'lob']),
              "Ship From Facility": safe_get(salesorder, ['asnNumbers', 0, 'shipFrom']),
              "Ship To Facility": safe_get(salesorder, ['asnNumbers', 0, 'shipTo']),
              "Facility": safe_get(fulfillmentbyid, ['fulfillments', 0, 'salesOrderLines', 0, 'facility']),
              "ASN Number": safe_get(salesorder, ['asnNumbers', 0, 'snNumber']),
              "Tax Regstrn Num": safe_get(fulfillmentbyid, ['fulfillments', 0, 'address', 0, 'taxRegstrnNum']),
              "Address Line1": safe_get(fulfillmentbyid, ['fulfillments', 0, 'address', 0, 'addressLine1']),
              "Postal Code": safe_get(fulfillmentbyid, ['fulfillments', 0, 'address', 0, 'postalCode']),
              "State Code": safe_get(fulfillmentbyid, ['fulfillments', 0, 'address', 0, 'stateCode']),
              "City Code": safe_get(fulfillmentbyid, ['fulfillments', 0, 'address', 0, 'cityCode']),
              "Customer Num": safe_get(fulfillmentbyid, ['fulfillments', 0, 'address', 0, 'customerNum']),
              "Customer Name Ext": safe_get(fulfillmentbyid, ['fulfillments', 0, 'address', 0, 'customerNameExt']),
              "Country": safe_get(fulfillmentbyid, ['fulfillments', 0, 'address', 0, 'country']),
              "Create Date": dateFormation(safe_get(fulfillmentbyid, ['fulfillments', 0, 'address', 0, 'createDate'])),
              "Ship Code": safe_get(fulfillmentbyid, ['fulfillments', 0, 'shipCode']),
              "Must Arrive By Date": dateFormation(safe_get(fulfillmentbyid, ['fulfillments', 0, 'mustArriveByDate'])),
              "Update Date": dateFormation(safe_get(fulfillmentbyid, ['fulfillments', 0, 'updateDate'])),
              "Merge Type": safe_get(fulfillmentbyid, ['fulfillments', 0, 'mergeType']),
              "Manifest Date": dateFormation(safe_get(fulfillmentbyid, ['fulfillments', 0, 'manifestDate'])),
              "Revised Delivery Date": dateFormation(safe_get(fulfillmentbyid, ['fulfillments', 0, 'revisedDeliveryDate'])),
              "Delivery City": safe_get(fulfillmentbyid, ['fulfillments', 0, 'deliveryCity']),
              "Source System Id": safe_get(fulfillmentbyid, ['sourceSystemId']),
              "IsDirect Ship": "",
              "SSC": "",
              "OIC Id": safe_get(fulfillmentbyid, ['fulfillments', 0, 'oicId']),
              "Order Date": dateFormation(safe_get(salesorder, ['salesOrder', 'createDate']))
          }
           
          flat_list.append(base_row)
        except Exception as inner_e:
            print(f"[ERROR] OutputFormat inner loop at index ")
            traceback.print_exc()
            continue

      if len(flat_list) > 0:
          if format_type == "export":
            return flat_list
          elif format_type == "grid":
            desired_order = [
                'BUID','PP Date','IP Date','MN Date','SC Date','CFI Flag','Agreement Id','Amount','Currency Code','Customer Po Number','Dp Id',
                'Location Number','Order Age','Order Amount usd','Order Update Date','Rate Usd Transactional','Sales Rep Name','Shipping Country',
                'Source System Status','Tie Number','Si Number','Req Ship Code','Reassigned Ip Date','RDD','Product Lob','Payment Term Code',
                'Ofs Status Code','Ofs Status','Fulfillment Status','DomsStatus','Company Name','Contact Type','Special Instruction Id',
                'Special Instruction Type','Shipping City Code','City','First Name','Last Name','State Code','Address Line1','Address Line2',
                'Phone Number','Postal Code','Sales Order Id','Fulfillment Id','Region Code','FoId','woId','System Qty','Ship By Date','LOB',
                'Ship From Facility','Ship To Facility','Facility','ASN Number','Tax Regstrn Num','Address Line1','Postal Code','State Code',
                'City Code','Customer Num','Customer Name Ext','Country','Create Date','Ship Code','Must Arrive By Date','Update Date',
                'Merge Type','Manifest Date','Revised Delivery Date','Delivery City','Source System Id','IsDirect Ship','SSC',
                'Vendor Work Order Num','Channel Status Code','Ismultipack','Ship Mode','Is Otm Enabled','OIC Id', 'Order Date'
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
