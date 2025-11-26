
same like following sample format code and response i need this following DownloadOrderEnvelopeReport
 
def DownloadOrderEnvelopeReport(filters, region, endPoint):
    path = getPath(region)
    url = path.get('OMTC') + endPoint

    if not url:
        return make_response(jsonify({"error": "Invalid region or URL not configured"}), 400)

    if not isinstance(filters, dict):
        return make_response(jsonify({"error": "Filters must be a dictionary"}), 400)

    from_date = filters.get("FromDate")
    to_date = filters.get("ToDate")

    if not from_date or not to_date:
        return make_response(jsonify({"error": "FromDate and ToDate are required in filters"}), 400)

    payload = {
        "FromDate": from_date,
        "ToDate": to_date,
        "Region": region
    }
  

    try:
        response = requests.post(url, json=payload, verify=cert_path)
        response.raise_for_status()
        data = response.json()
        
        return make_response(json.dumps(data, indent=4, sort_keys=False, ensure_ascii=False),
                             200,
                             {"Content-Type": "application/json; charset=utf-8"}
                             )

    except requests.exceptions.RequestException as e:
        return make_response(jsonify({"error": str(e)}), 500)

response is 

{
    "Results": [
        {
            "SONumber": "4447347893",
            "OICId": "6f9e5526-30e4-42f3-a8d3-fc6489dc1f4b",
            "Envelopes": [
                {
                    "EnvelopeId": "22186",
                    "OICId": "6f9e5526-30e4-42f3-a8d3-fc6489dc1f4b",
                    "SONumber": "4447347893",
                    "MessageType": "PREGSONOTIFICATION",
                    "Direction": "IN",
                    "Channel": "OIC",
                    "Status": "CMP",
                    "EnvMsgTextId": "22046",
                    "TraceId": "76532dca-bbe5-4800-ac2c-515b3cd163f9",
                    "CreateDate": "2025-11-06T07:23:40.801947",
                    "ModifyDate": "2025-11-06T07:23:40.801947",
                    "CreateBy": "PreGSONotifyReceiver",
                    "ModifyBy": "PreGSONotifyReceiver",
                    "Region": "EMEA",
                    "PartitionDate": "2025-11-06T07:23:40.801774",
                    "IsPregSO": "Y",
                    "StartProcTime": null,
                    "TryCount": null,
                    "ErrorCode": "",
                    "ErrorDescription": "",
                    "ProcessingBy": ""
                },
                {
                    "EnvelopeId": "22191",
                    "OICId": "6f9e5526-30e4-42f3-a8d3-fc6489dc1f4b",
                    "SONumber": "4447347893",
                    "MessageType": "NETWORKPLANRESP",
                    "Direction": "OUT",
                    "Channel": "OIC",
                    "Status": "CMP",
                    "EnvMsgTextId": "22051",
                    "TraceId": "76532dca-bbe5-4800-ac2c-515b3cd163f9",
                    "CreateDate": "2025-11-06T07:24:23.269475",
                    "ModifyDate": "2025-11-06T07:24:23.269475",
                    "CreateBy": "NetworkPlanRespProcess",
                    "ModifyBy": "NetworkPlanRespProcess",
                    "Region": "EMEA",
                    "PartitionDate": "2025-11-06T07:24:23.27591",
                    "IsPregSO": "Y",
                    "StartProcTime": "2025-11-06T07:24:23.27591",
                    "TryCount": null,
                    "ErrorCode": "",
                    "ErrorDescription": "",
                    "ProcessingBy": ""
                }
            ],
            "Fulfillments": [
                {
                    "FulfillmentMessageId": "",
                    "FulfillmentId": "",
                    "OICId": "",
                    "MessageType": "",
                    "Status": "",
                    "Direction": "",
                    "Exception": "",
                    "ChannelCode": "",
                    "FulfillmentMsgTextId": "",
                    "Region": "",
                    "CreateDate": null,
                    "ModifyDate": null,
                    "CreateBy": "",
                    "ModifyBy": "",
                    "PartitionDate": null,
                    "SONumber": "",
                    "WOId": "",
                    "StartProcTime": null,
                    "TryCount": null,
                    "FilePath": "",
                    "ErrorCode": "",
                    "ErrorDescription": "",
                    "ProcessingBy": ""
                }
            ]
        }
    ]
}

======================================================================


This is sample format 

elif format_type == "grid":
                desired_order = [
                                    "Fulfillment ID", "BUID", "BillingCustomerName", "CustomerName", "LOB", "Sales Order ID", "Agreement ID",
                                    "Amount", "Currency Code", "Customer Po Number", "Delivery City", "DOMS Status", "Dp ID", "Fulfillment Status",
                                    "Merge Type", "InstallInstruction2", "PP Date", "IP Date", "MN Date", "SC Date", "Location Number", "OFS Status Code",
                                    "OFS Status", "ShippingCityCode", "ShippingContactName", "ShippingCustName", "ShippingStateCode", "ShipToAddress1",
                                    "ShipToAddress2", "ShipToCompany", "ShipToPhone", "ShipToPostal", "Order Age", "Order Amount usd", "Rate Usd Transactional",
                                    "Sales Rep Name", "Shipping Country", "Source System Status", "Tie Number", "Si Number", "Req Ship Code", "Reassigned IP Date",
                                    "Payment Term Code", "Region Code", "FO ID", "System Qty", "Ship By Date", "Facility", "Tax Regstrn Num",
                                    "State Code", "Customer Num", "Country", "Ship Code", "Must Arrive By Date", "Manifest Date", "Revised Delivery Date",
                                    "Source System ID", "OIC ID", "Order Date", "Order Type", "Work Order ID", "Dell Blanket PO Num", "Ship To Facility",
                                    "Is Last Leg", "Ship From MCID", "Ship To MCID", "WO OTM Enabled", "WO Ship Mode", "Is Multipack", "Has Software",
                                    "Make WO Ack Date", "MCID Value", "Merge Facility", "ASN", "Destination", "Manifest ID", "Origin",
                                    "Way Bill Number", "Actual Ship Mode", "Actual Ship Code", "Order Vol Wt", "PP ID", "SVC Tag", "Target Delivery Date",
                                    "Total Box Count", "Total Gross Weight", "Total Volumetric Weight"
                                ]

                rows = []
                count =  len(ValidCount)
                for item in flat_list:
                    row = {"columns": [{"value": item.get(k, "")} for k in desired_order]}
                    rows.append(row)
                table_grid_output = tablestructural(rows, region) if rows else []


def tablestructural(data,IsPrimary):
    table_structure = {
        "columns": [
                    {"value": "Fulfillment ID", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ","EMEA","DAO","AMER","LA"], "group": "ID", "checked": IsPrimary in ["APJ","EMEA","DAO","AMER","LA"]},
                    {"value": "BUID", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ","EMEA","DAO","AMER","LA"], "group": "ID", "checked": IsPrimary in ["APJ","EMEA","DAO","AMER","LA"]},
                    {"value": "Billing Customer Name", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Address", "checked": IsPrimary in []},
                    {"value": "Customer Name", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Other", "checked": IsPrimary in []},
                    {"value": "LOB", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ","EMEA"], "group": "Other", "checked": IsPrimary in ["APJ","EMEA"]},
                    {"value": "Sales Order ID", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ","EMEA","DAO","AMER","LA"], "group": "ID", "checked": IsPrimary in ["APJ","EMEA","DAO","AMER","LA"]},
                    {"value": "Agreement ID", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Amount", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Currency Code", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Customer Po Number", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Delivery City", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Address", "checked": IsPrimary in []},
                    {"value": "DOMS Status", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "DP ID", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Fulfillment Status", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Merge Type", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Type", "checked": IsPrimary in []},
                    {"value": "Install Instruction 2", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Other", "checked": IsPrimary in []},
                    {"value": "PP Date", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ","EMEA","AMER"], "group": "Date", "checked": IsPrimary in ["APJ","EMEA"]},
                    {"value": "IP Date", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ","EMEA","AMER"], "group": "Date", "checked": IsPrimary in ["APJ","EMEA"]},
                    {"value": "MN Date", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "SC Date", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Location Number", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "OFS Status Code", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "OFS Status", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Shipping City Code", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Address", "checked": IsPrimary in []},
                    {"value": "Shipping Contact Name", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Address", "checked": IsPrimary in []},
                    {"value": "Shipping Cust Name", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Address", "checked": IsPrimary in []},
                    {"value": "Shipping State Code", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Code", "checked": IsPrimary in []},
                    {"value": "Ship To Address1", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Address", "checked": IsPrimary in []},
                    {"value": "Ship To Address2", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Address", "checked": IsPrimary in []},
                    {"value": "Ship To Company", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Address", "checked": IsPrimary in []},
                    {"value": "Ship To Phone", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Contact", "checked": IsPrimary in []},
                    {"value": "Ship To Postal", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Address", "checked": IsPrimary in []},
                    {"value": "Order Age", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Order Amount USD", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Rate USD Transactional", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Sales Rep Name", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Shipping Country", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Source System Status", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Tie Number", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Si Number", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Req Ship Code", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Reassigned IP Date", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Payment Term Code", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Region Code", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Code", "checked": IsPrimary in []},
                    {"value": "FO ID", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "System Qty", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ","EMEA"], "group": "Other", "checked": IsPrimary in ["APJ","EMEA"]},
                    {"value": "Ship By Date", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ","EMEA","DAO","AMER","LA"], "group": "Date", "checked": IsPrimary in ["APJ","EMEA","DAO","AMER","LA"]},
                    {"value": "Facility", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ","EMEA","DAO","AMER","LA"], "group": "Facility", "checked": IsPrimary in ["APJ","EMEA","DAO","AMER","LA"]},
                    {"value": "Tax Regstrn Num", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Other", "checked": IsPrimary in []},
                    {"value": "State Code", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Code", "checked": IsPrimary in []},
                    # {"value": "City Code", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Address", "checked": IsPrimary in []},
                    {"value": "Customer Num", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Other", "checked": IsPrimary in []},
                    # {"value": "Customer Name Ext", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Other", "checked": IsPrimary in []},
                    {"value": "Country", "sortBy": "ascending", "isPrimary": IsPrimary in ['APJ'], "group": "Address", "checked": IsPrimary in ['APJ']},
                    {"value": "Ship Code", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Code", "checked": IsPrimary in []},
                    {"value": "Must Arrive By Date", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Manifest Date", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Revised Delivery Date", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Date", "checked": IsPrimary in []},
                    {"value": "Source System ID", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "OIC ID", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Order Date", "sortBy": "ascending", "isPrimary": IsPrimary in ["APJ","DAO","AMER","LA"], "group": "Date", "checked": IsPrimary in ["APJ","DAO","AMER","LA"]},
                    {"value": "Order Type", "sortBy": "ascending", "isPrimary": IsPrimary in ["DAO","AMER"], "group": "Type", "checked": IsPrimary in ["DAO","AMER"]},
                    {"value": "Work Order ID", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Dell Blanket Po Num", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Ship To Facility", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Facility", "checked": IsPrimary in []},
                    {"value": "Is Last Leg", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},                    
                    {"value": "Ship From MCID", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Ship To Mcid", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "WO OTM Enable", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Wo Ship Mode", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Is Multi Pack", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "Type", "checked": IsPrimary in []},
                    {"value": "Has Software", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Make WO Ack Date", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "MCID Value", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Merge Facility", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "ASN", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Destination", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Manifest ID", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Origin", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Way Bill Number", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Actual Ship Mode", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Actual Ship Code", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Order Vol Wt", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "PP ID", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "SVC Tag", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Target Delivery Date", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Total Box Count", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Total Gross Weight", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []},
                    {"value": "Total Volumetric Weight", "sortBy": "ascending", "isPrimary": IsPrimary in [], "group": "ID", "checked": IsPrimary in []}
                ],
        "data": []
    }
    table_structure["data"].extend(data)
    return table_structure


response  

{
    "Count": 1,
    "columns": [
        {
            "checked": true,
            "group": "ID",
            "isPrimary": true,
            "sortBy": "ascending",
            "value": "Fulfillment ID"
        },
        {
            "checked": true,
            "group": "ID",
            "isPrimary": true,
            "sortBy": "ascending",
            "value": "BUID"
        },
        {
            "checked": false,
            "group": "Address",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Billing Customer Name"
        },
        {
            "checked": false,
            "group": "Other",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Customer Name"
        },
        {
            "checked": true,
            "group": "Other",
            "isPrimary": true,
            "sortBy": "ascending",
            "value": "LOB"
        },
        {
            "checked": true,
            "group": "ID",
            "isPrimary": true,
            "sortBy": "ascending",
            "value": "Sales Order ID"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Agreement ID"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Amount"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Currency Code"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Customer Po Number"
        },
        {
            "checked": false,
            "group": "Address",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Delivery City"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "DOMS Status"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "DP ID"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Fulfillment Status"
        },
        {
            "checked": false,
            "group": "Type",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Merge Type"
        },
        {
            "checked": false,
            "group": "Other",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Install Instruction 2"
        },
        {
            "checked": true,
            "group": "Date",
            "isPrimary": true,
            "sortBy": "ascending",
            "value": "PP Date"
        },
        {
            "checked": true,
            "group": "Date",
            "isPrimary": true,
            "sortBy": "ascending",
            "value": "IP Date"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "MN Date"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "SC Date"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Location Number"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "OFS Status Code"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "OFS Status"
        },
        {
            "checked": false,
            "group": "Address",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Shipping City Code"
        },
        {
            "checked": false,
            "group": "Address",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Shipping Contact Name"
        },
        {
            "checked": false,
            "group": "Address",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Shipping Cust Name"
        },
        {
            "checked": false,
            "group": "Code",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Shipping State Code"
        },
        {
            "checked": false,
            "group": "Address",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Ship To Address1"
        },
        {
            "checked": false,
            "group": "Address",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Ship To Address2"
        },
        {
            "checked": false,
            "group": "Address",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Ship To Company"
        },
        {
            "checked": false,
            "group": "Contact",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Ship To Phone"
        },
        {
            "checked": false,
            "group": "Address",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Ship To Postal"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Order Age"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Order Amount USD"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Rate USD Transactional"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Sales Rep Name"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Shipping Country"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Source System Status"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Tie Number"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Si Number"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Req Ship Code"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Reassigned IP Date"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Payment Term Code"
        },
        {
            "checked": false,
            "group": "Code",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Region Code"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "FO ID"
        },
        {
            "checked": true,
            "group": "Other",
            "isPrimary": true,
            "sortBy": "ascending",
            "value": "System Qty"
        },
        {
            "checked": true,
            "group": "Date",
            "isPrimary": true,
            "sortBy": "ascending",
            "value": "Ship By Date"
        },
        {
            "checked": true,
            "group": "Facility",
            "isPrimary": true,
            "sortBy": "ascending",
            "value": "Facility"
        },
        {
            "checked": false,
            "group": "Other",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Tax Regstrn Num"
        },
        {
            "checked": false,
            "group": "Code",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "State Code"
        },
        {
            "checked": false,
            "group": "Other",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Customer Num"
        },
        {
            "checked": true,
            "group": "Address",
            "isPrimary": true,
            "sortBy": "ascending",
            "value": "Country"
        },
        {
            "checked": false,
            "group": "Code",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Ship Code"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Must Arrive By Date"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Manifest Date"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Revised Delivery Date"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Source System ID"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "OIC ID"
        },
        {
            "checked": true,
            "group": "Date",
            "isPrimary": true,
            "sortBy": "ascending",
            "value": "Order Date"
        },
        {
            "checked": false,
            "group": "Type",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Order Type"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Work Order ID"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Dell Blanket Po Num"
        },
        {
            "checked": false,
            "group": "Facility",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Ship To Facility"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Is Last Leg"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Ship From MCID"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Ship To Mcid"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "WO OTM Enable"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Wo Ship Mode"
        },
        {
            "checked": false,
            "group": "Type",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Is Multi Pack"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Has Software"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Make WO Ack Date"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "MCID Value"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Merge Facility"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "ASN"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Destination"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Manifest ID"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Origin"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Way Bill Number"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Actual Ship Mode"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Actual Ship Code"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Order Vol Wt"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "PP ID"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "SVC Tag"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Target Delivery Date"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Total Box Count"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Total Gross Weight"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Total Volumetric Weight"
        }
    ],
    "data": [
        {
            "columns": [
                {
                    "value": "3000070327"
                },
                {
                    "value": "10036"
                },
                {
                    "value": "Qantas Airways Limited"
                },
                {
                    "value": "Qantas Airways Limited"
                },
                {
                    "value": "Dell UltraSharp Monitors"
                },
                {
                    "value": "8040070632"
                },
                {
                    "value": ""
                },
                {
                    "value": "11353.88"
                },
                {
                    "value": "AUD"
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": "SC"
                },
                {
                    "value": "8040070632"
                },
                {
                    "value": "Ship Confirm"
                },
                {
                    "value": "PTO-DIRECT"
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": "2025-07-29 07:35:16"
                },
                {
                    "value": "01"
                },
                {
                    "value": "SC"
                },
                {
                    "value": "Ship Confirm"
                },
                {
                    "value": ""
                },
                {
                    "value": "Vinay Kumar"
                },
                {
                    "value": "Qantas Airways Limited"
                },
                {
                    "value": ""
                },
                {
                    "value": "10 Bourke Road"
                },
                {
                    "value": ""
                },
                {
                    "value": "Qantas Airways Limited"
                },
                {
                    "value": "61-02261111818"
                },
                {
                    "value": "2020"
                },
                {
                    "value": "2025-07-29 06:59:04"
                },
                {
                    "value": "0"
                },
                {
                    "value": "0"
                },
                {
                    "value": ""
                },
                {
                    "value": "AU"
                },
                {
                    "value": "SC"
                },
                {
                    "value": "1"
                },
                {
                    "value": ""
                },
                {
                    "value": "IY"
                },
                {
                    "value": "SC"
                },
                {
                    "value": "IMMEDIATE"
                },
                {
                    "value": "APJ"
                },
                {
                    "value": "7355860096558333953"
                },
                {
                    "value": "1"
                },
                {
                    "value": ""
                },
                {
                    "value": "BX2"
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": "D11200523232"
                },
                {
                    "value": "AU"
                },
                {
                    "value": "IY"
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": "DSP"
                },
                {
                    "value": "ZTmNBGxMEfCHJe_Jjx4dAA"
                },
                {
                    "value": "2025-07-29 06:59:04"
                },
                {
                    "value": "Standard Order"
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": "3027522928"
                },
                {
                    "value": ""
                },
                {
                    "value": "3027522928"
                },
                {
                    "value": "BX2"
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": 20.0
                },
                {
                    "value": ""
                },
                {
                    "value": "APJ1538"
                },
                {
                    "value": ""
                },
                {
                    "value": 3
                },
                {
                    "value": 60.0
                },
                {
                    "value": 60.0
                }
            ]
        },
        {
            "columns": [
                {
                    "value": "3000070326"
                },
                {
                    "value": "10036"
                },
                {
                    "value": "Qantas Airways Limited"
                },
                {
                    "value": "Qantas Airways Limited"
                },
                {
                    "value": "Inspiron 14 5440"
                },
                {
                    "value": "8040070632"
                },
                {
                    "value": ""
                },
                {
                    "value": "11353.88"
                },
                {
                    "value": "AUD"
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": "IP"
                },
                {
                    "value": "8040070632"
                },
                {
                    "value": "In Production"
                },
                {
                    "value": "ATO-INDIRECT"
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": "2025-07-29 13:20:07"
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": "01"
                },
                {
                    "value": "IP"
                },
                {
                    "value": "In Production"
                },
                {
                    "value": ""
                },
                {
                    "value": "Vinay Kumar"
                },
                {
                    "value": "Qantas Airways Limited"
                },
                {
                    "value": ""
                },
                {
                    "value": "10 Bourke Road"
                },
                {
                    "value": ""
                },
                {
                    "value": "Qantas Airways Limited"
                },
                {
                    "value": "61-02261111818"
                },
                {
                    "value": "2020"
                },
                {
                    "value": "2025-07-29 06:59:04"
                },
                {
                    "value": "0"
                },
                {
                    "value": "0"
                },
                {
                    "value": ""
                },
                {
                    "value": "AU"
                },
                {
                    "value": "IP"
                },
                {
                    "value": "23"
                },
                {
                    "value": ""
                },
                {
                    "value": "IY"
                },
                {
                    "value": "IP"
                },
                {
                    "value": "IMMEDIATE"
                },
                {
                    "value": "APJ"
                },
                {
                    "value": "7355928803984830465"
                },
                {
                    "value": "5"
                },
                {
                    "value": ""
                },
                {
                    "value": "WCD"
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": "D11200523232"
                },
                {
                    "value": "AU"
                },
                {
                    "value": "IY"
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": "DSP"
                },
                {
                    "value": "_CiIpGxVEfCLGBGxs3YlMQ"
                },
                {
                    "value": "2025-07-29 06:59:04"
                },
                {
                    "value": "Standard Order"
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": "TMAV9890018651"
                },
                {
                    "value": "BX2"
                },
                {
                    "value": "TMAV9890018651"
                },
                {
                    "value": "WCD"
                },
                {
                    "value": "NNN1OV"
                },
                {
                    "value": "AR"
                },
                {
                    "value": ""
                },
                {
                    "value": 34.02
                },
                {
                    "value": "CNJXWXCY172O5012V51N"
                },
                {
                    "value": "8931240"
                },
                {
                    "value": "2025-07-29T13:45:41"
                },
                {
                    "value": 1
                },
                {
                    "value": 34.02
                },
                {
                    "value": 34.02
                }
            ]
        }
    ]
}
