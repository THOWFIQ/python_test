input given from front end 

output = getbySalesOrderID(
        salesorderid=["1004452326", "1004543337"],
        format_type="export",
        region="EMEA",
        filters={
            "Fullfillment Id": "262135",
            "wo_id ": "7360928459"
        }
    )

out getting like this 

[{'soHeaderRef': '7336030611445604352', 'buid': 202, 'salesOrderId': '1004543337', 'region': 'England', 'fulfillments': [{'systemQty': None, 'shipByDate': None, 'updateDate': '2025-06-04T09:06:35.542619', 'salesOrderLines': [{'lob': None}]}]}]

but i need 
if grid mean 
{
    "columns": [
        {
            "checked": true,
            "group": "ID",
            "isPrimary": true,
            "sortBy": "ascending",
            "value": "BUID"
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
            "group": "ID",
            "isPrimary": true,
            "sortBy": "ascending",
            "value": "Sales Order Id"
        },
        {
            "checked": true,
            "group": "ID",
            "isPrimary": true,
            "sortBy": "ascending",
            "value": "Fulfillment Id"
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
            "value": "FoId"
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
            "group": "Other",
            "isPrimary": true,
            "sortBy": "ascending",
            "value": "LOB"
        },
        {
            "checked": true,
            "group": "Facility",
            "isPrimary": true,
            "sortBy": "ascending",
            "value": "Ship From Facility"
        },
        {
            "checked": true,
            "group": "Facility",
            "isPrimary": true,
            "sortBy": "ascending",
            "value": "Ship To Facility"
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
            "group": "Address",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Address Line1"
        },
        {
            "checked": false,
            "group": "Address",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Postal Code"
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
            "group": "Address",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "City Code"
        },
        {
            "checked": false,
            "group": "Other",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Customer Num"
        },
        {
            "checked": false,
            "group": "Other",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Customer Name Ext"
        },
        {
            "checked": false,
            "group": "Address",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Country"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Create Date"
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
            "value": "Update Date"
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
            "group": "Address",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Delivery City"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Source System Id"
        },
        {
            "checked": false,
            "group": "Flag",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Is Direct Ship"
        },
        {
            "checked": false,
            "group": "Other",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "SSC"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Vendor Work Order Num"
        },
        {
            "checked": false,
            "group": "Code",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Channel Status Code"
        },
        {
            "checked": false,
            "group": "Flag",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Ismultipack"
        },
        {
            "checked": false,
            "group": "Mode",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Ship Mode"
        },
        {
            "checked": false,
            "group": "Flag",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Is Otm Enabled"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "SN Number"
        },
        {
            "checked": false,
            "group": "ID",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "OIC ID"
        },
        {
            "checked": false,
            "group": "Date",
            "isPrimary": false,
            "sortBy": "ascending",
            "value": "Order Date"
        }
    ],
    "data": [
        {
            "columns": [
                {
                    "value": 202
                },
                {
                    "value": ""
                },
                {
                    "value": "1004543337"
                },
                {
                    "value": "262135"
                },
                {
                    "value": "England"
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
                    "value": "EMFC"
                },
                {
                    "value": "COV"
                },
                {
                    "value": ""
                },
                {
                    "value": "123 Main Street"
                },
                {
                    "value": "10001"
                },
                {
                    "value": "NY"
                },
                {
                    "value": "NYC"
                },
                {
                    "value": "C12345"
                },
                {
                    "value": ""
                },
                {
                    "value": "USA"
                },
                {
                    "value": "2025-06-04T09:06:35.538557"
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": "2025-06-04T09:06:35.542619"
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
                    "value": "OMEGA"
                },
                {
                    "value": ""
                },
                {
                    "value": "SSC001"
                },
                {
                    "value": "7360970693"
                },
                {
                    "value": "400"
                },
                {
                    "value": ""
                },
                {
                    "value": "A"
                },
                {
                    "value": "Y"
                },
                {
                    "value": "CHVSN20250611110608251"
                },
                {
                    "value": ""
                },
                {
                    "value": "2024-12-19T12:34:56"
                }
            ]
        },
        {
            "columns": [
                {
                    "value": 202
                },
                {
                    "value": ""
                },
                {
                    "value": "1004543337"
                },
                {
                    "value": "262135"
                },
                {
                    "value": "England"
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
                    "value": "EMFC"
                },
                {
                    "value": "COV"
                },
                {
                    "value": ""
                },
                {
                    "value": "123 Main Street"
                },
                {
                    "value": "10001"
                },
                {
                    "value": "NY"
                },
                {
                    "value": "NYC"
                },
                {
                    "value": "C12345"
                },
                {
                    "value": ""
                },
                {
                    "value": "USA"
                },
                {
                    "value": "2025-06-04T09:06:35.538557"
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": "2025-06-04T09:06:35.542619"
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
                    "value": "OMEGA"
                },
                {
                    "value": ""
                },
                {
                    "value": "SSC001"
                },
                {
                    "value": "7360928459"
                },
                {
                    "value": "400"
                },
                {
                    "value": ""
                },
                {
                    "value": "A"
                },
                {
                    "value": "Y"
                },
                {
                    "value": "C2ZCWA24JXSZ"
                },
                {
                    "value": ""
                },
                {
                    "value": "2024-12-19T12:34:56"
                }
            ]
        },
        {
            "columns": [
                {
                    "value": 202
                },
                {
                    "value": ""
                },
                {
                    "value": "1004543337"
                },
                {
                    "value": "262135"
                },
                {
                    "value": "England"
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
                    "value": "EMFC"
                },
                {
                    "value": "COV"
                },
                {
                    "value": ""
                },
                {
                    "value": "123 Main Street"
                },
                {
                    "value": "10001"
                },
                {
                    "value": "NY"
                },
                {
                    "value": "NYC"
                },
                {
                    "value": "C12345"
                },
                {
                    "value": ""
                },
                {
                    "value": "USA"
                },
                {
                    "value": "2025-06-04T09:06:35.538557"
                },
                {
                    "value": ""
                },
                {
                    "value": ""
                },
                {
                    "value": "2025-06-04T09:06:35.542619"
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
                    "value": "OMEGA"
                },
                {
                    "value": ""
                },
                {
                    "value": "SSC001"
                },
                {
                    "value": "7360928459"
                },
                {
                    "value": "400"
                },
                {
                    "value": ""
                },
                {
                    "value": "A"
                },
                {
                    "value": "Y"
                },
                {
                    "value": "ORI5H54EGUKU"
                },
                {
                    "value": ""
                },
                {
                    "value": "2024-12-19T12:34:56"
                }
            ]
        }
    ],
    "logs": {
        "urls": [],
        "time": []
    }
}

if export mean

[
    {
        "Address Line1": "123 Main Street",
        "BUID": 202,
        "Channel Status Code": "400",
        "City Code": "NYC",
        "Country": "USA",
        "Create Date": "2025-06-04T09:06:35.538557",
        "Customer NameExt": "John A. Doe",
        "Customer Num": "C12345",
        "Delivery City": "",
        "FOID": "7336030653629440001",
        "Fulfillment Id": "262135",
        "Is Otm Enabled": "Y",
        "IsDirect Ship": "",
        "Ismultipack": "",
        "LOB": "",
        "Manifest Date": "",
        "Merge Type": "",
        "Must Arrive By Date": "",
        "OIC Id": "1eaf6101-b42b-4bc9-9036-98ee27442039",
        "Order Date": "2024-12-19T12:34:56",
        "PP Date": "",
        "Postal Code": "10001",
        "Region Code": "England",
        "Revised Delivery Date": "",
        "SN Number": "CHVSN20250611110608251",
        "SSC": "SSC001",
        "Sales Order Id": "1004543337",
        "Ship By Date": "",
        "Ship Code": "",
        "Ship From Facility": "EMFC",
        "Ship Mode": "A",
        "Ship To Facility": "COV",
        "Source System Id": "OMEGA",
        "State Code": "NY",
        "System Qty": "",
        "Tax Regstrn Num": "TX987654",
        "Update Date": "2025-06-04T09:06:35.542619",
        "Vendor Work Order Num": "7360970693",
        "logs": {
            "urls": [],
            "time": []
        }
    },
    {
        "Address Line1": "123 Main Street",
        "BUID": 202,
        "Channel Status Code": "400",
        "City Code": "NYC",
        "Country": "USA",
        "Create Date": "2025-06-04T09:06:35.538557",
        "Customer NameExt": "John A. Doe",
        "Customer Num": "C12345",
        "Delivery City": "",
        "FOID": "7336030653629440001",
        "Fulfillment Id": "262135",
        "Is Otm Enabled": "Y",
        "IsDirect Ship": "",
        "Ismultipack": "",
        "LOB": "",
        "Manifest Date": "",
        "Merge Type": "",
        "Must Arrive By Date": "",
        "OIC Id": "1eaf6101-b42b-4bc9-9036-98ee27442039",
        "Order Date": "2024-12-19T12:34:56",
        "PP Date": "",
        "Postal Code": "10001",
        "Region Code": "England",
        "Revised Delivery Date": "",
        "SN Number": "C2ZCWA24JXSZ",
        "SSC": "SSC001",
        "Sales Order Id": "1004543337",
        "Ship By Date": "",
        "Ship Code": "",
        "Ship From Facility": "EMFC",
        "Ship Mode": "A",
        "Ship To Facility": "COV",
        "Source System Id": "OMEGA",
        "State Code": "NY",
        "System Qty": "",
        "Tax Regstrn Num": "TX987654",
        "Update Date": "2025-06-04T09:06:35.542619",
        "Vendor Work Order Num": "7360928459"
    },
    {
        "Address Line1": "123 Main Street",
        "BUID": 202,
        "Channel Status Code": "400",
        "City Code": "NYC",
        "Country": "USA",
        "Create Date": "2025-06-04T09:06:35.538557",
        "Customer NameExt": "John A. Doe",
        "Customer Num": "C12345",
        "Delivery City": "",
        "FOID": "7336030653629440001",
        "Fulfillment Id": "262135",
        "Is Otm Enabled": "Y",
        "IsDirect Ship": "",
        "Ismultipack": "",
        "LOB": "",
        "Manifest Date": "",
        "Merge Type": "",
        "Must Arrive By Date": "",
        "OIC Id": "1eaf6101-b42b-4bc9-9036-98ee27442039",
        "Order Date": "2024-12-19T12:34:56",
        "PP Date": "",
        "Postal Code": "10001",
        "Region Code": "England",
        "Revised Delivery Date": "",
        "SN Number": "ORI5H54EGUKU",
        "SSC": "SSC001",
        "Sales Order Id": "1004543337",
        "Ship By Date": "",
        "Ship Code": "",
        "Ship From Facility": "EMFC",
        "Ship Mode": "A",
        "Ship To Facility": "COV",
        "Source System Id": "OMEGA",
        "State Code": "NY",
        "System Qty": "",
        "Tax Regstrn Num": "TX987654",
        "Update Date": "2025-06-04T09:06:35.542619",
        "Vendor Work Order Num": "7360928459"
    }
]
