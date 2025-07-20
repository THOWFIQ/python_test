[ERROR] formatting row 0: 'list' object has no attribute 'get'
Traceback (most recent call last):
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\test.py", line 303, in OutputFormat
    "Ship Code": sofulfillment.get("shipCode"),
                 ^^^^^^^^^^^^^^^^^
AttributeError: 'list' object has no attribute 'get'
[ERROR] formatting row 1: 'list' object has no attribute 'get'
Traceback (most recent call last):
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\test.py", line 303, in OutputFormat
    "Ship Code": sofulfillment.get("shipCode"),
                 ^^^^^^^^^^^^^^^^^
AttributeError: 'list' object has no attribute 'get'
[WARN] Skipping invalid wo entry at row 2: Vendor Work Order Num
[WARN] Skipping invalid wo entry at row 2: Channel Status Code
[WARN] Skipping invalid wo entry at row 2: Ismultipack
[WARN] Skipping invalid wo entry at row 2: Ship Mode
[WARN] Skipping invalid wo entry at row 2: Is Otm Enabled
[WARN] Skipping invalid wo entry at row 2: SN Number
{
  "status": "success",
  "message": "Validation and fetch completed.",
  "result_summary": {
    "Sales_Order_id": "3 response(s)",
    "foid": "1 response(s)",
    "wo_id": "3 response(s)",
    "Fullfillment Id": "2 response(s)"
  },
  "data": []
}
