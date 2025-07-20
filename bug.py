[ERROR] formatting row 0: 'str' object has no attribute 'get'
Traceback (most recent call last):
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\test.py", line 306, in OutputFormat
    sn_numbers = wo.get("SN Number", [])
                 ^^^^^^
AttributeError: 'str' object has no attribute 'get'
[ERROR] formatting row 1: 'list' object has no attribute 'get'
Traceback (most recent call last):
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\test.py", line 255, in OutputFormat
    forderline = (fulfillment.get("salesOrderLines") or [{}])[0]
                  ^^^^^^^^^^^^^^^
AttributeError: 'list' object has no attribute 'get'
[ERROR] formatting row 2: 'str' object has no attribute 'get'
Traceback (most recent call last):
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\test.py", line 306, in OutputFormat
    sn_numbers = wo.get("SN Number", [])
                 ^^^^^^
AttributeError: 'str' object has no attribute 'get'
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
