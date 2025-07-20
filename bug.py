foid_data print(f"[WARN] Missing SO headers or sales orders at row {so_index}")
              ^^^^^
SyntaxError: invalid syntax

(.venv) C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction>python test.py
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\test.py", line 229
    print(f"[DEBUG] so_data at row {so_index}: {json.dumps(so_data, indent=2)}")
IndentationError: unexpected indent

(.venv) C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction>python test.py
[ERROR] formatting row 0: 0
Traceback (most recent call last):
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\test.py", line 242, in OutputFormat
    salesorder = get_salesorders[0]
                 ~~~~~~~~~~~~~~~^^^
KeyError: 0
[ERROR] formatting row 1: 0
Traceback (most recent call last):
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\test.py", line 242, in OutputFormat
    salesorder = get_salesorders[0]
                 ~~~~~~~~~~~~~~~^^^
KeyError: 0
[ERROR] formatting row 2: 0
Traceback (most recent call last):
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\test.py", line 242, in OutputFormat
    salesorder = get_salesorders[0]
                 ~~~~~~~~~~~~~~~^^^
KeyError: 0
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
