[ERROR] OutputFormat failed: 'list' object has no attribute 'get'
Traceback (most recent call last):
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\combination_new.py", line 575, in OutputFormat
    "InstallInstruction2": get_install_instruction2_id(fulfillments_by_id[idx]),
                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\Thowfiq_S\Project\fdhdataservice\OrderLookUpFunction\combination_new.py", line 523, in get_install_instruction2_id
    fulfills = listify(fulfillment_entry.get("fulfillments", []))
                       ^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'list' object has no attribute 'get'
