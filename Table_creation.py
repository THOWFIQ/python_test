import json
import oracledb
from cryptography.fernet import Fernet
import datetime

# === Step 1: Sample Data ===
data = {
    "id": "412341234",
    "region": "EMEA",
    "format_type": "grid",
    "userName": "Amit_Chandra",
    "userEmail": "Amit_Chandra@dellteam.com",
    "filters": {
        "Sales_Order_id": "1004543337,1004543337",
        "foid": "FO999999,1004543337",
        "Fullfillment Id": "262135,262136",
        "wo_id": "7360928459,7360928460",
        "Sales_order_ref": "REF123456",
        "Order_create_date": "2025-07-15",
        "ISMULTIPACK": "Yes",
        "BUID": "202",
        "Facility": "WH_BANGALORE",
        "Manifest_ID": "MANI0001",
        "order_date": "2025-07-15"
    },
    "templatename": "asdf",
    "workorderid": "787",
    "shared": True,
    "sharedUserName": "asdf",
    "columns": [
        {
            "checked": True,
            "group": "ID",
            "isPrimary": False,
            "sortBy": "ascending",
            "value": "FoId"
        }
    ]
}

# === Step 2: Decrypt Oracle Credentials ===
with open(r'./config/config_ge4.json') as f:
    config = json.load(f)
    encrypted_username = config['username'].encode()
    encrypted_password = config['password'].encode()
    tns = config['tns']
    key = config['key']

    cipher_suite = Fernet(key)
    decrypted_username = cipher_suite.decrypt(encrypted_username).decode()
    decrypted_password = cipher_suite.decrypt(encrypted_password).decode()

print("Decrypted credentials successfully.")

# === Step 3: Connect to Oracle ===
connection = oracledb.connect(user=decrypted_username, password=decrypted_password, dsn=tns)
cursor = connection.cursor()

# === Step 4: Create Table ===
table_name = "TEMPLATE_DATA"
try:
    cursor.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
except oracledb.DatabaseError:
    pass

create_table_sql = f"""
CREATE TABLE {table_name} (
    ID IS A PRIMARY KEY
    TEMPLATE_ID VARCHAR2(100),
    REGION VARCHAR2(50),
    FORMAT_TYPE VARCHAR2(50),
    USERNAME VARCHAR2(100),
    USEREMAIL VARCHAR2(200),
    FILTERS CLOB,
    TEMPLATENAME VARCHAR2(100),
    WORKORDERID VARCHAR2(100),
    SHARED NUMBER(1),
    SHAREDUSERNAME VARCHAR2(100),
    COLUMNS CLOB,
    CREATED_AT DATE DEFAULT SYSDATE
)
"""
cursor.execute(create_table_sql)
print(f"Created table {table_name}")

# === Step 5: Prepare Insert ===
insert_sql = f"""
INSERT INTO {table_name} (
    ID, REGION, FORMAT_TYPE, USERNAME, USEREMAIL,
    FILTERS, TEMPLATENAME, WORKORDERID, SHARED,
    SHAREDUSERNAME, COLUMNS
) VALUES (
    :id, :region, :format_type, :userName, :userEmail,
    :filters, :templatename, :workorderid, :shared,
    :sharedUserName, :columns
)
"""

bind_data = {
    "id": data["id"],
    "region": data["region"],
    "format_type": data["format_type"],
    "userName": data["userName"],
    "userEmail": data["userEmail"],
    "filters": json.dumps(data["filters"]),
    "templatename": data["templatename"],
    "workorderid": data["workorderid"],
    "shared": 1 if data["shared"] else 0,
    "sharedUserName": data["sharedUserName"],
    "columns": json.dumps(data["columns"])
}

cursor.execute(insert_sql, bind_data)
connection.commit()
print("Data inserted successfully.")

# === Cleanup ===
cursor.close()
connection.close()
