import json
import oracledb
import pandas as pd
from cryptography.fernet import Fernet

# === Step 1: Read and process CSV ===
csv_path = "filter_data.csv"
df = pd.read_csv(csv_path)

# Clean column names
df.columns = [col.strip().replace(" ", "_").replace("-", "_") for col in df.columns]

# Normalize boolean-like columns
bool_columns = ['default', 'is_active']
for col in bool_columns:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.lower().map({'yes': 1, 'no': 0})

# Add REGION column
regions = ['APJ', 'EMEA', 'DAO', 'AMER', 'LA']
df_expanded = pd.concat([df.assign(REGION=region) for region in regions], ignore_index=True)

# === Step 2: Decrypt DB credentials ===
with open(r'./config/config_ge4.json') as f:
    config = json.load(f)
    encrypted_username = config['username'].encode()
    encrypted_password = config['password'].encode()
    tns = config['tns']
    key = config['key']

    cipher_suite = Fernet(key)
    decrypted_username = cipher_suite.decrypt(encrypted_username).decode()
    decrypted_password = cipher_suite.decrypt(encrypted_password).decode()

print("Decrypted username and password obtained.")

# === Step 3: Connect to Oracle ===
connection = oracledb.connect(user=decrypted_username, password=decrypted_password, dsn=tns)
cursor = connection.cursor()

# Table setup
table_name = "REGION_DATA_FILTERS"
sequence_name = f"{table_name}_SEQ"
trigger_name = f"{table_name}_BI"

# Drop trigger, sequence, table if they exist
for ddl in [f"DROP TRIGGER {trigger_name}", f"DROP SEQUENCE {sequence_name}", f"DROP TABLE {table_name} CASCADE CONSTRAINTS"]:
    try:
        cursor.execute(ddl)
    except oracledb.DatabaseError:
        pass

# === Step 4: Create Table ===
columns_ddl = ['"ID" NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY PRIMARY KEY']
for col in df_expanded.columns:
    if col in bool_columns:
        columns_ddl.append(f'"{col}" NUMBER(1)')
    else:
        columns_ddl.append(f'"{col}" VARCHAR2(4000)')

create_stmt = f'CREATE TABLE {table_name} ({", ".join(columns_ddl)})'
print("Creating table with:", create_stmt)
cursor.execute(create_stmt)

# === Step 5: Insert Data ===
# Exclude ID for insert
columns_to_insert = df_expanded.columns.tolist()
insert_stmt = f'''
    INSERT INTO {table_name} ({", ".join(f'"{col}"' for col in columns_to_insert)})
    VALUES ({", ".join([":" + str(i + 1) for i in range(len(columns_to_insert))])})
'''
print("Insert statement is:", insert_stmt)
cursor.executemany(insert_stmt, df_expanded[columns_to_insert].values.tolist())

# === Step 6: Commit & Cleanup ===
connection.commit()
print(f"Inserted {len(df_expanded)} rows into {table_name}.")
cursor.close()
connection.close()
