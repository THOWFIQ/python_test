@app.route('/GetTemplates', methods=['GET'])
def get_templates():
    if getattr(g, 'skip_logs', False):
        return jsonify({'message': 'Logs skipped'})
    g.skip_logs = True

    region = request.args.get('region')
    template_id = request.args.get('template_id')

    connection = db.get_db_connection(env)
    if not connection:
        return jsonify({'error': 'Failed to connect to the database'}), 500

    cursor = connection.cursor()
    result = []

    try:
        query = """
        SELECT TEMPLATE_ID, USERDATA, REGION, FORMAT_TYPE, 
               FILTERS, TEMPLATENAME, WORKORDERID, SHARED,
               SHAREDUSERNAME, COLUMNS
        FROM TEMPLATE_DATA
        """
        filters = []
        bind_params = {}

        if region:
            filters.append("REGION = :region")
            bind_params["region"] = region
        if template_id:
            filters.append("TEMPLATE_ID = :template_id")
            bind_params["template_id"] = template_id

        if filters:
            query += " WHERE " + " AND ".join(filters)
            
        cursor.execute(query, bind_params)
        rows = cursor.fetchall()

        for row in rows:
            try:
                userdata_clob = row[1]
                userdata_str = userdata_clob.read() if hasattr(userdata_clob, 'read') else userdata_clob
                userdata_json = json.loads(userdata_str) if userdata_str else {}
            except Exception as e:
                print("Failed to parse userdata:", e)
                userdata_json = {}

            try:
                filters_clob = row[4]
                filters_str = filters_clob.read() if hasattr(filters_clob, 'read') else filters_clob
                filters_json = json.loads(filters_str) if filters_str else {}
            except Exception as e:
                print("Failed to parse filters:", e)
                filters_json = {}

            try:
                columns_clob = row[9]
                columns_str = columns_clob.read() if hasattr(columns_clob, 'read') else columns_clob
                columns_json = json.loads(columns_str) if columns_str else []
            except Exception as e:
                print("Failed to parse columns:", e)
                columns_json = []

            result.append({
                "template_id": row[0],
                "userdata": userdata_json,
                "region": row[2],
                "format_type": row[3],
                "filters": filters_json,
                "templatename": row[5],
                "workorderid": row[6],
                "shared": bool(row[7]),
                "sharedUserName": row[8],
                "columns": columns_json
            })

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        connection.close()

    return jsonify(result)

@app.route('/SaveTemplate', methods=['POST'])
def save_template():
    data = request.get_json()

    connection = db.get_db_connection(env)
    if not connection:
        return jsonify({'error': 'Failed to connect to database'}), 500

    cursor = connection.cursor()
    try:
        insert_sql = """
        INSERT INTO TEMPLATE_DATA (
            TEMPLATE_ID, REGION, FORMAT_TYPE, USERNAME, USEREMAIL,
            FILTERS, TEMPLATENAME, WORKORDERID, SHARED,
            SHAREDUSERNAME, COLUMNS
        ) VALUES (
            :template_id, :region, :format_type, :userName, :userEmail,
            :filters, :templatename, :workorderid, :shared,
            :sharedUserName, :columns
        )
        """

        bind_data = {
            "template_id": data.get("template_id"),
            "region": data.get("region"),
            "format_type": data.get("format_type"),
            "userName": data.get("userName"),
            "userEmail": data.get("userEmail"),
            "filters": json.dumps(data.get("filters", {})),
            "templatename": data.get("templatename"),
            "workorderid": data.get("workorderid"),
            "shared": 1 if data.get("shared") else 0,
            "sharedUserName": data.get("sharedUserName"),
            "columns": json.dumps(data.get("columns", []))
        }

        cursor.execute(insert_sql, bind_data)
        connection.commit()
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        connection.close()

    return jsonify({'message': 'Template saved successfully'})

@app.route('/EditTemplate/<template_id>', methods=['PUT'])
def edit_template(template_id):
    data = request.get_json()

    connection = db.get_db_connection(env=DEV)
    if not connection:
        return jsonify({'error': 'Failed to connect to database'}), 500

    cursor = connection.cursor()
    try:
        update_sql = """
        UPDATE TEMPLATE_DATA SET
            REGION = :region,
            FORMAT_TYPE = :format_type,
            USERNAME = :userName,
            USEREMAIL = :userEmail,
            FILTERS = :filters,
            TEMPLATENAME = :templatename,
            WORKORDERID = :workorderid,
            SHARED = :shared,
            SHAREDUSERNAME = :sharedUserName,
            COLUMNS = :columns
        WHERE TEMPLATE_ID = :template_id
        """

        bind_data = {
            "region": data.get("region"),
            "format_type": data.get("format_type"),
            "userName": data.get("userName"),
            "userEmail": data.get("userEmail"),
            "filters": json.dumps(data.get("filters", {})),
            "templatename": data.get("templatename"),
            "workorderid": data.get("workorderid"),
            "shared": 1 if data.get("shared") else 0,
            "sharedUserName": data.get("sharedUserName"),
            "columns": json.dumps(data.get("columns", [])),
            "template_id": template_id
        }

        cursor.execute(update_sql, bind_data)
        connection.commit()
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        connection.close()

    return jsonify({'message': 'Template updated successfully'})

@app.route('/DeleteTemplate/<template_id>', methods=['DELETE'])
def delete_template(template_id):
    connection = db.get_db_connection(env=DEV)
    if not connection:
        return jsonify({'error': 'Failed to connect to database'}), 500

    cursor = connection.cursor()
    try:
        delete_sql = "DELETE FROM TEMPLATE_DATA WHERE TEMPLATE_ID = :template_id"
        cursor.execute(delete_sql, {"template_id": template_id})
        connection.commit()
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        connection.close()

    return jsonify({'message': f'Template with TEMPLATE_ID {template_id} deleted successfully'})

{
    "template_id": "23478254",
    "region": "DAO",
    "format_type": "Export",
	"userName": "Amit_Chandra",
	"userEmail": "dellteams@dellteam.com",
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
    "shared": true,
    "sharedUserName": "asdf",
    "columns": [
      {
        "checked": true,
        "group": "ID",
        "isPrimary": false,
        "sortBy": "ascending",
        "value": "FoId"
      }
    ]
  }
