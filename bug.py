@app.route('/GetTemplates', methods=['GET'])
def get_templates():
    if getattr(g, 'skip_logs', False):
        return jsonify({'message': 'Logs skipped'})
    g.skip_logs = True

    region = request.args.get('region')
    template_id = request.args.get('id')

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

        print("Running SQL:", query)
        print("With Bind Params:", bind_params)

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
                "columns": columns_json,
                "logs": {
                    "urls": [],
                    "time": []
                }
            })

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        connection.close()

    return jsonify(result)
