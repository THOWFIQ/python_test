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
        SELECT TEMPLATE_ID, REGION, FORMAT_TYPE, USERNAME, USEREMAIL,
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
                filters_clob = row[5]
                filters_str = filters_clob.read() if hasattr(filters_clob, 'read') else filters_clob
                filters_json = json.loads(filters_str) if filters_str else {}
            except Exception as e:
                print("❌ Failed to parse filters:", e)
                filters_json = {}

            try:
                columns_clob = row[10]
                columns_str = columns_clob.read() if hasattr(columns_clob, 'read') else columns_clob
                columns_json = json.loads(columns_str) if columns_str else []
            except Exception as e:
                print("❌ Failed to parse columns:", e)
                columns_json = []

            result.append({
                "template_id": row[0],
                "region": row[1],
                "format_type": row[2],
                "userName": row[3],
                "userEmail": row[4],
                "filters": filters_json,
                "templatename": row[6],
                "workorderid": row[7],
                "shared": bool(row[8]),
                "sharedUserName": row[9],
                "columns": columns_json,
                "logs": {
                    "urls": [],
                    "time": []
                }
            })

    except Exception as e:
        import traceback
        print("❌ Exception occurred:", str(e))
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

    finally:
        cursor.close()
        connection.close()

    return jsonify(result)
