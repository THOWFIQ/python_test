def DownloadOrderEnvelopeReport(filters, region, endPoint):
    path = getPath(region)
    url = path.get('OMTC') + endPoint

    if not url:
        return make_response(jsonify({"error": "Invalid region or URL not configured"}), 400)

    if not isinstance(filters, dict):
        return make_response(jsonify({"error": "Filters must be a dictionary"}), 400)

    from_date = filters.get("FromDate")
    to_date = filters.get("ToDate")

    if not from_date or not to_date:
        return make_response(jsonify({"error": "FromDate and ToDate are required in filters"}), 400)

    payload = {
        "FromDate": from_date,
        "ToDate": to_date,
        "Region": region
    }

    try:
        response = requests.post(url, json=payload, verify=cert_path)
        response.raise_for_status()
        api_data = response.json()

        results = api_data.get("Results", [])
        count = len(results)

        # ---------------------- COLLECT ALL UNIQUE KEYS ----------------------
        all_keys = set()

        for rec in results:
            # top-level keys
            for k in rec.keys():
                if k not in ("Envelopes", "Fulfillments"):
                    all_keys.add(k)

            # envelope keys
            for env in rec.get("Envelopes", []):
                for k in env.keys():
                    all_keys.add(f"Envelope_{k}")

            # fulfillment keys
            for fu in rec.get("Fulfillments", []):
                for k in fu.keys():
                    all_keys.add(f"Fulfillment_{k}")

        all_keys = list(all_keys)

        # ---------------------- BUILD COLUMN DEFINITIONS ----------------------
        columns = []
        for key in all_keys:
            columns.append({
                "checked": False,
                "group": "Data",
                "isPrimary": False,
                "sortBy": "ascending",
                "value": key
            })

        # ---------------------- BUILD DATA ROWS ----------------------
        data_rows = []

        for rec in results:
            row_values = {}

            # top-level fields
            for k in rec.keys():
                if k not in ("Envelopes", "Fulfillments"):
                    row_values[k] = rec.get(k, "")

            # envelope fields (flattened)
            for env in rec.get("Envelopes", []):
                for k, v in env.items():
                    row_values[f"Envelope_{k}"] = v

            # fulfillment fields (flattened)
            for fu in rec.get("Fulfillments", []):
                for k, v in fu.items():
                    row_values[f"Fulfillment_{k}"] = v

            # build final "columns" array
            row = {
                "columns": [{"value": row_values.get(col, "")} for col in all_keys]
            }

            data_rows.append(row)

        # ---------------------- FINAL OUTPUT ----------------------
        final_output = {
            "Count": count,
            "columns": columns,
            "data": data_rows
        }

        return make_response(json.dumps(final_output, indent=4, ensure_ascii=False),
                             200,
                             {"Content-Type": "application/json; charset=utf-8"}
                             )

    except requests.exceptions.RequestException as e:
        return make_response(jsonify({"error": str(e)}), 500)
