@app.after_request
def log_response_time(response):
    if hasattr(g, 'start_time'):
        duration = time.time() - g.start_time
        if response.content_type == "application/json":
            data=json.loads(response.get_data())
            if type(data) is list:
                 data[0]["logs"] = {
                                "urls":g.execution_urls,
                                "time":g.execution_times
                            }
            else:
                data["logs"] = {
                                "urls":g.execution_urls,
                                "time":g.execution_times
                            }
            response.set_data(json.dumps(data))
            response.mimetype="application/json"

            logging.info(f"Response Status: {response.status}")
            logging.info(f"Total Response time: {duration: .4f} seconds")

    return response
