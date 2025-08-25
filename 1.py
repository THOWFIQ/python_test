# Skip certain endpoints
    if request.endpoint in ["healthcheck", "static"]:
        return response
