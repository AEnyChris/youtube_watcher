config = {
    "google_api_key":"Insert google api key",
    "playlist_id":"any playlist id will do",
    "page_token": "page token",
    "kafka":{
        "bootstrap.servers": "insert the link to the server",
        "security.protocol":"sasl_ssl",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "insert key",
        "sasl.password": "insert secret here"
    },
    "schema_registry": {
        "url":"insert schema registry server url",
        "basic.auth.user.info":"insert schema registry key:insert schema registry secret"
    }
}