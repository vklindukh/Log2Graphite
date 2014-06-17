Log2Graphite
============

realtime parse nginx access.log and upload metrics to graphite server.


Supported access.log formats:

1.

    log_format  body  '$remote_addr - $remote_user [$time_local] "$request" '
                      '$status $body_bytes_sent "$request_body" '
                      '"$connection_requests" "$http_connection" '
                      '"$http_user_agent" "$http_x_forwarded_for" "$request_time" "$upstream_response_time" "$pipe"';


2. 

   TODO
