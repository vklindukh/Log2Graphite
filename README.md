Log2Graphite
============

access.log parser.
Application parse and upload metrics to graphite server in realtime.
In addition it supports non-realtime operation (reparsing olg logs).


Usage
---------------
```
usage: Log2Graphite -f <filepath> [ -c <config> -h <host> -t <arg> -atime <aggregate_time> -notail -start ]
 -atime <aggregate_time>   aggregate metric timeout in seconds. default is 60
 -c <config>               path to config file
 -f <filepath>             path to log file
 -h <host>                 Graphite host IP
 -notail                   parse single file without tail
 -start                    tail log from start. without option application start tail log file from end
 -t <arg>                  number of parsers. by default is 1 parser
```

Config file format
------------------
```
cat config.properties 
# log_format similar to nginx.conf
#
# escape lines for support multiply line format

# default value
#log_format='$remote_addr - $remote_user [$time_local] "$request" ' \
#         '$status $body_bytes_sent "$http_referer" ' \
#           '"$http_user_agent" "$http_x_forwarded_for" "$request_time" "$upstream_response_time" "$pipe"';

#log_format='$remote_addr - $remote_user [$time_local] "$request" '
#           '$status $body_bytes_sent "$http_referer" '
#           '"$http_user_agent" "$http_x_forwarded_for" "$request_time" "$upstream_response_time" "$pipe"';
```

Default access.log format
-------------------------
```
    '$remote_addr - $remote_user [$time_local] "$request" '
                 '$status $body_bytes_sent "$request_body" '
                  '"$connection_requests" "$http_connection" '
                  '"$http_user_agent" "$http_x_forwarded_for" "$request_time" "$upstream_response_time" "$pipe"';
```

Logging customization
---------------------
Applications use Log4j. By default logging enabled to stdout
```
# Root logger option
log4j.rootLogger=INFO, stdout

# Direct log messages to stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.Target=System.out
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d [%t] %-5p %c - %m%n
```

use custom log4j.properties in classpath to override.

Usage examples
--------------

tail access.log in realtime and upload metrics to Graphite
```
...
```

parse archived access.log and upload metrics to Graphite
```
...
```
