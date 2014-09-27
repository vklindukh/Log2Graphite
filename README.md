Log2Graphite
============

access.log parser.
Application parse and upload metrics to graphite server in realtime.
In addition it supports non-realtime operation (reparsing olg logs).

What is it
---------------
Sometimes you want to parse logs (maybe in realtime), retrieve some custom metrics and finally got some graphs.
For log parsing could be used Logstash, which has plugin for uploading metrics to Graphite.
Such configuraton works fine for most applications.

But not for me. I realized than parsing 100G access.log takes too mutch time. And realtime acccess.log parsing takes too mutch CPU cores for QPS rates 5K and more.

Log2Graphite app is pure Java app, tested using Java 1.7. Multithreaded, high performance.

Features
---------------
Log2Graphite supports:
* realtime metrics upload with 1 minute granularity
* customs access.log format. It is configurable using apache / nginx style
* real time log parsing
* .gz format for non-realtime parsing
* S3 Amazon storage for non-realtime parsing

Metrics supported
---------------
* total requests counter
* response and upstream time (min / max / avg / stdev / 99%)
* response code
* data size
* GET/POST/OTHER counters
* specific metrics for my application, likely nobody need it 


Usage
---------------
```
usage: Log2Graphite -f <filepath> [ options ]
 -atime <aggregate_time>   aggregate metric timeout in seconds. default is 60
 -c <config>               path to config file (for custom access.log format)
 -f <filepath>             path to log file (local or S3)
 -h <host>                 Graphite host IP. default is not upload metrics to graphite
 -p <port>                 Graphite port. default is 2003
 -notail                   parse single file from start without tail (non-realtime parsing)
 -start                    for realtime parsing only: tail log from start. default is to start parsing from the end
 -t <arg>                  number of parsers. default is 1 parser
 -key <AWS access key>     S3 access key
 -secret <AWS secret key>  S3 secret key
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
Used Log4j. By default logging enabled to stdout
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

tail access.log in realtime from the beginning and upload metrics to Graphite. 
```
java -classpath <path to config file>:<path to jar file> com.company.log2graphite.Log2Graphite -f <path to access.log> -t 4 -h <IP> -start
```

parse archived access.log and upload metrics to Graphite
```
java -classpath <path to config file>:<path to jar file> com.company.log2graphite.Log2Graphite -f <path to access.log-20140101.gz> -t 10 -h <IP> -notail
```
parse archived access.log from S3 and upload metrics to Graphite
```
java -classpath <path to config file>:<path to jar file> com.company.log2graphite.Log2Graphite -f s3://bucket/path/to/access.log.gz -t 10 -h <IP> -notail -key <S3 access key> -secret <S3 secret key>
```

Performance
--------------

Tested on c3.8xlarge AWS instance. Log2Graphite started with 10 Parser threads

10G archived access.log.gz : ~9 minutes

60G raw access.log : ~4 minutes

6G S3 archived access.log.gz : ~17 minutes
