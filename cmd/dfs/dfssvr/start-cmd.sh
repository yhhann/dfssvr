#!/bin/sh

bin/dfssvr -server-name "cd-dfssvr-50" \
    -listen-addr ":10000" -register-addr "192.168.1.50:10000" \
    -shard-name "shard" -shard-dburi "mongodb://192.168.1.57:27017" \
    -slog-dbname "slogdb" -slog-dburi "mongodb://192.168.1.57:27017" \
    -event-dbname "eventdb" -event-dburi "mongodb://192.168.1.57:27017" \
    -zk-addr "192.168.1.57:2181" \
    -log_dir "/root/dfssvr/log" -v 2  &
