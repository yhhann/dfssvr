#!/bin/sh

## SERVER_ID is server id, unique.
SERVER_ID="cd-dfssvr-50"
LISTEN_ADDR=":10000"
REGISTER_ADDR="192.168.1.50:10000"
SHARD_URI="mongodb://192.168.1.57:27017"
SHARD_DBNAME="shard"
EVENT_DBNAME="eventdb"
ZK_ADDR="192.168.1.57:2181"
HEALTH_CHECK_INTERVAL=600
HEALTH_CHECK_TIMEOUT=60
LOG_DIR="`pwd`/log"

bin/dfssvr -server-name "$SERVER_ID" \
    -listen-addr "$LISTEN_ADDR" -register-addr "$REGISTER_ADDR" \
    -shard-dburi "$SHARD_URI" -shard-name "$SHARD_DBNAME" \
    -event-dbname "$EVENT_DBNAME" \
    -zk-addr "$ZK_ADDR" \
    -health-check-interval "$HEALTH_CHECK_INTERVAL" \
    -health-check-timeout "$HEALTH_CHECK_TIMEOUT" \
    -gluster-log-dir "$LOG_DIR" \
    -log_dir "$LOG_DIR" -v 2 -logtostderr=false &
