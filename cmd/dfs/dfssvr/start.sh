#!/bin/sh
/dfs/bin/dfssvr -server-name "$SERVER_ID" \
    -listen-addr "$LISTEN_ADDR" -register-addr "$REGISTER_ADDR" \
    -shard-dburi "$DBURI" -shard-name "$DBNAME" \
    -zk-addr "$ZK_ADDR" \
    -log-dir "$LOG_DIR" \
    -health-check-interval "$HEALTH_CHECK_INTERVAL" &

wait
