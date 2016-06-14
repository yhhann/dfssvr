#!/bin/sh
/dfs/bin/dfssvr -server-name "$SERVER_ID" \
    -listen-addr "$LISTEN_ADDR" -register-addr "$REGISTER_ADDR" \
    -shard-dburi "$DBURI" -shard-name "$DBNAME" \
    -event-dburi "$EVENT_DBURI" -event-dbname "$EVENT_DBNAME" \
    -slog-dburi "$SLOG_DBURI" -slog-dbname "$SLOG_DBNAME" \
    -zk-addr "$ZK_ADDR" \
    -log-dir "$LOG_DIR" \
    -health-check-interval "$HEALTH_CHECK_INTERVAL" &

wait
