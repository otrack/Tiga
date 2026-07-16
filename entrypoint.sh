#!/bin/bash
set -e

PROTOCOL=${PROTOCOL:-tiga}
SERVER_NAME=${SERVER_NAME}
CONFIG_PATH=${CONFIG_PATH:-/app/config/config-local.yml}

if [ -z "$SERVER_NAME" ]; then
    echo "ERROR: SERVER_NAME environment variable must be set (e.g., SERVER_NAME=replica_1_1)"
    exit 1
fi
if [ "${PROTOCOL,,}" = "janus" ]; then
    if [ -f /usr/local/lib/libjanusycsb.so ]; then
        cp /usr/local/lib/libjanusycsb.so /usr/local/lib/libtigaycsb.so
    fi
fi

case "${PROTOCOL,,}" in
    tiga)
        echo "Starting Tiga server for $SERVER_NAME..."
        exec TigaServer -serverName "$SERVER_NAME" -config "$CONFIG_PATH" -logtostderr
        ;;
    calvin)
        echo "Starting Calvin server for $SERVER_NAME..."
        exec CalvinServer -serverName "$SERVER_NAME" -config "$CONFIG_PATH" -logtostderr
        ;;
    detock)
        echo "Starting Detock server for $SERVER_NAME..."
        exec DetockServer -serverName "$SERVER_NAME" -config "$CONFIG_PATH" -logtostderr
        ;;
    janus)
        echo "Starting Janus server for $SERVER_NAME..."
        exec deptran_server -P "$SERVER_NAME" -f "$CONFIG_PATH"
        ;;
    *)
        echo "ERROR: Unknown protocol '$PROTOCOL'. Supported protocols: tiga, calvin, detock, janus."
        exit 1
        ;;
esac
