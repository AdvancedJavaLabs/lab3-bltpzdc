#!/bin/bash
set -e

sleep 5
hdfs dfs -mkdir -p /input || true
hdfs dfs -mkdir -p /output || true
hdfs dfs -Ddfs.blocksize=5242880 -put -f /data/*.csv /input/
