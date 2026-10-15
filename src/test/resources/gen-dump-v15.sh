#!/bin/sh
# Generates dump-v15.rdb with Redis 8.10 (RDB version 15).
# usage: gen-dump-v15.sh <output-dir>
set -e
out=$(cd "$1" && pwd)
rm -f "$out/dump.rdb"
docker rm -f rdb-v15-gen >/dev/null 2>&1 || true
docker run -d --name rdb-v15-gen -v "$out":/data redis:8.10.2 redis-server --save "" \
  --hash-min-template-entries 2 --hash-max-template-entries 1000 >/dev/null
sleep 2
r() { docker exec -i rdb-v15-gen redis-cli "$@" >/dev/null; }
# template hashes: listpack (RDB_TYPE_HASH_TMPL_LP_REF) and array (RDB_TYPE_HASH_TMPL_ARRAY_REF)
r hset tl1 name n1 age 1 city c1
r hset tl2 name n2 age 2 city c2
# values over hash-max-listpack-value make template arrays
pad=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
r hset ta1 $(i=1; while [ $i -le 300 ]; do printf 'f%d v1-%d-%s ' $i $i $pad; i=$((i+1)); done)
r hset ta2 $(i=1; while [ $i -le 300 ]; do printf 'f%d v2-%d-%s ' $i $i $pad; i=$((i+1)); done)
r hset hplain only one
# array (RDB_TYPE_ARRAY): string, int, float and long string values, and a sparse index
r arset arr 0 hello 1 3.5 "a much longer string value that is not a small string"
r arset arr 1000 tail
# stream with a NACKed entry (RDB_TYPE_STREAM_LISTPACKS_5)
r xadd s1 1-1 a 1
r xadd s1 2-1 b 2
r xadd s1 3-1 c 3
r xgroup create s1 g1 0
r xreadgroup group g1 c1 count 3 streams s1 '>'
r xnack s1 g1 FAIL IDS 1 1-1
# stream with IDMP entries
r xadd s2 4-1 k v
r xcfgset s2 IDMP-DURATION 86400 IDMP-MAXSIZE 100
r xadd s2 IDMP p1 i1 '*' k v2
# plain types
r set str v
r set ttl v px 100000000000
r rpush list a b c
r sadd set a b
r zadd zset 1 a 2 b
r save
mv "$out/dump.rdb" "$out/dump-v15.rdb"
docker rm -f rdb-v15-gen >/dev/null
