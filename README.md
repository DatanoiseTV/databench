# databench

A multi-protocol network and database benchmark with a live ratatui
dashboard. Stresses every layer in front of a service — DNS, TCP, TLS,
HTTP — and benches the lower layers (ICMP, raw TCP, DNS, TLS) and four
data backends (Redis, memcached, PostgreSQL, MySQL/MariaDB) on their own,
so when your numbers regress you can see *where* they regressed.

## Highlights

- **Eleven modes** so far: `http`, `ping`, `tcp`, `dns`, `tls`, `redis`,
  `memcache`, `postgres`, `mysql`, `s3`, `tinyice`.
- **Real-world workloads** for the database modes — not toy `SELECT 1`s:
  - Redis / memcached default to `memtier_benchmark`'s 1:10 SET:GET on
    32-byte values (the canonical published "ops/sec" shape).
  - PostgreSQL defaults to a `pgbench`-style TPC-B-lite transaction
    (3 updates + 1 select + 1 insert per txn, all prepared).
  - MySQL/MariaDB defaults to `sysbench`'s `oltp_read_write`
    (10 point selects + 4 range queries + 2 updates + 1 delete + 1 insert
    per transaction).
  - S3 / MinIO defaults to `warp`'s mixed mix (45% GET, 30% STAT,
    15% PUT, 10% DELETE).
  - TinyIce / Icecast2 streams a real linear-chirp MP3 (200 Hz → 8 kHz
    over 60 s, 128 kbps stereo) so the wire data is a well-formed MPEG
    audio stream and the benchmark exercises an actual broadcaster.
- **Sandbox by default** — every database mode creates a unique
  `databench_<runid>` namespace, populates it, runs the workload there,
  and cleans up at the end. `Ctrl-C` is honoured. Cleanup refuses to
  drop anything whose name doesn't start with `databench_`.
- **Custom mode** when you pass `--query` / `--cmd` / `--db`: databench
  hits exactly the target you named, no allow-listing, you own it.
- **HDR-Histogram latency** with the full distribution down to p99.99.
  Per-statement / per-op breakdown (top-N op latencies + min / mean / p50
  / p95 / p99 / max). Top-20 individual slowest probes called out
  explicitly so you don't lose the outliers.
- **Live ratatui dashboard**: progress gauge, status-code panel,
  phase-average panel, and a real req/s line chart with auto-scaled
  y-axis. The gauge goes red and shows `STALLED` if no probe completes
  for more than two seconds.
- **`--qps` rate cap** (token bucket, shared across workers) for gentle
  production probing.
- **`--warmup`** discards the first N seconds of probes so the
  steady-state numbers aren't polluted by cold-cache / JIT artifacts.
  Recommended: 10 s for KV stores, 30 s for SQL.
- HTTP/1.1 *and* HTTP/2 (ALPN-negotiated, or `h2c` with `--http2`) on
  top of `hyper`'s raw `client::conn` — no pool, one persistent
  connection per worker.
- ICMP via `surge-ping` with the DGRAM socket hint, so on macOS it
  works without `sudo`.

## Install

Build from source — needs a recent Rust toolchain:

    cargo install --path .
    # or
    cargo build --release && ./target/release/databench --help

## Usage

    databench [GLOBAL FLAGS] <SUBCOMMAND> [SUBCOMMAND FLAGS] <TARGET>

### Global flags (work before or after the subcommand)

| flag | meaning | default |
| --- | --- | --- |
| `-c, --connections` | concurrent workers | `50` |
| `-n, --requests` | total probes (overrides `-z`) | — |
| `-z, --duration` | benchmark duration (e.g. `10s`, `1m`) | `10s` |
| `--timeout` | per-probe timeout | `30s` |
| `-t, --threads` | tokio worker threads | num CPUs |
| `--no-tui` | disable the live dashboard | off |
| `--qps` | hard cap on total probes/second | — |
| `--warmup` | discard the first N seconds of probes | — |

### http

    databench http https://example.com/                   # 50 conns, 10s
    databench -c 200 -z 30s http https://api.example.com/v1/health
    databench -n 10000 http -m POST -H "content-type: application/json" \
              -d '{"hello":"world"}' https://api.example.com/echo

`http` flags: `-m/--method`, `-H/--header` (repeatable), `-d/--body`,
`-k/--insecure`, `--http2`, `--no-keepalive`.

### ping

    databench -c 4 -z 5s ping 1.1.1.1
    databench -c 1 -n 100 -6 ping example.com           # IPv6
    databench -c 8 -z 30s --ttl 64 ping <target>

DGRAM-mode ICMP. macOS: works without `sudo`. Linux: needs
`CAP_NET_RAW` on the binary or
`sudo sysctl -w net.ipv4.ping_group_range="0 65535"`.

### tcp

How fast can the remote accept connections? Each probe = one TCP
handshake.

    databench -c 100 -z 10s tcp 10.0.0.1:443

### dns

How fast can your resolver answer? Uses the OS resolver (`getaddrinfo`).

    databench -c 16 -z 10s dns example.com

### tls

How fast can the remote terminate TLS? Each probe = TCP connect + full
TLS handshake (no HTTP).

    databench -c 50 -z 10s tls example.com:443
    databench -c 4 -n 100 tls 192.0.2.1:443 -k --sni my.host

### redis

`memtier_benchmark`-shaped workloads on a sandboxed key prefix.

    # default workload (memtier 1:10 SET:GET, 32B values, 100k keys)
    databench -c 50 -z 30s --warmup 10s redis 127.0.0.1:6379

    # write-heavy
    databench -c 50 -z 30s redis 127.0.0.1:6379 --workload write

    # custom command — no seeding, no cleanup, you own the target
    databench -c 50 -z 10s --qps 100 redis prod-cache.local --cmd "PING"

`redis` flags: `--workload {read|memtier|mixed|write}`, `--db <N>`,
`--user`, `--password` (or `DATABENCH_REDIS_PASSWORD` env), `--tls`,
`--seed-keys`, `--seed-value-size`, `--seed-ttl`, `--cmd`,
`--no-sandbox`. Sandbox lives on DB 15 by default. Warns if the
target's `maxmemory-policy` could evict user data.

### memcache

Same shape as Redis — memtier-canonical defaults, sandboxed by key
prefix with a TTL safety net.

    databench -c 50 -z 30s --warmup 10s memcache 127.0.0.1:11211
    databench -c 4 -n 100 memcache 127.0.0.1:11211 --cmd "get somekey"

`memcache` flags: `--workload {read|memtier|mixed|write}`, `--seed-keys`,
`--seed-value-size`, `--seed-ttl`, `--cmd`, `--no-sandbox`.

### postgres

`pgbench`-style TPC-B-lite transaction in a brand-new sandbox database
(falls back to a `databench_<runid>` schema in `--db` if the user can't
`CREATE DATABASE`).

    # default: TPC-B-lite, scale=1 (100k accounts)
    DATABENCH_PG_PASSWORD=secret databench -c 8 -z 30s --warmup 30s \
        postgres 127.0.0.1:5432 --user app

    # read-only
    databench -c 50 -z 30s postgres 127.0.0.1:5432 --user app \
        --workload select-only --scale 50

    # your own query against your own table
    databench -c 8 -z 30s postgres 127.0.0.1:5432 --user app --no-sandbox \
        --db prod_replica --query "SELECT id FROM users WHERE id = 1"

`postgres` flags: `--db`, `--user`, `--password` (or
`DATABENCH_PG_PASSWORD`), `--scale`, `--workload {select-only|tpcb|insert}`,
`--query`, `--no-sandbox`. databench refuses to start if `--scale < -c`
in TPC-B mode (the small branches/tellers tables would just measure
lock contention). The `application_name` is set to `databench` so a DBA
can spot us in `pg_stat_activity`.

### mysql

`sysbench oltp_read_write` shape on a sandboxed database. Schema and
statement set are the canonical sysbench `sbtest1`.

    # default: oltp_read_write, table_size=10000
    DATABENCH_MYSQL_PASSWORD=secret databench -c 16 -z 30s --warmup 30s \
        mysql 127.0.0.1:3306 --user app

    # read-only OLTP (10 selects + 4 ranges per txn)
    databench -c 50 -z 30s mysql 127.0.0.1:3306 --workload read-only \
        --table-size 1000000

    # raw point-select micro
    databench -c 100 -z 10s mysql 127.0.0.1:3306 --workload point-select

`mysql` flags: `--db`, `--user`, `--password` (or
`DATABENCH_MYSQL_PASSWORD`), `--table-size`,
`--workload {point-select|read-only|oltp|write-only}`, `--query`,
`--no-sandbox`. Tested against MariaDB 11. MySQL 8.x uses
`caching_sha2_password` which the underlying driver doesn't speak
without TLS — start the server with `mysql_native_password` if you hit
auth-hangs.

### s3

Object-store benchmark, modelled on MinIO's `warp`. Sandboxed in a
fresh `databench-<runid>` bucket; pre-seeds N objects, runs the
workload, then deletes every object and the bucket on exit. Cleanup
refuses to touch any bucket whose name doesn't start with `databench-`.

    # MinIO local — uses minioadmin/minioadmin defaults
    docker run -d --rm --name minio -p 9000:9000 -p 9001:9001 \
        -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
        minio/minio server /data --console-address ":9001"

    databench -c 32 -z 30s --warmup 5s s3 http://127.0.0.1:9000

    # warp-shaped large-object run
    databench -c 16 -z 30s s3 http://127.0.0.1:9000 \
        --object-size 10485760 --seed-objects 100

    # AWS S3
    DATABENCH_S3_ACCESS_KEY=... DATABENCH_S3_SECRET_KEY=... \
        databench -c 32 -z 60s s3 https://s3.us-east-1.amazonaws.com \
        --region us-east-1

### tinyice

Icecast2 streaming benchmark, modelled after how a TinyIce / Icecast2
server actually gets used: many listener clients pulling a stream from
a mount, sources pushing audio in.

Three modes:

- **listen** — `-c` workers do `GET /<mount>` against the server,
  drain bytes forever, count drops. Answers "how many concurrent
  listeners can the server hold?". Round-robins workers across
  `--mounts a,b,c`.
- **source** — `--sources` workers (or `-c` if not set) open
  `SOURCE /<mount>` with HTTP Basic auth, push the embedded chirp MP3
  paced at 128 kbps. Each worker takes one mount round-robin.
  Records source-connect handshake time.
- **mixed**  — sources start first (with a small grace period), then
  `-c` listener workers fan out across the mounts. The realistic
  radio-station shape.

```sh
# listen-only against a public stream — fan out 1000 listeners on /live
databench -c 1000 -z 60s tinyice https://radio.example.com --mounts /live

# push to a single mount with the source password
DATABENCH_TINYICE_PASSWORD=secret \
  databench -c 1 -z 30s tinyice https://radio.example.com \
  --mode source --mounts /live

# end-to-end: 1 source feeding 50 listeners across 3 mounts
DATABENCH_TINYICE_PASSWORD=secret \
  databench -c 50 -z 60s tinyice https://radio.example.com \
  --mode mixed --sources 3 --mounts /live,/chill,/talk
```

`tinyice` flags: `--mode {listen|source|mixed}`, `--mounts <list>`
(comma-separated), `--source-user`, `--source-password` (or env
`DATABENCH_TINYICE_PASSWORD`), `--source-bitrate`, `--sources`,
`-k/--insecure` (TLS verify off). Both `http://` and `https://`
endpoints are supported. The `connections` live counter is repurposed
for tinyice as **currently-connected listeners + sources** so the
dashboard reflects what an operator cares about. Per-op latency table
shows `listen-connect` (TCP+TLS+HTTP+TTFB to first audio byte) and
`source-connect` (TCP+TLS+HTTP SOURCE + auth) separately.

The source mode embeds a 60-second linear-chirp MP3 (200 Hz → 8 kHz at
128 kbps). Because the listener can pattern-match a window of received
bytes against the embedded file, **mixed mode also reports a real
end-to-end RTT** as `rtt-by-bytes` in the per-op latency table:

1. The first source worker stamps `t0` = wall time of the first byte
   sent on each mount.
2. Each listener takes a 512-byte window of recently-received bytes
   every ~500 ms and locates it (uniquely) in the embedded sweep, which
   gives the source byte position the listener has just observed.
3. RTT = (where the source is now in its byte stream) − (where the
   listener just observed) − converted to time at 128 kbps.

The 60 s period exceeds typical Icecast burst depth (~32 s), so the
mapping stays unambiguous even when a listener is consuming the
server's burst buffer. The first reported lag will be high (whole
burst replayed); after the burst drains, lag converges to the real
end-to-end RTT.

`s3` flags: `--access-key`, `--secret-key` (or `DATABENCH_S3_*` env),
`--region`, `--workload {read|mixed|write|stat}`, `--object-size`,
`--seed-objects`, `--bucket`, `--no-sandbox`. Mixed mode shows
`GET_404` / `STAT_404` in the per-op table separately from real
hits — those are objects the workload's own DELETEs removed earlier
in the run, not server errors.

## What you get back

```
Summary:
  Mode:           PostgreSQL
  Workers:        8
  Total time:     30.0042 s
  Total txns:       28104
  Successful:     28104
  Errors:         0
  txns/sec:         936.69

Per-operation latency:
  op                          count        min       mean        p50        p95        p99        max
  insert-history              28104      46 µs      78 µs      73 µs     103 µs     133 µs     432 µs
  update-tellers              28104      53 µs     407 µs      74 µs    1.89 ms    2.56 ms    5.39 ms
  commit                      28104     497 µs     625 µs     605 µs     780 µs     897 µs    4.17 ms
  ...

Slowest probes:
  #    op                          latency
  1    update-branches           5.39 ms
  2    update-tellers            5.24 ms
  ...
```

Plus a full latency distribution (10/25/50/75/90/95/99/99.9/99.99 %),
an inline ASCII histogram, and a per-phase table for connection-phase
benches.

## Live dashboard

While the benchmark runs you get five live panels:

- **header** — target, protocol, worker count, threads.
- **progress** — gauge for elapsed time or completed-probe count. Goes
  red with a `STALLED` label when no probe completes for more than two
  seconds.
- **live counters** — current ops/sec, bytes/sec, totals, errors.
- **phase averages** — running mean of every phase that applies.
- **responses / probes / ops / txns** — for `http`, full status-code
  distribution; for the others, ok/error or a per-mode summary.
- **req/s line chart** — recent throughput, auto-scaling y-axis,
  peak/avg/now overlaid in the title.

`q`, `Esc`, or `Ctrl-C` stop the run early and print the final summary.

## Safety defaults

- Database modes never default to writes against unknown data — the
  sandbox always owns its own keys/tables/database.
- Cleanup runs in a `defer`-style hook at the end of every run, even on
  `Ctrl-C`. Refuses to drop anything whose name doesn't start with
  `databench_`.
- Passwords are read from env vars by default (`DATABENCH_*_PASSWORD`)
  to keep them out of `ps` output.
- Per-probe timeout is enforced at every layer.
- `--qps` lets you cap the rate before stress-testing a live system.

## License

Dual-licensed under MIT or Apache-2.0, your choice.
