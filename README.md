# databench

A multi-protocol network and database benchmark with a live ratatui
dashboard. Stresses every layer in front of a service — DNS, TCP, TLS,
HTTP — and benches the lower layers (ICMP, raw TCP, DNS, TLS) and four
data backends (Redis, memcached, PostgreSQL, MySQL/MariaDB) on their own,
so when your numbers regress you can see *where* they regressed.

## Highlights

- **Nine modes** so far: `http`, `ping`, `tcp`, `dns`, `tls`, `redis`,
  `memcache`, `postgres`, `mysql`.
- **Real-world workloads** for the database modes — not toy `SELECT 1`s:
  - Redis / memcached default to `memtier_benchmark`'s 1:10 SET:GET on
    32-byte values (the canonical published "ops/sec" shape).
  - PostgreSQL defaults to a `pgbench`-style TPC-B-lite transaction
    (3 updates + 1 select + 1 insert per txn, all prepared).
  - MySQL/MariaDB defaults to `sysbench`'s `oltp_read_write`
    (10 point selects + 4 range queries + 2 updates + 1 delete + 1 insert
    per transaction).
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
