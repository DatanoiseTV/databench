# databench

A multi-protocol network benchmark with a live ratatui dashboard.

`databench` stresses every layer that goes into making a real request — DNS,
TCP, TLS, HTTP — and exposes the timing of each one separately, so when
your numbers drop you can see *where* they dropped. It also benches the
lower layers on their own (ping, raw TCP connect, DNS lookup, TLS handshake)
so you can isolate a single stage on a remote host.

## Highlights

- Five benchmark modes: `http`, `ping`, `tcp`, `dns`, `tls`.
- HDR-Histogram latency tracking with full distribution down to p99.99.
- Per-phase breakdown: DNS / TCP / TLS / HTTP-handshake / TTFB / body
  download — every phase reported with min / mean / p50 / p95 / p99 / max.
- Live ratatui dashboard with per-phase averages, a status-code panel,
  a live req/s line chart, and a stall indicator that turns the progress
  gauge red when no probes complete for more than two seconds.
- HTTP/1.1 and HTTP/2 (ALPN-negotiated, or `h2c` with `--http2`) on top
  of `hyper`'s raw `client::conn` API — no pool, one persistent worker per
  connection.
- ICMP echo via `surge-ping` with the DGRAM socket hint, so it works
  unprivileged on macOS.

## Install

Build from source — needs a recent Rust toolchain:

    cargo install --path .
    # or just
    cargo build --release && ./target/release/databench --help

## Usage

    databench [-c N] [-n N | -z DURATION] [--timeout T] [-t THREADS] [--no-tui] <SUBCOMMAND>

Global flags (work before or after the subcommand):

| flag | meaning | default |
| --- | --- | --- |
| `-c, --connections` | concurrent workers | `50` |
| `-n, --requests` | total probes (overrides `-z`) | — |
| `-z, --duration` | benchmark duration (e.g. `10s`, `1m`) | `10s` |
| `--timeout` | per-probe timeout | `30s` |
| `-t, --threads` | tokio worker threads | `num_cpus` |
| `--no-tui` | disable the live dashboard | off |

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

`ping` uses the DGRAM-mode ICMP socket on macOS (no `sudo`). On Linux you
need either `CAP_NET_RAW` on the binary or:

    sudo sysctl -w net.ipv4.ping_group_range="0 65535"

### tcp

How fast can the remote accept connections? Each probe = one TCP handshake.

    databench -c 100 -z 10s tcp 10.0.0.1:443

### dns

How fast can your resolver answer? Uses the OS resolver (`getaddrinfo`).

    databench -c 16 -z 10s dns example.com

### tls

How fast can the remote terminate TLS? Each probe = TCP connect + full TLS
handshake (no HTTP).

    databench -c 50 -z 10s tls example.com:443
    databench -c 4 -n 100 tls 192.0.2.1:443 -k --sni my.host

## What you get back

```
Summary:
  Mode:           HTTP
  Protocol:       HTTP/2
  Workers:        50
  Total time:     10.0042 s
  Total reqs:       428912
  Successful:       428902
  Errors:           10
  reqs/sec:         42874.32
  Throughput:       42.31 MiB/s

Connection phases (per connection):
  phase                       min       mean       p50       p95       p99       max
  DNS lookup                1 µs       2 µs       1 µs       3 µs       5 µs    1.20 ms
  TCP connect              65 µs   41.57 ms   30.61 ms   92.48 ms   92.54 ms   92.54 ms
  TLS handshake          70.02 ms  265.14 ms  188.16 ms  744.96 ms  744.96 ms  744.96 ms
  HTTP handshake             1 µs      12 µs       5 µs      55 µs      81 µs      81 µs

Request phases (per request):
  phase                       min       mean       p50       p95       p99       max
  Time to first byte       29 µs     574 µs     503 µs    1.34 ms    1.90 ms    9.74 ms
  Body download             1 µs     231 µs     216 µs     739 µs    1.25 ms    9.71 ms
  Total                    39 µs     806 µs     746 µs    1.75 ms    2.42 ms   10.73 ms
```

Plus a full latency distribution (10/25/50/75/90/95/99/99.9/99.99 %)
and an inline ASCII histogram of total request latency.

## Live dashboard

While the benchmark runs you get five live panels:

- **header** — target, protocol, worker count, threads.
- **progress** — gauge for elapsed time or completed-probe count. Goes red
  with a `STALLED` label when no probe has completed for more than two
  seconds (so you can tell apart "slow server" from "wrong server").
- **live counters** — current ops/sec, bytes/sec, totals, errors.
- **phase averages** — running mean of every phase that applies to the
  selected mode.
- **responses** — for `http`, full status-code distribution with bars; for
  the other modes, ok/error split.
- **req/s line chart** — last few minutes of throughput, with auto-scaling
  y-axis and peak/avg/now overlaid in the title.

`q`, `Esc`, or `Ctrl-C` stop the run early and print the final summary.

## License

Dual-licensed under MIT or Apache-2.0, your choice.
